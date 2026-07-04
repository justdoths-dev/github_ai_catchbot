from __future__ import annotations

import json

import pytest

from services.maintenance.redis_rebuild_readiness import (
    DurableCategoryInventory,
    DurableInventorySnapshot,
    KnownQueueSpec,
    RedisInventorySnapshot,
    RedisQueueInventory,
    RedisReadOnlyQueueInspector,
    RedisRebuildReadinessRequest,
    build_redis_rebuild_readiness_report,
    render_sanitized_json,
)
from tests.component.services.maintenance._fakes import config


RAW_RUNTIME_ENV_PATH = "/abs/private/runtime.env"
RAW_DB_URL = "postgresql+psycopg://sentinel-user:sentinel-pass@db.internal/github_ai_catchbot"
RAW_REDIS_URL = "redis://:sentinel-token@redis.internal:6379/0"
RAW_STREAM_ID = "1711111111111-42"
RAW_DEDUPE_KEY = "notify:retry-intent:sentinel-dedupe"
RAW_PAYLOAD = '{"payload_json":"sentinel payload"}'
RAW_SOURCE_TEXT = "raw telegram source text sentinel"
RAW_URL = "https://private.example.invalid/path"


class FakeRedisReader:
    def __init__(self, snapshot: RedisInventorySnapshot) -> None:
        self.snapshot = snapshot
        self.calls: list[tuple[str, ...]] = []
        self.xadd_called = False
        self.xack_called = False
        self.xgroup_create_called = False

    async def inspect_queues(self, queues):
        self.calls.append(tuple(queue.key for queue in queues))
        return self.snapshot

    async def xadd(self, *args, **kwargs):
        self.xadd_called = True
        raise AssertionError("redis mutation must not be called")

    async def xack(self, *args, **kwargs):
        self.xack_called = True
        raise AssertionError("redis ack must not be called")

    async def xgroup_create(self, *args, **kwargs):
        self.xgroup_create_called = True
        raise AssertionError("redis group mutation must not be called")


class FakeDurableReader:
    def __init__(self, snapshot: DurableInventorySnapshot) -> None:
        self.snapshot = snapshot
        self.calls: list[int] = []
        self.commit_called = False
        self.write_called = False

    async def load_rebuild_sources(self, *, max_sample: int):
        self.calls.append(max_sample)
        return self.snapshot

    async def commit(self):
        self.commit_called = True
        raise AssertionError("db commit must not be called")

    async def insert_job_attempt(self):
        self.write_called = True
        raise AssertionError("db write must not be called")


class FakeRedisClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.mutation_calls: list[str] = []
        self.groups = {"q.maintenance": [{"name": "maintenance", "pending": 2}]}

    async def ping(self):
        self.calls.append(("ping", ""))
        return True

    async def exists(self, name):
        self.calls.append(("exists", name))
        return 1 if name == "q.maintenance" else 0

    async def type(self, name):
        self.calls.append(("type", name))
        return "stream"

    async def xlen(self, name):
        self.calls.append(("xlen", name))
        return 3

    async def xinfo_stream(self, name):
        self.calls.append(("xinfo_stream", name))
        return {"length": 3, "last-entry": (RAW_STREAM_ID, {})}

    async def xinfo_groups(self, name):
        self.calls.append(("xinfo_groups", name))
        return self.groups.get(name, [])

    async def xpending(self, name, groupname):
        self.calls.append(("xpending", name))
        return {"pending": 2, "min": RAW_STREAM_ID, "max": RAW_STREAM_ID}

    async def flushdb(self, *args, **kwargs):
        self.mutation_calls.append("flushdb")
        raise AssertionError("flushdb must not be called")

    async def xadd(self, *args, **kwargs):
        self.mutation_calls.append("xadd")
        raise AssertionError("xadd must not be called")

    async def xack(self, *args, **kwargs):
        self.mutation_calls.append("xack")
        raise AssertionError("xack must not be called")

    async def xgroup_create(self, *args, **kwargs):
        self.mutation_calls.append("xgroup_create")
        raise AssertionError("xgroup_create must not be called")


def _redis_snapshot() -> RedisInventorySnapshot:
    return RedisInventorySnapshot(
        queues=(
            RedisQueueInventory(
                queue_key="maintenance",
                stream_present=True,
                stream_type_bucket="stream",
                stream_length=3,
                configured_group_count=1,
                present_group_count=1,
                missing_group_count=0,
                pending_count=2,
            ),
            RedisQueueInventory(
                queue_key="replay",
                stream_present=False,
                stream_type_bucket="missing",
                configured_group_count=1,
                present_group_count=0,
                missing_group_count=1,
                pending_count=0,
                reason_code="stream_missing",
            ),
        )
    )


def _durable_snapshot() -> DurableInventorySnapshot:
    return DurableInventorySnapshot(
        categories=(
            DurableCategoryInventory(
                name="event_outbox",
                state="present",
                total_count=3,
                status_counts={"pending": 2, "failed": 1},
                queue_counts={"maintenance": 1, "notification_send": 2},
                age_counts={"recent": 3},
                sample_shape_count=2,
            ),
            DurableCategoryInventory(
                name="job_attempts",
                state="present",
                total_count=1,
                status_counts={"running": 1},
                queue_counts={"maintenance": 1},
                stage_counts={"maintenance": 1},
                age_counts={"older": 1},
                sample_shape_count=1,
            ),
            DurableCategoryInventory(
                name="notification_plans",
                state="present",
                total_count=1,
                status_counts={"queued": 1},
                queue_counts={"notification_send": 1},
                age_counts={"fresh": 1},
                sample_shape_count=1,
            ),
            DurableCategoryInventory(
                name="replay_requests",
                state="present",
                total_count=1,
                status_counts={"pending": 1},
                queue_counts={"replay": 1},
                stage_counts={"delivery": 1},
                age_counts={"fresh": 1},
                sample_shape_count=1,
            ),
            DurableCategoryInventory(
                name="dead_letter_entries",
                state="not_present_in_current_head",
                reason_code="not_present_in_current_head",
            ),
        )
    )


@pytest.mark.asyncio
async def test_inventory_mode_uses_read_only_abstractions_and_returns_compact_json() -> None:
    redis = FakeRedisReader(_redis_snapshot())
    durable = FakeDurableReader(_durable_snapshot())

    report = await build_redis_rebuild_readiness_report(
        RedisRebuildReadinessRequest(mode="inventory", include_empty=True, max_sample=2),
        config=config(),
        redis_reader=redis,
        durable_reader=durable,
    )
    output = render_sanitized_json(report)
    parsed = json.loads(output)

    assert output.endswith("\n")
    assert "\n" not in output[:-1]
    assert parsed["schema_version"] == "redis_rebuild_readiness_report_v1"
    assert parsed["runner_name"] == "bounded_redis_rebuild_readiness_runner"
    assert parsed["mode"] == "inventory"
    assert parsed["status"] == "pass"
    assert parsed["authority"]["redis_read_attempted"] is True
    assert parsed["authority"]["db_read_attempted"] is True
    assert redis.calls == [("notification_send", "replay", "maintenance")]
    assert durable.calls == [2]
    assert redis.xadd_called is False
    assert redis.xack_called is False
    assert redis.xgroup_create_called is False
    assert durable.commit_called is False
    assert durable.write_called is False


@pytest.mark.asyncio
async def test_plan_mode_returns_dry_run_plan_with_all_mutation_fields_false() -> None:
    report = await build_redis_rebuild_readiness_report(
        RedisRebuildReadinessRequest(mode="plan", all_known_queues=True),
        config=config(),
        redis_reader=FakeRedisReader(_redis_snapshot()),
        durable_reader=FakeDurableReader(_durable_snapshot()),
    )
    plan = report["dry_run_rebuild_plan"]

    assert report["mode"] == "plan"
    assert report["status"] == "pass"
    assert plan["would_create_streams"] is False
    assert plan["would_create_groups"] is False
    assert plan["would_xadd_jobs"] is False
    assert plan["would_ack_or_delete_pending"] is False
    assert plan["planned_actions_are_dry_run_only"] is True
    assert "redis_xadd_for_rebuildable_durable_rows" in plan["o3b_required_authority"]
    assert report["authority"]["redis_mutation_attempted"] is False
    assert report["authority"]["db_write_attempted"] is False


@pytest.mark.asyncio
async def test_missing_redis_stream_and_group_are_reported_not_mutated() -> None:
    redis = FakeRedisReader(_redis_snapshot())
    report = await build_redis_rebuild_readiness_report(
        RedisRebuildReadinessRequest(mode="plan", include_empty=True),
        config=config(),
        redis_reader=redis,
        durable_reader=FakeDurableReader(_durable_snapshot()),
    )

    assert report["redis_inventory"]["stream_presence_buckets"]["replay"] == "missing"
    assert "replay" in report["redis_inventory"]["missing_stream_buckets"]
    assert "replay" in report["redis_inventory"]["missing_group_buckets"]
    assert report["dry_run_rebuild_plan"]["missing_stream_buckets"] == ["replay"]
    assert redis.xadd_called is False
    assert redis.xgroup_create_called is False


@pytest.mark.asyncio
async def test_pending_db_categories_are_bucketed_without_raw_ids_or_payloads() -> None:
    report = await build_redis_rebuild_readiness_report(
        RedisRebuildReadinessRequest(mode="inventory", include_empty=True),
        config=config(),
        redis_reader=FakeRedisReader(_redis_snapshot()),
        durable_reader=FakeDurableReader(_durable_snapshot()),
    )
    output = render_sanitized_json(report)
    categories = report["durable_inventory"]["durable_source_categories"]

    assert categories["event_outbox"]["count_bucket"] == "few"
    assert categories["event_outbox"]["status_buckets"] == {"failed": "one", "pending": "few"}
    assert categories["notification_plans"]["queue_buckets"] == {"notification_send": "one"}
    for raw in (RAW_DEDUPE_KEY, RAW_PAYLOAD, RAW_SOURCE_TEXT, RAW_URL, RAW_STREAM_ID):
        assert raw not in output


@pytest.mark.asyncio
async def test_absent_tables_are_reported_as_not_present_in_current_head() -> None:
    report = await build_redis_rebuild_readiness_report(
        RedisRebuildReadinessRequest(mode="plan"),
        config=config(),
        redis_reader=FakeRedisReader(_redis_snapshot()),
        durable_reader=FakeDurableReader(_durable_snapshot()),
    )

    dlq = report["durable_inventory"]["durable_source_categories"]["dead_letter_entries"]
    assert dlq["state"] == "not_present_in_current_head"
    assert "dead_letter_entries:not_present_in_current_head" in report["dry_run_rebuild_plan"][
        "planned_action_buckets"
    ]["unsupported_or_not_present"]


@pytest.mark.asyncio
async def test_runtime_env_path_and_values_are_not_output() -> None:
    report = await build_redis_rebuild_readiness_report(
        RedisRebuildReadinessRequest(mode="inventory"),
        config=config(),
        redis_reader=FakeRedisReader(_redis_snapshot()),
        durable_reader=FakeDurableReader(_durable_snapshot()),
    )
    output = render_sanitized_json(report)

    for raw in (RAW_RUNTIME_ENV_PATH, RAW_DB_URL, RAW_REDIS_URL, "sentinel-pass", "sentinel-token"):
        assert raw not in output
    assert report["redactions_applied"]["runtime_env_path_omitted"] is True
    assert report["authority"]["runtime_env_values_output"] is False
    assert report["authority"]["secrets_output"] is False


@pytest.mark.asyncio
async def test_read_only_redis_inspector_never_calls_mutating_methods() -> None:
    client = FakeRedisClient()
    inspector = RedisReadOnlyQueueInspector(client)

    snapshot = await inspector.inspect_queues(
        [
            KnownQueueSpec("maintenance", "q.maintenance", ("maintenance",)),
            KnownQueueSpec("replay", "q.replay", ("maintenance-replay",)),
        ]
    )

    assert client.mutation_calls == []
    assert [queue.queue_key for queue in snapshot.queues] == ["maintenance", "replay"]
    assert snapshot.queues[0].pending_count == 2
    assert snapshot.queues[1].stream_present is False
    called_names = {name for name, _ in client.calls}
    assert called_names == {"ping", "exists", "type", "xlen", "xinfo_stream", "xinfo_groups", "xpending"}
