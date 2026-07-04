from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest

from services.maintenance.redis_rebuild_execution import (
    EXPECTED_BASELINE_HEAD,
    RedisRebuildExecutionRequest,
    build_redis_rebuild_execution_report,
    render_sanitized_json,
)
from services.maintenance.redis_rebuild_readiness import DurableCategoryInventory, DurableInventorySnapshot
from services.outbox_relay.models import OutboxEventRow
from tests.component.services.maintenance._fakes import config


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/redis_rebuild_execution.py"
TOOL_PATH = ROOT / "tools/bounded_redis_rebuild_execution_runner.py"

RAW_RUNTIME_ENV_PATH = "/abs/private/runtime.env"
RAW_DB_URL = "raw-db-url-sentinel"
RAW_REDIS_URL = "raw-redis-url-sentinel"
RAW_STREAM_ID = "1711111111111-42"
RAW_EVENT_ID = UUID("11111111-1111-4111-8111-111111111111")
RAW_OBJECT_ID = UUID("22222222-2222-4222-8222-222222222222")
RAW_DEDUPE_KEY = "maintenance:secret-dedupe-key"
RAW_PAYLOAD = "payload_json sentinel body"
RAW_SOURCE_TEXT = "raw telegram source text sentinel"
RAW_URL = "raw-url-sentinel"


class FakeRedis:
    def __init__(self, *, stream_present: bool = True, group_present: bool = True) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {"q.maintenance": []} if stream_present else {}
        self.groups: dict[str, set[str]] = {"q.maintenance": {"maintenance"} if group_present else set()}
        self.xadd_calls: list[tuple[str, dict[str, str]]] = []
        self.xgroup_create_calls: list[tuple[str, str, bool]] = []
        self.xrange_calls: list[tuple[str, int | None]] = []
        self.forbidden_calls: list[str] = []

    async def ping(self):
        return True

    async def exists(self, name):
        return 1 if name in self.streams else 0

    async def type(self, name):
        return "stream"

    async def xlen(self, name):
        return len(self.streams.get(name, []))

    async def xinfo_stream(self, name):
        return {"length": len(self.streams.get(name, [])), "last-entry": (RAW_STREAM_ID, {})}

    async def xinfo_groups(self, name):
        return [{"name": group_name, "pending": 0} for group_name in sorted(self.groups.get(name, set()))]

    async def xpending(self, name, groupname):
        return {"pending": 0, "min": RAW_STREAM_ID, "max": RAW_STREAM_ID}

    async def xrange(self, name, min="-", max="+", count=None):
        self.xrange_calls.append((name, count))
        return list(self.streams.get(name, []))[: count or None]

    async def xadd(self, name, fields):
        self.xadd_calls.append((name, dict(fields)))
        self.streams.setdefault(name, [])
        message_id = f"1711111111111-{len(self.streams[name]) + 1}"
        self.streams[name].append((message_id, dict(fields)))
        return message_id

    async def xgroup_create(self, name, groupname, id="0", mkstream=False):
        self.xgroup_create_calls.append((name, groupname, mkstream))
        if name not in self.streams and not mkstream:
            raise RuntimeError("stream missing")
        groups = self.groups.setdefault(name, set())
        if groupname in groups:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        groups.add(groupname)
        return True

    async def flushdb(self, *args, **kwargs):
        self.forbidden_calls.append("flushdb")
        raise AssertionError("flushdb must not be called")

    async def flushall(self, *args, **kwargs):
        self.forbidden_calls.append("flushall")
        raise AssertionError("flushall must not be called")

    async def delete(self, *args, **kwargs):
        self.forbidden_calls.append("delete")
        raise AssertionError("delete must not be called")

    async def unlink(self, *args, **kwargs):
        self.forbidden_calls.append("unlink")
        raise AssertionError("unlink must not be called")

    async def xack(self, *args, **kwargs):
        self.forbidden_calls.append("xack")
        raise AssertionError("xack must not be called")

    async def xdel(self, *args, **kwargs):
        self.forbidden_calls.append("xdel")
        raise AssertionError("xdel must not be called")

    async def xreadgroup(self, *args, **kwargs):
        self.forbidden_calls.append("xreadgroup")
        raise AssertionError("xreadgroup must not be called")

    async def xclaim(self, *args, **kwargs):
        self.forbidden_calls.append("xclaim")
        raise AssertionError("xclaim must not be called")

    async def xautoclaim(self, *args, **kwargs):
        self.forbidden_calls.append("xautoclaim")
        raise AssertionError("xautoclaim must not be called")


class FakeDurableReader:
    def __init__(self, rows: list[OutboxEventRow]) -> None:
        self.rows = rows
        self.inventory_calls: list[int] = []
        self.row_calls: list[tuple[str, int]] = []
        self.commit_called = False
        self.write_called = False

    async def load_rebuild_sources(self, *, max_sample: int):
        self.inventory_calls.append(max_sample)
        return DurableInventorySnapshot(
            categories=(
                DurableCategoryInventory(
                    name="event_outbox",
                    state="present",
                    total_count=len(self.rows),
                    status_counts={"pending": len(self.rows)} if self.rows else {},
                    queue_counts={"maintenance": len(self.rows)} if self.rows else {},
                    age_counts={"fresh": len(self.rows)} if self.rows else {},
                    sample_shape_count=min(max_sample, len(self.rows)),
                ),
            )
        )

    async def load_rebuildable_event_outbox_rows(self, *, queue_name: str, limit: int):
        self.row_calls.append((queue_name, limit))
        return tuple(self.rows[:limit]) if queue_name == "q.maintenance" else ()

    async def commit(self):
        self.commit_called = True
        raise AssertionError("db commit must not be called")

    async def insert_job_attempt(self):
        self.write_called = True
        raise AssertionError("db write must not be called")


def _row() -> OutboxEventRow:
    return OutboxEventRow(
        event_id=RAW_EVENT_ID,
        event_type="notification.delivery.result.v1",
        aggregate_type="notification_plan",
        aggregate_id=RAW_OBJECT_ID,
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json={"payload_json": RAW_PAYLOAD, "source_text": RAW_SOURCE_TEXT, "url": RAW_URL},
        status="pending",
        fail_count=0,
        created_at=datetime(2026, 7, 4, tzinfo=timezone.utc),
    )


def _execute_request(**overrides) -> RedisRebuildExecutionRequest:
    values = {
        "mode": "execute",
        "queue_selector": "maintenance",
        "max_rebuild_jobs": 5,
        "expected_head": EXPECTED_BASELINE_HEAD,
        "understand_mutates_redis": True,
        "approve_redis_rebuild_execution": True,
    }
    values.update(overrides)
    return RedisRebuildExecutionRequest(**values)


def _assert_global_open_gates(report: dict) -> None:
    assert (
        report["open_gate_semantics"]
        == "global_project_lifecycle_state_after_report_not_invocation_authority_or_attempts"
    )
    assert report["open_gates"] == {
        "PRODUCTION_ROLLOUT_OPEN": True,
        "PRODUCT_COMPLETE_CLOSED": False,
        "ACTUAL_REDIS_FLUSH_OPEN": False,
        "ACTUAL_REDIS_DELETE_OPEN": False,
        "ACTUAL_REDIS_CONSUME_ACK_OPEN": False,
        "ACTUAL_REDIS_PENDING_CLAIM_OPEN": False,
        "ACTUAL_DB_WRITE_OPEN": False,
        "ACTUAL_TELEGRAM_SEND_OPEN": False,
    }


def _assert_runtime_authority(
    report: dict,
    *,
    redis_xadd: bool,
    redis_xgroup_create: bool,
) -> None:
    runtime_authority = report["runtime_authority_opened_in_this_run"]
    assert runtime_authority["redis_xadd_authority_opened"] is redis_xadd
    assert runtime_authority["redis_xgroup_create_authority_opened"] is redis_xgroup_create
    assert runtime_authority["redis_flush_authority_opened"] is False
    assert runtime_authority["redis_delete_authority_opened"] is False
    assert runtime_authority["redis_consume_ack_authority_opened"] is False
    assert runtime_authority["redis_pending_claim_authority_opened"] is False
    assert runtime_authority["db_write_authority_opened"] is False
    assert runtime_authority["telegram_send_authority_opened"] is False
    assert runtime_authority["production_rollout_authority_opened"] is False


def _assert_final_closure_claims_false(report: dict) -> None:
    claims = report["completion_claims"]
    assert claims["REDIS_REBUILD_CLOSED"] is False
    assert claims["PRODUCT_COMPLETE_CLOSED"] is False
    assert claims["PRODUCTION_ROLLOUT_CLOSED"] is False
    assert claims["final_bot_complete"] is False
    assert claims["one_hundred_percent_complete"] is False
    assert claims["production_rollout_complete"] is False
    assert "ACTUAL_REDIS_REBUILD_EXECUTED_IN_THIS_RUN" not in claims


@pytest.mark.asyncio
async def test_plan_mode_is_read_only_and_consumes_o3a_readiness() -> None:
    redis = FakeRedis(stream_present=True, group_present=False)
    durable = FakeDurableReader([_row()])

    report = await build_redis_rebuild_execution_report(
        RedisRebuildExecutionRequest(mode="plan", queue_selector="maintenance"),
        config=config(),
        redis_client=redis,
        durable_reader=durable,
    )

    assert report["schema_version"] == "redis_rebuild_execution_report_v1"
    assert report["status"] == "pass"
    assert report["reason_code"] == "execution_plan_has_work"
    assert report["preflight_readiness_summary"]["schema_version"] == "redis_rebuild_readiness_report_v1"
    assert durable.inventory_calls == [10]
    assert durable.row_calls == [("q.maintenance", 25)]
    assert redis.xadd_calls == []
    assert redis.xgroup_create_calls == []
    assert report["authority"]["redis_mutation_attempted"] is False
    assert report["authority"]["db_write_attempted"] is False
    _assert_runtime_authority(report, redis_xadd=False, redis_xgroup_create=False)
    _assert_global_open_gates(report)
    assert report["completion_claims"]["REDIS_REBUILD_EXECUTION_PLAN_PASSED"] is True
    assert report["completion_claims"]["ACTUAL_REDIS_REBUILD_MUTATION_EXECUTED_IN_THIS_RUN"] is False
    _assert_final_closure_claims_false(report)


@pytest.mark.asyncio
async def test_execute_mode_blocks_without_each_explicit_approval_flag() -> None:
    for override, reason_code in (
        ({"understand_mutates_redis": False}, "redis_mutation_understanding_required"),
        ({"approve_redis_rebuild_execution": False}, "redis_rebuild_execution_approval_required"),
        ({"expected_head": None}, "expected_head_required"),
        ({"expected_head": "bad"}, "expected_head_mismatch"),
    ):
        redis = FakeRedis(stream_present=False, group_present=False)
        durable = FakeDurableReader([_row()])
        report = await build_redis_rebuild_execution_report(
            _execute_request(**override),
            config=config(),
            redis_client=redis,
            durable_reader=durable,
        )

        assert report["status"] == "blocked"
        assert report["reason_code"] == reason_code
        assert redis.xadd_calls == []
        assert report["authority"]["redis_mutation_attempted"] is False
        _assert_runtime_authority(report, redis_xadd=False, redis_xgroup_create=False)
        _assert_global_open_gates(report)
        _assert_final_closure_claims_false(report)


@pytest.mark.asyncio
async def test_execute_blocks_unknown_queue_and_invalid_max_rebuild_jobs() -> None:
    redis = FakeRedis(stream_present=False, group_present=False)
    durable = FakeDurableReader([_row()])

    unknown = await build_redis_rebuild_execution_report(
        _execute_request(queue_selector="q.unknown"),
        config=config(),
        redis_client=redis,
        durable_reader=durable,
    )
    assert unknown["status"] == "blocked"
    assert unknown["reason_code"] == "queue_not_known"
    _assert_runtime_authority(unknown, redis_xadd=False, redis_xgroup_create=False)

    for max_rebuild_jobs in (None, 0, -1, 26):
        report = await build_redis_rebuild_execution_report(
            _execute_request(max_rebuild_jobs=max_rebuild_jobs),
            config=config(),
            redis_client=FakeRedis(stream_present=False, group_present=False),
            durable_reader=FakeDurableReader([_row()]),
        )
        assert report["status"] == "blocked"
        assert report["reason_code"] in {"max_rebuild_jobs_required", "max_rebuild_jobs_not_allowed"}
        _assert_runtime_authority(report, redis_xadd=False, redis_xgroup_create=False)


@pytest.mark.asyncio
async def test_execute_xadds_only_exact_queue_and_creates_missing_group_with_post_write_readback() -> None:
    redis = FakeRedis(stream_present=False, group_present=False)
    durable = FakeDurableReader([_row()])

    report = await build_redis_rebuild_execution_report(
        _execute_request(),
        config=config(),
        redis_client=redis,
        durable_reader=durable,
    )
    output = render_sanitized_json(report)

    assert report["status"] == "pass"
    assert report["reason_code"] == "redis_rebuild_execution_pass"
    assert redis.xadd_calls == [("q.maintenance", redis.xadd_calls[0][1])]
    assert set(redis.xadd_calls[0][1]) == {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "pipeline_run_id",
        "not_before",
        "trigger_event_id",
    }
    assert redis.xadd_calls[0][1]["stage_name"] == "maintenance"
    assert redis.xgroup_create_calls == [("q.maintenance", "maintenance", False)]
    assert redis.forbidden_calls == []
    assert durable.commit_called is False
    assert durable.write_called is False
    assert report["post_write_readback"]["performed"] is True
    assert report["post_write_readback"]["stream_presence_bucket"] == "present"
    assert report["post_write_readback"]["inserted_count_bucket"] == "one"
    assert report["authority"]["redis_xadd_attempted"] is True
    assert report["authority"]["redis_xgroup_create_attempted"] is True
    assert report["authority"]["db_write_attempted"] is False
    _assert_runtime_authority(report, redis_xadd=True, redis_xgroup_create=True)
    _assert_global_open_gates(report)
    assert report["completion_claims"]["REDIS_REBUILD_EXECUTION_PROOF_PASSED"] is True
    assert report["completion_claims"]["ACTUAL_REDIS_REBUILD_MUTATION_EXECUTED_IN_THIS_RUN"] is True
    _assert_final_closure_claims_false(report)
    for raw in (
        RAW_RUNTIME_ENV_PATH,
        RAW_DB_URL,
        RAW_REDIS_URL,
        RAW_STREAM_ID,
        str(RAW_EVENT_ID),
        str(RAW_OBJECT_ID),
        RAW_DEDUPE_KEY,
        RAW_PAYLOAD,
        RAW_SOURCE_TEXT,
        RAW_URL,
        "sentinel-pass",
        "sentinel-token",
    ):
        assert raw not in output


@pytest.mark.asyncio
async def test_repeated_execute_is_idempotent_and_skips_duplicate_xadd() -> None:
    redis = FakeRedis(stream_present=False, group_present=False)
    durable = FakeDurableReader([_row()])

    first = await build_redis_rebuild_execution_report(
        _execute_request(),
        config=config(),
        redis_client=redis,
        durable_reader=durable,
    )
    second = await build_redis_rebuild_execution_report(
        _execute_request(),
        config=config(),
        redis_client=redis,
        durable_reader=durable,
    )

    assert first["status"] == "pass"
    assert second["status"] == "pass"
    assert second["reason_code"] == "idempotent_noop"
    assert len(redis.xadd_calls) == 1
    assert second["execution_summary"]["skipped_duplicate_count_bucket"] == "one"
    _assert_runtime_authority(second, redis_xadd=True, redis_xgroup_create=True)
    assert second["authority"]["redis_mutation_attempted"] is False
    assert second["authority"]["redis_xadd_attempted"] is False
    assert second["authority"]["redis_xgroup_create_attempted"] is False
    assert second["completion_claims"]["ACTUAL_REDIS_REBUILD_MUTATION_EXECUTED_IN_THIS_RUN"] is False
    _assert_global_open_gates(second)
    _assert_final_closure_claims_false(second)
    assert redis.xrange_calls[-1] == ("q.maintenance", 1)


@pytest.mark.asyncio
async def test_existing_large_stream_blocks_when_exact_duplicate_guard_is_not_safe() -> None:
    redis = FakeRedis(stream_present=True, group_present=True)
    for index in range(26):
        redis.streams["q.maintenance"].append((f"1711111111111-{index}", {"idempotency_key": f"key-{index}"}))
    durable = FakeDurableReader([_row()])

    report = await build_redis_rebuild_execution_report(
        _execute_request(),
        config=config(),
        redis_client=redis,
        durable_reader=durable,
    )

    assert report["status"] == "blocked"
    assert report["reason_code"] == "idempotency_guard_missing"
    assert len(redis.xadd_calls) == 0
    assert report["authority"]["redis_mutation_attempted"] is False


def test_static_ast_prevents_forbidden_redis_db_and_external_authority_calls() -> None:
    forbidden_redis_call_names = {
        "flushdb",
        "flushall",
        "delete",
        "unlink",
        "xack",
        "xdel",
        "xreadgroup",
        "xclaim",
        "xautoclaim",
        "xgroup_destroy",
        "xgroup_delconsumer",
        "publish",
        "subscribe",
        "eval",
    }
    forbidden_import_roots = {
        "telegram",
        "openai",
        "github",
        "requests",
        "httpx",
        "aiohttp",
        "urllib",
        "docker",
        "subprocess",
        "alembic",
    }
    forbidden_authority_calls = {
        "commit",
        "rollback",
        "systemctl",
        "run_systemd_rollout",
        "LocalUserSystemdAdapter",
        "send_message",
        "edit_message_text",
        "run_forever",
        "alembic",
    }

    for path in (SOURCE_PATH, TOOL_PATH):
        called = _called_names(path)
        imported = _import_roots(path)
        assert called.isdisjoint(forbidden_redis_call_names), path
        assert called.isdisjoint(forbidden_authority_calls), path
        assert imported.isdisjoint(forbidden_import_roots), path
    source_text = SOURCE_PATH.read_text(encoding="utf-8")
    assert "INSERT INTO" not in source_text
    assert "UPDATE " not in source_text


def _called_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                names.add(node.func.attr)
    return names


def _import_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots
