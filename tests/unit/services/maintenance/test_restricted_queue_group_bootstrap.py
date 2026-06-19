from __future__ import annotations

from dataclasses import asdict

import pytest

from services.maintenance.redis_streams import RedisStreamConsumer
from services.maintenance.restricted_queue_group_bootstrap import (
    RestrictedQueueGroupBootstrapRequest,
    run_restricted_queue_group_bootstrap,
)


class FakeResponseError(Exception):
    pass


class FakeRedisClient:
    def __init__(self, *, groups: set[tuple[str, str]] | None = None, create_raises: Exception | None = None) -> None:
        self.groups = set(groups or set())
        self.create_raises = create_raises
        self.xinfo_groups_calls: list[str] = []
        self.xgroup_create_calls: list[tuple[str, str, str, bool]] = []
        self.xreadgroup_calls: list[object] = []
        self.xack_calls: list[object] = []

    async def xinfo_groups(self, name: str):
        self.xinfo_groups_calls.append(name)
        matching_groups = [group for queue_name, group in self.groups if queue_name == name]
        if not matching_groups:
            raise FakeResponseError("no such key")
        return [{"name": group} for group in sorted(matching_groups)]

    async def xgroup_create(self, name: str, groupname: str, id: str = "$", mkstream: bool = False):
        self.xgroup_create_calls.append((name, groupname, id, mkstream))
        if self.create_raises is not None:
            raise self.create_raises
        if (name, groupname) in self.groups:
            raise FakeResponseError("BUSYGROUP Consumer Group name already exists")
        self.groups.add((name, groupname))
        return True

    async def xrange(self, name: str, min: str = "-", max: str = "+", count: int | None = None):
        return []

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ):
        self.xreadgroup_calls.append((groupname, consumername, streams, count, block))
        return []

    async def xack(self, name: str, groupname: str, *ids: str):
        self.xack_calls.append((name, groupname, ids))
        return 1


class FailingReadbackConsumer:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    async def ensure_group(self, *, allow_create: bool = True) -> bool:
        self.calls.append(allow_create)
        if self.calls == [False]:
            return False
        if self.calls == [False, True]:
            return True
        return False


class ExplodingConsumer:
    def __init__(self, message: str) -> None:
        self.message = message
        self.calls: list[bool] = []

    async def ensure_group(self, *, allow_create: bool = True) -> bool:
        self.calls.append(allow_create)
        raise RuntimeError(self.message)


def _request(
    selector: str = "maintenance",
    *,
    mode: str = "plan",
    confirm: bool = False,
    queue_name: str | None = None,
    consumer_group: str | None = None,
) -> RestrictedQueueGroupBootstrapRequest:
    if selector == "maintenance":
        default_queue = "q.maintenance"
        default_group = "maintenance"
    else:
        default_queue = "q.replay"
        default_group = "maintenance-replay"
    return RestrictedQueueGroupBootstrapRequest(
        queue_selector=selector,
        queue_name=queue_name or default_queue,
        consumer_group=consumer_group or default_group,
        consumer_name=f"{selector}-operator",
        mode=mode,
        confirm_create_group=confirm,
    )


def _consumer(client: FakeRedisClient, *, queue_name: str = "q.maintenance", group: str = "maintenance"):
    return RedisStreamConsumer(
        client,
        queue_name=queue_name,
        consumer_group=group,
        consumer_name="operator",
        block_ms=1,
        batch_size=1,
    )


@pytest.mark.asyncio
async def test_plan_reports_missing_group_without_creating() -> None:
    client = FakeRedisClient()
    report = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode="plan"),
        consumer=_consumer(client),
    )

    assert report.status == "pass"
    assert report.reason_code == "consumer_group_missing"
    assert report.group_exists is False
    assert report.created is False
    assert client.xinfo_groups_calls == ["q.maintenance"]
    assert client.xgroup_create_calls == []
    assert client.xreadgroup_calls == []
    assert client.xack_calls == []


@pytest.mark.asyncio
async def test_proof_blocks_when_group_missing() -> None:
    client = FakeRedisClient()
    report = await run_restricted_queue_group_bootstrap(
        _request("replay", mode="proof"),
        consumer=_consumer(client, queue_name="q.replay", group="maintenance-replay"),
    )

    assert report.status == "blocked"
    assert report.reason_code == "consumer_group_missing"
    assert report.group_exists is False
    assert client.xgroup_create_calls == []
    assert client.xreadgroup_calls == []
    assert client.xack_calls == []


@pytest.mark.asyncio
async def test_execute_without_confirm_is_blocked_before_redis() -> None:
    consumer = FailingReadbackConsumer()
    report = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode="execute", confirm=False),
        consumer=consumer,
    )

    assert report.status == "blocked"
    assert report.reason_code == "create_group_confirm_missing"
    assert report.xgroup_create_attempted is False
    assert consumer.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["plan", "proof"])
async def test_read_only_modes_reject_confirm_without_creating(mode: str) -> None:
    consumer = FailingReadbackConsumer()
    report = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode=mode, confirm=True),
        consumer=consumer,
    )

    assert report.status == "blocked"
    assert report.reason_code == "create_group_confirm_not_allowed_for_read_only"
    assert report.xgroup_create_attempted is False
    assert consumer.calls == []


@pytest.mark.asyncio
async def test_execute_creates_maintenance_group_exactly_once_with_mkstream() -> None:
    client = FakeRedisClient()
    report = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode="execute", confirm=True),
        consumer=_consumer(client),
    )

    assert report.status == "pass"
    assert report.created is True
    assert report.already_exists is False
    assert report.group_exists is True
    assert report.xgroup_create_attempted is True
    assert client.xgroup_create_calls == [("q.maintenance", "maintenance", "0", True)]
    assert client.xinfo_groups_calls == ["q.maintenance", "q.maintenance"]
    assert client.xreadgroup_calls == []
    assert client.xack_calls == []


@pytest.mark.asyncio
async def test_execute_creates_replay_group_exactly_once_with_mkstream() -> None:
    client = FakeRedisClient()
    report = await run_restricted_queue_group_bootstrap(
        _request("replay", mode="execute", confirm=True),
        consumer=_consumer(client, queue_name="q.replay", group="maintenance-replay"),
    )

    assert report.status == "pass"
    assert report.created is True
    assert client.xgroup_create_calls == [("q.replay", "maintenance-replay", "0", True)]
    assert client.xreadgroup_calls == []
    assert client.xack_calls == []


@pytest.mark.asyncio
async def test_execute_is_idempotent_when_group_exists() -> None:
    client = FakeRedisClient(groups={("q.maintenance", "maintenance")})
    report = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode="execute", confirm=True),
        consumer=_consumer(client),
    )

    assert report.status == "pass"
    assert report.created is False
    assert report.already_exists is True
    assert report.group_exists is True
    assert report.xgroup_create_attempted is False
    assert client.xgroup_create_calls == []
    assert client.xreadgroup_calls == []
    assert client.xack_calls == []


@pytest.mark.asyncio
async def test_execute_blocks_if_create_readback_still_missing() -> None:
    report = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode="execute", confirm=True),
        consumer=FailingReadbackConsumer(),
    )

    assert report.status == "blocked"
    assert report.reason_code == "consumer_group_readback_missing"
    assert report.created is False
    assert report.xgroup_create_attempted is True


@pytest.mark.asyncio
async def test_exact_queue_and_group_are_required() -> None:
    client = FakeRedisClient()
    bad_queue = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode="execute", confirm=True, queue_name="q.anything"),
        consumer=_consumer(client),
    )
    bad_group = await run_restricted_queue_group_bootstrap(
        _request("replay", mode="execute", confirm=True, consumer_group="other-group"),
        consumer=_consumer(client, queue_name="q.replay", group="maintenance-replay"),
    )
    bad_selector = await run_restricted_queue_group_bootstrap(
        _request("arbitrary", mode="execute", confirm=True),
        consumer=_consumer(client),
    )

    assert bad_queue.reason_code == "queue_name_not_allowed"
    assert bad_group.reason_code == "consumer_group_not_allowed"
    assert bad_selector.reason_code == "queue_selector_not_allowed"
    assert client.xgroup_create_calls == []
    assert client.xreadgroup_calls == []
    assert client.xack_calls == []


@pytest.mark.asyncio
async def test_reports_redact_exception_bodies_and_runtime_values() -> None:
    sensitive = "redis://secret DATABASE_URL payload_json private exception body"
    report = await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode="proof"),
        consumer=ExplodingConsumer(sensitive),
    )

    rendered = repr(asdict(report))
    assert report.status == "blocked"
    assert report.reason_code == "group_metadata_read_failed"
    assert sensitive not in rendered
    assert "redis://secret" not in rendered
    assert "DATABASE_URL" not in rendered
    assert "payload_json private exception body" not in rendered
    assert report.redactions_applied["redis_url_omitted"] is True
    assert report.redactions_applied["database_url_omitted"] is True
    assert report.redactions_applied["payload_json_omitted"] is True
    assert report.redactions_applied["exception_body_omitted"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,confirm", [("plan", False), ("proof", False), ("execute", True)])
async def test_bootstrap_never_reads_or_acks_messages(mode: str, confirm: bool) -> None:
    client = FakeRedisClient()
    await run_restricted_queue_group_bootstrap(
        _request("maintenance", mode=mode, confirm=confirm),
        consumer=_consumer(client),
    )

    assert client.xreadgroup_calls == []
    assert client.xack_calls == []
