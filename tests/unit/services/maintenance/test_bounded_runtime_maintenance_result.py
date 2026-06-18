from __future__ import annotations

from uuid import UUID

import pytest

from services.maintenance.bounded_runtime import (
    MAINTENANCE_RESULT_COMMAND,
    BoundedMaintenanceQueueOnceConfig,
    BoundedMaintenanceRuntimeConfig,
    RedisExactNextMaintenanceConsumer,
    run_bounded_maintenance_queue_once,
)
from services.maintenance.models import DeliveryResultWorkerResult
from tests.component.services.maintenance._fakes import FakeRepository, config, latest_delivery_record, outbox_event, plan


MESSAGE_ID = "1740000000000-42"


class FakeRedis:
    def __init__(
        self,
        *,
        entries,
        pending: int = 0,
        lag: int = 1,
        groups=None,
        pending_entries=None,
        ack_raises: bool = False,
    ) -> None:
        self.entries = entries
        self.groups = groups
        self.pending = pending
        self.lag = lag
        self.pending_entries = pending_entries or []
        self.ack_raises = ack_raises
        self.xreadgroup_calls = []
        self.acked = []

    async def xinfo_groups(self, name):
        if self.groups is not None:
            return self.groups
        return [
            {
                "name": "maintenance",
                "pending": self.pending,
                "lag": self.lag,
                "last-delivered-id": "0-0",
            }
        ]

    async def xrange(self, name, min="-", max="+", count=None):
        del name, min, max
        return self.entries[: count or len(self.entries)]

    async def xpending_range(self, name, groupname, min, max, count):
        del name, groupname, min, max, count
        return self.pending_entries

    async def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        self.xreadgroup_calls.append(
            {"groupname": groupname, "consumername": consumername, "streams": streams, "count": count, "block": block}
        )
        return [("q.maintenance", self.entries[:1])]

    async def xack(self, name, groupname, *ids):
        if self.ack_raises:
            raise RuntimeError("sentinel redis ack failure with redacted locator")
        self.acked.extend(ids)
        return 1

    async def aclose(self):
        return None


class FakeQueueRuntime:
    def __init__(
        self,
        *,
        redis: FakeRedis,
        repository: FakeRepository,
        state,
        service_raises: bool = False,
    ) -> None:
        self.consumer = RedisExactNextMaintenanceConsumer(
            redis,
            queue_name="q.maintenance",
            consumer_group="maintenance",
            consumer_name="test",
            block_ms=1,
            state=state,
        )
        self.repository = repository
        self.state = state
        self.service_raises = service_raises
        self.invoked_trigger_event_ids = []
        self.order = []

    async def inspect_target(self, config):
        return await self.consumer.inspect_target(config)

    async def consume_target(self, expected, config):
        return await self.consumer.consume_target(expected, config)

    async def load_outbox_event(self, trigger_event_id: UUID):
        self.state.database_read_attempted = True
        return self.repository.events.get(trigger_event_id)

    async def invoke_maintenance(self, trigger_event_id: UUID):
        self.order.append("invoke_maintenance")
        self.state.service_called = True
        self.state.database_write_attempted = True
        self.invoked_trigger_event_ids.append(trigger_event_id)
        if self.service_raises:
            raise RuntimeError("sentinel database failure with redacted locator")
        return DeliveryResultWorkerResult(
            processed=True,
            classification="retryable_candidate",
            action="record_retryable_interpretation",
            reason_code="failed_retryable_deferred_to_due_scan",
        )

    async def invoke_replay(self, trigger_event_id: UUID):
        raise AssertionError(f"unexpected replay invocation {trigger_event_id}")

    async def commit_database(self):
        self.order.append("commit")
        self.state.database_committed = True

    async def rollback_database(self):
        self.order.append("rollback")
        self.state.database_rolled_back = True

    async def ack(self, message_id: str):
        self.order.append("ack")
        return await self.consumer.ack(message_id)

    async def close(self):
        return None


class FakeRuntimeBuilder:
    def __init__(self, runtime: FakeQueueRuntime) -> None:
        self.runtime = runtime

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger
        return self.runtime


def _runtime_loader():
    return BoundedMaintenanceRuntimeConfig(maintenance_config=config())


def _queue_config(event_id: UUID, plan_id: UUID, *, mode: str = "preview") -> BoundedMaintenanceQueueOnceConfig:
    return BoundedMaintenanceQueueOnceConfig(
        command=MAINTENANCE_RESULT_COMMAND,
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=mode == "execute",
        allow_database_write=mode == "execute",
        allow_redis_read=True,
        allow_redis_consume=mode == "execute",
        allow_redis_ack=mode == "execute",
        mode=mode,
        trigger_event_suffix=str(event_id)[-8:],
        root_object_id_suffix=str(plan_id)[-8:],
        redis_message_id_suffix=MESSAGE_ID[-5:],
    )


def _message(event_id: UUID, plan_id: UUID, *, message_id: str = MESSAGE_ID):
    return (
        message_id,
        {
            "stage_name": "maintenance",
            "root_object_type": "notification_plan",
            "root_object_id": str(plan_id),
            "trigger_event_id": str(event_id),
            "delivery_status": "do-not-trust-this",
            "notification_plan_id": "do-not-trust-this",
        },
    )


def _fixture():
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(notification_plan_id=notification_plan.notification_plan_id)
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "delivery_status": "failed_retryable",
            "attempt_count": 1,
        },
    )
    repository.events[event.event_id] = event
    return repository, notification_plan, event


@pytest.mark.asyncio
async def test_preview_finds_exact_next_unconsumed_target_and_writes_nothing() -> None:
    repository, notification_plan, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, notification_plan.notification_plan_id)])
    state_runtime = {}

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        runtime = FakeQueueRuntime(redis=redis, repository=repository, state=state)
        state_runtime["runtime"] = runtime
        return runtime

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, notification_plan.notification_plan_id),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is True
    report = result.to_sanitized_dict()
    assert report["status"] == "pass"
    assert report["redis_read_attempted"] is True
    assert report["redis_consume_called"] is False
    assert report["redis_ack_attempted"] is False
    assert report["database_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert state_runtime["runtime"].invoked_trigger_event_ids == []
    assert redis.acked == []


@pytest.mark.asyncio
async def test_preview_fails_when_group_missing() -> None:
    repository, notification_plan, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, notification_plan.notification_plan_id)], groups=[])

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeQueueRuntime(redis=redis, repository=repository, state=state)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, notification_plan.notification_plan_id),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is False
    assert result.error_code == "consumer_group_missing"
    assert redis.acked == []


@pytest.mark.asyncio
async def test_preview_fails_when_target_is_not_next_unconsumed() -> None:
    repository, notification_plan, event = _fixture()
    other_plan = plan()
    other_event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=other_plan.notification_plan_id,
        payload_json={"notification_plan_id": str(other_plan.notification_plan_id)},
    )
    redis = FakeRedis(
        entries=[
            _message(other_event.event_id, other_plan.notification_plan_id, message_id="1740000000000-41"),
            _message(event.event_id, notification_plan.notification_plan_id),
        ],
        lag=2,
    )

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeQueueRuntime(redis=redis, repository=repository, state=state)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, notification_plan.notification_plan_id),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is False
    assert result.error_code == "target_not_next_unconsumed"
    assert redis.xreadgroup_calls == []
    assert redis.acked == []


@pytest.mark.asyncio
async def test_execute_consumes_one_invokes_service_by_trigger_event_and_acks_after_commit() -> None:
    repository, notification_plan, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, notification_plan.notification_plan_id)])
    runtime_holder = {}

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        runtime = FakeQueueRuntime(redis=redis, repository=repository, state=state)
        runtime_holder["runtime"] = runtime
        return runtime

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, notification_plan.notification_plan_id, mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is True
    runtime = runtime_holder["runtime"]
    assert redis.xreadgroup_calls == [
        {
            "groupname": "maintenance",
            "consumername": "test",
            "streams": {"q.maintenance": ">"},
            "count": 1,
            "block": 1,
        }
    ]
    assert runtime.invoked_trigger_event_ids == [event.event_id]
    assert runtime.order == ["invoke_maintenance", "commit", "ack"]
    assert redis.acked == [MESSAGE_ID]


@pytest.mark.asyncio
async def test_service_failure_leaves_redis_message_unacked() -> None:
    repository, notification_plan, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, notification_plan.notification_plan_id)])

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeQueueRuntime(redis=redis, repository=repository, state=state, service_raises=True)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, notification_plan.notification_plan_id, mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is False
    assert result.error_code == "service_execution_failed"
    assert redis.acked == []
    assert result.to_sanitized_dict()["database_rolled_back"] is True


@pytest.mark.asyncio
async def test_ack_failure_after_durable_completion_is_reported_without_retrying() -> None:
    repository, notification_plan, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, notification_plan.notification_plan_id)], ack_raises=True)

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeQueueRuntime(redis=redis, repository=repository, state=state)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, notification_plan.notification_plan_id, mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    report = result.to_sanitized_dict()
    assert result.ok is False
    assert result.error_code == "ack_failed_after_durable_completion"
    assert report["database_committed"] is True
    assert report["ack_attempted"] is True
    assert report["acked"] is False
    assert "redacted locator" not in str(report)


@pytest.mark.asyncio
async def test_pending_target_under_another_consumer_fails_without_claim_takeover() -> None:
    repository, notification_plan, event = _fixture()
    redis = FakeRedis(
        entries=[_message(event.event_id, notification_plan.notification_plan_id)],
        pending=1,
        pending_entries=[{"message_id": MESSAGE_ID, "consumer": "other-maintenance"}],
    )

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeQueueRuntime(redis=redis, repository=repository, state=state)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, notification_plan.notification_plan_id),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is False
    assert result.error_code == "target_pending_under_another_consumer"
    assert redis.xreadgroup_calls == []
    assert redis.acked == []
