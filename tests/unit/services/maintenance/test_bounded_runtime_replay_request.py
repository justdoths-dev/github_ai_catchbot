from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from services.maintenance.bounded_runtime import (
    REPLAY_REQUEST_COMMAND,
    BoundedMaintenanceQueueOnceConfig,
    BoundedMaintenanceRuntimeConfig,
    RedisExactNextMaintenanceConsumer,
    run_bounded_maintenance_queue_once,
)
from services.maintenance.models import ReplayRequestRecord
from services.maintenance.service import MaintenanceService
from tests.component.services.maintenance._fakes import FakeRepository, config, outbox_event, plan


MESSAGE_ID = "1740000000000-77"


class FakeRedis:
    def __init__(self, *, entries, groups=None) -> None:
        self.entries = entries
        self.groups = groups
        self.xreadgroup_calls = []
        self.acked = []

    async def xinfo_groups(self, name):
        if self.groups is not None:
            return self.groups
        return [{"name": "maintenance-replay", "pending": 0, "lag": 1, "last-delivered-id": "0-0"}]

    async def xrange(self, name, min="-", max="+", count=None):
        del name, min, max
        return self.entries[: count or len(self.entries)]

    async def xpending_range(self, name, groupname, min, max, count):
        del name, groupname, min, max, count
        return []

    async def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        self.xreadgroup_calls.append(
            {"groupname": groupname, "consumername": consumername, "streams": streams, "count": count, "block": block}
        )
        return [("q.replay", self.entries[:1])]

    async def xack(self, name, groupname, *ids):
        self.acked.extend(ids)
        return 1

    async def aclose(self):
        return None


class FakeReplayRuntime:
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
            queue_name="q.replay",
            consumer_group="maintenance-replay",
            consumer_name="test",
            block_ms=1,
            state=state,
        )
        self.repository = repository
        self.state = state
        self.service = MaintenanceService(config(app_env="test"), repository=repository)
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
        raise AssertionError(f"unexpected maintenance invocation {trigger_event_id}")

    async def invoke_replay(self, trigger_event_id: UUID):
        self.order.append("invoke_replay")
        self.state.service_called = True
        self.state.database_write_attempted = True
        self.invoked_trigger_event_ids.append(trigger_event_id)
        if self.service_raises:
            raise RuntimeError("sentinel replay db failure with redacted locator")
        await self.service.handle_replay_trigger_event(trigger_event_id)

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


def _runtime_loader():
    return BoundedMaintenanceRuntimeConfig(maintenance_config=config())


def _queue_config(event_id: UUID, replay_request_id: UUID, *, mode: str = "preview") -> BoundedMaintenanceQueueOnceConfig:
    return BoundedMaintenanceQueueOnceConfig(
        command=REPLAY_REQUEST_COMMAND,
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=mode == "execute",
        allow_database_write=mode == "execute",
        allow_redis_read=True,
        allow_redis_consume=mode == "execute",
        allow_redis_ack=mode == "execute",
        mode=mode,
        trigger_event_suffix=str(event_id)[-8:],
        root_object_id_suffix=str(replay_request_id)[-8:],
        redis_message_id_suffix=MESSAGE_ID[-5:],
    )


def _replay_request(*, root_object_id: UUID, replay_type: str = "delivery", root_object_type: str = "notification_plan"):
    return ReplayRequestRecord(
        replay_request_id=uuid4(),
        replay_type=replay_type,
        root_object_type=root_object_type,
        root_object_id=root_object_id,
        status="requested",
        requested_by="operator",
    )


def _replay_event(request: ReplayRequestRecord):
    return outbox_event(
        "replay.requested.v1",
        aggregate_type="replay_request",
        aggregate_id=request.replay_request_id,
        payload_json={
            "replay_request_id": str(request.replay_request_id),
            "replay_type": request.replay_type,
            "root_object_type": request.root_object_type,
            "root_object_id": str(request.root_object_id),
            "replay_reason": "operator_requested",
        },
    )


def _message(event_id: UUID, replay_request_id: UUID, *, message_id: str = MESSAGE_ID, stage_name: str = "replay", root_type: str = "replay_request"):
    return (
        message_id,
        {
            "stage_name": stage_name,
            "root_object_type": root_type,
            "root_object_id": str(replay_request_id),
            "trigger_event_id": str(event_id),
            "replay_type": "do-not-trust-this",
            "root_object_id_from_payload": "do-not-trust-this",
        },
    )


def _fixture(*, replay_type: str = "delivery", root_object_type: str = "notification_plan"):
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    request = _replay_request(
        root_object_id=notification_plan.notification_plan_id,
        replay_type=replay_type,
        root_object_type=root_object_type,
    )
    repository.replay_requests[request.replay_request_id] = request
    event = _replay_event(request)
    repository.events[event.event_id] = event
    return repository, notification_plan, request, event


@pytest.mark.asyncio
async def test_preview_finds_exact_next_unconsumed_replay_request_and_writes_nothing() -> None:
    repository, _, request, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, request.replay_request_id)])

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeReplayRuntime(redis=redis, repository=repository, state=state)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, request.replay_request_id),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["database_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_consume_called"] is False
    assert repository.replay_status_updates == []
    assert redis.acked == []


@pytest.mark.asyncio
async def test_execute_dispatches_delivery_replay_from_trigger_event_and_acks_after_commit() -> None:
    repository, _, request, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, request.replay_request_id)])
    runtime_holder = {}

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        runtime = FakeReplayRuntime(redis=redis, repository=repository, state=state)
        runtime_holder["runtime"] = runtime
        return runtime

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, request.replay_request_id, mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is True
    runtime = runtime_holder["runtime"]
    assert runtime.invoked_trigger_event_ids == [event.event_id]
    assert runtime.order == ["invoke_replay", "commit", "ack"]
    assert repository.replay_status_updates[-1] == (request.replay_request_id, "completed")
    assert len(repository.plan_created_outbox) == 1
    assert redis.acked == [MESSAGE_ID]


@pytest.mark.asyncio
async def test_unsupported_replay_is_acked_only_after_durable_rejection_status() -> None:
    repository, _, request, event = _fixture(replay_type="source")
    redis = FakeRedis(entries=[_message(event.event_id, request.replay_request_id)])

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeReplayRuntime(redis=redis, repository=repository, state=state)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, request.replay_request_id, mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is True
    assert repository.replay_status_updates == [(request.replay_request_id, "unsupported_in_stage41")]
    assert repository.plan_created_outbox == []
    assert repository.job_attempts[0]["error_code"] == "unsupported_replay_type"
    assert redis.acked == [MESSAGE_ID]


@pytest.mark.asyncio
async def test_service_failure_leaves_replay_message_unacked() -> None:
    repository, _, request, event = _fixture()
    redis = FakeRedis(entries=[_message(event.event_id, request.replay_request_id)])

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeReplayRuntime(redis=redis, repository=repository, state=state, service_raises=True)

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, request.replay_request_id, mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    assert result.ok is False
    assert result.error_code == "service_execution_failed"
    assert repository.replay_status_updates == []
    assert redis.acked == []


@pytest.mark.asyncio
async def test_group_missing_and_wrong_stage_or_root_fail_closed() -> None:
    repository, _, request, event = _fixture()

    async def missing_group_builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeReplayRuntime(redis=FakeRedis(entries=[], groups=[]), repository=repository, state=state)

    missing = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, request.replay_request_id),
        runtime_config_loader=_runtime_loader,
        runtime_builder=missing_group_builder,
    )
    assert missing.error_code == "consumer_group_missing"

    for entry, expected_error in (
        (_message(event.event_id, request.replay_request_id, stage_name="maintenance"), "message_stage_mismatch"),
        (_message(event.event_id, request.replay_request_id, root_type="notification_plan"), "root_object_type_mismatch"),
    ):
        redis = FakeRedis(entries=[entry])

        async def builder(runtime_config, state, logger):
            del runtime_config, logger
            return FakeReplayRuntime(redis=redis, repository=repository, state=state)

        result = await run_bounded_maintenance_queue_once(
            _queue_config(event.event_id, request.replay_request_id),
            runtime_config_loader=_runtime_loader,
            runtime_builder=builder,
        )
        assert result.error_code == expected_error
        assert redis.acked == []


@pytest.mark.asyncio
async def test_wrong_configured_replay_queue_fails_before_runtime_consume() -> None:
    repository, _, request, event = _fixture()
    bad_config = replace(config(), replay_queue_name="q.not-replay")

    result = await run_bounded_maintenance_queue_once(
        _queue_config(event.event_id, request.replay_request_id),
        runtime_config_loader=lambda: BoundedMaintenanceRuntimeConfig(maintenance_config=bad_config),
        runtime_builder=None,
    )

    assert result.ok is False
    assert result.error_code == "queue_name_not_allowed"
