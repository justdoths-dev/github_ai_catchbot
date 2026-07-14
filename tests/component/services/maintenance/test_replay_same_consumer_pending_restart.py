from __future__ import annotations

from dataclasses import dataclass, field, replace
from uuid import UUID, uuid4

import pytest

from services.maintenance.models import DeliveryReplayDecision, ReplayRequestRecord, WorkerBatchResult
from services.maintenance.redis_streams import RedisStreamConsumer
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import ReplayQueueWorker
from tests.component.services.maintenance._fakes import FakeRepository, config, outbox_event, plan


@dataclass
class SharedRedisStreamState:
    new_entries: list[tuple[bytes, dict[bytes, bytes]]]
    pending: dict[tuple[str, str], list[tuple[bytes, dict[bytes, bytes]]]] = field(default_factory=dict)
    ack_attempts: list[tuple[str, str, tuple[str, ...]]] = field(default_factory=list)
    acked: list[str] = field(default_factory=list)


class ProcessRedisClient:
    def __init__(self, state: SharedRedisStreamState, *, fail_next_ack: bool = False) -> None:
        self.state = state
        self.fail_next_ack = fail_next_ack
        self.read_calls: list[tuple[str, str, str, str, int | None, int | None]] = []

    async def xreadgroup(self, groupname, consumername, streams, *, count=None, block=None):
        queue_name, stream_id = next(iter(streams.items()))
        self.read_calls.append((queue_name, groupname, consumername, stream_id, count, block))
        pending_key = (groupname, consumername)
        if stream_id == "0":
            entries = self.state.pending.get(pending_key, [])[:count]
        else:
            entries = self.state.new_entries[:count]
            del self.state.new_entries[: len(entries)]
            self.state.pending.setdefault(pending_key, []).extend(entries)
        return [] if not entries else [(queue_name.encode(), entries)]

    async def xack(self, name, groupname, *ids):
        self.state.ack_attempts.append((name, groupname, ids))
        if self.fail_next_ack:
            self.fail_next_ack = False
            raise RuntimeError("redacted uncertain ack response")
        for pending_key, entries in self.state.pending.items():
            if pending_key[0] != groupname:
                continue
            self.state.pending[pending_key] = [
                entry for entry in entries if entry[0].decode() not in ids
            ]
        self.state.acked.extend(ids)
        return len(ids)


class RecordingReplayService:
    def __init__(self, inner: MaintenanceService) -> None:
        self.inner = inner
        self.decisions: list[DeliveryReplayDecision | None] = []

    async def handle_replay_trigger_event(self, trigger_event_id: str | UUID):
        decision = await self.inner.handle_replay_trigger_event(trigger_event_id)
        self.decisions.append(decision)
        return decision


def _consumer(client: ProcessRedisClient, *, queue_config) -> RedisStreamConsumer:
    return RedisStreamConsumer(
        client,
        queue_name=queue_config.replay_queue_name,
        consumer_group=queue_config.replay_consumer_group,
        consumer_name=queue_config.replay_consumer_name,
        block_ms=queue_config.block_ms,
        batch_size=queue_config.batch_size,
    )


@pytest.mark.asyncio
async def test_restart_recovers_same_consumer_pending_after_commit_and_exact_ack_failure() -> None:
    queue_config = replace(config(), batch_size=1)
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    replay_request = ReplayRequestRecord(
        replay_request_id=uuid4(),
        replay_type="delivery",
        root_object_type="notification_plan",
        root_object_id=notification_plan.notification_plan_id,
        status="requested",
        requested_by="operator",
    )
    replay_event = outbox_event(
        "replay.requested.v1",
        aggregate_type="replay_request",
        aggregate_id=replay_request.replay_request_id,
        payload_json={
            "replay_request_id": str(replay_request.replay_request_id),
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": str(notification_plan.notification_plan_id),
            "replay_reason": "operator_requested",
        },
    )
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.replay_requests[replay_request.replay_request_id] = replay_request
    repository.events[replay_event.event_id] = replay_event
    exact_entry = (
        b"1-0",
        {b"trigger_event_id": str(replay_event.event_id).encode()},
    )
    unrelated_entry = (
        b"2-0",
        {b"trigger_event_id": str(uuid4()).encode()},
    )
    state = SharedRedisStreamState(new_entries=[exact_entry, unrelated_entry])

    first_client = ProcessRedisClient(state, fail_next_ack=True)
    first_service = RecordingReplayService(MaintenanceService(queue_config, repository=repository))
    first_worker = ReplayQueueWorker(
        queue_config,
        consumer=_consumer(first_client, queue_config=queue_config),
        service=first_service,
    )

    first_result = await first_worker.run_once()

    assert first_result == WorkerBatchResult(processed=1, acked=0)
    assert [decision.action for decision in first_service.decisions if decision is not None] == [
        "emit_replay_intent"
    ]
    assert repository.replay_requests[replay_request.replay_request_id].status == "completed"
    assert repository.plan_created_outbox_insert_calls == 1
    assert len(repository.plan_created_outbox) == 1
    assert len(repository.replay_status_updates) == 2
    assert len(repository.job_attempts) == 1
    pending_key = (queue_config.replay_consumer_group, queue_config.replay_consumer_name)
    assert state.pending[pending_key] == [exact_entry]
    assert state.new_entries == [unrelated_entry]
    assert [call[3] for call in first_client.read_calls] == ["0", ">"]

    second_client = ProcessRedisClient(state)
    second_service = RecordingReplayService(MaintenanceService(queue_config, repository=repository))
    second_worker = ReplayQueueWorker(
        queue_config,
        consumer=_consumer(second_client, queue_config=queue_config),
        service=second_service,
    )

    second_result = await second_worker.run_once()

    assert second_result == WorkerBatchResult(processed=1, acked=1)
    assert second_service.decisions == [
        DeliveryReplayDecision(
            action="already_completed_noop",
            reason_code="replay_request_already_completed_noop",
        )
    ]
    assert [call[3] for call in second_client.read_calls] == ["0"]
    assert all(
        call[0:3]
        == (
            queue_config.replay_queue_name,
            queue_config.replay_consumer_group,
            queue_config.replay_consumer_name,
        )
        for call in first_client.read_calls + second_client.read_calls
    )
    assert repository.plan_created_outbox_insert_calls == 1
    assert len(repository.plan_created_outbox) == 1
    assert len(repository.replay_status_updates) == 2
    assert len(repository.job_attempts) == 1
    assert state.pending[pending_key] == []
    assert state.new_entries == [unrelated_entry]
    assert state.ack_attempts == [
        (queue_config.replay_queue_name, queue_config.replay_consumer_group, ("1-0",)),
        (queue_config.replay_queue_name, queue_config.replay_consumer_group, ("1-0",)),
    ]
    assert state.acked == ["1-0"]
