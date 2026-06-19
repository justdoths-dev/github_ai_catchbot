from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.maintenance.foreground_smoke import (
    ForegroundSmokeRequest,
    foreground_smoke_request_error,
    run_foreground_smoke,
)
from services.maintenance.models import (
    DeliveryReplayDecision,
    DeliveryResultWorkerResult,
    StreamMessage,
    WorkerBatchResult,
)
from services.maintenance.worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker
from tests.component.services.maintenance._fakes import config


class FakeForegroundSmokeConsumer:
    def __init__(self, messages: list[StreamMessage], *, group_exists: bool = True) -> None:
        self.messages = messages
        self.group_exists = group_exists
        self.ensure_group_allow_create: list[bool] = []
        self.create_group_attempted = False
        self.read_calls = 0
        self.acked: list[str] = []

    async def ensure_group(self, *, allow_create: bool = True) -> bool:
        self.ensure_group_allow_create.append(allow_create)
        if allow_create:
            self.create_group_attempted = True
        return self.group_exists

    async def read_batch(self) -> list[StreamMessage]:
        self.read_calls += 1
        messages = self.messages
        self.messages = []
        return messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class FakeForegroundSmokeService:
    def __init__(self, *, due_action_count: int | None = None) -> None:
        self.due_action_count = due_action_count
        self.maintenance_calls: list[UUID] = []
        self.replay_calls: list[UUID] = []
        self.due_limits: list[int | None] = []

    async def handle_maintenance_trigger_event(self, trigger_event_id: str | UUID):
        self.maintenance_calls.append(UUID(str(trigger_event_id)))
        return DeliveryResultWorkerResult(
            processed=True,
            classification="terminal_success",
            action="mark_terminal_success",
            reason_code="delivery_result_terminal_success",
        )

    async def handle_replay_trigger_event(self, trigger_event_id: str | UUID):
        self.replay_calls.append(UUID(str(trigger_event_id)))
        return DeliveryReplayDecision(action="emit_replay_intent", reason_code="explicit_delivery_replay")

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        self.due_limits.append(limit)
        return self.due_action_count if self.due_action_count is not None else limit or 0


def _request(*, ticks: int = 1, max_messages: int = 1, confirm_run: bool = True) -> ForegroundSmokeRequest:
    return ForegroundSmokeRequest(
        mode="execute",
        ticks=ticks,
        max_messages=max_messages,
        confirm_run=confirm_run,
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
    )


def _message(queue_name: str, event_id: UUID, *, message_id: str) -> StreamMessage:
    return StreamMessage(stream=queue_name, message_id=message_id, fields={"trigger_event_id": str(event_id)})


def test_foreground_smoke_request_rejects_execute_without_confirm_run() -> None:
    assert foreground_smoke_request_error(_request(confirm_run=False)) == "run_confirm_missing"


@pytest.mark.parametrize("ticks", [0, 6])
def test_foreground_smoke_request_rejects_ticks_outside_bounds(ticks: int) -> None:
    assert foreground_smoke_request_error(_request(ticks=ticks)) == "ticks_not_allowed"


@pytest.mark.parametrize("max_messages", [0, 11])
def test_foreground_smoke_request_rejects_max_messages_outside_bounds(max_messages: int) -> None:
    assert foreground_smoke_request_error(_request(max_messages=max_messages)) == "max_messages_not_allowed"


@pytest.mark.asyncio
async def test_foreground_smoke_calls_each_bounded_worker_path_once_when_ticks_one(monkeypatch) -> None:
    calls: list[str] = []

    async def maintenance_run_once(self):
        calls.append("maintenance")
        return WorkerBatchResult(processed=1, acked=1)

    async def replay_run_once(self):
        calls.append("replay")
        return WorkerBatchResult(processed=1, acked=1)

    async def due_retry_run_once(self):
        calls.append("due-retry")
        return WorkerBatchResult(processed=1, acked=0)

    monkeypatch.setattr(MaintenanceQueueWorker, "run_once", maintenance_run_once)
    monkeypatch.setattr(ReplayQueueWorker, "run_once", replay_run_once)
    monkeypatch.setattr(DueRetryPromotionWorker, "run_once", due_retry_run_once)

    report = await run_foreground_smoke(
        _request(),
        config=config(),
        maintenance_consumer=FakeForegroundSmokeConsumer([]),
        replay_consumer=FakeForegroundSmokeConsumer([]),
        service=FakeForegroundSmokeService(),
    )

    assert calls == ["maintenance", "replay", "due-retry"]
    assert report.status == "pass"
    assert report.ticks_completed == 1
    assert report.maintenance_processed_count == 1
    assert report.maintenance_acked_count == 1
    assert report.replay_processed_count == 1
    assert report.replay_acked_count == 1
    assert report.due_retry_action_count == 1


@pytest.mark.asyncio
async def test_foreground_smoke_aggregates_processed_acked_and_due_retry_action_counts() -> None:
    maintenance_events = [uuid4(), uuid4()]
    replay_event = uuid4()
    maintenance_consumer = FakeForegroundSmokeConsumer(
        [
            _message("q.maintenance", maintenance_events[0], message_id="10-0"),
            _message("q.maintenance", maintenance_events[1], message_id="11-0"),
        ]
    )
    replay_consumer = FakeForegroundSmokeConsumer([_message("q.replay", replay_event, message_id="20-0")])
    service = FakeForegroundSmokeService()

    report = await run_foreground_smoke(
        _request(max_messages=2),
        config=config(),
        maintenance_consumer=maintenance_consumer,
        replay_consumer=replay_consumer,
        service=service,
    )

    assert report.status == "pass"
    assert report.ticks_requested == 1
    assert report.ticks_completed == 1
    assert report.maintenance_processed_count == 2
    assert report.maintenance_acked_count == 2
    assert report.replay_processed_count == 1
    assert report.replay_acked_count == 1
    assert report.due_retry_action_count == 2
    assert maintenance_consumer.acked == ["10-0", "11-0"]
    assert replay_consumer.acked == ["20-0"]
    assert service.maintenance_calls == maintenance_events
    assert service.replay_calls == [replay_event]
    assert service.due_limits == [2]


@pytest.mark.asyncio
async def test_foreground_smoke_checks_groups_without_create() -> None:
    maintenance_consumer = FakeForegroundSmokeConsumer([])
    replay_consumer = FakeForegroundSmokeConsumer([])

    report = await run_foreground_smoke(
        _request(),
        config=config(),
        maintenance_consumer=maintenance_consumer,
        replay_consumer=replay_consumer,
        service=FakeForegroundSmokeService(due_action_count=0),
    )

    assert report.status == "pass"
    assert maintenance_consumer.ensure_group_allow_create == [False]
    assert replay_consumer.ensure_group_allow_create == [False]
    assert maintenance_consumer.create_group_attempted is False
    assert replay_consumer.create_group_attempted is False


@pytest.mark.asyncio
async def test_foreground_smoke_blocks_missing_group_before_reads() -> None:
    maintenance_consumer = FakeForegroundSmokeConsumer([], group_exists=False)
    replay_consumer = FakeForegroundSmokeConsumer([])

    report = await run_foreground_smoke(
        _request(),
        config=config(),
        maintenance_consumer=maintenance_consumer,
        replay_consumer=replay_consumer,
        service=FakeForegroundSmokeService(due_action_count=0),
    )

    assert report.status == "blocked"
    assert report.reason_code == "maintenance_consumer_group_missing"
    assert maintenance_consumer.create_group_attempted is False
    assert replay_consumer.create_group_attempted is False
    assert maintenance_consumer.read_calls == 0
    assert replay_consumer.read_calls == 0


@pytest.mark.asyncio
async def test_foreground_smoke_never_calls_run_forever(monkeypatch) -> None:
    async def run_forever_called(self):
        raise AssertionError("run_forever_called")

    monkeypatch.setattr(MaintenanceQueueWorker, "run_forever", run_forever_called)
    monkeypatch.setattr(ReplayQueueWorker, "run_forever", run_forever_called)
    monkeypatch.setattr(DueRetryPromotionWorker, "run_forever", run_forever_called)

    report = await run_foreground_smoke(
        _request(),
        config=config(),
        maintenance_consumer=FakeForegroundSmokeConsumer([]),
        replay_consumer=FakeForegroundSmokeConsumer([]),
        service=FakeForegroundSmokeService(due_action_count=0),
    )

    assert report.status == "pass"
