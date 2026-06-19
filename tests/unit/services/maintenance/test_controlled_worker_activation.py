from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.maintenance.controlled_worker_activation import (
    ControlledWorkerActivationRequest,
    controlled_worker_activation_request_error,
    run_controlled_worker_activation,
)
from services.maintenance.models import (
    DeliveryReplayDecision,
    DeliveryResultWorkerResult,
    StreamMessage,
    WorkerBatchResult,
)
from services.maintenance.worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker
from tests.component.services.maintenance._fakes import config


class FakeControlledConsumer:
    def __init__(self, messages: list[StreamMessage], *, group_exists: bool = True) -> None:
        self.messages = messages
        self.group_exists = group_exists
        self.ensure_group_allow_create: list[bool] = []
        self.create_group_attempted = False
        self.read_calls = 0
        self.acked: list[str] = []

    async def ensure_group(self, *, allow_create: bool) -> bool:
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


class FakeControlledService:
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


class MutableClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    async def sleep(self, delay: float) -> None:
        self.value += delay


def _request(
    *,
    max_ticks: int = 3,
    max_runtime_sec: int = 30,
    max_messages: int = 1,
    idle_sleep_ms: int = 100,
    confirm_run: bool = True,
) -> ControlledWorkerActivationRequest:
    return ControlledWorkerActivationRequest(
        mode="execute",
        max_ticks=max_ticks,
        max_runtime_sec=max_runtime_sec,
        max_messages=max_messages,
        idle_sleep_ms=idle_sleep_ms,
        confirm_run=confirm_run,
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
    )


def _message(queue_name: str, event_id: UUID, *, message_id: str) -> StreamMessage:
    return StreamMessage(stream=queue_name, message_id=message_id, fields={"trigger_event_id": str(event_id)})


def test_controlled_worker_request_rejects_execute_without_confirm_run() -> None:
    assert controlled_worker_activation_request_error(_request(confirm_run=False)) == "run_confirm_missing"


@pytest.mark.parametrize("max_ticks", [0, 21])
def test_controlled_worker_request_rejects_max_ticks_outside_bounds(max_ticks: int) -> None:
    assert controlled_worker_activation_request_error(_request(max_ticks=max_ticks)) == "max_ticks_not_allowed"


@pytest.mark.parametrize("max_runtime_sec", [0, 301])
def test_controlled_worker_request_rejects_max_runtime_sec_outside_bounds(max_runtime_sec: int) -> None:
    assert (
        controlled_worker_activation_request_error(_request(max_runtime_sec=max_runtime_sec))
        == "max_runtime_sec_not_allowed"
    )


@pytest.mark.parametrize("max_messages", [0, 11])
def test_controlled_worker_request_rejects_max_messages_outside_bounds(max_messages: int) -> None:
    assert controlled_worker_activation_request_error(_request(max_messages=max_messages)) == "max_messages_not_allowed"


@pytest.mark.parametrize("idle_sleep_ms", [-1, 5001])
def test_controlled_worker_request_rejects_idle_sleep_ms_outside_bounds(idle_sleep_ms: int) -> None:
    assert (
        controlled_worker_activation_request_error(_request(idle_sleep_ms=idle_sleep_ms))
        == "idle_sleep_ms_not_allowed"
    )


@pytest.mark.asyncio
async def test_controlled_worker_checks_groups_without_create() -> None:
    maintenance_consumer = FakeControlledConsumer([])
    replay_consumer = FakeControlledConsumer([])

    report = await run_controlled_worker_activation(
        _request(max_ticks=1, idle_sleep_ms=0),
        config=config(),
        maintenance_consumer=maintenance_consumer,
        replay_consumer=replay_consumer,
        service=FakeControlledService(due_action_count=0),
    )

    assert report.status == "pass"
    assert maintenance_consumer.ensure_group_allow_create == [False]
    assert replay_consumer.ensure_group_allow_create == [False]
    assert maintenance_consumer.create_group_attempted is False
    assert replay_consumer.create_group_attempted is False


@pytest.mark.asyncio
async def test_controlled_worker_missing_group_blocks_before_reads() -> None:
    maintenance_consumer = FakeControlledConsumer([], group_exists=False)
    replay_consumer = FakeControlledConsumer([])

    report = await run_controlled_worker_activation(
        _request(),
        config=config(),
        maintenance_consumer=maintenance_consumer,
        replay_consumer=replay_consumer,
        service=FakeControlledService(due_action_count=0),
    )

    assert report.status == "blocked"
    assert report.reason_code == "maintenance_consumer_group_missing"
    assert maintenance_consumer.read_calls == 0
    assert replay_consumer.read_calls == 0
    assert maintenance_consumer.create_group_attempted is False
    assert replay_consumer.create_group_attempted is False


def test_controlled_worker_source_contains_no_group_creation_or_run_forever() -> None:
    source = Path("src/services/maintenance/controlled_worker_activation.py").read_text(encoding="utf-8")

    assert "xgroup_create" not in source
    assert "mkstream" not in source
    assert "allow_create=True" not in source
    assert "run_forever" not in source


@pytest.mark.asyncio
async def test_controlled_worker_calls_each_worker_run_once_per_tick(monkeypatch) -> None:
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

    report = await run_controlled_worker_activation(
        _request(max_ticks=2),
        config=config(),
        maintenance_consumer=FakeControlledConsumer([]),
        replay_consumer=FakeControlledConsumer([]),
        service=FakeControlledService(),
    )

    assert calls == ["maintenance", "replay", "due-retry", "maintenance", "replay", "due-retry"]
    assert report.status == "pass"
    assert report.stop_reason == "max_ticks_reached"
    assert report.ticks_completed == 2


@pytest.mark.asyncio
async def test_controlled_worker_stops_at_max_runtime_sec(monkeypatch) -> None:
    clock = MutableClock()
    calls = 0

    async def maintenance_run_once(self):
        nonlocal calls
        calls += 1
        clock.value += 2.0
        return WorkerBatchResult(processed=1, acked=1)

    async def replay_run_once(self):
        return WorkerBatchResult()

    async def due_retry_run_once(self):
        return WorkerBatchResult()

    monkeypatch.setattr(MaintenanceQueueWorker, "run_once", maintenance_run_once)
    monkeypatch.setattr(ReplayQueueWorker, "run_once", replay_run_once)
    monkeypatch.setattr(DueRetryPromotionWorker, "run_once", due_retry_run_once)

    report = await run_controlled_worker_activation(
        _request(max_ticks=20, max_runtime_sec=1),
        config=config(),
        maintenance_consumer=FakeControlledConsumer([]),
        replay_consumer=FakeControlledConsumer([]),
        service=FakeControlledService(),
        monotonic=clock,
        sleep=clock.sleep,
    )

    assert calls == 1
    assert report.status == "pass"
    assert report.stop_reason == "max_runtime_reached"
    assert report.ticks_completed == 1
    assert report.elapsed_ms == 2000


@pytest.mark.asyncio
async def test_controlled_worker_aggregates_processed_acked_and_due_retry_action_counts() -> None:
    maintenance_events = [uuid4(), uuid4()]
    replay_event = uuid4()
    maintenance_consumer = FakeControlledConsumer(
        [
            _message("q.maintenance", maintenance_events[0], message_id="10-0"),
            _message("q.maintenance", maintenance_events[1], message_id="11-0"),
        ]
    )
    replay_consumer = FakeControlledConsumer([_message("q.replay", replay_event, message_id="20-0")])
    service = FakeControlledService()

    report = await run_controlled_worker_activation(
        _request(max_ticks=1, max_messages=2),
        config=config(),
        maintenance_consumer=maintenance_consumer,
        replay_consumer=replay_consumer,
        service=service,
    )

    assert report.status == "pass"
    assert report.maintenance_processed_count == 2
    assert report.maintenance_acked_count == 2
    assert report.replay_processed_count == 1
    assert report.replay_acked_count == 1
    assert report.due_retry_action_count == 2
    assert service.due_limits == [2]


@pytest.mark.asyncio
async def test_controlled_worker_blocks_on_unacked_mismatch(monkeypatch) -> None:
    async def maintenance_run_once(self):
        return WorkerBatchResult(processed=1, acked=0)

    async def replay_run_once(self):
        return WorkerBatchResult(processed=1, acked=1)

    async def due_retry_run_once(self):
        return WorkerBatchResult(processed=1, acked=0)

    monkeypatch.setattr(MaintenanceQueueWorker, "run_once", maintenance_run_once)
    monkeypatch.setattr(ReplayQueueWorker, "run_once", replay_run_once)
    monkeypatch.setattr(DueRetryPromotionWorker, "run_once", due_retry_run_once)

    report = await run_controlled_worker_activation(
        _request(max_ticks=3),
        config=config(),
        maintenance_consumer=FakeControlledConsumer([]),
        replay_consumer=FakeControlledConsumer([]),
        service=FakeControlledService(),
    )

    assert report.status == "blocked"
    assert report.reason_code == "worker_run_once_left_messages_unacked"
    assert report.stop_reason == "unacked_detected"
    assert report.ticks_completed == 1
    assert report.maintenance_processed_count == 1
    assert report.maintenance_acked_count == 0


@pytest.mark.asyncio
async def test_controlled_worker_failed_when_worker_raises(monkeypatch) -> None:
    async def maintenance_run_once(self):
        raise RuntimeError("private failure detail")

    monkeypatch.setattr(MaintenanceQueueWorker, "run_once", maintenance_run_once)

    report = await run_controlled_worker_activation(
        _request(),
        config=config(),
        maintenance_consumer=FakeControlledConsumer([]),
        replay_consumer=FakeControlledConsumer([]),
        service=FakeControlledService(),
    )

    assert report.status == "failed"
    assert report.reason_code == "controlled_worker_run_once_failed"
    assert report.stop_reason == "failed"
    assert report.redactions_applied["exception_body_omitted"] is True


@pytest.mark.asyncio
async def test_controlled_worker_reports_no_work_observed_for_noop_run() -> None:
    report = await run_controlled_worker_activation(
        _request(max_ticks=2, idle_sleep_ms=0),
        config=config(),
        maintenance_consumer=FakeControlledConsumer([]),
        replay_consumer=FakeControlledConsumer([]),
        service=FakeControlledService(due_action_count=0),
    )

    assert report.status == "pass"
    assert report.ticks_completed == 2
    assert report.stop_reason == "no_work_observed"
