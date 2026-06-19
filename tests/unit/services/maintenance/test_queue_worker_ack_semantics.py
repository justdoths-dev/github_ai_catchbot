from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.maintenance.models import DeliveryReplayDecision, DeliveryResultWorkerResult, StreamMessage
from services.maintenance.worker import MaintenanceQueueWorker, ReplayQueueWorker
from tests.component.services.maintenance._fakes import FakeConsumer, config


class FakeQueueService:
    def __init__(
        self,
        *,
        maintenance_result: DeliveryResultWorkerResult | None = None,
        replay_result: DeliveryReplayDecision | None = None,
        maintenance_raises: bool = False,
        replay_raises: bool = False,
    ) -> None:
        self.maintenance_result = maintenance_result
        self.replay_result = replay_result
        self.maintenance_raises = maintenance_raises
        self.replay_raises = replay_raises
        self.maintenance_calls: list[UUID] = []
        self.replay_calls: list[UUID] = []

    async def handle_maintenance_trigger_event(self, trigger_event_id: str | UUID):
        if self.maintenance_raises:
            raise RuntimeError("redacted maintenance handler failure")
        self.maintenance_calls.append(UUID(str(trigger_event_id)))
        return self.maintenance_result

    async def handle_replay_trigger_event(self, trigger_event_id: str | UUID):
        if self.replay_raises:
            raise RuntimeError("redacted replay handler failure")
        self.replay_calls.append(UUID(str(trigger_event_id)))
        return self.replay_result

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        del limit
        return 0


def _maintenance_result(
    *,
    processed: bool = True,
    classification: str = "terminal_success",
    action: str = "mark_terminal_success",
    reason_code: str = "delivery_result_terminal_success",
) -> DeliveryResultWorkerResult:
    return DeliveryResultWorkerResult(
        processed=processed,
        classification=classification,  # type: ignore[arg-type]
        action=action,  # type: ignore[arg-type]
        reason_code=reason_code,
    )


def _maintenance_message(event_id: UUID | str | None, *, message_id: str = "1-0") -> StreamMessage:
    fields = {} if event_id is None else {"trigger_event_id": str(event_id)}
    return StreamMessage(stream="q.maintenance", message_id=message_id, fields=fields)


def _replay_message(event_id: UUID | str | None, *, message_id: str = "1-0") -> StreamMessage:
    fields = {} if event_id is None else {"trigger_event_id": str(event_id)}
    return StreamMessage(stream="q.replay", message_id=message_id, fields=fields)


@pytest.mark.asyncio
async def test_maintenance_worker_acks_successful_handler_result_once() -> None:
    event_id = uuid4()
    service = FakeQueueService(maintenance_result=_maintenance_result())
    consumer = FakeConsumer([_maintenance_message(event_id)])
    worker = MaintenanceQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert service.maintenance_calls == [event_id]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler_result",
    [
        _maintenance_result(
            processed=False,
            classification="identity_invalid",
            action="fail_closed",
            reason_code="notification_plan_missing",
        ),
        _maintenance_result(
            processed=False,
            classification="unsupported",
            action="unsupported",
            reason_code="unsupported_event_type",
        ),
        None,
    ],
)
async def test_maintenance_worker_does_not_ack_fail_closed_unsupported_or_none(handler_result) -> None:
    event_id = uuid4()
    service = FakeQueueService(maintenance_result=handler_result)
    consumer = FakeConsumer([_maintenance_message(event_id)])
    worker = MaintenanceQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 0
    assert consumer.acked == []
    assert service.maintenance_calls == [event_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("event_id", [None, "not-a-uuid"])
async def test_maintenance_worker_does_not_ack_missing_or_malformed_trigger_event_id(event_id) -> None:
    service = FakeQueueService(maintenance_result=_maintenance_result())
    consumer = FakeConsumer([_maintenance_message(event_id)])
    worker = MaintenanceQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 0
    assert consumer.acked == []
    assert service.maintenance_calls == []


@pytest.mark.asyncio
async def test_maintenance_worker_does_not_ack_handler_exception() -> None:
    event_id = uuid4()
    service = FakeQueueService(maintenance_raises=True)
    consumer = FakeConsumer([_maintenance_message(event_id)])
    worker = MaintenanceQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 0
    assert consumer.acked == []
    assert service.maintenance_calls == []


@pytest.mark.asyncio
async def test_replay_worker_acks_successful_replay_dispatch_once() -> None:
    event_id = uuid4()
    service = FakeQueueService(
        replay_result=DeliveryReplayDecision(action="emit_replay_intent", reason_code="explicit_delivery_replay")
    )
    consumer = FakeConsumer([_replay_message(event_id)])
    worker = ReplayQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert service.replay_calls == [event_id]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "replay_result",
    [
        DeliveryReplayDecision(action="reject", reason_code="unsupported_replay_type"),
        DeliveryReplayDecision(action="reject", reason_code="notification_plan_missing"),
        None,
    ],
)
async def test_replay_worker_does_not_ack_rejected_missing_root_or_none(replay_result) -> None:
    event_id = uuid4()
    service = FakeQueueService(replay_result=replay_result)
    consumer = FakeConsumer([_replay_message(event_id)])
    worker = ReplayQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 0
    assert consumer.acked == []
    assert service.replay_calls == [event_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("event_id", [None, "not-a-uuid"])
async def test_replay_worker_does_not_ack_missing_or_malformed_trigger_event_id(event_id) -> None:
    service = FakeQueueService(
        replay_result=DeliveryReplayDecision(action="emit_replay_intent", reason_code="explicit_delivery_replay")
    )
    consumer = FakeConsumer([_replay_message(event_id)])
    worker = ReplayQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 0
    assert consumer.acked == []
    assert service.replay_calls == []


@pytest.mark.asyncio
async def test_replay_worker_does_not_ack_handler_exception() -> None:
    event_id = uuid4()
    service = FakeQueueService(replay_raises=True)
    consumer = FakeConsumer([_replay_message(event_id)])
    worker = ReplayQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 0
    assert consumer.acked == []
    assert service.replay_calls == []
