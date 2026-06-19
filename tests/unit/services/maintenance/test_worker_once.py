from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.maintenance.models import DeliveryReplayDecision, DeliveryResultWorkerResult, StreamMessage
from services.maintenance.worker_once import WorkerOnceRequest, run_worker_once
from tests.component.services.maintenance._fakes import config


class FakeWorkerOnceConsumer:
    def __init__(self, messages: list[StreamMessage], *, group_exists: bool = True) -> None:
        self.messages = messages
        self.group_exists = group_exists
        self.ensure_group_allow_create: list[bool] = []
        self.read_calls = 0
        self.acked: list[str] = []
        self.create_group_attempted = False

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


class FakeWorkerOnceService:
    def __init__(
        self,
        *,
        maintenance_result: DeliveryResultWorkerResult | None = None,
        replay_result: DeliveryReplayDecision | None = None,
        maintenance_raises: bool = False,
    ) -> None:
        self.maintenance_result = maintenance_result
        self.replay_result = replay_result
        self.maintenance_raises = maintenance_raises
        self.maintenance_calls: list[UUID] = []
        self.replay_calls: list[UUID] = []

    async def handle_maintenance_trigger_event(self, trigger_event_id: str | UUID):
        if self.maintenance_raises:
            raise RuntimeError("redacted maintenance failure")
        self.maintenance_calls.append(UUID(str(trigger_event_id)))
        return self.maintenance_result

    async def handle_replay_trigger_event(self, trigger_event_id: str | UUID):
        self.replay_calls.append(UUID(str(trigger_event_id)))
        return self.replay_result

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        del limit
        return 0


def _request(worker_type: str = "maintenance", *, max_messages: int = 1, confirm_ack: bool = True) -> WorkerOnceRequest:
    if worker_type == "maintenance":
        return WorkerOnceRequest(
            worker_type="maintenance",
            queue_name="q.maintenance",
            consumer_group="maintenance",
            mode="execute",
            max_messages=max_messages,
            confirm_ack=confirm_ack,
        )
    return WorkerOnceRequest(
        worker_type="replay",
        queue_name="q.replay",
        consumer_group="maintenance-replay",
        mode="execute",
        max_messages=max_messages,
        confirm_ack=confirm_ack,
    )


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


def _message(worker_type: str, event_id: UUID | str | None, *, message_id: str = "1234567890-0") -> StreamMessage:
    queue_name = "q.maintenance" if worker_type == "maintenance" else "q.replay"
    fields = {} if event_id is None else {"trigger_event_id": str(event_id)}
    return StreamMessage(stream=queue_name, message_id=message_id, fields=fields)


@pytest.mark.asyncio
async def test_worker_once_rejects_execute_without_confirm_ack() -> None:
    consumer = FakeWorkerOnceConsumer([])
    report = await run_worker_once(
        _request(confirm_ack=False),
        config=config(),
        consumer=consumer,
        service=FakeWorkerOnceService(),
    )

    assert report.status == "blocked"
    assert report.reason_code == "ack_confirm_missing"
    assert report.processed_count == 0
    assert report.acked_count == 0
    assert consumer.ensure_group_allow_create == []


@pytest.mark.asyncio
@pytest.mark.parametrize("max_messages", [0, 11])
async def test_worker_once_rejects_max_messages_outside_bounds(max_messages: int) -> None:
    consumer = FakeWorkerOnceConsumer([])
    report = await run_worker_once(
        _request(max_messages=max_messages),
        config=config(),
        consumer=consumer,
        service=FakeWorkerOnceService(),
    )

    assert report.status == "blocked"
    assert report.reason_code == "max_messages_not_allowed"
    assert report.processed_count == 0
    assert report.acked_count == 0
    assert consumer.ensure_group_allow_create == []


@pytest.mark.asyncio
async def test_worker_once_maintenance_invokes_actual_worker_run_once_path() -> None:
    event_id = uuid4()
    consumer = FakeWorkerOnceConsumer([_message("maintenance", event_id)])
    service = FakeWorkerOnceService(maintenance_result=_maintenance_result())

    report = await run_worker_once(_request("maintenance"), config=config(), consumer=consumer, service=service)

    assert report.status == "pass"
    assert report.processed_count == 1
    assert report.acked_count == 1
    assert report.redactions_applied["full_uuid_omitted"] is True
    assert report.redactions_applied["full_redis_message_id_omitted"] is True
    assert report.redactions_applied["payload_json_omitted"] is True
    assert consumer.ensure_group_allow_create == [False]
    assert consumer.create_group_attempted is False
    assert consumer.acked == ["1234567890-0"]
    assert service.maintenance_calls == [event_id]


@pytest.mark.asyncio
async def test_worker_once_replay_invokes_actual_worker_run_once_path() -> None:
    event_id = uuid4()
    consumer = FakeWorkerOnceConsumer([_message("replay", event_id)])
    service = FakeWorkerOnceService(
        replay_result=DeliveryReplayDecision(action="emit_replay_intent", reason_code="explicit_delivery_replay")
    )

    report = await run_worker_once(_request("replay"), config=config(), consumer=consumer, service=service)

    assert report.status == "pass"
    assert report.processed_count == 1
    assert report.acked_count == 1
    assert consumer.ensure_group_allow_create == [False]
    assert consumer.create_group_attempted is False
    assert consumer.acked == ["1234567890-0"]
    assert service.replay_calls == [event_id]


@pytest.mark.asyncio
async def test_worker_once_malformed_message_reports_no_ack() -> None:
    consumer = FakeWorkerOnceConsumer([_message("maintenance", "not-a-uuid")])
    service = FakeWorkerOnceService(maintenance_result=_maintenance_result())

    report = await run_worker_once(_request("maintenance"), config=config(), consumer=consumer, service=service)

    assert report.status == "blocked"
    assert report.reason_code == "worker_run_once_left_messages_unacked"
    assert report.processed_count == 1
    assert report.acked_count == 0
    assert consumer.acked == []
    assert service.maintenance_calls == []


@pytest.mark.asyncio
async def test_worker_once_handler_failure_reports_no_ack() -> None:
    event_id = uuid4()
    consumer = FakeWorkerOnceConsumer([_message("maintenance", event_id)])
    service = FakeWorkerOnceService(maintenance_raises=True)

    report = await run_worker_once(_request("maintenance"), config=config(), consumer=consumer, service=service)

    assert report.status == "blocked"
    assert report.reason_code == "worker_run_once_left_messages_unacked"
    assert report.processed_count == 1
    assert report.acked_count == 0
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_worker_once_requires_existing_consumer_group_without_create() -> None:
    consumer = FakeWorkerOnceConsumer([_message("maintenance", uuid4())], group_exists=False)

    report = await run_worker_once(
        _request("maintenance"),
        config=config(),
        consumer=consumer,
        service=FakeWorkerOnceService(maintenance_result=_maintenance_result()),
    )

    assert report.status == "blocked"
    assert report.reason_code == "consumer_group_missing"
    assert report.processed_count == 0
    assert report.acked_count == 0
    assert consumer.ensure_group_allow_create == [False]
    assert consumer.create_group_attempted is False
    assert consumer.read_calls == 0
    assert consumer.acked == []
