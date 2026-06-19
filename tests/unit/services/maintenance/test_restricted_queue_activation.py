from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.maintenance.delivery_replay import REPLAY_REQUESTED_EVENT_TYPE
from services.maintenance.delivery_retry import DELIVERY_RESULT_EVENT_TYPE
from services.maintenance.models import DeliveryReplayDecision, DeliveryResultWorkerResult, OutboxEvent, StreamMessage
from services.maintenance.restricted_queue_activation import (
    RestrictedQueueActivationRequest,
    run_restricted_queue_activation,
)


class FakeConsumer:
    def __init__(self, messages: list[StreamMessage], *, group_exists: bool = True) -> None:
        self.messages = messages
        self.group_exists = group_exists
        self.preview_calls: list[int | None] = []
        self.read_calls = 0
        self.acked: list[str] = []
        self.order: list[str] = []

    async def ensure_group(self, *, allow_create: bool = True) -> bool:
        self.order.append(f"ensure_group:{allow_create}")
        return self.group_exists

    async def preview_batch(self, *, count: int | None = None) -> list[StreamMessage]:
        self.order.append("preview")
        self.preview_calls.append(count)
        return self.messages[: count or len(self.messages)]

    async def read_batch(self) -> list[StreamMessage]:
        self.order.append("read")
        self.read_calls += 1
        return self.messages

    async def ack(self, message_id: str) -> None:
        self.order.append("ack")
        self.acked.append(message_id)


class FakeService:
    def __init__(
        self,
        events: dict[UUID, OutboxEvent],
        *,
        maintenance_result: DeliveryResultWorkerResult | None = None,
        replay_result: DeliveryReplayDecision | None = None,
    ) -> None:
        self.events = events
        self.maintenance_result = maintenance_result or DeliveryResultWorkerResult(
            processed=True,
            classification="retryable_candidate",
            action="record_retryable_interpretation",
            reason_code="failed_retryable_deferred_to_due_scan",
        )
        self.replay_result = replay_result or DeliveryReplayDecision(
            action="emit_replay_intent",
            reason_code="explicit_delivery_replay",
        )
        self.maintenance_calls: list[UUID] = []
        self.replay_calls: list[UUID] = []
        self.order: list[str] = []

    async def load_outbox_event(self, trigger_event_id: UUID) -> OutboxEvent | None:
        self.order.append("load")
        return self.events.get(trigger_event_id)

    async def handle_maintenance_trigger_event(self, trigger_event_id: UUID):
        self.order.append("handle_maintenance")
        self.maintenance_calls.append(trigger_event_id)
        return self.maintenance_result

    async def handle_replay_trigger_event(self, trigger_event_id: UUID):
        self.order.append("handle_replay")
        self.replay_calls.append(trigger_event_id)
        return self.replay_result


def _request(queue_name: str, *, mode: str = "plan") -> RestrictedQueueActivationRequest:
    return RestrictedQueueActivationRequest(
        queue_name=queue_name,
        consumer_group="maintenance" if queue_name == "q.maintenance" else "maintenance-replay",
        consumer_name="operator",
        max_messages=1,
        ack=mode == "execute",
        dry_run=mode != "execute",
        allow_create_group=False,
        expected_event_type=DELIVERY_RESULT_EVENT_TYPE if queue_name == "q.maintenance" else REPLAY_REQUESTED_EVENT_TYPE,
    )


def _event(event_id: UUID, event_type: str, payload_json: dict) -> OutboxEvent:
    return OutboxEvent(
        event_id=event_id,
        event_type=event_type,
        aggregate_type="notification_plan",
        aggregate_id=uuid4(),
        payload_json=payload_json,
    )


def _message(event_id: UUID, *, message_id: str = "1740000000000-42", stream: str = "q.maintenance") -> StreamMessage:
    return StreamMessage(
        stream=stream,
        message_id=message_id,
        fields={"trigger_event_id": str(event_id), "payload_json": "must-not-be-used"},
    )


@pytest.mark.asyncio
async def test_plan_rehydrates_but_does_not_handle_or_ack() -> None:
    event_id = uuid4()
    service = FakeService(
        {
            event_id: _event(
                event_id,
                DELIVERY_RESULT_EVENT_TYPE,
                {"notification_plan_id": str(uuid4()), "delivery_status": "failed_retryable"},
            )
        }
    )
    consumer = FakeConsumer([_message(event_id)])

    report = await run_restricted_queue_activation(
        _request("q.maintenance", mode="plan"),
        consumer=consumer,
        service=service,
        mode="plan",
    )

    assert report.status == "pass"
    assert report.processed_count == 1
    assert report.acked_count == 0
    assert service.maintenance_calls == []
    assert consumer.acked == []
    assert report.results[0].reason_code == "dry_run_no_ack"
    assert report.results[0].db_writes_attempted is False


@pytest.mark.asyncio
async def test_execute_acks_only_after_maintenance_handler_success() -> None:
    event_id = uuid4()
    service = FakeService({event_id: _event(event_id, DELIVERY_RESULT_EVENT_TYPE, {})})
    consumer = FakeConsumer([_message(event_id)])

    report = await run_restricted_queue_activation(
        _request("q.maintenance", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "pass"
    assert report.acked_count == 1
    assert service.maintenance_calls == [event_id]
    assert service.order == ["load", "handle_maintenance"]
    assert consumer.order == ["ensure_group:False", "read", "ack"]


@pytest.mark.asyncio
async def test_missing_trigger_event_id_does_not_ack() -> None:
    consumer = FakeConsumer([StreamMessage(stream="q.maintenance", message_id="1-0", fields={})])
    service = FakeService({})

    report = await run_restricted_queue_activation(
        _request("q.maintenance", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "blocked"
    assert report.acked_count == 0
    assert report.results[0].reason_code == "invalid_stream_message"
    assert consumer.acked == []
    assert service.maintenance_calls == []


@pytest.mark.asyncio
async def test_maintenance_rejects_non_delivery_result_without_ack() -> None:
    event_id = uuid4()
    consumer = FakeConsumer([_message(event_id)])
    service = FakeService({event_id: _event(event_id, "replay.requested.v1", {})})

    report = await run_restricted_queue_activation(
        _request("q.maintenance", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "blocked"
    assert report.results[0].reason_code == "unsupported_event_type"
    assert report.acked_count == 0
    assert service.maintenance_calls == []


@pytest.mark.asyncio
async def test_replay_rejects_non_delivery_replay_without_ack() -> None:
    event_id = uuid4()
    consumer = FakeConsumer([_message(event_id, stream="q.replay")])
    service = FakeService(
        {
            event_id: _event(
                event_id,
                REPLAY_REQUESTED_EVENT_TYPE,
                {"replay_type": "source", "root_object_type": "notification_plan"},
            )
        }
    )

    report = await run_restricted_queue_activation(
        _request("q.replay", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "blocked"
    assert report.results[0].reason_code == "unsupported_replay_type"
    assert report.acked_count == 0
    assert service.replay_calls == []


@pytest.mark.asyncio
async def test_replay_rejects_non_notification_plan_root_without_ack() -> None:
    event_id = uuid4()
    consumer = FakeConsumer([_message(event_id, stream="q.replay")])
    service = FakeService(
        {
            event_id: _event(
                event_id,
                REPLAY_REQUESTED_EVENT_TYPE,
                {"replay_type": "delivery", "root_object_type": "analysis"},
            )
        }
    )

    report = await run_restricted_queue_activation(
        _request("q.replay", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "blocked"
    assert report.results[0].reason_code == "unsupported_replay_root"
    assert report.acked_count == 0
    assert service.replay_calls == []


@pytest.mark.asyncio
async def test_group_missing_is_reported_and_no_ack_happens() -> None:
    event_id = uuid4()
    consumer = FakeConsumer([_message(event_id)], group_exists=False)
    service = FakeService({})

    report = await run_restricted_queue_activation(
        _request("q.maintenance", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "blocked"
    assert report.reason_code == "consumer_group_missing"
    assert report.processed_count == 0
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_replay_execute_calls_replay_handler_and_acks_after_success() -> None:
    event_id = uuid4()
    consumer = FakeConsumer([_message(event_id, stream="q.replay")])
    service = FakeService(
        {
            event_id: _event(
                event_id,
                REPLAY_REQUESTED_EVENT_TYPE,
                {"replay_type": "delivery", "root_object_type": "notification_plan"},
            )
        }
    )

    report = await run_restricted_queue_activation(
        _request("q.replay", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "pass"
    assert report.acked_count == 1
    assert service.replay_calls == [event_id]
    assert service.order == ["load", "handle_replay"]
    assert consumer.order == ["ensure_group:False", "read", "ack"]


@pytest.mark.asyncio
async def test_fail_closed_maintenance_result_does_not_ack() -> None:
    event_id = uuid4()
    consumer = FakeConsumer([_message(event_id)])
    service = FakeService(
        {event_id: _event(event_id, DELIVERY_RESULT_EVENT_TYPE, {})},
        maintenance_result=DeliveryResultWorkerResult(
            processed=False,
            classification="identity_invalid",
            action="fail_closed",
            reason_code="notification_plan_missing",
        ),
    )

    report = await run_restricted_queue_activation(
        _request("q.maintenance", mode="execute"),
        consumer=consumer,
        service=service,
        mode="execute",
    )

    assert report.status == "blocked"
    assert report.results[0].handled is False
    assert report.results[0].reason_code == "notification_plan_missing"
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_report_redacts_full_ids_and_payloads() -> None:
    event_id = UUID("11111111-2222-3333-4444-555555555555")
    service = FakeService({event_id: _event(event_id, DELIVERY_RESULT_EVENT_TYPE, {"raw": "payload"})})
    consumer = FakeConsumer([_message(event_id, message_id="1740000000000-987654321")])

    report = await run_restricted_queue_activation(
        _request("q.maintenance", mode="plan"),
        consumer=consumer,
        service=service,
        mode="plan",
    )

    rendered = repr(report)
    assert str(event_id) not in rendered
    assert "'raw': 'payload'" not in rendered
    assert report.results[0].trigger_event_id_suffix == "55555555"
    assert report.redactions_applied["full_uuid_omitted"] is True
    assert report.redactions_applied["payload_json_omitted"] is True
