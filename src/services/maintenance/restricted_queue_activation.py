from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

from .delivery_replay import REPLAY_REQUESTED_EVENT_TYPE
from .delivery_retry import DELIVERY_RESULT_EVENT_TYPE, MAINTENANCE_QUEUE_NAME
from .models import DeliveryReplayDecision, DeliveryResultWorkerResult, OutboxEvent, StreamMessage
from .redis_streams import RedisConsumerGroupMissingError


REPLAY_QUEUE_NAME = "q.replay"
SCHEMA_VERSION = "restricted_queue_activation_report_v1"


@dataclass(frozen=True, slots=True)
class RestrictedQueueActivationRequest:
    queue_name: str
    consumer_group: str
    consumer_name: str
    max_messages: int
    ack: bool
    dry_run: bool
    allow_create_group: bool
    expected_event_type: str
    exact_trigger_event_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class RestrictedQueueMessageResult:
    stream_message_id: str
    trigger_event_id_suffix: str | None
    event_type: str | None
    action: str
    acked: bool
    handled: bool
    reason_code: str
    db_writes_attempted: bool


@dataclass(frozen=True, slots=True)
class RestrictedQueueActivationReport:
    schema_version: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    mode: str
    status: str
    processed_count: int
    acked_count: int
    skipped_count: int
    results: list[RestrictedQueueMessageResult]
    reason_code: str | None = None
    redactions_applied: dict[str, bool] = field(default_factory=dict)


class RestrictedQueueConsumerProtocol(Protocol):
    async def ensure_group(self, *, allow_create: bool = True) -> bool: ...
    async def preview_batch(self, *, count: int | None = None) -> list[StreamMessage]: ...
    async def read_batch(self) -> list[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...


class RestrictedQueueServiceProtocol(Protocol):
    async def load_outbox_event(self, trigger_event_id: UUID) -> OutboxEvent | None: ...
    async def handle_maintenance_trigger_event(self, trigger_event_id: UUID) -> DeliveryResultWorkerResult | None: ...
    async def handle_replay_trigger_event(self, trigger_event_id: UUID) -> DeliveryReplayDecision | None: ...


async def run_restricted_queue_activation(
    request: RestrictedQueueActivationRequest,
    *,
    consumer: RestrictedQueueConsumerProtocol,
    service: RestrictedQueueServiceProtocol,
    mode: str,
) -> RestrictedQueueActivationReport:
    gate_error = _request_gate_error(request, mode)
    if gate_error is not None:
        return _report(request, mode=mode, status="blocked", results=[], reason_code=gate_error)

    group_ok = await consumer.ensure_group(allow_create=request.allow_create_group)
    if not group_ok:
        return _report(request, mode=mode, status="blocked", results=[], reason_code="consumer_group_missing")

    try:
        messages = (
            await consumer.preview_batch(count=request.max_messages)
            if request.dry_run
            else await consumer.read_batch()
        )
    except RedisConsumerGroupMissingError:
        return _report(request, mode=mode, status="blocked", results=[], reason_code="consumer_group_missing")

    results: list[RestrictedQueueMessageResult] = []
    for message in messages[: request.max_messages]:
        results.append(await _process_message(request, consumer=consumer, service=service, message=message))
    return _report(request, mode=mode, status=_status_for(request, results), results=results)


async def _process_message(
    request: RestrictedQueueActivationRequest,
    *,
    consumer: RestrictedQueueConsumerProtocol,
    service: RestrictedQueueServiceProtocol,
    message: StreamMessage,
) -> RestrictedQueueMessageResult:
    if message.stream != request.queue_name:
        return _message_result(message, None, None, "skip", False, False, "queue_name_mismatch", False)
    trigger_event_id = _parse_uuid(message.fields.get("trigger_event_id"))
    if trigger_event_id is None:
        return _message_result(message, None, None, "skip", False, False, "invalid_stream_message", False)
    if request.exact_trigger_event_id is not None and trigger_event_id != request.exact_trigger_event_id:
        return _message_result(
            message,
            trigger_event_id,
            None,
            "skip",
            False,
            False,
            "trigger_event_id_mismatch",
            False,
        )

    event = await service.load_outbox_event(trigger_event_id)
    if event is None:
        return _message_result(
            message,
            trigger_event_id,
            None,
            "skip",
            False,
            False,
            "event_outbox_missing",
            False,
        )
    if event.event_type != request.expected_event_type:
        return _message_result(
            message,
            trigger_event_id,
            event.event_type,
            "skip",
            False,
            False,
            "unsupported_event_type",
            False,
        )

    replay_error = _replay_contract_error(request, event)
    if replay_error is not None:
        return _message_result(
            message,
            trigger_event_id,
            event.event_type,
            "skip",
            False,
            False,
            replay_error,
            False,
        )

    if request.dry_run:
        return _message_result(
            message,
            trigger_event_id,
            event.event_type,
            "dry_run_rehydrated",
            False,
            False,
            "dry_run_no_ack",
            False,
        )

    handled = False
    db_writes_attempted = True
    reason_code = "handler_not_processed"
    action = "handle"
    try:
        if request.queue_name == MAINTENANCE_QUEUE_NAME:
            result = await service.handle_maintenance_trigger_event(trigger_event_id)
            handled = _maintenance_result_allows_ack(result)
            reason_code = result.reason_code if result is not None else "handler_returned_none"
            action = result.action if result is not None else "handle"
        else:
            replay_result = await service.handle_replay_trigger_event(trigger_event_id)
            handled = replay_result is not None and replay_result.action == "emit_replay_intent"
            reason_code = replay_result.reason_code if replay_result is not None else "handler_returned_none"
            action = replay_result.action if replay_result is not None else "handle"
    except Exception:
        return _message_result(
            message,
            trigger_event_id,
            event.event_type,
            "handle_failed",
            False,
            False,
            "handler_failed",
            db_writes_attempted,
        )

    if not handled or not request.ack:
        return _message_result(
            message,
            trigger_event_id,
            event.event_type,
            action,
            False,
            handled,
            reason_code,
            db_writes_attempted,
        )

    try:
        await consumer.ack(message.message_id)
    except Exception:
        return _message_result(
            message,
            trigger_event_id,
            event.event_type,
            action,
            False,
            handled,
            "ack_failed_after_handler_success",
            db_writes_attempted,
        )
    return _message_result(
        message,
        trigger_event_id,
        event.event_type,
        action,
        True,
        handled,
        reason_code,
        db_writes_attempted,
    )


def _request_gate_error(request: RestrictedQueueActivationRequest, mode: str) -> str | None:
    if request.queue_name not in {MAINTENANCE_QUEUE_NAME, REPLAY_QUEUE_NAME}:
        return "queue_name_not_allowed"
    if not request.consumer_group or not request.consumer_name:
        return "consumer_identity_missing"
    if request.max_messages < 1 or request.max_messages > 10:
        return "max_messages_not_allowed"
    if mode not in {"plan", "execute", "proof"}:
        return "mode_not_allowed"
    if mode == "execute" and not request.ack:
        return "ack_confirm_missing"
    if mode != "execute" and request.ack:
        return "ack_not_allowed_for_dry_run"
    if mode != "execute" and request.allow_create_group:
        return "group_creation_not_allowed_for_dry_run"
    if request.queue_name == MAINTENANCE_QUEUE_NAME and request.expected_event_type != DELIVERY_RESULT_EVENT_TYPE:
        return "expected_event_type_mismatch"
    if request.queue_name == REPLAY_QUEUE_NAME and request.expected_event_type != REPLAY_REQUESTED_EVENT_TYPE:
        return "expected_event_type_mismatch"
    return None


def _maintenance_result_allows_ack(result: DeliveryResultWorkerResult | None) -> bool:
    if result is None or not result.processed:
        return False
    if result.classification == "identity_invalid" or result.action == "fail_closed":
        return False
    return True


def _replay_contract_error(request: RestrictedQueueActivationRequest, event: OutboxEvent) -> str | None:
    if request.queue_name != REPLAY_QUEUE_NAME:
        return None
    replay_type = event.payload_json.get("replay_type")
    root_object_type = event.payload_json.get("root_object_type")
    if replay_type != "delivery":
        return "unsupported_replay_type"
    if root_object_type != "notification_plan":
        return "unsupported_replay_root"
    return None


def _status_for(request: RestrictedQueueActivationRequest, results: list[RestrictedQueueMessageResult]) -> str:
    if not results:
        return "pass"
    if request.ack:
        return "pass" if all(result.acked for result in results) else "blocked"
    return "pass"


def _report(
    request: RestrictedQueueActivationRequest,
    *,
    mode: str,
    status: str,
    results: list[RestrictedQueueMessageResult],
    reason_code: str | None = None,
) -> RestrictedQueueActivationReport:
    return RestrictedQueueActivationReport(
        schema_version=SCHEMA_VERSION,
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        consumer_name=request.consumer_name,
        mode=mode,
        status=status,
        processed_count=len(results),
        acked_count=sum(1 for result in results if result.acked),
        skipped_count=sum(1 for result in results if not result.acked),
        results=results,
        reason_code=reason_code,
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _message_result(
    message: StreamMessage,
    trigger_event_id: UUID | None,
    event_type: str | None,
    action: str,
    acked: bool,
    handled: bool,
    reason_code: str,
    db_writes_attempted: bool,
) -> RestrictedQueueMessageResult:
    return RestrictedQueueMessageResult(
        stream_message_id=_safe_stream_id_suffix(message.message_id),
        trigger_event_id_suffix=_safe_uuid_suffix(trigger_event_id),
        event_type=event_type,
        action=action,
        acked=acked,
        handled=handled,
        reason_code=reason_code,
        db_writes_attempted=db_writes_attempted,
    )


def _parse_uuid(value: object) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_uuid_suffix(value: UUID | None) -> str | None:
    return None if value is None else str(value)[-8:]


def _safe_stream_id_suffix(value: str) -> str:
    return str(value)[-8:]
