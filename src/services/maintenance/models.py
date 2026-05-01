from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


DeliveryRetryAction = Literal["noop", "emit_retry_intent", "dead_letter_retry_ceiling"]
ReplayAction = Literal["reject", "emit_replay_intent"]


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


@dataclass(slots=True, frozen=True)
class OutboxEvent:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class DeliveryResultEvent:
    trigger_event_id: UUID
    notification_plan_id: UUID
    delivery_status: str
    notification_delivery_record_id: UUID | None
    attempt_count: int | None
    transport_error_code: str | None
    transport_error_class: str | None


@dataclass(slots=True, frozen=True)
class NotificationPlanRecord:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str | None
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime | None
    suppress_reason_code: str | None
    status: str


@dataclass(slots=True, frozen=True)
class LatestDeliveryRecord:
    notification_delivery_record_id: UUID
    notification_plan_id: UUID
    delivery_status: str
    attempt_count: int
    transport_error_code: str | None
    transport_error_class: str | None
    telegram_response_json: dict[str, Any] | None
    created_at: datetime


@dataclass(slots=True, frozen=True)
class RetryPromotionCandidate:
    plan: NotificationPlanRecord
    latest_delivery: LatestDeliveryRecord | None
    delivery_attempt_count: int


@dataclass(slots=True, frozen=True)
class DeliveryRetryDecision:
    action: DeliveryRetryAction
    reason_code: str
    retry_attempt: int | None = None
    dedupe_key: str | None = None
    payload: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class ReplayRequestRecord:
    replay_request_id: UUID
    replay_type: str
    root_object_type: str
    root_object_id: UUID
    status: str | None
    requested_by: str | None = None
    requested_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class ReplayRequestedEvent:
    trigger_event_id: UUID
    replay_request_id: UUID
    replay_type: str | None
    root_object_type: str | None
    root_object_id: UUID | None
    replay_reason: str | None


@dataclass(slots=True, frozen=True)
class DeliveryReplayDecision:
    action: ReplayAction
    reason_code: str
    dedupe_key: str | None = None
    payload: dict[str, Any] | None = None
