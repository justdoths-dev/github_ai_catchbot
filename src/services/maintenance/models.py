from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


DeliveryRetryAction = Literal["noop", "emit_retry_intent", "dead_letter_retry_ceiling"]
ReplayAction = Literal["reject", "emit_replay_intent"]
GateMode = Literal["restricted", "full"]
GateStatus = Literal["pass", "fail", "warn"]
RecoveryMode = Literal["replay-selected", "retry-selected-due"]
DeliveryResultWorkerClassification = Literal[
    "ignored",
    "unsupported",
    "terminal_success",
    "logical_noop_success",
    "retryable_candidate",
    "terminal_failure",
]
DeliveryResultWorkerAction = Literal[
    "ignored",
    "unsupported",
    "mark_terminal_success",
    "mark_logical_noop_success",
    "already_marked",
    "record_retryable_interpretation",
    "record_terminal_failure",
]


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
class DeliveryResultWorkerResult:
    processed: bool
    classification: DeliveryResultWorkerClassification
    action: DeliveryResultWorkerAction
    reason_code: str
    marker_written: bool = False
    already_marked: bool = False
    retry_intent_written: bool = False
    dead_letter_written: bool = False
    replay_request_written: bool = False


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


@dataclass(slots=True, frozen=True)
class DeliveryGateMetric:
    metric_name: str
    observed_value: float | int | str | bool | None
    threshold: float | int | str | bool | None
    comparator: str
    passed: bool
    severity: str = "block"


@dataclass(slots=True, frozen=True)
class DeliveryGateReportV1:
    mode: GateMode
    gate_status: GateStatus
    blocking_reason_codes: list[str]
    warning_reason_codes: list[str]
    metrics: list[DeliveryGateMetric]
    operator_review_required: bool
    operator_review_passed: bool | None
    recommended_flag_patch: dict[str, object]


@dataclass(slots=True, frozen=True)
class DeliveryGateSnapshot:
    success_rate_1h: float | None
    success_rate_24h: float | None
    high_source_to_delivery_p95_sec: float | None
    plan_to_transport_p95_sec: float | None
    due_retry_oldest_lag_sec: float | None
    open_delivery_dlq_count: int
    oldest_delivery_dlq_age_sec: float | None
    unexpected_send_disabled_count: int
    replay_guard_reject_count_24h: int
    retry_ceiling_exceeded_count_24h: int
    duplicate_noop_ratio_1h: float | None


@dataclass(slots=True, frozen=True)
class SelectedPlanRecoveryRow:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    plan_status: str
    delivery_status: str | None
    attempt_count: int | None
    send_after: datetime | None
    telegram_chat_id: int | None
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str | None
    dedupe_subject_key: str
    material_change_hash: str
    urgency_profile: str
    delivery_decision: str
    send_disabled: bool = False
    has_open_replay_request: bool = False
    has_delivery_dlq: bool = False


@dataclass(slots=True, frozen=True)
class RecoveryBatchResult:
    recovery_batch_id: str
    recovery_mode: RecoveryMode
    selected_count: int
    accepted_count: int
    skipped_count: int
    emitted_count: int
    skipped_reason_codes: dict[str, int] = field(default_factory=dict)
