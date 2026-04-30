from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


DeliveryDecision = Literal["send_now", "send_digest", "suppress"]
UrgencyProfile = Literal["high", "normal_silent", "digest", "suppressed"]
DeliveryStatus = Literal["planned", "rendered", "queued", "sent", "edited", "suppressed", "failed_retryable", "failed_terminal"]


@dataclass(slots=True, frozen=True)
class NotificationIntentJob:
    trigger_event_id: UUID
    event_type: str
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    delivery_decision: DeliveryDecision
    urgency_profile: UrgencyProfile
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str | None
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime | None
    suppress_reason_code: str | None


@dataclass(slots=True, frozen=True)
class AnalysisRenderContext:
    analysis_id: UUID
    candidate_group_id: UUID
    judge_output_id: UUID
    verdict: str
    delivery_decision: str
    reason_codes_json: list[str]
    evidence_limitations_ko: str | None
    recommended_action_ko: str | None
    freshness_note_ko: str | None
    created_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class JudgeOutputRenderContext:
    judge_output_id: UUID
    payload_json: dict[str, Any]
    model_confidence_band: str | None = None


@dataclass(slots=True, frozen=True)
class CandidateRenderContext:
    candidate_group_id: UUID
    source_message_id: UUID | None
    current_primary_artifact_id: UUID | None
    primary_artifact_type: str | None
    primary_canonical_url: str | None
    primary_canonical_id: str | None
    source_message_link: str | None
    source_text_surface: str | None


@dataclass(slots=True, frozen=True)
class NotificationPlanDraft:
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
    status: str = "planned"


@dataclass(slots=True, frozen=True)
class NotificationRenderDraft:
    notification_plan_id: UUID
    message_text: str
    entities_json: list[dict[str, Any]]
    link_preview_options_json: dict[str, Any]
    reply_markup_json: dict[str, Any] | None
    disable_notification: bool
    protect_content: bool
    parse_strategy: str
    render_hash: str


@dataclass(slots=True, frozen=True)
class DeliveryAction:
    mode: Literal["send", "noop"]
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class DeliveryResult:
    delivery_status: DeliveryStatus
    telegram_chat_id: int | None
    telegram_message_id: int | None
    attempt_count: int
    transport_error_code: str | None = None
    transport_error_class: str | None = None
    telegram_response_json: dict[str, Any] | None = None
    edited: bool = False


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
