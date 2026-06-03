from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


Verdict = Literal["inspect_now", "later", "skip"]
DeliveryDecision = Literal["send_now", "send_digest", "suppress"]
UrgencyProfile = Literal["high", "normal_silent", "digest", "suppressed"]


@dataclass(slots=True, frozen=True)
class AnalysisPolicyJob:
    trigger_event_id: UUID
    event_type: str
    judge_run_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID
    bundle_id: UUID


@dataclass(slots=True, frozen=True)
class CandidatePolicyContext:
    candidate_group_id: UUID
    current_bundle_id: UUID | None
    current_analysis_id: UUID | None


@dataclass(slots=True, frozen=True)
class JudgeRunPolicyContext:
    judge_run_id: UUID
    bundle_id: UUID
    prompt_version: str
    policy_version: str
    status: str


@dataclass(slots=True, frozen=True)
class JudgeOutputPolicyContext:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None
    created_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class BundlePolicyContext:
    bundle_id: UUID
    candidate_group_id: UUID
    current_primary_artifact_id: UUID
    current_primary_artifact_type: str | None
    created_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class VerdictDecision:
    verdict: Verdict
    reason_codes: list[str]


@dataclass(slots=True, frozen=True)
class DeliveryDecisionResult:
    delivery_decision: DeliveryDecision
    urgency_profile: UrgencyProfile
    suppress_reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class PolicyEvaluation:
    verdict: Verdict
    delivery_decision: DeliveryDecision
    urgency_profile: UrgencyProfile
    reason_codes: list[str]
    policy_reconciled_flag: bool
    suppress_reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class AnalysisDraft:
    candidate_group_id: UUID
    judge_output_id: UUID
    schema_version: str
    policy_version: str
    prompt_version: str
    delivery_policy_version: str
    verdict: Verdict
    delivery_decision: DeliveryDecision
    scores_json: dict[str, Any]
    reason_codes_json: list[str]
    evidence_limitations_ko: str | None
    recommended_action_ko: str | None
    freshness_note_ko: str | None
    model_proposed_verdict: str | None
    policy_reconciled_flag: bool


@dataclass(slots=True, frozen=True)
class ExistingAnalysisRecord:
    analysis_id: UUID
    judge_output_id: UUID
    policy_version: str
    delivery_policy_version: str


@dataclass(slots=True, frozen=True)
class NotificationPlanIntent:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    delivery_decision: DeliveryDecision
    urgency_profile: UrgencyProfile
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str
    dedupe_subject_key: str
    material_change_hash: str
    send_after: str | None
    suppress_reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
