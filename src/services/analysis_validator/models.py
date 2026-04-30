from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


ValidatorAction = Literal["forward_policy", "refused", "failed_terminal", "failed_retryable", "noop"]


@dataclass(slots=True, frozen=True)
class JudgeOutputReadyJob:
    trigger_event_id: UUID
    event_type: str
    judge_run_id: UUID
    judge_output_id: UUID
    finish_reason: str | None
    refusal_detected: bool


@dataclass(slots=True, frozen=True)
class JudgeRunValidationRecord:
    judge_run_id: UUID
    bundle_id: UUID
    judge_profile: str
    schema_version: str
    policy_version: str
    status: str
    finish_reason: str | None
    refusal_detected: bool


@dataclass(slots=True, frozen=True)
class JudgeOutputRecord:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None
    created_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class BundleValidationContext:
    bundle_id: UUID
    candidate_group_id: UUID
    current_primary_artifact_id: UUID
    current_primary_artifact_type: str | None
    created_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class ValidationDecision:
    action: ValidatorAction
    reason_code: str | None = None
    transition_to_state: str | None = None


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
