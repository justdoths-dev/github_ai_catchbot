from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


JudgeRunStatus = Literal["pending", "running", "succeeded", "failed_retryable", "failed_terminal"]


@dataclass(slots=True, frozen=True)
class JudgeCallJob:
    trigger_event_id: UUID
    event_type: str
    judge_run_id: UUID
    bundle_id: UUID
    model: str
    reasoning_effort: str
    prompt_version: str
    prompt_cache_key: str | None


@dataclass(slots=True, frozen=True)
class JudgeRunRecord:
    judge_run_id: UUID
    bundle_id: UUID
    judge_profile: str
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    prompt_cache_key: str | None
    status: str
    schema_retry_count: int


@dataclass(slots=True, frozen=True)
class BundleJudgeContext:
    bundle_id: UUID
    candidate_group_id: UUID
    current_primary_artifact_id: UUID
    primary_summary: dict[str, Any]
    supporting_summaries_json: list[dict[str, Any]]
    discovered_links_summary_json: list[dict[str, Any]]
    evidence_limitations: list[str]
    token_budget_profile: str | None
    reroot_count: int
    created_at: datetime | None = None

    def is_structurally_usable(self) -> bool:
        return bool(
            self.candidate_group_id
            and self.bundle_id
            and self.current_primary_artifact_id
            and self.primary_summary
            and self.token_budget_profile
        )


@dataclass(slots=True, frozen=True)
class PreparedModelContext:
    developer_prompt: str
    user_context: str
    preflight_notes: list[str]
    preflight_flags: dict[str, Any]


@dataclass(slots=True, frozen=True)
class OpenAIJudgeUsage:
    input_tokens: int | None = None
    cached_input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    latency_ms: int | None = None


@dataclass(slots=True, frozen=True)
class OpenAIJudgeResult:
    payload_json: dict[str, Any] | None
    refusal_text: str | None
    finish_reason: str | None
    usage: OpenAIJudgeUsage
    raw_response_id: str | None = None

    @property
    def refusal_detected(self) -> bool:
        return bool(self.refusal_text)

    @property
    def has_structured_payload(self) -> bool:
        return isinstance(self.payload_json, dict)


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
