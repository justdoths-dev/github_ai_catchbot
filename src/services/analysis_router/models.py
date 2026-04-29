from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID


JudgeProfile = Literal["github_primary", "x_primary", "text_idea_primary"]
RouteAction = Literal["judge", "refresh", "noop"]


@dataclass(slots=True, frozen=True)
class AnalysisRequestedJob:
    trigger_event_id: UUID
    event_type: str
    candidate_group_id: UUID
    bundle_id: UUID
    judge_profile: str | None
    escalation_allowed: bool


@dataclass(slots=True, frozen=True)
class CandidateRouteState:
    candidate_group_id: UUID
    current_bundle_id: UUID | None


@dataclass(slots=True, frozen=True)
class BundleRouteRecord:
    bundle_id: UUID
    candidate_group_id: UUID
    bundle_profile_version: str
    reroot_count: int
    ready_for_analysis: bool
    token_budget_profile: str | None
    created_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class BundleShapeStats:
    member_count: int
    supporting_count: int


@dataclass(slots=True, frozen=True)
class JudgeRouteDecision:
    action: RouteAction
    judge_profile: JudgeProfile | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    prompt_version: str | None = None
    schema_version: str | None = None
    policy_version: str | None = None
    prompt_cache_key: str | None = None
    refresh_reason: str | None = None


@dataclass(slots=True, frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    fields: dict[str, str]


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
