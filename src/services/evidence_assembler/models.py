from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


SnapshotStatus = Literal[
    "pending",
    "fetching",
    "ready",
    "partial_ready",
    "failed_transient",
    "failed_permanent",
    "rate_limited",
    "access_denied",
    "unsupported",
    "low_evidence",
]


@dataclass(slots=True, frozen=True)
class TriggerEventRecord:
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class BundleRefreshTarget:
    candidate_group_id: UUID
    trigger_event_id: UUID
    trigger_event_type: str
    trigger_artifact_id: UUID | None = None
    trigger_snapshot_id: UUID | None = None


@dataclass(slots=True, frozen=True)
class CandidateGroupRecord:
    candidate_group_id: UUID
    source_message_id: UUID
    source_version_no: int
    initial_primary_artifact_id: UUID
    current_primary_artifact_id: UUID
    proposal_status: str
    current_bundle_id: UUID | None


@dataclass(slots=True, frozen=True)
class CandidateMemberRecord:
    artifact_id: UUID
    artifact_type: str
    member_role: str
    member_order: int | None
    canonical_id: str | None = None
    canonical_url: str | None = None


@dataclass(slots=True, frozen=True)
class SnapshotRecord:
    snapshot_id: UUID
    artifact_id: UUID
    provider: str
    snapshot_type: str
    status: str
    fetched_at: datetime | None
    content_anchor: str
    normalized_projection: dict[str, Any] | None = None
    evidence_limitations: list[str] = field(default_factory=list)
    fetch_anomalies: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ExistingBundleRecord:
    bundle_id: UUID
    candidate_group_id: UUID
    bundle_version: int
    bundle_profile_version: str
    bundle_input_hash: str
    ready_for_analysis: bool


@dataclass(slots=True, frozen=True)
class DiscoveredLinkSummary:
    observed_url: str
    context_path: str | None
    discovery_reason: str
    parent_artifact_id: UUID
    parent_snapshot_id: UUID


@dataclass(slots=True, frozen=True)
class TextIdeaSnapshotDraft:
    artifact_id: UUID
    source_message_id: UUID
    source_version_no: int
    hash_surface: str
    display_surface: str
    dev_context_signals_json: dict[str, Any]
    status: SnapshotStatus
    evidence_limitations: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class RerootDecision:
    changed: bool
    from_artifact_id: UUID
    to_artifact_id: UUID
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class BundleMemberDraft:
    artifact_id: UUID
    snapshot_id: UUID
    member_role: str
    member_order: int | None


@dataclass(slots=True, frozen=True)
class EvidenceBundleDraft:
    candidate_group_id: UUID
    initial_primary_artifact_id: UUID
    current_primary_artifact_id: UUID
    bundle_profile_version: str
    bundle_input_hash: str
    reroot_count: int
    primary_summary: dict[str, Any]
    supporting_summaries_json: list[dict[str, Any]]
    discovered_links_summary_json: list[dict[str, Any]]
    evidence_limitations: list[str]
    ready_for_analysis: bool
    token_budget_profile: str
    members: list[BundleMemberDraft]
    judge_profile: str | None = None


@dataclass(slots=True, frozen=True)
class AssemblyResult:
    candidate_group_id: UUID
    bundle_id: UUID | None
    reused_existing_bundle: bool
    ready_for_analysis: bool
    emitted_analysis_requested: bool
