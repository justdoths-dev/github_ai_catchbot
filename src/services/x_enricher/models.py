from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


ArtifactType = Literal["x_post"]
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
class ArtifactEnrichmentJob:
    trigger_event_id: UUID
    event_type: str
    candidate_group_id: UUID
    artifact_id: UUID
    artifact_type: ArtifactType
    provider_route: str
    refresh_mode: str
    depth_budget: int
    requested_at: datetime


@dataclass(slots=True, frozen=True)
class ArtifactRecord:
    artifact_id: UUID
    artifact_type: str
    canonical_id: str
    canonical_url: str | None
    normalized_host: str | None
    artifact_key_json: dict[str, Any] | None
    current_snapshot_id: UUID | None
    current_status: str | None


@dataclass(slots=True, frozen=True)
class CurrentSnapshotRef:
    snapshot_id: UUID
    status: str
    fetched_at: datetime
    content_anchor: str
    normalized_projection: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class XApiRequestProfile:
    tweet_fields: tuple[str, ...]
    expansions: tuple[str, ...]
    user_fields: tuple[str, ...]
    media_fields: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class DiscoveredUrlObservationDraft:
    parent_candidate_group_id: UUID
    parent_artifact_id: UUID
    observed_url: str
    context_path: str
    discovery_reason: str
    depth_remaining: int = 0


@dataclass(slots=True, frozen=True)
class XPostSnapshotDraft:
    snapshot_type: str
    status: SnapshotStatus
    content_anchor: str
    auth_mode: str
    normalized_projection: dict[str, Any] | None
    raw_payload_ref: str | None
    evidence_limitations: list[str]
    fetch_anomalies: list[str]
    post_id: str
    content_anchor_post_version: str
    author_summary_json: dict[str, Any] | None
    text_full: str | None
    text_excerpt: str | None
    conversation_id: str | None
    referenced_post_ids_json: list[str]
    discovered_links_json: list[dict[str, Any]]
    media_summary_json: list[dict[str, Any]]
    metrics_summary_json: dict[str, Any] | None
    discovered_urls: list[DiscoveredUrlObservationDraft] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class EnrichmentResult:
    artifact_id: UUID
    snapshot_id: UUID | None
    status: SnapshotStatus
    content_anchor: str | None
    emitted_snapshot_updated: bool
