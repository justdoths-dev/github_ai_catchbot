from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


ArtifactType = Literal["web_article"]
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
    provider: str
    snapshot_type: str
    status: str
    fetched_at: datetime
    content_anchor: str
    normalized_projection: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class FetchedDocument:
    requested_url: str
    final_url: str
    status_code: int
    content_type: str | None
    body_bytes: bytes
    body_text: str
    response_headers_subset: dict[str, str]
    content_hash: str
    fetch_anomalies: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class DiscoveredUrlObservationDraft:
    parent_candidate_group_id: UUID
    parent_artifact_id: UUID
    observed_url: str
    context_path: str
    discovery_reason: str
    depth_remaining: int = 0


@dataclass(slots=True, frozen=True)
class WebArticleSnapshotDraft:
    snapshot_type: str
    status: SnapshotStatus
    content_anchor: str
    auth_mode: str
    normalized_projection: dict[str, Any] | None
    raw_payload_ref: str | None
    evidence_limitations: list[str]
    fetch_anomalies: list[str]
    final_url: str
    canonical_url_candidate: str | None
    site_name: str | None
    title: str | None
    description: str | None
    author: str | None
    published_at: datetime | None
    content_hash: str
    main_text_excerpt: str | None
    outbound_links_json: list[dict[str, Any]]
    discovered_urls: list[DiscoveredUrlObservationDraft] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class EnrichmentResult:
    artifact_id: UUID
    snapshot_id: UUID | None
    status: SnapshotStatus
    content_anchor: str | None
    emitted_snapshot_updated: bool
