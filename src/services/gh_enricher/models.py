from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


ArtifactType = Literal[
    "github_repo",
    "github_subpath",
    "github_repo_page",
    "github_gist",
]

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

AuthMode = Literal["app_installation", "anonymous_degraded"]


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
class EnrichmentRunRef:
    run_id: UUID
    status: str


@dataclass(slots=True, frozen=True)
class GitHubArtifactLocator:
    artifact_type: str
    owner: str | None = None
    repo: str | None = None
    ref: str | None = None
    path: str | None = None
    page_path: str | None = None
    gist_id: str | None = None


@dataclass(slots=True, frozen=True)
class GitHubFileSample:
    path: str
    role: str
    size_bytes: int | None
    content_hash: str | None
    excerpt: str | None
    raw_blob_ref: str | None = None


@dataclass(slots=True, frozen=True)
class GitHubRepoProjection:
    repo_full_name: str
    default_branch: str | None
    resolved_ref: str | None
    content_anchor_commit_sha: str | None
    repo_flags_json: dict[str, Any] | None
    license_spdx: str | None
    topics_json: list[str] | None
    readme_excerpt: str | None
    detected_build_systems_json: list[str] | None
    detected_languages_json: list[str] | None
    key_paths_json: list[str] | None
    test_paths_json: list[str] | None
    ci_paths_json: list[str] | None
    examples_paths_json: list[str] | None
    docs_paths_json: list[str] | None
    release_summary_json: dict[str, Any] | None
    normalized_projection: dict[str, Any] | None = None
    sampled_files: list[GitHubFileSample] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class DiscoveredUrlObservationDraft:
    parent_candidate_group_id: UUID
    parent_artifact_id: UUID
    observed_url: str
    context_path: str
    discovery_reason: str
    depth_remaining: int = 0


@dataclass(slots=True, frozen=True)
class SnapshotWritePlan:
    snapshot_type: str
    status: SnapshotStatus
    content_anchor: str
    auth_mode: AuthMode
    normalized_projection: dict[str, Any] | None
    raw_payload_ref: str | None
    evidence_limitations: list[str]
    fetch_anomalies: list[str]
    repo_child: GitHubRepoProjection | None = None
    discovered_urls: list[DiscoveredUrlObservationDraft] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class EnrichmentResult:
    artifact_id: UUID
    snapshot_id: UUID | None
    status: SnapshotStatus
    content_anchor: str | None
    emitted_snapshot_updated: bool
    error_code: str | None = None
