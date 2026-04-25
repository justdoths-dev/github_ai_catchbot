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
    "x_post",
    "web_article",
    "text_idea",
    "unknown_link",
    "short_url_unresolved",
]

TriggerStrength = Literal["strong", "medium", "weak"]


@dataclass(slots=True, frozen=True)
class RedisNormalizeMessage:
    job_id: str
    stage_name: str
    root_object_type: str
    root_object_id: str
    idempotency_key: str
    trigger_event_id: str
    pipeline_run_id: str | None = None
    not_before: str | None = None

    @classmethod
    def from_stream_fields(cls, fields: dict[str, Any]) -> "RedisNormalizeMessage":
        return cls(
            job_id=str(fields.get("job_id", "")),
            stage_name=str(fields.get("stage_name", "")),
            root_object_type=str(fields.get("root_object_type", "")),
            root_object_id=str(fields.get("root_object_id", "")),
            idempotency_key=str(fields.get("idempotency_key", "")),
            trigger_event_id=str(fields.get("trigger_event_id", "")),
            pipeline_run_id=_optional_str(fields.get("pipeline_run_id")),
            not_before=_optional_str(fields.get("not_before")),
        )


@dataclass(slots=True, frozen=True)
class OutboxEventRow:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class SourceMessageSnapshot:
    source_message_id: UUID
    source_version_no: int
    text_body: str | None
    caption_text: str | None
    text_surface: str | None
    entities_json: list[dict[str, Any]] | None
    url_surface_json: list[dict[str, Any]] | None
    raw_message_json: dict[str, Any]
    deleted_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class TextSurfaces:
    raw_text_surface: str
    keyword_scan_surface: str
    hash_surface: str
    display_surface: str


@dataclass(slots=True, frozen=True)
class ExtractedUrl:
    observed_url: str
    source_kind: str
    context_path: str | None = None


@dataclass(slots=True, frozen=True)
class ResolvedUrl:
    observed_url: str
    normalized_url: str
    resolved_url: str | None
    source_kind: str
    context_path: str | None = None
    resolution_status: str = "not_short_url"


@dataclass(slots=True, frozen=True)
class CanonicalArtifact:
    artifact_type: ArtifactType
    canonical_id: str
    canonical_url: str | None
    normalized_host: str | None
    artifact_key_json: dict[str, Any]
    observed_url: str | None = None
    normalized_url: str | None = None
    resolved_url: str | None = None
    source_kind: str = "derived"
    context_path: str | None = None
    classification: str | None = None
    provider_route: str | None = None
    inferred_repo: "CanonicalArtifact | None" = None


@dataclass(slots=True, frozen=True)
class TriggerEvaluation:
    signal_detected: bool
    candidate_eligible: bool
    trigger_strength: TriggerStrength | None
    reason_codes: list[str] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class PersistedArtifact:
    artifact_id: UUID
    canonical: CanonicalArtifact


@dataclass(slots=True, frozen=True)
class NormalizationResult:
    normalization_run_id: UUID
    signal_detected: bool
    candidate_eligible: bool
    trigger_strength: TriggerStrength | None
    artifact_count: int
    candidate_group_count: int
    suppression_reason_codes: list[str]


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
