from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import (
    ArtifactEnrichmentJob,
    ArtifactRecord,
    CurrentSnapshotRef,
    DiscoveredUrlObservationDraft,
    WebArticleSnapshotDraft,
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class WebEnricherRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> ArtifactEnrichmentJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, payload_json, created_at
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None or row["event_type"] != "artifact.enrich.requested.v1":
            return None
        payload = _json_loads(row["payload_json"]) or {}
        required = ["candidate_group_id", "artifact_id", "artifact_type", "provider_route", "refresh_mode", "depth_budget"]
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError(f"artifact.enrich.requested.v1 missing required fields: {', '.join(missing)}")
        if payload["provider_route"] != "web":
            return None
        return ArtifactEnrichmentJob(
            trigger_event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            candidate_group_id=UUID(str(payload["candidate_group_id"])),
            artifact_id=UUID(str(payload["artifact_id"])),
            artifact_type=str(payload["artifact_type"]),  # type: ignore[arg-type]
            provider_route=str(payload["provider_route"]),
            refresh_mode=str(payload["refresh_mode"]),
            depth_budget=int(payload["depth_budget"]),
            requested_at=_parse_requested_at(payload.get("requested_at"), row["created_at"]),
        )

    async def load_artifact(self, artifact_id: UUID) -> ArtifactRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT artifact_id, artifact_type, canonical_id, canonical_url,
                       normalized_host, artifact_key_json, current_snapshot_id,
                       current_status
                FROM artifact_registry
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": str(artifact_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ArtifactRecord(
            artifact_id=UUID(str(row["artifact_id"])),
            artifact_type=str(row["artifact_type"]),
            canonical_id=str(row["canonical_id"]),
            canonical_url=row["canonical_url"],
            normalized_host=row["normalized_host"],
            artifact_key_json=_json_loads(row["artifact_key_json"]),
            current_snapshot_id=UUID(str(row["current_snapshot_id"])) if row["current_snapshot_id"] else None,
            current_status=str(row["current_status"]) if row["current_status"] else None,
        )

    async def load_current_snapshot(self, snapshot_id: UUID | None) -> CurrentSnapshotRef | None:
        if snapshot_id is None:
            return None
        result = await self._session.execute(
            sa.text(
                """
                SELECT snapshot_id, provider, snapshot_type, status, fetched_at,
                       content_anchor, normalized_projection
                FROM artifact_snapshots
                WHERE snapshot_id = CAST(:snapshot_id AS uuid)
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CurrentSnapshotRef(
            snapshot_id=UUID(str(row["snapshot_id"])),
            provider=str(row["provider"]),
            snapshot_type=str(row["snapshot_type"]),
            status=str(row["status"]),
            fetched_at=row["fetched_at"],
            content_anchor=str(row["content_anchor"]),
            normalized_projection=_json_loads(row["normalized_projection"]),
        )

    async def insert_enrichment_run_if_absent(
        self,
        *,
        artifact_id: UUID,
        refresh_mode: str,
        depth_budget: int,
        status: str,
        job_idempotency_key: str,
        content_anchor: str | None = None,
    ) -> UUID | None:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_enrichment_runs (
                    artifact_id, provider, refresh_mode, depth_budget, status,
                    content_anchor, job_idempotency_key, requested_at
                )
                VALUES (
                    CAST(:artifact_id AS uuid), 'web', :refresh_mode, :depth_budget,
                    CAST(:status AS snapshot_status_enum), :content_anchor,
                    :job_idempotency_key, now()
                )
                ON CONFLICT (job_idempotency_key) DO NOTHING
                RETURNING artifact_enrichment_run_id
                """
            ),
            {
                "artifact_id": str(artifact_id),
                "refresh_mode": refresh_mode,
                "depth_budget": depth_budget,
                "status": status,
                "content_anchor": content_anchor,
                "job_idempotency_key": job_idempotency_key,
            },
        )
        row = result.scalar_one_or_none()
        return UUID(str(row)) if row else None

    async def mark_enrichment_run_started(self, run_id: UUID) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_enrichment_runs
                SET status = 'fetching'::snapshot_status_enum,
                    started_at = now()
                WHERE artifact_enrichment_run_id = CAST(:run_id AS uuid)
                """
            ),
            {"run_id": str(run_id)},
        )

    async def mark_enrichment_run_finished(self, *, run_id: UUID, status: str, content_anchor: str | None) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_enrichment_runs
                SET status = CAST(:status AS snapshot_status_enum),
                    content_anchor = :content_anchor,
                    finished_at = now()
                WHERE artifact_enrichment_run_id = CAST(:run_id AS uuid)
                """
            ),
            {"run_id": str(run_id), "status": status, "content_anchor": content_anchor},
        )

    async def insert_snapshot(self, *, artifact_id: UUID, draft: WebArticleSnapshotDraft) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshots (
                    artifact_id, provider, snapshot_type, status, fetched_at,
                    content_anchor, auth_mode, normalized_projection, raw_payload_ref,
                    evidence_limitations, fetch_anomalies
                )
                VALUES (
                    CAST(:artifact_id AS uuid), 'web', 'web_article',
                    CAST(:status AS snapshot_status_enum), now(), :content_anchor,
                    :auth_mode, CAST(:normalized_projection AS jsonb), :raw_payload_ref,
                    CAST(:evidence_limitations AS jsonb), CAST(:fetch_anomalies AS jsonb)
                )
                ON CONFLICT ON CONSTRAINT uq_artifact_snapshots_artifact_provider_anchor_type
                DO UPDATE SET status = artifact_snapshots.status
                RETURNING snapshot_id
                """
            ),
            {
                "artifact_id": str(artifact_id),
                "status": draft.status,
                "content_anchor": draft.content_anchor,
                "auth_mode": draft.auth_mode,
                "normalized_projection": _jsonb_dumps(draft.normalized_projection),
                "raw_payload_ref": draft.raw_payload_ref,
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "fetch_anomalies": _jsonb_dumps(draft.fetch_anomalies),
            },
        )
        return UUID(str(result.scalar_one()))

    async def upsert_web_article_child(self, *, snapshot_id: UUID, draft: WebArticleSnapshotDraft) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshot_web_article (
                    snapshot_id, final_url, canonical_url_candidate, site_name,
                    title, description, author, published_at, content_hash,
                    main_text_excerpt, outbound_links_json
                )
                VALUES (
                    CAST(:snapshot_id AS uuid), :final_url, :canonical_url_candidate,
                    :site_name, :title, :description, :author, :published_at,
                    :content_hash, :main_text_excerpt, CAST(:outbound_links_json AS jsonb)
                )
                ON CONFLICT (snapshot_id) DO NOTHING
                """
            ),
            {
                "snapshot_id": str(snapshot_id),
                "final_url": draft.final_url,
                "canonical_url_candidate": draft.canonical_url_candidate,
                "site_name": draft.site_name,
                "title": draft.title,
                "description": draft.description,
                "author": draft.author,
                "published_at": draft.published_at,
                "content_hash": draft.content_hash,
                "main_text_excerpt": draft.main_text_excerpt,
                "outbound_links_json": _jsonb_dumps(draft.outbound_links_json),
            },
        )

    async def insert_discovered_url(self, *, snapshot_id: UUID, draft: DiscoveredUrlObservationDraft) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO discovered_url_observations (
                    parent_candidate_group_id, parent_artifact_id, parent_snapshot_id,
                    observed_url, context_path, discovery_reason, depth_remaining, created_at
                )
                VALUES (
                    CAST(:parent_candidate_group_id AS uuid),
                    CAST(:parent_artifact_id AS uuid),
                    CAST(:parent_snapshot_id AS uuid),
                    :observed_url, :context_path, :discovery_reason, :depth_remaining, now()
                )
                """
            ),
            {
                "parent_candidate_group_id": str(draft.parent_candidate_group_id),
                "parent_artifact_id": str(draft.parent_artifact_id),
                "parent_snapshot_id": str(snapshot_id),
                "observed_url": draft.observed_url,
                "context_path": draft.context_path,
                "discovery_reason": draft.discovery_reason,
                "depth_remaining": draft.depth_remaining,
            },
        )

    async def update_artifact_current_snapshot(self, *, artifact_id: UUID, snapshot_id: UUID, status: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE artifact_registry
                SET current_snapshot_id = CAST(:snapshot_id AS uuid),
                    current_status = CAST(:status AS snapshot_status_enum),
                    updated_at = now()
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id), "status": status},
        )

    async def insert_snapshot_updated_outbox(
        self,
        *,
        artifact_id: UUID,
        candidate_group_id: UUID,
        snapshot_id: UUID,
        status: str,
        content_anchor: str,
    ) -> None:
        payload = {
            "artifact_id": str(artifact_id),
            "candidate_group_id": str(candidate_group_id),
            "snapshot_id": str(snapshot_id),
            "provider": "web",
            "provider_route": "web",
            "snapshot_type": "web_article",
            "status": status,
            "content_anchor": content_anchor,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                )
                VALUES (
                    'artifact.snapshot.updated.v1', 'artifact',
                    CAST(:artifact_id AS uuid), :dedupe_key,
                    CAST(:payload_json AS jsonb), 'pending'::outbox_status_enum, now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "artifact_id": str(artifact_id),
                "dedupe_key": f"artifact:snapshot_updated:{artifact_id}:{snapshot_id}",
                "payload_json": _jsonb_dumps(payload),
            },
        )


def _parse_requested_at(raw_value: Any, fallback: Any) -> datetime:
    value = raw_value if raw_value is not None else fallback
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.now(timezone.utc)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _jsonb_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")
