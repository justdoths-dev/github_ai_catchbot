from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import CanonicalArtifact, OutboxEventRow, SourceMessageSnapshot, TriggerEvaluation


class AsyncSessionLike(Protocol):
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class RouterNormalizerRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    async def get_outbox_event(self, event_id: UUID) -> OutboxEventRow | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    event_id,
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status,
                    created_at
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(event_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = _json_loads(row["payload_json"]) or {}
        return OutboxEventRow(
            event_id=row["event_id"],
            event_type=row["event_type"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            dedupe_key=row["dedupe_key"],
            payload_json=payload,
            status=str(row["status"]),
            created_at=row["created_at"],
        )

    async def get_current_source_message(self, source_message_id: UUID) -> SourceMessageSnapshot | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    source_message_id,
                    current_version_no,
                    text_body,
                    caption_text,
                    text_surface,
                    entities_json,
                    url_surface_json,
                    raw_message_json,
                    deleted_at
                FROM source_messages
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                """
            ),
            {"source_message_id": str(source_message_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return SourceMessageSnapshot(
            source_message_id=row["source_message_id"],
            source_version_no=int(row["current_version_no"]),
            text_body=row["text_body"],
            caption_text=row["caption_text"],
            text_surface=row["text_surface"],
            entities_json=_json_loads(row["entities_json"]),
            url_surface_json=_json_loads(row["url_surface_json"]),
            raw_message_json=_json_loads(row["raw_message_json"]) or {},
            deleted_at=row["deleted_at"],
        )

    async def get_source_message_version(
        self,
        *,
        source_message_id: UUID,
        version_no: int,
    ) -> SourceMessageSnapshot | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    source_message_id,
                    version_no,
                    text_surface,
                    entities_json,
                    raw_message_json
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND version_no = :version_no
                """
            ),
            {"source_message_id": str(source_message_id), "version_no": version_no},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return SourceMessageSnapshot(
            source_message_id=row["source_message_id"],
            source_version_no=int(row["version_no"]),
            text_body=None,
            caption_text=None,
            text_surface=row["text_surface"],
            entities_json=_json_loads(row["entities_json"]),
            url_surface_json=None,
            raw_message_json=_json_loads(row["raw_message_json"]) or {},
        )

    async def upsert_normalization_run(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        normalizer_version: str,
        evaluation: TriggerEvaluation,
        result_hash: str,
    ) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO normalization_runs (
                    source_message_id,
                    source_version_no,
                    normalizer_version,
                    signal_detected,
                    candidate_eligible,
                    trigger_strength,
                    result_hash,
                    completed_at
                )
                VALUES (
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    :normalizer_version,
                    :signal_detected,
                    :candidate_eligible,
                    :trigger_strength,
                    :result_hash,
                    now()
                )
                ON CONFLICT (source_message_id, source_version_no, normalizer_version)
                DO UPDATE SET
                    signal_detected = EXCLUDED.signal_detected,
                    candidate_eligible = EXCLUDED.candidate_eligible,
                    trigger_strength = EXCLUDED.trigger_strength,
                    result_hash = EXCLUDED.result_hash,
                    completed_at = now()
                RETURNING normalization_run_id
                """
            ),
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
                "normalizer_version": normalizer_version,
                "signal_detected": evaluation.signal_detected,
                "candidate_eligible": evaluation.candidate_eligible,
                "trigger_strength": evaluation.trigger_strength,
                "result_hash": result_hash,
            },
        )
        return result.scalar_one()

    async def insert_suppression_trace(
        self,
        *,
        normalization_run_id: UUID,
        reason_code: str,
        trigger_strength: str | None,
        notes_json: dict[str, Any] | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO normalization_suppression_traces (
                    normalization_run_id,
                    reason_code,
                    trigger_strength,
                    notes_json
                )
                SELECT
                    CAST(:normalization_run_id AS uuid),
                    :reason_code,
                    :trigger_strength,
                    CAST(:notes_json AS jsonb)
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM normalization_suppression_traces
                    WHERE normalization_run_id = CAST(:normalization_run_id AS uuid)
                      AND reason_code = :reason_code
                )
                """
            ),
            {
                "normalization_run_id": str(normalization_run_id),
                "reason_code": reason_code,
                "trigger_strength": trigger_strength,
                "notes_json": _jsonb_dumps(notes_json),
            },
        )

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_registry (
                    artifact_type,
                    canonical_id,
                    canonical_url,
                    normalized_host,
                    artifact_key_json,
                    current_status,
                    created_at,
                    updated_at
                )
                VALUES (
                    CAST(:artifact_type AS artifact_type_enum),
                    :canonical_id,
                    :canonical_url,
                    :normalized_host,
                    CAST(:artifact_key_json AS jsonb),
                    NULL,
                    now(),
                    now()
                )
                ON CONFLICT (canonical_id)
                DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    normalized_host = EXCLUDED.normalized_host,
                    artifact_key_json = EXCLUDED.artifact_key_json,
                    updated_at = now()
                RETURNING artifact_id
                """
            ),
            {
                "artifact_type": artifact.artifact_type,
                "canonical_id": artifact.canonical_id,
                "canonical_url": artifact.canonical_url,
                "normalized_host": artifact.normalized_host,
                "artifact_key_json": _jsonb_dumps(artifact.artifact_key_json),
            },
        )
        return result.scalar_one()

    async def insert_artifact_observation_if_absent(
        self,
        *,
        artifact_id: UUID,
        source_message_id: UUID,
        source_version_no: int,
        artifact: CanonicalArtifact,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_observations (
                    artifact_id,
                    source_message_id,
                    source_version_no,
                    observed_url,
                    source_kind,
                    normalized_url,
                    resolved_url,
                    canonical_url,
                    classification,
                    context_path
                )
                SELECT
                    CAST(:artifact_id AS uuid),
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    :observed_url,
                    :source_kind,
                    :normalized_url,
                    :resolved_url,
                    :canonical_url,
                    :classification,
                    :context_path
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM artifact_observations
                    WHERE artifact_id = CAST(:artifact_id AS uuid)
                      AND source_message_id = CAST(:source_message_id AS uuid)
                      AND source_version_no = :source_version_no
                      AND COALESCE(observed_url, '') = COALESCE(:observed_url, '')
                )
                """
            ),
            {
                "artifact_id": str(artifact_id),
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
                "observed_url": artifact.observed_url,
                "source_kind": artifact.source_kind,
                "normalized_url": artifact.normalized_url,
                "resolved_url": artifact.resolved_url,
                "canonical_url": artifact.canonical_url,
                "classification": artifact.classification,
                "context_path": artifact.context_path,
            },
        )

    async def upsert_candidate_group(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        primary_artifact_id: UUID,
        normalizer_version: str,
        dedupe_subject_key: str,
    ) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_group_proposals (
                    source_message_id,
                    source_version_no,
                    initial_primary_artifact_id,
                    current_primary_artifact_id,
                    proposal_status,
                    normalizer_version,
                    dedupe_subject_key,
                    created_at,
                    updated_at
                )
                VALUES (
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    CAST(:primary_artifact_id AS uuid),
                    CAST(:primary_artifact_id AS uuid),
                    'proposed',
                    :normalizer_version,
                    :dedupe_subject_key,
                    now(),
                    now()
                )
                ON CONFLICT (source_message_id, source_version_no, dedupe_subject_key)
                DO UPDATE SET
                    current_primary_artifact_id = EXCLUDED.current_primary_artifact_id,
                    normalizer_version = EXCLUDED.normalizer_version,
                    updated_at = now()
                RETURNING candidate_group_id
                """
            ),
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
                "primary_artifact_id": str(primary_artifact_id),
                "normalizer_version": normalizer_version,
                "dedupe_subject_key": dedupe_subject_key,
            },
        )
        return result.scalar_one()

    async def upsert_candidate_member(
        self,
        *,
        candidate_group_id: UUID,
        artifact_id: UUID,
        member_role: str,
        member_order: int,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_group_members (
                    candidate_group_id,
                    artifact_id,
                    member_role,
                    member_order
                )
                VALUES (
                    CAST(:candidate_group_id AS uuid),
                    CAST(:artifact_id AS uuid),
                    :member_role,
                    :member_order
                )
                ON CONFLICT (candidate_group_id, artifact_id, member_role)
                DO UPDATE SET member_order = EXCLUDED.member_order
                """
            ),
            {
                "candidate_group_id": str(candidate_group_id),
                "artifact_id": str(artifact_id),
                "member_role": member_role,
                "member_order": member_order,
            },
        )

    async def insert_enrichment_requested_outbox(
        self,
        *,
        artifact_id: UUID,
        artifact: CanonicalArtifact,
        source_message_id: UUID,
        source_version_no: int,
    ) -> None:
        if artifact.provider_route is None:
            return
        dedupe_key = f"artifact:enrich:{artifact.canonical_id}:{source_message_id}:{source_version_no}"
        payload = {
            "artifact_id": str(artifact_id),
            "artifact_type": artifact.artifact_type,
            "canonical_id": artifact.canonical_id,
            "provider_route": artifact.provider_route,
            "source_message_id": str(source_message_id),
            "source_version_no": source_version_no,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status,
                    created_at
                )
                VALUES (
                    'artifact.enrich.requested.v1',
                    'artifact',
                    CAST(:artifact_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "artifact_id": str(artifact_id),
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload),
            },
        )


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
