from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Iterable, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import (
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    DiscoveredLinkSummary,
    EvidenceBundleDraft,
    ExistingBundleRecord,
    SnapshotRecord,
    TextIdeaSnapshotDraft,
    TriggerEventRecord,
)
from .text_idea_builder import TextIdeaBuilder


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class EvidenceAssemblerRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_trigger_event(self, trigger_event_id: UUID) -> TriggerEventRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, aggregate_id, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return TriggerEventRecord(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            aggregate_id=UUID(str(row["aggregate_id"])),
            payload_json=_json_loads(row["payload_json"]) or {},
        )

    async def resolve_refresh_targets(self, trigger_event_id: UUID) -> list[BundleRefreshTarget]:
        event = await self.load_trigger_event(trigger_event_id)
        if event is None:
            return []
        payload = event.payload_json

        if event.event_type == "candidate.bundle.refresh.v1":
            raw_candidate_group_id = payload.get("candidate_group_id") or str(event.aggregate_id)
            return [
                BundleRefreshTarget(
                    candidate_group_id=UUID(str(raw_candidate_group_id)),
                    trigger_event_id=event.event_id,
                    trigger_event_type=event.event_type,
                    trigger_artifact_id=_uuid_or_none(payload.get("artifact_id") or payload.get("trigger_object_id")),
                    trigger_snapshot_id=_uuid_or_none(payload.get("snapshot_id")),
                )
            ]

        if event.event_type == "artifact.snapshot.updated.v1":
            raw_artifact_id = payload.get("artifact_id") or str(event.aggregate_id)
            artifact_id = UUID(str(raw_artifact_id))
            result = await self._session.execute(
                sa.text(
                    """
                    SELECT DISTINCT candidate_group_id
                    FROM candidate_group_members
                    WHERE artifact_id = CAST(:artifact_id AS uuid)
                    ORDER BY candidate_group_id
                    """
                ),
                {"artifact_id": str(artifact_id)},
            )
            return [
                BundleRefreshTarget(
                    candidate_group_id=UUID(str(row["candidate_group_id"])),
                    trigger_event_id=event.event_id,
                    trigger_event_type=event.event_type,
                    trigger_artifact_id=artifact_id,
                    trigger_snapshot_id=_uuid_or_none(payload.get("snapshot_id")),
                )
                for row in result.mappings().all()
            ]

        return []

    async def load_candidate_group(self, candidate_group_id: UUID) -> CandidateGroupRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT candidate_group_id, source_message_id, source_version_no,
                       initial_primary_artifact_id, current_primary_artifact_id,
                       proposal_status, current_bundle_id
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CandidateGroupRecord(
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            source_message_id=UUID(str(row["source_message_id"])),
            source_version_no=int(row["source_version_no"]),
            initial_primary_artifact_id=UUID(str(row["initial_primary_artifact_id"])),
            current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
            proposal_status=str(row["proposal_status"]),
            current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
        )

    async def load_candidate_members(self, candidate_group_id: UUID) -> list[CandidateMemberRecord]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT cgm.artifact_id, ar.artifact_type, cgm.member_role, cgm.member_order
                FROM candidate_group_members cgm
                JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                ORDER BY
                    CASE cgm.member_role
                        WHEN 'primary' THEN 0
                        WHEN 'supporting' THEN 1
                        WHEN 'inferred_anchor' THEN 2
                        ELSE 9
                    END,
                    cgm.member_order NULLS LAST,
                    cgm.artifact_id
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        return [
            CandidateMemberRecord(
                artifact_id=UUID(str(row["artifact_id"])),
                artifact_type=str(row["artifact_type"]),
                member_role=str(row["member_role"]),
                member_order=int(row["member_order"]) if row["member_order"] is not None else None,
            )
            for row in result.mappings().all()
        ]

    async def load_current_snapshots(self, artifact_ids: Iterable[UUID]) -> dict[UUID, SnapshotRecord]:
        ids = [str(artifact_id) for artifact_id in artifact_ids]
        if not ids:
            return {}
        result = await self._session.execute(
            sa.text(
                """
                SELECT ar.artifact_id, s.snapshot_id, s.provider, s.snapshot_type, s.status,
                       s.fetched_at, s.content_anchor, s.normalized_projection,
                       s.evidence_limitations, s.fetch_anomalies
                FROM artifact_registry ar
                JOIN artifact_snapshots s ON s.snapshot_id = ar.current_snapshot_id
                WHERE ar.artifact_id = ANY(CAST(:artifact_ids AS uuid[]))
                """
            ),
            {"artifact_ids": ids},
        )
        snapshots: dict[UUID, SnapshotRecord] = {}
        for row in result.mappings().all():
            artifact_id = UUID(str(row["artifact_id"]))
            snapshots[artifact_id] = SnapshotRecord(
                snapshot_id=UUID(str(row["snapshot_id"])),
                artifact_id=artifact_id,
                provider=str(row["provider"]),
                snapshot_type=str(row["snapshot_type"]),
                status=str(row["status"]),
                fetched_at=row["fetched_at"],
                content_anchor=str(row["content_anchor"]),
                normalized_projection=_json_loads(row["normalized_projection"]),
                evidence_limitations=_json_loads(row["evidence_limitations"]) or [],
                fetch_anomalies=_json_loads(row["fetch_anomalies"]) or [],
            )
        return snapshots

    async def load_source_message_text_surface(self, *, source_message_id: UUID, source_version_no: int) -> str | None:
        version_result = await self._session.execute(
            sa.text(
                """
                SELECT text_surface
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND version_no = :source_version_no
                """
            ),
            {"source_message_id": str(source_message_id), "source_version_no": source_version_no},
        )
        version_row = version_result.mappings().first()
        if version_row is not None and version_row["text_surface"]:
            return str(version_row["text_surface"])

        current_result = await self._session.execute(
            sa.text(
                """
                SELECT text_surface
                FROM source_messages
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                """
            ),
            {"source_message_id": str(source_message_id)},
        )
        current_row = current_result.mappings().first()
        if current_row is None or not current_row["text_surface"]:
            return None
        return str(current_row["text_surface"])

    async def ensure_text_idea_snapshot(self, draft: TextIdeaSnapshotDraft) -> SnapshotRecord:
        content_anchor = TextIdeaBuilder.input_hash(draft)
        existing = await self._session.execute(
            sa.text(
                """
                SELECT snapshot_id, fetched_at, status, normalized_projection,
                       evidence_limitations, fetch_anomalies
                FROM artifact_snapshots
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                  AND provider = 'local_text_idea'
                  AND snapshot_type = 'text_idea'
                  AND content_anchor = :content_anchor
                ORDER BY fetched_at DESC
                LIMIT 1
                """
            ),
            {"artifact_id": str(draft.artifact_id), "content_anchor": content_anchor},
        )
        row = existing.mappings().first()
        if row is not None:
            return SnapshotRecord(
                snapshot_id=UUID(str(row["snapshot_id"])),
                artifact_id=draft.artifact_id,
                provider="local_text_idea",
                snapshot_type="text_idea",
                status=str(row["status"]),
                fetched_at=row["fetched_at"],
                content_anchor=content_anchor,
                normalized_projection=_json_loads(row["normalized_projection"])
                or _text_idea_projection(draft),
                evidence_limitations=_json_loads(row["evidence_limitations"]) or draft.evidence_limitations,
                fetch_anomalies=_json_loads(row["fetch_anomalies"]) or [],
            )

        projection = _text_idea_projection(draft)
        parent = await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshots (
                    artifact_id, provider, snapshot_type, status, fetched_at,
                    content_anchor, auth_mode, normalized_projection,
                    raw_payload_ref, evidence_limitations, fetch_anomalies
                ) VALUES (
                    CAST(:artifact_id AS uuid),
                    'local_text_idea',
                    'text_idea',
                    CAST(:status AS snapshot_status_enum),
                    now(),
                    :content_anchor,
                    'local_text_idea',
                    CAST(:normalized_projection AS jsonb),
                    NULL,
                    CAST(:evidence_limitations AS jsonb),
                    CAST(:fetch_anomalies AS jsonb)
                )
                ON CONFLICT (artifact_id, provider, content_anchor, snapshot_type)
                DO UPDATE SET artifact_id = EXCLUDED.artifact_id
                RETURNING snapshot_id, fetched_at
                """
            ),
            {
                "artifact_id": str(draft.artifact_id),
                "status": draft.status,
                "content_anchor": content_anchor,
                "normalized_projection": _jsonb_dumps(projection),
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "fetch_anomalies": _jsonb_dumps([]),
            },
        )
        parent_row = parent.mappings().first()
        if parent_row is None:
            raise RuntimeError("artifact_snapshots insert did not return a row")
        snapshot_id = UUID(str(parent_row["snapshot_id"]))
        await self._session.execute(
            sa.text(
                """
                INSERT INTO artifact_snapshot_text_idea (
                    snapshot_id, source_message_id, source_version_no,
                    hash_surface, display_surface, dev_context_signals_json
                ) VALUES (
                    CAST(:snapshot_id AS uuid),
                    CAST(:source_message_id AS uuid),
                    :source_version_no,
                    :hash_surface,
                    :display_surface,
                    CAST(:dev_context_signals_json AS jsonb)
                )
                ON CONFLICT (snapshot_id) DO NOTHING
                """
            ),
            {
                "snapshot_id": str(snapshot_id),
                "source_message_id": str(draft.source_message_id),
                "source_version_no": draft.source_version_no,
                "hash_surface": draft.hash_surface,
                "display_surface": draft.display_surface,
                "dev_context_signals_json": _jsonb_dumps(draft.dev_context_signals_json),
            },
        )
        return SnapshotRecord(
            snapshot_id=snapshot_id,
            artifact_id=draft.artifact_id,
            provider="local_text_idea",
            snapshot_type="text_idea",
            status=draft.status,
            fetched_at=parent_row["fetched_at"],
            content_anchor=content_anchor,
            normalized_projection=projection,
            evidence_limitations=draft.evidence_limitations,
            fetch_anomalies=[],
        )

    async def load_discovered_links(
        self,
        *,
        candidate_group_id: UUID,
        parent_artifact_ids: Iterable[UUID],
    ) -> list[DiscoveredLinkSummary]:
        ids = [str(artifact_id) for artifact_id in parent_artifact_ids]
        if not ids:
            return []
        result = await self._session.execute(
            sa.text(
                """
                SELECT observed_url, context_path, discovery_reason,
                       parent_artifact_id, parent_snapshot_id, created_at
                FROM discovered_url_observations
                WHERE parent_candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND parent_artifact_id = ANY(CAST(:artifact_ids AS uuid[]))
                ORDER BY created_at DESC
                """
            ),
            {"candidate_group_id": str(candidate_group_id), "artifact_ids": ids},
        )
        return filter_discovered_links(
            [
                DiscoveredLinkSummary(
                    observed_url=str(row["observed_url"]),
                    context_path=str(row["context_path"]) if row["context_path"] else None,
                    discovery_reason=str(row["discovery_reason"]),
                    parent_artifact_id=UUID(str(row["parent_artifact_id"])),
                    parent_snapshot_id=UUID(str(row["parent_snapshot_id"])),
                )
                for row in result.mappings().all()
            ],
            parent_artifact_ids={UUID(value) for value in ids},
        )

    async def count_reroot_events(self, candidate_group_id: UUID) -> int:
        result = await self._session.execute(
            sa.text(
                """
                SELECT COUNT(*)
                FROM candidate_reroot_events
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        return int(result.scalar_one())

    async def append_reroot_event(
        self,
        *,
        candidate_group_id: UUID,
        from_artifact_id: UUID,
        to_artifact_id: UUID,
        reason_code: str,
        trigger_snapshot_id: UUID | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_reroot_events (
                    candidate_group_id, from_artifact_id, to_artifact_id,
                    reason_code, trigger_snapshot_id, created_at
                ) VALUES (
                    CAST(:candidate_group_id AS uuid),
                    CAST(:from_artifact_id AS uuid),
                    CAST(:to_artifact_id AS uuid),
                    :reason_code,
                    CAST(:trigger_snapshot_id AS uuid),
                    now()
                )
                """
            ),
            {
                "candidate_group_id": str(candidate_group_id),
                "from_artifact_id": str(from_artifact_id),
                "to_artifact_id": str(to_artifact_id),
                "reason_code": reason_code,
                "trigger_snapshot_id": str(trigger_snapshot_id) if trigger_snapshot_id else None,
            },
        )

    async def update_current_primary(self, *, candidate_group_id: UUID, artifact_id: UUID) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE candidate_group_proposals
                SET current_primary_artifact_id = CAST(:artifact_id AS uuid), updated_at = now()
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id), "artifact_id": str(artifact_id)},
        )

    async def load_existing_bundle(
        self,
        *,
        candidate_group_id: UUID,
        bundle_profile_version: str,
        bundle_input_hash: str,
    ) -> ExistingBundleRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT bundle_id, candidate_group_id, bundle_version,
                       bundle_profile_version, bundle_input_hash, ready_for_analysis
                FROM candidate_evidence_bundles
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND bundle_profile_version = :bundle_profile_version
                  AND bundle_input_hash = :bundle_input_hash
                LIMIT 1
                """
            ),
            {
                "candidate_group_id": str(candidate_group_id),
                "bundle_profile_version": bundle_profile_version,
                "bundle_input_hash": bundle_input_hash,
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ExistingBundleRecord(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            bundle_version=int(row["bundle_version"]),
            bundle_profile_version=str(row["bundle_profile_version"]),
            bundle_input_hash=str(row["bundle_input_hash"]),
            ready_for_analysis=bool(row["ready_for_analysis"]),
        )

    async def next_bundle_version(self, candidate_group_id: UUID) -> int:
        result = await self._session.execute(
            sa.text(
                """
                SELECT COALESCE(MAX(bundle_version), 0) + 1
                FROM candidate_evidence_bundles
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        return int(result.scalar_one())

    async def append_bundle(self, *, draft: EvidenceBundleDraft, bundle_version: int) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO candidate_evidence_bundles (
                    candidate_group_id,
                    initial_primary_artifact_id,
                    current_primary_artifact_id,
                    bundle_version,
                    bundle_profile_version,
                    bundle_input_hash,
                    reroot_count,
                    primary_summary,
                    supporting_summaries_json,
                    discovered_links_summary_json,
                    evidence_limitations,
                    ready_for_analysis,
                    token_budget_profile,
                    created_at
                ) VALUES (
                    CAST(:candidate_group_id AS uuid),
                    CAST(:initial_primary_artifact_id AS uuid),
                    CAST(:current_primary_artifact_id AS uuid),
                    :bundle_version,
                    :bundle_profile_version,
                    :bundle_input_hash,
                    :reroot_count,
                    CAST(:primary_summary AS jsonb),
                    CAST(:supporting_summaries_json AS jsonb),
                    CAST(:discovered_links_summary_json AS jsonb),
                    CAST(:evidence_limitations AS jsonb),
                    :ready_for_analysis,
                    :token_budget_profile,
                    now()
                )
                RETURNING bundle_id
                """
            ),
            {
                "candidate_group_id": str(draft.candidate_group_id),
                "initial_primary_artifact_id": str(draft.initial_primary_artifact_id),
                "current_primary_artifact_id": str(draft.current_primary_artifact_id),
                "bundle_version": bundle_version,
                "bundle_profile_version": draft.bundle_profile_version,
                "bundle_input_hash": draft.bundle_input_hash,
                "reroot_count": draft.reroot_count,
                "primary_summary": _jsonb_dumps(draft.primary_summary),
                "supporting_summaries_json": _jsonb_dumps(draft.supporting_summaries_json),
                "discovered_links_summary_json": _jsonb_dumps(draft.discovered_links_summary_json),
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "ready_for_analysis": draft.ready_for_analysis,
                "token_budget_profile": draft.token_budget_profile,
            },
        )
        bundle_id = UUID(str(result.scalar_one()))
        for member in draft.members:
            await self._session.execute(
                sa.text(
                    """
                    INSERT INTO candidate_evidence_members (
                        candidate_evidence_member_id,
                        bundle_id,
                        artifact_id,
                        snapshot_id,
                        member_role,
                        member_order
                    ) VALUES (
                        gen_random_uuid(),
                        CAST(:bundle_id AS uuid),
                        CAST(:artifact_id AS uuid),
                        CAST(:snapshot_id AS uuid),
                        :member_role,
                        :member_order
                    )
                    """
                ),
                {
                    "bundle_id": str(bundle_id),
                    "artifact_id": str(member.artifact_id),
                    "snapshot_id": str(member.snapshot_id),
                    "member_role": member.member_role,
                    "member_order": member.member_order,
                },
            )
        return bundle_id

    async def update_current_bundle(self, *, candidate_group_id: UUID, bundle_id: UUID) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE candidate_group_proposals
                SET current_bundle_id = CAST(:bundle_id AS uuid), updated_at = now()
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id), "bundle_id": str(bundle_id)},
        )

    async def insert_analysis_requested_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        judge_profile: str,
        escalation_allowed: bool,
    ) -> None:
        payload = {
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
            "judge_profile": judge_profile,
            "escalation_allowed": escalation_allowed,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                ) VALUES (
                    'analysis.requested.v1',
                    'candidate_group',
                    CAST(:candidate_group_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "candidate_group_id": str(candidate_group_id),
                "dedupe_key": f"analysis-request:{candidate_group_id}:{bundle_id}",
                "payload_json": _jsonb_dumps(payload),
            },
        )


def filter_discovered_links(
    links: Iterable[DiscoveredLinkSummary],
    *,
    parent_artifact_ids: set[UUID],
) -> list[DiscoveredLinkSummary]:
    seen: set[tuple[UUID, str, str]] = set()
    filtered: list[DiscoveredLinkSummary] = []
    for link in links:
        if link.parent_artifact_id not in parent_artifact_ids:
            continue
        key = (link.parent_artifact_id, link.observed_url, link.context_path or "")
        if key in seen:
            continue
        seen.add(key)
        filtered.append(link)
    filtered.sort(key=lambda item: (str(item.parent_artifact_id), item.context_path or "", item.observed_url))
    return filtered


def _text_idea_projection(draft: TextIdeaSnapshotDraft) -> dict[str, Any]:
    return {
        "display_surface": draft.display_surface,
        "dev_context_signals_json": draft.dev_context_signals_json,
    }


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    return UUID(str(value))


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported json type: {type(value)!r}")


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _jsonb_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)
