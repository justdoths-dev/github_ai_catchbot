from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, CandidateRouteState


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class AnalysisRouterRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> AnalysisRequestedJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None or row["event_type"] != "analysis.requested.v1":
            return None

        payload = _json_loads(row["payload_json"]) or {}
        candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
        bundle_id = _uuid_or_none(payload.get("bundle_id"))
        if candidate_group_id is None or bundle_id is None:
            return None

        return AnalysisRequestedJob(
            trigger_event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
            judge_profile=str(payload["judge_profile"]) if payload.get("judge_profile") else None,
            escalation_allowed=bool(payload.get("escalation_allowed", False)),
        )

    async def load_candidate_route_state(self, candidate_group_id: UUID) -> CandidateRouteState | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT candidate_group_id, current_bundle_id
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CandidateRouteState(
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
        )

    async def load_bundle(self, bundle_id: UUID) -> BundleRouteRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT bundle_id, candidate_group_id, bundle_profile_version,
                       reroot_count, ready_for_analysis, token_budget_profile, created_at
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundleRouteRecord(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            bundle_profile_version=str(row["bundle_profile_version"]),
            reroot_count=int(row["reroot_count"]),
            ready_for_analysis=bool(row["ready_for_analysis"]),
            token_budget_profile=str(row["token_budget_profile"]) if row["token_budget_profile"] else None,
            created_at=row["created_at"],
        )

    async def load_bundle_shape_stats(self, bundle_id: UUID) -> BundleShapeStats:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    COUNT(*) AS member_count,
                    COUNT(*) FILTER (WHERE member_role = 'supporting') AS supporting_count
                FROM candidate_evidence_members
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        )
        row = result.mappings().one()
        return BundleShapeStats(
            member_count=int(row["member_count"]),
            supporting_count=int(row["supporting_count"]),
        )

    async def load_existing_judge_run(
        self,
        *,
        bundle_id: UUID,
        prompt_version: str,
        model: str,
        reasoning_effort: str,
    ) -> UUID | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id
                FROM judge_runs
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                  AND prompt_version = :prompt_version
                  AND model = :model
                  AND reasoning_effort = :reasoning_effort
                LIMIT 1
                """
            ),
            {
                "bundle_id": str(bundle_id),
                "prompt_version": prompt_version,
                "model": model,
                "reasoning_effort": reasoning_effort,
            },
        )
        row = result.scalar_one_or_none()
        return UUID(str(row)) if row else None

    async def get_or_create_judge_run(
        self,
        *,
        bundle_id: UUID,
        judge_profile: str,
        model: str,
        reasoning_effort: str,
        prompt_version: str,
        schema_version: str,
        policy_version: str,
        prompt_cache_key: str,
    ) -> tuple[UUID, bool]:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO judge_runs (
                    bundle_id,
                    judge_profile,
                    model,
                    reasoning_effort,
                    prompt_version,
                    schema_version,
                    policy_version,
                    prompt_cache_key,
                    status
                ) VALUES (
                    CAST(:bundle_id AS uuid),
                    :judge_profile,
                    :model,
                    :reasoning_effort,
                    :prompt_version,
                    :schema_version,
                    :policy_version,
                    :prompt_cache_key,
                    'pending'
                )
                ON CONFLICT ON CONSTRAINT uq_judge_runs_bundle_prompt_model_effort
                DO NOTHING
                RETURNING judge_run_id
                """
            ),
            {
                "bundle_id": str(bundle_id),
                "judge_profile": judge_profile,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "prompt_version": prompt_version,
                "schema_version": schema_version,
                "policy_version": policy_version,
                "prompt_cache_key": prompt_cache_key,
            },
        )
        judge_run_id = result.scalar_one_or_none()
        if judge_run_id:
            return UUID(str(judge_run_id)), True

        existing_judge_run_id = await self.load_existing_judge_run(
            bundle_id=bundle_id,
            prompt_version=prompt_version,
            model=model,
            reasoning_effort=reasoning_effort,
        )
        if existing_judge_run_id is None:
            raise RuntimeError("judge_run insert conflicted but existing judge_run was not found")
        return existing_judge_run_id, False

    async def insert_judge_call_requested_outbox(
        self,
        *,
        judge_run_id: UUID,
        bundle_id: UUID,
        model: str,
        reasoning_effort: str,
        prompt_version: str,
        prompt_cache_key: str,
    ) -> None:
        payload = {
            "judge_run_id": str(judge_run_id),
            "bundle_id": str(bundle_id),
            "model": model,
            "reasoning_effort": reasoning_effort,
            "prompt_version": prompt_version,
            "prompt_cache_key": prompt_cache_key,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                ) VALUES (
                    'judge.call.requested.v1',
                    'judge_run',
                    CAST(:judge_run_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "dedupe_key": f"judge-call:{judge_run_id}",
                "payload_json": _jsonb_dumps(payload),
            },
        )

    async def insert_bundle_refresh_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        refresh_reason: str,
    ) -> None:
        payload = {
            "candidate_group_id": str(candidate_group_id),
            "trigger_kind": "analysis_router_recheck",
            "trigger_object_type": "bundle",
            "trigger_object_id": str(bundle_id),
            "refresh_reason": refresh_reason,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                ) VALUES (
                    'candidate.bundle.refresh.v1',
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
                "dedupe_key": f"bundle-refresh:{candidate_group_id}:{bundle_id}:{refresh_reason}",
                "payload_json": _jsonb_dumps(payload),
            },
        )


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported json type: {type(value)!r}")


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)
