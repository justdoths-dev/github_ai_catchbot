from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class AnalysisValidatorRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeOutputReadyJob | None:
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
        if row is None or row["event_type"] != "judge.output.ready.v1":
            return None

        payload = _json_loads(row["payload_json"]) or {}
        judge_run_id = _uuid_or_none(payload.get("judge_run_id"))
        judge_output_id = _uuid_or_none(payload.get("judge_output_id"))
        if judge_run_id is None or judge_output_id is None:
            return None
        if "finish_reason" not in payload or "refusal_detected" not in payload:
            return None

        return JudgeOutputReadyJob(
            trigger_event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            finish_reason=_string_or_none(payload.get("finish_reason")),
            refusal_detected=_bool_value(payload.get("refusal_detected")),
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunValidationRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id, bundle_id, judge_profile, schema_version,
                       policy_version, status, finish_reason, refusal_detected
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunValidationRecord(
            judge_run_id=UUID(str(row["judge_run_id"])),
            bundle_id=UUID(str(row["bundle_id"])),
            judge_profile=str(row["judge_profile"]),
            schema_version=str(row["schema_version"]),
            policy_version=str(row["policy_version"]),
            status=str(row["status"]),
            finish_reason=_string_or_none(row["finish_reason"]),
            refusal_detected=bool(row["refusal_detected"]),
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_output_id, judge_run_id, candidate_group_id,
                       judge_schema_version, payload_json, model_proposed_verdict,
                       model_confidence_band, created_at
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": str(judge_output_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeOutputRecord(
            judge_output_id=UUID(str(row["judge_output_id"])),
            judge_run_id=UUID(str(row["judge_run_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            judge_schema_version=str(row["judge_schema_version"]),
            payload_json=_json_loads(row["payload_json"]) or {},
            model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
            model_confidence_band=_string_or_none(row["model_confidence_band"]),
            created_at=row["created_at"],
        )

    async def load_bundle_context(self, bundle_id: UUID) -> BundleValidationContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT b.bundle_id, b.candidate_group_id, b.current_primary_artifact_id,
                       ar.artifact_type AS current_primary_artifact_type, b.created_at
                FROM candidate_evidence_bundles b
                LEFT JOIN artifact_registry ar
                  ON ar.artifact_id = b.current_primary_artifact_id
                WHERE b.bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundleValidationContext(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
            current_primary_artifact_type=_string_or_none(row["current_primary_artifact_type"]),
            created_at=row["created_at"],
        )

    async def update_judge_run_status(
        self,
        *,
        judge_run_id: UUID,
        status: str,
        finish_reason: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET status = :status,
                    finish_reason = :finish_reason,
                    finished_at = COALESCE(finished_at, now())
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id), "status": status, "finish_reason": finish_reason},
        )

    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: UUID,
        from_state: str | None,
        to_state: str,
        reason_code: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO state_transitions (
                    state_transition_id,
                    object_type,
                    object_id,
                    from_state,
                    to_state,
                    reason_code,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    :object_type,
                    CAST(:object_id AS uuid),
                    :from_state,
                    :to_state,
                    :reason_code,
                    now()
                )
                """
            ),
            {
                "object_type": object_type,
                "object_id": str(object_id),
                "from_state": from_state,
                "to_state": to_state,
                "reason_code": reason_code,
            },
        )

    async def insert_analysis_policy_apply_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> bool:
        payload = {
            "judge_run_id": str(judge_run_id),
            "judge_output_id": str(judge_output_id),
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
        }
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                ) VALUES (
                    'analysis.policy.apply.v1',
                    'judge_run',
                    CAST(:judge_run_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                RETURNING event_id
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "dedupe_key": f"analysis-policy-apply:{judge_run_id}:{judge_output_id}",
                "payload_json": _jsonb_dumps(payload),
            },
        )
        return result.scalar_one_or_none() is not None


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


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)
