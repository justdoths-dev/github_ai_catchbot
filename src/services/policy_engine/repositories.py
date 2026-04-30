from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import (
    AnalysisDraft,
    AnalysisPolicyJob,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class PolicyEngineRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> AnalysisPolicyJob | None:
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
        if row is None or row["event_type"] != "analysis.policy.apply.v1":
            return None

        payload = _json_loads(row["payload_json"]) or {}
        judge_run_id = _uuid_or_none(payload.get("judge_run_id"))
        judge_output_id = _uuid_or_none(payload.get("judge_output_id"))
        candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
        bundle_id = _uuid_or_none(payload.get("bundle_id"))
        if None in {judge_run_id, judge_output_id, candidate_group_id, bundle_id}:
            return None

        return AnalysisPolicyJob(
            trigger_event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
        )

    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT candidate_group_id, current_bundle_id, current_analysis_id
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CandidatePolicyContext(
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
            current_analysis_id=_uuid_or_none(row["current_analysis_id"]),
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id, bundle_id, prompt_version, policy_version, status
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunPolicyContext(
            judge_run_id=UUID(str(row["judge_run_id"])),
            bundle_id=UUID(str(row["bundle_id"])),
            prompt_version=str(row["prompt_version"]),
            policy_version=str(row["policy_version"]),
            status=str(row["status"]),
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_output_id, judge_run_id, candidate_group_id,
                       payload_json, model_proposed_verdict, model_confidence_band, created_at
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": str(judge_output_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeOutputPolicyContext(
            judge_output_id=UUID(str(row["judge_output_id"])),
            judge_run_id=UUID(str(row["judge_run_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            payload_json=_json_loads(row["payload_json"]) or {},
            model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
            model_confidence_band=_string_or_none(row["model_confidence_band"]),
            created_at=row["created_at"],
        )

    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None:
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
        return BundlePolicyContext(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
            current_primary_artifact_type=_string_or_none(row["current_primary_artifact_type"]),
            created_at=row["created_at"],
        )

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT analysis_id, judge_output_id, policy_version, delivery_policy_version
                FROM analyses
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                  AND policy_version = :policy_version
                  AND delivery_policy_version = :delivery_policy_version
                """
            ),
            {
                "judge_output_id": str(judge_output_id),
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ExistingAnalysisRecord(
            analysis_id=UUID(str(row["analysis_id"])),
            judge_output_id=UUID(str(row["judge_output_id"])),
            policy_version=str(row["policy_version"]),
            delivery_policy_version=str(row["delivery_policy_version"]),
        )

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO analyses (
                    candidate_group_id,
                    judge_output_id,
                    schema_version,
                    policy_version,
                    prompt_version,
                    delivery_policy_version,
                    verdict,
                    delivery_decision,
                    scores_json,
                    reason_codes_json,
                    evidence_limitations_ko,
                    recommended_action_ko,
                    freshness_note_ko,
                    model_proposed_verdict,
                    policy_reconciled_flag,
                    created_at
                ) VALUES (
                    CAST(:candidate_group_id AS uuid),
                    CAST(:judge_output_id AS uuid),
                    :schema_version,
                    :policy_version,
                    :prompt_version,
                    :delivery_policy_version,
                    CAST(:verdict AS verdict_enum),
                    CAST(:delivery_decision AS delivery_decision_enum),
                    CAST(:scores_json AS jsonb),
                    CAST(:reason_codes_json AS jsonb),
                    :evidence_limitations_ko,
                    :recommended_action_ko,
                    :freshness_note_ko,
                    CAST(:model_proposed_verdict AS verdict_enum),
                    :policy_reconciled_flag,
                    now()
                )
                ON CONFLICT ON CONSTRAINT uq_analyses_judge_output_policy_delivery_policy
                DO NOTHING
                RETURNING analysis_id
                """
            ),
            {
                "candidate_group_id": str(draft.candidate_group_id),
                "judge_output_id": str(draft.judge_output_id),
                "schema_version": draft.schema_version,
                "policy_version": draft.policy_version,
                "prompt_version": draft.prompt_version,
                "delivery_policy_version": draft.delivery_policy_version,
                "verdict": draft.verdict,
                "delivery_decision": draft.delivery_decision,
                "scores_json": _jsonb_dumps(draft.scores_json),
                "reason_codes_json": _jsonb_dumps(draft.reason_codes_json),
                "evidence_limitations_ko": draft.evidence_limitations_ko,
                "recommended_action_ko": draft.recommended_action_ko,
                "freshness_note_ko": draft.freshness_note_ko,
                "model_proposed_verdict": draft.model_proposed_verdict,
                "policy_reconciled_flag": draft.policy_reconciled_flag,
            },
        )
        analysis_id = result.scalar_one_or_none()
        if analysis_id is not None:
            return UUID(str(analysis_id))

        existing = await self.load_existing_analysis(
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        if existing is None:
            raise RuntimeError("analysis insert conflicted but existing analysis was not found")
        return existing.analysis_id

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

    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None:
        payload = {
            "notification_plan_id": str(intent.notification_plan_id),
            "analysis_id": str(intent.analysis_id),
            "candidate_group_id": str(intent.candidate_group_id),
            "delivery_decision": intent.delivery_decision,
            "urgency_profile": intent.urgency_profile,
            "target_chat_id": intent.target_chat_id,
            "target_thread_id": intent.target_thread_id,
            "render_profile": intent.render_profile,
            "dedupe_subject_key": intent.dedupe_subject_key,
            "material_change_hash": intent.material_change_hash,
            "send_after": intent.send_after,
            "suppress_reason_code": intent.suppress_reason_code,
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
                ) VALUES (
                    'notification.plan.created.v1',
                    'analysis',
                    CAST(:analysis_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "analysis_id": str(intent.analysis_id),
                "dedupe_key": (
                    f"notification-plan-created:{intent.analysis_id}:"
                    f"{intent.target_chat_id}:{intent.material_change_hash}"
                ),
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


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
