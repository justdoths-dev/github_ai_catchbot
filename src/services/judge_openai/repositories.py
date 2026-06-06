from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord, OpenAIJudgeUsage


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class JudgeOpenAIRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeCallJob | None:
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
        if row is None or row["event_type"] != "judge.call.requested.v1":
            return None

        payload = _json_loads(row["payload_json"]) or {}
        judge_run_id = _uuid_or_none(payload.get("judge_run_id"))
        bundle_id = _uuid_or_none(payload.get("bundle_id"))
        if judge_run_id is None or bundle_id is None:
            return None
        required = ["model", "reasoning_effort", "prompt_version"]
        if any(not payload.get(key) for key in required):
            return None
        prompt_cache_key = payload.get("prompt_cache_key")

        return JudgeCallJob(
            trigger_event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            judge_run_id=judge_run_id,
            bundle_id=bundle_id,
            model=str(payload["model"]),
            reasoning_effort=str(payload["reasoning_effort"]),
            prompt_version=str(payload["prompt_version"]),
            prompt_cache_key=str(prompt_cache_key) if prompt_cache_key else None,
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                       prompt_version, schema_version, policy_version, prompt_cache_key,
                       status, schema_retry_count
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunRecord(
            judge_run_id=UUID(str(row["judge_run_id"])),
            bundle_id=UUID(str(row["bundle_id"])),
            judge_profile=str(row["judge_profile"]),
            model=str(row["model"]),
            reasoning_effort=str(row["reasoning_effort"]),
            prompt_version=str(row["prompt_version"]),
            schema_version=str(row["schema_version"]),
            policy_version=str(row["policy_version"]),
            prompt_cache_key=str(row["prompt_cache_key"]) if row["prompt_cache_key"] else None,
            status=str(row["status"]),
            schema_retry_count=int(row["schema_retry_count"]),
        )

    async def load_bundle_context(self, bundle_id: UUID) -> BundleJudgeContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT bundle_id, candidate_group_id, current_primary_artifact_id,
                       primary_summary, supporting_summaries_json,
                       discovered_links_summary_json, evidence_limitations,
                       token_budget_profile, reroot_count, created_at
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundleJudgeContext(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
            primary_summary=_json_loads(row["primary_summary"]) or {},
            supporting_summaries_json=_json_loads(row["supporting_summaries_json"]) or [],
            discovered_links_summary_json=_json_loads(row["discovered_links_summary_json"]) or [],
            evidence_limitations=_json_loads(row["evidence_limitations"]) or [],
            token_budget_profile=str(row["token_budget_profile"]) if row["token_budget_profile"] else None,
            reroot_count=int(row["reroot_count"]),
            created_at=row["created_at"],
        )

    async def mark_judge_run_running(self, judge_run_id: UUID) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET status = 'running',
                    started_at = COALESCE(started_at, now())
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )

    async def increment_schema_retry_count(self, judge_run_id: UUID) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET schema_retry_count = schema_retry_count + 1
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )

    async def finish_judge_run(
        self,
        *,
        judge_run_id: UUID,
        status: str,
        usage: OpenAIJudgeUsage | None,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE judge_runs
                SET status = :status,
                    input_tokens = :input_tokens,
                    cached_input_tokens = :cached_input_tokens,
                    output_tokens = :output_tokens,
                    reasoning_tokens = :reasoning_tokens,
                    latency_ms = :latency_ms,
                    finish_reason = :finish_reason,
                    refusal_detected = :refusal_detected,
                    finished_at = now()
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "status": status,
                "input_tokens": usage.input_tokens if usage else None,
                "cached_input_tokens": usage.cached_input_tokens if usage else None,
                "output_tokens": usage.output_tokens if usage else None,
                "reasoning_tokens": usage.reasoning_tokens if usage else None,
                "latency_ms": usage.latency_ms if usage else None,
                "finish_reason": finish_reason,
                "refusal_detected": refusal_detected,
            },
        )

    async def insert_judge_output(
        self,
        *,
        judge_run_id: UUID,
        candidate_group_id: UUID,
        judge_schema_version: str,
        payload_json: dict[str, Any],
        model_proposed_verdict: str | None,
        model_confidence_band: str | None,
    ) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO judge_outputs (
                    judge_run_id, candidate_group_id, judge_schema_version,
                    payload_json, model_proposed_verdict, model_confidence_band, created_at
                ) VALUES (
                    CAST(:judge_run_id AS uuid),
                    CAST(:candidate_group_id AS uuid),
                    :judge_schema_version,
                    CAST(:payload_json AS jsonb),
                    :model_proposed_verdict,
                    :model_confidence_band,
                    now()
                )
                RETURNING judge_output_id
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "candidate_group_id": str(candidate_group_id),
                "judge_schema_version": judge_schema_version,
                "payload_json": _jsonb_dumps(payload_json),
                "model_proposed_verdict": model_proposed_verdict,
                "model_confidence_band": model_confidence_band,
            },
        )
        return UUID(str(result.scalar_one()))

    async def insert_judge_output_ready_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> None:
        payload = {
            "judge_run_id": str(judge_run_id),
            "judge_output_id": str(judge_output_id),
            "finish_reason": finish_reason,
            "refusal_detected": refusal_detected,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key,
                    payload_json, status, created_at
                ) VALUES (
                    'judge.output.ready.v1',
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
                "dedupe_key": f"judge-output-ready:{judge_run_id}:{judge_output_id}",
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


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)
