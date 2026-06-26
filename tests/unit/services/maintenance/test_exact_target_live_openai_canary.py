from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.analysis_validator.models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
)
from services.judge_openai.models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord, OpenAIJudgeUsage
from services.judge_openai.openai_client import OpenAIPermanentError, OpenAITransientError
from services.maintenance.exact_target_live_openai_canary import (
    AnalysisReadback,
    ExactTargetCanaryComponents,
    ExactTargetCanaryRequest,
    ExactTargetEvent,
    ExactTargetPreflight,
    JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
    JudgeReadback,
    NOTIFICATION_INTENT_PROOF_CONFIRM_TOKEN,
    NotificationIntentProofAuthority,
    NotificationReadback,
    POST_JUDGE_OUTPUT_RESUME_CONFIRM_TOKEN,
    RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
    PostJudgeOutputResumeAuthority,
    RuntimeConfigBundle,
    build_parser,
    run_cli,
    run_exact_target_canary,
)
from services.maintenance import exact_target_live_openai_canary as canary
from services.notifier_telegram.models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    ExistingRecentDelivery,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotificationPlanDraft,
    NotificationRenderDraft,
    NotifierPlanIdempotencySnapshot,
)
from services.policy_engine.models import (
    AnalysisDraft,
    AnalysisPolicyJob,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
)


class _Tx:
    async def __aenter__(self) -> "_Tx":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _EmptyRowsResult:
    def mappings(self) -> "_EmptyRowsResult":
        return self

    def all(self) -> list[dict[str, Any]]:
        return []


class _CaptureExecuteSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement: Any, params: dict[str, Any]) -> _EmptyRowsResult:
        self.calls.append((str(statement), dict(params)))
        return _EmptyRowsResult()


class _FakeOpenAIClient:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if not self.responses:
            raise AssertionError("unexpected third OpenAI request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _Ledger:
    def __init__(
        self,
        *,
        scores: dict[str, int | None] | None = None,
        verdict: str = "inspect_now",
        require_commit_visibility: bool = False,
    ) -> None:
        self.require_commit_visibility = require_commit_visibility
        self.commit_count = 0
        self._pending_event_ids: set[UUID] = set()
        self._pending_analysis_ids: set[UUID] = set()
        self._pending_plan_ids: set[UUID] = set()
        self._pending_render_keys: set[tuple[UUID, str]] = set()
        self._pending_delivery_record_ids: set[UUID] = set()
        self._pending_delivery_outbox_keys: set[tuple[UUID, UUID]] = set()
        self.trigger_event_id = uuid4()
        self.judge_run_id = uuid4()
        self.bundle_id = uuid4()
        self.candidate_group_id = uuid4()
        self.current_primary_artifact_id = uuid4()
        self.source_message_id = uuid4()
        self.judge_output_id: UUID | None = None
        self.analysis_id: UUID | None = None
        self.event_outbox: list[dict[str, Any]] = []
        self.runs = {
            self.judge_run_id: JudgeRunRecord(
                judge_run_id=self.judge_run_id,
                bundle_id=self.bundle_id,
                judge_profile="github_primary",
                model="gpt-5.4-mini",
                reasoning_effort="low",
                prompt_version="judge_prompt_v1",
                schema_version="judge_output_v1",
                policy_version="verdict_policy_v1",
                prompt_cache_key="judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1",
                status="pending",
                schema_retry_count=0,
            )
        }
        self.finish_reason: str | None = None
        self.refusal_detected = False
        self.bundles = {
            self.bundle_id: BundleJudgeContext(
                bundle_id=self.bundle_id,
                candidate_group_id=self.candidate_group_id,
                current_primary_artifact_id=self.current_primary_artifact_id,
                primary_summary={"title": "repo", "summary": "useful"},
                supporting_summaries_json=[{"kind": "repo"}],
                discovered_links_summary_json=[{"kind": "github"}],
                evidence_limitations=["limited public snapshot"],
                token_budget_profile="small",
                reroot_count=0,
            )
        }
        self.payload = _judge_payload(
            self.candidate_group_id,
            scores=scores or _send_scores(),
            verdict=verdict,
        )
        self.outputs: list[dict[str, Any]] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.analyses: list[tuple[UUID, AnalysisDraft]] = []
        self.existing_analysis: ExistingAnalysisRecord | None = None
        self.plans: dict[UUID, NotificationPlanDraft] = {}
        self.renders: list[NotificationRenderDraft] = []
        self.delivery_records: list[dict[str, Any]] = []
        self.delivery_outbox: list[dict[str, Any]] = []
        self.write_count = 0
        self.redis_calls = 0
        self.force_duplicate_policy_events = False
        self.force_duplicate_notification_events = False
        self.skip_notification_intent = False
        self.reset_retryable_count = 0
        self.status_before_mark_running: list[str] = []
        self._append_event(
            event_id=self.trigger_event_id,
            event_type="judge.call.requested.v1",
            aggregate_type="judge_run",
            aggregate_id=self.judge_run_id,
            dedupe_key=f"judge-call:{self.judge_run_id}",
            payload_json={
                "judge_run_id": str(self.judge_run_id),
                "bundle_id": str(self.bundle_id),
                "model": self.runs[self.judge_run_id].model,
                "reasoning_effort": self.runs[self.judge_run_id].reasoning_effort,
                "prompt_version": self.runs[self.judge_run_id].prompt_version,
                "prompt_cache_key": self.runs[self.judge_run_id].prompt_cache_key,
            },
            count_write=False,
        )

    def tx(self) -> _Tx:
        return _Tx()

    async def commit_active_transaction(self) -> None:
        self.commit_count += 1
        self._pending_event_ids.clear()
        self._pending_analysis_ids.clear()
        self._pending_plan_ids.clear()
        self._pending_render_keys.clear()
        self._pending_delivery_record_ids.clear()
        self._pending_delivery_outbox_keys.clear()

    def event_by_id(self, event_id: UUID) -> dict[str, Any] | None:
        for row in self.event_outbox:
            if row["event_id"] == event_id and self._event_is_visible(row):
                return row
        return None

    def events(
        self,
        event_type: str,
        *,
        judge_run_id: UUID | None = None,
        judge_output_id: UUID | None = None,
        analysis_id: UUID | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self.event_outbox
            if row["event_type"] == event_type and self._event_is_visible(row)
        ]
        if judge_run_id is not None:
            rows = [row for row in rows if row["payload_json"].get("judge_run_id") == str(judge_run_id)]
        if judge_output_id is not None:
            rows = [row for row in rows if row["payload_json"].get("judge_output_id") == str(judge_output_id)]
        if analysis_id is not None:
            rows = [row for row in rows if row["payload_json"].get("analysis_id") == str(analysis_id)]
        return rows

    def _append_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        dedupe_key: str,
        payload_json: dict[str, Any],
        event_id: UUID | None = None,
        count_write: bool = True,
    ) -> UUID:
        for row in self.event_outbox:
            if row["dedupe_key"] == dedupe_key:
                return row["event_id"]
        event_id = event_id or uuid4()
        self.event_outbox.append(
            {
                "event_id": event_id,
                "event_type": event_type,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "dedupe_key": dedupe_key,
                "payload_json": payload_json,
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
            }
        )
        if count_write:
            self.write_count += 1
            if self.require_commit_visibility:
                self._pending_event_ids.add(event_id)
        return event_id

    def analysis_rows(self) -> list[tuple[UUID, AnalysisDraft]]:
        if not self.require_commit_visibility:
            return list(self.analyses)
        return [
            (analysis_id, analysis)
            for analysis_id, analysis in self.analyses
            if analysis_id not in self._pending_analysis_ids
        ]

    def committed_plan_ids(self, *, analysis_id: UUID, notification_plan_id: UUID | None) -> list[UUID]:
        return [
            plan_id
            for plan_id, plan in self.plans.items()
            if plan.analysis_id == analysis_id
            and (notification_plan_id is None or plan_id == notification_plan_id)
            and (not self.require_commit_visibility or plan_id not in self._pending_plan_ids)
        ]

    def committed_render_count(self, plan_ids: list[UUID]) -> int:
        return sum(
            1
            for render in self.renders
            if render.notification_plan_id in plan_ids
            and (
                not self.require_commit_visibility
                or (render.notification_plan_id, render.render_hash) not in self._pending_render_keys
            )
        )

    def committed_send_disabled_delivery_count(self, plan_ids: list[UUID]) -> int:
        return sum(
            1
            for record in self.delivery_records
            if record["notification_plan_id"] in plan_ids
            and (
                not self.require_commit_visibility
                or record["notification_delivery_record_id"] not in self._pending_delivery_record_ids
            )
            and record["result_status"] == "suppressed"
            and (record.get("telegram_response_json") or {}).get("send_disabled") is True
            and (record.get("telegram_response_json") or {}).get("dry_run") is True
        )

    def committed_delivery_result_event_count(self, plan_ids: list[UUID]) -> int:
        return sum(
            1
            for row in self.delivery_outbox
            if row["notification_plan_id"] in plan_ids
            and (
                not self.require_commit_visibility
                or (row["notification_plan_id"], row["notification_delivery_record_id"])
                not in self._pending_delivery_outbox_keys
            )
        )

    def _event_is_visible(self, row: dict[str, Any]) -> bool:
        return not self.require_commit_visibility or row["event_id"] not in self._pending_event_ids


class _CanaryRepository:
    def __init__(self, ledger: _Ledger) -> None:
        self.ledger = ledger

    async def commit_active_transaction(self) -> None:
        await self.ledger.commit_active_transaction()

    async def select_latest_eligible_judge_call(self) -> UUID | None:
        rows = [
            row
            for row in self.ledger.event_outbox
            if row["event_type"] == "judge.call.requested.v1"
            and row["aggregate_type"] == "judge_run"
            and self.ledger._event_is_visible(row)
        ]
        rows.sort(key=lambda row: (row["created_at"], row["event_id"]), reverse=True)
        for row in rows:
            judge_run_id = row["aggregate_id"]
            run = self.ledger.runs.get(judge_run_id)
            if run is None or run.status != "pending":
                continue
            if row["payload_json"].get("judge_run_id") != str(judge_run_id):
                continue
            outputs = [output for output in self.ledger.outputs if output["judge_run_id"] == judge_run_id]
            if outputs:
                continue
            if self.ledger.events("judge.output.ready.v1", judge_run_id=judge_run_id):
                continue
            if self.ledger.events("analysis.policy.apply.v1", judge_run_id=judge_run_id):
                continue
            if _downstream_rows_exist_for_run(self.ledger, judge_run_id):
                continue
            return row["event_id"]
        return None

    async def select_latest_retryable_judge_call(self) -> UUID | None:
        rows = [
            row
            for row in self.ledger.event_outbox
            if row["event_type"] == "judge.call.requested.v1"
            and row["aggregate_type"] == "judge_run"
            and self.ledger._event_is_visible(row)
        ]
        rows.sort(key=lambda row: (row["created_at"], row["event_id"]), reverse=True)
        for row in rows:
            if self._retryable_row_is_eligible(row):
                return row["event_id"]
        return None

    async def reset_retryable_judge_call_for_retry(self, trigger_event_id: UUID) -> bool:
        row = self.ledger.event_by_id(trigger_event_id)
        if row is None or not self._retryable_row_is_eligible(row):
            return False
        judge_run_id = row["aggregate_id"]
        self.ledger.runs[judge_run_id] = replace(self.ledger.runs[judge_run_id], status="pending")
        self.ledger.finish_reason = None
        self.ledger.refusal_detected = False
        self.ledger.reset_retryable_count += 1
        self.ledger.write_count += 1
        return True

    def _retryable_row_is_eligible(self, row: dict[str, Any]) -> bool:
        judge_run_id = row["aggregate_id"]
        run = self.ledger.runs.get(judge_run_id)
        if run is None or run.status != "failed_retryable":
            return False
        payload = row["payload_json"]
        if payload.get("judge_run_id") != str(judge_run_id):
            return False
        if payload.get("bundle_id") != str(run.bundle_id):
            return False
        if payload.get("model") != run.model:
            return False
        if payload.get("reasoning_effort") != run.reasoning_effort:
            return False
        if payload.get("prompt_version") != run.prompt_version:
            return False
        payload_cache_key = payload.get("prompt_cache_key")
        if payload_cache_key is not None and run.prompt_cache_key is not None and payload_cache_key != run.prompt_cache_key:
            return False
        outputs = [output for output in self.ledger.outputs if output["judge_run_id"] == judge_run_id]
        if outputs:
            return False
        if self.ledger.events("judge.output.ready.v1", judge_run_id=judge_run_id):
            return False
        if self.ledger.events("analysis.policy.apply.v1", judge_run_id=judge_run_id):
            return False
        if _downstream_rows_exist_for_run(self.ledger, judge_run_id):
            return False
        return True

    async def load_preflight(self, trigger_event_id: UUID) -> ExactTargetPreflight:
        event_row = self.ledger.event_by_id(trigger_event_id)
        if event_row is None:
            return ExactTargetPreflight(None, None, None, None, reason_code="target_event_missing")
        event = ExactTargetEvent(
            event_id=event_row["event_id"],
            event_type=event_row["event_type"],
            payload_json=event_row["payload_json"],
        )
        if event.event_type != "judge.call.requested.v1":
            return ExactTargetPreflight(event, None, None, None, reason_code="wrong_event_type")
        job = canary._job_from_event(event)
        if job is None:
            return ExactTargetPreflight(event, None, None, None, reason_code="invalid_event_payload")
        run = self.ledger.runs.get(job.judge_run_id)
        if run is None:
            return ExactTargetPreflight(event, job, None, None, reason_code="judge_run_missing")
        output_count = len([row for row in self.ledger.outputs if row["judge_run_id"] == run.judge_run_id])
        ready_count = len(self.ledger.events("judge.output.ready.v1", judge_run_id=run.judge_run_id))
        policy_count = len(self.ledger.events("analysis.policy.apply.v1", judge_run_id=run.judge_run_id))
        analysis_count = len(self.ledger.analyses)
        notification_intent_count = len(self.ledger.events("notification.plan.created.v1"))
        snapshot = ExactTargetPreflight(
            event=event,
            job=job,
            judge_run=run,
            bundle=self.ledger.bundles.get(run.bundle_id),
            judge_output_count=output_count,
            ready_event_count=ready_count,
            policy_event_count=policy_count,
            analysis_count=analysis_count,
            notification_intent_count=notification_intent_count,
            notification_plan_count=len(self.ledger.plans),
            notification_render_count=len(self.ledger.renders),
            notification_delivery_count=len(self.ledger.delivery_records),
        )
        return canary._validate_preflight_snapshot(snapshot)

    async def load_judge_readback(self, *, judge_run_id: UUID) -> JudgeReadback:
        run = self.ledger.runs.get(judge_run_id)
        outputs = [row for row in self.ledger.outputs if row["judge_run_id"] == judge_run_id]
        output_id = outputs[0]["judge_output_id"] if len(outputs) == 1 else None
        ready_events = self.ledger.events("judge.output.ready.v1", judge_run_id=judge_run_id)
        if output_id is not None:
            ready_events = [row for row in ready_events if row["payload_json"].get("judge_output_id") == str(output_id)]
        return JudgeReadback(
            judge_status=run.status if run else None,
            finish_reason=self.ledger.finish_reason,
            refusal_detected=self.ledger.refusal_detected,
            judge_output_count=len(outputs),
            judge_output_id=output_id,
            ready_event_count=len(ready_events),
            ready_event_id=ready_events[0]["event_id"] if len(ready_events) == 1 else None,
        )

    async def load_policy_event_ids(self, *, judge_run_id: UUID, judge_output_id: UUID) -> list[UUID]:
        rows = self.ledger.events(
            "analysis.policy.apply.v1",
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
        )
        if self.ledger.force_duplicate_policy_events and len(rows) == 1:
            rows = [*rows, {**rows[0], "event_id": uuid4()}]
        return [row["event_id"] for row in rows]

    async def load_analysis_readback(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> AnalysisReadback:
        rows = [
            (analysis_id, analysis)
            for analysis_id, analysis in self.ledger.analysis_rows()
            if analysis.judge_output_id == judge_output_id
            and analysis.policy_version == policy_version
            and analysis.delivery_policy_version == delivery_policy_version
        ]
        return AnalysisReadback(
            analysis_count=len(rows),
            analysis_id=rows[0][0] if len(rows) == 1 else None,
            final_verdict=rows[0][1].verdict if len(rows) == 1 else None,
            delivery_decision=rows[0][1].delivery_decision if len(rows) == 1 else None,
        )

    async def load_notification_intent_event_ids(self, *, analysis_id: UUID) -> list[UUID]:
        rows = self.ledger.events("notification.plan.created.v1", analysis_id=analysis_id)
        if self.ledger.force_duplicate_notification_events and len(rows) == 1:
            rows = [*rows, {**rows[0], "event_id": uuid4()}]
        return [row["event_id"] for row in rows]

    async def load_notification_readback(
        self,
        *,
        analysis_id: UUID,
        notification_plan_id: UUID | None,
    ) -> NotificationReadback:
        plan_ids = self.ledger.committed_plan_ids(
            analysis_id=analysis_id,
            notification_plan_id=notification_plan_id,
        )
        return NotificationReadback(
            notification_plan_count=len(plan_ids),
            notification_render_count=self.ledger.committed_render_count(plan_ids),
            send_disabled_delivery_record_count=self.ledger.committed_send_disabled_delivery_count(plan_ids),
            delivery_result_event_count=self.ledger.committed_delivery_result_event_count(plan_ids),
        )


def _downstream_rows_exist_for_run(ledger: _Ledger, judge_run_id: UUID) -> bool:
    output_ids = {
        output["judge_output_id"]
        for output in ledger.outputs
        if output["judge_run_id"] == judge_run_id
    }
    analysis_ids = {
        analysis_id
        for analysis_id, analysis in ledger.analysis_rows()
        if analysis.judge_output_id in output_ids
    }
    plan_ids = {
        plan_id
        for plan_id, plan in ledger.plans.items()
        if plan.analysis_id in analysis_ids
    }
    return bool(
        analysis_ids
        or any(ledger.events("notification.plan.created.v1", analysis_id=analysis_id) for analysis_id in analysis_ids)
        or plan_ids
        or any(render.notification_plan_id in plan_ids for render in ledger.renders)
        or any(record["notification_plan_id"] in plan_ids for record in ledger.delivery_records)
    )


class _JudgeRepository:
    def __init__(self, ledger: _Ledger) -> None:
        self.ledger = ledger

    def transaction(self) -> _Tx:
        return self.ledger.tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeCallJob | None:
        event_row = self.ledger.event_by_id(trigger_event_id)
        if event_row is None:
            return None
        return canary._job_from_event(
            ExactTargetEvent(
                event_id=event_row["event_id"],
                event_type=event_row["event_type"],
                payload_json=event_row["payload_json"],
            )
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunRecord | None:
        return self.ledger.runs.get(judge_run_id)

    async def load_bundle_context(self, bundle_id: UUID) -> BundleJudgeContext | None:
        return self.ledger.bundles.get(bundle_id)

    async def mark_judge_run_running(self, judge_run_id: UUID) -> None:
        self.ledger.status_before_mark_running.append(self.ledger.runs[judge_run_id].status)
        self.ledger.runs[judge_run_id] = replace(self.ledger.runs[judge_run_id], status="running")
        self.ledger.write_count += 1

    async def increment_schema_retry_count(self, judge_run_id: UUID) -> None:
        run = self.ledger.runs[judge_run_id]
        self.ledger.runs[judge_run_id] = replace(run, schema_retry_count=run.schema_retry_count + 1)
        self.ledger.write_count += 1

    async def finish_judge_run(
        self,
        *,
        judge_run_id: UUID,
        status: str,
        usage: OpenAIJudgeUsage | None,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> None:
        del usage
        self.ledger.runs[judge_run_id] = replace(self.ledger.runs[judge_run_id], status=status)
        self.ledger.finish_reason = finish_reason
        self.ledger.refusal_detected = refusal_detected
        self.ledger.write_count += 1

    async def insert_judge_output(self, **kwargs: Any) -> UUID:
        judge_output_id = uuid4()
        self.ledger.judge_output_id = judge_output_id
        payload_json = kwargs["payload_json"]
        self.ledger.payload = payload_json
        self.ledger.outputs.append({"judge_output_id": judge_output_id, **kwargs})
        self.ledger.write_count += 1
        return judge_output_id

    async def insert_judge_output_ready_outbox(self, **kwargs: Any) -> None:
        self.ledger._append_event(
            event_type="judge.output.ready.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=f"judge-output-ready:{kwargs['judge_run_id']}:{kwargs['judge_output_id']}",
            payload_json={
                "judge_run_id": str(kwargs["judge_run_id"]),
                "judge_output_id": str(kwargs["judge_output_id"]),
                "finish_reason": kwargs["finish_reason"],
                "refusal_detected": kwargs["refusal_detected"],
            },
        )


class _ValidatorRepository:
    def __init__(self, ledger: _Ledger) -> None:
        self.ledger = ledger

    def transaction(self) -> _Tx:
        return self.ledger.tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeOutputReadyJob | None:
        row = self.ledger.event_by_id(trigger_event_id)
        if row is None or row["event_type"] != "judge.output.ready.v1":
            return None
        payload = row["payload_json"]
        return JudgeOutputReadyJob(
            trigger_event_id=row["event_id"],
            event_type=row["event_type"],
            judge_run_id=UUID(payload["judge_run_id"]),
            judge_output_id=UUID(payload["judge_output_id"]),
            finish_reason=payload.get("finish_reason"),
            refusal_detected=bool(payload.get("refusal_detected")),
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunValidationRecord | None:
        run = self.ledger.runs.get(judge_run_id)
        if run is None:
            return None
        return JudgeRunValidationRecord(
            judge_run_id=run.judge_run_id,
            bundle_id=run.bundle_id,
            judge_profile=run.judge_profile,
            schema_version=run.schema_version,
            policy_version=run.policy_version,
            status=run.status,
            finish_reason=self.ledger.finish_reason,
            refusal_detected=self.ledger.refusal_detected,
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputRecord | None:
        for row in self.ledger.outputs:
            if row["judge_output_id"] != judge_output_id:
                continue
            return JudgeOutputRecord(
                judge_output_id=judge_output_id,
                judge_run_id=row["judge_run_id"],
                candidate_group_id=row["candidate_group_id"],
                judge_schema_version=row["judge_schema_version"],
                payload_json=row["payload_json"],
                model_proposed_verdict=row["model_proposed_verdict"],
                model_confidence_band=row["model_confidence_band"],
                created_at=datetime.now(timezone.utc),
            )
        return None

    async def load_bundle_context(self, bundle_id: UUID) -> BundleValidationContext | None:
        bundle = self.ledger.bundles.get(bundle_id)
        if bundle is None:
            return None
        return BundleValidationContext(
            bundle_id=bundle.bundle_id,
            candidate_group_id=bundle.candidate_group_id,
            current_primary_artifact_id=bundle.current_primary_artifact_id,
            current_primary_artifact_type="github_repo",
            created_at=datetime.now(timezone.utc),
        )

    async def update_judge_run_status(self, *, judge_run_id: UUID, status: str, finish_reason: str | None) -> None:
        self.ledger.runs[judge_run_id] = replace(self.ledger.runs[judge_run_id], status=status)
        self.ledger.finish_reason = finish_reason
        self.ledger.write_count += 1

    async def insert_state_transition(self, **kwargs: Any) -> None:
        self.ledger.state_transitions.append(kwargs)
        self.ledger.write_count += 1

    async def insert_analysis_policy_apply_outbox(self, **kwargs: Any) -> bool:
        self.ledger._append_event(
            event_type="analysis.policy.apply.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=f"analysis-policy-apply:{kwargs['judge_run_id']}:{kwargs['judge_output_id']}",
            payload_json={
                "judge_run_id": str(kwargs["judge_run_id"]),
                "judge_output_id": str(kwargs["judge_output_id"]),
                "candidate_group_id": str(kwargs["candidate_group_id"]),
                "bundle_id": str(kwargs["bundle_id"]),
            },
        )
        return True


class _PolicyRepository:
    def __init__(self, ledger: _Ledger) -> None:
        self.ledger = ledger

    def transaction(self) -> _Tx:
        return self.ledger.tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> AnalysisPolicyJob | None:
        row = self.ledger.event_by_id(trigger_event_id)
        if row is None or row["event_type"] != "analysis.policy.apply.v1":
            return None
        payload = row["payload_json"]
        return AnalysisPolicyJob(
            trigger_event_id=row["event_id"],
            event_type=row["event_type"],
            judge_run_id=UUID(payload["judge_run_id"]),
            judge_output_id=UUID(payload["judge_output_id"]),
            candidate_group_id=UUID(payload["candidate_group_id"]),
            bundle_id=UUID(payload["bundle_id"]),
        )

    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None:
        if candidate_group_id != self.ledger.candidate_group_id:
            return None
        return CandidatePolicyContext(
            candidate_group_id=candidate_group_id,
            current_bundle_id=self.ledger.bundle_id,
            current_analysis_id=self.ledger.existing_analysis.analysis_id if self.ledger.existing_analysis else None,
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None:
        run = self.ledger.runs.get(judge_run_id)
        if run is None:
            return None
        return JudgeRunPolicyContext(
            judge_run_id=run.judge_run_id,
            bundle_id=run.bundle_id,
            prompt_version=run.prompt_version,
            policy_version=run.policy_version,
            status=run.status,
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None:
        for row in self.ledger.outputs:
            if row["judge_output_id"] != judge_output_id:
                continue
            return JudgeOutputPolicyContext(
                judge_output_id=judge_output_id,
                judge_run_id=row["judge_run_id"],
                candidate_group_id=row["candidate_group_id"],
                payload_json=row["payload_json"],
                model_proposed_verdict=row["model_proposed_verdict"],
                model_confidence_band=row["model_confidence_band"],
                created_at=datetime.now(timezone.utc),
                judge_schema_version=row["judge_schema_version"],
            )
        return None

    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None:
        bundle = self.ledger.bundles.get(bundle_id)
        if bundle is None:
            return None
        return BundlePolicyContext(
            bundle_id=bundle.bundle_id,
            candidate_group_id=bundle.candidate_group_id,
            current_primary_artifact_id=bundle.current_primary_artifact_id,
            current_primary_artifact_type="github_repo",
            created_at=datetime.now(timezone.utc),
        )

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisRecord | None:
        existing = self.ledger.existing_analysis
        if existing is None:
            return None
        if (
            existing.judge_output_id == judge_output_id
            and existing.policy_version == policy_version
            and existing.delivery_policy_version == delivery_policy_version
        ):
            return existing
        return None

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        if self.ledger.existing_analysis is not None:
            return self.ledger.existing_analysis.analysis_id
        analysis_id = uuid4()
        self.ledger.analysis_id = analysis_id
        self.ledger.existing_analysis = ExistingAnalysisRecord(
            analysis_id=analysis_id,
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        self.ledger.analyses.append((analysis_id, draft))
        if self.ledger.require_commit_visibility:
            self.ledger._pending_analysis_ids.add(analysis_id)
        self.ledger.write_count += 1
        return analysis_id

    async def insert_state_transition(self, **kwargs: Any) -> None:
        self.ledger.state_transitions.append(kwargs)
        self.ledger.write_count += 1

    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None:
        if self.ledger.skip_notification_intent:
            return
        self.ledger._append_event(
            event_type="notification.plan.created.v1",
            aggregate_type="analysis",
            aggregate_id=intent.analysis_id,
            dedupe_key=f"notification-plan-created:{intent.analysis_id}:{intent.target_chat_id}:{intent.material_change_hash}",
            payload_json={
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
            },
        )


class _NotifierRepository:
    def __init__(self, ledger: _Ledger) -> None:
        self.ledger = ledger

    def transaction(self) -> _Tx:
        return self.ledger.tx()

    async def load_intent_job(self, trigger_event_id: UUID) -> NotificationIntentJob | None:
        row = self.ledger.event_by_id(trigger_event_id)
        if row is None or row["event_type"] != "notification.plan.created.v1":
            return None
        payload = row["payload_json"]
        return NotificationIntentJob(
            trigger_event_id=row["event_id"],
            event_type=row["event_type"],
            notification_plan_id=UUID(payload["notification_plan_id"]),
            analysis_id=UUID(payload["analysis_id"]),
            candidate_group_id=UUID(payload["candidate_group_id"]),
            delivery_decision=payload["delivery_decision"],
            urgency_profile=payload["urgency_profile"],
            target_chat_id=int(payload["target_chat_id"]),
            target_thread_id=payload.get("target_thread_id"),
            render_profile=payload.get("render_profile"),
            dedupe_subject_key=payload["dedupe_subject_key"],
            material_change_hash=payload["material_change_hash"],
            send_after=None,
            suppress_reason_code=payload.get("suppress_reason_code"),
        )

    async def load_notification_plan(self, notification_plan_id: UUID) -> dict[str, Any] | None:
        plan = self.ledger.plans.get(notification_plan_id)
        return _plan_row(plan) if plan else None

    async def load_notification_plan_intent(self, notification_plan_id: UUID) -> NotificationIntentJob | None:
        del notification_plan_id
        return None

    async def load_existing_plan_by_material(
        self,
        *,
        analysis_id: UUID,
        target_chat_id: int,
        material_change_hash: str,
    ) -> dict[str, Any] | None:
        for plan in self.ledger.plans.values():
            if (
                plan.analysis_id == analysis_id
                and plan.target_chat_id == target_chat_id
                and plan.material_change_hash == material_change_hash
            ):
                return _plan_row(plan)
        return None

    async def load_idempotency_plan_snapshots(self, intent: NotificationIntentJob) -> list[NotifierPlanIdempotencySnapshot]:
        snapshots = []
        for plan in self.ledger.plans.values():
            if not (
                plan.notification_plan_id == intent.notification_plan_id
                or (
                    plan.analysis_id == intent.analysis_id
                    and plan.candidate_group_id == intent.candidate_group_id
                    and plan.target_chat_id == intent.target_chat_id
                    and plan.dedupe_subject_key == intent.dedupe_subject_key
                    and plan.material_change_hash == intent.material_change_hash
                )
            ):
                continue
            records = [
                record
                for record in self.ledger.delivery_records
                if record["notification_plan_id"] == plan.notification_plan_id
            ]
            snapshots.append(
                NotifierPlanIdempotencySnapshot(
                    notification_plan_id=plan.notification_plan_id,
                    status=plan.status,
                    render_count=sum(
                        1 for render in self.ledger.renders if render.notification_plan_id == plan.notification_plan_id
                    ),
                    delivery_record_count=len(records),
                    suppressed_delivery_count=sum(1 for record in records if record["result_status"] == "suppressed"),
                    terminal_delivery_count=sum(1 for record in records if record["result_status"] == "suppressed"),
                )
            )
        return snapshots

    async def insert_notification_plan(self, draft: NotificationPlanDraft) -> UUID:
        existing = await self.load_existing_plan_by_material(
            analysis_id=draft.analysis_id,
            target_chat_id=draft.target_chat_id,
            material_change_hash=draft.material_change_hash,
        )
        if existing is not None:
            return UUID(str(existing["notification_plan_id"]))
        self.ledger.plans[draft.notification_plan_id] = draft
        if self.ledger.require_commit_visibility:
            self.ledger._pending_plan_ids.add(draft.notification_plan_id)
        self.ledger.write_count += 1
        return draft.notification_plan_id

    async def load_analysis(self, analysis_id: UUID) -> AnalysisRenderContext | None:
        for current_id, analysis in self.ledger.analysis_rows():
            if current_id != analysis_id:
                continue
            return AnalysisRenderContext(
                analysis_id=current_id,
                candidate_group_id=analysis.candidate_group_id,
                judge_output_id=analysis.judge_output_id,
                verdict=analysis.verdict,
                delivery_decision=analysis.delivery_decision,
                reason_codes_json=analysis.reason_codes_json,
                evidence_limitations_ko=analysis.evidence_limitations_ko,
                recommended_action_ko=analysis.recommended_action_ko,
                freshness_note_ko=analysis.freshness_note_ko,
                created_at=datetime.now(timezone.utc),
            )
        return None

    async def load_judge_output_render_fields(self, judge_output_id: UUID) -> JudgeOutputRenderContext | None:
        if judge_output_id != self.ledger.judge_output_id:
            return None
        return JudgeOutputRenderContext(
            judge_output_id=judge_output_id,
            payload_json=self.ledger.payload,
            model_confidence_band=self.ledger.payload.get("model_confidence_band"),
        )

    async def load_candidate_render_context(self, candidate_group_id: UUID) -> CandidateRenderContext | None:
        if candidate_group_id != self.ledger.candidate_group_id:
            return None
        return CandidateRenderContext(
            candidate_group_id=candidate_group_id,
            source_message_id=self.ledger.source_message_id,
            current_primary_artifact_id=self.ledger.current_primary_artifact_id,
            primary_artifact_type="github_repo",
            primary_canonical_url=None,
            primary_canonical_id="repo",
            source_message_link=None,
            source_text_surface=None,
        )

    async def load_recent_successful_delivery(self, **kwargs: Any) -> ExistingRecentDelivery | None:
        del kwargs
        return None

    async def load_successful_delivery_for_material(self, **kwargs: Any) -> ExistingRecentDelivery | None:
        del kwargs
        return None

    async def has_previous_edit_restriction(self, *, notification_plan_id: UUID) -> bool:
        del notification_plan_id
        return False

    async def count_delivery_attempts(self, *, notification_plan_id: UUID) -> int:
        return sum(1 for record in self.ledger.delivery_records if record["notification_plan_id"] == notification_plan_id)

    async def insert_notification_render(self, draft: NotificationRenderDraft) -> UUID | None:
        if any(
            render.notification_plan_id == draft.notification_plan_id and render.render_hash == draft.render_hash
            for render in self.ledger.renders
        ):
            return None
        self.ledger.renders.append(draft)
        if self.ledger.require_commit_visibility:
            self.ledger._pending_render_keys.add((draft.notification_plan_id, draft.render_hash))
        self.ledger.write_count += 1
        return uuid4()

    async def insert_delivery_record(self, **kwargs: Any) -> UUID:
        record_id = uuid4()
        self.ledger.delivery_records.append({"notification_delivery_record_id": record_id, **kwargs})
        if self.ledger.require_commit_visibility:
            self.ledger._pending_delivery_record_ids.add(record_id)
        self.ledger.write_count += 1
        return record_id

    async def update_plan_status(self, *, notification_plan_id: UUID, status: str, send_after=None) -> None:
        plan = self.ledger.plans.get(notification_plan_id)
        if plan is not None:
            self.ledger.plans[notification_plan_id] = replace(plan, status=status, send_after=send_after or plan.send_after)
        self.ledger.write_count += 1

    async def insert_state_transition(self, **kwargs: Any) -> None:
        self.ledger.state_transitions.append(kwargs)
        self.ledger.write_count += 1

    async def insert_delivery_result_outbox(self, **kwargs: Any) -> None:
        self.ledger.delivery_outbox.append(kwargs)
        if self.ledger.require_commit_visibility:
            self.ledger._pending_delivery_outbox_keys.add(
                (kwargs["notification_plan_id"], kwargs["notification_delivery_record_id"])
            )
        self.ledger.write_count += 1


def _components(ledger: _Ledger) -> ExactTargetCanaryComponents:
    return ExactTargetCanaryComponents(
        canary_repository=_CanaryRepository(ledger),
        judge_repository=_JudgeRepository(ledger),
        validator_repository=_ValidatorRepository(ledger),
        policy_repository=_PolicyRepository(ledger),
        notifier_repository=_NotifierRepository(ledger),
    )


def _joined(*parts: str) -> str:
    return "".join(parts)


def _test_database_url() -> str:
    return _joined("postgresql+psycopg://", "local", "-", "db")


def _test_redis_url() -> str:
    return _joined("redis://", "local", "-", "redis")


def _runtime(
    tmp_path: Path,
    *,
    enable_notification_send: str = "true",
    telegram_operator_chat_id: str = "12345",
) -> RuntimeConfigBundle:
    key_file = tmp_path / "openai-key.secret"
    key_file.write_text(_joined("test", "-", "openai", "-", "key"), encoding="utf-8")
    return RuntimeConfigBundle(
        database_url=_test_database_url(),
        values={
            "APP_ENV": "test",
            "DATABASE_URL": _test_database_url(),
            "REDIS_URL": _test_redis_url(),
            "OPENAI_API_KEY_FILE": str(key_file),
            "TELEGRAM_OPERATOR_CHAT_ID": telegram_operator_chat_id,
            "ENABLE_NOTIFICATION_SEND": enable_notification_send,
            "ENABLE_LATER_DELIVERY": "true",
            "ENABLE_SILENT_LATER": "true",
            "JUDGE_MAX_OUTPUT_TOKENS": "800",
            "LOG_LEVEL": "INFO",
        },
    )


def _write_cli_runtime_env(
    tmp_path: Path,
    *,
    require_openai_key: bool,
    enable_notification_send: str = "true",
) -> Path:
    lines = [
        _joined("DATABASE_URL=", _test_database_url()),
        _joined("REDIS_URL=", _test_redis_url()),
        "TELEGRAM_OPERATOR_CHAT_ID=12345",
        f"ENABLE_NOTIFICATION_SEND={enable_notification_send}",
        "ENABLE_LATER_DELIVERY=true",
        "ENABLE_SILENT_LATER=true",
        "JUDGE_MAX_OUTPUT_TOKENS=800",
        "LOG_LEVEL=INFO",
    ]
    if require_openai_key:
        key_file = tmp_path / "openai-key.secret"
        key_file.write_text(_joined("test", "-", "openai", "-", "key"), encoding="utf-8")
        lines.append(f"OPENAI_API_KEY_FILE={key_file}")
    env_file = tmp_path / "runtime.env"
    env_file.write_text("\n".join(lines), encoding="utf-8")
    return env_file


async def _run_execute(
    ledger: _Ledger,
    responses: list[Any],
    tmp_path: Path,
    *,
    runtime: RuntimeConfigBundle | None = None,
    request: ExactTargetCanaryRequest | None = None,
):
    fake_client = _FakeOpenAIClient(responses)
    report = await run_exact_target_canary(
        request or ExactTargetCanaryRequest(mode="execute", trigger_event_id=ledger.trigger_event_id),
        runtime=runtime or _runtime(tmp_path),
        components=_components(ledger),
        openai_client_builder=lambda _config: fake_client,
    )
    return report, fake_client


def _resume_authority() -> PostJudgeOutputResumeAuthority:
    return PostJudgeOutputResumeAuthority(
        allow_existing_judge_output_resume=True,
        resume_confirm=POST_JUDGE_OUTPUT_RESUME_CONFIRM_TOKEN,
    )


def _notification_intent_authority() -> NotificationIntentProofAuthority:
    return NotificationIntentProofAuthority(
        allow_policy_notification_intent_for_send_disabled_proof=True,
        notification_intent_proof_confirm=NOTIFICATION_INTENT_PROOF_CONFIRM_TOKEN,
    )


def _mark_failed_retryable(ledger: _Ledger) -> _Ledger:
    ledger.runs[ledger.judge_run_id] = replace(ledger.runs[ledger.judge_run_id], status="failed_retryable")
    ledger.finish_reason = "openai_transport_retryable"
    ledger.refusal_detected = True
    return ledger


def _materialize_existing_judge_output(ledger: _Ledger) -> UUID:
    judge_output_id = uuid4()
    ledger.judge_output_id = judge_output_id
    ledger.runs[ledger.judge_run_id] = replace(ledger.runs[ledger.judge_run_id], status="succeeded")
    ledger.finish_reason = "stop"
    ledger.refusal_detected = False
    ledger.outputs.append(
        {
            "judge_output_id": judge_output_id,
            "judge_run_id": ledger.judge_run_id,
            "candidate_group_id": ledger.candidate_group_id,
            "judge_schema_version": "judge_output_v1",
            "payload_json": ledger.payload,
            "model_proposed_verdict": ledger.payload["model_proposed_verdict"],
            "model_confidence_band": ledger.payload["model_confidence_band"],
        }
    )
    ledger._append_event(
        event_type="judge.output.ready.v1",
        aggregate_type="judge_run",
        aggregate_id=ledger.judge_run_id,
        dedupe_key=f"judge-output-ready:{ledger.judge_run_id}:{judge_output_id}",
        payload_json={
            "judge_run_id": str(ledger.judge_run_id),
            "judge_output_id": str(judge_output_id),
            "finish_reason": ledger.finish_reason,
            "refusal_detected": False,
        },
        count_write=False,
    )
    return judge_output_id


async def _run_resume_execute(ledger: _Ledger, tmp_path: Path, *, openai_client_builder=None):
    report = await run_exact_target_canary(
        ExactTargetCanaryRequest(
            mode="execute",
            trigger_event_id=ledger.trigger_event_id,
            post_judge_output_resume_authority=_resume_authority(),
        ),
        runtime=_runtime(tmp_path),
        components=_components(ledger),
        openai_client_builder=(
            openai_client_builder
            or (lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built in resume mode")))
        ),
    )
    return report


def _add_partial_notification_state(ledger: _Ledger, judge_output_id: UUID) -> None:
    analysis_id = uuid4()
    draft = AnalysisDraft(
        candidate_group_id=ledger.candidate_group_id,
        judge_output_id=judge_output_id,
        schema_version="analysis_v1",
        policy_version="verdict_policy_v1",
        prompt_version=ledger.runs[ledger.judge_run_id].prompt_version,
        delivery_policy_version="delivery_policy_v1",
        verdict="inspect_now",
        delivery_decision="send_now",
        scores_json=_send_scores(),
        reason_codes_json=["specific_evidence"],
        evidence_limitations_ko="limited",
        recommended_action_ko="inspect",
        freshness_note_ko="fresh",
        model_proposed_verdict="inspect_now",
        policy_reconciled_flag=False,
    )
    ledger.analysis_id = analysis_id
    ledger.existing_analysis = ExistingAnalysisRecord(
        analysis_id=analysis_id,
        judge_output_id=judge_output_id,
        policy_version=draft.policy_version,
        delivery_policy_version=draft.delivery_policy_version,
    )
    ledger.analyses.append((analysis_id, draft))
    plan_id = uuid4()
    ledger._append_event(
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=analysis_id,
        dedupe_key=f"notification-plan-created:{analysis_id}:12345:partial",
        payload_json={
            "notification_plan_id": str(plan_id),
            "analysis_id": str(analysis_id),
            "candidate_group_id": str(ledger.candidate_group_id),
            "delivery_decision": "send_now",
            "urgency_profile": "high",
            "target_chat_id": 12345,
            "target_thread_id": None,
            "render_profile": "telegram_single_alert_high_v1",
            "dedupe_subject_key": f"candidate:{ledger.candidate_group_id}",
            "material_change_hash": "partial",
            "send_after": None,
            "suppress_reason_code": None,
        },
        count_write=False,
    )
    ledger.plans[plan_id] = NotificationPlanDraft(
        notification_plan_id=plan_id,
        analysis_id=analysis_id,
        candidate_group_id=ledger.candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key=f"candidate:{ledger.candidate_group_id}",
        material_change_hash="partial",
        send_after=None,
        suppress_reason_code=None,
        status="planned",
    )


def _plan_row(plan: NotificationPlanDraft | None) -> dict[str, Any] | None:
    if plan is None:
        return None
    return {
        "notification_plan_id": plan.notification_plan_id,
        "analysis_id": plan.analysis_id,
        "candidate_group_id": plan.candidate_group_id,
        "delivery_decision": plan.delivery_decision,
        "urgency_profile": plan.urgency_profile,
        "target_chat_id": plan.target_chat_id,
        "target_thread_id": plan.target_thread_id,
        "render_profile": plan.render_profile,
        "dedupe_subject_key": plan.dedupe_subject_key,
        "material_change_hash": plan.material_change_hash,
        "send_after": plan.send_after,
        "suppress_reason_code": plan.suppress_reason_code,
        "status": plan.status,
    }


def _send_scores() -> dict[str, int | None]:
    return {
        "novelty": 75,
        "practical_usefulness": 88,
        "evidence_strength": 74,
        "hype_penalty": 12,
        "confidence": 72,
        "code_quality": 82,
        "maintenance_signal": 70,
        "specificity": 73,
        "reproducibility_signal": 60,
    }


def _suppress_scores() -> dict[str, int | None]:
    return {
        "novelty": 10,
        "practical_usefulness": 20,
        "evidence_strength": 15,
        "hype_penalty": 50,
        "confidence": 20,
        "code_quality": 10,
        "maintenance_signal": 10,
        "specificity": 20,
        "reproducibility_signal": None,
    }


def _runtime_empty_comparable_skip_scores() -> dict[str, int | None]:
    return {
        "novelty": 2,
        "practical_usefulness": 1,
        "evidence_strength": 1,
        "hype_penalty": 0,
        "confidence": 9,
        "code_quality": None,
        "maintenance_signal": None,
        "specificity": 1,
        "reproducibility_signal": None,
    }


def _judge_payload(candidate_group_id: UUID, *, scores: dict[str, int | None], verdict: str) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful repo",
        "summary_one_line_ko": "summary",
        "skeptical_take_ko": "skeptical take",
        "why_it_might_matter_ko": "why it matters",
        "comparables": ["existing tool"],
        "scores": scores,
        "reason_codes": ["specific_evidence"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["limited"],
        "recommended_action_ko": "inspect",
        "freshness_note_ko": "fresh",
        "model_proposed_verdict": verdict,
        "model_confidence_band": "medium",
    }


def _structured_response(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "completed",
        "output_text": json.dumps(payload),
        "usage": {"input_tokens": 10, "output_tokens": 5, "output_tokens_details": {"reasoning_tokens": 1}},
    }


def _invalid_response() -> dict[str, Any]:
    return {"status": "completed", "output_text": "{not-json"}


def _refusal_response() -> dict[str, Any]:
    return {
        "status": "completed",
        "output": [{"type": "message", "content": [{"type": "refusal", "refusal": "cannot evaluate"}]}],
    }


@pytest.mark.asyncio
async def test_plan_is_read_only(tmp_path: Path) -> None:
    ledger = _Ledger()
    report = await run_exact_target_canary(
        ExactTargetCanaryRequest(mode="plan", trigger_event_id=ledger.trigger_event_id),
        runtime=RuntimeConfigBundle(database_url=_test_database_url(), values={}),
        components=_components(ledger),
    )

    assert report.status == "pass"
    assert report.reason_code == "plan_ready"
    assert report.preflight_passed is True
    assert report.openai_request_count == 0
    assert report.redis_attempted is False
    assert report.telegram_transport_attempted is False
    assert ledger.write_count == 0


@pytest.mark.asyncio
async def test_confirmation_gate_blocks_before_env_secret_read(tmp_path: Path) -> None:
    outputs: list[str] = []

    async def fail_builder(runtime):
        raise AssertionError("env/session builder must not be called without confirmation")
        yield runtime

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--trigger-event-id",
            str(uuid4()),
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
        ],
        emit_json=outputs.append,
        session_components_builder=asynccontextmanager(fail_builder),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "live_openai_confirm_missing"
    assert payload["openai_request_count"] == 0
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection_args",
    [
        ["--select-latest-eligible-judge-call"],
        ["--select-latest-eligible-judge-call", "--selection-confirm", "latest"],
        ["--selection-confirm", JUDGE_CALL_SELECTION_CONFIRM_TOKEN],
    ],
)
async def test_selection_authority_requires_both_flag_and_exact_confirm_before_env_load(
    selection_args: list[str],
    tmp_path: Path,
) -> None:
    outputs: list[str] = []

    async def fail_builder(runtime):
        raise AssertionError("DB/session builder must not be called without selection authority")
        yield runtime

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            *selection_args,
        ],
        emit_json=outputs.append,
        session_components_builder=asynccontextmanager(fail_builder),
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "judge_call_selection_authority_required"
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection_args",
    [
        ["--select-latest-retryable-judge-call"],
        ["--retryable-selection-confirm", RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN],
        ["--select-latest-retryable-judge-call", "--retryable-selection-confirm", "latest"],
    ],
)
async def test_retryable_selection_authority_requires_flag_and_exact_confirm_before_env_load(
    selection_args: list[str],
    tmp_path: Path,
) -> None:
    outputs: list[str] = []

    async def fail_builder(runtime):
        raise AssertionError("DB/session builder must not be called without retryable selection authority")
        yield runtime

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            *selection_args,
        ],
        emit_json=outputs.append,
        session_components_builder=asynccontextmanager(fail_builder),
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "retryable_judge_call_selection_authority_required"
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_trigger_event_id_conflicts_with_selection_flag_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []
    trigger_event_id = uuid4()

    async def fail_builder(runtime):
        raise AssertionError("DB/session builder must not be called on target conflict")
        yield runtime

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--trigger-event-id",
            str(trigger_event_id),
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--select-latest-eligible-judge-call",
            "--selection-confirm",
            JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=asynccontextmanager(fail_builder),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "target_selection_conflict"
    assert payload["target_event_fingerprint"] == canary._fingerprint(trigger_event_id)
    assert str(trigger_event_id) not in outputs[0]
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_trigger_event_id_conflicts_with_retryable_selection_flag_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []
    trigger_event_id = uuid4()

    async def fail_builder(runtime):
        raise AssertionError("DB/session builder must not be called on retryable target conflict")
        yield runtime

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--trigger-event-id",
            str(trigger_event_id),
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--select-latest-retryable-judge-call",
            "--retryable-selection-confirm",
            RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=asynccontextmanager(fail_builder),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "target_selection_conflict"
    assert payload["target_event_fingerprint"] == canary._fingerprint(trigger_event_id)
    assert str(trigger_event_id) not in outputs[0]
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_pending_selector_conflicts_with_retryable_selector_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []

    async def fail_builder(runtime):
        raise AssertionError("DB/session builder must not be called on selector conflict")
        yield runtime

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--select-latest-eligible-judge-call",
            "--selection-confirm",
            JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
            "--select-latest-retryable-judge-call",
            "--retryable-selection-confirm",
            RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=asynccontextmanager(fail_builder),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "target_selection_conflict"
    assert payload["openai_request_count"] == 0
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_eligible_blocks_when_no_target_exists(tmp_path: Path) -> None:
    ledger = _Ledger()
    ledger.runs[ledger.judge_run_id] = replace(ledger.runs[ledger.judge_run_id], status="succeeded")
    env_file = _write_cli_runtime_env(tmp_path, require_openai_key=False)
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(env_file),
            "--select-latest-eligible-judge-call",
            "--selection-confirm",
            JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "eligible_judge_call_target_missing"
    assert payload["target_event_fingerprint"] is None
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False
    assert str(ledger.trigger_event_id) not in outputs[0]
    assert str(ledger.judge_run_id) not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_eligible_plan_reaches_plan_ready_without_raw_uuid_output(
    tmp_path: Path,
) -> None:
    ledger = _Ledger()
    env_file = _write_cli_runtime_env(tmp_path, require_openai_key=False)
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(env_file),
            "--select-latest-eligible-judge-call",
            "--selection-confirm",
            JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "plan_ready"
    assert payload["target_event_fingerprint"] == canary._fingerprint(ledger.trigger_event_id)
    assert payload["target_run_fingerprint"] == canary._fingerprint(ledger.judge_run_id)
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False
    assert str(ledger.trigger_event_id) not in outputs[0]
    assert str(ledger.judge_run_id) not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_eligible_execute_uses_canary_path_and_send_disabled_proof(
    tmp_path: Path,
) -> None:
    ledger = _Ledger()
    env_file = _write_cli_runtime_env(tmp_path, require_openai_key=True)
    fake_client = _FakeOpenAIClient([_structured_response(ledger.payload)])
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--env-file",
            str(env_file),
            "--confirm",
            "live-openai",
            "--select-latest-eligible-judge-call",
            "--selection-confirm",
            JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
        openai_client_builder=lambda _config: fake_client,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "notification_send_disabled_suppressed"
    assert payload["openai_request_count"] == 1
    assert len(fake_client.calls) == 1
    assert payload["judge_output_created"] is True
    assert payload["validator_forwarded_policy"] is True
    assert payload["analysis_created"] is True
    assert payload["notification_plan_created"] is True
    assert payload["notification_render_created"] is True
    assert payload["send_disabled_delivery_record_created"] is True
    assert payload["telegram_transport_attempted"] is False
    assert payload["redis_attempted"] is False
    assert ledger.delivery_records[0]["telegram_response_json"]["send_disabled"] is True
    assert str(ledger.trigger_event_id) not in outputs[0]
    assert str(ledger.judge_run_id) not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_retryable_blocks_when_no_target_exists(tmp_path: Path) -> None:
    ledger = _Ledger()
    env_file = _write_cli_runtime_env(tmp_path, require_openai_key=False)
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(env_file),
            "--select-latest-retryable-judge-call",
            "--retryable-selection-confirm",
            RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "retryable_judge_call_target_missing"
    assert payload["target_event_fingerprint"] is None
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False
    assert payload["redis_attempted"] is False
    assert str(ledger.trigger_event_id) not in outputs[0]
    assert str(ledger.judge_run_id) not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_retryable_plan_reaches_retry_plan_ready_without_raw_uuid_output(
    tmp_path: Path,
) -> None:
    ledger = _mark_failed_retryable(_Ledger())
    env_file = _write_cli_runtime_env(tmp_path, require_openai_key=False)
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(env_file),
            "--select-latest-retryable-judge-call",
            "--retryable-selection-confirm",
            RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "retry_plan_ready"
    assert payload["preflight_passed"] is True
    assert payload["judge_status"] == "failed_retryable"
    assert payload["target_event_fingerprint"] == canary._fingerprint(ledger.trigger_event_id)
    assert payload["target_run_fingerprint"] == canary._fingerprint(ledger.judge_run_id)
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False
    assert payload["redis_attempted"] is False
    assert ledger.reset_retryable_count == 0
    assert str(ledger.trigger_event_id) not in outputs[0]
    assert str(ledger.judge_run_id) not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_retryable_execute_resets_and_reuses_existing_judge_path(
    tmp_path: Path,
) -> None:
    ledger = _mark_failed_retryable(_Ledger())
    original_run_ids = set(ledger.runs)
    original_judge_call_event_count = len(ledger.events("judge.call.requested.v1"))
    env_file = _write_cli_runtime_env(tmp_path, require_openai_key=True)
    fake_client = _FakeOpenAIClient([_structured_response(ledger.payload)])
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--env-file",
            str(env_file),
            "--confirm",
            "live-openai",
            "--select-latest-retryable-judge-call",
            "--retryable-selection-confirm",
            RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
        openai_client_builder=lambda _config: fake_client,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "notification_send_disabled_suppressed"
    assert payload["openai_request_count"] == 1
    assert len(fake_client.calls) == 1
    assert ledger.reset_retryable_count == 1
    assert ledger.status_before_mark_running == ["pending"]
    assert set(ledger.runs) == original_run_ids
    assert len(ledger.events("judge.call.requested.v1")) == original_judge_call_event_count
    assert len(ledger.outputs) == 1
    assert len(ledger.events("judge.output.ready.v1", judge_run_id=ledger.judge_run_id)) == 1
    assert payload["judge_output_created"] is True
    assert payload["judge_output_ready_event_created"] is True
    assert payload["validator_forwarded_policy"] is True
    assert payload["analysis_created"] is True
    assert payload["notification_plan_created"] is True
    assert payload["notification_render_created"] is True
    assert payload["send_disabled_delivery_record_created"] is True
    assert payload["telegram_transport_attempted"] is False
    assert payload["redis_attempted"] is False
    assert str(ledger.trigger_event_id) not in outputs[0]
    assert str(ledger.judge_run_id) not in outputs[0]


@pytest.mark.asyncio
async def test_consumed_or_downstream_target_is_not_selection_eligible() -> None:
    consumed = _Ledger()
    consumed.outputs.append({"judge_output_id": uuid4(), "judge_run_id": consumed.judge_run_id})

    downstream = _Ledger()
    downstream._append_event(
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=downstream.judge_run_id,
        dedupe_key="existing-policy",
        payload_json={
            "judge_run_id": str(downstream.judge_run_id),
            "judge_output_id": str(uuid4()),
            "candidate_group_id": str(downstream.candidate_group_id),
            "bundle_id": str(downstream.bundle_id),
        },
    )

    assert await _CanaryRepository(consumed).select_latest_eligible_judge_call() is None
    assert await _CanaryRepository(downstream).select_latest_eligible_judge_call() is None


@pytest.mark.asyncio
async def test_retryable_target_with_existing_judge_output_is_not_eligible() -> None:
    ledger = _mark_failed_retryable(_Ledger())
    ledger.outputs.append({"judge_output_id": uuid4(), "judge_run_id": ledger.judge_run_id})

    assert await _CanaryRepository(ledger).select_latest_retryable_judge_call() is None


@pytest.mark.parametrize("downstream_kind", ["ready", "policy", "analysis", "notification"])
@pytest.mark.asyncio
async def test_retryable_target_with_downstream_rows_is_not_eligible(downstream_kind: str) -> None:
    ledger = _mark_failed_retryable(_Ledger())
    judge_output_id = uuid4()
    if downstream_kind == "ready":
        ledger._append_event(
            event_type="judge.output.ready.v1",
            aggregate_type="judge_run",
            aggregate_id=ledger.judge_run_id,
            dedupe_key="existing-ready",
            payload_json={"judge_run_id": str(ledger.judge_run_id), "judge_output_id": str(judge_output_id)},
            count_write=False,
        )
    elif downstream_kind == "policy":
        ledger._append_event(
            event_type="analysis.policy.apply.v1",
            aggregate_type="judge_run",
            aggregate_id=ledger.judge_run_id,
            dedupe_key="existing-policy",
            payload_json={
                "judge_run_id": str(ledger.judge_run_id),
                "judge_output_id": str(judge_output_id),
                "candidate_group_id": str(ledger.candidate_group_id),
                "bundle_id": str(ledger.bundle_id),
            },
            count_write=False,
        )
    else:
        ledger.outputs.append({"judge_output_id": judge_output_id, "judge_run_id": ledger.judge_run_id})
        if downstream_kind == "analysis":
            ledger.analyses.append(
                (
                    uuid4(),
                    AnalysisDraft(
                        candidate_group_id=ledger.candidate_group_id,
                        judge_output_id=judge_output_id,
                        schema_version="analysis_v1",
                        policy_version="verdict_policy_v1",
                        prompt_version=ledger.runs[ledger.judge_run_id].prompt_version,
                        delivery_policy_version="delivery_policy_v1",
                        verdict="skip",
                        delivery_decision="suppress",
                        scores_json=_suppress_scores(),
                        reason_codes_json=["low_score"],
                        evidence_limitations_ko="limited",
                        recommended_action_ko="skip",
                        freshness_note_ko="fresh",
                        model_proposed_verdict="skip",
                        policy_reconciled_flag=False,
                    ),
                )
            )
        else:
            _add_partial_notification_state(ledger, judge_output_id)

    assert await _CanaryRepository(ledger).select_latest_retryable_judge_call() is None


@pytest.mark.asyncio
async def test_retryable_target_with_route_payload_conflict_is_not_eligible() -> None:
    ledger = _mark_failed_retryable(_Ledger())
    ledger.event_by_id(ledger.trigger_event_id)["payload_json"]["model"] = "other-model"

    assert await _CanaryRepository(ledger).select_latest_retryable_judge_call() is None


@pytest.mark.asyncio
async def test_preflight_block_occurs_before_redis_or_openai_secret_indirection(tmp_path: Path) -> None:
    ledger = _Ledger()
    ledger.event_by_id(ledger.trigger_event_id)["event_type"] = "other.event"
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                _joined("DATABASE_URL=", _test_database_url()),
                f"REDIS_URL_FILE={tmp_path / 'missing-redis.secret'}",
                f"OPENAI_API_KEY_FILE={tmp_path / 'missing-openai.secret'}",
                "TELEGRAM_OPERATOR_CHAT_ID=12345",
            ]
        ),
        encoding="utf-8",
    )
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--trigger-event-id",
            str(ledger.trigger_event_id),
            "--env-file",
            str(env_file),
            "--confirm",
            "live-openai",
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "wrong_event_type"
    assert payload["openai_request_count"] == 0
    assert "missing-redis" not in outputs[0]
    assert "missing-openai" not in outputs[0]


@pytest.mark.asyncio
async def test_resume_execute_cli_does_not_require_live_openai_confirm_or_key(tmp_path: Path) -> None:
    ledger = _Ledger()
    _materialize_existing_judge_output(ledger)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                _joined("DATABASE_URL=", _test_database_url()),
                _joined("REDIS_URL=", _test_redis_url()),
                "TELEGRAM_OPERATOR_CHAT_ID=12345",
                "ENABLE_NOTIFICATION_SEND=true",
                "ENABLE_LATER_DELIVERY=true",
                "ENABLE_SILENT_LATER=true",
            ]
        ),
        encoding="utf-8",
    )
    outputs: list[str] = []

    @asynccontextmanager
    async def builder(runtime):
        del runtime
        yield _components(ledger)

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--trigger-event-id",
            str(ledger.trigger_event_id),
            "--env-file",
            str(env_file),
            "--allow-existing-judge-output-resume",
            "--resume-confirm",
            POST_JUDGE_OUTPUT_RESUME_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        session_components_builder=builder,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "notification_send_disabled_suppressed"
    assert payload["post_judge_output_resume_attempted"] is True
    assert payload["openai_call_attempted"] is False
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False


@pytest.mark.asyncio
async def test_resume_authority_requires_both_flags_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--trigger-event-id",
            str(uuid4()),
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--allow-existing-judge-output-resume",
        ],
        emit_json=outputs.append,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "post_judge_output_resume_authority_required"
    assert payload["openai_request_count"] == 0
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_notification_intent_proof_authority_requires_confirm_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--trigger-event-id",
            str(uuid4()),
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--confirm",
            "live-openai",
            "--allow-policy-notification-intent-for-send-disabled-proof",
        ],
        emit_json=outputs.append,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "notification_intent_proof_authority_required"
    assert payload["openai_request_count"] == 0
    assert "missing-runtime" not in outputs[0]


@pytest.mark.parametrize(
    ("mutate", "reason_code"),
    [
        (lambda ledger: ledger.event_by_id(ledger.trigger_event_id).__setitem__("event_type", "other.event"), "wrong_event_type"),
        (
            lambda ledger: ledger.event_by_id(ledger.trigger_event_id)["payload_json"].__setitem__("bundle_id", str(uuid4())),
            "event_run_bundle_mismatch",
        ),
        (lambda ledger: ledger.runs.__setitem__(ledger.judge_run_id, replace(ledger.runs[ledger.judge_run_id], status="running")), "judge_run_not_pending"),
        (lambda ledger: ledger.outputs.append({"judge_output_id": uuid4(), "judge_run_id": ledger.judge_run_id}), "target_already_consumed"),
        (
            lambda ledger: ledger._append_event(
                event_type="judge.output.ready.v1",
                aggregate_type="judge_run",
                aggregate_id=ledger.judge_run_id,
                dedupe_key="existing-ready",
                payload_json={"judge_run_id": str(ledger.judge_run_id), "judge_output_id": str(uuid4())},
            ),
            "target_already_consumed",
        ),
    ],
)
@pytest.mark.asyncio
async def test_exact_target_preflight_blocks(mutate, reason_code: str, tmp_path: Path) -> None:
    ledger = _Ledger()
    mutate(ledger)

    report, fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "blocked"
    assert report.reason_code == reason_code
    assert report.openai_request_count == 0
    assert fake_client.calls == []


@pytest.mark.asyncio
async def test_malformed_uuid_blocks_before_env_file_load(tmp_path: Path) -> None:
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--trigger-event-id",
            "not-a-uuid",
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--confirm",
            "live-openai",
        ],
        emit_json=outputs.append,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "invalid_trigger_event_id"


@pytest.mark.asyncio
async def test_no_auto_selection_or_multiple_targets_are_accepted() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--mode", "plan", "--latest"])

    outputs: list[str] = []
    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--trigger-event-id",
            str(uuid4()),
            "--trigger-event-id",
            str(uuid4()),
            "--env-file",
            "/tmp/runtime.env",
        ],
        emit_json=outputs.append,
    )
    assert exit_code == 2
    assert json.loads(outputs[0])["reason_code"] == "exactly_one_trigger_event_id_required"


@pytest.mark.asyncio
async def test_successful_live_shape_fake_reaches_send_disabled_notifier(tmp_path: Path) -> None:
    ledger = _Ledger()

    report, fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "notification_send_disabled_suppressed"
    assert report.openai_request_count == 1
    assert len(fake_client.calls) == 1
    assert report.judge_output_created is True
    assert report.validator_attempted is True
    assert report.validator_forwarded_policy is True
    assert report.policy_attempted is True
    assert report.analysis_created is True
    assert report.final_verdict == "inspect_now"
    assert report.delivery_decision == "send_now"
    assert report.notification_intent_created is True
    assert report.notifier_attempted is True
    assert report.notification_plan_created is True
    assert report.notification_render_created is True
    assert report.send_disabled_delivery_record_created is True
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert ledger.redis_calls == 0


@pytest.mark.asyncio
async def test_notification_intent_proof_authority_overrides_policy_only_for_send_disabled_proof(
    tmp_path: Path,
) -> None:
    ledger = _Ledger()
    runtime = _runtime(tmp_path, enable_notification_send="false")
    request = ExactTargetCanaryRequest(
        mode="execute",
        trigger_event_id=ledger.trigger_event_id,
        notification_intent_proof_authority=_notification_intent_authority(),
    )

    report, fake_client = await _run_execute(
        ledger,
        [_structured_response(ledger.payload)],
        tmp_path,
        runtime=runtime,
        request=request,
    )

    assert report.status == "pass"
    assert report.reason_code == "notification_send_disabled_suppressed"
    assert report.openai_request_count == 1
    assert len(fake_client.calls) == 1
    assert report.final_verdict == "inspect_now"
    assert report.delivery_decision == "send_now"
    assert report.notification_intent_created is True
    assert report.notification_plan_created is True
    assert report.notification_render_created is True
    assert report.send_disabled_delivery_record_created is True
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert ledger.delivery_records[0]["telegram_response_json"] == {
        "dry_run": True,
        "send_disabled": True,
        "send_enabled": False,
        "transport_skipped": True,
        "reason_code": "dry_run_skip_transport",
        "delivery_action": "send",
    }


@pytest.mark.asyncio
async def test_send_disabled_runtime_without_notification_intent_authority_fails_closed(
    tmp_path: Path,
) -> None:
    ledger = _Ledger()
    runtime = _runtime(tmp_path, enable_notification_send="false")

    report, fake_client = await _run_execute(
        ledger,
        [_structured_response(ledger.payload)],
        tmp_path,
        runtime=runtime,
    )

    assert report.status == "failed"
    assert report.reason_code == "notification_intent_missing"
    assert report.openai_request_count == 1
    assert len(fake_client.calls) == 1
    assert report.analysis_created is True
    assert report.delivery_decision == "send_now"
    assert report.notification_intent_created is False
    assert report.notifier_attempted is False
    assert report.telegram_transport_attempted is False
    assert len(ledger.events("notification.plan.created.v1")) == 0
    assert len(ledger.plans) == 0
    assert len(ledger.renders) == 0
    assert len(ledger.delivery_records) == 0


@pytest.mark.asyncio
async def test_notification_intent_proof_authority_requires_operator_chat_id(
    tmp_path: Path,
) -> None:
    ledger = _Ledger()
    runtime = _runtime(
        tmp_path,
        enable_notification_send="false",
        telegram_operator_chat_id="0",
    )
    request = ExactTargetCanaryRequest(
        mode="execute",
        trigger_event_id=ledger.trigger_event_id,
        notification_intent_proof_authority=_notification_intent_authority(),
    )

    report, fake_client = await _run_execute(
        ledger,
        [_structured_response(ledger.payload)],
        tmp_path,
        runtime=runtime,
        request=request,
    )

    assert report.status == "blocked"
    assert report.reason_code == "notification_intent_operator_chat_id_missing"
    assert report.openai_request_count == 0
    assert fake_client.calls == []
    assert report.telegram_transport_attempted is False
    assert len(ledger.events("notification.plan.created.v1")) == 0
    assert len(ledger.plans) == 0


@pytest.mark.asyncio
async def test_fresh_execute_writes_are_committed_before_durable_readbacks(tmp_path: Path) -> None:
    ledger = _Ledger(require_commit_visibility=True)

    report, fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "notification_send_disabled_suppressed"
    assert report.openai_request_count == 1
    assert len(fake_client.calls) == 1
    assert report.judge_output_created is True
    assert report.judge_output_ready_event_created is True
    assert report.validator_forwarded_policy is True
    assert report.analysis_created is True
    assert report.notification_plan_created is True
    assert report.notification_render_created is True
    assert report.send_disabled_delivery_record_created is True
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert ledger.commit_count == 4


@pytest.mark.asyncio
async def test_forbidden_telegram_transport_attempt_reports_bounded_failure(tmp_path: Path, monkeypatch) -> None:
    ledger = _Ledger()

    class _UnsafeNotifierService:
        def __init__(self, config, *, repository, telegram_client) -> None:
            del config, repository
            self._telegram_client = telegram_client

        async def handle_trigger_event(self, trigger_event_id: UUID) -> None:
            del trigger_event_id
            await self._telegram_client.send_message(chat_id=12345, text="forbidden")

    monkeypatch.setattr(canary, "NotifierTelegramService", _UnsafeNotifierService)

    report, fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert len(fake_client.calls) == 1
    assert report.status == "failed"
    assert report.reason_code == "telegram_transport_attempted"
    assert report.notifier_attempted is True
    assert report.telegram_transport_attempted is True


@pytest.mark.asyncio
async def test_schema_retry_bound_success_then_failure(tmp_path: Path) -> None:
    ledger = _Ledger()
    report, fake_client = await _run_execute(ledger, [_invalid_response(), _structured_response(ledger.payload)], tmp_path)

    assert report.status == "pass"
    assert report.openai_request_count == 2
    assert len(fake_client.calls) == 2

    failing = _Ledger()
    report, fake_client = await _run_execute(failing, [_invalid_response(), _invalid_response()], tmp_path)

    assert report.status == "failed"
    assert report.reason_code == "judge_failed_terminal"
    assert report.openai_request_count == 2
    assert len(fake_client.calls) == 2
    assert report.validator_attempted is False
    assert report.policy_attempted is False


@pytest.mark.asyncio
async def test_refusal_stops_after_validator_terminal_behavior(tmp_path: Path) -> None:
    ledger = _Ledger()

    report, fake_client = await _run_execute(ledger, [_refusal_response()], tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "validator_refusal_terminal"
    assert report.openai_request_count == 1
    assert len(fake_client.calls) == 1
    assert report.refusal_detected is True
    assert report.judge_output_created is True
    assert report.validator_attempted is True
    assert report.validator_forwarded_policy is False
    assert report.analysis_created is False
    assert report.notifier_attempted is False


@pytest.mark.asyncio
async def test_policy_suppress_writes_analysis_without_notification(tmp_path: Path) -> None:
    ledger = _Ledger(scores=_suppress_scores(), verdict="skip")

    report, _fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "policy_suppressed"
    assert report.analysis_created is True
    assert report.final_verdict == "skip"
    assert report.delivery_decision == "suppress"
    assert report.notification_intent_created is False
    assert report.notifier_attempted is False
    assert len(ledger.plans) == 0


@pytest.mark.asyncio
async def test_send_now_analysis_without_notification_intent_fails_closed(tmp_path: Path) -> None:
    ledger = _Ledger()
    ledger.skip_notification_intent = True

    report, _fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "failed"
    assert report.reason_code == "notification_intent_missing"
    assert report.analysis_created is True
    assert report.delivery_decision == "send_now"
    assert report.notification_intent_created is False
    assert report.notifier_attempted is False
    assert report.telegram_transport_attempted is False
    assert len(ledger.events("notification.plan.created.v1")) == 0
    assert len(ledger.plans) == 0
    assert len(ledger.renders) == 0
    assert len(ledger.delivery_records) == 0
    assert len(ledger.delivery_outbox) == 0


@pytest.mark.asyncio
async def test_multiple_notification_intents_fail_closed_before_notifier(tmp_path: Path) -> None:
    ledger = _Ledger()
    ledger.force_duplicate_notification_events = True

    report, _fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "failed"
    assert report.reason_code == "multiple_notification_intents"
    assert report.notification_intent_created is False
    assert report.notifier_attempted is False
    assert report.telegram_transport_attempted is False
    assert len(ledger.plans) == 0
    assert len(ledger.renders) == 0
    assert len(ledger.delivery_records) == 0


@pytest.mark.asyncio
async def test_multiple_derived_policy_events_block(tmp_path: Path) -> None:
    ledger = _Ledger()
    ledger.force_duplicate_policy_events = True

    report, _fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "failed"
    assert report.reason_code == "multiple_policy_events"
    assert report.policy_attempted is False
    assert report.notifier_attempted is False


@pytest.mark.asyncio
async def test_rerun_blocks_before_openai_and_does_not_duplicate_rows(tmp_path: Path) -> None:
    ledger = _Ledger()
    first, _first_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)
    counts = (len(ledger.outputs), len(ledger.analyses), len(ledger.plans), len(ledger.renders), len(ledger.delivery_records))

    second, second_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert first.status == "pass"
    assert second.status == "blocked"
    assert second.reason_code == "target_already_consumed"
    assert second.openai_request_count == 0
    assert second_client.calls == []
    assert counts == (
        len(ledger.outputs),
        len(ledger.analyses),
        len(ledger.plans),
        len(ledger.renders),
        len(ledger.delivery_records),
    )


@pytest.mark.asyncio
async def test_existing_judge_output_without_resume_flags_keeps_current_block(tmp_path: Path) -> None:
    ledger = _Ledger()
    _materialize_existing_judge_output(ledger)

    report, fake_client = await _run_execute(ledger, [_structured_response(ledger.payload)], tmp_path)

    assert report.status == "blocked"
    assert report.reason_code == "target_already_consumed"
    assert report.post_judge_output_resume_attempted is False
    assert report.openai_request_count == 0
    assert fake_client.calls == []
    assert report.validator_attempted is False
    assert report.notifier_attempted is False


@pytest.mark.asyncio
async def test_resume_from_existing_judge_output_reaches_send_disabled_notifier(tmp_path: Path) -> None:
    ledger = _Ledger()
    _materialize_existing_judge_output(ledger)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "notification_send_disabled_suppressed"
    assert report.post_judge_output_resume_attempted is True
    assert report.openai_call_attempted is False
    assert report.openai_request_count == 0
    assert report.judge_output_created is True
    assert report.judge_output_ready_event_created is True
    assert report.validator_attempted is True
    assert report.validator_forwarded_policy is True
    assert report.policy_attempted is True
    assert report.analysis_created is True
    assert report.final_verdict == "inspect_now"
    assert report.delivery_decision == "send_now"
    assert report.notification_intent_created is True
    assert report.notifier_attempted is True
    assert report.notification_plan_created is True
    assert report.notification_render_created is True
    assert report.send_disabled_delivery_record_created is True
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert len(ledger.plans) == 1
    assert len(ledger.renders) == 1
    assert len(ledger.delivery_records) == 1
    assert len(ledger.delivery_outbox) == 1


@pytest.mark.asyncio
async def test_resume_downstream_writes_are_committed_before_durable_readback(tmp_path: Path) -> None:
    ledger = _Ledger(require_commit_visibility=True)
    _materialize_existing_judge_output(ledger)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "notification_send_disabled_suppressed"
    assert report.post_judge_output_resume_attempted is True
    assert report.openai_call_attempted is False
    assert report.openai_request_count == 0
    assert report.validator_attempted is True
    assert report.validator_forwarded_policy is True
    assert report.policy_attempted is True
    assert report.analysis_created is True
    assert report.notification_intent_created is True
    assert report.notifier_attempted is True
    assert report.notification_plan_created is True
    assert report.notification_render_created is True
    assert report.send_disabled_delivery_record_created is True
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert ledger.commit_count == 3
    assert len(ledger.events("analysis.policy.apply.v1", judge_run_id=ledger.judge_run_id)) == 1
    assert len(ledger.analysis_rows()) == 1
    assert len(ledger.events("notification.plan.created.v1", analysis_id=ledger.analysis_id)) == 1
    assert ledger.committed_plan_ids(analysis_id=ledger.analysis_id, notification_plan_id=None) == list(ledger.plans)
    assert ledger.committed_render_count(list(ledger.plans)) == 1
    assert ledger.committed_send_disabled_delivery_count(list(ledger.plans)) == 1
    assert ledger.committed_delivery_result_event_count(list(ledger.plans)) == 1


@pytest.mark.asyncio
async def test_resume_downstream_commit_failure_fails_closed_without_raw_error(tmp_path: Path) -> None:
    ledger = _Ledger(require_commit_visibility=True)
    _materialize_existing_judge_output(ledger)
    private_error_text = _joined("private", "-", "commit", "-", "body")

    class _FailingCommitCanaryRepository(_CanaryRepository):
        async def commit_active_transaction(self) -> None:
            raise RuntimeError(private_error_text)

    base_components = _components(ledger)
    components = ExactTargetCanaryComponents(
        canary_repository=_FailingCommitCanaryRepository(ledger),
        judge_repository=base_components.judge_repository,
        validator_repository=base_components.validator_repository,
        policy_repository=base_components.policy_repository,
        notifier_repository=base_components.notifier_repository,
    )

    report = await run_exact_target_canary(
        ExactTargetCanaryRequest(
            mode="execute",
            trigger_event_id=ledger.trigger_event_id,
            post_judge_output_resume_authority=_resume_authority(),
        ),
        runtime=_runtime(tmp_path),
        components=components,
        openai_client_builder=lambda _config: (_ for _ in ()).throw(AssertionError("OpenAI must not be built")),
    )

    rendered = json.dumps(asdict(report), default=str)
    assert report.status == "failed"
    assert report.reason_code == "validator_commit_failed"
    assert report.openai_request_count == 0
    assert report.telegram_transport_attempted is False
    assert private_error_text not in rendered
    assert len(ledger.events("analysis.policy.apply.v1", judge_run_id=ledger.judge_run_id)) == 0


@pytest.mark.asyncio
async def test_resume_policy_suppressed_returns_pass_without_notifier(tmp_path: Path) -> None:
    ledger = _Ledger(scores=_suppress_scores(), verdict="skip")
    _materialize_existing_judge_output(ledger)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "policy_suppressed"
    assert report.post_judge_output_resume_attempted is True
    assert report.openai_request_count == 0
    assert report.validator_attempted is True
    assert report.policy_attempted is True
    assert report.analysis_created is True
    assert report.final_verdict == "skip"
    assert report.delivery_decision == "suppress"
    assert report.notification_intent_created is False
    assert report.notifier_attempted is False
    assert len(ledger.plans) == 0
    assert len(ledger.renders) == 0
    assert len(ledger.delivery_records) == 0


@pytest.mark.asyncio
async def test_resume_existing_skip_judge_output_with_empty_github_comparables_reaches_policy_suppressed(
    tmp_path: Path,
) -> None:
    ledger = _Ledger(scores=_runtime_empty_comparable_skip_scores(), verdict="skip")
    ledger.payload["comparables"] = []
    ledger.payload["model_confidence_band"] = "high"
    _materialize_existing_judge_output(ledger)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "policy_suppressed"
    assert report.post_judge_output_resume_attempted is True
    assert report.openai_call_attempted is False
    assert report.openai_request_count == 0
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert report.validator_attempted is True
    assert report.validator_forwarded_policy is True
    assert report.policy_attempted is True
    assert report.analysis_created is True
    assert report.final_verdict == "skip"
    assert report.delivery_decision == "suppress"
    assert report.notification_intent_created is False
    assert report.notifier_attempted is False
    assert len(ledger.plans) == 0
    assert len(ledger.renders) == 0
    assert len(ledger.delivery_records) == 0


@pytest.mark.parametrize("shape", ["missing", "null"])
@pytest.mark.asyncio
async def test_resume_existing_skip_judge_output_with_missing_or_null_github_comparables_reaches_policy_suppressed(
    tmp_path: Path,
    shape: str,
) -> None:
    ledger = _Ledger(scores=_runtime_empty_comparable_skip_scores(), verdict="skip")
    if shape == "missing":
        ledger.payload.pop("comparables")
    else:
        ledger.payload["comparables"] = None
    ledger.payload["model_confidence_band"] = "high"
    _materialize_existing_judge_output(ledger)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "pass"
    assert report.reason_code == "policy_suppressed"
    assert report.post_judge_output_resume_attempted is True
    assert report.openai_call_attempted is False
    assert report.openai_request_count == 0
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert report.validator_attempted is True
    assert report.validator_forwarded_policy is True
    assert report.policy_attempted is True
    assert report.analysis_created is True
    assert report.final_verdict == "skip"
    assert report.delivery_decision == "suppress"
    assert report.notification_intent_created is False
    assert report.notifier_attempted is False
    assert len(ledger.plans) == 0
    assert len(ledger.renders) == 0
    assert len(ledger.delivery_records) == 0


@pytest.mark.asyncio
async def test_resume_multiple_policy_events_fail_closed(tmp_path: Path) -> None:
    ledger = _Ledger()
    ledger.force_duplicate_policy_events = True
    _materialize_existing_judge_output(ledger)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "failed"
    assert report.reason_code == "resume_policy_state_ambiguous"
    assert report.openai_request_count == 0
    assert report.validator_attempted is True
    assert report.validator_forwarded_policy is False
    assert report.policy_attempted is False
    assert report.notifier_attempted is False
    assert report.telegram_transport_attempted is False


@pytest.mark.asyncio
async def test_resume_existing_partial_notification_state_fails_closed(tmp_path: Path) -> None:
    ledger = _Ledger()
    judge_output_id = _materialize_existing_judge_output(ledger)
    _add_partial_notification_state(ledger, judge_output_id)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "blocked"
    assert report.reason_code == "resume_notification_state_ambiguous"
    assert report.openai_request_count == 0
    assert report.validator_attempted is False
    assert report.policy_attempted is False
    assert report.notifier_attempted is False
    assert report.telegram_transport_attempted is False


@pytest.mark.asyncio
async def test_resume_mode_fails_if_openai_request_count_becomes_nonzero(tmp_path: Path, monkeypatch) -> None:
    ledger = _Ledger()
    _materialize_existing_judge_output(ledger)

    async def fake_continue(**kwargs):
        report = kwargs["report"]
        return replace(report, openai_call_attempted=True, openai_request_count=1)

    monkeypatch.setattr(canary, "_continue_from_judge_output_ready", fake_continue)

    report = await _run_resume_execute(ledger, tmp_path)

    assert report.status == "failed"
    assert report.reason_code == "resume_openai_attempted"
    assert report.openai_call_attempted is True
    assert report.openai_request_count == 1
    assert report.telegram_transport_attempted is False


@pytest.mark.asyncio
async def test_openai_failure_mapping_is_sanitized(tmp_path: Path) -> None:
    retryable = _Ledger()
    private_exception_body = _joined("sentinel", "-", "private", "-", "exception", "-", "body")
    report, _fake_client = await _run_execute(
        retryable,
        [
            OpenAITransientError(
                private_exception_body,
                safe_code="openai_retryable_rate_limited",
            )
        ],
        tmp_path,
    )
    rendered = json.dumps(asdict(report), default=str)
    assert report.status == "failed"
    assert report.reason_code == "judge_failed_retryable_openai_retryable_rate_limited"
    assert report.judge_status == "failed_retryable"
    assert report.openai_call_attempted is True
    assert report.openai_request_count == 1
    assert report.validator_attempted is False
    assert report.telegram_transport_attempted is False
    assert report.redis_attempted is False
    assert private_exception_body not in rendered
    assert str(retryable.trigger_event_id) not in rendered
    assert str(retryable.judge_run_id) not in rendered

    permanent = _Ledger()
    report, _fake_client = await _run_execute(
        permanent,
        [
            OpenAIPermanentError(
                private_exception_body,
                safe_code="openai_permanent_permission",
            )
        ],
        tmp_path,
    )
    rendered = json.dumps(asdict(report), default=str)
    assert report.status == "failed"
    assert report.reason_code == "judge_failed_terminal_openai_permanent_permission"
    assert report.judge_status == "failed_terminal"
    assert report.validator_attempted is False
    assert private_exception_body not in rendered


def test_judge_readback_legacy_openai_transport_retryable_stays_generic() -> None:
    reason_code = canary._judge_readback_block_reason(
        JudgeReadback(
            judge_status="failed_retryable",
            finish_reason="openai_transport_retryable",
            refusal_detected=False,
            judge_output_count=0,
            judge_output_id=None,
            ready_event_count=0,
            ready_event_id=None,
        )
    )

    assert reason_code == "judge_failed_retryable"


def test_judge_readback_safe_permanent_finish_reason_is_specific() -> None:
    reason_code = canary._judge_readback_block_reason(
        JudgeReadback(
            judge_status="failed_terminal",
            finish_reason="openai_permanent_auth",
            refusal_detected=False,
            judge_output_count=0,
            judge_output_id=None,
            ready_event_count=0,
            ready_event_id=None,
        )
    )

    assert reason_code == "judge_failed_terminal_openai_permanent_auth"


def test_module_entrypoint_emits_json_for_missing_env_file(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[4]
    missing_env = tmp_path / "definitely-missing-runtime.env"
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "services.maintenance.exact_target_live_openai_canary",
            "--mode",
            "plan",
            "--env-file",
            str(missing_env),
            "--select-latest-retryable-judge-call",
            "--retryable-selection-confirm",
            RETRYABLE_JUDGE_CALL_SELECTION_CONFIRM_TOKEN,
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    payload = json.loads(result.stdout)
    assert result.returncode == 2
    assert result.stderr == ""
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "env_file_missing"
    assert payload["openai_request_count"] == 0
    assert payload["telegram_transport_attempted"] is False
    assert payload["redis_attempted"] is False
    assert str(missing_env) not in result.stdout


@pytest.mark.asyncio
async def test_event_rows_for_run_uses_distinct_uuid_cast_and_json_text_binds() -> None:
    session = _CaptureExecuteSession()
    repository = canary.SqlExactTargetCanaryRepository(session)
    judge_run_id = uuid4()
    judge_output_id = uuid4()

    rows = await repository._event_rows_for_run(
        event_type="analysis.policy.apply.v1",
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
    )

    assert rows == []
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    uuid_cast_binds = set(re.findall(r"CAST\(:([A-Za-z0-9_]+)\s+AS\s+uuid\)", sql))
    json_text_binds = set(re.findall(r"payload_json->>'[^']+'\s*=\s*:([A-Za-z0-9_]+)", sql))
    assert uuid_cast_binds.isdisjoint(json_text_binds)
    assert uuid_cast_binds == {"judge_run_id_uuid"}
    assert {"judge_run_id_text", "judge_output_id_text"}.issubset(json_text_binds)
    assert params == {
        "event_type": "analysis.policy.apply.v1",
        "judge_run_id_uuid": str(judge_run_id),
        "judge_run_id_text": str(judge_run_id),
        "judge_output_id_text": str(judge_output_id),
    }


@pytest.mark.asyncio
async def test_redaction_omits_sentinel_values_from_report_and_logs(tmp_path: Path, caplog) -> None:
    ledger = _Ledger()
    prompt_text = _joined("prompt", " text ", "sentinel")
    source_text = _joined("raw", " source ", "sentinel")
    db_url = _joined("sentinel", "-", "db", "-", "url")
    redis_url = _joined("sentinel", "-", "redis", "-", "url")
    openai_key = _joined("sentinel", "-", "openai", "-", "key")
    openai_project = _joined("sentinel", "-", "openai", "-", "project")
    exception_body = _joined("sentinel", "-", "exception", "-", "body")
    ledger.bundles[ledger.bundle_id] = replace(
        ledger.bundles[ledger.bundle_id],
        primary_summary={"title": _joined(prompt_text, " ", source_text)},
    )
    runtime = _runtime(tmp_path)
    runtime.values.update(
        {
            "DATABASE_URL": db_url,
            "REDIS_URL": redis_url,
            "OPENAI_PROJECT": openai_project,
        }
    )
    key_path = Path(runtime.values["OPENAI_API_KEY_FILE"])
    key_path.write_text(openai_key, encoding="utf-8")

    report = await run_exact_target_canary(
        ExactTargetCanaryRequest(mode="execute", trigger_event_id=ledger.trigger_event_id),
        runtime=runtime,
        components=_components(ledger),
        openai_client_builder=lambda _config: _FakeOpenAIClient(
            [OpenAITransientError(exception_body)]
        ),
    )

    output = json.dumps(asdict(report), default=str)
    forbidden = [
        db_url,
        redis_url,
        openai_key,
        openai_project,
        prompt_text,
        source_text,
        exception_body,
        str(ledger.trigger_event_id),
        str(ledger.judge_run_id),
        str(ledger.candidate_group_id),
    ]
    for value in forbidden:
        assert value not in output
        assert value not in caplog.text
    assert report.redactions_applied is True


def test_source_contract_forbids_redis_workers_real_telegram_and_subprocess() -> None:
    source = Path("src/services/maintenance/exact_target_live_openai_canary.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_names.update(alias.name for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imported_names.add(module)
            imported_names.update(alias.name for alias in node.names)

    forbidden_imports = {
        "redis",
        "redis.asyncio",
        "RedisStreamConsumer",
        "JudgeOpenAIWorker",
        "TelegramBotClient",
        "TelegramBotApiTransport",
        "subprocess",
        "docker",
        "systemd",
    }
    assert imported_names.isdisjoint(forbidden_imports)
    assert "Redis.from_url" not in source
    assert "subprocess" not in source
    assert "JudgeOpenAIWorker" not in source
