from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.analysis_router.config import AnalysisRouterConfig
from services.analysis_router.models import (
    AnalysisRequestedJob,
    BundleShapeStats,
    CandidateRouteState,
    StreamMessage as AnalysisRouteStreamMessage,
)
from services.analysis_router.service import AnalysisRouterService
from services.analysis_router.worker import AnalysisRouterWorker
from services.analysis_validator.config import AnalysisValidatorConfig
from services.analysis_validator.models import JudgeOutputReadyJob, StreamMessage as AnalysisValidateStreamMessage
from services.analysis_validator.service import AnalysisValidatorService
from services.analysis_validator.worker import AnalysisValidatorWorker
from services.judge_openai.config import JudgeOpenAIConfig
from services.judge_openai.models import JudgeCallJob, StreamMessage as JudgeStreamMessage
from services.judge_openai.service import JudgeOpenAIService
from services.judge_openai.worker import JudgeOpenAIWorker
from services.notifier_telegram.config import NotifierTelegramConfig
from services.notifier_telegram.models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    ExistingRecentDelivery,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotificationPlanDraft,
    NotificationRenderDraft,
    StreamMessage as NotificationStreamMessage,
)
from services.notifier_telegram.service import NotifierTelegramService
from services.notifier_telegram.worker import NotifierTelegramWorker
from services.policy_engine.config import PolicyEngineConfig
from services.policy_engine.models import (
    AnalysisDraft,
    AnalysisPolicyJob,
    ExistingAnalysisRecord,
    NotificationPlanIntent,
    StreamMessage as PolicyStreamMessage,
)
from services.policy_engine.service import PolicyEngineService
from services.policy_engine.worker import PolicyEngineWorker


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "upstream"
FIXTURE_PATH = FIXTURE_ROOT / "analysis_to_delivery_fake_offline_valid_bundle.json"

EVENT_SEQUENCE = [
    "analysis.requested.v1",
    "judge.call.requested.v1",
    "judge.output.ready.v1",
    "analysis.policy.apply.v1",
    "notification.plan.created.v1",
    "notification.delivery.result.v1",
]


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConsumer:
    def __init__(self, message: Any) -> None:
        self.message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self):
        return [self.message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class FakeOpenAIClient:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class TripwireTelegramClient:
    def __init__(self) -> None:
        self.send_calls = 0
        self.edit_calls = 0

    async def send_message(self, **kwargs):
        self.send_calls += 1
        raise AssertionError("telegram send transport must not be called")

    async def edit_message_text(self, **kwargs):
        self.edit_calls += 1
        raise AssertionError("telegram edit transport must not be called")


@dataclass(slots=True)
class BundleLedgerRow:
    bundle_id: UUID
    candidate_group_id: UUID
    current_primary_artifact_id: UUID
    current_primary_artifact_type: str | None
    primary_summary: dict[str, Any]
    supporting_summaries_json: list[dict[str, Any]]
    discovered_links_summary_json: list[dict[str, Any]]
    evidence_limitations: list[str]
    token_budget_profile: str | None
    reroot_count: int
    bundle_profile_version: str
    ready_for_analysis: bool
    created_at: datetime | None = None

    def is_structurally_usable(self) -> bool:
        return bool(
            self.candidate_group_id
            and self.bundle_id
            and self.current_primary_artifact_id
            and self.primary_summary
            and self.token_budget_profile
        )


@dataclass(slots=True)
class JudgeRunLedgerRow:
    judge_run_id: UUID
    candidate_group_id: UUID
    bundle_id: UUID
    judge_profile: str
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    prompt_cache_key: str | None
    status: str
    schema_retry_count: int = 0
    finish_reason: str | None = None
    refusal_detected: bool = False
    usage_telemetry: dict[str, Any] | None = None


@dataclass(slots=True)
class JudgeOutputLedgerRow:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None
    created_at: datetime | None = None


class AnalysisToDeliveryLedger:
    def __init__(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture
        self.trigger_event_id = UUID(fixture["trigger_event_id"])
        self.candidate_group_id = UUID(fixture["candidate_group_id"])
        self.bundle_id = UUID(fixture["bundle_id"])
        self.current_primary_artifact_id = UUID(fixture["current_primary_artifact_id"])
        self.source_message_id = UUID(fixture["source_message_id"])

        self.event_outbox: list[dict[str, Any]] = []
        self._event_by_id: dict[UUID, dict[str, Any]] = {}
        self._outbox_dedupe_keys: set[str] = set()

        self.loaded_trigger_event_ids: list[UUID] = []
        self.loaded_analysis_ids: list[UUID] = []
        self.loaded_judge_output_ids: list[UUID] = []
        self.loaded_candidate_group_ids: list[UUID] = []
        self.loaded_bundle_ids: list[UUID] = []

        self.candidate_group_proposals: dict[UUID, dict[str, Any]] = {
            self.candidate_group_id: {
                "candidate_group_id": self.candidate_group_id,
                "current_bundle_id": self.bundle_id,
                "current_analysis_id": None,
                "source_message_id": self.source_message_id,
                "current_primary_artifact_id": self.current_primary_artifact_id,
                "primary_artifact_type": fixture["current_primary_artifact_type"],
                "primary_canonical_url": fixture["primary_canonical_url"],
                "primary_canonical_id": fixture["primary_canonical_id"],
                "source_message_link": fixture["source_message_link"],
                "source_text_surface": fixture["source_text_surface"],
            }
        }
        self.candidate_evidence_bundles: dict[UUID, BundleLedgerRow] = {
            self.bundle_id: BundleLedgerRow(
                bundle_id=self.bundle_id,
                candidate_group_id=self.candidate_group_id,
                current_primary_artifact_id=self.current_primary_artifact_id,
                current_primary_artifact_type=fixture["current_primary_artifact_type"],
                primary_summary=fixture["primary_summary"],
                supporting_summaries_json=fixture["supporting_summaries_json"],
                discovered_links_summary_json=fixture["discovered_links_summary_json"],
                evidence_limitations=fixture["evidence_limitations"],
                token_budget_profile=fixture["token_budget_profile"],
                reroot_count=int(fixture["reroot_count"]),
                bundle_profile_version=fixture["bundle_profile_version"],
                ready_for_analysis=bool(fixture["ready_for_analysis"]),
                created_at=datetime.now(timezone.utc),
            )
        }
        self.candidate_evidence_members = {
            self.bundle_id: BundleShapeStats(
                member_count=int(fixture["member_count"]),
                supporting_count=int(fixture["supporting_count"]),
            )
        }
        self.artifact_registry = {
            self.current_primary_artifact_id: {
                "artifact_id": self.current_primary_artifact_id,
                "artifact_type": fixture["current_primary_artifact_type"],
                "canonical_url": fixture["primary_canonical_url"],
                "canonical_id": fixture["primary_canonical_id"],
            }
        }
        self.source_messages = {
            self.source_message_id: {
                "source_message_id": self.source_message_id,
                "message_link": fixture["source_message_link"],
                "text_surface": fixture["source_text_surface"],
            }
        }

        self.judge_runs: dict[UUID, JudgeRunLedgerRow] = {}
        self._judge_run_by_route: dict[tuple[UUID, str, str, str], UUID] = {}
        self.judge_run_updates: list[dict[str, Any]] = []
        self.judge_outputs: dict[UUID, JudgeOutputLedgerRow] = {}
        self._judge_outputs_by_run: dict[UUID, UUID] = {}
        self.analyses: dict[UUID, AnalysisDraft] = {}
        self._analysis_by_policy: dict[tuple[UUID, str, str], ExistingAnalysisRecord] = {}
        self.notification_plan_intents: list[NotificationPlanIntent] = []
        self.notification_plans: dict[UUID, NotificationPlanDraft] = {}
        self.notification_renders: list[NotificationRenderDraft] = []
        self.notification_delivery_records: list[dict[str, Any]] = []
        self.notification_delivery_result_events: list[dict[str, Any]] = []
        self.state_transitions: list[dict[str, Any]] = []

        self.redis_dispatches: list[dict[str, Any]] = []
        self.telegram_calls: list[dict[str, Any]] = []
        self.openai_calls: list[dict[str, Any]] = []
        self.maintenance_calls: list[dict[str, Any]] = []
        self.replay_requests: list[dict[str, Any]] = []
        self.dead_letter_entries: list[dict[str, Any]] = []

        self._append_event(
            event_id=self.trigger_event_id,
            event_type="analysis.requested.v1",
            aggregate_type="candidate_group",
            aggregate_id=self.candidate_group_id,
            dedupe_key=f"analysis-request:{self.candidate_group_id}:{self.bundle_id}",
            payload_json={
                "candidate_group_id": str(self.candidate_group_id),
                "bundle_id": str(self.bundle_id),
                "judge_profile": fixture["judge_profile"],
                "escalation_allowed": bool(fixture["escalation_allowed"]),
            },
        )

    def transaction(self):
        return Tx()

    def _append_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        dedupe_key: str,
        payload_json: dict[str, Any],
        event_id: UUID | None = None,
    ) -> UUID:
        if dedupe_key in self._outbox_dedupe_keys:
            return next(row["event_id"] for row in self.event_outbox if row["dedupe_key"] == dedupe_key)
        event_id = event_id or uuid4()
        row = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "dedupe_key": dedupe_key,
            "payload_json": payload_json,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
        }
        self.event_outbox.append(row)
        self._event_by_id[event_id] = row
        self._outbox_dedupe_keys.add(dedupe_key)
        return event_id

    def event_id_for_type(self, event_type: str) -> UUID:
        matches = [row["event_id"] for row in self.event_outbox if row["event_type"] == event_type]
        assert len(matches) == 1
        return matches[0]

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        self.loaded_trigger_event_ids.append(trigger_event_id)
        row = self._event_by_id.get(trigger_event_id)
        if row is None:
            return None
        payload = row["payload_json"]
        try:
            if row["event_type"] == "analysis.requested.v1":
                return AnalysisRequestedJob(
                    trigger_event_id=trigger_event_id,
                    event_type=row["event_type"],
                    candidate_group_id=UUID(str(payload["candidate_group_id"])),
                    bundle_id=UUID(str(payload["bundle_id"])),
                    judge_profile=payload.get("judge_profile"),
                    escalation_allowed=bool(payload.get("escalation_allowed", False)),
                )
            if row["event_type"] == "judge.call.requested.v1":
                return JudgeCallJob(
                    trigger_event_id=trigger_event_id,
                    event_type=row["event_type"],
                    judge_run_id=UUID(str(payload["judge_run_id"])),
                    bundle_id=UUID(str(payload["bundle_id"])),
                    model=str(payload["model"]),
                    reasoning_effort=str(payload["reasoning_effort"]),
                    prompt_version=str(payload["prompt_version"]),
                    prompt_cache_key=_string_or_none(payload.get("prompt_cache_key")),
                )
            if row["event_type"] == "judge.output.ready.v1":
                return JudgeOutputReadyJob(
                    trigger_event_id=trigger_event_id,
                    event_type=row["event_type"],
                    judge_run_id=UUID(str(payload["judge_run_id"])),
                    judge_output_id=UUID(str(payload["judge_output_id"])),
                    finish_reason=_string_or_none(payload.get("finish_reason")),
                    refusal_detected=bool(payload.get("refusal_detected", False)),
                )
            if row["event_type"] == "analysis.policy.apply.v1":
                return AnalysisPolicyJob(
                    trigger_event_id=trigger_event_id,
                    event_type=row["event_type"],
                    judge_run_id=UUID(str(payload["judge_run_id"])),
                    judge_output_id=UUID(str(payload["judge_output_id"])),
                    candidate_group_id=UUID(str(payload["candidate_group_id"])),
                    bundle_id=UUID(str(payload["bundle_id"])),
                )
        except (KeyError, TypeError, ValueError):
            return None
        return None

    async def load_candidate_route_state(self, candidate_group_id: UUID):
        row = self.candidate_group_proposals.get(candidate_group_id)
        if row is None:
            return None
        return CandidateRouteState(candidate_group_id=candidate_group_id, current_bundle_id=row["current_bundle_id"])

    async def load_bundle(self, bundle_id: UUID):
        return self.candidate_evidence_bundles.get(bundle_id)

    async def load_bundle_shape_stats(self, bundle_id: UUID):
        return self.candidate_evidence_members.get(bundle_id, BundleShapeStats(member_count=0, supporting_count=0))

    async def get_or_create_judge_run(self, **kwargs):
        key = (
            kwargs["bundle_id"],
            kwargs["prompt_version"],
            kwargs["model"],
            kwargs["reasoning_effort"],
        )
        if key in self._judge_run_by_route:
            return self._judge_run_by_route[key], False
        judge_run_id = uuid4()
        bundle = self.candidate_evidence_bundles[kwargs["bundle_id"]]
        self._judge_run_by_route[key] = judge_run_id
        self.judge_runs[judge_run_id] = JudgeRunLedgerRow(
            judge_run_id=judge_run_id,
            candidate_group_id=bundle.candidate_group_id,
            bundle_id=kwargs["bundle_id"],
            judge_profile=kwargs["judge_profile"],
            model=kwargs["model"],
            reasoning_effort=kwargs["reasoning_effort"],
            prompt_version=kwargs["prompt_version"],
            schema_version=kwargs["schema_version"],
            policy_version=kwargs["policy_version"],
            prompt_cache_key=kwargs["prompt_cache_key"],
            status="pending",
        )
        return judge_run_id, True

    async def insert_judge_call_requested_outbox(self, **kwargs) -> None:
        self._append_event(
            event_type="judge.call.requested.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=f"judge-call:{kwargs['judge_run_id']}",
            payload_json={
                "judge_run_id": str(kwargs["judge_run_id"]),
                "candidate_group_id": str(kwargs["candidate_group_id"]),
                "bundle_id": str(kwargs["bundle_id"]),
                "judge_profile": kwargs["judge_profile"],
                "model": kwargs["model"],
                "reasoning_effort": kwargs["reasoning_effort"],
                "prompt_version": kwargs["prompt_version"],
                "prompt_cache_key": kwargs["prompt_cache_key"],
            },
        )

    async def insert_bundle_refresh_outbox(self, **kwargs) -> None:
        raise AssertionError("offline hot path fixture must not request bundle refresh")

    async def load_judge_run(self, judge_run_id: UUID):
        return self.judge_runs.get(judge_run_id)

    async def load_bundle_context(self, bundle_id: UUID):
        self.loaded_bundle_ids.append(bundle_id)
        return self.candidate_evidence_bundles.get(bundle_id)

    async def mark_judge_run_running(self, judge_run_id: UUID) -> None:
        run = self.judge_runs[judge_run_id]
        run.status = "running"
        self.judge_run_updates.append({"judge_run_id": judge_run_id, "status": "running"})

    async def increment_schema_retry_count(self, judge_run_id: UUID) -> None:
        run = self.judge_runs[judge_run_id]
        run.schema_retry_count += 1
        self.judge_run_updates.append({"judge_run_id": judge_run_id, "schema_retry_count": run.schema_retry_count})

    async def finish_judge_run(self, **kwargs) -> None:
        run = self.judge_runs[kwargs["judge_run_id"]]
        usage = kwargs["usage"]
        run.status = kwargs["status"]
        run.finish_reason = kwargs["finish_reason"]
        run.refusal_detected = bool(kwargs["refusal_detected"])
        run.usage_telemetry = {
            "input_tokens": usage.input_tokens if usage else None,
            "cached_input_tokens": usage.cached_input_tokens if usage else None,
            "output_tokens": usage.output_tokens if usage else None,
            "reasoning_tokens": usage.reasoning_tokens if usage else None,
            "latency_ms_present": usage.latency_ms is not None if usage else False,
            "finish_reason": kwargs["finish_reason"],
            "refusal_detected": bool(kwargs["refusal_detected"]),
        }
        self.judge_run_updates.append({"judge_run_id": run.judge_run_id, "status": run.status, **run.usage_telemetry})

    async def insert_judge_output(self, **kwargs) -> UUID:
        if kwargs["judge_run_id"] in self._judge_outputs_by_run:
            return self._judge_outputs_by_run[kwargs["judge_run_id"]]
        judge_output_id = uuid4()
        self._judge_outputs_by_run[kwargs["judge_run_id"]] = judge_output_id
        self.judge_outputs[judge_output_id] = JudgeOutputLedgerRow(
            judge_output_id=judge_output_id,
            judge_run_id=kwargs["judge_run_id"],
            candidate_group_id=kwargs["candidate_group_id"],
            judge_schema_version=kwargs["judge_schema_version"],
            payload_json=kwargs["payload_json"],
            model_proposed_verdict=kwargs["model_proposed_verdict"],
            model_confidence_band=kwargs["model_confidence_band"],
            created_at=datetime.now(timezone.utc),
        )
        return judge_output_id

    async def insert_judge_output_ready_outbox(self, **kwargs) -> None:
        self._append_event(
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

    async def load_judge_output(self, judge_output_id: UUID):
        self.loaded_judge_output_ids.append(judge_output_id)
        return self.judge_outputs.get(judge_output_id)

    async def update_judge_run_status(self, *, judge_run_id: UUID, status: str, finish_reason: str | None) -> None:
        run = self.judge_runs[judge_run_id]
        run.status = status
        run.finish_reason = finish_reason
        self.judge_run_updates.append({"judge_run_id": judge_run_id, "status": status, "finish_reason": finish_reason})

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

    async def insert_analysis_policy_apply_outbox(self, **kwargs) -> None:
        self._append_event(
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

    async def load_candidate_context(self, candidate_group_id: UUID):
        self.loaded_candidate_group_ids.append(candidate_group_id)
        row = self.candidate_group_proposals.get(candidate_group_id)
        if row is None:
            return None
        from services.policy_engine.models import CandidatePolicyContext

        return CandidatePolicyContext(
            candidate_group_id=candidate_group_id,
            current_bundle_id=row["current_bundle_id"],
            current_analysis_id=row["current_analysis_id"],
        )

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ):
        return self._analysis_by_policy.get((judge_output_id, policy_version, delivery_policy_version))

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        existing = await self.load_existing_analysis(
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        if existing is not None:
            return existing.analysis_id
        analysis_id = uuid4()
        self.analyses[analysis_id] = draft
        self._analysis_by_policy[(draft.judge_output_id, draft.policy_version, draft.delivery_policy_version)] = (
            ExistingAnalysisRecord(
                analysis_id=analysis_id,
                judge_output_id=draft.judge_output_id,
                policy_version=draft.policy_version,
                delivery_policy_version=draft.delivery_policy_version,
            )
        )
        return analysis_id

    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None:
        self.notification_plan_intents.append(intent)
        self._append_event(
            event_type="notification.plan.created.v1",
            aggregate_type="analysis",
            aggregate_id=intent.analysis_id,
            dedupe_key=(
                f"notification-plan-created:{intent.analysis_id}:"
                f"{intent.target_chat_id}:{intent.material_change_hash}"
            ),
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

    async def load_intent_job(self, trigger_event_id: UUID):
        self.loaded_trigger_event_ids.append(trigger_event_id)
        row = self._event_by_id.get(trigger_event_id)
        if row is None or row["event_type"] != "notification.plan.created.v1":
            return None
        payload = row["payload_json"]
        try:
            return NotificationIntentJob(
                trigger_event_id=trigger_event_id,
                event_type=row["event_type"],
                notification_plan_id=UUID(str(payload["notification_plan_id"])),
                analysis_id=UUID(str(payload["analysis_id"])),
                candidate_group_id=UUID(str(payload["candidate_group_id"])),
                delivery_decision=str(payload["delivery_decision"]),  # type: ignore[arg-type]
                urgency_profile=str(payload["urgency_profile"]),  # type: ignore[arg-type]
                target_chat_id=int(payload["target_chat_id"]),
                target_thread_id=_int_or_none(payload.get("target_thread_id")),
                render_profile=_string_or_none(payload.get("render_profile")),
                dedupe_subject_key=str(payload["dedupe_subject_key"]),
                material_change_hash=str(payload["material_change_hash"]),
                send_after=_datetime_or_none(payload.get("send_after")),
                suppress_reason_code=_string_or_none(payload.get("suppress_reason_code")),
            )
        except (KeyError, TypeError, ValueError):
            return None

    async def load_notification_plan(self, notification_plan_id: UUID):
        plan = self.notification_plans.get(notification_plan_id)
        return _plan_row(plan) if plan else None

    async def load_existing_plan_by_material(
        self,
        *,
        analysis_id: UUID,
        target_chat_id: int,
        material_change_hash: str,
    ):
        for plan in self.notification_plans.values():
            if (
                plan.analysis_id == analysis_id
                and plan.target_chat_id == target_chat_id
                and plan.material_change_hash == material_change_hash
            ):
                return _plan_row(plan)
        return None

    async def insert_notification_plan(self, draft: NotificationPlanDraft) -> UUID:
        existing = await self.load_existing_plan_by_material(
            analysis_id=draft.analysis_id,
            target_chat_id=draft.target_chat_id,
            material_change_hash=draft.material_change_hash,
        )
        if existing is not None:
            return UUID(str(existing["notification_plan_id"]))
        self.notification_plans[draft.notification_plan_id] = draft
        return draft.notification_plan_id

    async def load_analysis(self, analysis_id: UUID):
        self.loaded_analysis_ids.append(analysis_id)
        draft = self.analyses.get(analysis_id)
        if draft is None:
            return None
        return AnalysisRenderContext(
            analysis_id=analysis_id,
            candidate_group_id=draft.candidate_group_id,
            judge_output_id=draft.judge_output_id,
            verdict=draft.verdict,
            delivery_decision=draft.delivery_decision,
            reason_codes_json=draft.reason_codes_json,
            evidence_limitations_ko=draft.evidence_limitations_ko,
            recommended_action_ko=draft.recommended_action_ko,
            freshness_note_ko=draft.freshness_note_ko,
            created_at=datetime.now(timezone.utc),
        )

    async def load_judge_output_render_fields(self, judge_output_id: UUID):
        self.loaded_judge_output_ids.append(judge_output_id)
        output = self.judge_outputs.get(judge_output_id)
        if output is None:
            return None
        return JudgeOutputRenderContext(
            judge_output_id=judge_output_id,
            payload_json=output.payload_json,
            model_confidence_band=output.model_confidence_band,
        )

    async def load_candidate_render_context(self, candidate_group_id: UUID):
        self.loaded_candidate_group_ids.append(candidate_group_id)
        row = self.candidate_group_proposals.get(candidate_group_id)
        if row is None:
            return None
        return CandidateRenderContext(
            candidate_group_id=candidate_group_id,
            source_message_id=row["source_message_id"],
            current_primary_artifact_id=row["current_primary_artifact_id"],
            primary_artifact_type=row["primary_artifact_type"],
            primary_canonical_url=row["primary_canonical_url"],
            primary_canonical_id=row["primary_canonical_id"],
            source_message_link=row["source_message_link"],
            source_text_surface=row["source_text_surface"],
        )

    async def load_successful_delivery_for_material(
        self,
        *,
        dedupe_subject_key: str,
        target_chat_id: int,
        material_change_hash: str,
    ):
        for record, plan in self._successful_deliveries():
            if (
                plan.dedupe_subject_key == dedupe_subject_key
                and plan.target_chat_id == target_chat_id
                and plan.material_change_hash == material_change_hash
            ):
                return _existing_delivery(record, plan, self.candidate_group_proposals.get(plan.candidate_group_id))
        return None

    async def load_recent_successful_delivery(self, *, dedupe_subject_key: str, target_chat_id: int):
        for record, plan in reversed(self._successful_deliveries()):
            if plan.dedupe_subject_key == dedupe_subject_key and plan.target_chat_id == target_chat_id:
                return _existing_delivery(record, plan, self.candidate_group_proposals.get(plan.candidate_group_id))
        return None

    def _successful_deliveries(self) -> list[tuple[dict[str, Any], NotificationPlanDraft]]:
        successful: list[tuple[dict[str, Any], NotificationPlanDraft]] = []
        for record in self.notification_delivery_records:
            if record["result_status"] not in {"sent", "edited"}:
                continue
            plan = self.notification_plans.get(record["notification_plan_id"])
            if plan is not None:
                successful.append((record, plan))
        return successful

    async def has_previous_edit_restriction(self, *, notification_plan_id: UUID) -> bool:
        return False

    async def count_delivery_attempts(self, *, notification_plan_id: UUID) -> int:
        return sum(
            1 for record in self.notification_delivery_records if record["notification_plan_id"] == notification_plan_id
        )

    async def insert_notification_render(self, draft: NotificationRenderDraft):
        for existing in self.notification_renders:
            if existing.notification_plan_id == draft.notification_plan_id and existing.render_hash == draft.render_hash:
                return None
        self.notification_renders.append(draft)
        return uuid4()

    async def insert_delivery_record(self, **kwargs) -> UUID:
        record_id = uuid4()
        self.notification_delivery_records.append(
            {
                "notification_delivery_record_id": record_id,
                "created_at": datetime.now(timezone.utc),
                **kwargs,
            }
        )
        return record_id

    async def update_plan_status(
        self,
        *,
        notification_plan_id: UUID,
        status: str,
        send_after: datetime | None = None,
    ) -> None:
        plan = self.notification_plans.get(notification_plan_id)
        if plan is not None:
            self.notification_plans[notification_plan_id] = replace(
                plan,
                status=status,
                send_after=send_after or plan.send_after,
            )

    async def insert_delivery_result_outbox(self, **kwargs) -> None:
        payload = {
            "notification_plan_id": str(kwargs["notification_plan_id"]),
            "notification_delivery_record_id": str(kwargs["notification_delivery_record_id"]),
            "delivery_status": kwargs["delivery_status"],
            "telegram_chat_id": kwargs["telegram_chat_id"],
            "telegram_message_id": kwargs["telegram_message_id"],
            "attempt_count": kwargs["attempt_count"],
            "transport_error_code": kwargs["transport_error_code"],
            "transport_error_class": kwargs["transport_error_class"],
            "edited": kwargs["edited"],
        }
        event_id = self._append_event(
            event_type="notification.delivery.result.v1",
            aggregate_type="notification_plan",
            aggregate_id=kwargs["notification_plan_id"],
            dedupe_key=(
                f"notification-delivery-result:{kwargs['notification_plan_id']}:"
                f"{kwargs['notification_delivery_record_id']}"
            ),
            payload_json=payload,
        )
        self.notification_delivery_result_events.append(self._event_by_id[event_id])


def _analysis_router_config() -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        enable_model_escalation=False,
        default_model="gpt-5.4-mini",
        escalation_model="gpt-5.4",
        default_reasoning_effort="low",
        escalation_reasoning_effort="medium",
        github_prompt_version="judge_github_primary_v1",
        x_prompt_version="judge_x_primary_v1",
        text_idea_prompt_version="judge_text_idea_primary_v1",
        judge_schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        log_level="INFO",
    )


def _judge_config() -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.judge",
        consumer_group="judge-openai",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        openai_api_key="unused",
        openai_project=None,
        request_timeout_sec=1.0,
        max_output_tokens=800,
        enable_prompt_guard_preflight=False,
        log_level="INFO",
    )


def _validator_config() -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.validate",
        consumer_group="analysis-validator",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="INFO",
    )


def _policy_config() -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=12345,
        enable_later_delivery=True,
        enable_silent_later=True,
        enable_notification_send=True,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )


def _notifier_config() -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        telegram_bot_token="",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        dry_run=False,
        allow_edits=False,
        enable_notification_send=False,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10,
        log_level="INFO",
    )


def _load_fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _judge_payload(candidate_group_id: UUID) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Offline hot path fixture",
        "summary_one_line_ko": "Synthetic fixture summary",
        "skeptical_take_ko": "Synthetic skepticism remains bounded.",
        "why_it_might_matter_ko": "Synthetic evidence is enough for the offline chain.",
        "comparables": ["offline comparable"],
        "scores": {
            "novelty": 75,
            "practical_usefulness": 76,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 72,
            "code_quality": 70,
            "maintenance_signal": 60,
            "specificity": 65,
            "reproducibility_signal": 50,
        },
        "reason_codes": ["repo_has_clear_scope"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["fixture only"],
        "recommended_action_ko": "inspect fixture",
        "freshness_note_ko": "fixture fresh",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _openai_response(candidate_group_id: UUID) -> dict[str, Any]:
    return {
        "id": "fake-response-id",
        "status": "completed",
        "output_text": json.dumps(_judge_payload(candidate_group_id)),
        "usage": {
            "input_tokens": 90,
            "input_tokens_details": {"cached_tokens": 70},
            "output_tokens": 20,
            "output_tokens_details": {"reasoning_tokens": 6},
        },
    }


def _event_type_sequence(ledger: AnalysisToDeliveryLedger) -> list[str]:
    return [row["event_type"] for row in ledger.event_outbox]


def _event_payloads_are_thin(ledger: AnalysisToDeliveryLedger) -> bool:
    forbidden_keys = {
        "scores",
        "primary_summary",
        "supporting_summaries_json",
        "source_text_surface",
        "telegram_response_json",
    }
    return all(forbidden_keys.isdisjoint(row["payload_json"]) for row in ledger.event_outbox)


def _upstream_snapshot(ledger: AnalysisToDeliveryLedger) -> dict[str, Any]:
    return {
        "candidate_group_proposals": deepcopy(ledger.candidate_group_proposals),
        "candidate_evidence_bundles": deepcopy(ledger.candidate_evidence_bundles),
        "candidate_evidence_members": deepcopy(ledger.candidate_evidence_members),
        "artifact_registry": deepcopy(ledger.artifact_registry),
        "source_messages": deepcopy(ledger.source_messages),
    }


def _forbidden_runtime_state(ledger: AnalysisToDeliveryLedger) -> dict[str, Any]:
    return {
        "redis_dispatches": ledger.redis_dispatches,
        "telegram_calls": ledger.telegram_calls,
        "openai_calls": ledger.openai_calls,
        "maintenance_calls": ledger.maintenance_calls,
        "replay_requests": ledger.replay_requests,
        "dead_letter_entries": ledger.dead_letter_entries,
    }


def _poisoned_fields(trigger_event_id: UUID) -> dict[str, str]:
    poisoned_id = str(uuid4())
    return {
        "job_id": poisoned_id,
        "stage_name": "poisoned-stage",
        "root_object_type": "poisoned-root",
        "root_object_id": poisoned_id,
        "idempotency_key": "poisoned-idempotency-key",
        "pipeline_run_id": poisoned_id,
        "not_before": "2099-01-01T00:00:00+00:00",
        "trigger_event_id": str(trigger_event_id),
        "candidate_group_id": str(uuid4()),
        "bundle_id": str(uuid4()),
        "judge_run_id": str(uuid4()),
        "judge_output_id": str(uuid4()),
        "notification_plan_id": str(uuid4()),
        "analysis_id": str(uuid4()),
        "target_chat_id": "999999",
        "payload_json": json.dumps({"candidate_group_id": str(uuid4()), "delivery_decision": "suppress"}),
        "telegram_bot_token": "poisoned",
        "openai_api_key": "poisoned",
    }


async def _run_analysis_router_stage(ledger: AnalysisToDeliveryLedger, event_id: UUID):
    config = _analysis_router_config()
    consumer = FakeConsumer(
        AnalysisRouteStreamMessage(
            stream="q.analysis.route",
            message_id=f"analysis-route-{len(ledger.loaded_trigger_event_ids)}",
            fields=_poisoned_fields(event_id),
        )
    )
    service = AnalysisRouterService(config, repository=ledger)  # type: ignore[arg-type]
    worker = AnalysisRouterWorker(config, consumer=consumer, service=service)  # type: ignore[arg-type]
    return consumer, await worker.run_once()


async def _run_judge_stage(ledger: AnalysisToDeliveryLedger, event_id: UUID, openai_client: FakeOpenAIClient):
    config = _judge_config()
    consumer = FakeConsumer(
        JudgeStreamMessage(
            stream="q.analysis.judge",
            message_id=f"judge-{len(ledger.loaded_trigger_event_ids)}",
            fields=_poisoned_fields(event_id),
        )
    )
    service = JudgeOpenAIService(config, repository=ledger, openai_client=openai_client)  # type: ignore[arg-type]
    worker = JudgeOpenAIWorker(config, consumer=consumer, service=service)
    return consumer, await worker.run_once()


async def _run_validator_stage(ledger: AnalysisToDeliveryLedger, event_id: UUID):
    config = _validator_config()
    consumer = FakeConsumer(
        AnalysisValidateStreamMessage(
            stream="q.analysis.validate",
            message_id=f"analysis-validate-{len(ledger.loaded_trigger_event_ids)}",
            fields=_poisoned_fields(event_id),
        )
    )
    service = AnalysisValidatorService(config, repository=ledger)  # type: ignore[arg-type]
    worker = AnalysisValidatorWorker(config, consumer=consumer, service=service)  # type: ignore[arg-type]
    return consumer, await worker.run_once()


async def _run_policy_stage(ledger: AnalysisToDeliveryLedger, event_id: UUID):
    config = _policy_config()
    consumer = FakeConsumer(
        PolicyStreamMessage(
            stream="q.analysis.policy",
            message_id=f"analysis-policy-{len(ledger.loaded_trigger_event_ids)}",
            fields=_poisoned_fields(event_id),
        )
    )
    service = PolicyEngineService(config, repository=ledger)  # type: ignore[arg-type]
    worker = PolicyEngineWorker(config, consumer=consumer, service=service)
    return consumer, await worker.run_once()


async def _run_notifier_stage(
    ledger: AnalysisToDeliveryLedger,
    event_id: UUID,
    telegram_client: TripwireTelegramClient,
):
    config = _notifier_config()
    consumer = FakeConsumer(
        NotificationStreamMessage(
            stream="q.notification.send",
            message_id=f"notification-send-{len(ledger.loaded_trigger_event_ids)}",
            fields=_poisoned_fields(event_id),
        )
    )
    service = NotifierTelegramService(
        config,
        repository=ledger,  # type: ignore[arg-type]
        telegram_client=telegram_client,  # type: ignore[arg-type]
    )
    worker = NotifierTelegramWorker(config, consumer=consumer, service=service)
    return consumer, await worker.run_once()


async def _run_success_chain(
    ledger: AnalysisToDeliveryLedger,
    *,
    openai_client: FakeOpenAIClient,
    telegram_client: TripwireTelegramClient,
) -> None:
    before = _upstream_snapshot(ledger)
    assert _event_type_sequence(ledger) == ["analysis.requested.v1"]

    consumer, result = await _run_analysis_router_stage(ledger, ledger.trigger_event_id)
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["analysis-route-0"]
    assert len(ledger.judge_runs) == 1
    judge_run = next(iter(ledger.judge_runs.values()))
    assert judge_run.status == "pending"
    assert _event_type_sequence(ledger) == ["analysis.requested.v1", "judge.call.requested.v1"]
    assert len(ledger.judge_outputs) == 0
    assert len(ledger.analyses) == 0
    assert len(ledger.notification_delivery_records) == 0

    consumer, result = await _run_judge_stage(
        ledger,
        ledger.event_id_for_type("judge.call.requested.v1"),
        openai_client,
    )
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["judge-1"]
    assert len(openai_client.calls) == 1
    assert len(ledger.judge_outputs) == 1
    assert judge_run.status == "succeeded"
    assert judge_run.usage_telemetry == {
        "input_tokens": 90,
        "cached_input_tokens": 70,
        "output_tokens": 20,
        "reasoning_tokens": 6,
        "latency_ms_present": True,
        "finish_reason": "completed",
        "refusal_detected": False,
    }
    assert _event_type_sequence(ledger) == [
        "analysis.requested.v1",
        "judge.call.requested.v1",
        "judge.output.ready.v1",
    ]
    assert len(ledger.analyses) == 0
    assert len(ledger.notification_delivery_records) == 0

    before_outputs = deepcopy(ledger.judge_outputs)
    consumer, result = await _run_validator_stage(ledger, ledger.event_id_for_type("judge.output.ready.v1"))
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["analysis-validate-2"]
    assert ledger.judge_outputs == before_outputs
    assert _event_type_sequence(ledger) == [
        "analysis.requested.v1",
        "judge.call.requested.v1",
        "judge.output.ready.v1",
        "analysis.policy.apply.v1",
    ]
    assert len(ledger.analyses) == 0

    consumer, result = await _run_policy_stage(ledger, ledger.event_id_for_type("analysis.policy.apply.v1"))
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["analysis-policy-3"]
    assert len(ledger.analyses) == 1
    analysis = next(iter(ledger.analyses.values()))
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"
    assert analysis.policy_reconciled_flag is False
    assert "policy_overrode_model_verdict" in analysis.reason_codes_json
    assert _event_type_sequence(ledger) == [
        "analysis.requested.v1",
        "judge.call.requested.v1",
        "judge.output.ready.v1",
        "analysis.policy.apply.v1",
        "notification.plan.created.v1",
    ]
    assert len(ledger.notification_plans) == 0

    consumer, result = await _run_notifier_stage(
        ledger,
        ledger.event_id_for_type("notification.plan.created.v1"),
        telegram_client,
    )
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["notification-send-4"]
    assert _event_type_sequence(ledger) == EVENT_SEQUENCE
    assert _upstream_snapshot(ledger) == before


def _install_tripwires(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.judge_openai import openai_client
    from services.maintenance import worker as maintenance_worker
    from services.notifier_telegram import telegram_client
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.outbox_relay import service as outbox_relay_service

    def fail_forbidden_runtime(*args, **kwargs):
        raise AssertionError("fake/offline hot path acceptance must not open runtime infrastructure")

    monkeypatch.setattr(openai_client, "OpenAIJudgeClient", fail_forbidden_runtime)
    monkeypatch.setattr(telegram_client.TelegramBotClient, "send_message", fail_forbidden_runtime)
    monkeypatch.setattr(telegram_client.TelegramBotClient, "edit_message_text", fail_forbidden_runtime)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_forbidden_runtime)
    monkeypatch.setattr(outbox_relay_service, "OutboxRelayService", fail_forbidden_runtime)
    monkeypatch.setattr(maintenance_worker, "MaintenanceQueueWorker", fail_forbidden_runtime)
    monkeypatch.setattr(maintenance_worker, "ReplayQueueWorker", fail_forbidden_runtime)
    monkeypatch.setattr(maintenance_worker, "DueRetryPromotionWorker", fail_forbidden_runtime)


@pytest.mark.asyncio
async def test_analysis_requested_to_delivery_result_fake_offline_hot_path_rehydrates_through_current_handoffs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_tripwires(monkeypatch)
    ledger = AnalysisToDeliveryLedger(_load_fixture())
    openai_client = FakeOpenAIClient(_openai_response(ledger.candidate_group_id))
    telegram_client = TripwireTelegramClient()

    await _run_success_chain(ledger, openai_client=openai_client, telegram_client=telegram_client)

    assert _event_type_sequence(ledger) == EVENT_SEQUENCE
    assert len(ledger.judge_runs) == 1
    assert len(ledger.judge_outputs) == 1
    assert len(ledger.analyses) == 1
    assert len(ledger.notification_plans) == 1
    assert len(ledger.notification_renders) == 1
    assert len(ledger.notification_delivery_records) == 1
    assert len(ledger.notification_delivery_result_events) == 1
    assert telegram_client.send_calls == 0
    assert telegram_client.edit_calls == 0
    assert len(openai_client.calls) == 1
    assert _forbidden_runtime_state(ledger) == {
        "redis_dispatches": [],
        "telegram_calls": [],
        "openai_calls": [],
        "maintenance_calls": [],
        "replay_requests": [],
        "dead_letter_entries": [],
    }

    delivery = ledger.notification_delivery_records[0]
    assert delivery["result_status"] == "suppressed"
    assert delivery["attempt_count"] == 0
    assert delivery["transport_error_code"] == "notification_send_flag_disabled"

    result_event = ledger.notification_delivery_result_events[0]
    assert result_event["event_type"] == "notification.delivery.result.v1"
    assert result_event["payload_json"]["delivery_status"] == "suppressed"
    assert result_event["payload_json"]["transport_error_code"] == "notification_send_flag_disabled"
    assert result_event["payload_json"]["notification_plan_id"] == str(next(iter(ledger.notification_plans)))

    assert _event_payloads_are_thin(ledger)
    assert [transition["reason_code"] for transition in ledger.state_transitions] == [
        "validator_passed",
        "policy_applied:inspect_now:send_now",
        "notification_rendered",
        "notification_send_flag_disabled",
    ]
    assert ledger.loaded_trigger_event_ids == [ledger.event_id_for_type(event) for event in EVENT_SEQUENCE[:-1]]


@pytest.mark.asyncio
async def test_duplicate_chain_replay_does_not_duplicate_terminal_business_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_tripwires(monkeypatch)
    ledger = AnalysisToDeliveryLedger(_load_fixture())
    first_openai = FakeOpenAIClient(_openai_response(ledger.candidate_group_id))
    telegram_client = TripwireTelegramClient()
    await _run_success_chain(ledger, openai_client=first_openai, telegram_client=telegram_client)

    counts = {
        "judge_runs": len(ledger.judge_runs),
        "judge_outputs": len(ledger.judge_outputs),
        "analyses": len(ledger.analyses),
        "notification_plans": len(ledger.notification_plans),
        "notification_renders": len(ledger.notification_renders),
        "notification_delivery_records": len(ledger.notification_delivery_records),
        "notification_delivery_result_events": len(ledger.notification_delivery_result_events),
    }
    duplicate_openai = FakeOpenAIClient(_openai_response(ledger.candidate_group_id))

    await _run_analysis_router_stage(ledger, ledger.event_id_for_type("analysis.requested.v1"))
    await _run_judge_stage(ledger, ledger.event_id_for_type("judge.call.requested.v1"), duplicate_openai)
    await _run_validator_stage(ledger, ledger.event_id_for_type("judge.output.ready.v1"))
    await _run_policy_stage(ledger, ledger.event_id_for_type("analysis.policy.apply.v1"))
    await _run_notifier_stage(
        ledger,
        ledger.event_id_for_type("notification.plan.created.v1"),
        telegram_client,
    )

    assert {
        "judge_runs": len(ledger.judge_runs),
        "judge_outputs": len(ledger.judge_outputs),
        "analyses": len(ledger.analyses),
        "notification_plans": len(ledger.notification_plans),
        "notification_renders": len(ledger.notification_renders),
        "notification_delivery_records": len(ledger.notification_delivery_records),
        "notification_delivery_result_events": len(ledger.notification_delivery_result_events),
    } == counts
    assert _event_type_sequence(ledger) == EVENT_SEQUENCE
    assert len(first_openai.calls) == 1
    assert duplicate_openai.calls == []
    assert telegram_client.send_calls == 0
    assert telegram_client.edit_calls == 0
    assert ledger.state_transitions[-1]["reason_code"] == "notification_duplicate_terminal_noop"


def _plan_row(plan: NotificationPlanDraft) -> dict[str, Any]:
    return {
        "notification_plan_id": plan.notification_plan_id,
        "analysis_id": plan.analysis_id,
        "candidate_group_id": plan.candidate_group_id,
        "target_chat_id": plan.target_chat_id,
        "target_thread_id": plan.target_thread_id,
        "render_profile": plan.render_profile,
        "dedupe_subject_key": plan.dedupe_subject_key,
        "material_change_hash": plan.material_change_hash,
        "send_after": plan.send_after,
        "status": plan.status,
    }


def _existing_delivery(
    record: dict[str, Any],
    plan: NotificationPlanDraft,
    candidate: dict[str, Any] | None,
) -> ExistingRecentDelivery:
    return ExistingRecentDelivery(
        notification_plan_id=plan.notification_plan_id,
        telegram_message_id=record.get("telegram_message_id"),
        telegram_chat_id=record.get("telegram_chat_id"),
        material_change_hash=plan.material_change_hash,
        primary_canonical_url=candidate["primary_canonical_url"] if candidate else None,
        urgency_profile=plan.urgency_profile,
        render_profile=plan.render_profile,
        created_at=record.get("created_at") or datetime.now(timezone.utc),
    )


def _int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def _string_or_none(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    return text if text else None


def _datetime_or_none(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
