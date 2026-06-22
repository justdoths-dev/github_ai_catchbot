from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.analysis_validator.config import AnalysisValidatorConfig
from services.analysis_validator.models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
)
from services.analysis_validator.service import AnalysisValidatorService
from services.notifier_telegram.config import NotifierTelegramConfig
from services.notifier_telegram.models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    DeliveryResult,
    ExistingRecentDelivery,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotificationPlanDraft,
    NotificationRenderDraft,
    NotifierPlanIdempotencySnapshot,
)
from services.notifier_telegram.service import NotifierTelegramService
from services.outbox_relay.models import OutboxEventRow
from services.policy_engine.config import PolicyEngineConfig
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
from services.policy_engine.service import PolicyEngineService


class _Tx:
    async def __aenter__(self) -> "_Tx":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _RaisingTelegramTransport:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self.calls.append("send_message")
        raise AssertionError("telegram transport must not be called")

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self.calls.append("edit_message_text")
        raise AssertionError("telegram transport must not be called")


class _HotPathLedger:
    def __init__(
        self,
        *,
        scores: dict[str, int | None],
        model_proposed_verdict: str,
        mutate_payload: Any | None = None,
    ) -> None:
        self.source_message_id = uuid4()
        self.current_primary_artifact_id = uuid4()
        self.candidate_group_id = uuid4()
        self.bundle_id = uuid4()
        self.current_candidate_bundle_id = self.bundle_id
        self.judge_run_id = uuid4()
        self.judge_output_id = uuid4()
        self.ready_event_id = uuid4()
        self.analysis_id = uuid4()
        self.judge_run_status = "succeeded"
        self.private_runtime_values = {
            "database_locator_fixture": uuid4().hex,
            "redis_locator_fixture": uuid4().hex,
            "telegram_secret_fixture": uuid4().hex,
            "openai_secret_fixture": uuid4().hex,
            "private_source_fixture": uuid4().hex,
        }
        self.payload = _judge_output_payload(
            candidate_group_id=self.candidate_group_id,
            scores=scores,
            model_proposed_verdict=model_proposed_verdict,
        )
        if mutate_payload is not None:
            mutate_payload(self.payload)
        self.event_outbox: list[OutboxEventRow] = [
            OutboxEventRow(
                event_id=self.ready_event_id,
                event_type="judge.output.ready.v1",
                aggregate_type="judge_run",
                aggregate_id=self.judge_run_id,
                dedupe_key=f"judge-output-ready:{self.judge_run_id}:{self.judge_output_id}",
                payload_json={
                    "judge_run_id": str(self.judge_run_id),
                    "judge_output_id": str(self.judge_output_id),
                    "finish_reason": "completed",
                    "refusal_detected": False,
                },
                status="published",
                fail_count=0,
                created_at=datetime.now(timezone.utc),
            )
        ]
        self.state_transitions: list[dict[str, Any]] = []
        self.analyses: list[tuple[UUID, AnalysisDraft]] = []
        self.existing_analysis: ExistingAnalysisRecord | None = None
        self.plans: dict[UUID, NotificationPlanDraft] = {}
        self.renders: list[NotificationRenderDraft] = []
        self.delivery_records: list[dict[str, Any]] = []

    def transaction(self) -> _Tx:
        return _Tx()

    def row_by_id(self, event_id: UUID) -> OutboxEventRow | None:
        for row in self.event_outbox:
            if row.event_id == event_id:
                return row
        return None

    def events_of_type(self, event_type: str, *, aggregate_id: UUID | None = None) -> list[OutboxEventRow]:
        return [
            row
            for row in self.event_outbox
            if row.event_type == event_type and (aggregate_id is None or row.aggregate_id == aggregate_id)
        ]

    def append_outbox_once(self, row: OutboxEventRow) -> bool:
        if any(existing.dedupe_key == row.dedupe_key for existing in self.event_outbox):
            return False
        self.event_outbox.append(row)
        return True

    def notifier_owned_counts(self) -> dict[str, int]:
        return {
            "notification_plans": len(self.plans),
            "notification_renders": len(self.renders),
            "notification_delivery_records": len(self.delivery_records),
        }


class _ValidatorRepository:
    def __init__(self, ledger: _HotPathLedger) -> None:
        self.ledger = ledger

    def transaction(self) -> _Tx:
        return self.ledger.transaction()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeOutputReadyJob | None:
        row = self.ledger.row_by_id(trigger_event_id)
        if row is None or row.event_type != "judge.output.ready.v1":
            return None
        payload = row.payload_json
        return JudgeOutputReadyJob(
            trigger_event_id=row.event_id,
            event_type=row.event_type,
            judge_run_id=UUID(str(payload["judge_run_id"])),
            judge_output_id=UUID(str(payload["judge_output_id"])),
            finish_reason=str(payload["finish_reason"]),
            refusal_detected=bool(payload["refusal_detected"]),
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunValidationRecord | None:
        if judge_run_id != self.ledger.judge_run_id:
            return None
        return JudgeRunValidationRecord(
            judge_run_id=self.ledger.judge_run_id,
            bundle_id=self.ledger.bundle_id,
            judge_profile="github_primary",
            schema_version="judge_output_v1",
            policy_version="verdict_policy_v1",
            status=self.ledger.judge_run_status,
            finish_reason="completed",
            refusal_detected=False,
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputRecord | None:
        if judge_output_id != self.ledger.judge_output_id:
            return None
        return JudgeOutputRecord(
            judge_output_id=self.ledger.judge_output_id,
            judge_run_id=self.ledger.judge_run_id,
            candidate_group_id=self.ledger.candidate_group_id,
            judge_schema_version="judge_output_v1",
            payload_json=self.ledger.payload,
            model_proposed_verdict=_string_or_none(self.ledger.payload.get("model_proposed_verdict")),
            model_confidence_band=_string_or_none(self.ledger.payload.get("model_confidence_band")),
            created_at=datetime.now(timezone.utc),
        )

    async def load_bundle_context(self, bundle_id: UUID) -> BundleValidationContext | None:
        if bundle_id != self.ledger.bundle_id:
            return None
        return BundleValidationContext(
            bundle_id=self.ledger.bundle_id,
            candidate_group_id=self.ledger.candidate_group_id,
            current_primary_artifact_id=self.ledger.current_primary_artifact_id,
            current_primary_artifact_type="github_repo",
            created_at=datetime.now(timezone.utc),
        )

    async def update_judge_run_status(self, *, judge_run_id: UUID, status: str, finish_reason: str | None) -> None:
        del finish_reason
        if judge_run_id == self.ledger.judge_run_id:
            self.ledger.judge_run_status = status

    async def insert_state_transition(self, **kwargs: Any) -> None:
        self.ledger.state_transitions.append(kwargs)

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
        return self.ledger.append_outbox_once(
            OutboxEventRow(
                event_id=uuid4(),
                event_type="analysis.policy.apply.v1",
                aggregate_type="judge_run",
                aggregate_id=judge_run_id,
                dedupe_key=f"analysis-policy-apply:{judge_run_id}:{judge_output_id}",
                payload_json=payload,
                status="pending",
                fail_count=0,
                created_at=datetime.now(timezone.utc),
            )
        )


class _PolicyRepository:
    def __init__(self, ledger: _HotPathLedger) -> None:
        self.ledger = ledger

    def transaction(self) -> _Tx:
        return self.ledger.transaction()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> AnalysisPolicyJob | None:
        row = self.ledger.row_by_id(trigger_event_id)
        if row is None or row.event_type != "analysis.policy.apply.v1":
            return None
        payload = row.payload_json
        return AnalysisPolicyJob(
            trigger_event_id=row.event_id,
            event_type=row.event_type,
            judge_run_id=UUID(str(payload["judge_run_id"])),
            judge_output_id=UUID(str(payload["judge_output_id"])),
            candidate_group_id=UUID(str(payload["candidate_group_id"])),
            bundle_id=UUID(str(payload["bundle_id"])),
        )

    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None:
        if candidate_group_id != self.ledger.candidate_group_id:
            return None
        return CandidatePolicyContext(
            candidate_group_id=self.ledger.candidate_group_id,
            current_bundle_id=self.ledger.current_candidate_bundle_id,
            current_analysis_id=self.ledger.existing_analysis.analysis_id if self.ledger.existing_analysis else None,
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None:
        if judge_run_id != self.ledger.judge_run_id:
            return None
        return JudgeRunPolicyContext(
            judge_run_id=self.ledger.judge_run_id,
            bundle_id=self.ledger.bundle_id,
            prompt_version="judge_github_primary_v1",
            policy_version="verdict_policy_v1",
            status=self.ledger.judge_run_status,
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None:
        if judge_output_id != self.ledger.judge_output_id:
            return None
        return JudgeOutputPolicyContext(
            judge_output_id=self.ledger.judge_output_id,
            judge_run_id=self.ledger.judge_run_id,
            candidate_group_id=self.ledger.candidate_group_id,
            payload_json=self.ledger.payload,
            model_proposed_verdict=_string_or_none(self.ledger.payload.get("model_proposed_verdict")),
            model_confidence_band=_string_or_none(self.ledger.payload.get("model_confidence_band")),
            created_at=datetime.now(timezone.utc),
            judge_schema_version="judge_output_v1",
        )

    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None:
        if bundle_id != self.ledger.bundle_id:
            return None
        return BundlePolicyContext(
            bundle_id=self.ledger.bundle_id,
            candidate_group_id=self.ledger.candidate_group_id,
            current_primary_artifact_id=self.ledger.current_primary_artifact_id,
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
        self.ledger.existing_analysis = ExistingAnalysisRecord(
            analysis_id=self.ledger.analysis_id,
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        self.ledger.analyses.append((self.ledger.analysis_id, draft))
        return self.ledger.analysis_id

    async def insert_state_transition(self, **kwargs: Any) -> None:
        self.ledger.state_transitions.append(kwargs)

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
        self.ledger.append_outbox_once(
            OutboxEventRow(
                event_id=uuid4(),
                event_type="notification.plan.created.v1",
                aggregate_type="analysis",
                aggregate_id=intent.analysis_id,
                dedupe_key=(
                    f"notification-plan-created:{intent.analysis_id}:"
                    f"{intent.target_chat_id}:{intent.material_change_hash}"
                ),
                payload_json=payload,
                status="pending",
                fail_count=0,
                created_at=datetime.now(timezone.utc),
            )
        )


class _NotifierRepository:
    def __init__(self, ledger: _HotPathLedger) -> None:
        self.ledger = ledger

    def transaction(self) -> _Tx:
        return self.ledger.transaction()

    async def load_intent_job(self, trigger_event_id: UUID) -> NotificationIntentJob | None:
        row = self.ledger.row_by_id(trigger_event_id)
        if row is None or row.event_type != "notification.plan.created.v1":
            return None
        payload = row.payload_json
        notification_plan_id = _uuid_or_none(payload.get("notification_plan_id"))
        analysis_id = _uuid_or_none(payload.get("analysis_id"))
        candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
        target_chat_id = _int_or_none(payload.get("target_chat_id"))
        if None in {notification_plan_id, analysis_id, candidate_group_id, target_chat_id}:
            return None
        return NotificationIntentJob(
            trigger_event_id=row.event_id,
            event_type=row.event_type,
            notification_plan_id=notification_plan_id,
            analysis_id=analysis_id,
            candidate_group_id=candidate_group_id,
            delivery_decision=str(payload["delivery_decision"]),  # type: ignore[arg-type]
            urgency_profile=str(payload["urgency_profile"]),  # type: ignore[arg-type]
            target_chat_id=int(target_chat_id),
            target_thread_id=_int_or_none(payload.get("target_thread_id")),
            render_profile=_string_or_none(payload.get("render_profile")),
            dedupe_subject_key=str(payload["dedupe_subject_key"]),
            material_change_hash=str(payload["material_change_hash"]),
            send_after=None,
            suppress_reason_code=_string_or_none(payload.get("suppress_reason_code")),
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

    async def insert_notification_plan(self, draft: NotificationPlanDraft) -> UUID:
        existing = await self.load_existing_plan_by_material(
            analysis_id=draft.analysis_id,
            target_chat_id=draft.target_chat_id,
            material_change_hash=draft.material_change_hash,
        )
        if existing is not None:
            return UUID(str(existing["notification_plan_id"]))
        self.ledger.plans[draft.notification_plan_id] = draft
        return draft.notification_plan_id

    async def load_analysis(self, analysis_id: UUID) -> AnalysisRenderContext | None:
        for current_analysis_id, analysis in self.ledger.analyses:
            if current_analysis_id != analysis_id:
                continue
            return AnalysisRenderContext(
                analysis_id=current_analysis_id,
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
            judge_output_id=self.ledger.judge_output_id,
            payload_json=self.ledger.payload,
            model_confidence_band=_string_or_none(self.ledger.payload.get("model_confidence_band")),
        )

    async def load_candidate_render_context(self, candidate_group_id: UUID) -> CandidateRenderContext | None:
        if candidate_group_id != self.ledger.candidate_group_id:
            return None
        return CandidateRenderContext(
            candidate_group_id=self.ledger.candidate_group_id,
            source_message_id=self.ledger.source_message_id,
            current_primary_artifact_id=self.ledger.current_primary_artifact_id,
            primary_artifact_type="github_repo",
            primary_canonical_url=None,
            primary_canonical_id="candidate-fixture",
            source_message_link=None,
            source_text_surface=None,
        )

    async def load_successful_delivery_for_material(
        self,
        *,
        dedupe_subject_key: str,
        target_chat_id: int,
        material_change_hash: str,
    ) -> ExistingRecentDelivery | None:
        del dedupe_subject_key, target_chat_id, material_change_hash
        return None

    async def load_recent_successful_delivery(
        self,
        *,
        dedupe_subject_key: str,
        target_chat_id: int,
    ) -> ExistingRecentDelivery | None:
        del dedupe_subject_key, target_chat_id
        return None

    async def has_previous_edit_restriction(self, *, notification_plan_id: UUID) -> bool:
        del notification_plan_id
        return False

    async def count_delivery_attempts(self, *, notification_plan_id: UUID) -> int:
        return sum(
            1 for record in self.ledger.delivery_records if record["notification_plan_id"] == notification_plan_id
        )

    async def insert_notification_render(self, draft: NotificationRenderDraft) -> UUID | None:
        for existing in self.ledger.renders:
            if existing.notification_plan_id == draft.notification_plan_id and existing.render_hash == draft.render_hash:
                return None
        self.ledger.renders.append(draft)
        return uuid4()

    async def insert_delivery_record(self, **kwargs: Any) -> UUID:
        record_id = uuid4()
        self.ledger.delivery_records.append(
            {"notification_delivery_record_id": record_id, "created_at": datetime.now(timezone.utc), **kwargs}
        )
        return record_id

    async def update_plan_status(
        self,
        *,
        notification_plan_id: UUID,
        status: str,
        send_after: datetime | None = None,
    ) -> None:
        plan = self.ledger.plans.get(notification_plan_id)
        if plan is not None:
            self.ledger.plans[notification_plan_id] = replace(plan, status=status, send_after=send_after or plan.send_after)

    async def insert_state_transition(self, **kwargs: Any) -> None:
        self.ledger.state_transitions.append(kwargs)

    async def insert_delivery_result_outbox(
        self,
        *,
        notification_plan_id: UUID,
        delivery_status: str,
        telegram_chat_id: int | None,
        telegram_message_id: int | None,
        notification_delivery_record_id: UUID,
        attempt_count: int,
        transport_error_code: str | None,
        transport_error_class: str | None,
        edited: bool,
    ) -> None:
        payload = {
            "notification_plan_id": str(notification_plan_id),
            "notification_delivery_record_id": str(notification_delivery_record_id),
            "delivery_status": delivery_status,
            "telegram_chat_id": telegram_chat_id,
            "telegram_message_id": telegram_message_id,
            "attempt_count": attempt_count,
            "transport_error_code": transport_error_code,
            "transport_error_class": transport_error_class,
            "edited": edited,
        }
        self.ledger.append_outbox_once(
            OutboxEventRow(
                event_id=uuid4(),
                event_type="notification.delivery.result.v1",
                aggregate_type="notification_plan",
                aggregate_id=notification_plan_id,
                dedupe_key=f"notification-delivery-result:{notification_plan_id}:{notification_delivery_record_id}",
                payload_json=payload,
                status="pending",
                fail_count=0,
                created_at=datetime.now(timezone.utc),
            )
        )

    async def load_idempotency_plan_snapshots(
        self,
        intent: NotificationIntentJob,
    ) -> list[NotifierPlanIdempotencySnapshot]:
        snapshots: list[NotifierPlanIdempotencySnapshot] = []
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
            sent_records = [record for record in records if record["result_status"] in {"sent", "edited"}]
            snapshots.append(
                NotifierPlanIdempotencySnapshot(
                    notification_plan_id=plan.notification_plan_id,
                    status=plan.status,
                    render_count=sum(
                        1 for render in self.ledger.renders if render.notification_plan_id == plan.notification_plan_id
                    ),
                    delivery_record_count=len(records),
                    sent_delivery_count=len(sent_records),
                    suppressed_delivery_count=sum(1 for record in records if record["result_status"] == "suppressed"),
                    terminal_delivery_count=sum(
                        1
                        for record in records
                        if record["result_status"] in {"sent", "edited", "suppressed", "failed_terminal"}
                    ),
                    retryable_failure_count=sum(1 for record in records if record["result_status"] == "failed_retryable"),
                    sent_delivery_chat_id_present_count=sum(
                        1 for record in sent_records if record.get("telegram_chat_id") is not None
                    ),
                    sent_delivery_message_id_present_count=sum(
                        1 for record in sent_records if record.get("telegram_message_id") is not None
                    ),
                )
            )
        return snapshots


@pytest.mark.asyncio
async def test_valid_send_now_reaches_send_disabled_render_and_replays_without_duplicates() -> None:
    ledger = _HotPathLedger(scores=_send_worthy_scores(), model_proposed_verdict="inspect_now")
    validator = AnalysisValidatorService(_validator_config(), repository=_ValidatorRepository(ledger))
    policy = PolicyEngineService(_policy_config(), repository=_PolicyRepository(ledger))
    telegram = _RaisingTelegramTransport()
    notifier = NotifierTelegramService(
        _notifier_config(),
        repository=_NotifierRepository(ledger),
        telegram_client=telegram,
    )

    await validator.handle_trigger_event(ledger.ready_event_id)
    await validator.handle_trigger_event(ledger.ready_event_id)

    policy_events = ledger.events_of_type("analysis.policy.apply.v1", aggregate_id=ledger.judge_run_id)
    assert len(policy_events) == 1
    assert _transitions(ledger, object_type="judge_run", to_state="analysis_validated") == [
        {
            "object_type": "judge_run",
            "object_id": ledger.judge_run_id,
            "from_state": "succeeded",
            "to_state": "analysis_validated",
            "reason_code": "validator_passed",
        }
    ]

    await policy.handle_trigger_event(policy_events[0].event_id)
    await policy.handle_trigger_event(policy_events[0].event_id)

    assert len(ledger.analyses) == 1
    analysis_id, analysis = ledger.analyses[0]
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"
    assert analysis.model_proposed_verdict == "inspect_now"
    assert ledger.notifier_owned_counts() == {
        "notification_plans": 0,
        "notification_renders": 0,
        "notification_delivery_records": 0,
    }

    notification_events = ledger.events_of_type("notification.plan.created.v1", aggregate_id=analysis_id)
    assert len(notification_events) == 1
    notification_payload = notification_events[0].payload_json
    assert notification_payload["analysis_id"] == str(analysis_id)
    assert notification_payload["delivery_decision"] == "send_now"
    assert notification_payload["urgency_profile"] == "high"
    assert notification_payload["material_change_hash"]

    first_result = await notifier.handle_trigger_event(notification_events[0].event_id)
    second_result = await notifier.handle_trigger_event(notification_events[0].event_id)

    assert isinstance(first_result, DeliveryResult)
    assert isinstance(second_result, DeliveryResult)
    assert first_result.delivery_status == "suppressed"
    assert first_result.transport_error_code == "dry_run_skip_transport"
    assert second_result.delivery_status == "suppressed"
    assert second_result.transport_error_code == "notification_existing_suppressed_noop"
    assert telegram.calls == []
    assert ledger.notifier_owned_counts() == {
        "notification_plans": 1,
        "notification_renders": 1,
        "notification_delivery_records": 1,
    }
    assert len(ledger.events_of_type("notification.delivery.result.v1")) == 1

    plan = next(iter(ledger.plans.values()))
    render = ledger.renders[0]
    record = ledger.delivery_records[0]
    assert plan.status == "suppressed"
    assert plan.material_change_hash != render.render_hash
    assert record["result_status"] == "suppressed"
    assert record["telegram_chat_id"] == 12345
    assert record["telegram_message_id"] is None
    assert record["telegram_response_json"] == {
        "dry_run": True,
        "send_disabled": True,
        "send_enabled": False,
        "transport_skipped": True,
        "reason_code": "dry_run_skip_transport",
        "delivery_action": "send",
    }
    _assert_no_private_runtime_values(ledger)


@pytest.mark.asyncio
async def test_invalid_judge_output_stops_in_validator_without_downstream_writes() -> None:
    ledger = _HotPathLedger(
        scores=_send_worthy_scores(),
        model_proposed_verdict="inspect_now",
        mutate_payload=lambda payload: payload.pop("headline"),
    )
    validator = AnalysisValidatorService(_validator_config(), repository=_ValidatorRepository(ledger))

    await validator.handle_trigger_event(ledger.ready_event_id)

    assert ledger.events_of_type("analysis.policy.apply.v1") == []
    assert ledger.analyses == []
    assert ledger.events_of_type("notification.plan.created.v1") == []
    assert ledger.notifier_owned_counts() == {
        "notification_plans": 0,
        "notification_renders": 0,
        "notification_delivery_records": 0,
    }
    assert ledger.judge_run_status == "failed_terminal"
    assert _transitions(ledger, object_type="judge_run", to_state="analysis_failed_schema")


@pytest.mark.asyncio
async def test_stale_bundle_policy_input_fails_closed_without_analysis_or_notification() -> None:
    ledger = _HotPathLedger(scores=_send_worthy_scores(), model_proposed_verdict="inspect_now")
    validator = AnalysisValidatorService(_validator_config(), repository=_ValidatorRepository(ledger))
    policy = PolicyEngineService(_policy_config(), repository=_PolicyRepository(ledger))

    await validator.handle_trigger_event(ledger.ready_event_id)
    policy_event = ledger.events_of_type("analysis.policy.apply.v1")[0]
    ledger.current_candidate_bundle_id = uuid4()

    await policy.handle_trigger_event(policy_event.event_id)

    assert ledger.analyses == []
    assert ledger.events_of_type("notification.plan.created.v1") == []
    assert ledger.notifier_owned_counts() == {
        "notification_plans": 0,
        "notification_renders": 0,
        "notification_delivery_records": 0,
    }
    assert _transitions(ledger, object_type="candidate_group", to_state="analysis_policy_stale_bundle") == [
        {
            "object_type": "candidate_group",
            "object_id": ledger.candidate_group_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_policy_stale_bundle",
            "reason_code": "policy_stale_bundle_request",
        }
    ]


@pytest.mark.asyncio
async def test_suppressed_policy_result_does_not_create_notification_intent_or_render() -> None:
    ledger = _HotPathLedger(scores=_suppress_scores(), model_proposed_verdict="skip")
    validator = AnalysisValidatorService(_validator_config(), repository=_ValidatorRepository(ledger))
    policy = PolicyEngineService(_policy_config(), repository=_PolicyRepository(ledger))

    await validator.handle_trigger_event(ledger.ready_event_id)
    policy_event = ledger.events_of_type("analysis.policy.apply.v1")[0]
    await policy.handle_trigger_event(policy_event.event_id)

    assert len(ledger.analyses) == 1
    _analysis_id, analysis = ledger.analyses[0]
    assert analysis.verdict == "skip"
    assert analysis.delivery_decision == "suppress"
    assert "policy_verdict_skip" in analysis.reason_codes_json
    assert ledger.events_of_type("notification.plan.created.v1") == []
    assert ledger.notifier_owned_counts() == {
        "notification_plans": 0,
        "notification_renders": 0,
        "notification_delivery_records": 0,
    }


def _validator_config() -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url="database_locator",
        redis_url="redis_locator",
        queue_name="q.analysis.validate",
        consumer_group="analysis-validator",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="INFO",
    )


def _policy_config() -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url="database_locator",
        redis_url="redis_locator",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
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
        database_url="database_locator",
        redis_url="redis_locator",
        telegram_bot_token="",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        dry_run=True,
        allow_edits=False,
        enable_notification_send=False,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="api_base_locator",
        request_timeout_sec=10,
        log_level="INFO",
    )


def _judge_output_payload(
    *,
    candidate_group_id: UUID,
    scores: dict[str, int | None],
    model_proposed_verdict: str,
) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful repository",
        "summary_one_line_ko": "short summary",
        "skeptical_take_ko": "needs more evidence before acting",
        "why_it_might_matter_ko": "could help workflow automation",
        "comparables": ["existing-tool"],
        "scores": scores,
        "reason_codes": ["judge_output_validated"],
        "red_flags_ko": ["production use is still unclear"],
        "evidence_limitations_ko": ["only fixture evidence was checked"],
        "recommended_action_ko": "inspect repository",
        "freshness_note_ko": "fixture freshness",
        "model_proposed_verdict": model_proposed_verdict,
        "model_confidence_band": "high",
    }


def _send_worthy_scores() -> dict[str, int | None]:
    return {
        "novelty": 82,
        "practical_usefulness": 90,
        "evidence_strength": 80,
        "hype_penalty": 10,
        "confidence": 85,
        "code_quality": 80,
        "maintenance_signal": 75,
        "specificity": 80,
        "reproducibility_signal": 70,
    }


def _suppress_scores() -> dict[str, int | None]:
    return {
        "novelty": 20,
        "practical_usefulness": 20,
        "evidence_strength": 20,
        "hype_penalty": 70,
        "confidence": 20,
        "code_quality": 20,
        "maintenance_signal": 20,
        "specificity": 20,
        "reproducibility_signal": 20,
    }


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


def _transitions(ledger: _HotPathLedger, *, object_type: str, to_state: str) -> list[dict[str, Any]]:
    return [
        transition
        for transition in ledger.state_transitions
        if transition["object_type"] == object_type and transition["to_state"] == to_state
    ]


def _assert_no_private_runtime_values(ledger: _HotPathLedger) -> None:
    public_evidence = repr(
        {
            "outbox_payloads": [row.payload_json for row in ledger.event_outbox],
            "delivery_records": ledger.delivery_records,
            "state_transitions": ledger.state_transitions,
        }
    )
    for value in ledger.private_runtime_values.values():
        assert value not in public_evidence


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
