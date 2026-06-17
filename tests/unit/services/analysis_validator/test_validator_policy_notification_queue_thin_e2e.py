from __future__ import annotations

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
from services.outbox_relay.config import OutboxRelayConfig
from services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from services.outbox_relay.routing import OutboxRouteResolver
from services.outbox_relay.service import OutboxRelayService
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


REQUIRED_NOTIFICATION_PLAN_PAYLOAD_KEYS = {
    "notification_plan_id",
    "analysis_id",
    "candidate_group_id",
    "delivery_decision",
    "urgency_profile",
    "target_chat_id",
    "target_thread_id",
    "render_profile",
    "dedupe_subject_key",
    "material_change_hash",
    "send_after",
    "suppress_reason_code",
}

THIN_REDIS_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}

NO_EXTERNAL_AUTHORITY = {
    "telegram": 0,
    "openai": 0,
    "github": 0,
    "docker": 0,
    "systemd": 0,
    "alembic": 0,
    "ddl": 0,
}


class _NullTransaction:
    async def __aenter__(self) -> "_NullTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _ValidatorPolicyLedger:
    def __init__(
        self,
        *,
        scores: dict[str, int | None],
        model_proposed_verdict: str,
        mutate_payload: Any | None = None,
    ) -> None:
        self.trigger_event_id = uuid4()
        self.candidate_group_id = uuid4()
        self.bundle_id = uuid4()
        self.current_primary_artifact_id = uuid4()
        self.judge_run_id = uuid4()
        self.judge_output_id = uuid4()
        self.analysis_id = uuid4()
        self.judge_run_status = "succeeded"
        self.payload = _judge_output_payload(
            candidate_group_id=self.candidate_group_id,
            scores=scores,
            model_proposed_verdict=model_proposed_verdict,
        )
        if mutate_payload is not None:
            mutate_payload(self.payload)

        self.event_outbox = [
            OutboxEventRow(
                event_id=self.trigger_event_id,
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
        self.judge_run_status_updates: list[dict[str, Any]] = []
        self.analyses: list[AnalysisDraft] = []
        self.existing_analysis: ExistingAnalysisRecord | None = None
        self.notification_plans: list[dict[str, Any]] = []
        self.notification_renders: list[dict[str, Any]] = []
        self.notification_delivery_records: list[dict[str, Any]] = []
        self.authority_calls = dict(NO_EXTERNAL_AUTHORITY)

    def row_by_id(self, event_id: UUID) -> OutboxEventRow | None:
        for row in self.event_outbox:
            if row.event_id == event_id:
                return row
        return None

    def rows_of_type(self, event_type: str) -> list[OutboxEventRow]:
        return [row for row in self.event_outbox if row.event_type == event_type]

    def append_outbox_once(self, row: OutboxEventRow) -> None:
        if any(existing.dedupe_key == row.dedupe_key for existing in self.event_outbox):
            return
        self.event_outbox.append(row)


class _ValidatorRepository:
    def __init__(self, ledger: _ValidatorPolicyLedger) -> None:
        self.ledger = ledger

    def transaction(self) -> _NullTransaction:
        return _NullTransaction()

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

    async def update_judge_run_status(self, **kwargs) -> None:
        self.ledger.judge_run_status_updates.append(kwargs)
        if kwargs.get("judge_run_id") == self.ledger.judge_run_id:
            self.ledger.judge_run_status = str(kwargs["status"])

    async def insert_state_transition(self, **kwargs) -> None:
        self.ledger.state_transitions.append(kwargs)

    async def insert_analysis_policy_apply_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> None:
        payload = {
            "judge_run_id": str(judge_run_id),
            "judge_output_id": str(judge_output_id),
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
        }
        self.ledger.append_outbox_once(
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
    def __init__(self, ledger: _ValidatorPolicyLedger) -> None:
        self.ledger = ledger
        self.loaded_trigger_event_ids: list[UUID] = []

    def transaction(self) -> _NullTransaction:
        return _NullTransaction()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> AnalysisPolicyJob | None:
        self.loaded_trigger_event_ids.append(trigger_event_id)
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
            current_bundle_id=self.ledger.bundle_id,
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
        self.ledger.analyses.append(draft)
        self.ledger.existing_analysis = ExistingAnalysisRecord(
            analysis_id=self.ledger.analysis_id,
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        return self.ledger.analysis_id

    async def insert_state_transition(self, **kwargs) -> None:
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


class _OutboxRepository:
    def __init__(self, rows: list[OutboxEventRow]) -> None:
        self._rows = list(rows)
        self.marked_published: list[UUID] = []
        self.marked_failed: list[UUID] = []
        self.job_attempts: list[dict[str, Any]] = []

    async def fetch_pending_batch(self, *, limit: int) -> list[OutboxEventRow]:
        batch = self._rows[:limit]
        self._rows = self._rows[limit:]
        return batch

    async def mark_published(self, *, event_id: UUID, published_at: datetime) -> None:
        del published_at
        self.marked_published.append(event_id)

    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None:
        del error_text
        self.marked_failed.append(event_id)

    async def insert_job_attempt(self, **kwargs) -> None:
        self.job_attempts.append(kwargs)


class _RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[QueueRoute, RedisQueuedMessage]] = []
        self.notifier_transport_calls = 0

    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        self.published.append((route, message))
        return "0-1"


@pytest.mark.asyncio
async def test_validator_policy_notification_queue_send_worthy_thin_e2e() -> None:
    ledger = _ValidatorPolicyLedger(scores=_send_worthy_scores(), model_proposed_verdict="inspect_now")
    validator_repo, policy_repo, relay_repo, publisher, processed = await _run_pipeline(ledger)

    policy_apply_events = ledger.rows_of_type("analysis.policy.apply.v1")
    assert len(policy_apply_events) == 1
    assert policy_apply_events[0].payload_json == {
        "judge_run_id": str(ledger.judge_run_id),
        "judge_output_id": str(ledger.judge_output_id),
        "candidate_group_id": str(ledger.candidate_group_id),
        "bundle_id": str(ledger.bundle_id),
    }
    assert validator_repo.ledger.state_transitions[0] == {
        "object_type": "judge_run",
        "object_id": ledger.judge_run_id,
        "from_state": "succeeded",
        "to_state": "analysis_validated",
        "reason_code": "validator_passed",
    }

    assert policy_repo.loaded_trigger_event_ids == [policy_apply_events[0].event_id]
    assert len(ledger.analyses) == 1
    analysis = ledger.analyses[0]
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"

    notification_events = ledger.rows_of_type("notification.plan.created.v1")
    assert len(notification_events) == 1
    notification_row = notification_events[0]
    assert notification_row.event_type == "notification.plan.created.v1"
    assert notification_row.aggregate_type == "analysis"
    assert notification_row.aggregate_id == ledger.analysis_id
    assert notification_row.payload_json["analysis_id"] == str(ledger.analysis_id)
    assert notification_row.payload_json["candidate_group_id"] == str(ledger.candidate_group_id)
    assert notification_row.payload_json["delivery_decision"] == "send_now"
    assert notification_row.payload_json["urgency_profile"] == "high"
    assert notification_row.payload_json["target_chat_id"] == 12345
    assert notification_row.payload_json["target_thread_id"] is None
    assert notification_row.payload_json["render_profile"] == "telegram_single_alert_high_v1"
    assert notification_row.payload_json["dedupe_subject_key"] == str(ledger.candidate_group_id)
    assert notification_row.payload_json["material_change_hash"]
    assert notification_row.payload_json["send_after"] is None
    assert notification_row.payload_json["suppress_reason_code"] is None
    assert set(notification_row.payload_json) == REQUIRED_NOTIFICATION_PLAN_PAYLOAD_KEYS

    assert processed == 1
    assert relay_repo.marked_published == [notification_row.event_id]
    assert relay_repo.marked_failed == []
    assert relay_repo.job_attempts == [
        {
            "stage_name": "notify",
            "queue_name": "q.notification.send",
            "root_object_type": "analysis",
            "root_object_id": ledger.analysis_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]
    assert len(publisher.published) == 1
    route, message = publisher.published[0]
    assert route.queue_name == "q.notification.send"
    assert route.stage_name == "notify"
    fields = message.as_stream_fields()
    assert fields == {
        "job_id": str(notification_row.event_id),
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": str(ledger.analysis_id),
        "idempotency_key": notification_row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(notification_row.event_id),
    }
    assert set(fields) == THIN_REDIS_FIELDS
    assert "payload_json" not in fields
    assert publisher.notifier_transport_calls == 0
    _assert_no_notifier_owned_writes(ledger)
    _assert_no_external_authority(ledger)


@pytest.mark.asyncio
async def test_validator_policy_suppress_path_stops_before_notification_queue() -> None:
    ledger = _ValidatorPolicyLedger(scores=_suppress_scores(), model_proposed_verdict="skip")
    _validator_repo, policy_repo, relay_repo, publisher, processed = await _run_pipeline(ledger)

    policy_apply_events = ledger.rows_of_type("analysis.policy.apply.v1")
    assert len(policy_apply_events) == 1
    assert policy_repo.loaded_trigger_event_ids == [policy_apply_events[0].event_id]
    assert len(ledger.analyses) == 1
    analysis = ledger.analyses[0]
    assert analysis.verdict == "skip"
    assert analysis.delivery_decision == "suppress"
    assert "policy_verdict_skip" in analysis.reason_codes_json
    assert ledger.rows_of_type("notification.plan.created.v1") == []
    assert processed == 0
    assert relay_repo.marked_published == []
    assert publisher.published == []
    _assert_no_notifier_owned_writes(ledger)
    _assert_no_external_authority(ledger)


@pytest.mark.asyncio
async def test_validator_invalid_judge_output_fails_closed_before_policy() -> None:
    ledger = _ValidatorPolicyLedger(
        scores=_send_worthy_scores(),
        model_proposed_verdict="inspect_now",
        mutate_payload=lambda payload: payload.pop("headline"),
    )
    validator_repo, policy_repo, relay_repo, publisher, processed = await _run_pipeline(ledger)

    assert ledger.rows_of_type("analysis.policy.apply.v1") == []
    assert policy_repo.loaded_trigger_event_ids == []
    assert ledger.analyses == []
    assert ledger.rows_of_type("notification.plan.created.v1") == []
    assert processed == 0
    assert relay_repo.marked_published == []
    assert publisher.published == []
    assert validator_repo.ledger.judge_run_status_updates == [
        {
            "judge_run_id": ledger.judge_run_id,
            "status": "failed_terminal",
            "finish_reason": "validator_schema_invalid",
        }
    ]
    assert ledger.state_transitions == [
        {
            "object_type": "judge_run",
            "object_id": ledger.judge_run_id,
            "from_state": "succeeded",
            "to_state": "analysis_failed_schema",
            "reason_code": "validator_schema_invalid",
        }
    ]
    _assert_no_notifier_owned_writes(ledger)
    _assert_no_external_authority(ledger)


@pytest.mark.asyncio
async def test_duplicate_validator_and_policy_trigger_dedupes_terminal_handoffs() -> None:
    ledger = _ValidatorPolicyLedger(scores=_send_worthy_scores(), model_proposed_verdict="inspect_now")
    validator_repo = _ValidatorRepository(ledger)
    policy_repo = _PolicyRepository(ledger)
    validator_service = AnalysisValidatorService(_validator_config(), repository=validator_repo)
    policy_service = PolicyEngineService(_policy_config(), repository=policy_repo)

    await validator_service.handle_trigger_event(ledger.trigger_event_id)
    await validator_service.handle_trigger_event(ledger.trigger_event_id)

    policy_apply_events = ledger.rows_of_type("analysis.policy.apply.v1")
    assert len(policy_apply_events) == 1

    await policy_service.handle_trigger_event(policy_apply_events[0].event_id)
    await policy_service.handle_trigger_event(policy_apply_events[0].event_id)

    notification_events = ledger.rows_of_type("notification.plan.created.v1")
    assert policy_repo.loaded_trigger_event_ids == [
        policy_apply_events[0].event_id,
        policy_apply_events[0].event_id,
    ]
    assert len(ledger.analyses) == 1
    assert len(notification_events) == 1
    assert len([row for row in ledger.state_transitions if row["object_type"] == "analysis"]) == 1

    relay_repo = _OutboxRepository(notification_events)
    publisher = _RecordingPublisher()
    relay_service = OutboxRelayService(
        _outbox_config(),
        repository=relay_repo,
        publisher=publisher,
        route_resolver=OutboxRouteResolver(),
    )
    assert await relay_service.run_once() == 1
    assert await relay_service.run_once() == 0
    assert len(publisher.published) == 1
    _assert_no_notifier_owned_writes(ledger)
    _assert_no_external_authority(ledger)


async def _run_pipeline(
    ledger: _ValidatorPolicyLedger,
) -> tuple[_ValidatorRepository, _PolicyRepository, _OutboxRepository, _RecordingPublisher, int]:
    validator_repo = _ValidatorRepository(ledger)
    policy_repo = _PolicyRepository(ledger)
    validator_service = AnalysisValidatorService(_validator_config(), repository=validator_repo)
    policy_service = PolicyEngineService(_policy_config(), repository=policy_repo)

    await validator_service.handle_trigger_event(ledger.trigger_event_id)
    for event in ledger.rows_of_type("analysis.policy.apply.v1"):
        await policy_service.handle_trigger_event(event.event_id)

    relay_repo = _OutboxRepository(ledger.rows_of_type("notification.plan.created.v1"))
    publisher = _RecordingPublisher()
    relay_service = OutboxRelayService(
        _outbox_config(),
        repository=relay_repo,
        publisher=publisher,
        route_resolver=OutboxRouteResolver(),
    )
    processed = await relay_service.run_once()
    return validator_repo, policy_repo, relay_repo, publisher, processed


def _validator_config() -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url="unused",
        redis_url="unused",
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
        database_url="unused",
        redis_url="unused",
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


def _outbox_config() -> OutboxRelayConfig:
    return OutboxRelayConfig(
        app_env="test",
        database_url="unused",
        redis_url="unused",
        poll_interval_ms=1000,
        batch_size=10,
        xadd_maxlen=10000,
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
        "evidence_limitations_ko": ["only public docs were checked"],
        "recommended_action_ko": "inspect repository",
        "freshness_note_ko": "recent activity needs verification",
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
        "practical_usefulness": 10,
        "evidence_strength": 10,
        "hype_penalty": 90,
        "confidence": 10,
        "code_quality": 10,
        "maintenance_signal": 10,
        "specificity": 10,
        "reproducibility_signal": 10,
    }


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _assert_no_notifier_owned_writes(ledger: _ValidatorPolicyLedger) -> None:
    assert ledger.notification_plans == []
    assert ledger.notification_renders == []
    assert ledger.notification_delivery_records == []


def _assert_no_external_authority(ledger: _ValidatorPolicyLedger) -> None:
    assert ledger.authority_calls == NO_EXTERNAL_AUTHORITY
