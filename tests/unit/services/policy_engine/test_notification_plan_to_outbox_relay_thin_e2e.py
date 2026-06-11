from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

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


class _NullTransaction:
    async def __aenter__(self) -> "_NullTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _PolicyRepository:
    def __init__(
        self,
        *,
        scores: dict[str, Any],
        primary_artifact_type: str = "github_repo",
        model_proposed_verdict: str = "inspect_now",
    ) -> None:
        self.trigger_event_id = uuid4()
        self.candidate_group_id = uuid4()
        self.bundle_id = uuid4()
        self.current_primary_artifact_id = uuid4()
        self.judge_run_id = uuid4()
        self.judge_output_id = uuid4()
        self.analysis_id = uuid4()
        self.scores = scores
        self.primary_artifact_type = primary_artifact_type
        self.model_proposed_verdict = model_proposed_verdict
        self.loaded_trigger_event_ids: list[UUID] = []
        self.analyses: list[AnalysisDraft] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.notification_plan_created_rows: list[OutboxEventRow] = []
        self.notification_plans_written = 0
        self._existing_analysis: ExistingAnalysisRecord | None = None

    def transaction(self) -> _NullTransaction:
        return _NullTransaction()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> AnalysisPolicyJob | None:
        self.loaded_trigger_event_ids.append(trigger_event_id)
        if trigger_event_id != self.trigger_event_id:
            return None
        return AnalysisPolicyJob(
            trigger_event_id=self.trigger_event_id,
            event_type="analysis.policy.apply.v1",
            judge_run_id=self.judge_run_id,
            judge_output_id=self.judge_output_id,
            candidate_group_id=self.candidate_group_id,
            bundle_id=self.bundle_id,
        )

    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None:
        if candidate_group_id != self.candidate_group_id:
            return None
        return CandidatePolicyContext(
            candidate_group_id=self.candidate_group_id,
            current_bundle_id=self.bundle_id,
            current_analysis_id=None,
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None:
        if judge_run_id != self.judge_run_id:
            return None
        return JudgeRunPolicyContext(
            judge_run_id=self.judge_run_id,
            bundle_id=self.bundle_id,
            prompt_version="judge_github_primary_v1",
            policy_version="verdict_policy_v1",
            status="succeeded",
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None:
        if judge_output_id != self.judge_output_id:
            return None
        return JudgeOutputPolicyContext(
            judge_output_id=self.judge_output_id,
            judge_run_id=self.judge_run_id,
            candidate_group_id=self.candidate_group_id,
            payload_json={
                "scores": self.scores,
                "reason_codes": ["judge_output_validated"],
                "recommended_action_ko": "inspect repository",
                "freshness_note_ko": "fresh enough for operator review",
            },
            model_proposed_verdict=self.model_proposed_verdict,
            model_confidence_band="high",
            created_at=datetime.now(timezone.utc),
        )

    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None:
        if bundle_id != self.bundle_id:
            return None
        return BundlePolicyContext(
            bundle_id=self.bundle_id,
            candidate_group_id=self.candidate_group_id,
            current_primary_artifact_id=self.current_primary_artifact_id,
            current_primary_artifact_type=self.primary_artifact_type,
            created_at=datetime.now(timezone.utc),
        )

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisRecord | None:
        if self._existing_analysis is None:
            return None
        if (
            self._existing_analysis.judge_output_id == judge_output_id
            and self._existing_analysis.policy_version == policy_version
            and self._existing_analysis.delivery_policy_version == delivery_policy_version
        ):
            return self._existing_analysis
        return None

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        self.analyses.append(draft)
        self._existing_analysis = ExistingAnalysisRecord(
            analysis_id=self.analysis_id,
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        return self.analysis_id

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

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
        row = OutboxEventRow(
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
        self.notification_plan_created_rows.append(row)


class _OutboxRepository:
    def __init__(self, rows: list[OutboxEventRow]) -> None:
        self._rows = rows
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
async def test_policy_notification_plan_created_routes_to_thin_notify_queue() -> None:
    policy_repo = _PolicyRepository(scores=_send_worthy_scores())
    policy_service = PolicyEngineService(_policy_config(), repository=policy_repo)

    await policy_service.handle_trigger_event(policy_repo.trigger_event_id)

    assert policy_repo.loaded_trigger_event_ids == [policy_repo.trigger_event_id]
    assert len(policy_repo.analyses) == 1
    analysis = policy_repo.analyses[0]
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"
    assert policy_repo.state_transitions == [
        {
            "object_type": "analysis",
            "object_id": policy_repo.analysis_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_finalized",
            "reason_code": "policy_applied:inspect_now:send_now",
        }
    ]
    assert policy_repo.notification_plans_written == 0
    assert len(policy_repo.notification_plan_created_rows) == 1

    notification_row = policy_repo.notification_plan_created_rows[0]
    assert notification_row.event_type == "notification.plan.created.v1"
    assert notification_row.payload_json["analysis_id"] == str(policy_repo.analysis_id)
    assert notification_row.payload_json["candidate_group_id"] == str(policy_repo.candidate_group_id)
    assert notification_row.payload_json["delivery_decision"] == "send_now"
    assert notification_row.payload_json["urgency_profile"] == "high"
    assert notification_row.payload_json["target_chat_id"] == 12345
    assert notification_row.payload_json["target_thread_id"] is None
    assert notification_row.payload_json["render_profile"] == "telegram_single_alert_high_v1"
    assert notification_row.payload_json["dedupe_subject_key"] == str(policy_repo.candidate_group_id)
    assert notification_row.payload_json["material_change_hash"]
    assert notification_row.payload_json["send_after"] is None
    assert notification_row.payload_json["suppress_reason_code"] is None
    assert set(notification_row.payload_json) == REQUIRED_NOTIFICATION_PLAN_PAYLOAD_KEYS

    relay_repo = _OutboxRepository([notification_row])
    publisher = _RecordingPublisher()
    relay_service = OutboxRelayService(
        _outbox_config(),
        repository=relay_repo,
        publisher=publisher,
        route_resolver=OutboxRouteResolver(),
    )

    processed = await relay_service.run_once()

    assert processed == 1
    assert relay_repo.marked_published == [notification_row.event_id]
    assert relay_repo.marked_failed == []
    assert relay_repo.job_attempts == [
        {
            "stage_name": "notify",
            "queue_name": "q.notification.send",
            "root_object_type": "analysis",
            "root_object_id": policy_repo.analysis_id,
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
        "root_object_id": str(policy_repo.analysis_id),
        "idempotency_key": notification_row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(notification_row.event_id),
    }
    assert set(fields) == THIN_REDIS_FIELDS
    assert "payload_json" not in fields
    assert publisher.notifier_transport_calls == 0


@pytest.mark.asyncio
async def test_policy_suppressed_delivery_does_not_emit_notification_event() -> None:
    policy_repo = _PolicyRepository(scores=_suppress_scores(), model_proposed_verdict="skip")
    policy_service = PolicyEngineService(_policy_config(), repository=policy_repo)

    await policy_service.handle_trigger_event(policy_repo.trigger_event_id)

    assert len(policy_repo.analyses) == 1
    analysis = policy_repo.analyses[0]
    assert analysis.verdict == "skip"
    assert analysis.delivery_decision == "suppress"
    assert "verdict_skip" in analysis.reason_codes_json
    assert policy_repo.state_transitions == [
        {
            "object_type": "analysis",
            "object_id": policy_repo.analysis_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_suppressed",
            "reason_code": "policy_applied:skip:suppress",
        }
    ]
    assert policy_repo.notification_plan_created_rows == []
    assert policy_repo.notification_plans_written == 0


@pytest.mark.asyncio
async def test_duplicate_policy_apply_does_not_duplicate_analysis_or_notification_event() -> None:
    policy_repo = _PolicyRepository(scores=_send_worthy_scores())
    policy_service = PolicyEngineService(_policy_config(), repository=policy_repo)

    await policy_service.handle_trigger_event(policy_repo.trigger_event_id)
    await policy_service.handle_trigger_event(policy_repo.trigger_event_id)

    assert policy_repo.loaded_trigger_event_ids == [
        policy_repo.trigger_event_id,
        policy_repo.trigger_event_id,
    ]
    assert len(policy_repo.analyses) == 1
    assert len(policy_repo.notification_plan_created_rows) == 1
    assert len(policy_repo.state_transitions) == 1
    assert policy_repo.notification_plans_written == 0


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


def _send_worthy_scores() -> dict[str, int]:
    return {
        "practical_usefulness": 90,
        "evidence_strength": 80,
        "confidence": 85,
        "hype_penalty": 10,
        "code_quality": 80,
        "specificity": 80,
    }


def _suppress_scores() -> dict[str, int]:
    return {
        "practical_usefulness": 10,
        "evidence_strength": 10,
        "confidence": 10,
        "hype_penalty": 90,
        "code_quality": 10,
        "specificity": 10,
    }
