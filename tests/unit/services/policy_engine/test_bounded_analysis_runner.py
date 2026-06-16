from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from src.services.outbox_relay.models import OutboxEventRow, QueueRoute
from src.services.policy_engine.bounded_analysis_runner import (
    BoundedPolicyEngineAnalysisConfig,
    BoundedPolicyEngineAnalysisRuntimeConfig,
    BoundedPolicyEngineRedisPublisherHandle,
    BoundedPolicyEngineRedisReaderHandle,
    BoundedPolicyEngineRepositoryHandle,
    RedisStreamMessage,
    SqlAlchemyBoundedPolicyEngineAnalysisRepository,
    run_bounded_policy_engine_analysis_sync,
)
from src.services.policy_engine.delivery_policy import DeliveryPolicy
from src.services.policy_engine.models import (
    AnalysisDraft,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
)
from src.services.policy_engine.verdict_policy import VerdictPolicy, reconcile_model_verdict


ROOT = Path(__file__).resolve().parents[4]
RUNNER_PATH = ROOT / "src/services/policy_engine/bounded_analysis_runner.py"

REDIS_MESSAGE_ID = "1700000223450-0"
Q_NOTIFICATION_MESSAGE_ID = "1700000700002-0"
POLICY_APPLY_EVENT_ID = UUID("00000000-0000-4000-8000-00003d5b3290")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-00007a111d13")
JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-0000c7d7ef5e")
BUNDLE_ID = UUID("00000000-0000-4000-8000-0000c51bd89e")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000042c0d691")
ANALYSIS_ID = UUID("00000000-0000-4000-8000-0000a6a1a6a1")
NOTIFICATION_EVENT_ID = UUID("00000000-0000-4000-8000-0000b7b2b7b2")
DB_LOCATOR = "sentinel-private-database-locator"
REDIS_LOCATOR = "sentinel-private-redis-locator"
CHAT_ID = 987654321
RAW_PAYLOAD_SENTINEL = "private judge output payload must not print"
RAW_EXCEPTION_SENTINEL = "private sql failure detail must not print"
IDEMPOTENCY_SENTINEL = "analysis-policy-apply:private-dedupe-key"


class FakeRedisReader:
    def __init__(self, messages: list[RedisStreamMessage]) -> None:
        self.messages = messages
        self.calls: list[dict[str, Any]] = []

    async def read_candidate_messages(self, *, queue_name, config):
        self.calls.append({"queue_name": queue_name, "scan_limit": config.scan_limit})
        return list(self.messages)


class FakeRedisReaderBuilder:
    def __init__(self, reader: FakeRedisReader) -> None:
        self.reader = reader
        self.calls = 0
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_reader_created = True

        async def close() -> None:
            self.closed = True

        return BoundedPolicyEngineRedisReaderHandle(reader=self.reader, close=close)


class FakeRedisPublisher:
    def __init__(self, *, message_id: str = Q_NOTIFICATION_MESSAGE_ID) -> None:
        self.message_id = message_id
        self.calls: list[tuple[QueueRoute, object]] = []

    async def publish(self, route, message) -> str:
        self.calls.append((route, message))
        return self.message_id


class FakeRedisPublisherBuilder:
    def __init__(self, publisher: FakeRedisPublisher) -> None:
        self.publisher = publisher
        self.calls = 0
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_publisher_created = True

        async def close() -> None:
            self.closed = True

        return BoundedPolicyEngineRedisPublisherHandle(publisher=self.publisher, close=close)


class FakeRepository:
    def __init__(
        self,
        *,
        event: OutboxEventRow | None = None,
        candidate: CandidatePolicyContext | None = None,
        judge_run: JudgeRunPolicyContext | None = None,
        judge_output: JudgeOutputPolicyContext | None = None,
        bundle: BundlePolicyContext | None = None,
        existing_analysis: ExistingAnalysisRecord | None = None,
        notification_rows: list[OutboxEventRow] | None = None,
        commit_failures: set[int] | None = None,
    ) -> None:
        self.event = event if event is not None else _policy_event()
        self.candidate = candidate if candidate is not None else _candidate()
        self.judge_run = judge_run if judge_run is not None else _judge_run()
        self.judge_output = judge_output if judge_output is not None else _judge_output(_inspect_scores())
        self.bundle = bundle if bundle is not None else _bundle()
        self.existing_analysis = existing_analysis
        self.notification_rows = list(notification_rows or [])
        self.commit_failures = commit_failures or set()
        self.inserted_analyses: list[AnalysisDraft] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.mark_published_calls: list[dict[str, Any]] = []
        self.job_attempt_calls: list[dict[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0
        self.load_event_calls: list[UUID] = []

    async def load_event_outbox(self, trigger_event_id):
        self.load_event_calls.append(trigger_event_id)
        return self.event if self.event is not None and self.event.event_id == trigger_event_id else None

    async def load_candidate_context(self, candidate_group_id):
        if self.candidate is None or self.candidate.candidate_group_id != candidate_group_id:
            return None
        return self.candidate

    async def load_judge_run(self, judge_run_id):
        if self.judge_run is None or self.judge_run.judge_run_id != judge_run_id:
            return None
        return self.judge_run

    async def load_judge_output(self, judge_output_id):
        if self.judge_output is None or self.judge_output.judge_output_id != judge_output_id:
            return None
        return self.judge_output

    async def load_bundle_context(self, bundle_id):
        if self.bundle is None or self.bundle.bundle_id != bundle_id:
            return None
        return self.bundle

    async def load_existing_analysis(self, *, judge_output_id, policy_version, delivery_policy_version):
        if self.existing_analysis is None:
            return None
        if (
            self.existing_analysis.judge_output_id == judge_output_id
            and self.existing_analysis.policy_version == policy_version
            and self.existing_analysis.delivery_policy_version == delivery_policy_version
        ):
            return self.existing_analysis
        return None

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        self.inserted_analyses.append(draft)
        self.existing_analysis = ExistingAnalysisRecord(
            analysis_id=ANALYSIS_ID,
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        return ANALYSIS_ID

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

    async def load_notification_plan_intent_outboxes(self, intent: NotificationPlanIntent):
        dedupe_key = _notification_dedupe_key(intent)
        return [
            row
            for row in self.notification_rows
            if row.event_type == "notification.plan.created.v1"
            and row.aggregate_type == "analysis"
            and row.aggregate_id == intent.analysis_id
            and row.dedupe_key == dedupe_key
        ][:2]

    async def insert_or_load_notification_plan_intent_outbox(self, intent: NotificationPlanIntent):
        dedupe_key = _notification_dedupe_key(intent)
        for row in self.notification_rows:
            if row.dedupe_key == dedupe_key:
                return row, False
        row = _notification_outbox(intent=intent, status="pending")
        self.notification_rows.append(row)
        return row, True

    async def mark_notification_plan_intent_outbox_published(self, **kwargs) -> None:
        self.mark_published_calls.append(kwargs)
        self.notification_rows = [
            replace(row, status="published") if row.event_id == kwargs["event_id"] else row
            for row in self.notification_rows
        ]

    async def insert_publish_job_attempt(self, **kwargs) -> None:
        self.job_attempt_calls.append(kwargs)

    async def commit(self) -> None:
        self.commits += 1
        if self.commits in self.commit_failures:
            raise RuntimeError(RAW_EXCEPTION_SENTINEL)

    async def rollback(self) -> None:
        self.rollbacks += 1


class RaisingLookupRepository(FakeRepository):
    async def load_notification_plan_intent_outboxes(self, intent: NotificationPlanIntent):
        del intent
        raise RuntimeError(RAW_EXCEPTION_SENTINEL)


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.calls = 0
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close() -> None:
            self.closed = True

        return BoundedPolicyEngineRepositoryHandle(repository=self.repository, close=close)


class WrongNotificationStageResolver:
    def resolve(self, row):
        del row
        return QueueRoute("q.notification.send", "notification_send")


class FakeMappings:
    def all(self) -> list[dict[str, Any]]:
        return []


class FakeSqlResult:
    def mappings(self) -> FakeMappings:
        return FakeMappings()


class FakeSqlSession:
    def __init__(self) -> None:
        self.calls: list[tuple[object, dict[str, Any]]] = []

    async def execute(self, statement, params):
        self.calls.append((statement, params))
        return FakeSqlResult()


def _runtime_config(*, enable_later_delivery: bool = True) -> BoundedPolicyEngineAnalysisRuntimeConfig:
    return BoundedPolicyEngineAnalysisRuntimeConfig(
        database_url=DB_LOCATOR,
        redis_url=REDIS_LOCATOR,
        input_queue_name="q.analysis.policy",
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=CHAT_ID,
        enable_later_delivery=enable_later_delivery,
        enable_notification_send=True,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
    )


def _raising_runtime_config() -> BoundedPolicyEngineAnalysisRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _approved_config(**overrides) -> BoundedPolicyEngineAnalysisConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_read": True,
        "allow_redis_publish": True,
        "allow_database_read": True,
        "allow_database_write": True,
        "allow_policy_engine": True,
        "redis_message_suffix": "223450-0",
        "trigger_event_suffix": "3d5b3290",
        "judge_run_suffix": "7a111d13",
        "judge_output_suffix": "c7d7ef5e",
        "bundle_suffix": "c51bd89e",
        "candidate_group_suffix": "42c0d691",
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedPolicyEngineAnalysisConfig(**values)


def _redis_message(**field_overrides: str) -> RedisStreamMessage:
    fields = {
        "job_id": str(POLICY_APPLY_EVENT_ID),
        "stage_name": "analysis_policy",
        "root_object_type": "judge_run",
        "root_object_id": str(JUDGE_RUN_ID),
        "idempotency_key": IDEMPOTENCY_SENTINEL,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(POLICY_APPLY_EVENT_ID),
    }
    fields.update(field_overrides)
    return RedisStreamMessage(message_id=REDIS_MESSAGE_ID, fields=fields)


def _policy_event(
    *,
    event_id: UUID = POLICY_APPLY_EVENT_ID,
    status: str = "published",
    payload_json: dict[str, Any] | None = None,
) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id,
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=JUDGE_RUN_ID,
        dedupe_key=f"analysis-policy-apply:{JUDGE_RUN_ID}:{JUDGE_OUTPUT_ID}",
        payload_json=payload_json
        if payload_json is not None
        else {
            "judge_run_id": str(JUDGE_RUN_ID),
            "judge_output_id": str(JUDGE_OUTPUT_ID),
            "candidate_group_id": str(CANDIDATE_GROUP_ID),
            "bundle_id": str(BUNDLE_ID),
        },
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _candidate(*, current_bundle_id: UUID | None = BUNDLE_ID) -> CandidatePolicyContext:
    return CandidatePolicyContext(
        candidate_group_id=CANDIDATE_GROUP_ID,
        current_bundle_id=current_bundle_id,
        current_analysis_id=None,
    )


def _judge_run(*, status: str = "succeeded") -> JudgeRunPolicyContext:
    return JudgeRunPolicyContext(
        judge_run_id=JUDGE_RUN_ID,
        bundle_id=BUNDLE_ID,
        prompt_version="judge_github_primary_v1",
        policy_version="verdict_policy_v1",
        status=status,
    )


def _judge_output(scores: dict[str, int], *, model_proposed_verdict: str | None = "inspect_now"):
    return JudgeOutputPolicyContext(
        judge_output_id=JUDGE_OUTPUT_ID,
        judge_run_id=JUDGE_RUN_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        payload_json={
            "judge_schema_version": "judge_output_v1",
            "scores": scores,
            "reason_codes": ["judge_output_validated"],
            "evidence_limitations_ko": ["limited public telemetry"],
            "recommended_action_ko": "inspect repository",
            "freshness_note_ko": "fresh enough for operator review",
            "raw_payload": RAW_PAYLOAD_SENTINEL,
        },
        model_proposed_verdict=model_proposed_verdict,
        model_confidence_band="high",
        created_at=datetime.now(timezone.utc),
        judge_schema_version="judge_output_v1",
    )


def _bundle(*, artifact_type: str = "github_repo") -> BundlePolicyContext:
    return BundlePolicyContext(
        bundle_id=BUNDLE_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        current_primary_artifact_id=uuid4(),
        current_primary_artifact_type=artifact_type,
        created_at=datetime.now(timezone.utc),
    )


def _existing_analysis() -> ExistingAnalysisRecord:
    return ExistingAnalysisRecord(
        analysis_id=ANALYSIS_ID,
        judge_output_id=JUDGE_OUTPUT_ID,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )


def _notification_outbox(*, intent: NotificationPlanIntent, status: str) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=NOTIFICATION_EVENT_ID,
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=intent.analysis_id,
        dedupe_key=_notification_dedupe_key(intent),
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
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _notification_dedupe_key(intent: NotificationPlanIntent) -> str:
    return f"notification-plan-created:{intent.analysis_id}:{intent.target_chat_id}:{intent.material_change_hash}"


def _inspect_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 70,
        "practical_usefulness": 75,
        "evidence_strength": 60,
        "hype_penalty": 20,
        "confidence": 70,
        "code_quality": 70,
        "maintenance_signal": 60,
    }
    scores.update(overrides)
    return scores


def _later_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 50,
        "practical_usefulness": 50,
        "evidence_strength": 35,
        "hype_penalty": 85,
        "confidence": 40,
        "code_quality": 0,
    }
    scores.update(overrides)
    return scores


def _skip_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 20,
        "practical_usefulness": 30,
        "evidence_strength": 20,
        "hype_penalty": 20,
        "confidence": 20,
        "code_quality": 0,
    }
    scores.update(overrides)
    return scores


def _run(
    repository: FakeRepository,
    *,
    messages: list[RedisStreamMessage] | None = None,
    publisher: FakeRedisPublisher | None = None,
    runtime_config: BoundedPolicyEngineAnalysisRuntimeConfig | None = None,
    route_resolver=None,
):
    reader = FakeRedisReader(messages if messages is not None else [_redis_message()])
    publisher = publisher or FakeRedisPublisher()
    result = run_bounded_policy_engine_analysis_sync(
        _approved_config(),
        runtime_config_loader=lambda: runtime_config or _runtime_config(),
        redis_reader_builder=FakeRedisReaderBuilder(reader),
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
        route_resolver=route_resolver,
    )
    return result, reader, publisher


def test_authority_gates_fail_in_order_before_runtime_config() -> None:
    cases = (
        (BoundedPolicyEngineAnalysisConfig(), "operator_approval_missing"),
        (_approved_config(allow_runtime_config=False), "runtime_config_not_allowed"),
        (_approved_config(allow_redis_read=False), "redis_read_not_allowed"),
        (_approved_config(allow_database_read=False), "database_read_not_allowed"),
        (_approved_config(allow_database_write=False), "database_write_not_allowed"),
        (_approved_config(allow_redis_publish=False), "redis_publish_not_allowed"),
        (_approved_config(allow_policy_engine=False), "policy_engine_not_allowed"),
    )
    for config, error_code in cases:
        result = run_bounded_policy_engine_analysis_sync(
            config,
            runtime_config_loader=_raising_runtime_config,
        )
        report = result.to_sanitized_dict()

        assert report["status"] == "blocked"
        assert report["error_code"] == error_code
        assert report["redis_read_attempted"] is False
        assert report["database_read_attempted"] is False
        assert report["database_write_attempted"] is False
        assert report["policy_engine_called"] is False


def test_redis_input_must_remain_thin_id_only() -> None:
    repository = FakeRepository()

    result, _, publisher = _run(
        repository,
        messages=[_redis_message(payload_json="{}", bundle_id=str(BUNDLE_ID))],
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_message_forbidden_business_fields"
    assert report["database_read_attempted"] is False
    assert publisher.calls == []


def test_exact_target_success_inserts_analysis_outbox_publishes_and_marks() -> None:
    repository = FakeRepository()

    result, reader, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "published"
    assert report["target_policy_apply_event_suffix"] == "3d5b3290"
    assert report["target_judge_run_id_suffix"] == "7a111d13"
    assert report["target_judge_output_id_suffix"] == "c7d7ef5e"
    assert report["target_bundle_id_suffix"] == "c51bd89e"
    assert report["target_candidate_group_suffix"] == "42c0d691"
    assert report["analysis_written"] is True
    assert report["analysis_reused"] is False
    assert report["analysis_id_suffix"] == "a6a1a6a1"
    assert report["verdict"] == "inspect_now"
    assert report["delivery_decision"] == "send_now"
    assert report["urgency_profile"] == "high"
    assert report["policy_reconciled_flag"] is True
    assert report["state_transition_written"] is True
    assert report["notification_plan_intent_outbox_written"] is True
    assert report["notification_plan_intent_published"] is True
    assert report["q_notification_send_message_id_suffix"] == "700002-0"
    assert report["event_outbox_found"] is True
    assert report["judge_run_found"] is True
    assert report["judge_output_found"] is True
    assert report["bundle_found"] is True
    assert report["candidate_group_found"] is True
    assert report["analysis_found"] is True
    assert reader.calls == [{"queue_name": "q.analysis.policy", "scan_limit": 25}]
    assert len(repository.inserted_analyses) == 1
    assert repository.state_transitions == [
        {
            "object_type": "analysis",
            "object_id": ANALYSIS_ID,
            "from_state": "analysis_validated",
            "to_state": "analysis_policy_applied",
            "reason_code": "policy_engine_applied",
        }
    ]
    assert len(repository.notification_rows) == 1
    assert repository.notification_rows[0].status == "published"
    assert repository.mark_published_calls[0]["event_id"] == NOTIFICATION_EVENT_ID
    assert repository.job_attempt_calls == [
        {
            "stage_name": "notify",
            "queue_name": "q.notification.send",
            "root_object_type": "analysis",
            "root_object_id": ANALYSIS_ID,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]
    route, message = publisher.calls[0]
    assert route == QueueRoute("q.notification.send", "notify")
    assert message.as_stream_fields() == {
        "job_id": str(NOTIFICATION_EVENT_ID),
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": str(ANALYSIS_ID),
        "idempotency_key": repository.notification_rows[0].dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(NOTIFICATION_EVENT_ID),
    }
    assert "payload_json" not in message.as_stream_fields()


def test_skip_verdict_inserts_analysis_without_notification_plan_intent() -> None:
    repository = FakeRepository(judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"))

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "applied"
    assert report["verdict"] == "skip"
    assert report["delivery_decision"] == "suppress"
    assert report["urgency_profile"] == "suppressed"
    assert report["notification_plan_intent_outbox_written"] is False
    assert report["notification_plan_intent_published"] is False
    assert publisher.calls == []
    assert len(repository.inserted_analyses) == 1
    assert "policy_verdict_skip" in repository.inserted_analyses[0].reason_codes_json


def test_existing_analysis_reuses_pending_notification_outbox_without_duplicate_analysis() -> None:
    seed_repository = FakeRepository(existing_analysis=_existing_analysis())
    _, _, _ = _run(seed_repository)
    pending_row = replace(seed_repository.notification_rows[0], status="pending")
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        notification_rows=[pending_row],
    )

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "published"
    assert report["analysis_written"] is False
    assert report["analysis_reused"] is True
    assert report["notification_plan_intent_outbox_written"] is False
    assert report["notification_plan_intent_published"] is True
    assert repository.inserted_analyses == []
    assert len(repository.notification_rows) == 1
    assert repository.notification_rows[0].status == "published"
    assert len(publisher.calls) == 1


def test_existing_published_notification_outbox_returns_noop_without_duplicate_publish() -> None:
    seed_repository = FakeRepository(existing_analysis=_existing_analysis())
    _, _, _ = _run(seed_repository)
    published_row = replace(seed_repository.notification_rows[0], status="published")
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        notification_rows=[published_row],
    )

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "noop"
    assert report["analysis_written"] is False
    assert report["analysis_reused"] is True
    assert report["notification_plan_intent_outbox_written"] is False
    assert report["notification_plan_intent_published"] is True
    assert publisher.calls == []
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []


def test_duplicate_notification_plan_intent_outbox_rows_fail_closed() -> None:
    seed_repository = FakeRepository(existing_analysis=_existing_analysis())
    _, _, _ = _run(seed_repository)
    duplicate_a = replace(seed_repository.notification_rows[0], event_id=uuid4())
    duplicate_b = replace(seed_repository.notification_rows[0], event_id=uuid4())
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        notification_rows=[duplicate_a, duplicate_b],
    )

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is False
    assert report["status"] == "blocked"
    assert report["error_code"] == "duplicate_notification_plan_intent_outbox"
    assert publisher.calls == []
    assert repository.inserted_analyses == []


def test_dedupe_key_lookup_with_bad_payload_fails_closed_by_python_validation() -> None:
    seed_repository = FakeRepository(existing_analysis=_existing_analysis())
    _, _, _ = _run(seed_repository)
    bad_row = replace(
        seed_repository.notification_rows[0],
        payload_json={
            **seed_repository.notification_rows[0].payload_json,
            "candidate_group_id": str(uuid4()),
        },
    )
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        notification_rows=[bad_row],
    )

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is False
    assert report["status"] == "blocked"
    assert report["error_code"] == "notification_plan_intent_candidate_mismatch"
    assert publisher.calls == []
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []


def test_notification_route_stage_drift_is_rejected_without_rewrite() -> None:
    repository = FakeRepository()

    result, _, publisher = _run(repository, route_resolver=WrongNotificationStageResolver())
    report = result.to_sanitized_dict()

    assert report["ok"] is False
    assert report["status"] == "blocked"
    assert report["error_code"] == "route_not_allowed"
    assert publisher.calls == []
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []


def test_notification_plan_intent_lookup_sql_uses_dedupe_key_without_json_operators() -> None:
    intent = NotificationPlanIntent(
        notification_plan_id=uuid4(),
        analysis_id=ANALYSIS_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=CHAT_ID,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="candidate:42c0d691",
        material_change_hash="material-hash",
        send_after=None,
        suppress_reason_code=None,
    )
    session = FakeSqlSession()
    repository = SqlAlchemyBoundedPolicyEngineAnalysisRepository(session)

    import asyncio

    rows = asyncio.run(repository.load_notification_plan_intent_outboxes(intent))

    assert rows == []
    assert len(session.calls) == 1
    statement, params = session.calls[0]
    sql = str(statement)
    assert "dedupe_key = :dedupe_key" in sql
    assert "payload_json ->" not in sql
    assert "payload_json->" not in sql
    assert "->>" not in sql
    assert params == {
        "analysis_id": str(ANALYSIS_ID),
        "dedupe_key": _notification_dedupe_key(intent),
    }


def test_database_commit_before_publish_failure_does_not_publish_redis() -> None:
    repository = FakeRepository(commit_failures={1})

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is False
    assert report["status"] == "failed"
    assert report["error_code"] == "database_commit_failed_before_redis_publish"
    assert report["redis_publish_attempted"] is False
    assert publisher.calls == []


def test_database_commit_after_redis_publish_failure_is_explicit() -> None:
    repository = FakeRepository(commit_failures={2})

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is False
    assert report["status"] == "failed"
    assert report["error_code"] == "database_commit_failed_after_redis_publish"
    assert report["redis_publish_attempted"] is True
    assert report["q_notification_send_message_id_suffix"] == "700002-0"
    assert len(publisher.calls) == 1


def test_stale_bundle_noops_without_analysis_or_notification() -> None:
    repository = FakeRepository(candidate=_candidate(current_bundle_id=uuid4()))

    result, _, publisher = _run(repository)
    report = result.to_sanitized_dict()

    assert report["status"] == "noop"
    assert report["error_code"] == "stale_bundle"
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []
    assert publisher.calls == []


def test_policy_thresholds_and_model_override_reason_are_deterministic() -> None:
    assert VerdictPolicy().evaluate(scores=_inspect_scores(), current_primary_artifact_type="github_repo").verdict == (
        "inspect_now"
    )
    assert VerdictPolicy().evaluate(scores=_later_scores(), current_primary_artifact_type="github_repo").verdict == "later"
    assert VerdictPolicy().evaluate(scores=_skip_scores(), current_primary_artifact_type="github_repo").verdict == "skip"
    assert VerdictPolicy().evaluate(
        scores=_inspect_scores(code_quality=64),
        current_primary_artifact_type="github_repo",
    ).verdict == "later"
    assert VerdictPolicy().evaluate(
        scores={
            **_inspect_scores(),
            "specificity": 59,
        },
        current_primary_artifact_type="x_post",
    ).verdict == "later"

    reconciled, reason_codes = reconcile_model_verdict(
        model_proposed_verdict="skip",
        final_verdict="inspect_now",
        reason_codes=["policy_threshold_inspect_now"],
    )

    assert reconciled is False
    assert reason_codes == ["policy_threshold_inspect_now", "policy_overrode_model_verdict"]


def test_null_model_verdict_is_reconciled_by_policy_contract() -> None:
    reconciled, reason_codes = reconcile_model_verdict(
        model_proposed_verdict=None,
        final_verdict="later",
        reason_codes=["policy_threshold_later"],
    )

    assert reconciled is True
    assert reason_codes == ["policy_threshold_later"]


def test_delivery_policy_mappings() -> None:
    inspect_now = DeliveryPolicy().evaluate(verdict="inspect_now")
    later_enabled = DeliveryPolicy().evaluate(verdict="later")
    later_disabled = DeliveryPolicy(enable_later_delivery=False).evaluate(verdict="later")
    skip = DeliveryPolicy().evaluate(verdict="skip")

    assert (inspect_now.delivery_decision, inspect_now.urgency_profile, inspect_now.suppress_reason_code) == (
        "send_now",
        "high",
        None,
    )
    assert (later_enabled.delivery_decision, later_enabled.urgency_profile, later_enabled.suppress_reason_code) == (
        "send_now",
        "normal_silent",
        None,
    )
    assert (later_disabled.delivery_decision, later_disabled.urgency_profile, later_disabled.suppress_reason_code) == (
        "suppress",
        "suppressed",
        "later_delivery_disabled",
    )
    assert (skip.delivery_decision, skip.urgency_profile, skip.suppress_reason_code) == (
        "suppress",
        "suppressed",
        "policy_verdict_skip",
    )


def test_later_delivery_disabled_suppresses_without_notification_plan() -> None:
    repository = FakeRepository(judge_output=_judge_output(_later_scores(), model_proposed_verdict="later"))

    result, _, publisher = _run(repository, runtime_config=_runtime_config(enable_later_delivery=False))
    report = result.to_sanitized_dict()

    assert report["status"] == "applied"
    assert report["verdict"] == "later"
    assert report["delivery_decision"] == "suppress"
    assert report["urgency_profile"] == "suppressed"
    assert repository.notification_rows == []
    assert publisher.calls == []
    assert "later_delivery_disabled" in repository.inserted_analyses[0].reason_codes_json


def test_redaction_excludes_full_ids_chat_ids_locators_payload_idempotency_and_exception_detail() -> None:
    repository = RaisingLookupRepository()

    result, _, _ = _run(repository)
    report_text = json.dumps(result.to_sanitized_dict(), ensure_ascii=False)

    assert str(POLICY_APPLY_EVENT_ID) not in report_text
    assert str(JUDGE_RUN_ID) not in report_text
    assert str(JUDGE_OUTPUT_ID) not in report_text
    assert str(BUNDLE_ID) not in report_text
    assert str(CANDIDATE_GROUP_ID) not in report_text
    assert str(CHAT_ID) not in report_text
    assert DB_LOCATOR not in report_text
    assert REDIS_LOCATOR not in report_text
    assert RAW_PAYLOAD_SENTINEL not in report_text
    assert RAW_EXCEPTION_SENTINEL not in report_text
    assert IDEMPOTENCY_SENTINEL not in report_text
    assert "sql failure" not in report_text


def test_ast_guards_forbidden_authorities_and_runtime_calls() -> None:
    source = RUNNER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    call_attrs: set[str] = set()
    call_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                call_attrs.add(node.func.attr.lower())
            elif isinstance(node.func, ast.Name):
                call_names.add(node.func.id.lower())

    forbidden_import_fragments = (
        "openai",
        "notifier_telegram",
        "telegram_client",
        "gh_enricher",
        "x_enricher",
        "web_enricher",
        "subprocess",
    )
    assert all(not any(fragment in imported for fragment in forbidden_import_fragments) for imported in imports)
    assert not {"xack", "xreadgroup", "xgroup", "xclaim", "xautoclaim"} & call_attrs
    assert not {"systemctl", "docker", "alembic"} & call_names
    assert "send_message" not in call_attrs
