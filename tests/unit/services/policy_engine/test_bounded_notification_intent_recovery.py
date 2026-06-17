from __future__ import annotations

import ast
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from src.services.outbox_relay.models import OutboxEventRow
from src.services.policy_engine.bounded_notification_intent_recovery import (
    AnalysisRecoveryRecord,
    BoundedNotificationIntentRecoveryConfig,
    BoundedNotificationIntentRecoveryRedisPublisherHandle,
    BoundedNotificationIntentRecoveryRepositoryHandle,
    BoundedNotificationIntentRecoveryRuntimeConfig,
    SqlAlchemyBoundedNotificationIntentRecoveryRepository,
    render_sanitized_json,
    run_bounded_notification_intent_recovery_sync,
)
from src.services.policy_engine.models import (
    BundlePolicyContext,
    CandidatePolicyContext,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
)


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/policy_engine/bounded_notification_intent_recovery.py"

POLICY_APPLY_EVENT_ID = UUID("00000000-0000-4000-8000-000056e18229")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-00005f2340d7")
JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-00009a117f69")
BUNDLE_ID = UUID("00000000-0000-4000-8000-00001e39fe2f")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000078dbbb98")
ANALYSIS_ID = UUID("00000000-0000-4000-8000-0000aa7a5150")
NOTIFICATION_EVENT_ID = UUID("00000000-0000-4000-8000-0000baddcafe")
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel_redis_url"
CHAT_ID = 987654321
RAW_PAYLOAD_SENTINEL = "private judge output payload must not print"
REDIS_MESSAGE_ID = "1700000123456-0"


class FakeRepository:
    def __init__(self) -> None:
        self.events: list[OutboxEventRow] = []
        self.candidates: dict[UUID, CandidatePolicyContext] = {}
        self.judge_runs: dict[UUID, JudgeRunPolicyContext] = {}
        self.judge_outputs: dict[UUID, JudgeOutputPolicyContext] = {}
        self.bundles: dict[UUID, BundlePolicyContext] = {}
        self.analyses_by_judge_output: dict[UUID, AnalysisRecoveryRecord] = {}
        self.analyses_by_id: dict[UUID, AnalysisRecoveryRecord] = {}
        self.notification_outboxes: dict[str, OutboxEventRow] = {}
        self.load_events_calls: list[int] = []
        self.insert_calls: list[object] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.notification_plan_inserts = 0
        self.notifier_called = False
        self.telegram_send_called = False

    async def load_policy_apply_events(self, *, db_scan_limit: int) -> list[OutboxEventRow]:
        self.load_events_calls.append(db_scan_limit)
        return list(self.events)

    async def load_candidate_context(self, candidate_group_id: UUID):
        return self.candidates.get(candidate_group_id)

    async def load_judge_run(self, judge_run_id: UUID):
        return self.judge_runs.get(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID):
        return self.judge_outputs.get(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID):
        return self.bundles.get(bundle_id)

    async def load_existing_analysis_recovery(self, *, judge_output_id, policy_version, delivery_policy_version):
        analysis = self.analyses_by_judge_output.get(judge_output_id)
        if analysis is None:
            return None
        if analysis.policy_version != policy_version:
            return None
        if analysis.delivery_policy_version != delivery_policy_version:
            return None
        return analysis

    async def load_analysis_recovery_by_id(self, analysis_id):
        return self.analyses_by_id.get(analysis_id)

    async def load_notification_plan_intent_outboxes(self, intent):
        row = self.notification_outboxes.get(_dedupe_key(intent))
        return [] if row is None else [row]

    async def insert_or_load_notification_plan_intent_outbox(self, intent):
        self.insert_calls.append(intent)
        dedupe_key = _dedupe_key(intent)
        existing = self.notification_outboxes.get(dedupe_key)
        if existing is not None:
            return existing, False
        row = _notification_outbox(intent=intent)
        self.notification_outboxes[dedupe_key] = row
        return row, True

    async def commit(self) -> None:
        self.commit_calls += 1

    async def rollback(self) -> None:
        self.rollback_calls += 1


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.calls = 0
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close() -> None:
            self.close_calls += 1

        return BoundedNotificationIntentRecoveryRepositoryHandle(repository=self.repository, close=close)


class FakeRedisPublisher:
    def __init__(self) -> None:
        self.publish_calls: list[tuple[object, object]] = []
        self.xack_called = False
        self.xreadgroup_called = False
        self.xgroup_called = False
        self.xclaim_called = False
        self.xautoclaim_called = False

    async def publish(self, route, message):
        self.publish_calls.append((route, message))
        return REDIS_MESSAGE_ID


class FakeRedisPublisherBuilder:
    def __init__(self, publisher: FakeRedisPublisher) -> None:
        self.publisher = publisher
        self.calls = 0
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_publisher_created = True

        async def close() -> None:
            self.close_calls += 1

        return BoundedNotificationIntentRecoveryRedisPublisherHandle(publisher=self.publisher, close=close)


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []

    def all(self) -> list[dict[str, Any]]:
        return list(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None


class FakeSqlResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._rows)


class FakeSqlSession:
    def __init__(self) -> None:
        self.calls: list[tuple[object, dict[str, Any]]] = []

    async def execute(self, statement, params):
        self.calls.append((statement, params))
        return FakeSqlResult()


def _runtime_config(*, enable_notification_send: bool = True, redis_url: str = REDIS_URL):
    return BoundedNotificationIntentRecoveryRuntimeConfig(
        database_url=DB_URL,
        redis_url=redis_url,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=CHAT_ID,
        enable_notification_send=enable_notification_send,
    )


def _approved_config(**overrides) -> BoundedNotificationIntentRecoveryConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "allow_policy_preview": True,
        "policy_apply_event_suffix": "56e18229",
        "judge_run_suffix": "5f2340d7",
        "judge_output_suffix": "9a117f69",
        "bundle_suffix": "1e39fe2f",
        "candidate_group_suffix": "78dbbb98",
    }
    values.update(overrides)
    return BoundedNotificationIntentRecoveryConfig(**values)


def _seed_repository(*, delivery_decision: str = "send_now", verdict: str = "inspect_now") -> FakeRepository:
    repository = FakeRepository()
    repository.events.append(_policy_event())
    repository.candidates[CANDIDATE_GROUP_ID] = CandidatePolicyContext(
        candidate_group_id=CANDIDATE_GROUP_ID,
        current_bundle_id=BUNDLE_ID,
        current_analysis_id=ANALYSIS_ID,
    )
    repository.judge_runs[JUDGE_RUN_ID] = JudgeRunPolicyContext(
        judge_run_id=JUDGE_RUN_ID,
        bundle_id=BUNDLE_ID,
        prompt_version="judge_github_primary_v1",
        policy_version="verdict_policy_v1",
        status="succeeded",
    )
    repository.judge_outputs[JUDGE_OUTPUT_ID] = JudgeOutputPolicyContext(
        judge_output_id=JUDGE_OUTPUT_ID,
        judge_run_id=JUDGE_RUN_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        payload_json={"raw_payload": RAW_PAYLOAD_SENTINEL},
        model_proposed_verdict=verdict,
        model_confidence_band="high",
        created_at=datetime.now(timezone.utc),
        judge_schema_version="judge_output_v1",
    )
    repository.bundles[BUNDLE_ID] = BundlePolicyContext(
        bundle_id=BUNDLE_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        current_primary_artifact_id=uuid4(),
        current_primary_artifact_type="github_repo",
        created_at=datetime.now(timezone.utc),
    )
    analysis = _analysis(delivery_decision=delivery_decision, verdict=verdict)
    repository.analyses_by_judge_output[JUDGE_OUTPUT_ID] = analysis
    repository.analyses_by_id[ANALYSIS_ID] = analysis
    return repository


def _run(repository: FakeRepository, *, config=None, runtime_config=None, publisher_builder=None):
    return run_bounded_notification_intent_recovery_sync(
        config or _approved_config(),
        runtime_config_loader=lambda: runtime_config or _runtime_config(),
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )


def _policy_event(event_id: UUID = POLICY_APPLY_EVENT_ID) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id,
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=JUDGE_RUN_ID,
        dedupe_key="analysis-policy-apply-private-key",
        payload_json={
            "judge_run_id": str(JUDGE_RUN_ID),
            "judge_output_id": str(JUDGE_OUTPUT_ID),
            "candidate_group_id": str(CANDIDATE_GROUP_ID),
            "bundle_id": str(BUNDLE_ID),
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _analysis(*, delivery_decision: str = "send_now", verdict: str = "inspect_now") -> AnalysisRecoveryRecord:
    return AnalysisRecoveryRecord(
        analysis_id=ANALYSIS_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        judge_output_id=JUDGE_OUTPUT_ID,
        schema_version="analysis_v1",
        policy_version="verdict_policy_v1",
        prompt_version="judge_github_primary_v1",
        delivery_policy_version="delivery_policy_v1",
        verdict=verdict,
        delivery_decision=delivery_decision,
        scores_json={"confidence": 80},
        reason_codes_json=["judge_output_validated"],
        evidence_limitations_ko="limited public telemetry",
        recommended_action_ko="inspect repository",
        freshness_note_ko="fresh enough for operator review",
        model_proposed_verdict=verdict,
        policy_reconciled_flag=False,
    )


def _notification_outbox(*, intent, event_id: UUID = NOTIFICATION_EVENT_ID) -> OutboxEventRow:
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
    return OutboxEventRow(
        event_id=event_id,
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=intent.analysis_id,
        dedupe_key=_dedupe_key(intent),
        payload_json=payload,
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _dedupe_key(intent) -> str:
    return f"notification-plan-created:{intent.analysis_id}:{intent.target_chat_id}:{intent.material_change_hash}"


def test_runtime_flag_disabled_preview_reports_send_disabled_without_write_or_publish() -> None:
    repository = _seed_repository()

    result = _run(repository, runtime_config=_runtime_config(enable_notification_send=False))
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["status"] == "blocked"
    assert report["notification_intent_recovery_reason_code"] == "notification_send_disabled"
    assert report["notification_intent_possible"] is False
    assert report["database_read_attempted"] is True
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert repository.insert_calls == []
    assert repository.commit_calls == 0


def test_runtime_flag_enabled_preview_reports_possible_without_write_or_publish() -> None:
    repository = _seed_repository()

    result = _run(repository)
    report = result.to_sanitized_dict()
    rendered = render_sanitized_json(report)

    assert result.ok is True
    assert report["status"] == "pass"
    assert report["notification_intent_possible"] is True
    assert report["notification_intent_outbox_written"] is False
    assert report["notification_intent_outbox_existing"] is False
    assert report["predicted_verdict"] == "inspect_now"
    assert report["predicted_delivery_decision"] == "send_now"
    assert report["predicted_urgency_profile"] == "high"
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert str(ANALYSIS_ID) not in rendered
    assert str(CHAT_ID) not in rendered
    assert RAW_PAYLOAD_SENTINEL not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered


def test_write_mode_inserts_one_outbox_row_and_duplicate_rerun_is_idempotent() -> None:
    repository = _seed_repository()
    config = _approved_config(
        allow_database_write=True,
        allow_notification_intent_write=True,
        require_notification_send_enabled=True,
    )

    first = _run(repository, config=config)
    second = _run(repository, config=config)
    first_report = first.to_sanitized_dict()
    second_report = second.to_sanitized_dict()

    assert first.ok is True
    assert first_report["notification_intent_outbox_written"] is True
    assert first_report["notification_intent_outbox_existing"] is False
    assert first_report["notification_intent_outbox_event_suffix"] == "baddcafe"
    assert first_report["database_write_attempted"] is True
    assert repository.commit_calls == 1
    assert len(repository.insert_calls) == 1
    assert repository.notification_plan_inserts == 0
    assert repository.notifier_called is False
    assert repository.telegram_send_called is False

    assert second.ok is True
    assert second_report["notification_intent_outbox_written"] is False
    assert second_report["notification_intent_outbox_existing"] is True
    assert second_report["notification_intent_recovery_reason_code"] == "notification_intent_already_exists"
    assert len(repository.insert_calls) == 1


def test_optional_redis_publish_requires_flags_and_publishes_thin_notify_message() -> None:
    repository = _seed_repository()
    publisher = FakeRedisPublisher()
    publisher_builder = FakeRedisPublisherBuilder(publisher)
    config = _approved_config(
        allow_database_write=True,
        allow_notification_intent_write=True,
        require_notification_send_enabled=True,
        allow_redis_read=True,
        allow_redis_publish=True,
        allow_notification_send_queue_publish=True,
    )

    result = _run(repository, config=config, publisher_builder=publisher_builder)
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["q_notification_send_published"] is True
    assert report["q_notification_send_message_id_suffix"] == "123456-0"
    assert report["redis_read_attempted"] is False
    assert report["redis_publish_attempted"] is True
    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.notification.send"
    assert fields["stage_name"] == "notify"
    assert fields["root_object_type"] == "analysis"
    assert fields["trigger_event_id"] == fields["job_id"]
    for forbidden in (
        "payload_json",
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "target_chat_id",
        "message_text",
        "material_change_hash",
    ):
        assert forbidden not in fields
    assert publisher.xack_called is False
    assert publisher.xreadgroup_called is False
    assert publisher.xgroup_called is False
    assert publisher.xclaim_called is False
    assert publisher.xautoclaim_called is False


def test_suppress_analysis_does_not_write_intent() -> None:
    repository = _seed_repository(delivery_decision="suppress", verdict="skip")
    config = _approved_config(
        allow_database_write=True,
        allow_notification_intent_write=True,
        require_notification_send_enabled=True,
    )

    result = _run(repository, config=config)
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["notification_intent_recovery_reason_code"] == "analysis_delivery_suppress"
    assert report["notification_intent_outbox_written"] is False
    assert report["database_write_attempted"] is False
    assert repository.insert_calls == []


def test_existing_notification_intent_reports_reused_without_duplicate_insert() -> None:
    repository = _seed_repository()
    write_config = _approved_config(
        allow_database_write=True,
        allow_notification_intent_write=True,
        require_notification_send_enabled=True,
    )
    first = _run(repository, config=write_config)
    assert first.ok is True

    preview = _run(repository)
    report = preview.to_sanitized_dict()

    assert preview.ok is True
    assert report["notification_intent_outbox_existing"] is True
    assert report["notification_intent_recovery_reason_code"] == "notification_intent_already_exists"
    assert len(repository.insert_calls) == 1


def test_suffix_missing_or_ambiguous_fails_closed_before_write_or_publish() -> None:
    missing_repository = _seed_repository()
    missing = _run(missing_repository, config=_approved_config(policy_apply_event_suffix=None))

    assert missing.ok is False
    assert missing.error_code == "suffix_ambiguous_or_missing"
    assert missing.state.database_read_attempted is False
    assert missing_repository.insert_calls == []

    ambiguous_repository = _seed_repository()
    ambiguous_repository.events.append(_policy_event(event_id=UUID("10000000-0000-4000-8000-000056e18229")))
    ambiguous = _run(ambiguous_repository)

    assert ambiguous.ok is False
    assert ambiguous.error_code == "suffix_ambiguous_or_missing"
    assert ambiguous.state.database_write_attempted is False


def test_sql_shape_and_ast_guards() -> None:
    session = FakeSqlSession()
    repository = SqlAlchemyBoundedNotificationIntentRecoveryRepository(session)

    asyncio.run(repository.load_policy_apply_events(db_scan_limit=25))
    statement = str(session.calls[0][0])
    assert "FROM event_outbox" in statement
    assert "event_type = :event_type" in statement
    assert "payload_json ->>" not in statement
    assert "payload_json->>" not in statement
    assert session.calls[0][1] == {"event_type": "analysis.policy.apply.v1", "limit": 25}

    source = SOURCE_PATH.read_text(encoding="utf-8")
    assert "payload_json ->>" not in source
    assert "payload_json->>" not in source
    assert source.count("INSERT INTO event_outbox") == 1
    assert "'notification.plan.created.v1'" in source
    assert "INSERT INTO analyses" not in source
    assert "INSERT INTO notification_plans" not in source
    assert "\n                UPDATE " not in source
    assert "\n                DELETE " not in source

    tree = ast.parse(source)
    imported_names: set[str] = set()
    imported_modules: set[str] = set()
    called_attrs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            called_attrs.add(node.func.attr.lower())

    imports = " ".join(sorted(imported_names | imported_modules)).lower()
    for fragment in (
        "openai",
        "notifier_telegram",
        "telegram",
        "github",
        "gh_enricher",
        "x_enricher",
        "web_enricher",
        "subprocess",
        "docker",
        "alembic",
    ):
        assert fragment not in imports
    assert not ({"xack", "xreadgroup", "xgroup", "xclaim", "xautoclaim"} & called_attrs)
