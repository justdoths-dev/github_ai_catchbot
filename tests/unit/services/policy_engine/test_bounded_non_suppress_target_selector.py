from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from src.services.outbox_relay.models import OutboxEventRow
from src.services.policy_engine.bounded_analysis_runner import RedisStreamMessage
from src.services.policy_engine.bounded_non_suppress_target_selector import (
    BoundedPolicyNonSuppressRedisReaderHandle,
    BoundedPolicyNonSuppressRepositoryHandle,
    BoundedPolicyNonSuppressTargetSelectorConfig,
    BoundedPolicyNonSuppressTargetSelectorRuntimeConfig,
    SqlAlchemyBoundedPolicyNonSuppressTargetSelectorRepository,
    run_bounded_policy_non_suppress_target_selector_sync,
)
from src.services.policy_engine.models import (
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
)


ROOT = Path(__file__).resolve().parents[4]
SELECTOR_PATH = ROOT / "src/services/policy_engine/bounded_non_suppress_target_selector.py"

INSPECT_REDIS_ID = "1700000900000-0"
LATER_REDIS_ID = "1700000950000-0"
SKIP_REDIS_ID = "1700000990000-0"

POLICY_APPLY_EVENT_ID = UUID("00000000-0000-4000-8000-000011111111")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-000022222222")
JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-000033333333")
BUNDLE_ID = UUID("00000000-0000-4000-8000-000044444444")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000055555555")
ANALYSIS_ID = UUID("00000000-0000-4000-8000-000066666666")
NOTIFICATION_EVENT_ID = UUID("00000000-0000-4000-8000-000077777777")

LATER_EVENT_ID = UUID("00000000-0000-4000-8000-100011111111")
LATER_JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-100022222222")
LATER_JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-100033333333")
LATER_BUNDLE_ID = UUID("00000000-0000-4000-8000-100044444444")
LATER_CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-100055555555")

DB_LOCATOR = "private-db-url-must-not-print"
REDIS_LOCATOR = "private-redis-url-must-not-print"
CHAT_ID = 987654321
IDEMPOTENCY_SENTINEL = "analysis-policy-private-idempotency"
RAW_PAYLOAD_SENTINEL = "private judge output payload must not print"


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

        return BoundedPolicyNonSuppressRedisReaderHandle(reader=self.reader, close=close)


class FakeRepository:
    def __init__(self) -> None:
        self.events: dict[UUID, OutboxEventRow] = {}
        self.candidates: dict[UUID, CandidatePolicyContext] = {}
        self.judge_runs: dict[UUID, JudgeRunPolicyContext] = {}
        self.judge_outputs: dict[UUID, JudgeOutputPolicyContext] = {}
        self.bundles: dict[UUID, BundlePolicyContext] = {}
        self.existing_analyses: dict[UUID, ExistingAnalysisRecord] = {}
        self.notification_status_by_analysis_id: dict[UUID, str | list[str]] = {}
        self.load_event_calls: list[UUID] = []
        self.notification_lookup_calls: list[object] = []

    async def load_event_outbox(self, trigger_event_id):
        self.load_event_calls.append(trigger_event_id)
        return self.events.get(trigger_event_id)

    async def load_candidate_context(self, candidate_group_id):
        return self.candidates.get(candidate_group_id)

    async def load_judge_run(self, judge_run_id):
        return self.judge_runs.get(judge_run_id)

    async def load_judge_output(self, judge_output_id):
        return self.judge_outputs.get(judge_output_id)

    async def load_bundle_context(self, bundle_id):
        return self.bundles.get(bundle_id)

    async def load_existing_analysis(self, *, judge_output_id, policy_version, delivery_policy_version):
        existing = self.existing_analyses.get(judge_output_id)
        if existing is None:
            return None
        if (
            existing.policy_version == policy_version
            and existing.delivery_policy_version == delivery_policy_version
        ):
            return existing
        return None

    async def load_notification_plan_intent_outboxes(self, intent):
        self.notification_lookup_calls.append(intent)
        status = self.notification_status_by_analysis_id.get(intent.analysis_id)
        if status is None:
            return []
        statuses = status if isinstance(status, list) else [status]
        return [
            _notification_outbox(intent=intent, status=item, event_id=_offset_uuid(NOTIFICATION_EVENT_ID, index))
            for index, item in enumerate(statuses)
        ]


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

        return BoundedPolicyNonSuppressRepositoryHandle(repository=self.repository, close=close)


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


def _runtime_config(
    *,
    enable_later_delivery: bool = True,
    enable_notification_send: bool = True,
) -> BoundedPolicyNonSuppressTargetSelectorRuntimeConfig:
    return BoundedPolicyNonSuppressTargetSelectorRuntimeConfig(
        database_url=DB_LOCATOR,
        redis_url=REDIS_LOCATOR,
        queue_name="q.analysis.policy",
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=CHAT_ID,
        enable_later_delivery=enable_later_delivery,
        enable_notification_send=enable_notification_send,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
    )


def _raising_runtime_config() -> BoundedPolicyNonSuppressTargetSelectorRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _approved_config(**overrides) -> BoundedPolicyNonSuppressTargetSelectorConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_read": True,
        "allow_database_read": True,
        "allow_policy_preview": True,
        "scan_limit": 100,
        "max_results": 5,
        "prefer_verdict": "any",
    }
    values.update(overrides)
    return BoundedPolicyNonSuppressTargetSelectorConfig(**values)


def _run(
    repository: FakeRepository,
    messages: list[RedisStreamMessage],
    *,
    config: BoundedPolicyNonSuppressTargetSelectorConfig | None = None,
    runtime_config: BoundedPolicyNonSuppressTargetSelectorRuntimeConfig | None = None,
):
    reader = FakeRedisReader(messages)
    result = run_bounded_policy_non_suppress_target_selector_sync(
        config or _approved_config(),
        runtime_config_loader=lambda: runtime_config or _runtime_config(),
        redis_reader_builder=FakeRedisReaderBuilder(reader),
        repository_builder=FakeRepositoryBuilder(repository),
    )
    return result, reader


def _seed_target(
    repository: FakeRepository,
    *,
    event_id: UUID = POLICY_APPLY_EVENT_ID,
    judge_run_id: UUID = JUDGE_RUN_ID,
    judge_output_id: UUID = JUDGE_OUTPUT_ID,
    bundle_id: UUID = BUNDLE_ID,
    candidate_group_id: UUID = CANDIDATE_GROUP_ID,
    scores: dict[str, int] | None = None,
    model_proposed_verdict: str | None = "inspect_now",
    redis_message_id: str = INSPECT_REDIS_ID,
    event_status: str = "published",
    judge_run_status: str = "succeeded",
    current_bundle_id: UUID | None = None,
    artifact_type: str = "github_repo",
    payload_extra: dict[str, Any] | None = None,
) -> RedisStreamMessage:
    repository.events[event_id] = _policy_event(
        event_id=event_id,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        status=event_status,
    )
    repository.candidates[candidate_group_id] = CandidatePolicyContext(
        candidate_group_id=candidate_group_id,
        current_bundle_id=bundle_id if current_bundle_id is None else current_bundle_id,
        current_analysis_id=None,
    )
    repository.judge_runs[judge_run_id] = JudgeRunPolicyContext(
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        prompt_version="judge_github_primary_v1",
        policy_version="verdict_policy_v1",
        status=judge_run_status,
    )
    repository.judge_outputs[judge_output_id] = _judge_output(
        judge_output_id=judge_output_id,
        judge_run_id=judge_run_id,
        candidate_group_id=candidate_group_id,
        scores=scores or _inspect_scores(),
        model_proposed_verdict=model_proposed_verdict,
        payload_extra=payload_extra,
    )
    repository.bundles[bundle_id] = BundlePolicyContext(
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        current_primary_artifact_id=uuid4(),
        current_primary_artifact_type=artifact_type,
        created_at=datetime.now(timezone.utc),
    )
    return _redis_message(
        message_id=redis_message_id,
        event_id=event_id,
        judge_run_id=judge_run_id,
    )


def _redis_message(
    *,
    message_id: str = INSPECT_REDIS_ID,
    event_id: UUID = POLICY_APPLY_EVENT_ID,
    judge_run_id: UUID = JUDGE_RUN_ID,
    **field_overrides: str,
) -> RedisStreamMessage:
    fields = {
        "job_id": str(event_id),
        "stage_name": "analysis_policy",
        "root_object_type": "judge_run",
        "root_object_id": str(judge_run_id),
        "idempotency_key": IDEMPOTENCY_SENTINEL,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }
    fields.update(field_overrides)
    return RedisStreamMessage(message_id=message_id, fields=fields)


def _policy_event(
    *,
    event_id: UUID,
    judge_run_id: UUID,
    judge_output_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
    status: str = "published",
) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id,
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=judge_run_id,
        dedupe_key=f"analysis-policy-apply:{judge_run_id}:{judge_output_id}",
        payload_json={
            "judge_run_id": str(judge_run_id),
            "judge_output_id": str(judge_output_id),
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
        },
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _judge_output(
    *,
    judge_output_id: UUID,
    judge_run_id: UUID,
    candidate_group_id: UUID,
    scores: dict[str, int],
    model_proposed_verdict: str | None,
    payload_extra: dict[str, Any] | None = None,
) -> JudgeOutputPolicyContext:
    payload = {
        "judge_schema_version": "judge_output_v1",
        "scores": scores,
        "reason_codes": ["judge_output_validated"],
        "evidence_limitations_ko": ["limited public telemetry"],
        "recommended_action_ko": "inspect repository",
        "freshness_note_ko": "fresh enough for operator review",
        "raw_payload": RAW_PAYLOAD_SENTINEL,
    }
    if payload_extra:
        payload.update(payload_extra)
    return JudgeOutputPolicyContext(
        judge_output_id=judge_output_id,
        judge_run_id=judge_run_id,
        candidate_group_id=candidate_group_id,
        payload_json=payload,
        model_proposed_verdict=model_proposed_verdict,
        model_confidence_band="high",
        created_at=datetime.now(timezone.utc),
        judge_schema_version="judge_output_v1",
    )


def _existing_analysis(judge_output_id: UUID = JUDGE_OUTPUT_ID) -> ExistingAnalysisRecord:
    return ExistingAnalysisRecord(
        analysis_id=ANALYSIS_ID,
        judge_output_id=judge_output_id,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )


def _notification_outbox(*, intent, status: str, event_id: UUID) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id,
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
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _offset_uuid(value: UUID, offset: int) -> UUID:
    return UUID(int=value.int + offset)


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


def test_authority_gates_fail_in_order_before_runtime_config() -> None:
    cases = (
        (BoundedPolicyNonSuppressTargetSelectorConfig(), "operator_approval_missing"),
        (_approved_config(allow_runtime_config=False), "runtime_config_not_allowed"),
        (_approved_config(allow_redis_read=False), "redis_read_not_allowed"),
        (_approved_config(allow_database_read=False), "database_read_not_allowed"),
        (_approved_config(allow_policy_preview=False), "policy_preview_not_allowed"),
    )
    for config, error_code in cases:
        result = run_bounded_policy_non_suppress_target_selector_sync(
            config,
            runtime_config_loader=_raising_runtime_config,
        )
        report = result.to_sanitized_dict()

        assert report["status"] == "blocked"
        assert report["error_code"] == error_code
        assert report["redis_read_attempted"] is False
        assert report["database_read_attempted"] is False
        assert report["database_write_attempted"] is False
        assert report["policy_preview_called"] is False
        assert report["policy_engine_called"] is False


def test_redis_input_stays_thin_id_only_and_business_fields_are_blocked_before_database_read() -> None:
    repository = FakeRepository()
    message = _redis_message(judge_output_id=str(JUDGE_OUTPUT_ID))

    result, _ = _run(repository, [message])
    report = result.to_sanitized_dict()

    assert report["status"] == "no_candidate_found"
    assert report["blocked_candidate_count"] == 1
    assert report["redis_read_attempted"] is True
    assert report["database_read_attempted"] is False
    assert report["policy_preview_called"] is False
    assert repository.load_event_calls == []


def test_successful_selection_returns_exact_suffixes_ready_argv_and_no_mutations() -> None:
    repository = FakeRepository()
    message = _seed_target(repository)

    result, reader = _run(repository, [message])
    report = result.to_sanitized_dict()
    selected = report["selected_target"]
    ready_argv = selected["ready_policy_runner_argv"]

    assert report["ok"] is True
    assert report["status"] == "selected"
    assert report["eligible_non_suppress_count"] == 1
    assert selected["redis_message_id_suffix"] == INSPECT_REDIS_ID[-8:]
    assert selected["policy_apply_event_suffix"] == "11111111"
    assert selected["judge_run_suffix"] == "22222222"
    assert selected["judge_output_suffix"] == "33333333"
    assert selected["bundle_suffix"] == "44444444"
    assert selected["candidate_group_suffix"] == "55555555"
    assert selected["predicted_verdict"] == "inspect_now"
    assert selected["predicted_delivery_decision"] == "send_now"
    assert selected["predicted_urgency_profile"] == "high"
    assert selected["analysis_exists"] is False
    assert selected["notification_outbox_exists"] is False
    assert ready_argv[0:2] == ["venv/bin/python", "tools/bounded_policy_engine_analysis_runner.py"]
    assert "--allow-database-write" in ready_argv
    assert "--allow-redis-publish" in ready_argv
    assert "--allow-policy-engine" in ready_argv
    assert ready_argv[ready_argv.index("--redis-message-suffix") + 1] == INSPECT_REDIS_ID[-8:]
    assert ready_argv[ready_argv.index("--trigger-event-suffix") + 1] == "11111111"
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["redis_ack_called"] is False
    assert report["redis_consume_called"] is False
    assert report["policy_engine_called"] is False
    assert reader.calls == [{"queue_name": "q.analysis.policy", "scan_limit": 100}]


def test_prefer_verdict_filters_and_any_ranks_inspect_now_first() -> None:
    repository = FakeRepository()
    inspect_message = _seed_target(repository, redis_message_id=INSPECT_REDIS_ID)
    later_message = _seed_target(
        repository,
        event_id=LATER_EVENT_ID,
        judge_run_id=LATER_JUDGE_RUN_ID,
        judge_output_id=LATER_JUDGE_OUTPUT_ID,
        bundle_id=LATER_BUNDLE_ID,
        candidate_group_id=LATER_CANDIDATE_GROUP_ID,
        scores=_later_scores(),
        model_proposed_verdict="later",
        redis_message_id=LATER_REDIS_ID,
    )

    any_report = _run(repository, [later_message, inspect_message])[0].to_sanitized_dict()
    inspect_report = _run(
        repository,
        [later_message, inspect_message],
        config=_approved_config(prefer_verdict="inspect_now"),
    )[0].to_sanitized_dict()
    later_report = _run(
        repository,
        [later_message, inspect_message],
        config=_approved_config(prefer_verdict="later"),
    )[0].to_sanitized_dict()

    assert any_report["selected_target"]["predicted_verdict"] == "inspect_now"
    assert [candidate["predicted_verdict"] for candidate in any_report["candidates"]] == ["inspect_now", "later"]
    assert inspect_report["eligible_non_suppress_count"] == 1
    assert inspect_report["selected_target"]["predicted_verdict"] == "inspect_now"
    assert later_report["eligible_non_suppress_count"] == 1
    assert later_report["selected_target"]["predicted_verdict"] == "later"


def test_suppressed_candidate_is_counted_but_not_selected() -> None:
    repository = FakeRepository()
    message = _seed_target(
        repository,
        scores=_skip_scores(),
        model_proposed_verdict="skip",
        redis_message_id=SKIP_REDIS_ID,
    )

    report = _run(repository, [message])[0].to_sanitized_dict()

    assert report["status"] == "no_candidate_found"
    assert "selected_target" not in report
    assert report["suppressed_count"] == 1
    assert report["eligible_non_suppress_count"] == 0
    assert report["policy_preview_called"] is True


def test_existing_analysis_with_published_notification_outbox_is_already_processed() -> None:
    repository = FakeRepository()
    message = _seed_target(repository)
    repository.existing_analyses[JUDGE_OUTPUT_ID] = _existing_analysis()
    repository.notification_status_by_analysis_id[ANALYSIS_ID] = "published"

    report = _run(repository, [message])[0].to_sanitized_dict()

    assert report["status"] == "no_candidate_found"
    assert report["already_processed_count"] == 1
    assert report["eligible_non_suppress_count"] == 0


def test_existing_analysis_with_pending_matching_notification_outbox_is_eligible() -> None:
    repository = FakeRepository()
    message = _seed_target(repository)
    repository.existing_analyses[JUDGE_OUTPUT_ID] = _existing_analysis()
    repository.notification_status_by_analysis_id[ANALYSIS_ID] = "pending"

    report = _run(repository, [message])[0].to_sanitized_dict()
    selected = report["selected_target"]

    assert report["status"] == "selected"
    assert selected["analysis_exists"] is True
    assert selected["notification_outbox_exists"] is True
    assert selected["notification_outbox_status"] == "pending"
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False


def test_stale_bundle_candidate_is_blocked() -> None:
    repository = FakeRepository()
    message = _seed_target(repository, current_bundle_id=uuid4())

    report = _run(repository, [message])[0].to_sanitized_dict()

    assert report["status"] == "no_candidate_found"
    assert report["blocked_candidate_count"] == 1
    assert report["eligible_non_suppress_count"] == 0


def test_refusal_candidate_is_blocked() -> None:
    repository = FakeRepository()
    message = _seed_target(repository, payload_extra={"refusal_detected": True})

    report = _run(repository, [message])[0].to_sanitized_dict()

    assert report["status"] == "no_candidate_found"
    assert report["blocked_candidate_count"] == 1
    assert report["eligible_non_suppress_count"] == 0


def test_duplicate_redis_matches_and_malformed_messages_are_counted_blocked_without_mutation() -> None:
    repository = FakeRepository()
    valid = _seed_target(repository)
    duplicate = _redis_message(message_id="1700000900001-0")
    malformed = RedisStreamMessage(message_id="1700000900002-0", fields={"stage_name": "analysis_policy"})

    report = _run(repository, [valid, duplicate, malformed])[0].to_sanitized_dict()

    assert report["status"] == "selected"
    assert report["eligible_non_suppress_count"] == 1
    assert report["blocked_candidate_count"] == 2
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False


def test_notification_outbox_lookup_sql_uses_dedupe_key_without_json_operators() -> None:
    class Intent:
        analysis_id = ANALYSIS_ID
        target_chat_id = CHAT_ID
        material_change_hash = "material-hash"

    session = FakeSqlSession()
    repository = SqlAlchemyBoundedPolicyNonSuppressTargetSelectorRepository(session)

    import asyncio

    rows = asyncio.run(repository.load_notification_plan_intent_outboxes(Intent()))

    assert rows == []
    assert len(session.calls) == 1
    statement, params = session.calls[0]
    sql = str(statement)
    assert "event_type = 'notification.plan.created.v1'" in sql
    assert "aggregate_type = 'analysis'" in sql
    assert "aggregate_id = CAST(:analysis_id AS uuid)" in sql
    assert "dedupe_key = :dedupe_key" in sql
    assert "payload_json ->" not in sql
    assert "payload_json->" not in sql
    assert "->>" not in sql
    assert params == {
        "analysis_id": str(ANALYSIS_ID),
        "dedupe_key": f"notification-plan-created:{ANALYSIS_ID}:{CHAT_ID}:material-hash",
    }


def test_redaction_excludes_full_ids_chat_ids_locators_payload_idempotency_sql_and_exception_detail() -> None:
    repository = FakeRepository()
    message = _seed_target(repository)

    report_text = json.dumps(_run(repository, [message])[0].to_sanitized_dict(), ensure_ascii=False)

    assert str(POLICY_APPLY_EVENT_ID) not in report_text
    assert str(JUDGE_RUN_ID) not in report_text
    assert str(JUDGE_OUTPUT_ID) not in report_text
    assert str(BUNDLE_ID) not in report_text
    assert str(CANDIDATE_GROUP_ID) not in report_text
    assert str(CHAT_ID) not in report_text
    assert DB_LOCATOR not in report_text
    assert REDIS_LOCATOR not in report_text
    assert IDEMPOTENCY_SENTINEL not in report_text
    assert RAW_PAYLOAD_SENTINEL not in report_text
    assert "SELECT " not in report_text
    assert "Traceback" not in report_text


def test_ast_guards_forbidden_authorities_and_runtime_calls() -> None:
    source = SELECTOR_PATH.read_text(encoding="utf-8")
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
        "telegram",
        "gh_enricher",
        "x_enricher",
        "web_enricher",
        "subprocess",
    )
    assert all(not any(fragment in imported for fragment in forbidden_import_fragments) for imported in imports)
    assert not {"xack", "xreadgroup", "xgroup", "xclaim", "xautoclaim"} & call_attrs
    assert "xrevrange" in call_attrs
    assert not {"systemctl", "docker", "alembic"} & call_names
    assert "send_message" not in call_attrs
