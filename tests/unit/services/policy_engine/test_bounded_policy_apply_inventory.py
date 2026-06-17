from __future__ import annotations

import ast
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from src.services.outbox_relay.models import OutboxEventRow
from src.services.policy_engine.bounded_analysis_runner import RedisStreamMessage
from src.services.policy_engine.bounded_policy_apply_inventory import (
    BoundedPolicyApplyInventoryConfig,
    BoundedPolicyApplyInventoryRedisReaderHandle,
    BoundedPolicyApplyInventoryRepositoryHandle,
    BoundedPolicyApplyInventoryRuntimeConfig,
    ExistingAnalysisInventoryRecord,
    SqlAlchemyBoundedPolicyApplyInventoryRepository,
    render_sanitized_json,
    run_bounded_policy_apply_inventory_sync,
)
from src.services.policy_engine.models import (
    BundlePolicyContext,
    CandidatePolicyContext,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
)


ROOT = Path(__file__).resolve().parents[4]
INVENTORY_PATH = ROOT / "src/services/policy_engine/bounded_policy_apply_inventory.py"

POLICY_APPLY_EVENT_ID = UUID("00000000-0000-4000-8000-00003d5b3290")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-00007a111d13")
JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-0000c7d7ef5e")
BUNDLE_ID = UUID("00000000-0000-4000-8000-0000c51bd89e")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000042c0d691")
ANALYSIS_ID = UUID("00000000-0000-4000-8000-0000859490df")
NOTIFICATION_EVENT_ID = UUID("00000000-0000-4000-8000-000099999999")

LATER_EVENT_ID = UUID("00000000-0000-4000-8000-10003d5b3290")
LATER_JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-10007a111d13")
LATER_JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-1000c7d7ef5e")
LATER_BUNDLE_ID = UUID("00000000-0000-4000-8000-1000c51bd89e")
LATER_CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-100042c0d691")

DB_LOCATOR = "sentinel-private-database-locator"
REDIS_LOCATOR = "sentinel-private-redis-locator"
CHAT_ID = 987654321
RAW_PAYLOAD_SENTINEL = "private judge output payload must not print"
IDEMPOTENCY_SENTINEL = "private-idempotency-key-must-not-print"


class FakeRedisReader:
    def __init__(self, messages: list[RedisStreamMessage]) -> None:
        self.messages = messages
        self.calls: list[dict[str, Any]] = []

    async def read_candidate_messages(self, *, queue_name, config):
        self.calls.append({"queue_name": queue_name, "redis_scan_limit": config.redis_scan_limit})
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

        return BoundedPolicyApplyInventoryRedisReaderHandle(reader=self.reader, close=close)


class FakeRepository:
    def __init__(self) -> None:
        self.events: list[OutboxEventRow] = []
        self.candidates: dict[UUID, CandidatePolicyContext] = {}
        self.judge_runs: dict[UUID, JudgeRunPolicyContext] = {}
        self.judge_outputs: dict[UUID, JudgeOutputPolicyContext] = {}
        self.bundles: dict[UUID, BundlePolicyContext] = {}
        self.existing_analyses: dict[UUID, ExistingAnalysisInventoryRecord] = {}
        self.notification_status_by_analysis_id: dict[UUID, str | list[str] | None] = {}
        self.load_policy_apply_calls: list[int] = []
        self.notification_lookup_calls: list[object] = []

    async def load_policy_apply_events(self, *, db_limit):
        self.load_policy_apply_calls.append(db_limit)
        return sorted(self.events, key=lambda row: (row.created_at, row.event_id), reverse=True)[:db_limit]

    async def load_candidate_context(self, candidate_group_id):
        return self.candidates.get(candidate_group_id)

    async def load_judge_run(self, judge_run_id):
        return self.judge_runs.get(judge_run_id)

    async def load_judge_output(self, judge_output_id):
        return self.judge_outputs.get(judge_output_id)

    async def load_bundle_context(self, bundle_id):
        return self.bundles.get(bundle_id)

    async def load_existing_analysis_inventory(self, *, judge_output_id, policy_version, delivery_policy_version):
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
        rows = []
        for index, item in enumerate(statuses):
            if item == "invalid_payload":
                rows.append(_notification_outbox(intent=intent, status="pending", invalid_payload=True))
            else:
                rows.append(_notification_outbox(intent=intent, status=str(item), event_id=_offset_uuid(NOTIFICATION_EVENT_ID, index)))
        return rows


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

        return BoundedPolicyApplyInventoryRepositoryHandle(repository=self.repository, close=close)


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


def _runtime_config(
    *,
    enable_later_delivery: bool = True,
    enable_notification_send: bool = True,
) -> BoundedPolicyApplyInventoryRuntimeConfig:
    return BoundedPolicyApplyInventoryRuntimeConfig(
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


def _approved_config(**overrides) -> BoundedPolicyApplyInventoryConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "allow_redis_read": True,
        "allow_policy_preview": True,
        "db_limit": 100,
        "redis_scan_limit": 100,
        "max_results": 10,
        "prefer_verdict": "any",
        "include_processed": False,
        "include_suppressed": False,
    }
    values.update(overrides)
    return BoundedPolicyApplyInventoryConfig(**values)


def _run(
    repository: FakeRepository,
    messages: list[RedisStreamMessage],
    *,
    config: BoundedPolicyApplyInventoryConfig | None = None,
    runtime_config: BoundedPolicyApplyInventoryRuntimeConfig | None = None,
):
    reader = FakeRedisReader(messages)
    result = run_bounded_policy_apply_inventory_sync(
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
    analysis_id: UUID = ANALYSIS_ID,
    scores: dict[str, int] | None = None,
    model_proposed_verdict: str | None = "inspect_now",
    event_status: str = "published",
    judge_run_status: str = "succeeded",
    current_bundle_id: UUID | None = None,
    artifact_type: str = "github_repo",
    payload_extra: dict[str, Any] | None = None,
    existing_delivery_decision: str | None = None,
    existing_verdict: str = "inspect_now",
    created_offset: int = 0,
) -> RedisStreamMessage:
    repository.events.append(
        _policy_event(
            event_id=event_id,
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
            status=event_status,
            created_offset=created_offset,
        )
    )
    repository.candidates[candidate_group_id] = CandidatePolicyContext(
        candidate_group_id=candidate_group_id,
        current_bundle_id=bundle_id if current_bundle_id is None else current_bundle_id,
        current_analysis_id=analysis_id if existing_delivery_decision else None,
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
    if existing_delivery_decision is not None:
        repository.existing_analyses[judge_output_id] = ExistingAnalysisInventoryRecord(
            analysis_id=analysis_id,
            judge_output_id=judge_output_id,
            policy_version="verdict_policy_v1",
            delivery_policy_version="delivery_policy_v1",
            verdict=existing_verdict,
            delivery_decision=existing_delivery_decision,
        )
    return _redis_message(event_id=event_id, judge_run_id=judge_run_id)


def _redis_message(
    *,
    message_id: str = "1700000223450-0",
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
    payload_json: dict[str, Any] | None = None,
    created_offset: int = 0,
) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id,
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=judge_run_id,
        dedupe_key=f"analysis-policy-apply:{judge_run_id}:{judge_output_id}",
        payload_json=payload_json
        if payload_json is not None
        else {
            "judge_run_id": str(judge_run_id),
            "judge_output_id": str(judge_output_id),
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
        },
        status=status,
        fail_count=0,
        created_at=datetime(2026, 6, 17, tzinfo=timezone.utc) + timedelta(seconds=created_offset),
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
        judge_schema_version=None,
    )


def _notification_outbox(
    *,
    intent,
    status: str,
    event_id: UUID = NOTIFICATION_EVENT_ID,
    invalid_payload: bool = False,
) -> OutboxEventRow:
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
    if invalid_payload:
        payload.pop("material_change_hash")
    return OutboxEventRow(
        event_id=event_id,
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=intent.analysis_id,
        dedupe_key=f"notification-plan-created:{intent.analysis_id}:{intent.target_chat_id}:{intent.material_change_hash}",
        payload_json=payload,
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _inspect_scores() -> dict[str, int]:
    return {
        "practical_usefulness": 80,
        "evidence_strength": 80,
        "confidence": 80,
        "hype_penalty": 0,
        "code_quality": 80,
        "specificity": 80,
    }


def _later_scores() -> dict[str, int]:
    return {
        "practical_usefulness": 50,
        "evidence_strength": 40,
        "confidence": 40,
        "hype_penalty": 0,
        "code_quality": 10,
        "specificity": 10,
    }


def _skip_scores() -> dict[str, int]:
    return {
        "practical_usefulness": 10,
        "evidence_strength": 10,
        "confidence": 10,
        "hype_penalty": 0,
        "code_quality": 10,
        "specificity": 10,
    }


def _offset_uuid(value: UUID, offset: int) -> UUID:
    return UUID(int=value.int + offset)


def test_inventory_success_with_direct_runnable_non_suppress_emits_ready_argv() -> None:
    repository = FakeRepository()
    redis_message = _seed_target(repository)

    result, reader = _run(repository, [redis_message], config=_approved_config(redis_scan_limit=77))
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["status"] == "inventory_completed"
    assert report["db_policy_apply_event_count"] == 1
    assert report["redis_message_count"] == 1
    assert report["rehydrated_event_count"] == 1
    assert report["direct_runnable_non_suppress_count"] == 1
    assert report["db_non_suppress_unpublished_outbox_count"] == 0
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["redis_ack_called"] is False
    assert report["redis_consume_called"] is False
    assert report["policy_engine_called"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert report["openai_called"] is False
    assert reader.calls == [{"queue_name": "q.analysis.policy", "redis_scan_limit": 77}]

    selected = report["selected_direct_target"]
    assert selected["classification"] == "direct_runnable_non_suppress"
    assert selected["redis_message_id_suffix"] == "223450-0"
    assert selected["policy_apply_event_suffix"] == "3d5b3290"
    assert selected["judge_run_suffix"] == "7a111d13"
    assert selected["judge_output_suffix"] == "c7d7ef5e"
    assert selected["bundle_suffix"] == "c51bd89e"
    assert selected["candidate_group_suffix"] == "42c0d691"
    assert selected["policy_apply_outbox_status"] == "published"
    assert selected["predicted_verdict"] == "inspect_now"
    assert selected["predicted_delivery_decision"] == "send_now"
    assert selected["predicted_urgency_profile"] == "high"
    assert selected["analysis_exists"] is False
    assert selected["ready_policy_runner_argv"] == [
        "venv/bin/python",
        "tools/bounded_policy_engine_analysis_runner.py",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-redis-read",
        "--allow-redis-publish",
        "--allow-database-read",
        "--allow-database-write",
        "--allow-policy-engine",
        "--redis-message-suffix",
        "223450-0",
        "--trigger-event-suffix",
        "3d5b3290",
        "--judge-run-suffix",
        "7a111d13",
        "--judge-output-suffix",
        "c7d7ef5e",
        "--bundle-suffix",
        "c51bd89e",
        "--candidate-group-suffix",
        "42c0d691",
        "--scan-limit",
        "77",
    ]


def test_inventory_non_suppress_missing_redis_emits_recovery_target_without_ready_argv() -> None:
    repository = FakeRepository()
    _seed_target(repository)

    result, _reader = _run(repository, [])
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["selected_direct_target"] is None
    assert report["db_non_suppress_missing_redis_count"] == 1
    assert report["db_non_suppress_unpublished_outbox_count"] == 0
    recovery = report["selected_recovery_target"]
    assert recovery["classification"] == "db_non_suppress_missing_redis"
    assert recovery["policy_apply_outbox_status"] == "published"
    assert recovery["requires_requeue_or_db_direct_runner"] is True
    assert recovery["reason_code"] == "redis_message_missing"
    assert "ready_policy_runner_argv" not in recovery
    assert report["candidates"][0]["requires_requeue_or_db_direct_runner"] is True
    assert report["candidates"][0]["policy_apply_outbox_status"] == "published"
    assert "ready_policy_runner_argv" not in report["candidates"][0]


def _assert_unpublished_non_suppress_recovery(event_status: str) -> None:
    repository = FakeRepository()
    _seed_target(repository, event_status=event_status)

    result, _reader = _run(repository, [])
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["db_policy_apply_event_count"] == 1
    assert report["redis_message_count"] == 0
    assert report["rehydrated_event_count"] == 1
    assert report["direct_runnable_non_suppress_count"] == 0
    assert report["db_non_suppress_missing_redis_count"] == 0
    assert report["db_non_suppress_unpublished_outbox_count"] == 1
    assert report["selected_direct_target"] is None
    recovery = report["selected_recovery_target"]
    assert recovery is not None
    assert recovery["classification"] == "db_non_suppress_unpublished_outbox"
    assert recovery["policy_apply_outbox_status"] == event_status
    assert recovery["requires_outbox_relay_or_db_direct_runner"] is True
    assert recovery["reason_code"] == "outbox_status_not_published"
    assert "requires_requeue_or_db_direct_runner" not in recovery
    assert "ready_policy_runner_argv" not in recovery
    candidate = report["candidates"][0]
    assert candidate["classification"] == "db_non_suppress_unpublished_outbox"
    assert candidate["policy_apply_outbox_status"] == event_status
    assert candidate["requires_outbox_relay_or_db_direct_runner"] is True
    assert candidate["reason_code"] == "outbox_status_not_published"
    assert "ready_policy_runner_argv" not in candidate


def test_inventory_pending_non_suppress_policy_event_is_visible_as_unpublished_recovery() -> None:
    _assert_unpublished_non_suppress_recovery("pending")


def test_inventory_failed_non_suppress_policy_event_is_visible_as_unpublished_recovery() -> None:
    _assert_unpublished_non_suppress_recovery("failed")


def test_recovery_target_prefers_published_missing_redis_before_unpublished_outbox() -> None:
    repository = FakeRepository()
    _seed_target(repository, created_offset=1)
    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 20),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 20),
        judge_output_id=_offset_uuid(JUDGE_OUTPUT_ID, 20),
        bundle_id=_offset_uuid(BUNDLE_ID, 20),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 20),
        event_status="failed",
        created_offset=20,
    )

    result, _reader = _run(repository, [])
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["db_non_suppress_missing_redis_count"] == 1
    assert report["db_non_suppress_unpublished_outbox_count"] == 1
    assert report["selected_recovery_target"]["classification"] == "db_non_suppress_missing_redis"
    assert report["selected_recovery_target"]["policy_apply_outbox_status"] == "published"


def test_inventory_counts_suppress_processed_notification_and_blocked_classes() -> None:
    repository = FakeRepository()
    _seed_target(repository, scores=_skip_scores(), created_offset=1)

    processed_suppress_output = _offset_uuid(JUDGE_OUTPUT_ID, 10)
    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 10),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 10),
        judge_output_id=processed_suppress_output,
        bundle_id=_offset_uuid(BUNDLE_ID, 10),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 10),
        scores=_skip_scores(),
        existing_delivery_decision="suppress",
        existing_verdict="skip",
        created_offset=2,
    )

    pending_output = _offset_uuid(JUDGE_OUTPUT_ID, 20)
    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 20),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 20),
        judge_output_id=pending_output,
        bundle_id=_offset_uuid(BUNDLE_ID, 20),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 20),
        existing_delivery_decision="send_now",
        analysis_id=_offset_uuid(ANALYSIS_ID, 20),
        created_offset=3,
    )
    repository.notification_status_by_analysis_id[_offset_uuid(ANALYSIS_ID, 20)] = "pending"

    published_output = _offset_uuid(JUDGE_OUTPUT_ID, 30)
    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 30),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 30),
        judge_output_id=published_output,
        bundle_id=_offset_uuid(BUNDLE_ID, 30),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 30),
        existing_delivery_decision="send_now",
        analysis_id=_offset_uuid(ANALYSIS_ID, 30),
        created_offset=4,
    )
    repository.notification_status_by_analysis_id[_offset_uuid(ANALYSIS_ID, 30)] = "published"

    missing_output = _offset_uuid(JUDGE_OUTPUT_ID, 40)
    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 40),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 40),
        judge_output_id=missing_output,
        bundle_id=_offset_uuid(BUNDLE_ID, 40),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 40),
        existing_delivery_decision="send_now",
        analysis_id=_offset_uuid(ANALYSIS_ID, 40),
        created_offset=5,
    )

    invalid_output = _offset_uuid(JUDGE_OUTPUT_ID, 50)
    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 50),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 50),
        judge_output_id=invalid_output,
        bundle_id=_offset_uuid(BUNDLE_ID, 50),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 50),
        existing_delivery_decision="send_now",
        analysis_id=_offset_uuid(ANALYSIS_ID, 50),
        created_offset=6,
    )
    repository.notification_status_by_analysis_id[_offset_uuid(ANALYSIS_ID, 50)] = "invalid_payload"

    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 60),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 60),
        judge_output_id=_offset_uuid(JUDGE_OUTPUT_ID, 60),
        bundle_id=_offset_uuid(BUNDLE_ID, 60),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 60),
        current_bundle_id=_offset_uuid(BUNDLE_ID, 61),
        created_offset=7,
    )

    result, _reader = _run(
        repository,
        [],
        config=_approved_config(include_processed=True, include_suppressed=True, max_results=20),
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["unprocessed_suppress_count"] == 1
    assert report["processed_suppress_count"] == 1
    assert report["processed_notification_pending_count"] == 1
    assert report["processed_notification_published_count"] == 1
    assert report["processed_notification_missing_count"] == 1
    assert report["processed_notification_invalid_count"] == 1
    assert report["blocked_count"] == 1
    classifications = {candidate["classification"] for candidate in report["candidates"]}
    assert {
        "unprocessed_suppress",
        "processed_suppress",
        "processed_notification_pending",
        "processed_notification_published",
        "processed_notification_missing",
        "processed_notification_invalid",
        "blocked",
    } <= classifications


def test_blocked_class_covers_refusal_schema_and_malformed_payload() -> None:
    repository = FakeRepository()
    _seed_target(repository, payload_extra={"refusal_detected": True})
    _seed_target(
        repository,
        event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 100),
        judge_run_id=_offset_uuid(JUDGE_RUN_ID, 100),
        judge_output_id=_offset_uuid(JUDGE_OUTPUT_ID, 100),
        bundle_id=_offset_uuid(BUNDLE_ID, 100),
        candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 100),
        payload_extra={"judge_schema_version": "wrong"},
    )
    repository.events.append(
        _policy_event(
            event_id=_offset_uuid(POLICY_APPLY_EVENT_ID, 200),
            judge_run_id=_offset_uuid(JUDGE_RUN_ID, 200),
            judge_output_id=_offset_uuid(JUDGE_OUTPUT_ID, 200),
            bundle_id=_offset_uuid(BUNDLE_ID, 200),
            candidate_group_id=_offset_uuid(CANDIDATE_GROUP_ID, 200),
            payload_json={"judge_run_id": "not-a-uuid"},
        )
    )

    result, _reader = _run(repository, [])
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["blocked_count"] == 3
    assert {
        candidate["reason_code"]
        for candidate in report["candidates"]
        if candidate["classification"] == "blocked"
    } == {
        "judge_output_refusal_detected",
        "judge_output_schema_invalid",
        "event_payload_missing_required_field",
    }


def test_prefer_verdict_filtering_and_default_ranking() -> None:
    repository = FakeRepository()
    later_message = _seed_target(
        repository,
        event_id=LATER_EVENT_ID,
        judge_run_id=LATER_JUDGE_RUN_ID,
        judge_output_id=LATER_JUDGE_OUTPUT_ID,
        bundle_id=LATER_BUNDLE_ID,
        candidate_group_id=LATER_CANDIDATE_GROUP_ID,
        scores=_later_scores(),
        model_proposed_verdict="later",
        created_offset=20,
    )
    inspect_message = _seed_target(repository, created_offset=10)

    any_result, _reader = _run(repository, [later_message, inspect_message])
    assert any_result.to_sanitized_dict()["selected_direct_target"]["predicted_verdict"] == "inspect_now"
    assert [candidate.predicted_verdict for candidate in any_result.candidates[:2]] == ["inspect_now", "later"]

    inspect_result, _reader = _run(
        repository,
        [later_message, inspect_message],
        config=_approved_config(prefer_verdict="inspect_now"),
    )
    assert inspect_result.to_sanitized_dict()["selected_direct_target"]["predicted_verdict"] == "inspect_now"
    assert [candidate.predicted_verdict for candidate in inspect_result.candidates] == ["inspect_now"]

    later_result, _reader = _run(
        repository,
        [later_message, inspect_message],
        config=_approved_config(prefer_verdict="later"),
    )
    assert later_result.to_sanitized_dict()["selected_direct_target"]["predicted_verdict"] == "later"
    assert [candidate.predicted_verdict for candidate in later_result.candidates] == ["later"]


def test_sql_shape_uses_stable_columns_without_payload_operator_filtering() -> None:
    session = FakeSqlSession()
    repository = SqlAlchemyBoundedPolicyApplyInventoryRepository(session)

    asyncio.run(repository.load_policy_apply_events(db_limit=25))
    statement = str(session.calls[0][0])
    assert "FROM event_outbox" in statement
    assert "event_type = 'analysis.policy.apply.v1'" in statement
    assert "status = 'published'" not in statement
    assert "payload_json ->>" not in statement
    assert "payload_json->>" not in statement
    assert "ORDER BY created_at DESC, event_id DESC" in statement
    assert "LIMIT :limit" in statement
    assert session.calls[0][1] == {"limit": 25}

    source = INVENTORY_PATH.read_text(encoding="utf-8")
    assert "payload_json ->>" not in source
    assert "payload_json->>" not in source
    assert "event_type = 'notification.plan.created.v1'" in source
    assert "aggregate_type = 'analysis'" in source
    assert "aggregate_id = CAST(:analysis_id AS uuid)" in source
    assert "dedupe_key = :dedupe_key" in source


def test_redaction_omits_full_ids_private_locators_chat_ids_payloads_and_sql() -> None:
    repository = FakeRepository()
    redis_message = _seed_target(repository)

    result, _reader = _run(repository, [redis_message])
    rendered = render_sanitized_json(result.to_sanitized_dict())

    forbidden = [
        str(POLICY_APPLY_EVENT_ID),
        str(JUDGE_RUN_ID),
        str(JUDGE_OUTPUT_ID),
        str(BUNDLE_ID),
        str(CANDIDATE_GROUP_ID),
        str(ANALYSIS_ID),
        "1700000223450-0",
        DB_LOCATOR,
        REDIS_LOCATOR,
        str(CHAT_ID),
        IDEMPOTENCY_SENTINEL,
        RAW_PAYLOAD_SENTINEL,
        "SELECT event_id",
        "analysis-policy-apply:",
    ]
    for value in forbidden:
        assert value not in rendered

    parsed = json.loads(rendered)
    assert parsed["selected_direct_target"]["policy_apply_outbox_status"] == "published"
    assert parsed["candidates"][0]["policy_apply_outbox_status"] == "published"
    assert parsed["redactions_applied"]["full_policy_apply_event_id_omitted"] is True
    assert parsed["redactions_applied"]["target_chat_id_omitted"] is True
    assert parsed["redactions_applied"]["sql_text_omitted"] is True


def test_ast_import_and_side_effect_guard() -> None:
    source = INVENTORY_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_names: set[str] = set()
    called_attrs: set[str] = set()
    imported_from_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_names.update(alias.name for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_from_modules.add(node.module)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            called_attrs.add(node.func.attr.lower())

    forbidden_import_fragments = (
        "openai",
        "notifier",
        "telegram",
        "github",
        "gh_enricher",
        "x_enricher",
        "web_enricher",
        "subprocess",
        "docker",
        "alembic",
    )
    joined_imports = " ".join(sorted(imported_names | imported_from_modules)).lower()
    for fragment in forbidden_import_fragments:
        assert fragment not in joined_imports

    assert not ({"xack", "xreadgroup", "xgroup", "xclaim", "xautoclaim", "xadd"} & called_attrs)
    upper_source = source.upper()
    assert "INSERT INTO" not in upper_source
    assert "\n                UPDATE " not in upper_source
    assert "\n                DELETE " not in upper_source
    assert "SYSTEMD" not in upper_source
    assert "DOCKER" not in upper_source
    assert "ALEMBIC" not in upper_source
