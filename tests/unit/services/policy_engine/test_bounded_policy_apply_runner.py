from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from src.services.outbox_relay.models import OutboxEventRow
from src.services.policy_engine.bounded_policy_apply_runner import (
    BoundedPolicyApplyConfig,
    BoundedPolicyApplyRedisHandle,
    BoundedPolicyApplyRepositoryHandle,
    BoundedPolicyApplyRuntimeConfig,
    RedisPolicyApplyConsumer,
    TargetPolicyApplyMessage,
    run_bounded_policy_apply_sync,
)
from src.services.policy_engine.models import (
    AnalysisDraft,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
)


ROOT = Path(__file__).resolve().parents[4]
RUNNER_PATH = ROOT / "src/services/policy_engine/bounded_policy_apply_runner.py"

REDIS_MESSAGE_ID = "1700000223450-0"
POLICY_APPLY_EVENT_ID = UUID("00000000-0000-4000-8000-00003d5b3290")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-00007a111d13")
JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-0000c7d7ef5e")
BUNDLE_ID = UUID("00000000-0000-4000-8000-0000c51bd89e")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000042c0d691")
ANALYSIS_ID = UUID("00000000-0000-4000-8000-0000a6a1a6a1")
NOTIFICATION_EVENT_ID = UUID("00000000-0000-4000-8000-0000b7b2b7b2")
CHAT_ID = 987654321
DB_LOCATOR = "sentinel-private-database-locator"
REDIS_LOCATOR = "sentinel-private-redis-locator"
RAW_PAYLOAD_SENTINEL = "private judge output payload must not print"
RAW_EXCEPTION_SENTINEL = "private failure detail must not print"
IDEMPOTENCY_SENTINEL = "analysis-policy-apply:private-dedupe-key"


class FakeRedisClient:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, object]]] | None = None,
        *,
        group_exists: bool = True,
        group_pending: int = 0,
        group_lag: int | None = None,
        group_last_delivered_id: str = "0-0",
        pending_entries: list[dict[str, object]] | None = None,
        group_create_error: BaseException | None = None,
        ack_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.entries = entries or []
        self.group_exists = group_exists
        self.group_pending = group_pending
        self.group_lag = len(self.entries) if group_lag is None else group_lag
        self.group_last_delivered_id = group_last_delivered_id
        self.pending_entries = pending_entries or []
        self.group_create_error = group_create_error
        self.ack_error = ack_error
        self.order = order
        self.cursor = 0
        self.xrange_calls: list[dict[str, object]] = []
        self.xinfo_calls = 0
        self.xpending_range_calls: list[dict[str, object]] = []
        self.xgroup_create_calls: list[dict[str, object]] = []
        self.xreadgroup_calls = 0
        self.acked: list[str] = []

    async def xlen(self, name: str) -> int:
        assert name == "q.analysis.policy"
        return len(self.entries)

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, object]]]:
        assert name == "q.analysis.policy"
        assert max == "+" or max == min
        self.xrange_calls.append({"min": min, "count": count})
        if min == "-":
            entries = self.entries
        elif min.startswith("("):
            entries = [entry for entry in self.entries if _redis_stream_id_greater(entry[0], min[1:])]
        elif max == min:
            entries = [entry for entry in self.entries if entry[0] == min]
        else:
            raise AssertionError(f"unexpected xrange min: {min}")
        return entries[: count or len(entries)]

    async def xinfo_groups(self, name: str) -> list[dict[str, object]]:
        assert name == "q.analysis.policy"
        self.xinfo_calls += 1
        if not self.group_exists:
            return []
        return [
            {
                "name": "policy-engine",
                "pending": self.group_pending,
                "lag": self.group_lag,
                "last-delivered-id": self.group_last_delivered_id,
            }
        ]

    async def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
    ) -> list[dict[str, object]]:
        assert name == "q.analysis.policy"
        assert groupname == "policy-engine"
        assert min == "-"
        assert max == "+"
        self.xpending_range_calls.append(
            {
                "name": name,
                "groupname": groupname,
                "min": min,
                "max": max,
                "count": count,
            }
        )
        return self.pending_entries[:count]

    async def xgroup_create(self, name: str, groupname: str, id: str = "$", mkstream: bool = False):
        assert name == "q.analysis.policy"
        assert groupname == "policy-engine"
        assert id == "0-0"
        assert mkstream is False
        if self.order is not None:
            self.order.append("redis:group_create")
        self.xgroup_create_calls.append(
            {
                "name": name,
                "groupname": groupname,
                "id": id,
                "mkstream": mkstream,
            }
        )
        if self.group_create_error is not None:
            raise self.group_create_error
        self.group_exists = True
        self.group_pending = 0
        self.group_lag = len(self.entries)
        self.group_last_delivered_id = id
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, object]]]]]:
        assert groupname == "policy-engine"
        assert consumername == "bounded-test"
        assert streams == {"q.analysis.policy": ">"}
        assert block is None
        self.xreadgroup_calls += 1
        end = min(len(self.entries), self.cursor + (count or len(self.entries)))
        batch = self.entries[self.cursor : end]
        self.cursor = end
        return [("q.analysis.policy", batch)]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        assert name == "q.analysis.policy"
        assert groupname == "policy-engine"
        if self.order is not None:
            self.order.append("redis:ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(ids)
        return len(ids)


class FakeRedisBuilder:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        consumer = RedisPolicyApplyConsumer(
            self.client,
            queue_name="q.analysis.policy",
            consumer_group="policy-engine",
            consumer_name="bounded-test",
        )

        async def close() -> None:
            return None

        return BoundedPolicyApplyRedisHandle(consumer=consumer, close=close)


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
        commit_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.event = event if event is not None else _policy_event()
        self.candidate = candidate if candidate is not None else _candidate()
        self.judge_run = judge_run if judge_run is not None else _judge_run()
        self.judge_output = judge_output if judge_output is not None else _judge_output(_inspect_scores())
        self.bundle = bundle if bundle is not None else _bundle()
        self.existing_analysis = existing_analysis
        self.notification_rows = list(notification_rows or [])
        self.commit_error = commit_error
        self.order = order
        self.inserted_analyses: list[AnalysisDraft] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0
        self.notification_plan_table_writes = 0

    async def load_event_outbox(self, trigger_event_id):
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

    async def insert_analysis(self, draft):
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

    async def load_notification_plan_intent_outboxes(self, intent):
        dedupe_key = _notification_dedupe_key(intent)
        return [
            row
            for row in self.notification_rows
            if row.event_type == "notification.plan.created.v1"
            and row.aggregate_type == "analysis"
            and row.aggregate_id == intent.analysis_id
            and row.dedupe_key == dedupe_key
        ][:2]

    async def insert_or_load_notification_plan_intent_outbox(self, intent):
        dedupe_key = _notification_dedupe_key(intent)
        for row in self.notification_rows:
            if row.dedupe_key == dedupe_key:
                return row, False
        row = _notification_outbox(intent=intent)
        self.notification_rows.append(row)
        return row, True

    async def commit(self) -> None:
        self.commits += 1
        if self.order is not None:
            self.order.append("db:commit")
        if self.commit_error is not None:
            raise self.commit_error

    async def rollback(self) -> None:
        self.rollbacks += 1


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close() -> None:
            return None

        return BoundedPolicyApplyRepositoryHandle(repository=self.repository, close=close)


def _runtime_config(*, enable_later_delivery: bool = True, operator_chat_id: int = CHAT_ID):
    return BoundedPolicyApplyRuntimeConfig(
        database_url=DB_LOCATOR,
        redis_url=REDIS_LOCATOR,
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="bounded-test",
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=operator_chat_id,
        enable_later_delivery=enable_later_delivery,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
    )


def _raising_runtime_config():
    raise AssertionError("runtime config must not load")


def _config(*, mode: str = "execute", **overrides) -> BoundedPolicyApplyConfig:
    values = {
        "mode": mode,
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_read": True,
        "allow_redis_group_create": False,
        "allow_database_read": True,
        "allow_redis_consume": mode == "execute",
        "allow_database_write": mode == "execute",
        "allow_redis_ack": mode == "execute",
        "trigger_event_suffix": "3d5b3290",
        "judge_run_suffix": "7a111d13",
        "judge_output_suffix": "c7d7ef5e",
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedPolicyApplyConfig(**values)


def _redis_message(**field_overrides: str) -> tuple[str, dict[str, object]]:
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
    return REDIS_MESSAGE_ID, fields


def _policy_event(payload_json: dict[str, Any] | None = None, status: str = "published") -> OutboxEventRow:
    return OutboxEventRow(
        event_id=POLICY_APPLY_EVENT_ID,
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


def _judge_output(
    scores: dict[str, int],
    *,
    model_proposed_verdict: str | None = "inspect_now",
    judge_run_id: UUID = JUDGE_RUN_ID,
) -> JudgeOutputPolicyContext:
    return JudgeOutputPolicyContext(
        judge_output_id=JUDGE_OUTPUT_ID,
        judge_run_id=judge_run_id,
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


def _notification_outbox(*, intent: NotificationPlanIntent) -> OutboxEventRow:
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
            "render_profile": intent.render_profile,
            "dedupe_subject_key": intent.dedupe_subject_key,
            "material_change_hash": intent.material_change_hash,
            "target_chat_id": intent.target_chat_id,
            "target_thread_id": intent.target_thread_id,
            "send_after": intent.send_after,
            "suppress_reason_code": intent.suppress_reason_code,
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _notification_dedupe_key(intent: NotificationPlanIntent) -> str:
    return f"notification-plan-created:{intent.analysis_id}:{intent.target_chat_id}:{intent.material_change_hash}"


def _inspect_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "practical_usefulness": 75,
        "evidence_strength": 60,
        "confidence": 70,
        "hype_penalty": 20,
        "code_quality": 70,
    }
    scores.update(overrides)
    return scores


def _later_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "practical_usefulness": 50,
        "evidence_strength": 35,
        "confidence": 40,
        "hype_penalty": 85,
        "code_quality": 0,
    }
    scores.update(overrides)
    return scores


def _skip_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "practical_usefulness": 30,
        "evidence_strength": 20,
        "confidence": 20,
        "hype_penalty": 20,
        "code_quality": 0,
    }
    scores.update(overrides)
    return scores


def _live_like_0_to_10_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 5,
        "practical_usefulness": 7,
        "evidence_strength": 7,
        "hype_penalty": 2,
        "confidence": 7,
        "code_quality": 7,
        "maintenance_signal": 7,
        "specificity": 8,
        "reproducibility_signal": 6,
    }
    scores.update(overrides)
    return scores


def _run(
    repository: FakeRepository,
    *,
    client: FakeRedisClient | None = None,
    config: BoundedPolicyApplyConfig | None = None,
    runtime_config: BoundedPolicyApplyRuntimeConfig | None = None,
):
    client = client or FakeRedisClient([_redis_message()])
    repo_builder = FakeRepositoryBuilder(repository)
    result = run_bounded_policy_apply_sync(
        config or _config(),
        runtime_config_loader=lambda: runtime_config or _runtime_config(),
        redis_builder=FakeRedisBuilder(client),
        repository_builder=repo_builder,
    )
    return result, client, repo_builder


def test_gate_failures_before_runtime_config_db_or_redis() -> None:
    cases = (
        (BoundedPolicyApplyConfig(mode="execute"), "operator_approval_missing"),
        (_config(allow_runtime_config=False), "runtime_config_not_allowed"),
        (_config(allow_redis_read=False), "redis_read_not_allowed"),
        (_config(allow_database_read=False), "database_read_not_allowed"),
        (_config(allow_redis_consume=False), "redis_consume_not_allowed"),
        (_config(allow_database_write=False), "database_write_not_allowed"),
        (_config(allow_redis_ack=False), "redis_ack_not_allowed"),
    )
    for config, error_code in cases:
        result = run_bounded_policy_apply_sync(config, runtime_config_loader=_raising_runtime_config)
        report = result.to_sanitized_dict()

        assert report["status"] == "blocked"
        assert report["error_code"] == error_code
        assert report["side_effects"]["redis_read_called"] is False
        assert report["side_effects"]["db_read"] is False
        assert report["side_effects"]["db_write"] is False


def test_preview_with_group_missing_blocks_without_side_effects_even_when_create_flag_passed() -> None:
    repository = FakeRepository()
    client = FakeRedisClient([_redis_message()], group_exists=False)

    result, client, repo_builder = _run(
        repository,
        client=client,
        config=_config(mode="preview", allow_redis_group_create=True),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is False
    assert report["status"] == "blocked"
    assert report["error_code"] == "consumer_group_missing"
    assert report["group_name"] == "policy-engine"
    assert report["group_exists"] is False
    assert report["group_create_attempted"] is False
    assert report["group_created"] is False
    assert report["group_create_skipped_reason"] == "preview_mode"
    assert report["planned_action"] == "fail_closed"
    assert report["would_fail_closed"] is True
    assert client.xgroup_create_calls == []
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []
    assert repository.commits == 0
    assert report["side_effects"]["redis_group_create_called"] is False
    assert report["side_effects"]["redis_consume_called"] is False
    assert report["side_effects"]["redis_ack_called"] is False
    assert report["side_effects"]["db_write"] is False


def test_exact_target_happy_path_creates_analysis_and_notification_plan_intent_then_acks() -> None:
    order: list[str] = []
    repository = FakeRepository(order=order)
    client = FakeRedisClient([_redis_message()], order=order)

    result, client, _ = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "applied"
    assert report["planned_action"] == "create_analysis_and_notification_intent"
    assert report["target_redis_message_id_suffix"] == "223450-0"
    assert report["target_policy_apply_event_suffix"] == "3d5b3290"
    assert report["target_judge_run_id_suffix"] == "7a111d13"
    assert report["target_judge_output_id_suffix"] == "c7d7ef5e"
    assert report["target_analysis_id_suffix"] == "a6a1a6a1"
    assert report["analysis_written"] is True
    assert report["state_transition_written"] is True
    assert report["notification_plan_intent_outbox_written"] is True
    assert report["side_effects"]["q_notification_send_published"] is False
    assert report["redis_ack_status"] == "acked"
    assert client.acked == [REDIS_MESSAGE_ID]
    assert order == ["db:commit", "redis:ack"]
    assert repository.notification_rows[0].status == "pending"


def test_later_with_delivery_enabled_creates_normal_silent_plan_intent() -> None:
    repository = FakeRepository(judge_output=_judge_output(_later_scores(), model_proposed_verdict="later"))

    result, _, _ = _run(repository)
    report = result.to_sanitized_dict()

    assert report["verdict"] == "later"
    assert report["delivery_decision"] == "send_now"
    assert report["urgency_profile"] == "normal_silent"
    assert report["notification_plan_intent_outbox_written"] is True
    assert repository.notification_rows[0].payload_json["urgency_profile"] == "normal_silent"


def test_0_to_10_model_scores_are_normalized_before_bounded_apply_persistence() -> None:
    repository = FakeRepository(judge_output=_judge_output(_live_like_0_to_10_scores(), model_proposed_verdict="later"))

    result, _, _ = _run(repository)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["verdict"] in {"later", "inspect_now"}
    assert len(repository.inserted_analyses) == 1
    analysis = repository.inserted_analyses[0]
    assert analysis.scores_json["practical_usefulness"] == 70
    assert analysis.scores_json["specificity"] == 80
    assert analysis.reason_codes_json[:3] == [
        "judge_output_validated",
        "policy_score_scale_normalized_0_10_to_0_100",
        "policy_threshold_inspect_now",
    ]
    assert analysis.reason_codes_json[-1] == "policy_overrode_model_verdict"


def test_later_with_delivery_disabled_creates_analysis_without_notification_intent() -> None:
    repository = FakeRepository(judge_output=_judge_output(_later_scores(), model_proposed_verdict="later"))

    result, _, _ = _run(repository, runtime_config=_runtime_config(enable_later_delivery=False))
    report = result.to_sanitized_dict()

    assert report["planned_action"] == "create_analysis_suppress_only"
    assert report["verdict"] == "later"
    assert report["delivery_decision"] == "suppress"
    assert report["urgency_profile"] == "suppressed"
    assert len(repository.inserted_analyses) == 1
    assert repository.notification_rows == []


def test_skip_creates_analysis_without_notification_intent() -> None:
    repository = FakeRepository(judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"))

    result, _, _ = _run(repository)
    report = result.to_sanitized_dict()

    assert report["planned_action"] == "create_analysis_suppress_only"
    assert report["verdict"] == "skip"
    assert report["delivery_decision"] == "suppress"
    assert len(repository.inserted_analyses) == 1
    assert repository.notification_rows == []
    assert report["target_notification_plan_event_suffix"] is None
    assert report["side_effects"]["q_notification_send_published"] is False


def test_existing_analysis_reuse_does_not_duplicate_analysis_or_notification_intent() -> None:
    order: list[str] = []
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"),
        order=order,
    )
    client = FakeRedisClient([_redis_message()], order=order)

    result, client, _ = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["planned_action"] == "reuse_existing_analysis"
    assert report["existing_analysis_found"] is True
    assert report["verdict"] == "skip"
    assert report["delivery_decision"] == "suppress"
    assert report["urgency_profile"] == "suppressed"
    assert report["analysis_written"] is False
    assert report["notification_plan_intent_outbox_written"] is False
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []
    assert repository.commits == 1
    assert client.acked == [REDIS_MESSAGE_ID]
    assert order == ["db:commit", "redis:ack"]


def test_stale_bundle_blocks_before_consume_write_or_ack() -> None:
    repository = FakeRepository(candidate=_candidate(current_bundle_id=uuid4()))
    client = FakeRedisClient([_redis_message()])

    result, client, _ = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "stale_bundle"
    assert report["planned_action"] == "noop"
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []


def test_mismatched_judge_output_blocks_before_write() -> None:
    repository = FakeRepository(judge_output=_judge_output(_inspect_scores(), judge_run_id=uuid4()))
    client = FakeRedisClient([_redis_message()])

    result, client, _ = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "judge_output_judge_run_mismatch"
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repository.inserted_analyses == []
    assert repository.commits == 0


def test_policy_override_adds_reason_code() -> None:
    repository = FakeRepository(judge_output=_judge_output(_inspect_scores(), model_proposed_verdict="skip"))

    result, _, _ = _run(repository)
    report = result.to_sanitized_dict()

    assert report["policy_reconciled_flag"] is False
    assert "policy_overrode_model_verdict" in repository.inserted_analyses[0].reason_codes_json


def test_suppress_path_still_writes_analysis_and_state_transition() -> None:
    repository = FakeRepository(judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"))

    result, _, _ = _run(repository)
    report = result.to_sanitized_dict()

    assert report["analysis_written"] is True
    assert report["state_transition_written"] is True
    assert repository.state_transitions[0]["to_state"] == "analysis_policy_suppressed"
    assert repository.notification_rows == []


def test_non_suppress_path_does_not_write_notification_plans_table() -> None:
    repository = FakeRepository()

    result, _, _ = _run(repository)
    report = result.to_sanitized_dict()

    assert report["notification_plan_intent_outbox_written"] is True
    assert repository.notification_plan_table_writes == 0
    assert report["side_effects"]["notification_plans_table_written"] is False


def test_notification_plan_created_payload_is_intent_only_without_rendered_telegram_text() -> None:
    repository = FakeRepository()

    result, _, _ = _run(repository)
    payload = repository.notification_rows[0].payload_json
    report_text = json.dumps(result.to_sanitized_dict(), ensure_ascii=False)

    assert {
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "delivery_decision",
        "urgency_profile",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
        "target_chat_id",
        "target_thread_id",
        "send_after",
        "suppress_reason_code",
    } <= set(payload)
    assert "message_text" not in payload
    assert "entities_json" not in payload
    assert "reply_markup_json" not in payload
    assert RAW_PAYLOAD_SENTINEL not in report_text
    assert str(CHAT_ID) not in report_text


def test_sanitized_report_omits_full_ids_locators_chat_raw_payload_and_dedupe_keys() -> None:
    repository = FakeRepository(existing_analysis=_existing_analysis())

    result, _, _ = _run(repository)
    report_text = json.dumps(result.to_sanitized_dict(), ensure_ascii=False)

    forbidden_fragments = {
        REDIS_MESSAGE_ID,
        str(POLICY_APPLY_EVENT_ID),
        str(JUDGE_RUN_ID),
        str(JUDGE_OUTPUT_ID),
        str(BUNDLE_ID),
        str(CANDIDATE_GROUP_ID),
        str(ANALYSIS_ID),
        DB_LOCATOR,
        REDIS_LOCATOR,
        str(CHAT_ID),
        RAW_PAYLOAD_SENTINEL,
        RAW_EXCEPTION_SENTINEL,
        IDEMPOTENCY_SENTINEL,
        f"analysis-policy-apply:{JUDGE_RUN_ID}:{JUDGE_OUTPUT_ID}",
    }
    for fragment in forbidden_fragments:
        assert fragment not in report_text

    assert '"target_policy_apply_event_suffix": "3d5b3290"' in report_text
    assert '"target_judge_run_id_suffix": "7a111d13"' in report_text
    assert '"target_judge_output_id_suffix": "c7d7ef5e"' in report_text
    assert '"target_redis_message_id_suffix": "223450-0"' in report_text


def test_redis_group_missing_blocks_execute_before_consume_db_or_ack() -> None:
    repository = FakeRepository()
    client = FakeRedisClient([_redis_message()], group_exists=False)

    result, client, repo_builder = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["error_code"] == "redis_group_create_not_allowed"
    assert report["group_create_attempted"] is False
    assert report["group_created"] is False
    assert report["group_create_skipped_reason"] == "redis_group_create_not_allowed"
    assert client.xgroup_create_calls == []
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0
    assert repository.inserted_analyses == []


def test_group_missing_with_create_authority_creates_group_then_reuses_existing_analysis_and_acks() -> None:
    order: list[str] = []
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"),
        order=order,
    )
    client = FakeRedisClient([_redis_message()], group_exists=False, order=order)

    result, client, _ = _run(
        repository,
        client=client,
        config=_config(allow_redis_group_create=True),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "applied"
    assert report["group_create_attempted"] is True
    assert report["group_created"] is True
    assert report["group_create_skipped_reason"] is None
    assert report["group_exists"] is True
    assert report["group_last_delivered_id_suffix"] == "0-0"
    assert report["target_is_next_deliverable"] is True
    assert report["existing_analysis_found"] is True
    assert report["planned_action"] == "reuse_existing_analysis"
    assert report["analysis_written"] is False
    assert report["notification_plan_intent_outbox_written"] is False
    assert report["verdict"] == "skip"
    assert report["delivery_decision"] == "suppress"
    assert report["urgency_profile"] == "suppressed"
    assert client.xgroup_create_calls == [
        {
            "name": "q.analysis.policy",
            "groupname": "policy-engine",
            "id": "0-0",
            "mkstream": False,
        }
    ]
    assert client.xreadgroup_calls == 1
    assert client.acked == [REDIS_MESSAGE_ID]
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []
    assert repository.commits == 1
    assert order == ["redis:group_create", "db:commit", "redis:ack"]


def test_group_create_denied_when_target_is_not_first_deliverable() -> None:
    _, non_target_fields = _redis_message(trigger_event_id=str(uuid4()))
    non_target = "1700000223449-0", non_target_fields
    repository = FakeRepository(existing_analysis=_existing_analysis())
    client = FakeRedisClient([non_target, _redis_message()], group_exists=False)

    result, client, repo_builder = _run(
        repository,
        client=client,
        config=_config(allow_redis_group_create=True),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "target_not_first_deliverable_for_group_create"
    assert report["group_create_attempted"] is False
    assert report["group_created"] is False
    assert report["group_create_skipped_reason"] == "target_not_first_deliverable_for_group_create"
    assert client.xgroup_create_calls == []
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0


def test_group_create_requires_exact_secondary_selector() -> None:
    repository = FakeRepository()
    client = FakeRedisClient([_redis_message()], group_exists=False)

    result, client, repo_builder = _run(
        repository,
        client=client,
        config=_config(allow_redis_group_create=True, judge_run_suffix=None, judge_output_suffix=None),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "group_create_exact_selector_missing"
    assert report["group_create_attempted"] is False
    assert report["group_create_skipped_reason"] == "group_create_exact_selector_missing"
    assert client.xgroup_create_calls == []
    assert repo_builder.calls == 0


def test_group_pending_nonzero_blocks_execute() -> None:
    repository = FakeRepository()
    client = FakeRedisClient([_redis_message()], group_pending=1)

    result, client, repo_builder = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["error_code"] == "consumer_group_pending_not_exactly_one"
    assert report["target_pending_found"] is False
    assert report["pending_recovery_attempted"] is False
    assert report["pending_recovery_skipped_reason"] == "consumer_group_pending_not_exactly_one"
    assert client.xpending_range_calls == [
        {
            "name": "q.analysis.policy",
            "groupname": "policy-engine",
            "min": "-",
            "max": "+",
            "count": 2,
        }
    ]
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0


def test_preview_with_exact_pending_target_reports_without_recovery_or_ack() -> None:
    repository = FakeRepository(existing_analysis=_existing_analysis())
    client = FakeRedisClient(
        [_redis_message()],
        group_pending=1,
        pending_entries=[
            {
                "message_id": REDIS_MESSAGE_ID,
                "consumer": "bounded-policy-apply-f4bfa3c1",
                "times_delivered": 1,
            }
        ],
    )

    result, client, repo_builder = _run(repository, client=client, config=_config(mode="preview"))
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "preview_mode"
    assert report["target_pending_found"] is True
    assert report["target_pending_consumer"] == "bounded-policy-apply-f4bfa3c1"
    assert report["target_pending_times_delivered"] == 1
    assert report["pending_recovery_attempted"] is False
    assert report["pending_recovery_path"] is None
    assert report["pending_recovery_skipped_reason"] == "preview_mode"
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0
    assert repository.commits == 0


def test_execute_with_pending_target_missing_consume_authority_fails_before_runtime() -> None:
    result = run_bounded_policy_apply_sync(
        _config(allow_redis_consume=False),
        runtime_config_loader=_raising_runtime_config,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_consume_not_allowed"
    assert report["target_pending_found"] is False
    assert report["pending_recovery_attempted"] is False
    assert report["side_effects"]["redis_read_called"] is False
    assert report["side_effects"]["redis_ack_called"] is False


def test_execute_with_pending_target_missing_ack_authority_fails_before_ack() -> None:
    result = run_bounded_policy_apply_sync(
        _config(allow_redis_ack=False),
        runtime_config_loader=_raising_runtime_config,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_ack_not_allowed"
    assert report["pending_recovery_attempted"] is False
    assert report["side_effects"]["redis_read_called"] is False
    assert report["side_effects"]["redis_ack_called"] is False


def test_exact_pending_target_rehydrates_via_xrange_reuses_existing_analysis_and_acks_after_commit() -> None:
    order: list[str] = []
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"),
        order=order,
    )
    client = FakeRedisClient(
        [_redis_message()],
        group_pending=1,
        pending_entries=[
            {
                "message_id": REDIS_MESSAGE_ID,
                "consumer": "bounded-policy-apply-f4bfa3c1",
                "times_delivered": 1,
            }
        ],
        order=order,
    )

    result, client, _ = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "applied"
    assert report["target_pending_found"] is True
    assert report["target_pending_consumer"] == "bounded-policy-apply-f4bfa3c1"
    assert report["target_pending_times_delivered"] == 1
    assert report["pending_recovery_attempted"] is True
    assert report["pending_recovery_path"] == "xrange_exact_stream_id"
    assert report["pending_recovery_skipped_reason"] is None
    assert report["existing_analysis_found"] is True
    assert report["planned_action"] == "reuse_existing_analysis"
    assert report["analysis_written"] is False
    assert report["notification_plan_intent_outbox_written"] is False
    assert report["verdict"] == "skip"
    assert report["delivery_decision"] == "suppress"
    assert report["urgency_profile"] == "suppressed"
    assert report["redis_ack_status"] == "acked"
    assert report["redis_acked_count"] == 1
    assert client.xreadgroup_calls == 0
    assert {"min": REDIS_MESSAGE_ID, "count": 1} in client.xrange_calls
    assert client.acked == [REDIS_MESSAGE_ID]
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []
    assert repository.commits == 1
    assert order == ["db:commit", "redis:ack"]


def test_pending_target_selector_mismatch_fails_closed_without_ack() -> None:
    repository = FakeRepository(
        existing_analysis=_existing_analysis(),
        judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"),
    )
    client = FakeRedisClient(
        [_redis_message()],
        group_pending=1,
        pending_entries=[
            {
                "message_id": REDIS_MESSAGE_ID,
                "consumer": "bounded-policy-apply-f4bfa3c1",
                "times_delivered": 1,
            }
        ],
    )

    result, client, _ = _run(repository, client=client, config=_config(judge_output_suffix="badc0de"))
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "judge_output_selector_mismatch"
    assert report["target_pending_found"] is True
    assert report["pending_recovery_attempted"] is True
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repository.commits == 0


def test_non_target_pending_message_is_not_processed_or_acked() -> None:
    repository = FakeRepository(existing_analysis=_existing_analysis())
    client = FakeRedisClient(
        [_redis_message()],
        group_pending=1,
        pending_entries=[
            {
                "message_id": "1700000223449-0",
                "consumer": "bounded-policy-apply-other",
                "times_delivered": 1,
            }
        ],
    )

    result, client, repo_builder = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "pending_message_not_target"
    assert report["target_pending_found"] is False
    assert report["pending_recovery_attempted"] is False
    assert report["pending_recovery_skipped_reason"] == "pending_message_not_target"
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0
    assert repository.commits == 0


def test_multiple_pending_messages_fail_closed_without_touching_non_target() -> None:
    repository = FakeRepository(existing_analysis=_existing_analysis())
    client = FakeRedisClient(
        [_redis_message()],
        group_pending=2,
        pending_entries=[
            {
                "message_id": REDIS_MESSAGE_ID,
                "consumer": "bounded-policy-apply-f4bfa3c1",
                "times_delivered": 1,
            },
            {
                "message_id": "1700000223449-0",
                "consumer": "bounded-policy-apply-other",
                "times_delivered": 1,
            },
        ],
    )

    result, client, repo_builder = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "consumer_group_pending_not_exactly_one"
    assert report["target_pending_found"] is False
    assert report["pending_recovery_attempted"] is False
    assert client.xpending_range_calls == []
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0


def test_target_not_next_deliverable_blocks_execute() -> None:
    _, non_target_fields = _redis_message(trigger_event_id=str(uuid4()))
    non_target = "1700000223449-0", non_target_fields
    repository = FakeRepository()
    client = FakeRedisClient([non_target, _redis_message()])

    result, client, repo_builder = _run(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["error_code"] == "target_not_next_deliverable"
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repo_builder.calls == 0


def test_database_commit_failure_does_not_ack() -> None:
    repository = FakeRepository(commit_error=RuntimeError(RAW_EXCEPTION_SENTINEL))
    client = FakeRedisClient([_redis_message()])

    result, client, _ = _run(repository, client=client)
    report_text = json.dumps(result.to_sanitized_dict(), ensure_ascii=False)
    report = result.to_sanitized_dict()

    assert report["status"] == "failed"
    assert report["error_code"] == "database_commit_failed"
    assert report["redis_ack_status"] == "not_attempted"
    assert client.acked == []
    assert RAW_EXCEPTION_SENTINEL not in report_text


def test_ack_failure_reports_sanitized_failure_after_commit() -> None:
    repository = FakeRepository()
    client = FakeRedisClient([_redis_message()], ack_error=RuntimeError(RAW_EXCEPTION_SENTINEL))

    result, client, _ = _run(repository, client=client)
    report = result.to_sanitized_dict()
    report_text = json.dumps(report, ensure_ascii=False)

    assert report["status"] == "failed"
    assert report["error_code"] == "redis_ack_failed"
    assert report["redis_ack_status"] == "failed"
    assert repository.commits == 1
    assert client.acked == []
    assert RAW_EXCEPTION_SENTINEL not in report_text


def test_ast_guards_forbidden_authorities_and_runtime_calls() -> None:
    source = RUNNER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    call_attrs: set[str] = set()
    call_names: set[str] = set()
    constants: set[str] = set()
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
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            constants.add(node.value.lower())

    forbidden_import_fragments = (
        "openai",
        "notifier_telegram",
        "telegram_client",
        "gh_enricher",
        "x_enricher",
        "web_enricher",
        "subprocess",
    )
    forbidden_calls = {
        "send_message",
        "publish",
        "run_forever",
        "systemctl",
        "docker",
        "alembic",
        "create_subprocess_exec",
        "create_subprocess_shell",
    }
    assert all(not any(fragment in imported for fragment in forbidden_import_fragments) for imported in imports)
    assert not forbidden_calls & call_attrs
    assert not forbidden_calls & call_names
    assert "q.notification.send" not in constants


def _redis_stream_id_greater(left: str, right: str) -> bool:
    return _parse_redis_stream_id(left) > _parse_redis_stream_id(right)


def _parse_redis_stream_id(value: str) -> tuple[int, int]:
    left, right = value.split("-", 1)
    return int(left), int(right)
