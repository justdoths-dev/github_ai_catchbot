from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.policy_engine.bounded_notification_intent import (
    BoundedPolicyNotificationIntentConfig,
    BoundedPolicyNotificationIntentRuntimeHandle,
    PolicyApplyBacklogCounts,
    PolicyApplyEventRow,
    PolicyInvocationSummary,
    run_bounded_policy_notification_intent,
)
from src.services.policy_engine.config import PolicyEngineConfig


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/policy_engine/bounded_notification_intent.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel_policy_redis_url"
TELEGRAM_TOKEN = "123456:sentinel_telegram_token"
RAW_PAYLOAD_VALUE = "sentinel raw policy payload"
RENDERED_TEXT = "sentinel rendered message text"
EXCEPTION_DETAIL = "sentinel private policy failure detail"
REQUIRED_FIELDS = (
    "judge_run_id",
    "judge_output_id",
    "candidate_group_id",
    "bundle_id",
)


class FakeRepository:
    def __init__(
        self,
        *,
        rows: list[PolicyApplyEventRow] | None = None,
        existing_analysis_judge_output_ids: set[UUID] | None = None,
        counts: PolicyApplyBacklogCounts | None = None,
        operation_log: list[str] | None = None,
    ) -> None:
        self.rows = rows if rows is not None else []
        self.existing_analysis_judge_output_ids = existing_analysis_judge_output_ids or set()
        self.counts = counts
        self.operation_log = operation_log if operation_log is not None else []
        self.count_calls = 0
        self.fetch_calls = 0

    async def load_pending_policy_apply_event_counts(self) -> PolicyApplyBacklogCounts:
        self.operation_log.append("count")
        self.count_calls += 1
        if self.counts is not None:
            return self.counts
        malformed = 0
        stale = 0
        eligible = 0
        for row in self.rows:
            if not _payload_valid(row.payload_json):
                malformed += 1
                continue
            judge_output_id = UUID(str(row.payload_json["judge_output_id"]))
            if judge_output_id in self.existing_analysis_judge_output_ids:
                stale += 1
            else:
                eligible += 1
        return PolicyApplyBacklogCounts(
            raw_pending=len(self.rows),
            eligible_pending=eligible,
            stale_already_analyzed=stale,
            malformed_pending=malformed,
        )

    async def fetch_oldest_eligible_pending_policy_apply_event(self) -> PolicyApplyEventRow | None:
        self.operation_log.append("fetch_eligible")
        self.fetch_calls += 1
        eligible_rows = [
            row
            for row in self.rows
            if _payload_valid(row.payload_json)
            and UUID(str(row.payload_json["judge_output_id"])) not in self.existing_analysis_judge_output_ids
        ]
        return sorted(eligible_rows, key=lambda row: (row.created_at, row.event_id))[0] if eligible_rows else None


class FakePolicyInvoker:
    def __init__(
        self,
        *,
        summary: PolicyInvocationSummary | None = None,
        failure: BaseException | None = None,
        operation_log: list[str] | None = None,
    ) -> None:
        self.summary = summary or PolicyInvocationSummary(
            processed_event_count=1,
            analysis_created=True,
            state_transition_inserted=True,
            notification_plan_created_event_emitted=True,
            notification_plan_created_event_id=uuid4(),
            delivery_decision="send_now",
            verdict="inspect_now",
        )
        self.failure = failure
        self.operation_log = operation_log if operation_log is not None else []
        self.calls: list[UUID] = []

    async def __call__(self, trigger_event_id: UUID) -> PolicyInvocationSummary:
        self.operation_log.append("policy")
        self.calls.append(trigger_event_id)
        if self.failure is not None:
            raise self.failure
        return self.summary


class FakeRuntimeBuilder:
    def __init__(self, repository: FakeRepository, invoker: FakePolicyInvoker) -> None:
        self.repository = repository
        self.invoker = invoker
        self.calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)

        return BoundedPolicyNotificationIntentRuntimeHandle(
            repository=self.repository,
            policy_invoker=self.invoker,
            close=close,
        )


def _runtime_config() -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url=DB_URL,
        redis_url=REDIS_URL,
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="bounded-policy-notification-intent-test",
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


def _raising_runtime_config() -> PolicyEngineConfig:
    raise AssertionError("runtime config must not be loaded")


def _payload(**overrides) -> dict[str, object]:
    payload = {
        "judge_run_id": str(uuid4()),
        "judge_output_id": str(uuid4()),
        "candidate_group_id": str(uuid4()),
        "bundle_id": str(uuid4()),
        "database_url": DB_URL,
        "telegram_bot_token": TELEGRAM_TOKEN,
        "rendered_message_text": RENDERED_TEXT,
        "raw_payload": RAW_PAYLOAD_VALUE,
    }
    payload.update(overrides)
    return payload


def _payload_valid(payload: dict[str, object]) -> bool:
    for field_name in REQUIRED_FIELDS:
        try:
            UUID(str(payload.get(field_name)))
        except (TypeError, ValueError, AttributeError):
            return False
    return True


def _row(
    *,
    payload_json: dict[str, object] | None = None,
    created_at: datetime | None = None,
) -> PolicyApplyEventRow:
    return PolicyApplyEventRow(
        event_id=uuid4(),
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=uuid4(),
        payload_json=payload_json if payload_json is not None else _payload(),
        status="pending",
        created_at=created_at if created_at is not None else datetime.now(timezone.utc),
    )


def _approved_config(**overrides) -> BoundedPolicyNotificationIntentConfig:
    values = {
        "operator_approved": True,
        "allow_database_read": True,
        "allow_policy_write": True,
        "expected_pending_count": 1,
    }
    values.update(overrides)
    return BoundedPolicyNotificationIntentConfig(**values)


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_db() -> None:
    runtime_builder = FakeRuntimeBuilder(FakeRepository(rows=[_row()]), FakePolicyInvoker())

    result = await run_bounded_policy_notification_intent(
        BoundedPolicyNotificationIntentConfig(),
        runtime_config_loader=_raising_runtime_config,
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["database_read_attempted"] is False
    assert report["policy_invocation_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["event_outbox_emit_attempted"] is False
    assert report["side_effects"]["db_write"] is False
    assert report["side_effects"]["redis_mutation"] is False
    assert runtime_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_database_read_gate_blocks_before_db_session() -> None:
    runtime_builder = FakeRuntimeBuilder(FakeRepository(rows=[_row()]), FakePolicyInvoker())

    result = await run_bounded_policy_notification_intent(
        BoundedPolicyNotificationIntentConfig(operator_approved=True),
        runtime_config_loader=_raising_runtime_config,
        runtime_builder=runtime_builder,
    )

    assert result.error_code == "database_read_not_allowed"
    assert result.state.database_session_opened is False
    assert result.state.database_read_attempted is False
    assert result.state.policy_invocation_attempted is False
    assert runtime_builder.calls == 0


@pytest.mark.asyncio
async def test_two_stale_already_analyzed_rows_and_zero_eligible_blocks_before_policy_invocation() -> None:
    stale_judge_output_1 = uuid4()
    stale_judge_output_2 = uuid4()
    repository = FakeRepository(
        rows=[
            _row(payload_json=_payload(judge_output_id=str(stale_judge_output_1))),
            _row(payload_json=_payload(judge_output_id=str(stale_judge_output_2))),
        ],
        existing_analysis_judge_output_ids={stale_judge_output_1, stale_judge_output_2},
    )
    invoker = FakePolicyInvoker()
    runtime_builder = FakeRuntimeBuilder(repository, invoker)

    result = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )

    assert result.status == "blocked"
    assert result.error_code == "eligible_pending_count_mismatch"
    assert result.pending_policy_apply_count_observed == 2
    assert result.raw_pending_policy_apply_count_observed == 2
    assert result.eligible_pending_policy_apply_count_observed == 0
    assert result.stale_already_analyzed_policy_apply_count_observed == 2
    assert result.malformed_pending_policy_apply_count_observed == 0
    assert result.state.database_read_attempted is True
    assert result.state.policy_invocation_attempted is False
    assert repository.count_calls == 1
    assert repository.fetch_calls == 0
    assert invoker.calls == []
    assert runtime_builder.close_commits == [False]


@pytest.mark.asyncio
async def test_eligible_count_greater_than_expected_blocks_before_policy_invocation() -> None:
    repository = FakeRepository(rows=[_row(), _row()])
    invoker = FakePolicyInvoker()

    result = await run_bounded_policy_notification_intent(
        _approved_config(expected_eligible_pending_count=1),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(repository, invoker),
    )

    assert result.error_code == "eligible_pending_count_mismatch"
    assert result.pending_policy_apply_count_observed == 2
    assert result.raw_pending_policy_apply_count_observed == 2
    assert result.eligible_pending_policy_apply_count_observed == 2
    assert result.stale_already_analyzed_policy_apply_count_observed == 0
    assert result.state.policy_invocation_attempted is False
    assert repository.fetch_calls == 0
    assert invoker.calls == []


@pytest.mark.asyncio
async def test_two_stale_rows_plus_one_fresh_eligible_selects_only_fresh_event() -> None:
    operation_log: list[str] = []
    stale_judge_output_1 = uuid4()
    stale_judge_output_2 = uuid4()
    stale_row_1 = _row(
        payload_json=_payload(judge_output_id=str(stale_judge_output_1)),
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    stale_row_2 = _row(
        payload_json=_payload(judge_output_id=str(stale_judge_output_2)),
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    fresh_row = _row(created_at=datetime(2026, 1, 3, tzinfo=timezone.utc))
    repository = FakeRepository(
        rows=[stale_row_1, stale_row_2, fresh_row],
        existing_analysis_judge_output_ids={stale_judge_output_1, stale_judge_output_2},
        operation_log=operation_log,
    )
    invoker = FakePolicyInvoker(operation_log=operation_log)

    result = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(repository, invoker),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["pending_policy_apply_count_observed"] == 3
    assert report["raw_pending_policy_apply_count_observed"] == 3
    assert report["eligible_pending_policy_apply_count_observed"] == 1
    assert report["stale_already_analyzed_policy_apply_count_observed"] == 2
    assert report["malformed_pending_policy_apply_count_observed"] == 0
    assert report["selected_event_id_suffix"] == str(fresh_row.event_id)[-8:]
    assert invoker.calls == [fresh_row.event_id]
    assert operation_log == ["count", "fetch_eligible", "policy"]


@pytest.mark.asyncio
async def test_malformed_payload_blocks_before_policy_invocation_without_printing_payload() -> None:
    payload = _payload()
    raw_payload = str(payload.pop("bundle_id"))
    row = _row(payload_json=payload)
    repository = FakeRepository(rows=[row])
    invoker = FakePolicyInvoker()

    result = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(repository, invoker),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "malformed_policy_apply_payload"
    assert result.raw_pending_policy_apply_count_observed == 1
    assert result.eligible_pending_policy_apply_count_observed == 0
    assert result.malformed_pending_policy_apply_count_observed == 1
    assert result.selected_event_present is False
    assert result.state.policy_invocation_attempted is False
    assert repository.fetch_calls == 0
    assert invoker.calls == []
    assert raw_payload not in rendered
    assert RAW_PAYLOAD_VALUE not in rendered
    assert '"payload_json":' not in rendered


@pytest.mark.asyncio
async def test_missing_policy_write_gate_blocks_before_policy_invocation() -> None:
    repository = FakeRepository(rows=[_row()])
    invoker = FakePolicyInvoker()

    result = await run_bounded_policy_notification_intent(
        _approved_config(allow_policy_write=False),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(repository, invoker),
    )

    assert result.error_code == "policy_write_not_allowed"
    assert result.state.policy_invocation_attempted is False
    assert result.state.database_write_attempted is False
    assert result.state.event_outbox_emit_attempted is False
    assert invoker.calls == []


@pytest.mark.asyncio
async def test_successful_fake_non_suppress_invokes_policy_once_and_reports_intent() -> None:
    operation_log: list[str] = []
    row = _row()
    notification_event_id = uuid4()
    repository = FakeRepository(rows=[row], operation_log=operation_log)
    invoker = FakePolicyInvoker(
        summary=PolicyInvocationSummary(
            processed_event_count=1,
            analysis_created=True,
            state_transition_inserted=True,
            notification_plan_created_event_emitted=True,
            notification_plan_created_event_id=notification_event_id,
            delivery_decision="send_now",
            verdict="inspect_now",
        ),
        operation_log=operation_log,
    )
    runtime_builder = FakeRuntimeBuilder(repository, invoker)

    result = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["pending_policy_apply_count_observed"] == 1
    assert report["selected_event_present"] is True
    assert report["selected_event_status"] == "pending"
    assert report["selected_event_id_suffix"] == str(row.event_id)[-8:]
    assert report["selected_aggregate_type"] == "judge_run"
    assert report["selected_aggregate_id_suffix"] == str(row.aggregate_id)[-8:]
    assert report["payload_has_judge_run_id"] is True
    assert report["payload_has_judge_output_id"] is True
    assert report["payload_has_candidate_group_id"] is True
    assert report["payload_has_bundle_id"] is True
    assert report["policy_invocation_attempted"] is True
    assert report["processed_event_count"] == 1
    assert report["analysis_created"] is True
    assert report["state_transition_inserted"] is True
    assert report["notification_plan_created_event_emitted"] is True
    assert report["notification_plan_created_event_id_suffix"] == str(notification_event_id)[-8:]
    assert report["delivery_decision"] == "send_now"
    assert report["verdict"] == "inspect_now"
    assert report["side_effects"]["db_write"] is True
    assert report["side_effects"]["redis_mutation"] is False
    assert report["side_effects"]["notification_plan_table_write"] is False
    assert report["side_effects"]["notification_render_write"] is False
    assert report["side_effects"]["notification_delivery_record_write"] is False
    assert report["side_effects"]["telegram_send_called"] is False
    assert report["side_effects"]["telegram_edit_called"] is False
    assert report["raw_pending_policy_apply_count_observed"] == 1
    assert report["eligible_pending_policy_apply_count_observed"] == 1
    assert report["stale_already_analyzed_policy_apply_count_observed"] == 0
    assert report["malformed_pending_policy_apply_count_observed"] == 0
    assert operation_log == ["count", "fetch_eligible", "policy"]
    assert invoker.calls == [row.event_id]
    assert runtime_builder.close_commits == [True]


@pytest.mark.asyncio
async def test_successful_fake_suppress_invokes_policy_once_without_notification_event() -> None:
    row = _row()
    invoker = FakePolicyInvoker(
        summary=PolicyInvocationSummary(
            processed_event_count=1,
            analysis_created=True,
            state_transition_inserted=True,
            notification_plan_created_event_emitted=False,
            delivery_decision="suppress",
            verdict="skip",
        )
    )

    result = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(FakeRepository(rows=[row]), invoker),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["processed_event_count"] == 1
    assert report["notification_plan_created_event_emitted"] is False
    assert report["event_outbox_emit_attempted"] is False
    assert report["delivery_decision"] == "suppress"
    assert report["verdict"] == "skip"
    assert report["recommended_next_action"] == "no_notification_intent_emitted_by_policy"
    assert report["side_effects"]["telegram_send_called"] is False
    assert report["side_effects"]["telegram_edit_called"] is False
    assert invoker.calls == [row.event_id]


@pytest.mark.asyncio
async def test_policy_invocation_failure_is_sanitized_and_does_not_call_notifier() -> None:
    row = _row()
    invoker = FakePolicyInvoker(failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(FakeRepository(rows=[row]), invoker),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "policy_invocation_failed"
    assert result.state.policy_invocation_attempted is True
    assert result.state.database_write_attempted is False
    assert result.notification_plan_created_event_emitted is False
    assert result.processed_event_count == 0
    assert EXCEPTION_DETAIL not in rendered
    assert "RuntimeError" not in rendered
    assert result.to_sanitized_dict()["side_effects"]["telegram_send_called"] is False
    assert result.to_sanitized_dict()["side_effects"]["telegram_edit_called"] is False


@pytest.mark.asyncio
async def test_sanitized_output_omits_full_ids_payload_urls_tokens_rendered_text_and_exception_detail() -> None:
    row = _row()
    result = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(FakeRepository(rows=[row]), FakePolicyInvoker()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        DB_URL,
        REDIS_URL,
        TELEGRAM_TOKEN,
        RAW_PAYLOAD_VALUE,
        RENDERED_TEXT,
    ):
        assert raw not in rendered
    assert rendered.count(str(row.event_id)[-8:]) == 1
    assert rendered.count(str(row.aggregate_id)[-8:]) == 1

    failing = await run_bounded_policy_notification_intent(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(
            FakeRepository(rows=[_row()]),
            FakePolicyInvoker(failure=RuntimeError(EXCEPTION_DETAIL)),
        ),
    )
    assert EXCEPTION_DETAIL not in json.dumps(failing.to_sanitized_dict(), sort_keys=True)


def test_source_ast_guard_has_no_worker_or_external_client_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    forbidden_call_attrs = {
        "run_forever",
        "sleep",
        "system",
        "popen",
        "call",
        "check_call",
        "check_output",
    }

    assert not any(isinstance(node, ast.While) for node in ast.walk(tree))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in forbidden_call_attrs

    assert {
        "subprocess",
        "redis",
        "requests",
        "httpx",
        "aiohttp",
        "telegram",
        "openai",
    }.isdisjoint(imported_roots)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "PolicyEngineWorker" not in source
    assert "run_forever(" not in source
    assert "subprocess" not in source
    assert "systemctl" not in source
    assert "docker(" not in source
    assert "alembic(" not in source
