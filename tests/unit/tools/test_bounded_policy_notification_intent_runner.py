from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.services.policy_engine import bounded_notification_intent
from src.services.policy_engine.bounded_notification_intent import (
    BoundedPolicyNotificationIntentRuntimeHandle,
    PolicyApplyEventRow,
    PolicyEngineConfig,
    PolicyInvocationSummary,
)
from tools import bounded_policy_notification_intent_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_policy_notification_intent_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel_policy_cli_redis_url"
RAW_PAYLOAD_VALUE = "sentinel cli policy raw payload"
TELEGRAM_TOKEN = "123456:sentinel_cli_telegram_token"
RENDERED_TEXT = "sentinel cli rendered message text"


class FakeRepository:
    def __init__(self, row: PolicyApplyEventRow) -> None:
        self.row = row

    async def count_pending_policy_apply_events(self) -> int:
        return 1

    async def fetch_oldest_pending_policy_apply_event(self) -> PolicyApplyEventRow | None:
        return self.row


class FakePolicyInvoker:
    def __init__(self) -> None:
        self.calls = []

    async def __call__(self, trigger_event_id):
        self.calls.append(trigger_event_id)
        return PolicyInvocationSummary(
            processed_event_count=1,
            analysis_created=True,
            state_transition_inserted=True,
            notification_plan_created_event_emitted=True,
            notification_plan_created_event_id=uuid4(),
            delivery_decision="send_now",
            verdict="inspect_now",
        )


class FakeRuntimeBuilder:
    def __init__(self, repository: FakeRepository, invoker: FakePolicyInvoker) -> None:
        self.repository = repository
        self.invoker = invoker

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            del commit

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
        consumer_name="bounded-policy-notification-intent-cli-test",
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


def _row() -> PolicyApplyEventRow:
    return PolicyApplyEventRow(
        event_id=uuid4(),
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=uuid4(),
        payload_json={
            "judge_run_id": str(uuid4()),
            "judge_output_id": str(uuid4()),
            "candidate_group_id": str(uuid4()),
            "bundle_id": str(uuid4()),
            "raw_payload": RAW_PAYLOAD_VALUE,
            "telegram_bot_token": TELEGRAM_TOKEN,
            "rendered_message_text": RENDERED_TEXT,
        },
        status="pending",
        created_at=datetime.now(timezone.utc),
    )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_module_and_config_types() -> None:
    assert runner.BoundedPolicyNotificationIntentConfig is (
        bounded_notification_intent.BoundedPolicyNotificationIntentConfig
    )
    assert runner.BoundedPolicyNotificationIntentResult is (
        bounded_notification_intent.BoundedPolicyNotificationIntentResult
    )


def test_main_with_no_flags_returns_required_fail_closed_json(capsys) -> None:
    exit_code = runner.main([])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["schema_version"] == "bounded_policy_notification_intent_v1"
    assert parsed["runner_name"] == "bounded_policy_notification_intent_runner"
    assert parsed["mode"] == "policy_apply_one_shot_notification_intent"
    assert parsed["operator_approved"] is False
    assert parsed["database_read_allowed"] is False
    assert parsed["policy_write_allowed"] is False
    assert parsed["database_read_attempted"] is False
    assert parsed["policy_invocation_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["event_outbox_emit_attempted"] is False
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["db_write"] is False
    assert parsed["side_effects"]["redis_mutation"] is False
    assert parsed["side_effects"]["telegram_send_called"] is False
    assert parsed["side_effects"]["run_forever_called"] is False


def test_parser_exposes_only_approved_bounded_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert parser_flags == {
        "--operator-approved",
        "--allow-database-read",
        "--allow-policy-write",
        "--expected-pending-count",
    }


def test_unsupported_live_send_network_identity_and_secret_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-send",
        "--allow-network",
        "--telegram-bot-token",
        "--database-url",
        "--event-id",
        "--trigger-event-id",
    ):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["database_read_attempted"] is False
        assert parsed["policy_invocation_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["event_outbox_emit_attempted"] is False


def test_valid_cli_run_prints_sanitized_json_and_delegates_to_source(capsys) -> None:
    row = _row()
    repository = FakeRepository(row)
    invoker = FakePolicyInvoker()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-database-read",
            "--allow-policy-write",
            "--expected-pending-count",
            "1",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(repository, invoker),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["processed_event_count"] == 1
    assert parsed["analysis_created"] is True
    assert parsed["state_transition_inserted"] is True
    assert parsed["notification_plan_created_event_emitted"] is True
    assert parsed["delivery_decision"] == "send_now"
    assert parsed["verdict"] == "inspect_now"
    assert invoker.calls == [row.event_id]
    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        RAW_PAYLOAD_VALUE,
        TELEGRAM_TOKEN,
        RENDERED_TEXT,
        DB_URL,
        REDIS_URL,
    ):
        assert raw not in output


def test_run_with_explicit_args_uses_expected_pending_count() -> None:
    row = _row()
    repository = FakeRepository(row)
    invoker = FakePolicyInvoker()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-database-read",
            "--allow-policy-write",
            "--expected-pending-count",
            "1",
        ),
        runtime_config_loader=_runtime_config,
        runtime_builder=FakeRuntimeBuilder(repository, invoker),
    )

    assert result.exit_code == 0
    assert result.report["pending_policy_apply_count_observed"] == 1
    assert result.report["processed_event_count"] == 1
    assert invoker.calls == [row.event_id]


def test_tool_source_imports_no_db_redis_or_external_clients_and_has_no_business_logic() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    call_attrs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            call_attrs.add(node.func.attr)

    assert {"sqlalchemy", "redis", "openai", "requests", "httpx", "aiohttp", "telegram"}.isdisjoint(
        imported_roots
    )
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "print(" not in source
    assert "run_bounded_policy_notification_intent_sync" in source
    assert "payload_json" not in source
