from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID, uuid4

from src.services.notifier_telegram.bounded_invocation import (
    EXPECTED_EVENT_TYPE,
    BoundedNotifierRuntime,
    EventOutboxRecord,
    NotifierInvocationOutcome,
)
from src.services.notifier_telegram.config import NotifierTelegramConfig
from src.services.notifier_telegram.models import DeliveryResult
from tools import bounded_notifier_dry_run_invocation_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_notifier_dry_run_invocation_runner.py"
BOT_TOKEN = "123456:sentinel_cli_bot_token"
DB_URL = "postgresql+psycopg://user:sentinel_password@127.0.0.1/db"
RAW_RESPONSE = "sentinel raw response"
RAW_REQUEST = "sentinel raw request"
RENDERED_MESSAGE = "sentinel rendered message"


def _config() -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="prod",
        database_url=DB_URL,
        redis_url="redis://sentinel_redis",
        telegram_bot_token=BOT_TOKEN,
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="unit",
        batch_size=20,
        block_ms=5000,
        dry_run=False,
        allow_edits=True,
        enable_notification_send=True,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10,
        log_level="INFO",
    )


class FakeRuntimeBuilder:
    def __init__(self) -> None:
        self.calls = 0
        self.invoked: list[UUID] = []

    async def __call__(self, notifier_config, state, logger) -> BoundedNotifierRuntime:
        del logger
        self.calls += 1
        state.database_session_opened = True

        async def load_event(event_id: UUID):
            return EventOutboxRecord(event_id=event_id, event_type=EXPECTED_EVENT_TYPE)

        async def invoke(event_id: UUID):
            self.invoked.append(event_id)
            return NotifierInvocationOutcome(
                delivery_result=DeliveryResult(
                    delivery_status="suppressed",
                    telegram_chat_id=123,
                    telegram_message_id=None,
                    attempt_count=0,
                    transport_error_code="dry_run_skip_transport",
                    telegram_response_json={"raw_response": RAW_RESPONSE},
                ),
                notifier_owned_write_counts={"notification_delivery_records_insert_calls": 1},
            )

        async def close(commit: bool):
            del commit

        return BoundedNotifierRuntime(
            notifier_config=notifier_config,
            load_event_outbox=load_event,
            invoke_notifier=invoke,
            close=close,
        )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_bounded_invocation_module() -> None:
    assert runner.BoundedNotifierDryRunInvocationConfig.__module__.endswith(".bounded_invocation")


def test_main_with_no_flags_returns_sanitized_json_and_exit_one(capsys) -> None:
    exit_code = runner.main([])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["runner_name"] == "bounded_notifier_dry_run_invocation_runner"
    assert parsed["mode"] == "notifier_dry_run_send_disabled_one_shot"
    assert parsed["trigger_event_id_present"] is False
    assert parsed["operator_approved"] is False
    assert parsed["database_write_allowed"] is False
    assert parsed["send_enabled"] is False
    assert parsed["dry_run"] is True
    assert parsed["edits_allowed"] is False
    assert parsed["network_attempted"] is False
    assert parsed["transport_attempted"] is False
    assert parsed["processed_event_count"] == 0
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["event_outbox_read_attempted"] is False


def test_missing_trigger_event_id_returns_json_blocked() -> None:
    runtime_builder = FakeRuntimeBuilder()

    result = runner.run(
        _parse_args("--operator-approved", "--allow-database-write"),
        notifier_config_loader=lambda: _config(),
        runtime_builder=runtime_builder,
    )

    assert result.exit_code == 1
    assert result.report["error_code"] == "trigger_event_id_missing"
    assert runtime_builder.calls == 0


def test_missing_database_write_flag_blocks_before_config_or_runtime() -> None:
    runtime_builder = FakeRuntimeBuilder()

    result = runner.run(
        _parse_args("--operator-approved", "--trigger-event-id", str(uuid4())),
        notifier_config_loader=lambda: (_ for _ in ()).throw(AssertionError("loader must not run")),
        runtime_builder=runtime_builder,
    )

    assert result.exit_code == 1
    assert result.report["error_code"] == "database_write_not_allowed"
    assert result.report["side_effects"]["database_session_opened"] is False
    assert runtime_builder.calls == 0


def test_valid_invocation_prints_sanitized_json_without_raw_values(capsys) -> None:
    trigger_event_id = uuid4()
    runtime_builder = FakeRuntimeBuilder()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-database-write",
            "--trigger-event-id",
            str(trigger_event_id),
        ],
        notifier_config_loader=lambda: _config(),
        runtime_builder=runtime_builder,
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["processed_event_count"] == 1
    assert parsed["notifier_owned_write_counts"]["notification_delivery_records_insert_calls"] == 1
    assert runtime_builder.invoked == [trigger_event_id]
    for raw in (BOT_TOKEN, DB_URL, RAW_RESPONSE, RAW_REQUEST, RENDERED_MESSAGE):
        assert raw not in output


def test_live_send_network_and_edit_flags_are_rejected_as_json(capsys) -> None:
    for flag in ("--allow-send", "--allow-network", "--allow-edits", "--edit", "--telegram-bot-token"):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["network_attempted"] is False
        assert parsed["transport_attempted"] is False


def test_tool_source_imports_no_openai_github_x_web_or_telegram_clients() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert {"redis", "sqlalchemy", "openai", "requests", "httpx", "aiohttp", "telegram"}.isdisjoint(imported_roots)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert parser_flags == {"--trigger-event-id", "--operator-approved", "--allow-database-write"}
    assert "allow-send" not in source
    assert "allow-network" not in source
    assert "allow-edits" not in source
    assert "TELEGRAM_BOT_TOKEN" not in source
    assert "DATABASE_URL" not in source
    assert "REDIS_URL" not in source
    assert "raw_response" not in source
    assert "traceback" not in source.lower()
    assert "print(" not in source
    assert "render_sanitized_json(result.report)" in source
