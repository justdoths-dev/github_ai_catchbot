from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_notifier_idempotent_noop_worker_once as runner
from tests.unit.services.notifier_telegram.test_idempotent_noop_worker_once import (
    FakeRuntime,
    FakeRuntimeBuilder,
    _intent,
    _runtime_config_loader,
)


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_notifier_idempotent_noop_worker_once.py"
RAW_DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
RAW_REDIS_URL = "redis://:sentinel_password@127.0.0.1/0"
RAW_TOKEN = "sentinel_telegram_token"


def _preview_argv(intent) -> list[str]:
    return [
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-redis-read",
        "--require-telegram-disabled",
        "--mode",
        "preview",
        "--queue-name",
        "q.notification.send",
        "--trigger-event-suffix",
        str(intent.trigger_event_id)[-8:],
        "--analysis-suffix",
        str(intent.analysis_id)[-8:],
    ]


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_notifier_idempotent_noop_worker_once_v1"
    assert parsed["runner_name"] == "bounded_notifier_idempotent_noop_worker_once"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["runtime_config_loaded"] is False
    assert parsed["database_read_attempted"] is False
    assert parsed["redis_read_attempted"] is False
    assert parsed["redis_consume_called"] is False
    assert parsed["redis_ack_attempted"] is False
    assert parsed["telegram_send_called"] is False
    assert parsed["telegram_edit_called"] is False


def test_preview_cli_delegates_once_and_omits_raw_ids_or_secrets(capsys) -> None:
    intent = _intent()
    runtime = FakeRuntime(intent=intent)
    exit_code = runner.main(
        _preview_argv(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["status"] == "pass"
    assert parsed["ack_safe_candidate"] is True
    assert parsed["ack_attempted"] is False
    assert parsed["acked"] is False
    assert runtime.inspect_calls == 1
    assert runtime.invoke_calls == []
    assert runtime.acked == []
    for raw in (str(intent.trigger_event_id), str(intent.analysis_id), RAW_DB_URL, RAW_REDIS_URL, RAW_TOKEN):
        assert raw not in output


def test_unsupported_live_or_raw_authority_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-send",
        "--allow-telegram",
        "--allow-network",
        "--allow-edits",
        "--database-url",
        "--redis-url",
        "--telegram-bot-token",
        "--runtime-env",
        "--trigger-event-id",
        "--analysis-id",
    ):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["runtime_config_loaded"] is False
        assert parsed["redis_consume_called"] is False
        assert parsed["telegram_send_called"] is False


def test_tool_source_imports_no_db_redis_external_clients_or_live_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert {"sqlalchemy", "redis", "openai", "requests", "httpx", "aiohttp", "telegram", "subprocess"}.isdisjoint(
        imported_roots
    )
    assert parser_flags == {
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-database-write-for-notifier-noop-only",
        "--allow-redis-read",
        "--allow-redis-consume",
        "--allow-redis-ack",
        "--require-telegram-disabled",
        "--mode",
        "--queue-name",
        "--redis-message-id-suffix",
        "--trigger-event-suffix",
        "--analysis-suffix",
    }
    for forbidden in ("allow-send", "allow-network", "allow-edits", "TELEGRAM_BOT_TOKEN", "DATABASE_URL", "REDIS_URL"):
        assert forbidden not in source
    assert "print(" not in source
    assert "render_sanitized_json(result.report)" in source
