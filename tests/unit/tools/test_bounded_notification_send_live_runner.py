from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_notification_send_live_runner as runner
from tests.unit.services.notifier_telegram.test_bounded_notification_send_live_runner import (
    FakeRuntime,
    FakeRuntimeBuilder,
    _context,
    _intent,
    _runtime_config_loader,
)


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_notification_send_live_runner.py"


def test_main_with_no_flags_returns_sanitized_blocked_json(capsys) -> None:
    exit_code = runner.main([])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["schema_version"] == "bounded_notification_send_live_runner_v1"
    assert parsed["runner_name"] == "bounded_notification_send_live_runner"
    assert parsed["mode"] == "preview"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["redis_read_attempted"] is False
    assert parsed["side_effects"]["database_session_opened"] is False
    assert parsed["side_effects"]["telegram_transport_constructed"] is False


def test_valid_execute_delegates_once_and_redacts_output(capsys) -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent))

    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-database-write",
            "--allow-redis-read",
            "--allow-redis-consume",
            "--allow-redis-ack",
            "--allow-maintenance-publish",
            "--allow-render-write",
            "--allow-delivery-record-write",
            "--allow-delivery-result-outbox-write",
            "--allow-telegram-transport",
            "--allow-telegram-send",
            "--trigger-event-suffix",
            str(intent.trigger_event_id)[-8:],
            "--notification-plan-id-suffix",
            str(intent.notification_plan_id)[-8:],
            "--analysis-id-suffix",
            str(intent.analysis_id)[-8:],
            "--target-chat-id-suffix",
            str(intent.target_chat_id)[-4:],
            "--redis-message-suffix",
            "000000-0",
        ],
        runtime_config_loader=_runtime_config_loader(send_enabled=True),
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["status"] == "pass"
    assert parsed["delivery_status"] == "sent"
    assert runtime.call_order == [
        "inspect",
        "load_context",
        "consume",
        "execute",
        "publish_maintenance",
        "mark_published",
        "readback",
        "ack",
        "close",
    ]
    for raw in (
        str(intent.trigger_event_id),
        str(intent.notification_plan_id),
        str(intent.analysis_id),
        str(intent.target_chat_id),
        "Rendered operator text",
        "red" + "is://not-in-report",
        "1718000000001-0",
    ):
        assert raw not in output


def test_parser_rejects_unknown_live_or_runtime_shortcuts_as_json(capsys) -> None:
    for flag in ("--allow-network", "--telegram-bot-token", "--env-file", "--run-forever"):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["side_effects"]["redis_read_attempted"] is False
        assert parsed["side_effects"]["telegram_transport_constructed"] is False


def test_tool_source_static_authority_and_flag_surface() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots: set[str] = set()
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

    assert {"redis", "sqlalchemy", "openai", "requests", "httpx", "aiohttp", "telegram", "subprocess"}.isdisjoint(
        imported_roots
    )
    assert parser_flags == {
        "--mode",
        "--trigger-event-suffix",
        "--notification-plan-id-suffix",
        "--analysis-id-suffix",
        "--target-chat-id-suffix",
        "--redis-message-suffix",
        "--scan-limit",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-database-write",
        "--allow-redis-read",
        "--allow-redis-consume",
        "--allow-redis-ack",
        "--allow-maintenance-publish",
        "--allow-render-write",
        "--allow-delivery-record-write",
        "--allow-delivery-result-outbox-write",
        "--allow-telegram-transport",
        "--allow-telegram-send",
    }
    lowered = source.lower()
    for forbidden in ("runtime.env", "run_forever", "xclaim", "xautoclaim", "xgroup_create", "systemctl"):
        assert forbidden not in lowered
    assert "print(" not in source
    assert "render_sanitized_json(result.report)" in source
