from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

from src.services.policy_engine import bounded_notification_intent_recovery
from tools import bounded_notification_intent_recovery as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_notification_intent_recovery.py"


def test_runner_uses_source_level_module_types() -> None:
    assert runner.BoundedNotificationIntentRecoveryConfig is (
        bounded_notification_intent_recovery.BoundedNotificationIntentRecoveryConfig
    )
    assert runner.BoundedNotificationIntentRecoveryResult is (
        bounded_notification_intent_recovery.BoundedNotificationIntentRecoveryResult
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_notification_intent_recovery_v1"
    assert parsed["runner_name"] == "bounded_notification_intent_recovery"
    assert parsed["mode"] == "preview"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["database_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_read_attempted"] is False
    assert parsed["redis_publish_attempted"] is False
    assert parsed["redis_ack_called"] is False
    assert parsed["redis_consume_called"] is False
    assert parsed["notifier_called"] is False
    assert parsed["telegram_send_called"] is False
    assert parsed["openai_called"] is False
    assert parsed["github_api_called"] is False
    assert parsed["x_api_called"] is False
    assert parsed["web_fetch_called"] is False


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
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-policy-preview",
        "--allow-database-write",
        "--allow-notification-intent-write",
        "--require-notification-send-enabled",
        "--allow-redis-read",
        "--allow-redis-publish",
        "--allow-notification-send-queue-publish",
        "--policy-apply-event-suffix",
        "--judge-run-suffix",
        "--judge-output-suffix",
        "--bundle-suffix",
        "--candidate-group-suffix",
        "--analysis-suffix",
    }


def test_unsupported_authority_and_secret_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-notifier",
        "--allow-telegram",
        "--allow-openai",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--database-url",
        "--redis-url",
        "--runtime-env",
        "--analysis-id",
        "--event-id",
        "--run-forever",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["database_read_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["redis_publish_attempted"] is False
        assert parsed["notifier_called"] is False
        assert parsed["telegram_send_called"] is False


def test_gate_order_blocks_before_runtime_config() -> None:
    cases = (
        (["--operator-approved"], "runtime_config_not_allowed"),
        (["--operator-approved", "--allow-runtime-config"], "database_read_not_allowed"),
        (
            ["--operator-approved", "--allow-runtime-config", "--allow-database-read"],
            "policy_preview_not_allowed",
        ),
        (
            [
                "--operator-approved",
                "--allow-runtime-config",
                "--allow-database-read",
                "--allow-policy-preview",
            ],
            "suffix_ambiguous_or_missing",
        ),
    )
    for argv, error_code in cases:
        result = runner.run(runner.build_parser().parse_args(argv), runtime_config_loader=_raising_runtime_config)

        assert result.exit_code == 1
        assert result.report["error_code"] == error_code
        assert result.report["database_read_attempted"] is False
        assert result.report["database_write_attempted"] is False
        assert result.report["redis_publish_attempted"] is False


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
    assert "run_bounded_notification_intent_recovery_sync" in source
    assert "payload_json" not in source


def _raising_runtime_config() -> Any:
    raise AssertionError("runtime config must not be loaded")
