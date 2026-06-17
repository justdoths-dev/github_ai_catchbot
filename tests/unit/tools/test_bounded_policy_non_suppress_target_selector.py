from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_policy_non_suppress_target_selector as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_policy_non_suppress_target_selector.py"


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_policy_non_suppress_target_selector_v1"
    assert parsed["runner_name"] == "bounded_policy_non_suppress_target_selector"
    assert parsed["mode"] == "policy_non_suppress_exact_target_selection"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_read_attempted"] is False
    assert parsed["redis_publish_attempted"] is False
    assert parsed["redis_ack_called"] is False
    assert parsed["redis_consume_called"] is False
    assert parsed["database_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["policy_preview_called"] is False
    assert parsed["policy_engine_called"] is False
    assert parsed["notifier_called"] is False
    assert parsed["telegram_send_called"] is False
    assert parsed["openai_called"] is False


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
        "--allow-redis-read",
        "--allow-database-read",
        "--allow-policy-preview",
        "--scan-limit",
        "--max-results",
        "--prefer-verdict",
    }


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    unsupported_flags = (
        "--allow-database-write",
        "--allow-redis-publish",
        "--allow-redis-consume",
        "--allow-redis-ack",
        "--allow-policy-engine",
        "--allow-notifier",
        "--allow-telegram",
        "--allow-openai",
        "--allow-fake-openai",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--run-forever",
        "--database-url",
        "--redis-url",
        "--runtime-env",
        "--analysis-id",
        "--notification-plan-id",
    )
    for flag in unsupported_flags:
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["redis_read_attempted"] is False
        assert parsed["database_read_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["policy_preview_called"] is False
        assert parsed["policy_engine_called"] is False
        assert parsed["notifier_called"] is False
        assert parsed["telegram_send_called"] is False
        assert parsed["openai_called"] is False


def test_required_authority_flags_gate_in_order_before_runtime_config() -> None:
    cases = (
        (["--operator-approved"], "runtime_config_not_allowed"),
        (
            [
                "--operator-approved",
                "--allow-runtime-config",
            ],
            "redis_read_not_allowed",
        ),
        (
            [
                "--operator-approved",
                "--allow-runtime-config",
                "--allow-redis-read",
            ],
            "database_read_not_allowed",
        ),
        (
            [
                "--operator-approved",
                "--allow-runtime-config",
                "--allow-redis-read",
                "--allow-database-read",
            ],
            "policy_preview_not_allowed",
        ),
    )
    for argv, error_code in cases:
        result = runner.run(runner.build_parser().parse_args(argv))

        assert result.exit_code == 1
        assert result.report["status"] == "blocked"
        assert result.report["error_code"] == error_code
        assert result.report["redis_read_attempted"] is False
        assert result.report["database_read_attempted"] is False
        assert result.report["database_write_attempted"] is False
        assert result.report["policy_preview_called"] is False


def test_invalid_limits_and_prefer_verdict_return_json_without_runtime_config() -> None:
    cases = (
        (["--operator-approved", "--scan-limit", "0"], "invalid_scan_limit"),
        (["--operator-approved", "--scan-limit", "501"], "invalid_scan_limit"),
        (["--operator-approved", "--max-results", "0"], "invalid_max_results"),
        (["--operator-approved", "--max-results", "26"], "invalid_max_results"),
        (["--operator-approved", "--prefer-verdict", "skip"], "invalid_prefer_verdict"),
    )
    for argv, error_code in cases:
        result = runner.run(runner.build_parser().parse_args(argv))

        assert result.exit_code == 1
        assert result.report["status"] == "blocked"
        assert result.report["error_code"] == error_code
        assert result.report["redis_read_attempted"] is False
        assert result.report["database_read_attempted"] is False
        assert result.report["database_write_attempted"] is False
