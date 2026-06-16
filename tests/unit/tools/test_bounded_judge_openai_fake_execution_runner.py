from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_judge_openai_fake_execution_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_judge_openai_fake_execution_runner.py"


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_judge_openai_fake_execution_v1"
    assert parsed["runner_name"] == "bounded_judge_openai_fake_execution_runner"
    assert parsed["mode"] == "fake_openai_exact_target_execute_and_publish"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_read_attempted"] is False
    assert parsed["redis_publish_attempted"] is False
    assert parsed["database_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["fake_openai_called"] is False
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
        "--allow-redis-publish",
        "--allow-database-read",
        "--allow-database-write",
        "--allow-fake-openai",
        "--redis-message-suffix",
        "--trigger-event-suffix",
        "--scan-limit",
    }


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    unsupported_flags = (
        "--allow-openai",
        "--allow-openai-live",
        "--openai-api-key",
        "--allow-redis-consume",
        "--allow-redis-ack",
        "--allow-telegram",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--allow-policy",
        "--allow-notifier",
        "--allow-analysis-validator",
        "--run-forever",
        "--database-url",
        "--redis-url",
        "--judge-run-id",
        "--bundle-id",
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
        assert parsed["redis_publish_attempted"] is False
        assert parsed["database_read_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["fake_openai_called"] is False
        assert parsed["openai_called"] is False


def test_invalid_suffix_returns_json_without_runtime_config(capsys) -> None:
    cases = (
        (["--operator-approved", "--redis-message-suffix", "bad:suffix"], "invalid_redis_message_suffix"),
        (["--operator-approved", "--trigger-event-suffix", "not-a-suffix"], "invalid_trigger_event_suffix"),
    )
    for argv, error_code in cases:
        exit_code = runner.main(argv)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["error_code"] == error_code
        assert parsed["redis_read_attempted"] is False
        assert parsed["database_read_attempted"] is False


def test_run_accepts_expected_selector_pair_before_runtime_gate() -> None:
    args = runner.build_parser().parse_args(
        [
            "--operator-approved",
            "--redis-message-suffix",
            "356724-0",
            "--trigger-event-suffix",
            "a1c22bcb",
            "--scan-limit",
            "25",
        ]
    )

    result = runner.run(args)

    assert result.exit_code == 1
    assert result.report["error_code"] == "runtime_config_not_allowed"
    assert result.report["target_redis_message_id_suffix"] == "356724-0"
    assert result.report["target_trigger_event_id_suffix"] == "a1c22bcb"


def test_required_authority_flags_gate_in_order_before_runtime_config() -> None:
    base = [
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-redis-read",
        "--allow-database-read",
        "--redis-message-suffix",
        "356724-0",
        "--trigger-event-suffix",
        "a1c22bcb",
        "--scan-limit",
        "25",
    ]
    cases = (
        (base, "database_write_not_allowed"),
        ([*base, "--allow-database-write"], "redis_publish_not_allowed"),
        ([*base, "--allow-database-write", "--allow-redis-publish"], "fake_openai_not_allowed"),
    )
    for argv, error_code in cases:
        result = runner.run(runner.build_parser().parse_args(argv))

        assert result.exit_code == 1
        assert result.report["error_code"] == error_code
        assert result.report["redis_read_attempted"] is False
        assert result.report["database_read_attempted"] is False
