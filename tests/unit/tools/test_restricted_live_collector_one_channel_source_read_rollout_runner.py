from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.collector_telegram.restricted_source_read_rollout import (
    FAKE_CHAT_ID,
    FAKE_CONFIG_VALUE,
    FAKE_MESSAGE_ID,
    FAKE_MESSAGE_TEXT,
)
from tools import restricted_live_collector_one_channel_source_read_rollout_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/restricted_live_collector_one_channel_source_read_rollout_runner.py"


def test_parser_exposes_only_fake_backed_source_read_proof_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert parser_flags == {"--source-value", "--max-messages"}


def test_runner_prints_operator_packet_without_live_authority_or_raw_values(capsys) -> None:
    exit_code = runner.main(["--source-value", "@trendingrepo", "--max-messages", "1"])
    captured = capsys.readouterr()
    report = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert report["schema_version"] == "restricted_live_collector_one_channel_source_read_rollout_v1"
    assert report["status"] == "pass"
    assert report["authority"]["live_telegram_read_attempted"] is False
    assert report["runtime_authority_opened_in_this_run"]["live_telegram_read"] is False
    assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is True
    assert report["readback"]["duplicate_guard_preserved"] is True
    for raw in (
        "trendingrepo",
        str(FAKE_CHAT_ID),
        str(FAKE_MESSAGE_ID),
        FAKE_MESSAGE_TEXT,
        FAKE_CONFIG_VALUE,
        "not-used-by-fake-proof",
    ):
        assert raw not in captured.out


def test_runner_rejects_unsupported_live_authority_flags_as_json(capsys) -> None:
    for flag in (
        "--allow-live-telegram-read",
        "--allow-telegram-read",
        "--allow-send",
        "--runtime-env-path",
        "--all-channels",
        "--chat-id",
        "--registry-id",
    ):
        exit_code = runner.main([flag])
        report = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert report["status"] == "blocked"
        assert report["reason_code"] == "unsupported_cli_argument"
        assert report["authority"]["live_telegram_read_attempted"] is False
        assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False
        assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is False
