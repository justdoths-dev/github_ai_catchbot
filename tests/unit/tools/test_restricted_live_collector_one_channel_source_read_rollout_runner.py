from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.collector_telegram.restricted_source_read_rollout import (
    BOUNDED_RUNNER_PATH,
    COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS,
    FAKE_CHAT_ID,
    FAKE_CONFIG_VALUE,
    FAKE_MESSAGE_ID,
    FAKE_MESSAGE_TEXT,
    SOURCE_VALUE_PLACEHOLDER,
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

    assert parser_flags == {"--source-value", "--max-messages", "--emit-live-preflight-command"}


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


def test_runner_prints_preflight_command_packet_without_live_authority_or_raw_values(capsys) -> None:
    exit_code = runner.main(
        [
            "--source-value",
            "@trendingrepo",
            "--max-messages",
            "1",
            "--emit-live-preflight-command",
        ]
    )
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    command = report["future_execution_command"]
    command_tokens = command["command_tokens"]
    runtime_env = command["runtime_env"]
    safe_loader = runtime_env["safe_loader_pattern"]

    assert exit_code == 0
    assert captured.err == ""
    assert report["schema_version"] == "restricted_live_collector_one_channel_source_read_preflight_v1"
    assert report["status"] == "pass"
    assert report["authority"]["live_telegram_read_attempted"] is False
    assert report["runtime_authority_opened_in_this_run"]["live_telegram_read"] is False
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False
    assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is False
    assert command["runner_path"] == BOUNDED_RUNNER_PATH
    assert command["max_messages_argument"] == "--max-messages"
    assert command["source_outbox_publish_disabled"] is True
    assert command["redis_publish_disabled"] is True
    assert command["send_disabled"] is True
    assert command_tokens[:2] == ["venv/bin/python", BOUNDED_RUNNER_PATH]
    assert SOURCE_VALUE_PLACEHOLDER in command_tokens
    assert "--max-messages" in command_tokens
    assert runtime_env["runtime_env_file_placeholder"] == "<RUNTIME_ENV_FILE>"
    assert runtime_env["runtime_env_loaded"] is False
    assert runtime_env["actual_runtime_env_file_read_in_this_task"] is False
    assert runtime_env["runtime_env_values_redacted"] is True
    assert runtime_env["safe_loader_pattern_available"] is True
    assert safe_loader["runtime_env_file_placeholder"] == "<RUNTIME_ENV_FILE>"
    assert safe_loader["runtime_env_loaded"] is False
    assert safe_loader["actual_runtime_env_file_read_in_this_task"] is False
    assert safe_loader["allowed_env_keys"] == list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
    assert safe_loader["reject_unknown_env_keys"] is True
    assert safe_loader["load_values_into_child_env_overlay_only"] is True
    assert safe_loader["uses_sys_executable_for_child"] is True
    assert safe_loader["child_command_uses_existing_runner"] is True
    assert safe_loader["child_command_runner_path"] == BOUNDED_RUNNER_PATH
    assert safe_loader["child_command_tokens"] == command_tokens
    assert safe_loader["child_command_omits_runtime_env_file_token"] is True
    assert safe_loader["child_command_omits_source_outbox_publish"] is True
    assert safe_loader["child_command_omits_redis_publish"] is True
    assert safe_loader["child_command_omits_send_edit"] is True
    assert safe_loader["child_command_omits_chat_id"] is True
    assert safe_loader["child_command_omits_registry_id"] is True
    assert safe_loader["child_command_omits_docker_systemd_alembic"] is True
    assert "--allow-source-outbox-publish" not in command_tokens
    assert "--allow-redis-publish" not in command_tokens
    assert "--allow-send" not in command_tokens
    assert "--chat-id" not in command_tokens
    assert "--registry-id" not in command_tokens
    assert "--env-file" not in command_tokens
    assert report["completion_claims"]["F1_LIVE_ONE_CHANNEL_SOURCE_READ_PREFLIGHT_PACKET_READY"] is True
    assert report["completion_claims"]["F1_LIVE_ONE_CHANNEL_EXACT_COMMAND_PACKET_READY"] is True
    assert report["completion_claims"]["LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK"] is True
    for raw in (
        "trendingrepo",
        str(FAKE_CHAT_ID),
        str(FAKE_MESSAGE_ID),
        FAKE_MESSAGE_TEXT,
        FAKE_CONFIG_VALUE,
        "not-used-by-fake-proof",
        "runtime.env",
        "SENTINEL_DATABASE_URL_VALUE",
        "SENTINEL_REDIS_URL_VALUE",
        "SENTINEL_TELEGRAM_API_HASH_VALUE",
        "SENTINEL_TELEGRAM_PHONE_NUMBER_VALUE",
        "SENTINEL_TDLIB_STATE_PATH_VALUE",
        "Traceback",
        "private stderr",
    ):
        assert raw not in captured.out


def test_runner_rejects_unsupported_live_authority_flags_as_json(capsys) -> None:
    for flag in (
        "--allow-live-telegram-read",
        "--allow-telegram-read",
        "--allow-send",
        "--runtime-env-path",
        "--env-file",
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
