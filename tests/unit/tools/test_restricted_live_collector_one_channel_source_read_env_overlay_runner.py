from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from src.services.collector_telegram.runtime_env_overlay import COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS
from tools import restricted_live_collector_one_channel_source_read_env_overlay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/restricted_live_collector_one_channel_source_read_env_overlay_runner.py"

SENTINEL_VALUES = (
    "SENTINEL_DATABASE_URL_VALUE",
    "SENTINEL_REDIS_URL_VALUE",
    "SENTINEL_TELEGRAM_API_HASH_VALUE",
    "SENTINEL_TELEGRAM_PHONE_NUMBER_VALUE",
    "SENTINEL_TDLIB_STATE_PATH_VALUE",
    "SENTINEL_TDLIB_ENCRYPTION_VALUE",
    "SENTINEL_OPENAI_KEY_FILE_VALUE",
    "SENTINEL_X_BEARER_TOKEN_VALUE",
    "SENTINEL_TELEGRAM_BOT_TOKEN_VALUE",
    "SENTINEL_UNKNOWN_VALUE",
)


def _fixture_env(tmp_path: Path) -> Path:
    path = tmp_path / "fixture-runtime.env"
    path.write_text(
        "\n".join(
            (
                "APP_ENV=prod",
                "COLLECTOR_MODE=live",
                "DATABASE_URL=SENTINEL_DATABASE_URL_VALUE",
                "REDIS_URL=SENTINEL_REDIS_URL_VALUE",
                "TELEGRAM_API_ID=12345",
                "TELEGRAM_API_HASH=SENTINEL_TELEGRAM_API_HASH_VALUE",
                "TELEGRAM_PHONE_NUMBER=SENTINEL_TELEGRAM_PHONE_NUMBER_VALUE",
                "TDLIB_STATE_DIR=SENTINEL_TDLIB_STATE_PATH_VALUE",
                "TDLIB_DB_ENCRYPTION_KEY=SENTINEL_TDLIB_ENCRYPTION_VALUE",
                "OPENAI_API_KEY_FILE=SENTINEL_OPENAI_KEY_FILE_VALUE",
                "TELEGRAM_BOT_TOKEN=SENTINEL_TELEGRAM_BOT_TOKEN_VALUE",
                "X_BEARER_TOKEN=SENTINEL_X_BEARER_TOKEN_VALUE",
                "UNKNOWN_EXTRA=SENTINEL_UNKNOWN_VALUE",
                "",
            )
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def test_parser_exposes_only_env_overlay_preflight_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert parser_flags == {
        "--mode",
        "--runtime-env-file",
        "--source-value",
        "--max-messages",
        "--operator-approved",
        "--confirm-token",
    }


def test_plan_mode_emits_sanitized_json_and_does_not_invoke_child_runner(tmp_path: Path, capsys) -> None:
    env_file = _fixture_env(tmp_path)

    def forbidden_runner(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("plan mode must not invoke child runner")

    exit_code = runner.main(
        [
            "--runtime-env-file",
            str(env_file),
            "--source-value",
            "@trendingrepo",
            "--max-messages",
            "1",
        ],
        subprocess_runner=forbidden_runner,
    )
    captured = capsys.readouterr()
    report = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert report["status"] == "pass"
    assert report["reason_code"] == "collector_runtime_env_overlay_plan_ready"
    assert report["actual_attempted_operations"]["runtime_env_read_attempted"] is True
    assert report["actual_attempted_operations"]["child_runner_invoked"] is False
    assert report["runtime_env_overlay"]["status"] == "pass"
    assert report["runtime_env_overlay"]["source_runtime_env_allows_extra_keys"] is True
    assert report["runtime_env_overlay"]["source_unknown_keys_ignored"] is True
    assert report["runtime_env_overlay"]["source_forbidden_keys_ignored"] is True
    assert report["runtime_env_overlay"]["child_overlay_only"] is True
    assert report["child_command"]["command_tokens"][0] == "sys.executable"
    assert runner.SOURCE_VALUE_PLACEHOLDER in report["child_command"]["command_tokens"]
    assert "trendingrepo" not in captured.out
    for value in SENTINEL_VALUES:
        assert value not in captured.out


def test_execute_without_approval_blocks_before_runtime_env_read_or_child_invoke(tmp_path: Path, capsys) -> None:
    calls: list[Any] = []

    def fake_runner(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        raise AssertionError("execute without approval must not invoke child runner")

    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--runtime-env-file",
            str(tmp_path / "missing.env"),
            "--source-value",
            "@trendingrepo",
            "--max-messages",
            "1",
            "--confirm-token",
            "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE",
        ],
        subprocess_runner=fake_runner,
    )
    report = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert calls == []
    assert report["reason_code"] == "operator_approval_missing"
    assert report["actual_attempted_operations"]["runtime_env_read_attempted"] is False
    assert report["actual_attempted_operations"]["child_runner_invoked"] is False


def test_execute_with_wrong_confirm_blocks_before_runtime_env_read_or_child_invoke(tmp_path: Path, capsys) -> None:
    calls: list[Any] = []

    def fake_runner(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        raise AssertionError("execute with wrong token must not invoke child runner")

    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--runtime-env-file",
            str(tmp_path / "missing.env"),
            "--source-value",
            "@trendingrepo",
            "--max-messages",
            "1",
            "--operator-approved",
            "--confirm-token",
            "wrong-token",
        ],
        subprocess_runner=fake_runner,
    )
    report = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert calls == []
    assert report["reason_code"] == "confirm_token_invalid"
    assert report["actual_attempted_operations"]["runtime_env_read_attempted"] is False
    assert report["actual_attempted_operations"]["child_runner_invoked"] is False


def test_execute_with_valid_fixture_invokes_child_with_collector_only_env(tmp_path: Path, capsys) -> None:
    env_file = _fixture_env(tmp_path)
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_runner(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout='{"status":"pass","reason_code":"child_ok"}\n',
            stderr="SENTINEL_PRIVATE_STDERR_SHOULD_NOT_PRINT",
        )

    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--runtime-env-file",
            str(env_file),
            "--source-value",
            "@trendingrepo",
            "--max-messages",
            "1",
            "--operator-approved",
            "--confirm-token",
            "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE",
        ],
        subprocess_runner=fake_runner,
    )
    captured = capsys.readouterr()
    report = json.loads(captured.out)

    assert exit_code == 0
    assert len(calls) == 1
    command, kwargs = calls[0]
    child_env = kwargs["env"]
    assert command[0] == sys.executable
    assert command[1] == runner.CHILD_RUNNER_PATH
    for token in (
        "--mode",
        "execute",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-telegram-read",
        "--allow-database-write",
        "--allow-source-message-write",
        "--allow-source-version-write",
        "--allow-source-outbox-write",
        "--source-kind",
        "public_username",
        "--source-value",
        "trendingrepo",
        "--max-messages",
        "1",
        "--confirm-token",
        "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE",
    ):
        assert token in command
    for forbidden in (
        "--allow-source-outbox-publish",
        "--allow-redis-publish",
        "--allow-send",
        "--chat-id",
        "--registry-id",
        "--all-channels",
        "--docker",
        "--systemd",
    ):
        assert forbidden not in command
    assert set(child_env) <= set(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
    assert "OPENAI_API_KEY_FILE" not in child_env
    assert "TELEGRAM_BOT_TOKEN" not in child_env
    assert "X_BEARER_TOKEN" not in child_env
    assert "UNKNOWN_EXTRA" not in child_env
    assert report["status"] == "pass"
    assert report["actual_attempted_operations"]["child_runner_invoked"] is True
    assert report["child_report"]["status"] == "pass"
    assert report["child_report"]["reason_code"] == "child_ok"
    assert "SENTINEL_PRIVATE_STDERR_SHOULD_NOT_PRINT" not in captured.out
    assert "trendingrepo" not in captured.out
    for value in SENTINEL_VALUES:
        assert value not in captured.out


def test_cli_rejects_unsupported_authority_flags_as_json(capsys) -> None:
    for flag in (
        "--allow-send",
        "--allow-redis-publish",
        "--allow-source-outbox-publish",
        "--chat-id",
        "--registry-id",
        "--all-channels",
        "--docker",
        "--systemd",
    ):
        exit_code = runner.main([flag])
        report = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert report["status"] == "blocked"
        assert report["reason_code"] == "unsupported_cli_argument"
        assert report["actual_attempted_operations"]["runtime_env_read_attempted"] is False
        assert report["actual_attempted_operations"]["child_runner_invoked"] is False
