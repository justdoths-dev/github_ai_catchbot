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
CHILD_STDOUT_SENTINELS = (
    "SENTINEL_CHILD_RAW_STDOUT_SHOULD_NOT_PRINT",
    "SENTINEL_CHILD_RAW_SOURCE_TEXT_SHOULD_NOT_PRINT",
    "https://example.invalid/raw-child-url-should-not-print",
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


def _rich_child_report(*, duplicate_noop_count: int = 1) -> dict[str, Any]:
    return {
        "schema_version": "live_collector_one_channel_source_last_rollout_v1",
        "runner_name": "bounded_collector_history_ingest_runner",
        "mode": "execute",
        "rollout_scope": "exact_targets",
        "status": "pass",
        "reason_code": "ok",
        "target_fingerprints": ["sha256:1111111111111111"],
        "source_message_fingerprints": ["sha256:2222222222222222"],
        "source_outbox_event_fingerprints": ["sha256:3333333333333333"],
        "exact_channel_target_fingerprint": "sha256:4444444444444444",
        "registry_target_fingerprint": "sha256:5555555555555555",
        "authority": {
            "live_telegram_read_attempted": True,
            "telegram_send_attempted": False,
            "openai_attempted": False,
            "github_attempted": False,
            "x_attempted": False,
            "web_attempted": False,
            "redis_consume_or_ack": False,
            "broad_registry_ingest": False,
            "docker_or_systemd_called": False,
            "alembic_or_ddl_ran": False,
        },
        "gates": {
            "operator_approved": True,
            "confirm_token_valid": True,
            "runtime_config_allowed": True,
            "database_read_allowed": True,
            "telegram_read_allowed": True,
            "database_write_allowed": True,
            "source_message_write_allowed": True,
            "source_version_write_allowed": True,
            "source_outbox_write_allowed": True,
            "source_outbox_publish_allowed": False,
            "redis_publish_allowed": False,
        },
        "telegram_read_attempted": True,
        "telegram_read_called": True,
        "database_read_attempted": True,
        "database_write_attempted": True,
        "source_message_write_attempted": True,
        "source_version_write_attempted": True,
        "source_outbox_write_attempted": True,
        "source_outbox_publish_attempted": False,
        "redis_publish_attempted": False,
        "messages_requested": 1,
        "messages_seen": 1,
        "bounded_counts": {
            "registry_targets": 1,
            "source_messages_created": 1,
            "source_versions_created": 1,
            "source_created_events": 1,
            "source_normalize_handoffs": 0,
            "duplicate_noops": duplicate_noop_count,
        },
        "readback": {
            "source_current_found_count": 1,
            "source_version_rows_count": 1,
            "source_created_events_count": 1,
            "source_outbox_events_count": 1,
        },
        "duplicate_noop_proof": {
            "proved_count": duplicate_noop_count,
            "without_second_telegram_read": True,
        },
        "duplicate_noop_proof_count": duplicate_noop_count,
        "raw_values_printed": {
            "source_text": False,
            "source_ref": False,
            "url": False,
            "raw_id": False,
            "tdlib_payload": False,
            "database_url": False,
            "redis_url": False,
            "secret": False,
            "runtime_value": False,
            "stderr": False,
            "traceback": False,
            "exception_body": False,
        },
        "redactions_applied": {
            "full_chat_id_omitted": True,
            "full_registry_id_omitted": True,
            "source_ref_omitted": True,
            "full_source_message_id_omitted": True,
            "full_event_id_omitted": True,
            "full_redis_message_id_omitted": True,
            "raw_message_json_omitted": True,
            "message_text_omitted": True,
            "entities_json_omitted": True,
            "url_surface_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "telegram_credentials_omitted": True,
            "tdlib_session_paths_omitted": True,
            "exception_detail_omitted": True,
            "traceback_omitted": True,
            "stderr_omitted": True,
        },
        "rollback_stop_readback": {
            "exact_runner_completed": True,
        },
        "side_effects": {
            "telegram_send_called": False,
            "telegram_edit_called": False,
        },
        "ok": True,
        "error_code": None,
        "error_class": None,
        "unsafe_raw_source_text": CHILD_STDOUT_SENTINELS[1],
        "unsafe_raw_url": CHILD_STDOUT_SENTINELS[2],
    }


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
    assert report["child_report_projection"]["stdout_parsed_as_json"] is False
    assert report["f1_live_readback_closure"]["child_report_available"] is False
    assert report["f1_live_readback_closure"]["safe_to_review_for_f1_live_read_closure"] is False
    assert report["completion_claims"]["F1_CHILD_READBACK_PROJECTION_READY"] is False
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
    assert report["child_report_projection"]["stdout_parsed_as_json"] is False
    assert report["f1_live_readback_closure"]["safe_to_review_for_f1_live_read_closure"] is False


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
    assert report["child_report_projection"]["stdout_parsed_as_json"] is False
    assert report["f1_live_readback_closure"]["safe_to_review_for_f1_live_read_closure"] is False


def test_execute_projects_rich_child_pass_readback_without_raw_stdout(tmp_path: Path, capsys) -> None:
    env_file = _fixture_env(tmp_path)
    rich_child_report = _rich_child_report()

    def fake_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(rich_child_report, sort_keys=True) + "\n",
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
    projection = report["child_report_projection"]
    closure = report["f1_live_readback_closure"]

    assert exit_code == 0
    assert report["status"] == "pass"
    assert report["reason_code"] == "child_bounded_runner_passed"
    assert report["child_report"]["stdout_parsed_as_json"] is True
    assert report["child_report"]["stdout_printed"] is False
    assert report["child_report"]["stderr_printed"] is False
    assert projection["status"] == "pass"
    assert projection["reason_code"] == "ok"
    assert projection["schema_version"] == "live_collector_one_channel_source_last_rollout_v1"
    assert projection["runner_name"] == "bounded_collector_history_ingest_runner"
    assert projection["mode"] == "execute"
    assert projection["rollout_scope"] == "exact_targets"
    assert projection["authority"]["live_telegram_read_attempted"] is True
    assert projection["authority"]["telegram_send_attempted"] is False
    assert projection["authority"]["openai_attempted"] is False
    assert projection["authority"]["github_attempted"] is False
    assert projection["authority"]["x_attempted"] is False
    assert projection["authority"]["web_attempted"] is False
    assert projection["authority"]["docker_or_systemd_called"] is False
    assert projection["authority"]["alembic_or_ddl_ran"] is False
    assert projection["gates"]["source_outbox_publish_allowed"] is False
    assert projection["gates"]["redis_publish_allowed"] is False
    assert projection["telegram_read_called"] is True
    assert projection["database_write_attempted"] is True
    assert projection["source_message_write_attempted"] is True
    assert projection["source_version_write_attempted"] is True
    assert projection["source_outbox_write_attempted"] is True
    assert projection["source_outbox_publish_attempted"] is False
    assert projection["redis_publish_attempted"] is False
    assert projection["messages_requested"] == 1
    assert projection["messages_seen"] == 1
    assert projection["bounded_counts"]["registry_targets"] == 1
    assert projection["bounded_counts"]["source_messages_created"] == 1
    assert projection["bounded_counts"]["source_versions_created"] == 1
    assert projection["bounded_counts"]["source_created_events"] == 1
    assert projection["bounded_counts"]["source_normalize_handoffs"] == 0
    assert projection["bounded_counts"]["duplicate_noops"] == 1
    assert projection["readback"]["source_current_found_count"] == 1
    assert projection["readback"]["source_version_rows_count"] == 1
    assert projection["readback"]["source_outbox_events_count"] == 1
    assert projection["duplicate_noop_proof"]["proved_count"] == 1
    assert projection["duplicate_noop_proof_count"] == 1
    assert projection["duplicate_noop_proof"]["without_second_telegram_read"] is True
    assert projection["target_fingerprints"] == ["sha256:1111111111111111"]
    assert projection["source_message_fingerprints"] == ["sha256:2222222222222222"]
    assert projection["source_outbox_event_fingerprints"] == ["sha256:3333333333333333"]
    assert projection["exact_channel_target_fingerprint"] == "sha256:4444444444444444"
    assert projection["registry_target_fingerprint"] == "sha256:5555555555555555"
    assert projection["raw_values_printed"]["runtime_value"] is False
    assert projection["redactions_applied"]["stderr_omitted"] is True
    assert projection["rollback_stop_readback"]["exact_runner_completed"] is True
    assert projection["side_effects"]["telegram_send_called"] is False
    assert projection["side_effects"]["telegram_edit_called"] is False
    assert closure["child_runner_returncode_zero"] is True
    assert closure["wrapper_child_execution_passed"] is True
    assert closure["exact_child_runner_passed"] is True
    assert closure["source_current_readback_present"] is True
    assert closure["source_version_readback_present"] is True
    assert closure["source_outbox_readback_present"] is True
    assert closure["duplicate_noop_proof_present"] is True
    assert closure["duplicate_noop_without_second_telegram_read"] is True
    assert closure["source_outbox_publish_disabled"] is True
    assert closure["redis_publish_disabled"] is True
    assert closure["telegram_send_disabled"] is True
    assert closure["provider_calls_disabled"] is True
    assert closure["docker_systemd_alembic_disabled"] is True
    assert closure["raw_values_not_printed"] is True
    assert closure["runtime_values_not_printed"] is True
    assert closure["safe_to_review_for_f1_live_read_closure"] is True
    assert report["completion_claims"]["F1_CHILD_READBACK_PROJECTION_READY"] is True
    assert report["completion_claims"]["F1_LIVE_EXECUTION_REVIEWABILITY_REPAIRED"] is True
    assert "unsafe_raw_source_text" not in captured.out
    assert "unsafe_raw_url" not in captured.out
    assert "trendingrepo" not in captured.out
    assert "SENTINEL_PRIVATE_STDERR_SHOULD_NOT_PRINT" not in captured.out
    for value in SENTINEL_VALUES + CHILD_STDOUT_SENTINELS:
        assert value not in captured.out


def test_execute_child_nonzero_returncode_with_pass_json_does_not_claim_f1_closure(
    tmp_path: Path, capsys
) -> None:
    env_file = _fixture_env(tmp_path)
    rich_child_report = _rich_child_report()

    def fake_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=command,
            returncode=1,
            stdout=json.dumps(rich_child_report, sort_keys=True) + "\n",
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

    assert exit_code == 1
    assert report["status"] == "failed"
    assert report["reason_code"] == "child_bounded_runner_failed"
    assert report["actual_attempted_operations"]["child_runner_returncode"] == 1
    assert report["child_report_projection"]["status"] == "pass"
    assert report["child_report_projection"]["reason_code"] == "ok"
    assert report["f1_live_readback_closure"]["child_runner_returncode_zero"] is False
    assert report["f1_live_readback_closure"]["wrapper_child_execution_passed"] is False
    assert report["f1_live_readback_closure"]["exact_child_runner_passed"] is False
    assert report["f1_live_readback_closure"]["safe_to_review_for_f1_live_read_closure"] is False
    assert report["completion_claims"]["F1_LIVE_EXECUTION_REVIEWABILITY_REPAIRED"] is False
    assert "unsafe_raw_source_text" not in captured.out
    assert "unsafe_raw_url" not in captured.out
    assert "trendingrepo" not in captured.out
    assert "SENTINEL_PRIVATE_STDERR_SHOULD_NOT_PRINT" not in captured.out
    for value in SENTINEL_VALUES + CHILD_STDOUT_SENTINELS:
        assert value not in captured.out


def test_execute_child_pass_missing_duplicate_noop_does_not_claim_f1_closure(tmp_path: Path, capsys) -> None:
    env_file = _fixture_env(tmp_path)
    rich_child_report = _rich_child_report(duplicate_noop_count=0)
    rich_child_report.pop("duplicate_noop_proof")
    rich_child_report.pop("duplicate_noop_proof_count")

    def fake_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(rich_child_report, sort_keys=True) + "\n",
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
    assert report["status"] == "pass"
    assert report["child_report_projection"]["status"] == "pass"
    assert report["f1_live_readback_closure"]["exact_child_runner_passed"] is True
    assert report["f1_live_readback_closure"]["duplicate_noop_proof_present"] is False
    assert report["f1_live_readback_closure"]["safe_to_review_for_f1_live_read_closure"] is False
    assert "SENTINEL_PRIVATE_STDERR_SHOULD_NOT_PRINT" not in captured.out
    for value in CHILD_STDOUT_SENTINELS:
        assert value not in captured.out


def test_execute_child_malformed_stdout_does_not_print_raw_or_claim_f1_closure(tmp_path: Path, capsys) -> None:
    env_file = _fixture_env(tmp_path)

    def fake_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=f"not-json {CHILD_STDOUT_SENTINELS[0]}",
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
    assert report["status"] == "pass"
    assert report["child_report"]["stdout_parsed_as_json"] is False
    assert report["child_report_projection"]["stdout_parsed_as_json"] is False
    assert report["f1_live_readback_closure"]["child_report_available"] is False
    assert report["f1_live_readback_closure"]["safe_to_review_for_f1_live_read_closure"] is False
    assert report["redaction_audit"]["child_stdout_printed"] is False
    assert "SENTINEL_PRIVATE_STDERR_SHOULD_NOT_PRINT" not in captured.out
    for value in CHILD_STDOUT_SENTINELS:
        assert value not in captured.out


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
    assert report["child_report_projection"]["status"] == "pass"
    assert report["child_report_projection"]["reason_code"] == "child_ok"
    assert report["f1_live_readback_closure"]["safe_to_review_for_f1_live_read_closure"] is False
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
