from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_tdlib_auth_operator_execution_wrapper.py"
RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdlib_auth_operator_execution_wrapper.md"
)

SIDE_EFFECT_BOOLEAN_KEYS = (
    "runtime_env_read",
    "runtime_env_values_printed",
    "login_code_prompted",
    "login_code_submitted",
    "login_code_value_printed",
    "login_code_value_stored",
    "tdlib_auth_attempted",
    "tdlib_auth_completed",
    "telegram_connected",
    "session_state_created_or_reused",
    "database_connected",
    "redis_connected",
    "alembic_run",
    "app_runtime_started",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "files_mutated",
    "network_called",
)

REQUIRED_REPORT_KEYS = (
    "report_type",
    "contract_status",
    "approval_required",
    "approved_execution_requested",
    "approved_tdlib_auth_code_entry_requested",
    "auth_only_entrypoint_status",
    "selected_entrypoint",
    "blocked_reason",
    "runtime_env_path",
    "tdlib_auth_receive_timeout_sec",
    "tdlib_auth_max_authorization_updates",
    "tdlib_auth_receive_budget_seconds",
    "runtime_env_read",
    "runtime_env_values_printed",
    "secret_values_printed",
    "login_code_prompted",
    "login_code_submitted",
    "login_code_value_printed",
    "login_code_value_stored",
    "tdlib_auth_attempted",
    "tdlib_auth_completed",
    "manual_intervention_required",
    "telegram_connected",
    "session_state_created_or_reused",
    "db_connected",
    "redis_connected",
    "database_connected",
    "redis_connected",
    "alembic_run",
    "app_runtime_started",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "collector_main_used",
    "collector_service_used",
    "collector_runtime_used",
    "systemd_or_docker_changed",
    "files_mutated",
    "network_called",
    "checks_failed",
    "failures",
    "likely_next_slice",
    "tdlib_parameters_shape_guard",
    "tdlib_parameters_semantic_guard",
)

REQUIRED_RUNBOOK_HEADINGS = (
    "## Purpose",
    "## Source-of-truth / architecture boundary",
    "## Current closed prerequisites",
    "## Scope",
    "## Non-authorizations",
    "## Wrapper behavior",
    "## Auth-only entrypoint decision",
    "## Approved execution guard",
    "## Redacted output shape",
    "## Operator safety rules",
    "## Acceptance criteria",
    "## Next bounded action",
)

FORBIDDEN_PATTERNS = {
    "telegram_bot_token": re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b"),
    "telegram_api_hash_assignment": re.compile(
        r"TELEGRAM_API_HASH\s*[:=]\s*[0-9a-fA-F]{32}"
    ),
    "telegram_phone_assignment": re.compile(
        r"TELEGRAM_PHONE_NUMBER\s*[:=]\s*\+?\d[\d\s().-]{6,}"
    ),
    "telegram_login_code_assignment": re.compile(r"TELEGRAM_LOGIN_CODE\s*[:=]"),
    "auth_code_assignment": re.compile(r"AUTH_CODE\s*[:=]"),
    "private_invite_plus": re.compile(r"https?://t\.me/\+"),
    "private_invite_joinchat": re.compile(r"https?://t\.me/joinchat/"),
    "legacy_private_invite_joinchat": re.compile(r"telegram\.me/joinchat/"),
    "cat_runtime_env": re.compile(r"cat /etc/github-ai-catchbot/runtime\.env"),
    "source_runtime_env": re.compile(r"source /etc/github-ai-catchbot/runtime\.env"),
    "dot_source_runtime_env": re.compile(r"(?m)^\s*\.\s+/etc/github-ai-catchbot/runtime\.env"),
    "export_cat": re.compile(r"export \$\(cat"),
    "postgresql_url": re.compile("postgresql" + r"://"),
    "postgresql_psycopg_url": re.compile("postgresql" + r"\+psycopg://"),
    "redis_url": re.compile("redis" + r"://"),
    "echo_secret_append": re.compile(r"echo .*TELEGRAM" + r"_.*>" + r">", re.IGNORECASE),
    "tee_append_runtime_env": re.compile(r"tee -a /etc/github-ai-catchbot/runtime\.env"),
    "ipv4_literal": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
}


def _module():
    from scripts.ops import dedicated_vps_tdlib_auth_operator_execution_wrapper as module

    return module


class FakeAuthOnlyResult:
    def __init__(
        self,
        *,
        status: str = "manual_intervention_required",
        attempted: bool = True,
        completed: bool = False,
        manual_intervention_required: bool = True,
        extra_payload: dict | None = None,
    ) -> None:
        self.status = status
        self.attempted = attempted
        self.completed = completed
        self.manual_intervention_required = manual_intervention_required
        self.extra_payload = extra_payload or {}

    def to_redacted_dict(self) -> dict:
        payload = {
            "schema_version": "tdlib_auth_only_result_v1",
            "auth_entrypoint_status": self.status,
            "tdlib_auth_attempted": self.attempted,
            "tdlib_auth_completed": self.completed,
            "telegram_connected": False,
            "session_state_created_or_reused": False,
            "manual_intervention_required": self.manual_intervention_required,
            "manual_intervention_reason": "Telegram login code required from operator",
            "final_authorization_state": "waiting_code",
            "requests_sent_count": 0,
            "auth_request_types_sent": [],
            "last_auth_request_type": None,
            "authorization_updates_seen_count": 1,
            "non_auth_response_count": 0,
            "non_auth_response_type_counts": {},
            "tdlib_ok_seen": False,
            "last_non_auth_response_type": None,
            "ok_response_count": 0,
            "ok_response_auth_request_types": [],
            "last_ok_response_auth_request_type": None,
            "pending_auth_request_types_at_timeout": [],
            "connection_state_updates_seen_count": 0,
            "last_connection_state_type": None,
            "connection_state_type_counts": {},
            "max_authorization_updates": 120,
            "receive_timeout_sec": 1.0,
            "runtime_env_values_printed": False,
            "login_code_prompted": False,
            "login_code_submitted": False,
            "login_code_value_printed": False,
            "login_code_value_stored": False,
            "database_connected": False,
            "redis_connected": False,
            "alembic_run": False,
            "app_runtime_started": False,
            "live_collector_started": False,
            "notifier_transport_enabled": False,
            "production_rollout_performed": False,
            "secret_values_printed": False,
            "source_message_persisted": False,
            "outbox_event_emitted": False,
            "collector_main_imported": False,
            "collector_runtime_started": False,
            "error": None,
            "error_present": False,
            "error_type": None,
            "tdlib_error_present": False,
            "tdlib_error_code": None,
            "tdlib_error_type": None,
            "tdlib_error_message_len": None,
            "tdlib_error_categories": [],
            "completion_failure_category": None,
        }
        payload.update(self.extra_payload)
        return payload


def _approved_env(tmp_path: Path) -> dict[str, str]:
    return {
        "APP_ENV": "dev",
        "COLLECTOR_MODE": "replay",
        "DATABASE_URL": "postgresql://collector:secret@localhost:5432/catchbot",
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_API_HASH": "fake-api-hash",
        "TELEGRAM_PHONE_NUMBER": "+10000000000",
        "TELEGRAM_2FA_PASSWORD": "",
        "TDLIB_DB_ENCRYPTION_KEY": "fake-tdlib-key",
        "TDLIB_STATE_DIR": str(tmp_path / "tdlib-state"),
        "TDLIB_FILES_DIR": str(tmp_path / "tdlib-files"),
    }


def _sensitive_runtime_env(tmp_path: Path) -> dict[str, str]:
    env = _approved_env(tmp_path)
    env.update(
        {
            "DATABASE_URL": "postgresql://collector:secret@localhost:5432/catchbot",
            "REDIS_URL": "redis://:redis-secret@localhost:6379/0",
            "TELEGRAM_API_HASH": "0123456789abcdef0123456789abcdef",
            "TELEGRAM_PHONE_NUMBER": "+15555550123",
            "TELEGRAM_LOGIN_CODE": "12345",
            "TELEGRAM_2FA_PASSWORD": "correct horse battery staple",
            "TDLIB_DB_ENCRYPTION_KEY": "tdlib-secret-encryption-key",
        }
    )
    return env


def _assert_invalid_budget_blocks_without_runtime_or_secret_leak(
    *,
    tmp_path: Path,
    receive_timeout_sec: object,
    max_authorization_updates: object,
    expected_receive_timeout_sec: float | None,
    expected_max_authorization_updates: int | None,
    expected_receive_budget_seconds: float | None,
    forbidden_fragments: tuple[str, ...] = (),
) -> None:
    env_reader_called = False
    transport_called = False
    runner_called = False
    sensitive_env = _sensitive_runtime_env(tmp_path)

    def forbidden_env_reader(path: str | Path) -> dict[str, str]:
        nonlocal env_reader_called
        env_reader_called = True
        return sensitive_env

    def forbidden_transport() -> object:
        nonlocal transport_called
        transport_called = True
        raise AssertionError("transport must not be built for invalid receive budget")

    async def forbidden_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner must not be called for invalid receive budget")

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        runtime_env_path=tmp_path / "runtime.env",
        tdlib_auth_receive_timeout_sec=receive_timeout_sec,
        tdlib_auth_max_authorization_updates=max_authorization_updates,
        real_transport_factory=forbidden_transport,
        runtime_env_reader=forbidden_env_reader,
        auth_runner=forbidden_runner,
    )

    rendered = json.dumps(result.report, allow_nan=False, sort_keys=True)
    assert json.loads(rendered) == result.report
    assert result.exit_code != 0
    assert result.report["contract_status"] == "blocked_tdlib_auth_receive_budget_invalid"
    assert result.report["blocked_reason"] == "tdlib_auth_receive_budget_invalid"
    assert "tdlib_auth_receive_budget.invalid" in result.report["checks_failed"]
    assert result.report["tdlib_auth_attempted"] is False
    assert result.report["tdlib_auth_completed"] is False
    assert result.report["runtime_env_read"] is False
    assert result.report["runtime_env_values_printed"] is False
    assert result.report["secret_values_printed"] is False
    assert env_reader_called is False
    assert transport_called is False
    assert runner_called is False
    assert result.report["tdlib_auth_receive_timeout_sec"] == expected_receive_timeout_sec
    assert result.report["tdlib_auth_max_authorization_updates"] == expected_max_authorization_updates
    assert result.report["tdlib_auth_receive_budget_seconds"] == expected_receive_budget_seconds
    assert "NaN" not in rendered
    assert "Infinity" not in rendered
    forbidden_values = (
        sensitive_env["DATABASE_URL"],
        sensitive_env["REDIS_URL"],
        sensitive_env["TELEGRAM_API_HASH"],
        sensitive_env["TELEGRAM_PHONE_NUMBER"],
        sensitive_env["TELEGRAM_LOGIN_CODE"],
        sensitive_env["TELEGRAM_2FA_PASSWORD"],
        sensitive_env["TDLIB_DB_ENCRYPTION_KEY"],
        sensitive_env["TDLIB_STATE_DIR"],
        sensitive_env["TDLIB_FILES_DIR"],
    )
    for value in forbidden_values:
        assert value not in rendered
    for fragment in forbidden_fragments:
        assert fragment not in rendered


def _run_default_wrapper() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_wrapper_default_no_approval_returns_json_and_no_side_effects() -> None:
    result = _run_default_wrapper()

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["report_type"] == "dedicated_vps_tdlib_auth_operator_execution_wrapper_v1"
    assert report["auth_only_entrypoint_status"] == "available"
    assert report["selected_entrypoint"] == "src.services.collector_telegram.auth_entrypoint"
    assert report["contract_status"] == "approval_required"
    assert report["likely_next_slice"] == "dedicated_vps_tdlib_auth_operator_execution"
    assert report["approval_required"] is True
    assert report["approved_execution_requested"] is False
    assert report["approved_tdlib_auth_code_entry_requested"] is False
    for key in SIDE_EFFECT_BOOLEAN_KEYS:
        assert report[key] is False


def test_wrapper_output_includes_required_shape() -> None:
    report = _module().generate_report(ROOT).report

    for key in REQUIRED_REPORT_KEYS:
        assert key in report


def test_wrapper_default_receive_budget_is_preserved() -> None:
    report = _module().generate_report(ROOT).report

    assert report["tdlib_auth_receive_timeout_sec"] == 1.0
    assert report["tdlib_auth_max_authorization_updates"] == 120
    assert report["tdlib_auth_receive_budget_seconds"] == 120.0


def test_parser_accepts_receive_budget_override_options_as_raw_strings() -> None:
    args = _module().build_parser().parse_args(
        [
            "--tdlib-auth-receive-timeout-sec",
            "2.5",
            "--tdlib-auth-max-authorization-updates",
            "200",
        ]
    )

    assert args.tdlib_auth_receive_timeout_sec == "2.5"
    assert args.tdlib_auth_max_authorization_updates == "200"


def test_parser_accepts_approved_code_entry_flag_without_code_value_options() -> None:
    parser = _module().build_parser()
    args = parser.parse_args(["--approved-tdlib-auth-code-entry"])
    option_strings = {
        option
        for action in parser._actions
        for option in action.option_strings
    }

    assert args.approved_tdlib_auth_code_entry is True
    assert "--approved-tdlib-auth-code-entry" in option_strings
    for forbidden_option in (
        "--telegram-login-code",
        "--tdlib-auth-login-code",
        "--tdlib-auth-code",
        "--auth-code",
    ):
        assert forbidden_option not in option_strings
        with pytest.raises(SystemExit):
            parser.parse_args([forbidden_option, "sensitive-code-for-test"])


def test_wrapper_does_not_read_runtime_env() -> None:
    report = _module().generate_report(ROOT).report

    assert report["runtime_env_path"] == "/etc/github-ai-catchbot/runtime.env"
    assert report["runtime_env_read"] is False
    assert report["runtime_env_values_printed"] is False


def test_wrapper_output_does_not_expose_secret_looking_values() -> None:
    result = _run_default_wrapper()

    combined = result.stdout + result.stderr
    for name, pattern in FORBIDDEN_PATTERNS.items():
        assert not pattern.search(combined), name


def test_telegram_bot_token_is_not_tdlib_auth_credential() -> None:
    report = _module().generate_report(ROOT).report
    runbook_text = RUNBOOK.read_text(encoding="utf-8")

    assert report["entrypoint_assessment"]["telegram_bot_token_used_for_tdlib_auth"] is False
    assert "`TELEGRAM_BOT_TOKEN` is not used for TDLib auth." in runbook_text
    assert "`TELEGRAM_BOT_TOKEN` is not a TDLib auth credential." in runbook_text


def test_wrapper_reports_available_or_missing_and_next_slice_matches() -> None:
    report = _module().generate_report(ROOT).report

    assert report["auth_only_entrypoint_status"] == "available"
    assert report["selected_entrypoint"] == "src.services.collector_telegram.auth_entrypoint"
    assert report["contract_status"] == "approval_required"
    assert report["likely_next_slice"] == "dedicated_vps_tdlib_auth_operator_execution"
    assert "approval.required" in report["checks_failed"]


def test_tests_do_not_call_approved_execution_flag() -> None:
    env = dict(os.environ)
    env["TDJSON_LIBRARY_PATH"] = "/definitely/missing/libtdjson.so"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--approved-tdlib-auth-operator-execution",
            "--runtime-env-path",
            str(ROOT / "missing-runtime.env"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
        env=env,
    )

    report = json.loads(result.stdout)
    assert result.returncode != 0
    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert report["blocked_reason"] == "runtime_env_unreadable"
    assert report["runtime_env_read"] is False
    assert report["tdlib_auth_attempted"] is False


def test_code_entry_flag_noninteractive_blocks_before_runtime_or_auth(tmp_path: Path) -> None:
    env_reader_called = False
    transport_called = False
    runner_called = False
    prompt_called = False

    def forbidden_env_reader(path: str | Path) -> dict[str, str]:
        nonlocal env_reader_called
        env_reader_called = True
        return _sensitive_runtime_env(tmp_path)

    def forbidden_transport() -> object:
        nonlocal transport_called
        transport_called = True
        raise AssertionError("transport must not be built without an interactive terminal")

    async def forbidden_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner must not be called without an interactive terminal")

    def forbidden_prompt(prompt_text: str) -> str:
        nonlocal prompt_called
        prompt_called = True
        raise AssertionError("prompt must not run without an interactive terminal")

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        approved_tdlib_auth_code_entry=True,
        runtime_env_path=tmp_path / "runtime.env",
        real_transport_factory=forbidden_transport,
        runtime_env_reader=forbidden_env_reader,
        auth_runner=forbidden_runner,
        login_code_prompt=forbidden_prompt,
        code_entry_interactive_checker=lambda: False,
    )

    rendered = json.dumps(result.report, sort_keys=True)
    assert result.exit_code != 0
    assert result.report["contract_status"] == "blocked_tdlib_auth_code_entry_not_interactive"
    assert result.report["blocked_reason"] == "tdlib_auth_code_entry_not_interactive"
    assert "tdlib_auth_code_entry.not_interactive" in result.report["checks_failed"]
    assert result.report["approved_tdlib_auth_code_entry_requested"] is True
    assert result.report["tdlib_auth_attempted"] is False
    assert result.report["runtime_env_read"] is False
    assert result.report["login_code_prompted"] is False
    assert result.report["login_code_submitted"] is False
    assert result.report["login_code_value_printed"] is False
    assert result.report["login_code_value_stored"] is False
    assert env_reader_called is False
    assert transport_called is False
    assert runner_called is False
    assert prompt_called is False
    assert "sensitive-code-for-test" not in rendered


def test_cli_code_entry_noninteractive_returns_redacted_json_without_prompt_stderr() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--approved-tdlib-auth-operator-execution",
            "--approved-tdlib-auth-code-entry",
            "--runtime-env-path",
            str(ROOT / "missing-runtime.env"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    report = json.loads(result.stdout)
    assert result.returncode != 0
    assert result.stderr == ""
    assert report["contract_status"] == "blocked_tdlib_auth_code_entry_not_interactive"
    assert report["blocked_reason"] == "tdlib_auth_code_entry_not_interactive"
    assert report["runtime_env_read"] is False
    assert report["tdlib_auth_attempted"] is False
    assert report["login_code_prompted"] is False
    assert report["login_code_submitted"] is False
    assert report["login_code_value_printed"] is False
    assert report["login_code_value_stored"] is False


def test_approved_mode_blocks_when_real_transport_missing_after_redacted_shape_guard(tmp_path: Path) -> None:
    def missing_transport() -> object:
        raise RuntimeError("tdjson unavailable")

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        real_transport_factory=missing_transport,
        runtime_env_reader=lambda path: _approved_env(tmp_path),
    )

    assert result.exit_code != 0
    assert result.report["contract_status"] == "blocked_real_transport_missing"
    assert result.report["blocked_reason"] == "blocked_real_transport_missing"
    assert result.report["tdlib_auth_attempted"] is False
    assert result.report["runtime_env_read"] is True
    assert result.report["session_state_created_or_reused"] is False
    assert result.report["tdlib_parameters_shape_guard"] == {
        "checked": True,
        "valid": True,
        "errors": [],
    }
    assert result.report["tdlib_parameters_semantic_guard"] == {
        "checked": True,
        "valid": True,
        "errors": [],
    }


def test_approved_mode_blocks_invalid_tdlib_parameters_before_transport_or_runner(tmp_path: Path) -> None:
    transport_called = False
    runner_called = False

    def forbidden_transport() -> object:
        nonlocal transport_called
        transport_called = True
        raise AssertionError("transport must not be built for invalid TDLib parameters")

    async def forbidden_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner must not be called for invalid TDLib parameters")

    invalid_env = _approved_env(tmp_path)
    invalid_env["TELEGRAM_API_ID"] = "0"
    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        runtime_env_path=tmp_path / "runtime.env",
        real_transport_factory=forbidden_transport,
        runtime_env_reader=lambda path: invalid_env,
        auth_runner=forbidden_runner,
    )

    rendered = json.dumps(result.report)
    assert result.exit_code != 0
    assert result.report["contract_status"] == "blocked_tdlib_parameters_invalid"
    assert result.report["blocked_reason"] == "tdlib_parameters_invalid"
    assert "tdlib_parameters.invalid" in result.report["checks_failed"]
    assert result.report["tdlib_auth_attempted"] is False
    assert result.report["tdlib_auth_completed"] is False
    assert result.report["telegram_connected"] is False
    assert result.report["runtime_env_read"] is True
    assert result.report["runtime_env_values_printed"] is False
    assert result.report["secret_values_printed"] is False
    assert result.report["manual_intervention_required"] is False
    assert result.report["session_state_created_or_reused"] is False
    assert result.report["tdlib_parameters_shape_guard"]["checked"] is True
    assert result.report["tdlib_parameters_shape_guard"]["valid"] is False
    assert "api_id.invalid" in result.report["tdlib_parameters_shape_guard"]["errors"]
    assert result.report["tdlib_parameters_semantic_guard"]["checked"] is False
    assert transport_called is False
    assert runner_called is False
    assert "fake-api-hash" not in rendered
    assert "fake-tdlib-key" not in rendered
    assert "+10000000000" not in rendered
    assert "postgresql://collector:secret" not in rendered
    assert str(tmp_path / "tdlib-state") not in rendered
    assert str(tmp_path / "tdlib-files") not in rendered


def test_approved_mode_blocks_semantic_invalid_tdlib_parameters_before_transport_or_runner(
    tmp_path: Path,
    monkeypatch,
) -> None:
    transport_called = False
    runner_called = False

    def forbidden_transport() -> object:
        nonlocal transport_called
        transport_called = True
        raise AssertionError("transport must not be built for semantically invalid TDLib parameters")

    async def forbidden_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner must not be called for semantically invalid TDLib parameters")

    from src.services.collector_telegram import tdlib_client

    monkeypatch.setattr(
        tdlib_client,
        "tdlib_parameters_semantic_errors",
        lambda payload: ("database_encryption_key.invalid_base64",),
    )

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        runtime_env_path=tmp_path / "runtime.env",
        real_transport_factory=forbidden_transport,
        runtime_env_reader=lambda path: _approved_env(tmp_path),
        auth_runner=forbidden_runner,
    )

    rendered = json.dumps(result.report)
    assert result.exit_code != 0
    assert result.report["contract_status"] == "blocked_tdlib_parameters_semantic_invalid"
    assert result.report["blocked_reason"] == "tdlib_parameters_semantic_invalid"
    assert "tdlib_parameters.semantic_invalid" in result.report["checks_failed"]
    assert result.report["tdlib_auth_attempted"] is False
    assert result.report["tdlib_auth_completed"] is False
    assert result.report["telegram_connected"] is False
    assert result.report["runtime_env_read"] is True
    assert result.report["runtime_env_values_printed"] is False
    assert result.report["secret_values_printed"] is False
    assert result.report["manual_intervention_required"] is False
    assert result.report["session_state_created_or_reused"] is False
    assert result.report["tdlib_parameters_shape_guard"] == {
        "checked": True,
        "valid": True,
        "errors": [],
    }
    assert result.report["tdlib_parameters_semantic_guard"] == {
        "checked": True,
        "valid": False,
        "errors": ["database_encryption_key.invalid_base64"],
    }
    assert transport_called is False
    assert runner_called is False
    assert "fake-api-hash" not in rendered
    assert "fake-tdlib-key" not in rendered
    assert "+10000000000" not in rendered
    assert "postgresql://collector:secret" not in rendered
    assert str(tmp_path / "tdlib-state") not in rendered
    assert str(tmp_path / "tdlib-files") not in rendered


def test_approved_mode_uses_auth_only_entrypoint_once_with_redacted_result(tmp_path: Path) -> None:
    calls: list[dict] = []

    async def fake_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        calls.append({"args": args, "kwargs": kwargs})
        return FakeAuthOnlyResult()

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        runtime_env_path=tmp_path / "runtime.env",
        real_transport_factory=object,
        runtime_env_reader=lambda path: _approved_env(tmp_path),
        auth_runner=fake_runner,
    )

    rendered = json.dumps(result.report)
    assert result.exit_code != 0
    assert len(calls) == 1
    config = calls[0]["args"][0]
    assert calls[0]["kwargs"]["receive_timeout_sec"] == 1.0
    assert calls[0]["kwargs"]["max_authorization_updates"] == 120
    assert result.report["tdlib_auth_receive_timeout_sec"] == 1.0
    assert result.report["tdlib_auth_max_authorization_updates"] == 120
    assert result.report["tdlib_auth_receive_budget_seconds"] == 120.0
    assert config.telegram_api_id == 12345
    assert config.telegram_api_hash == "fake-api-hash"
    assert config.tdlib_state_dir == str(tmp_path / "tdlib-state")
    assert config.tdlib_files_dir == str(tmp_path / "tdlib-files")
    assert config.tdlib_db_encryption_key == "fake-tdlib-key"
    assert result.report["contract_status"] == "manual_intervention_required"
    assert result.report["auth_only_entrypoint_status"] == "manual_intervention_required"
    assert result.report["selected_entrypoint"] == "src.services.collector_telegram.auth_entrypoint"
    assert result.report["approved_execution_requested"] is True
    assert result.report["tdlib_auth_attempted"] is True
    assert result.report["manual_intervention_required"] is True
    assert result.report["runtime_env_read"] is True
    assert result.report["tdlib_parameters_shape_guard"] == {
        "checked": True,
        "valid": True,
        "errors": [],
    }
    assert result.report["tdlib_parameters_semantic_guard"] == {
        "checked": True,
        "valid": True,
        "errors": [],
    }
    assert result.report["runtime_env_values_printed"] is False
    assert result.report["secret_values_printed"] is False
    assert result.report["collector_main_used"] is False
    assert result.report["collector_service_used"] is False
    assert result.report["collector_runtime_used"] is False
    assert "fake-api-hash" not in rendered
    assert "fake-tdlib-key" not in rendered


def test_approved_code_entry_passes_only_prompt_function_and_safe_booleans(tmp_path: Path) -> None:
    calls: list[dict] = []
    sensitive_code = "sensitive-code-for-test"

    def prompt(prompt_text: str) -> str:
        raise AssertionError("wrapper unit test fake runner must own prompt timing")

    async def fake_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        calls.append({"args": args, "kwargs": kwargs})
        return FakeAuthOnlyResult(
            extra_payload={
                "requests_sent_count": 1,
                "auth_request_types_sent": ["checkAuthenticationCode_redacted"],
                "last_auth_request_type": "checkAuthenticationCode_redacted",
                "login_code_prompted": True,
                "login_code_submitted": True,
                "login_code_value_printed": False,
                "login_code_value_stored": False,
            }
        )

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        approved_tdlib_auth_code_entry=True,
        runtime_env_path=tmp_path / "runtime.env",
        real_transport_factory=object,
        runtime_env_reader=lambda path: _approved_env(tmp_path),
        auth_runner=fake_runner,
        login_code_prompt=prompt,
        code_entry_interactive_checker=lambda: True,
    )

    rendered = json.dumps(result.report, sort_keys=True)
    auth_payload = result.report["auth_only_entrypoint_result"]
    assert result.exit_code != 0
    assert len(calls) == 1
    assert calls[0]["kwargs"]["approved_tdlib_auth_code_entry"] is True
    assert calls[0]["kwargs"]["login_code_prompt"] is prompt
    assert calls[0]["kwargs"]["login_code_entry_is_interactive"]() is True
    assert result.report["approved_tdlib_auth_code_entry_requested"] is True
    assert result.report["login_code_prompted"] is True
    assert result.report["login_code_submitted"] is True
    assert result.report["login_code_value_printed"] is False
    assert result.report["login_code_value_stored"] is False
    assert auth_payload["auth_request_types_sent"] == ["checkAuthenticationCode_redacted"]
    assert auth_payload["last_auth_request_type"] == "checkAuthenticationCode_redacted"
    assert "code" not in auth_payload
    assert sensitive_code not in rendered


def test_approved_mode_passes_custom_receive_budget_to_auth_runner(tmp_path: Path) -> None:
    calls: list[dict] = []

    async def fake_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        calls.append({"args": args, "kwargs": kwargs})
        return FakeAuthOnlyResult(
            extra_payload={
                "receive_timeout_sec": kwargs["receive_timeout_sec"],
                "max_authorization_updates": kwargs["max_authorization_updates"],
            }
        )

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        runtime_env_path=tmp_path / "runtime.env",
        tdlib_auth_receive_timeout_sec="2.5",
        tdlib_auth_max_authorization_updates="200",
        real_transport_factory=object,
        runtime_env_reader=lambda path: _approved_env(tmp_path),
        auth_runner=fake_runner,
    )

    auth_payload = result.report["auth_only_entrypoint_result"]
    assert result.exit_code != 0
    assert len(calls) == 1
    assert calls[0]["kwargs"]["receive_timeout_sec"] == 2.5
    assert calls[0]["kwargs"]["max_authorization_updates"] == 200
    assert result.report["tdlib_auth_receive_timeout_sec"] == 2.5
    assert result.report["tdlib_auth_max_authorization_updates"] == 200
    assert result.report["tdlib_auth_receive_budget_seconds"] == 500.0
    assert auth_payload["receive_timeout_sec"] == 2.5
    assert auth_payload["max_authorization_updates"] == 200
    assert result.report["contract_status"] == "manual_intervention_required"
    assert result.report["runtime_env_values_printed"] is False
    assert result.report["secret_values_printed"] is False


def test_invalid_receive_timeout_blocks_before_transport_or_runner(tmp_path: Path) -> None:
    _assert_invalid_budget_blocks_without_runtime_or_secret_leak(
        tmp_path=tmp_path,
        receive_timeout_sec=0.0,
        max_authorization_updates=120,
        expected_receive_timeout_sec=0.0,
        expected_max_authorization_updates=120,
        expected_receive_budget_seconds=0.0,
    )


def test_invalid_max_authorization_updates_blocks_before_transport_or_runner(tmp_path: Path) -> None:
    _assert_invalid_budget_blocks_without_runtime_or_secret_leak(
        tmp_path=tmp_path,
        receive_timeout_sec=1.0,
        max_authorization_updates=0,
        expected_receive_timeout_sec=1.0,
        expected_max_authorization_updates=0,
        expected_receive_budget_seconds=0.0,
    )


def test_total_receive_budget_over_limit_blocks_before_transport_or_runner(tmp_path: Path) -> None:
    _assert_invalid_budget_blocks_without_runtime_or_secret_leak(
        tmp_path=tmp_path,
        receive_timeout_sec=2.0,
        max_authorization_updates=301,
        expected_receive_timeout_sec=2.0,
        expected_max_authorization_updates=301,
        expected_receive_budget_seconds=602.0,
    )


def test_receive_budget_parse_invalid_cases_block_before_runtime_transport_or_runner(
    tmp_path: Path,
) -> None:
    cases = (
        ("not-a-number", 120, None, 120, None, ("not-a-number",)),
        (1.0, "not-an-int", 1.0, None, None, ("not-an-int",)),
        ("NaN", 120, None, 120, None, ("NaN",)),
        ("Infinity", 120, None, 120, None, ("Infinity",)),
        (0.0, 120, 0.0, 120, 0.0, ()),
        (5.1, 120, 5.1, 120, 612.0, ()),
        (1.0, 0, 1.0, 0, 0.0, ()),
        (1.0, 601, 1.0, 601, 601.0, ()),
        (5.0, 121, 5.0, 121, 605.0, ()),
    )

    for (
        receive_timeout_sec,
        max_authorization_updates,
        expected_receive_timeout_sec,
        expected_max_authorization_updates,
        expected_receive_budget_seconds,
        forbidden_fragments,
    ) in cases:
        _assert_invalid_budget_blocks_without_runtime_or_secret_leak(
            tmp_path=tmp_path,
            receive_timeout_sec=receive_timeout_sec,
            max_authorization_updates=max_authorization_updates,
            expected_receive_timeout_sec=expected_receive_timeout_sec,
            expected_max_authorization_updates=expected_max_authorization_updates,
            expected_receive_budget_seconds=expected_receive_budget_seconds,
            forbidden_fragments=forbidden_fragments,
        )


def test_cli_invalid_receive_budget_values_return_wrapper_json_without_parser_stderr() -> None:
    cases = (
        (
            "--tdlib-auth-receive-timeout-sec",
            "not-a-number",
            "not-a-number",
        ),
        (
            "--tdlib-auth-max-authorization-updates",
            "not-an-int",
            "not-an-int",
        ),
        (
            "--tdlib-auth-receive-timeout-sec",
            "NaN",
            "NaN",
        ),
        (
            "--tdlib-auth-receive-timeout-sec",
            "Infinity",
            "Infinity",
        ),
    )

    for option, value, forbidden_fragment in cases:
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--format",
                "json",
                "--approved-tdlib-auth-operator-execution",
                "--runtime-env-path",
                str(ROOT / "missing-runtime.env"),
                option,
                value,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )

        assert result.returncode != 0
        assert result.stderr == ""
        report = json.loads(result.stdout)
        rendered = json.dumps(report, allow_nan=False, sort_keys=True)
        assert report["contract_status"] == "blocked_tdlib_auth_receive_budget_invalid"
        assert report["blocked_reason"] == "tdlib_auth_receive_budget_invalid"
        assert "tdlib_auth_receive_budget.invalid" in report["checks_failed"]
        assert report["tdlib_auth_attempted"] is False
        assert report["runtime_env_read"] is False
        assert report["runtime_env_values_printed"] is False
        assert report["secret_values_printed"] is False
        assert "argument --tdlib-auth" not in result.stderr
        assert forbidden_fragment not in rendered


def test_wrapper_passes_through_auth_progress_fields_without_secret_payload(tmp_path: Path) -> None:
    async def fake_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        return FakeAuthOnlyResult(
            status="degraded",
            manual_intervention_required=False,
            extra_payload={
                "manual_intervention_reason": None,
                "final_authorization_state": "waiting_phone_number",
                "requests_sent_count": 3,
                "auth_request_types_sent": [
                    "setTdlibParameters",
                    "checkDatabaseEncryptionKey",
                    "setAuthenticationPhoneNumber",
                ],
                "last_auth_request_type": "setAuthenticationPhoneNumber",
                "authorization_updates_seen_count": 3,
                "non_auth_response_count": 3,
                "non_auth_response_type_counts": {
                    "ok": 1,
                    "updateConnectionState": 2,
                },
                "tdlib_ok_seen": True,
                "last_non_auth_response_type": "updateConnectionState",
                "ok_response_count": 1,
                "ok_response_auth_request_types": [
                    "setTdlibParameters",
                ],
                "last_ok_response_auth_request_type": "setTdlibParameters",
                "pending_auth_request_types_at_timeout": [
                    "checkDatabaseEncryptionKey",
                    "setAuthenticationPhoneNumber",
                ],
                "connection_state_updates_seen_count": 2,
                "last_connection_state_type": "unrecognized",
                "connection_state_type_counts": {
                    "connectionStateWaitingForNetwork": 1,
                    "unrecognized": 1,
                },
                "max_authorization_updates": 120,
                "receive_timeout_sec": 1.0,
                "error": "authorization_not_ready",
                "error_present": True,
                "error_type": "completion_failure",
                "tdlib_error_categories": [
                    "timeout_or_no_update_related",
                    "connection_not_ready_before_max_updates",
                ],
                "completion_failure_category": (
                    "waiting_phone_number_request_sent_auth_state_not_advanced_before_max_updates"
                ),
            },
        )

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        runtime_env_path=tmp_path / "runtime.env",
        real_transport_factory=object,
        runtime_env_reader=lambda path: _approved_env(tmp_path),
        auth_runner=fake_runner,
    )

    rendered = json.dumps(result.report, sort_keys=True)
    auth_payload = result.report["auth_only_entrypoint_result"]
    assert result.exit_code != 0
    assert result.report["contract_status"] == "auth_only_entrypoint_not_completed"
    assert auth_payload["authorization_updates_seen_count"] == 3
    assert auth_payload["non_auth_response_count"] == 3
    assert auth_payload["non_auth_response_type_counts"] == {
        "ok": 1,
        "updateConnectionState": 2,
    }
    assert auth_payload["tdlib_ok_seen"] is True
    assert auth_payload["last_non_auth_response_type"] == "updateConnectionState"
    assert auth_payload["ok_response_count"] == 1
    assert auth_payload["ok_response_auth_request_types"] == [
        "setTdlibParameters",
    ]
    assert auth_payload["last_ok_response_auth_request_type"] == "setTdlibParameters"
    assert auth_payload["pending_auth_request_types_at_timeout"] == [
        "checkDatabaseEncryptionKey",
        "setAuthenticationPhoneNumber",
    ]
    assert auth_payload["auth_request_types_sent"] == [
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "setAuthenticationPhoneNumber",
    ]
    assert auth_payload["last_auth_request_type"] == "setAuthenticationPhoneNumber"
    assert auth_payload["connection_state_updates_seen_count"] == 2
    assert auth_payload["last_connection_state_type"] == "unrecognized"
    assert auth_payload["connection_state_type_counts"] == {
        "connectionStateWaitingForNetwork": 1,
        "unrecognized": 1,
    }
    assert auth_payload["tdlib_error_categories"] == [
        "timeout_or_no_update_related",
        "connection_not_ready_before_max_updates",
    ]
    assert auth_payload["max_authorization_updates"] == 120
    assert auth_payload["receive_timeout_sec"] == 1.0
    assert (
        auth_payload["completion_failure_category"]
        == "waiting_phone_number_request_sent_auth_state_not_advanced_before_max_updates"
    )
    assert "fake-api-hash" not in rendered
    assert "fake-tdlib-key" not in rendered
    assert "+10000000000" not in rendered
    assert "postgresql://collector:secret" not in rendered


def test_wrapper_preserves_redacted_auth_error_classification_without_raw_message(tmp_path: Path) -> None:
    raw_message = (
        "setTdlibParameters rejected api_hash=fake-api-hash-secret "
        "TDLIB_DB_ENCRYPTION_KEY=fake-tdlib-key-secret "
        "TELEGRAM_PHONE_NUMBER=+15555550123 LOGIN_CODE=12345"
    )

    async def fake_runner(*args, **kwargs) -> FakeAuthOnlyResult:
        return FakeAuthOnlyResult(
            status="degraded",
            manual_intervention_required=False,
            extra_payload={
                "manual_intervention_reason": None,
                "final_authorization_state": "waiting_tdlib_parameters",
                "requests_sent_count": 1,
                "error": "tdlib_error_redacted",
                "error_present": True,
                "error_type": "tdlib_error",
                "tdlib_error_present": True,
                "tdlib_error_code": 400,
                "tdlib_error_type": "error",
                "tdlib_error_message_len": len(raw_message),
                "tdlib_error_categories": [
                    "api_hash_related",
                    "encryption_key_related",
                    "tdlib_parameters_related",
                ],
                "completion_failure_category": "api_hash_related",
            },
        )

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        runtime_env_path=tmp_path / "runtime.env",
        real_transport_factory=object,
        runtime_env_reader=lambda path: _approved_env(tmp_path),
        auth_runner=fake_runner,
    )

    rendered = json.dumps(result.report, sort_keys=True)
    auth_payload = result.report["auth_only_entrypoint_result"]
    assert result.exit_code != 0
    assert result.report["contract_status"] == "auth_only_entrypoint_not_completed"
    assert auth_payload["error"] == "tdlib_error_redacted"
    assert auth_payload["error_present"] is True
    assert auth_payload["error_type"] == "tdlib_error"
    assert auth_payload["tdlib_error_present"] is True
    assert auth_payload["tdlib_error_code"] == 400
    assert auth_payload["tdlib_error_type"] == "error"
    assert auth_payload["tdlib_error_message_len"] == len(raw_message)
    assert auth_payload["tdlib_error_categories"] == [
        "api_hash_related",
        "encryption_key_related",
        "tdlib_parameters_related",
    ]
    assert auth_payload["completion_failure_category"] == "api_hash_related"
    assert raw_message not in rendered
    assert "fake-api-hash-secret" not in rendered
    assert "fake-tdlib-key-secret" not in rendered
    assert "+15555550123" not in rendered
    assert "12345" not in rendered


def test_wrapper_imports_auth_entrypoint_only_for_approved_execution() -> None:
    script_text = SCRIPT.read_text(encoding="utf-8")

    assert "from src.services.collector_telegram import auth_entrypoint" in script_text
    assert "collector_telegram.main" not in script_text
    assert "collector_telegram.service" not in script_text
    assert "collector_telegram.runtime" not in script_text
    assert "TDLibClient(" not in script_text
    assert "CollectorTelegramService(" not in script_text
    assert "CollectorRuntime(" not in script_text


def test_runbook_exists_and_preserves_non_authorizations() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    normalized = " ".join(text.split())

    for heading in REQUIRED_RUNBOOK_HEADINGS:
        assert text.count(heading) == 1

    required_markers = (
        "This slice does not execute TDLib auth.",
        "This wrapper is not approval to run TDLib auth.",
        "No Telegram connection occurs in this slice.",
        "No live collector start occurs.",
        "No notifier transport or rollout occurs.",
        "This wrapper does not read `/etc/github-ai-catchbot/runtime.env`.",
        "The current auth-only entrypoint is `src.services.collector_telegram.auth_entrypoint`.",
        "Actual operator execution still requires separate explicit approval.",
        "Telegram login code and 2FA prompt values must never be pasted into ChatGPT, Codex, GitHub, repo files, markdown, terminal history, or review bundles.",
    )
    for marker in required_markers:
        assert " ".join(marker.split()) in normalized


def test_unsafe_command_secret_and_login_code_patterns_are_absent() -> None:
    text = "\n".join(
        [
            RUNBOOK.read_text(encoding="utf-8"),
            SCRIPT.read_text(encoding="utf-8"),
        ]
    )

    for name, pattern in FORBIDDEN_PATTERNS.items():
        assert not pattern.search(text), name


def test_collector_main_is_not_treated_as_auth_only_when_runtime_bound() -> None:
    report = _module().generate_report(ROOT).report

    assert report["entrypoint_assessment"]["collector_main_runtime_entrypoint"] is True
    assert report["auth_only_entrypoint_status"] == "available"
    assert report["selected_entrypoint"] != "src.services.collector_telegram.main"
    assert report["entrypoint_assessment"]["auth_entrypoint_imports_runtime"] is False
