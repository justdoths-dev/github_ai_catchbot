from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path


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
    "auth_only_entrypoint_status",
    "selected_entrypoint",
    "blocked_reason",
    "runtime_env_path",
    "runtime_env_read",
    "runtime_env_values_printed",
    "secret_values_printed",
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
    ) -> None:
        self.status = status
        self.attempted = attempted
        self.completed = completed
        self.manual_intervention_required = manual_intervention_required

    def to_redacted_dict(self) -> dict:
        return {
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
            "runtime_env_values_printed": False,
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
        }


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
    for key in SIDE_EFFECT_BOOLEAN_KEYS:
        assert report[key] is False


def test_wrapper_output_includes_required_shape() -> None:
    report = _module().generate_report(ROOT).report

    for key in REQUIRED_REPORT_KEYS:
        assert key in report


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
    assert report["contract_status"] == "blocked_real_transport_missing"
    assert report["blocked_reason"] == "blocked_real_transport_missing"
    assert report["runtime_env_read"] is False
    assert report["tdlib_auth_attempted"] is False


def test_approved_mode_blocks_when_real_transport_missing_without_runtime_env_read() -> None:
    def missing_transport() -> object:
        raise RuntimeError("tdjson unavailable")

    def forbidden_env_reader(path: str | Path) -> dict[str, str]:
        raise AssertionError("runtime.env must not be read when real transport is missing")

    result = _module().generate_report(
        ROOT,
        approved_tdlib_auth_operator_execution=True,
        real_transport_factory=missing_transport,
        runtime_env_reader=forbidden_env_reader,
    )

    assert result.exit_code != 0
    assert result.report["contract_status"] == "blocked_real_transport_missing"
    assert result.report["blocked_reason"] == "blocked_real_transport_missing"
    assert result.report["tdlib_auth_attempted"] is False
    assert result.report["runtime_env_read"] is False


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
    assert result.report["contract_status"] == "manual_intervention_required"
    assert result.report["auth_only_entrypoint_status"] == "manual_intervention_required"
    assert result.report["selected_entrypoint"] == "src.services.collector_telegram.auth_entrypoint"
    assert result.report["approved_execution_requested"] is True
    assert result.report["tdlib_auth_attempted"] is True
    assert result.report["manual_intervention_required"] is True
    assert result.report["runtime_env_read"] is True
    assert result.report["runtime_env_values_printed"] is False
    assert result.report["secret_values_printed"] is False
    assert result.report["collector_main_used"] is False
    assert result.report["collector_service_used"] is False
    assert result.report["collector_runtime_used"] is False
    assert "fake-api-hash" not in rendered
    assert "fake-tdlib-key" not in rendered


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
