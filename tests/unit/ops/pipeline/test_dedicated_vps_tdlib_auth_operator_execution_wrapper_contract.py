from __future__ import annotations

import json
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
    "runtime_env_path",
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
    "postgresql_url": re.compile(r"postgresql://"),
    "postgresql_psycopg_url": re.compile(r"postgresql\+psycopg://"),
    "redis_url": re.compile(r"redis://"),
    "echo_secret_append": re.compile(r"echo .*TELEGRAM_.*>>", re.IGNORECASE),
    "tee_append_runtime_env": re.compile(r"tee -a /etc/github-ai-catchbot/runtime\.env"),
    "ipv4_literal": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
}


def _module():
    from scripts.ops import dedicated_vps_tdlib_auth_operator_execution_wrapper as module

    return module


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

    assert report["auth_only_entrypoint_status"] in {"available", "missing", "ambiguous"}
    if report["auth_only_entrypoint_status"] in {"missing", "ambiguous"}:
        assert report["contract_status"] == "blocked"
        assert report["likely_next_slice"] == "dedicated_vps_tdlib_auth_entrypoint_implementation"
        assert "auth_only_entrypoint.missing" in report["checks_failed"]
    else:
        assert report["contract_status"] == "approval_required"
        assert report["likely_next_slice"] == "dedicated_vps_tdlib_auth_operator_execution"
        assert report["selected_entrypoint"]


def test_tests_do_not_call_approved_execution_flag() -> None:
    text = Path(__file__).read_text(encoding="utf-8")
    approval_flag = "--approved-tdlib-auth-" + "operator-execution"

    assert approval_flag not in text


def test_wrapper_does_not_import_runtime_or_instantiate_tdlib_client() -> None:
    script_text = SCRIPT.read_text(encoding="utf-8")

    assert "from src.services.collector_telegram" not in script_text
    assert "import src.services.collector_telegram" not in script_text
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
        "If no auth-only entrypoint exists, the next slice must implement one rather than misusing collector runtime main.",
        "If a future auth-only entrypoint exists, actual operator execution still requires separate explicit approval.",
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
    if report["auth_only_entrypoint_status"] != "available":
        assert report["selected_entrypoint"] is None
