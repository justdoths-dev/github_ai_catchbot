from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.py"
)
RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan.md"
)

SAFETY_BOOLEAN_KEYS = (
    "auth_wrapper_executed",
    "tdlib_auth_attempted",
    "tdlib_auth_completed",
    "telegram_connected",
    "session_state_created_or_reused",
    "manual_intervention_required",
    "runtime_env_modified",
    "runtime_env_values_printed",
    "secret_values_printed",
    "telegram_login_code_or_2fa_requested",
    "collector_main_used",
    "collector_service_used",
    "collector_runtime_used",
    "live_collector_started",
    "app_runtime_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "database_connected",
    "redis_connected",
    "alembic_run",
    "docker_or_systemd_changed",
    "source_build_attempted",
    "package_manager_mutation_attempted",
)

FORBIDDEN_OUTPUT_PATTERNS = {
    "telegram_bot_token": re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b"),
    "telegram_api_hash_assignment": re.compile(
        r"\bTELEGRAM_API_HASH\s*[:=]\s*[0-9a-fA-F]{32}\b"
    ),
    "telegram_phone_assignment": re.compile(
        r"\bTELEGRAM_PHONE_NUMBER\s*[:=]\s*\+?\d[\d\s().-]{6,}"
    ),
    "telegram_login_code_assignment": re.compile(
        r"\b(?:TELEGRAM_LOGIN_CODE|LOGIN_CODE|AUTH_CODE)\s*[:=]\s*\S+",
        re.IGNORECASE,
    ),
    "two_factor_or_password_assignment": re.compile(
        r"\b(?:TELEGRAM_2FA_PASSWORD|TWO_FACTOR_PASSWORD|2FA_PASSWORD|PASSWORD)"
        r"\s*[:=]\s*\S+",
        re.IGNORECASE,
    ),
    "postgresql_url": re.compile(r"\bpostgresql(?:\+psycopg)?://", re.IGNORECASE),
    "redis_url": re.compile(r"\bredis://", re.IGNORECASE),
    "private_invite_link": re.compile(
        r"https?://(?:t|telegram)\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+",
        re.IGNORECASE,
    ),
}

FORBIDDEN_COMMAND_PATTERNS = {
    "cat_runtime_env": re.compile(r"cat /etc/github-ai-catchbot/runtime\.env"),
    "echo_secret": re.compile(r"echo\s+.*secret", re.IGNORECASE),
    "print_env_values": re.compile(r"print\s+env\s+values", re.IGNORECASE),
}


def _module():
    from scripts.ops import (
        dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan as module,
    )

    return module


def _runtime_env_text(**overrides: str | None) -> str:
    values: dict[str, str] = {
        "APP_ENV": "prod",
        "COLLECTOR_MODE": "live",
        "DATABASE_URL": (
            "postgresql+psycopg://github_ai_catchbot_app:"
            "fixture-db-password@localhost:5432/github_ai_catchbot"
        ),
        "REDIS_URL": "redis://:fixture-redis-password@localhost:6379/0",
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_API_HASH": "a" * 32,
        "TELEGRAM_PHONE_NUMBER": "+15555550123",
        "TELEGRAM_2FA_PASSWORD": "fixture-2fa-password",
        "TDLIB_STATE_DIR": "/var/lib/catchbot/tdlib",
        "TDLIB_FILES_DIR": "/var/lib/catchbot/tdlib/files",
        "TDLIB_DB_ENCRYPTION_KEY": "fixture-tdlib-key",
        "STARTUP_WARM_BACKFILL_ENABLED": "true",
    }
    for key, value in overrides.items():
        if value is None:
            values.pop(key, None)
        else:
            values[key] = value
    return "\n".join(f"{key}={value}" for key, value in values.items())


def _report(runtime_env_text: str | None = None, **kwargs: object) -> dict[str, object]:
    return _module().generate_report(
        repo_root=ROOT,
        runtime_env_text=runtime_env_text,
        **kwargs,
    )


def _key_check(report: dict[str, object], key: str) -> dict[str, object]:
    checks = report["redacted_key_checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        if check["key"] == key:
            return check
    raise AssertionError(key)


def _rendered(report: dict[str, object]) -> str:
    return json.dumps(report, sort_keys=True)


def test_runtime_env_path_omitted_blocks_read_and_prints_no_values() -> None:
    report = _report()
    rendered = _rendered(report)

    assert report["contract_status"] == "runtime_env_read_blocked"
    assert report["recommended_next_slice"] == "defer_manual_review"
    inspection = report["runtime_env_inspection"]
    assert isinstance(inspection, dict)
    assert inspection["runtime_env_path_provided"] is False
    assert inspection["runtime_env_read"] is False
    assert inspection["runtime_env_values_printed"] is False
    assert inspection["secret_values_printed"] is False
    assert "fixture-db-password" not in rendered


def test_missing_runtime_env_path_reports_path_missing(tmp_path: Path) -> None:
    report = _module().generate_report(
        repo_root=ROOT,
        runtime_env_path=tmp_path / "missing-runtime.env",
    )

    assert report["contract_status"] == "runtime_env_path_missing"
    assert report["recommended_next_slice"] == "defer_manual_review"
    inspection = report["runtime_env_inspection"]
    assert inspection["runtime_env_path_provided"] is True
    assert inspection["runtime_env_read"] is False


def test_missing_required_keys_reports_key_names_only() -> None:
    report = _report(_runtime_env_text(TELEGRAM_API_HASH=None, TDLIB_DB_ENCRYPTION_KEY=None))
    rendered = _rendered(report)

    assert report["contract_status"] == "runtime_env_invalid_diagnostic_ready"
    assert report["recommended_next_slice"] == "tdlib_auth_runtime_env_operator_fix_plan"
    assert _key_check(report, "TELEGRAM_API_HASH")["issue_code"] == "missing_required_key"
    assert _key_check(report, "TDLIB_DB_ENCRYPTION_KEY")["issue_code"] == "missing_required_key"
    plan_keys = {item["key"] for item in report["redacted_fix_plan"]}
    assert {"TELEGRAM_API_HASH", "TDLIB_DB_ENCRYPTION_KEY"}.issubset(plan_keys)
    assert "fixture-tdlib-key" not in rendered
    assert "a" * 32 not in rendered


def test_empty_required_key_reports_empty_required_key_without_raw_value() -> None:
    report = _report(_runtime_env_text(TELEGRAM_API_HASH=""))

    check = _key_check(report, "TELEGRAM_API_HASH")
    assert check["present"] is True
    assert check["empty"] is True
    assert check["value_class"] == "empty"
    assert check["issue_code"] == "empty_required_key"
    assert "TELEGRAM_API_HASH=" not in _rendered(report)


def test_invalid_integer_boolean_and_path_like_shapes_are_redacted() -> None:
    report = _report(
        _runtime_env_text(
            TELEGRAM_API_ID="not-an-integer",
            STARTUP_WARM_BACKFILL_ENABLED="maybe",
            TDLIB_STATE_DIR="relative/tdlib",
        )
    )

    assert _key_check(report, "TELEGRAM_API_ID")["issue_code"] == "invalid_integer"
    assert (
        _key_check(report, "STARTUP_WARM_BACKFILL_ENABLED")["issue_code"]
        == "invalid_boolean"
    )
    assert _key_check(report, "TDLIB_STATE_DIR")["issue_code"] == "invalid_path_format"
    assert "not-an-integer" not in _rendered(report)
    assert "relative/tdlib" not in _rendered(report)


def test_duplicate_keys_are_reported_without_values() -> None:
    text = _runtime_env_text() + "\nTELEGRAM_API_ID=67890\n"
    report = _report(text)

    inspection = report["runtime_env_inspection"]
    assert inspection["duplicate_key_names"] == ["TELEGRAM_API_ID"]
    assert any(
        item["action_type"] == "remove_duplicate_key" and item["key"] == "TELEGRAM_API_ID"
        for item in report["redacted_fix_plan"]
    )
    assert "67890" not in _rendered(report)


def test_malformed_lines_are_counted_without_values() -> None:
    text = _runtime_env_text() + "\nthis line is malformed\n1INVALID=value\n"
    report = _report(text)

    inspection = report["runtime_env_inspection"]
    assert inspection["malformed_line_count"] == 2
    assert "malformed_line_count: 2" in report["diagnostic_reasons"]
    assert "this line is malformed" not in _rendered(report)


def test_collector_config_build_exception_is_redacted() -> None:
    def failing_builder(_values: object, _checks: object) -> object:
        raise ValueError(
            "do not leak TELEGRAM_API_HASH="
            + ("b" * 32)
            + " or postgresql://user:pass@localhost/db"
        )

    report = _report(
        _runtime_env_text(),
        config_build_checker=failing_builder,
    )
    rendered = _rendered(report)
    build_check = report["collector_config_build_check"]

    assert build_check["attempted"] is True
    assert build_check["status"] == "build_invalid_redacted"
    assert build_check["exception_class"] == "ValueError"
    assert build_check["raw_exception_message_included"] is False
    assert "do not leak" not in rendered
    assert "TELEGRAM_API_HASH=" not in rendered
    assert "postgresql://user:pass" not in rendered


def test_output_contains_all_safety_booleans_and_they_remain_false() -> None:
    report = _report(_runtime_env_text())

    assert report["boundary_check"] == "pass"
    for key in SAFETY_BOOLEAN_KEYS:
        assert key in report
        assert report[key] is False


def test_source_does_not_read_runtime_env_without_explicit_path() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_RUNTIME_ENV_PATH" not in text
    assert 'default="/etc/github-ai-catchbot/runtime.env"' not in text
    assert "runtime_env_path is None" in text
    assert "_read_runtime_env_file" in text


def test_source_and_tests_do_not_call_auth_wrapper() -> None:
    combined = "\n".join(
        [
            SCRIPT.read_text(encoding="utf-8"),
            Path(__file__).read_text(encoding="utf-8"),
        ]
    )

    approved_auth_flag = "--approved-tdlib-" + "auth-operator-execution"
    auth_wrapper_script = (
        "dedicated_vps_tdlib_" + "auth_operator_execution_wrapper.py"
    )

    assert approved_auth_flag not in combined
    assert auth_wrapper_script not in combined
    assert "auth_wrapper_executed\": True" not in combined


def test_source_and_tests_do_not_import_forbidden_runtime_modules() -> None:
    imported: list[str] = []
    for path in (SCRIPT, Path(__file__)):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)

    forbidden_fragments = (
        "collector_telegram.main",
        "collector_telegram.service",
        "collector_telegram.runtime",
        "notifier",
        "database",
        "redis",
        "alembic",
        "docker",
        "systemd",
    )
    assert not [
        name
        for name in imported
        if any(fragment in name.lower() for fragment in forbidden_fragments)
    ]


def test_source_runbook_and_tests_do_not_include_forbidden_commands() -> None:
    combined = "\n".join(
        [
            SCRIPT.read_text(encoding="utf-8"),
            RUNBOOK.read_text(encoding="utf-8"),
            Path(__file__).read_text(encoding="utf-8"),
        ]
    )

    for name, pattern in FORBIDDEN_COMMAND_PATTERNS.items():
        assert not pattern.search(combined), name


def test_secret_like_patterns_are_absent_from_output(tmp_path: Path) -> None:
    token = "123456789:" + ("A" * 35)
    private_invite = "https://t.me/+" + ("A" * 20)
    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text(
        _runtime_env_text(
            TELEGRAM_BOT_TOKEN=token,
            PRIVATE_INVITE_LINK=private_invite,
            TELEGRAM_LOGIN_CODE="12345",
            TELEGRAM_2FA_PASSWORD="fixture-password",
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(runtime_env),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0
    report = json.loads(result.stdout)
    rendered = json.dumps(report, ensure_ascii=False)

    for name, pattern in FORBIDDEN_OUTPUT_PATTERNS.items():
        assert pattern.search(rendered) is None, name
    assert token not in rendered
    assert private_invite not in rendered
    assert "fixture-password" not in rendered


def test_cli_emits_valid_json(tmp_path: Path) -> None:
    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text(_runtime_env_text(), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(runtime_env),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["schema_version"] == (
        "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan_v1"
    )
    assert data["contract_name"] == (
        "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan"
    )
    assert data["contract_status"] in {
        "runtime_env_invalid_diagnostic_ready",
        "runtime_env_invalid_diagnostic_inconclusive",
        "runtime_env_shape_appears_valid",
        "runtime_env_path_missing",
        "runtime_env_read_blocked",
    }
    assert data["recommended_next_slice"] in {
        "tdlib_auth_runtime_env_operator_fix_plan",
        "tdlib_auth_operator_execution_rerun_after_fix",
        "defer_manual_review",
    }
