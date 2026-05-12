from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_app_runtime_import_config_preflight_result_record.md"
)
PARENT_RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_app_runtime_import_config_preflight.md"
)

REQUIRED_SECTIONS = (
    "# Dedicated VPS app runtime import/config preflight result record",
    "## Scope",
    "## Source execution being recorded",
    "## Result summary",
    "## Import/config surface facts",
    "## Deferred loader facts",
    "## Side-effect denials",
    "## Redaction guarantees",
    "## Non-authorizations",
    "## Next bounded slice",
    "## Anti-overconservatism check",
)

SIDE_EFFECT_FALSE_FLAGS = (
    "database_connected",
    "redis_connected",
    "db_write_performed",
    "redis_mutation_performed",
    "alembic_run",
    "app_runtime_started",
    "tdlib_auth_performed",
    "telegram_connected",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "docker_used",
    "systemd_modified",
    "migration_files_modified",
)

NON_AUTHORIZATIONS = (
    "does not authorize runtime start",
    "does not authorize service readiness",
    "does not authorize TDLib auth",
    "does not authorize Telegram connection",
    "does not authorize live collector startup",
    "does not authorize notifier transport",
    "does not authorize production rollout",
    "does not authorize DB or Redis mutation",
    "does not authorize Alembic",
    "does not authorize Docker or systemd",
)

FORBIDDEN_ASSIGNMENT_KEYS = (
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "GITHUB_PRIVATE_KEY",
    "X_BEARER_TOKEN",
    "TELEGRAM_API_HASH",
    "TDLIB_DB_ENCRYPTION_KEY",
)


def _text() -> str:
    return RESULT_RECORD.read_text(encoding="utf-8")


def _normalized_text() -> str:
    return " ".join(_text().split())


def test_result_record_exists_and_has_required_sections_once() -> None:
    assert RESULT_RECORD.exists()
    text = _text()

    for section in REQUIRED_SECTIONS:
        assert text.count(section) == 1, section


def test_result_record_captures_pass_status_and_empty_failures() -> None:
    text = _text()

    for phrase in (
        "contract_status: passed",
        "process_status: 0",
        "checks_failed: []",
        "failures: []",
    ):
        assert phrase in text, phrase


def test_result_record_captures_warnings_and_schema_version() -> None:
    text = _text()

    assert "Secret-bound config loaders were deferred by design." in text
    assert "Runtime-bound config loaders were deferred by design." in text
    assert "schema_version: dedicated_vps_app_runtime_import_config_preflight_v1" in text


def test_runtime_env_read_is_limited_to_approved_operator_execution() -> None:
    text = _text()
    normalized = _normalized_text()

    assert "runtime_env_read: true" in text
    assert "applies only to the approved operator runner execution" in normalized
    assert "This repository result-record slice did not read `/etc/github-ai-catchbot/runtime.env`" in normalized
    assert "does not read the runtime env file" in normalized
    assert "does not read the `/tmp` JSON output file" in normalized


def test_result_record_captures_redacted_env_posture() -> None:
    text = _text()

    for phrase in (
        "runtime_env_values_printed: false",
        "secret_values_printed: false",
        "process_env_inspected: false",
        "runtime_env_key_count: 8",
        "app_env_seen: prod",
    ):
        assert phrase in text, phrase


def test_result_record_captures_import_and_config_booleans() -> None:
    text = _text()

    for phrase in (
        "import_surface_attempted: true",
        "app_imports_attempted: true",
        "config_surface_attempted: true",
        "import_surface_passed: true",
        "safe_config_surface_passed: true",
    ):
        assert phrase in text, phrase


def test_result_record_captures_deferred_loader_booleans() -> None:
    text = _text()

    for phrase in (
        "secret_bound_config_loaders_deferred: true",
        "runtime_bound_config_loaders_deferred: true",
    ):
        assert phrase in text, phrase


def test_result_record_captures_all_side_effect_flags_as_false() -> None:
    text = _text()
    normalized = _normalized_text()

    for flag in SIDE_EFFECT_FALSE_FLAGS:
        assert f"{flag}: false" in text, flag

    for phrase in (
        "No PostgreSQL connection occurred",
        "No Redis connection occurred",
        "No DB write occurred",
        "No Redis mutation occurred",
        "No Alembic run occurred",
        "No app runtime started",
        "No TDLib auth occurred",
        "No Telegram connection occurred",
        "No live collector started",
        "No notifier transport was enabled",
        "No Docker was used",
        "No systemd was modified",
        "No migration files were modified",
        "No production rollout occurred",
    ):
        assert phrase in normalized, phrase


def test_result_record_captures_import_and_config_counts() -> None:
    text = _text()

    for phrase in (
        "import_result_count: 152",
        "'import_ok': 13",
        "'skipped_forbidden_runtime_surface': 139",
        "config_result_count: 13",
        "'config_loader_deferred_runtime_bound': 6",
        "'config_loader_deferred_secret_bound': 5",
        "'config_loader_ok': 2",
        "13 config modules imported successfully",
        "139 forbidden runtime, client, service, worker",
        "2 safe config loaders succeeded",
        "5 secret-bound config loaders deferred",
        "6 runtime-bound config loaders deferred",
    ):
        assert phrase in text, phrase


def test_result_record_states_redaction_guarantees() -> None:
    text = _text()

    for phrase in (
        "Runtime env values are not recorded.",
        "Runtime env values were not printed by the approved runner.",
        "Secret values are not recorded.",
        "Secret values were not printed by the approved runner.",
        "Full DB and Redis URL values are not recorded.",
        "DB credential material is not recorded.",
        "Redis credential material is not recorded.",
        "Telegram, OpenAI, GitHub, X, and TDLib credential material is not recorded.",
        "Raw public VPS IP values are not recorded.",
        "Raw operator IP values are not recorded.",
        "Runtime env file contents are not recorded.",
        "Secret file contents are not recorded.",
    ):
        assert phrase in text, phrase


def test_result_record_states_non_authorizations() -> None:
    text = _text()

    for phrase in NON_AUTHORIZATIONS:
        assert phrase in text, phrase


def test_result_record_points_to_next_bounded_slice_only() -> None:
    text = _text()

    assert "dedicated_vps_telegram_credentials_acquisition_plan" in text
    assert "Do not jump directly to TDLib auth" in text
    assert "Telegram connection" in text
    assert "live collector startup" in text
    assert "notifier transport" in text
    assert "production rollout" in text


def test_result_record_rejects_forbidden_raw_url_secret_and_ip_patterns() -> None:
    text = _text()

    forbidden_patterns = {
        "postgresql_psycopg_url": r"postgresql\+psycopg://",
        "redis_url": r"redis://",
        "raw_url_credentials": r"[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^@\s]+@",
        "non_loopback_ipv4": r"\b(?!127\.0\.0\.1\b)(?:\d{1,3}\.){3}\d{1,3}\b",
    }

    for name, pattern in forbidden_patterns.items():
        assert re.search(pattern, text) is None, name

    for key in FORBIDDEN_ASSIGNMENT_KEYS:
        assert re.search(rf"\b{re.escape(key)}\s*=", text) is None, key


def test_parent_runbook_points_narrowly_to_result_record_and_next_slice() -> None:
    text = PARENT_RUNBOOK.read_text(encoding="utf-8")
    normalized = " ".join(text.split())

    assert "## Completed redacted result record" in text
    assert "dedicated_vps_app_runtime_import_config_preflight_result_record.md" in text
    assert "dedicated_vps_telegram_credentials_acquisition_plan" in text
    assert "does not authorize TDLib auth, Telegram connection, live collector" in normalized
    assert "notifier transport, or production rollout" in normalized
