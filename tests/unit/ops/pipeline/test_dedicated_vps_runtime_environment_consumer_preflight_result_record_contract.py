from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_runtime_environment_consumer_preflight_result_record.md"
)
PARENT_RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_runtime_environment_consumer_preflight.md"
)

REQUIRED_KEYS = (
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "ENABLE_NOTIFICATION_SEND",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "ENABLE_REPLAY_TO_PROD_DB",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
)

FEATURE_FLAG_FACTS = (
    "ENABLE_NOTIFICATION_SEND: false",
    "NOTIFIER_TELEGRAM_DRY_RUN: false",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS: true",
    "ENABLE_REPLAY_TO_PROD_DB: false",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION: false",
)

CONSUMER_PROFILE_FACTS = (
    "database_consumers_ready: true",
    "redis_consumers_ready: true",
    "notification_transport_disabled: true",
    "replay_to_prod_disabled: true",
    "maintenance_retry_promotion_disabled: true",
    "runtime_start_authorized: false",
    "tdlib_authorized: false",
    "telegram_authorized: false",
    "live_collector_authorized: false",
    "notifier_transport_authorized: false",
    "production_rollout_authorized: false",
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

REQUIRED_SECTIONS = (
    "# Dedicated VPS runtime environment consumer preflight result record",
    "## Scope",
    "## Source result being recorded",
    "## Result summary",
    "## Approved operator execution facts",
    "## Redacted key and shape facts",
    "## Feature flag facts",
    "## Safety profile facts",
    "## Consumer readiness facts",
    "## Explicit side-effect denials",
    "## Redaction guarantees",
    "## Non-authorizations",
    "## Next bounded slice",
    "## Anti-overconservatism check",
)


def _text() -> str:
    return RESULT_RECORD.read_text(encoding="utf-8")


def _normalized_text() -> str:
    return " ".join(_text().split())


def _code_blocks(text: str) -> list[str]:
    return re.findall(r"```[a-zA-Z0-9_-]*\n(.*?)```", text, flags=re.DOTALL)


def test_result_record_exists_and_has_required_sections_once() -> None:
    assert RESULT_RECORD.exists()
    text = _text()

    for section in REQUIRED_SECTIONS:
        assert text.count(section) == 1, section


def test_result_record_captures_pass_status_and_empty_failures() -> None:
    text = _text()

    for phrase in (
        "contract_status: passed",
        "checks_failed: []",
        "failures: []",
        "warnings: []",
        "app_env_seen: prod",
        "runtime_env_read: true",
        "runtime_env_values_printed: false",
        "secret_values_printed: false",
        "process_env_inspected: false",
        "required_keys_missing: []",
        "runtime_env_key_count: 8",
    ):
        assert phrase in text, phrase


def test_runtime_env_read_is_limited_to_approved_operator_runner() -> None:
    normalized = _normalized_text()

    assert "approved Python runner read `/etc/github-ai-catchbot/runtime.env`" in normalized
    assert "This repository result-record slice did not read `/etc/github-ai-catchbot/runtime.env`" in normalized
    assert "did not rerun the preflight" in normalized
    assert "does not inspect process env vars" in normalized


def test_result_record_lists_all_required_key_names() -> None:
    text = _text()

    for key in REQUIRED_KEYS:
        assert key in text, key


def test_result_record_captures_database_shape_without_full_url_or_credential() -> None:
    text = _text()

    for phrase in (
        "DATABASE_URL shape metadata:",
        "database_url:",
        "present: true",
        "scheme: postgresql+psycopg",
        "username: github_ai_catchbot_app",
        "host: 127.0.0.1",
        "port: 5432",
        "database: github_ai_catchbot",
        "has_credentials: true",
        "loopback_only: true",
        "full_value_printed: false",
    ):
        assert phrase in text, phrase

    assert "postgresql+psycopg://" not in text


def test_result_record_captures_redis_shape_without_full_url_or_credential() -> None:
    text = _text()

    for phrase in (
        "REDIS_URL shape metadata:",
        "redis_url:",
        "scheme: redis",
        "host: 127.0.0.1",
        "port: 6379",
        "database_index: 0",
        "loopback_only: true",
        "full_value_printed: false",
    ):
        assert phrase in text, phrase

    assert "redis://" not in text


def test_result_record_captures_feature_flags_exactly() -> None:
    text = _text()

    for phrase in FEATURE_FLAG_FACTS:
        assert phrase in text, phrase
    assert "`NOTIFIER_TELEGRAM_DRY_RUN=false` is acceptable" in text
    assert "`ENABLE_NOTIFICATION_SEND=false` is the actual transport blocker" in text
    assert "does not infer notifier transport authorization" in text


def test_result_record_captures_safety_profile() -> None:
    text = _text()

    assert "safety_profile: prod_pre_runtime" in text
    assert "safety_profile_passed: true" in text


def test_result_record_captures_consumer_readiness_and_unauthorized_flags() -> None:
    text = _text()

    for phrase in CONSUMER_PROFILE_FACTS:
        assert phrase in text, phrase


def test_result_record_captures_all_side_effect_flags_as_false() -> None:
    text = _text()
    normalized = _normalized_text()

    for flag in SIDE_EFFECT_FALSE_FLAGS:
        assert f"{flag}: false" in text, flag

    for phrase in (
        "No DB connection happened",
        "No Redis connection happened",
        "No DB write happened",
        "No Redis mutation happened",
        "No Alembic happened",
        "No app runtime happened",
        "No TDLib auth happened",
        "No Telegram connection happened",
        "No live collector happened",
        "No notifier transport happened",
        "No Docker or systemd change happened",
        "No production rollout happened",
    ):
        assert phrase in normalized, phrase


def test_result_record_states_non_authorizations() -> None:
    text = _text()

    for phrase in (
        "Passing this result does not authorize runtime start",
        "Passing this result does not authorize TDLib auth",
        "Passing this result does not authorize Telegram connection",
        "Passing this result does not authorize live collector startup",
        "Passing this result does not authorize notifier transport",
        "Passing this result does not authorize production rollout",
    ):
        assert phrase in text, phrase


def test_result_record_includes_next_bounded_slice_and_no_direct_jump() -> None:
    text = _text()

    assert "dedicated_vps_app_runtime_import_config_preflight" in text
    assert "separately reviewed TDLib auth package" in text
    assert "Do not jump directly to live collector or production rollout" in text


def test_result_record_rejects_forbidden_full_url_and_secret_patterns() -> None:
    text = _text()

    forbidden_patterns = {
        "postgresql_psycopg_url": r"postgresql\+psycopg://",
        "redis_url": r"redis://",
        "url_credentials_at_localhost": r"@[Ll]ocalhost",
        "url_credentials_at_loopback": r"@127\.0\.0\.1",
        "database_url_assignment": r"\bDATABASE_URL\s*=",
        "redis_url_assignment": r"\bREDIS_URL\s*=",
        "raw_url_credentials": r"[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^@\s]+@",
        "sensitive_env_names": r"\b(?:OPENAI_API_KEY|TELEGRAM_BOT_TOKEN|GITHUB_PRIVATE_KEY|X_BEARER_TOKEN)\b",
        "tdlib_sensitive_names": r"\b(?:api_hash|api_key|bot_token)\b",
        "private_ipv4": r"\b(?!127\.0\.0\.1\b)(?:\d{1,3}\.){3}\d{1,3}\b",
    }

    for name, pattern in forbidden_patterns.items():
        assert re.search(pattern, text) is None, name

    allowed_secret_phrases = {
        "secret_values_printed: false",
        "- Secret values were not printed by the approved runner.",
        "Secret values were not printed by the approved runner.",
    }
    secret_lines = [line.strip() for line in text.splitlines() if "secret" in line.lower()]
    assert set(secret_lines) <= allowed_secret_phrases

    forbidden_code_block_terms = (
        "password",
        "passwd",
        "token",
        "api_hash",
        "api_key",
        "bot_token",
    )
    for block in _code_blocks(text):
        lowered = block.lower()
        for term in forbidden_code_block_terms:
            assert term not in lowered, term


def test_parent_runbook_points_to_result_record_and_next_slice_only() -> None:
    text = PARENT_RUNBOOK.read_text(encoding="utf-8")
    normalized = " ".join(text.split())

    assert "## Completed redacted result record" in text
    assert "dedicated_vps_runtime_environment_consumer_preflight_result_record.md" in text
    assert "dedicated_vps_app_runtime_import_config_preflight" in text
    assert "does not authorize TDLib auth, Telegram connection, live collector" in normalized
    assert "notifier transport, or production rollout" in normalized
