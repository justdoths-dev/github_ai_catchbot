from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_telegram_runtime_secret_placement_result_record.md"
)

REQUIRED_SECTIONS = (
    "# Dedicated VPS Telegram runtime secret placement result record",
    "## Purpose",
    "## Source-of-truth / architecture boundary",
    "## Execution summary",
    "## Runtime secret file target",
    "## Redacted validation result",
    "## Key status summary",
    "## Side-effect boundary result",
    "## Secret / redaction confirmation",
    "## Non-authorizations preserved",
    "## Known limitations",
    "## Acceptance criteria",
    "## Next bounded action",
)

REQUIRED_KEYS = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
    "TELEGRAM_BOT_TOKEN",
)

SIDE_EFFECT_FALSE_FLAGS = (
    "database_connected",
    "redis_connected",
    "alembic_run",
    "app_runtime_started",
    "tdlib_auth_performed",
    "telegram_connected",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
)

FORBIDDEN_PATTERNS = {
    "telegram_bot_token": r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b",
    "telegram_api_hash_assignment": r"\bTELEGRAM_API_HASH\s*[:=]\s*[0-9a-fA-F]{32}\b",
    "telegram_phone_assignment": r"\bTELEGRAM_PHONE_NUMBER\s*[:=]\s*\+?\d[\d\s().-]{6,}",
    "private_invite_plus": r"https?://t\.me/\+",
    "private_invite_joinchat": r"https?://t\.me/joinchat/",
    "legacy_private_invite_joinchat": r"telegram\.me/joinchat/",
    "cat_runtime_env": r"\bcat /etc/github-ai-catchbot/runtime\.env\b",
    "source_runtime_env": r"\bsource /etc/github-ai-catchbot/runtime\.env\b",
    "dot_source_runtime_env": r"(?m)^\s*\.\s+/etc/github-ai-catchbot/runtime\.env\b",
    "export_cat": r"\bexport \$\(cat\b",
    "postgresql_url": r"postgresql://",
    "postgresql_psycopg_url": r"postgresql\+psycopg://",
    "redis_url": r"redis://",
    "echo_secret_append": r"(?is)\becho\b[^\n]*TELEGRAM_[^\n]*>>",
    "tee_append_runtime_env": r"\btee\s+-a\s+/etc/github-ai-catchbot/runtime\.env\b",
    "ipv4_literal": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
}


def _text() -> str:
    return RESULT_RECORD.read_text(encoding="utf-8")


def _normalized_text() -> str:
    return " ".join(_text().split())


def test_result_record_exists_and_has_required_sections_once() -> None:
    assert RESULT_RECORD.exists()
    text = _text()

    for section in REQUIRED_SECTIONS:
        assert text.count(section) == 1, section


def test_result_record_identifies_approved_package_commit() -> None:
    text = _text()

    assert "eec297d" in text
    assert "test(ops): add telegram runtime secret placement package" in text


def test_runtime_secret_target_is_recorded_as_path_label_not_raw_content() -> None:
    text = _text()
    normalized = _normalized_text()

    assert "/etc/github-ai-catchbot/runtime.env" in text
    assert "path label only" in normalized
    assert "raw file content" in normalized
    assert "runtime env assignments" in normalized


def test_redacted_validation_status_is_recorded() -> None:
    text = _text()

    assert "contract_status: passed" in text
    assert "runtime_env_read: true" in text
    assert "runtime_env_values_printed: false" in text
    assert "applies only to the approved operator validation" in _normalized_text()


def test_required_key_statuses_are_present_redacted() -> None:
    text = _text()

    for key in REQUIRED_KEYS:
        assert re.search(rf"^{key}: present_redacted$", text, re.MULTILINE), key

    assert re.search(
        r"^TELEGRAM_2FA_PASSWORD: present_redacted$", text, re.MULTILINE
    )


def test_side_effect_booleans_are_recorded_false() -> None:
    text = _text()
    normalized = _normalized_text()

    for flag in SIDE_EFFECT_FALSE_FLAGS:
        assert re.search(rf"^{flag}: false$", text, re.MULTILINE), flag

    for phrase in (
        "It did not connect to DB or Redis",
        "run Alembic",
        "start the app runtime",
        "perform TDLib auth",
        "connect Telegram",
        "start the live collector",
        "enable notifier transport",
        "perform production rollout",
    ):
        assert phrase in normalized, phrase


def test_no_real_looking_secret_token_hash_phone_invite_db_redis_or_ip_patterns() -> None:
    text = _text()

    for name, pattern in FORBIDDEN_PATTERNS.items():
        assert re.search(pattern, text) is None, name


def test_next_bounded_action_is_tdlib_auth_package() -> None:
    text = _text()

    assert "dedicated_vps_tdlib_auth_package" in text
    assert "dedicated_vps_telegram_runtime_secret_placement_result_record" not in text


def test_record_does_not_authorize_runtime_telegram_notifier_or_rollout() -> None:
    text = _text()
    normalized = _normalized_text()

    for phrase in (
        "does not authorize TDLib auth",
        "does not authorize Telegram connection",
        "does not authorize live collector startup",
        "does not authorize notifier transport",
        "does not authorize production rollout",
        "TDLib auth is not executed by this record",
        "Telegram connection, live collector startup, notifier transport, and production rollout remain unauthorized",
    ):
        assert phrase in normalized, phrase


def test_test_file_does_not_read_runtime_env() -> None:
    test_source = Path(__file__).read_text(encoding="utf-8")
    direct_path_single = "Path('/etc/github-ai-catchbot/" + "runtime.env')"
    direct_path_double = 'Path("/etc/github-ai-catchbot/' + 'runtime.env")'
    open_single = "open('/etc/github-ai-catchbot/" + "runtime.env'"
    open_double = 'open("/etc/github-ai-catchbot/' + 'runtime.env"'

    assert direct_path_single not in test_source
    assert direct_path_double not in test_source
    assert open_single not in test_source
    assert open_double not in test_source
