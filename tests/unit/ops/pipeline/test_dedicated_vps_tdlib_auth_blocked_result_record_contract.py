from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdlib_auth_blocked_result_record.md"
)

REQUIRED_MARKERS = (
    "blocked_real_transport_missing",
    "tdlib_auth_attempted=false",
    "runtime_env_read=false",
    "secret_values_printed=false",
    "live_collector_started=false",
    "app_runtime_started=false",
    "notifier_transport_enabled=false",
    "production_rollout_performed=false",
)

REQUIRED_SENTENCES = (
    "No TDLib auth attempt occurred because real tdjson transport was missing.",
    "No runtime.env values were printed.",
    "The next slice is tdjson runtime dependency preflight.",
)

FORBIDDEN_SECRET_PATTERNS = {
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
    "postgresql_url": re.compile(r"\bpostgres(?:ql)?(?:\+psycopg)?://", re.IGNORECASE),
    "redis_url": re.compile(r"\bredis://", re.IGNORECASE),
    "private_invite_link": re.compile(
        r"https?://(?:t|telegram)\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+",
        re.IGNORECASE,
    ),
}


def _text() -> str:
    return RESULT_RECORD.read_text(encoding="utf-8")


def _normalized_text() -> str:
    return " ".join(_text().split())


def test_result_record_contains_required_blocked_result_markers() -> None:
    text = _text()

    for marker in REQUIRED_MARKERS:
        assert marker in text, marker


def test_result_record_states_required_boundary_sentences() -> None:
    text = _text()

    for sentence in REQUIRED_SENTENCES:
        assert sentence in text, sentence


def test_result_record_denies_success_rerun_runtime_notifier_and_rollout() -> None:
    text = _normalized_text()

    for phrase in (
        "must not be interpreted as Telegram auth success",
        "This result must not authorize rerun, live collector, notifier, or rollout.",
        "not result success",
        "not a rerun authorization",
        "not live collector startup",
        "not notifier enablement",
        "not production rollout",
    ):
        assert phrase in text, phrase


def test_result_record_does_not_contain_obvious_secret_patterns() -> None:
    text = _text()

    for name, pattern in FORBIDDEN_SECRET_PATTERNS.items():
        assert pattern.search(text) is None, name
