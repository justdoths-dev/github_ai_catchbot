from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdjson_source_build_dependency_install_result_record.md"
)

REQUIRED_RESULT_MARKERS = (
    "github-ai-catchbot-prod-1",
    "cmake",
    "gperf",
    "/usr/bin/cmake",
    "/usr/bin/gperf",
    "cmake version 3.28.3",
    "GNU gperf 3.1",
    "cmake 3.28.3-1build7 install ok installed",
    "gperf 3.1-1build1 install ok installed",
    (
        "TDJSON_SOURCE_BUILD_PLAN_AFTER_DEPENDENCY_INSTALL_PASS "
        "source_build_plan_ready tdjson_source_build_operator_execution [] []"
    ),
    "## main...origin/main",
)

REQUIRED_BOUNDARY_MARKERS = (
    "does not install tdjson/libtdjson",
    "does not perform source build",
    "does not run git clone",
    "does not run cmake configure/build",
    "does not run make/ninja",
    "does not create build directories",
    "does not place binaries or symlinks",
    "does not read runtime.env",
    "does not print secrets",
    "does not create TDLib client/session",
    "does not contact Telegram",
    "does not run TDLib auth",
    "does not connect to DB/Redis or run Alembic",
    "does not change Docker/systemd",
    "does not start live collector/app runtime/notifier/rollout",
)

REQUIRED_NEXT_SLICE_MARKERS = (
    "tdjson_source_build_operator_execution",
    "separate approved bounded slice",
    "result record does not authorize source build execution by itself",
)

FORBIDDEN_CLAIMS = (
    "tdjson is available",
    "libtdjson is installed",
    "TDLib auth succeeded",
    "auth rerun is authorized",
    "collector is ready",
    "production is ready",
    "rollout is authorized",
    "source build has been completed",
    "TDLib has been built",
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


def test_result_record_file_exists() -> None:
    assert RESULT_RECORD.is_file()


def test_result_record_contains_required_result_markers() -> None:
    text = _text()

    for marker in REQUIRED_RESULT_MARKERS:
        assert marker in text, marker


def test_result_record_contains_required_boundary_markers() -> None:
    text = _text()

    for marker in REQUIRED_BOUNDARY_MARKERS:
        assert marker in text, marker


def test_result_record_contains_next_slice_boundary() -> None:
    text = _normalized_text()

    for marker in REQUIRED_NEXT_SLICE_MARKERS:
        assert marker in text, marker


def test_result_record_omits_forbidden_claims() -> None:
    text = _text()

    for claim in FORBIDDEN_CLAIMS:
        assert claim not in text, claim


def test_result_record_omits_obvious_secret_patterns() -> None:
    text = _text()

    for name, pattern in FORBIDDEN_SECRET_PATTERNS.items():
        assert pattern.search(text) is None, name
