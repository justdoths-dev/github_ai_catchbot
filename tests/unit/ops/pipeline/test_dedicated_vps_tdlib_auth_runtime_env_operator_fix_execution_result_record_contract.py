from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdlib_auth_runtime_env_operator_fix_execution_result_record.md"
)

REQUIRED_CONTEXT_RESULT_MARKERS = (
    "github-ai-catchbot-prod-1",
    "deploy",
    "11443f3 feat(ops): add TDLib auth runtime env operator fix plan",
    "## main...origin/main",
    "TDLIB_AUTH_RUNTIME_ENV_OPERATOR_FIX_PLAN_PRECHECK_PASS",
    "replace_invalid_value TDLIB_STATE_DIR invalid_path_format value_required_from_operator=True",
    "replace_invalid_value TDLIB_FILES_DIR invalid_path_format value_required_from_operator=True",
    "manual_review None invalid_path_format value_required_from_operator=False",
    "/etc/github-ai-catchbot/runtime.env",
    "private VPS editor/equivalent",
    "/var/lib/github-ai-catchbot/tdlib",
    "/var/lib/github-ai-catchbot/tdlib/state",
    "/var/lib/github-ai-catchbot/tdlib/files",
    "deploy:deploy",
    "drwx------",
    "/tmp/dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_after_operator_fix.json",
    (
        "TDLIB_AUTH_RUNTIME_ENV_OPERATOR_FIX_EXECUTION_RESULT "
        "runtime_env_shape_appears_valid tdlib_auth_operator_execution_rerun_after_fix"
    ),
    "contract_status: runtime_env_shape_appears_valid",
    "recommended_next_slice: tdlib_auth_operator_execution_rerun_after_fix",
    "boundary_check: pass",
    "DIAGNOSTIC_REASONS: empty",
    "KEY_ISSUES: empty",
)

REQUIRED_SAFETY_MARKERS = (
    "runtime.env values were not printed",
    "secret values were not printed",
    "Telegram API hash was not printed",
    "Telegram phone number was not printed",
    "Telegram login code or 2FA was not requested or printed",
    "DB URL was not printed",
    "Redis URL was not printed",
    "raw runtime.env lines were not printed",
    "shell history was not included",
    "no `cat /etc/github-ai-catchbot/runtime.env`",
    "no `echo KEY=value`",
    "no value-bearing `sed -i`",
)

REQUIRED_NON_AUTHORITY_MARKERS = (
    "runtime_env_shape_appears_valid is not TDLib auth success",
    "runtime_env_shape_appears_valid is not Telegram login success",
    "runtime_env_shape_appears_valid is not collector readiness",
    "runtime_env_shape_appears_valid is not notifier readiness",
    "runtime_env_shape_appears_valid is not production readiness",
    "this result does not authorize live collector startup",
    "this result does not authorize notifier startup",
    "this result does not authorize production rollout",
    "TDLib auth rerun after fix must be a separate approved bounded slice",
    "tdlib_auth_operator_execution_rerun_after_fix",
    "this result record does not perform that rerun",
)

REQUIRED_BOUNDARY_MARKERS = (
    "does not edit runtime.env",
    "does not read runtime.env",
    "does not print runtime.env values",
    "does not print secrets",
    "does not run TDLib auth",
    "does not run the auth wrapper",
    "does not create TDLib client/session",
    "does not contact Telegram",
    "does not start collector/app runtime/notifier/rollout",
    "does not connect to DB/Redis or run Alembic",
    "does not change Docker/systemd",
    "does not run source build/git clone/cmake/make/ninja",
    "does not mutate packages",
)

FORBIDDEN_CLAIMS = (
    "TDLib auth succeeded",
    "Telegram auth succeeded",
    "collector is ready",
    "live collector is ready",
    "notifier is ready",
    "production is ready",
    "rollout is authorized",
    "auth rerun is authorized",
    "runtime.env value is",
    "missing value is",
    "phone number is",
    "API hash is",
    "login code is",
    "2FA password is",
    "DATABASE_URL is",
    "REDIS_URL is",
)

SECRET_LIKE_PATTERNS = {
    "telegram_bot_token": re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b"),
    "telegram_api_hash_assignment": re.compile(
        r"\bTELEGRAM_API_HASH\b[\"']?\s*[:=]\s*[\"']?[0-9a-fA-F]{32}\b"
    ),
    "telegram_phone_assignment": re.compile(
        r"\bTELEGRAM_PHONE_NUMBER\b[\"']?\s*[:=]\s*[\"']?\+?\d[\d\s().-]{6,}"
    ),
    "telegram_login_code_assignment": re.compile(
        r"\b(?:TELEGRAM_LOGIN_CODE|LOGIN_CODE|AUTH_CODE)\b[\"']?\s*[:=]\s*[\"']?\S+",
        re.IGNORECASE,
    ),
    "two_factor_or_password_assignment": re.compile(
        r"\b(?:TELEGRAM_2FA_PASSWORD|TWO_FACTOR_PASSWORD|2FA_PASSWORD|PASSWORD)"
        r"\b[\"']?\s*[:=]\s*[\"']?\S+",
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


def test_result_record_file_exists() -> None:
    assert RESULT_RECORD.is_file()


def test_required_context_and_result_markers_are_present() -> None:
    text = _text()

    for marker in REQUIRED_CONTEXT_RESULT_MARKERS:
        assert marker in text, marker


def test_required_safety_markers_are_present() -> None:
    text = _text()

    for marker in REQUIRED_SAFETY_MARKERS:
        assert marker in text, marker


def test_required_non_authority_markers_are_present() -> None:
    text = _text()

    for marker in REQUIRED_NON_AUTHORITY_MARKERS:
        assert marker in text, marker


def test_required_boundary_markers_are_present() -> None:
    text = _text()

    for marker in REQUIRED_BOUNDARY_MARKERS:
        assert marker in text, marker


def test_forbidden_claims_are_absent() -> None:
    text = _text()

    for claim in FORBIDDEN_CLAIMS:
        assert claim not in text, claim


def test_secret_like_patterns_are_absent() -> None:
    text = _text()

    for name, pattern in SECRET_LIKE_PATTERNS.items():
        assert pattern.search(text) is None, name
