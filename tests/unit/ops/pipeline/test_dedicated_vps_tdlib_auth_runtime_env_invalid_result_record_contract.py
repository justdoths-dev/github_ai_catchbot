from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdlib_auth_runtime_env_invalid_result_record.md"
)

REQUIRED_CONTEXT_AND_RESULT_MARKERS = (
    "github-ai-catchbot-prod-1",
    "deploy",
    "3731134 docs(ops): record tdjson source build operator result",
    "## main...origin/main",
    "/opt/github-ai-catchbot/tdlib/lib/libtdjson.so.1.8.64",
    "TDJSON_PREFLIGHT_BEFORE_AUTH_RERUN_PASS tdjson_available pass",
    "scripts/ops/dedicated_vps_tdlib_auth_operator_execution_wrapper.py",
    "--approved-tdlib-auth-operator-execution",
    "--runtime-env-path /etc/github-ai-catchbot/runtime.env",
    "/tmp/dedicated_vps_tdlib_auth_operator_execution_rerun.json",
    "contract_status: blocked_runtime_env_invalid",
    "blocked_reason: runtime_env_invalid",
    "runtime_env.invalid",
    "Approved TDLib auth execution could not build collector config: ConfigurationError.",
    "approved_execution_requested: true",
    "approval_required: false",
    "auth_only_entrypoint_status: available",
    "selected_entrypoint: src.services.collector_telegram.auth_entrypoint",
    "runtime_env_read: true",
    "runtime_env_values_printed: false",
    "secret_values_printed: false",
    "tdlib_auth_attempted: false",
    "tdlib_auth_completed: false",
    "telegram_connected: false",
    "session_state_created_or_reused: false",
    "manual_intervention_required: false",
    "network_called: false",
    "files_mutated: false",
    "collector_main_used: false",
    "collector_service_used: false",
    "collector_runtime_used: false",
    "live_collector_started: false",
    "app_runtime_started: false",
    "notifier_transport_enabled: false",
    "production_rollout_performed: false",
    "database_connected: false",
    "db_connected: false",
    "redis_connected: false",
    "alembic_run: false",
    "docker_or_systemd_changed: false",
    "systemd_or_docker_changed: false",
    "telegram_bot_token_used_for_tdlib_auth: false",
    (
        "TDLIB_AUTH_OPERATOR_EXECUTION_RERUN_RESULT blocked_runtime_env_invalid "
        "tdlib_auth_attempted=False tdlib_auth_completed=False "
        "telegram_connected=False session_state_created_or_reused=False "
        "manual_intervention_required=False"
    ),
)

REQUIRED_INTERPRETATION_MARKERS = (
    "pre-auth runtime env / collector config construction block",
    "not TDLib auth success",
    "not TDLib auth failure after Telegram contact",
    "No TDLib auth attempt occurred",
    "No Telegram connection occurred",
    "No session state was created or reused",
    "No manual intervention was requested",
    "runtime.env was read only by the approved wrapper",
    "runtime.env values were not printed",
    "secrets were not printed",
)

REQUIRED_BOUNDARY_MARKERS = (
    "does not run TDLib auth",
    "does not run the auth wrapper",
    "does not read runtime.env",
    "does not write runtime.env",
    "does not print runtime.env values",
    "does not print secrets",
    "does not request or handle Telegram login code/2FA",
    "does not create TDLib client/session",
    "does not contact Telegram",
    "does not start collector/app runtime/notifier/rollout",
    "does not connect to DB/Redis or run Alembic",
    "does not change Docker/systemd",
    "does not run source build/git clone/cmake/make/ninja",
    "does not mutate packages",
    "does not diagnose or fix runtime.env",
)

REQUIRED_NON_AUTHORITY_MARKERS = (
    "blocked_runtime_env_invalid does not authorize direct auth retry",
    "blocked_runtime_env_invalid does not authorize live collector startup",
    "blocked_runtime_env_invalid does not authorize notifier startup",
    "blocked_runtime_env_invalid does not authorize production rollout",
    (
        "blocked_runtime_env_invalid must be handled by a separate bounded "
        "redacted diagnostic/fix-plan slice"
    ),
    "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan",
    "result record does not perform that diagnostic or fix",
)

FORBIDDEN_CLAIMS = (
    "TDLib auth succeeded",
    "Telegram auth succeeded",
    "auth failed after Telegram contact",
    "auth retry is authorized",
    "collector is ready",
    "live collector is ready",
    "notifier is ready",
    "production is ready",
    "rollout is authorized",
    "runtime.env value is",
    "missing value is",
    "phone number is",
    "API hash is",
    "login code is",
    "2FA password is",
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


def test_result_record_contains_required_context_and_result_markers() -> None:
    text = _text()

    for marker in REQUIRED_CONTEXT_AND_RESULT_MARKERS:
        assert marker in text, marker


def test_result_record_contains_required_interpretation_markers() -> None:
    text = _normalized_text()

    for marker in REQUIRED_INTERPRETATION_MARKERS:
        assert marker in text, marker


def test_result_record_contains_required_boundary_markers() -> None:
    text = _normalized_text()

    for marker in REQUIRED_BOUNDARY_MARKERS:
        assert marker in text, marker


def test_result_record_contains_required_non_authority_markers() -> None:
    text = _normalized_text()

    for marker in REQUIRED_NON_AUTHORITY_MARKERS:
        assert marker in text, marker


def test_result_record_omits_forbidden_claims() -> None:
    text = _text()

    for claim in FORBIDDEN_CLAIMS:
        assert claim not in text, claim


def test_result_record_omits_obvious_secret_patterns() -> None:
    text = _text()

    for name, pattern in FORBIDDEN_SECRET_PATTERNS.items():
        assert pattern.search(text) is None, name
