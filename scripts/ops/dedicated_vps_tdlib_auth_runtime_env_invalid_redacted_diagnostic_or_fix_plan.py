from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = (
    "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan_v1"
)
CONTRACT_NAME = "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan"

CONFIG_SOURCE_FILE = "src/services/collector_telegram/config.py"
CONFIG_SOURCE_LABEL_INJECTED = "<injected collector telegram config source>"

PRIOR_RESULT = {
    "contract_status": "blocked_runtime_env_invalid",
    "blocked_reason": "runtime_env_invalid",
    "tdlib_auth_attempted": False,
    "telegram_connected": False,
}

SAFETY_FLAGS = {
    "auth_wrapper_executed": False,
    "tdlib_auth_attempted": False,
    "tdlib_auth_completed": False,
    "telegram_connected": False,
    "session_state_created_or_reused": False,
    "manual_intervention_required": False,
    "runtime_env_modified": False,
    "runtime_env_values_printed": False,
    "secret_values_printed": False,
    "telegram_login_code_or_2fa_requested": False,
    "collector_main_used": False,
    "collector_service_used": False,
    "collector_runtime_used": False,
    "live_collector_started": False,
    "app_runtime_started": False,
    "notifier_transport_enabled": False,
    "production_rollout_performed": False,
    "database_connected": False,
    "db_connected": False,
    "redis_connected": False,
    "alembic_run": False,
    "docker_or_systemd_changed": False,
    "systemd_or_docker_changed": False,
    "source_build_attempted": False,
    "package_manager_mutation_attempted": False,
}

REQUIRED_KEY_FALLBACK = (
    "DATABASE_URL",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TDLIB_STATE_DIR",
    "TDLIB_DB_ENCRYPTION_KEY",
)

OPTIONAL_KEY_FALLBACK = (
    "APP_ENV",
    "COLLECTOR_MODE",
    "REDIS_URL",
    "TELEGRAM_2FA_PASSWORD",
    "TDLIB_FILES_DIR",
    "RECONCILE_INTERVAL_SEC",
    "RECONCILE_BACKFILL_LIMIT",
    "WARM_BACKFILL_LIMIT",
    "HISTORY_PAGE_LIMIT",
    "COLLECTOR_SINGLETON_LOCK_PATH",
    "STARTUP_PROBE_TIMEOUT_SEC",
    "STARTUP_WARM_BACKFILL_ENABLED",
    "LOG_LEVEL",
    "TELEGRAM_API_HASH_FILE",
    "TELEGRAM_2FA_PASSWORD_FILE",
    "TDLIB_DB_ENCRYPTION_KEY_FILE",
)

SECRET_FILE_ALTERNATIVES = {
    "TELEGRAM_API_HASH": "TELEGRAM_API_HASH_FILE",
    "TELEGRAM_2FA_PASSWORD": "TELEGRAM_2FA_PASSWORD_FILE",
    "TDLIB_DB_ENCRYPTION_KEY": "TDLIB_DB_ENCRYPTION_KEY_FILE",
}

INT_KEYS = {
    "TELEGRAM_API_ID",
    "RECONCILE_INTERVAL_SEC",
    "RECONCILE_BACKFILL_LIMIT",
    "WARM_BACKFILL_LIMIT",
    "HISTORY_PAGE_LIMIT",
    "STARTUP_PROBE_TIMEOUT_SEC",
}

POSITIVE_INT_KEYS = {
    "RECONCILE_INTERVAL_SEC",
    "STARTUP_PROBE_TIMEOUT_SEC",
}

ONE_TO_HUNDRED_INT_KEYS = {
    "RECONCILE_BACKFILL_LIMIT",
    "WARM_BACKFILL_LIMIT",
    "HISTORY_PAGE_LIMIT",
}

BOOL_KEYS = {"STARTUP_WARM_BACKFILL_ENABLED"}
PATH_KEYS = {
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
    "COLLECTOR_SINGLETON_LOCK_PATH",
    "TELEGRAM_API_HASH_FILE",
    "TELEGRAM_2FA_PASSWORD_FILE",
    "TDLIB_DB_ENCRYPTION_KEY_FILE",
}
URL_KEYS = {"DATABASE_URL", "REDIS_URL"}
ENUM_KEYS = {
    "APP_ENV": {"prod", "dev", "test"},
    "COLLECTOR_MODE": {"live", "replay"},
}
SECRET_LIKE_FRAGMENTS = ("SECRET", "PASSWORD", "TOKEN", "HASH", "KEY")
PHONE_LIKE_KEYS = {"TELEGRAM_PHONE_NUMBER"}
BOOLEAN_LITERALS = {"0", "1", "true", "false", "yes", "no"}

ConfigBuildChecker = Callable[[Mapping[str, str], list[dict[str, Any]]], Any]
RuntimeEnvReader = Callable[[str | Path], str]


@dataclass(frozen=True, slots=True)
class ParsedRuntimeEnv:
    values: dict[str, str]
    duplicate_key_names: list[str]
    malformed_line_count: int
    line_count: int
    parsed_key_count: int


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Emit a redacted diagnostic/fix-plan for the prior "
            "blocked_runtime_env_invalid TDLib auth wrapper result. The tool "
            "reads runtime.env only when --runtime-env-path is explicitly "
            "provided, never prints values, and never runs auth or collector code."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--runtime-env-path", default=None)
    return parser


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def parse_runtime_env_text(text: str) -> ParsedRuntimeEnv:
    values: dict[str, str] = {}
    duplicate_key_names: set[str] = set()
    malformed_line_count = 0
    lines = text.splitlines()

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            malformed_line_count += 1
            continue

        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            malformed_line_count += 1
            continue

        if key in values:
            duplicate_key_names.add(key)
        values[key] = _strip_optional_quotes(raw_value)

    return ParsedRuntimeEnv(
        values=values,
        duplicate_key_names=sorted(duplicate_key_names),
        malformed_line_count=malformed_line_count,
        line_count=len(lines),
        parsed_key_count=len(values),
    )


def _read_runtime_env_file(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8", errors="replace")


def _read_config_source(repo_root: Path) -> tuple[str, list[str]]:
    path = repo_root / CONFIG_SOURCE_FILE
    try:
        return path.read_text(encoding="utf-8", errors="replace"), [CONFIG_SOURCE_FILE]
    except OSError:
        return "", []


def _literal_keys(source_text: str) -> set[str]:
    return set(re.findall(r'"([A-Z][A-Z0-9_]+)"', source_text))


def infer_config_contract(
    config_source_text: str,
    *,
    source_files_inspected: Sequence[str],
) -> dict[str, Any]:
    literal_keys = _literal_keys(config_source_text)
    has_config_class = "CollectorTelegramConfig" in config_source_text

    required_keys = [
        key
        for key in REQUIRED_KEY_FALLBACK
        if key in literal_keys or key in config_source_text
    ]
    optional_keys = [
        key
        for key in OPTIONAL_KEY_FALLBACK
        if key in literal_keys
        or key in config_source_text
        or (key.endswith("_FILE") and key.removesuffix("_FILE") in config_source_text)
    ]

    inferred = has_config_class and set(REQUIRED_KEY_FALLBACK).issubset(required_keys)

    return {
        "source_files_inspected": list(source_files_inspected),
        "required_keys": required_keys,
        "optional_keys": optional_keys,
        "inferred_from_existing_config": inferred,
        "secret_file_alternatives": dict(SECRET_FILE_ALTERNATIVES),
    }


def _is_secret_like_key(key: str) -> bool:
    if key in PHONE_LIKE_KEYS:
        return True
    return any(fragment in key for fragment in SECRET_LIKE_FRAGMENTS)


def _classify_url(key: str, value: str) -> tuple[str, str, str | None]:
    if "://" not in value:
        return "invalid_format", "invalid", "unsupported_value_shape"
    scheme = value.split("://", 1)[0].lower()
    allowed = {"postgresql", "postgresql+psycopg"} if key == "DATABASE_URL" else {"redis", "rediss"}
    if scheme not in allowed:
        return "invalid_format", "invalid", "unsupported_value_shape"
    return "url_like_redacted", "valid", None


def _classify_path(value: str) -> tuple[str, str, str | None]:
    if "\x00" in value or any(ch in value for ch in "\r\n"):
        return "invalid_format", "invalid", "invalid_path_format"
    if value.startswith("/") or value.startswith("~/"):
        return "path_like", "valid", None
    return "invalid_format", "invalid", "invalid_path_format"


def _classify_int(key: str, value: str) -> tuple[str, str, str | None]:
    try:
        parsed = int(value)
    except ValueError:
        return "invalid_format", "invalid", "invalid_integer"

    if key in POSITIVE_INT_KEYS and parsed <= 0:
        return "integer_like", "invalid", "unsupported_value_shape"
    if key in ONE_TO_HUNDRED_INT_KEYS and (parsed <= 0 or parsed > 100):
        return "integer_like", "invalid", "unsupported_value_shape"
    return "integer_like", "valid", None


def _classify_bool(value: str) -> tuple[str, str, str | None]:
    if value.strip().lower() in BOOLEAN_LITERALS:
        return "boolean_like", "valid", None
    return "invalid_format", "invalid", "invalid_boolean"


def _classify_enum(key: str, value: str) -> tuple[str, str, str | None]:
    if value.strip().lower() in ENUM_KEYS[key]:
        return "opaque_present_redacted", "valid", None
    return "opaque_present_redacted", "invalid", "unsupported_value_shape"


def _effective_value(values: Mapping[str, str], key: str) -> tuple[bool, str | None, str]:
    file_key = SECRET_FILE_ALTERNATIVES.get(key)
    if file_key:
        file_value = values.get(file_key)
        if file_value is not None and file_value.strip():
            return True, file_value, file_key
    if key in values:
        return True, values[key], key
    if file_key and file_key in values:
        return True, values[file_key], file_key
    return False, None, key


def _check_key(
    *,
    key: str,
    required: bool,
    values: Mapping[str, str],
) -> dict[str, Any]:
    present, value, source_key = _effective_value(values, key)
    if not present:
        return {
            "key": key,
            "required": required,
            "present": False,
            "empty": None,
            "value_class": "absent",
            "format_status": "invalid" if required else "unknown",
            "issue_code": "missing_required_key" if required else None,
        }

    assert value is not None
    empty = not value.strip()
    if empty:
        return {
            "key": key,
            "required": required,
            "present": True,
            "empty": True,
            "value_class": "empty",
            "format_status": "invalid" if required else "unknown",
            "issue_code": "empty_required_key" if required else None,
        }

    classifier_key = source_key
    if classifier_key in INT_KEYS:
        value_class, format_status, issue_code = _classify_int(classifier_key, value)
    elif classifier_key in BOOL_KEYS:
        value_class, format_status, issue_code = _classify_bool(value)
    elif classifier_key in PATH_KEYS:
        value_class, format_status, issue_code = _classify_path(value)
    elif classifier_key in URL_KEYS:
        value_class, format_status, issue_code = _classify_url(classifier_key, value)
    elif classifier_key in ENUM_KEYS:
        value_class, format_status, issue_code = _classify_enum(classifier_key, value)
    elif _is_secret_like_key(classifier_key):
        value_class, format_status, issue_code = "secret_like_redacted", "unknown", None
    else:
        value_class, format_status, issue_code = "opaque_present_redacted", "unknown", None

    return {
        "key": key,
        "required": required,
        "present": True,
        "empty": False,
        "value_class": value_class,
        "format_status": format_status,
        "issue_code": issue_code,
    }


def build_key_checks(
    parsed: ParsedRuntimeEnv,
    config_contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    required_keys = list(config_contract["required_keys"])
    optional_keys = [
        key
        for key in config_contract["optional_keys"]
        if key not in required_keys
    ]

    for key in required_keys:
        checks.append(_check_key(key=key, required=True, values=parsed.values))
    for key in optional_keys:
        checks.append(_check_key(key=key, required=False, values=parsed.values))
    return checks


def _redacted_config_build_failure(
    *,
    exception_class: str | None,
    redacted_error_category: str | None,
) -> dict[str, Any]:
    return {
        "attempted": True,
        "status": "build_invalid_redacted",
        "exception_class": exception_class,
        "redacted_error_category": redacted_error_category or "config_build_failed_redacted",
        "raw_exception_message_included": False,
    }


def _run_injected_config_build_checker(
    config_build_checker: ConfigBuildChecker,
    values: Mapping[str, str],
    key_checks: list[dict[str, Any]],
) -> dict[str, Any]:
    try:
        result = config_build_checker(values, key_checks)
    except Exception as exc:
        return _redacted_config_build_failure(
            exception_class=type(exc).__name__,
            redacted_error_category="config_build_failed_redacted",
        )

    if isinstance(result, Mapping):
        status = str(result.get("status", "build_valid"))
        return {
            "attempted": True,
            "status": status,
            "exception_class": result.get("exception_class"),
            "redacted_error_category": result.get("redacted_error_category"),
            "raw_exception_message_included": False,
        }
    if result is False:
        return _redacted_config_build_failure(
            exception_class=None,
            redacted_error_category="config_build_failed_redacted",
        )
    return {
        "attempted": True,
        "status": "build_valid",
        "exception_class": None,
        "redacted_error_category": None,
        "raw_exception_message_included": False,
    }


def _safe_structural_config_build_check(
    values: Mapping[str, str],
    key_checks: list[dict[str, Any]],
) -> dict[str, Any]:
    for check in key_checks:
        if check["issue_code"] is not None and check["required"]:
            return _redacted_config_build_failure(
                exception_class=None,
                redacted_error_category=check["issue_code"],
            )
        if check["issue_code"] in {
            "invalid_integer",
            "invalid_boolean",
            "invalid_path_format",
            "unsupported_value_shape",
        }:
            return _redacted_config_build_failure(
                exception_class=None,
                redacted_error_category=check["issue_code"],
            )

    app_env = values.get("APP_ENV", "dev").strip().lower() or "dev"
    collector_mode = values.get("COLLECTOR_MODE", "replay").strip().lower() or "replay"
    if app_env not in ENUM_KEYS["APP_ENV"]:
        return _redacted_config_build_failure(
            exception_class=None,
            redacted_error_category="unsupported_value_shape",
        )
    if collector_mode not in ENUM_KEYS["COLLECTOR_MODE"]:
        return _redacted_config_build_failure(
            exception_class=None,
            redacted_error_category="unsupported_value_shape",
        )
    if app_env == "prod" and collector_mode != "live":
        return _redacted_config_build_failure(
            exception_class=None,
            redacted_error_category="config_build_failed_redacted",
        )
    if app_env in {"dev", "test"} and collector_mode == "live":
        return _redacted_config_build_failure(
            exception_class=None,
            redacted_error_category="config_build_failed_redacted",
        )

    return {
        "attempted": True,
        "status": "build_valid",
        "exception_class": None,
        "redacted_error_category": None,
        "raw_exception_message_included": False,
    }


def run_collector_config_build_check(
    values: Mapping[str, str],
    key_checks: list[dict[str, Any]],
    *,
    config_build_checker: ConfigBuildChecker | None = None,
) -> dict[str, Any]:
    if config_build_checker is not None:
        return _run_injected_config_build_checker(config_build_checker, values, key_checks)
    return _safe_structural_config_build_check(values, key_checks)


def _reason_for_check(check: Mapping[str, Any]) -> str | None:
    issue_code = check["issue_code"]
    if issue_code is None:
        return None
    return f"{issue_code}: {check['key']}"


def build_diagnostic_reasons(
    *,
    parsed: ParsedRuntimeEnv,
    key_checks: list[dict[str, Any]],
    config_build_check: Mapping[str, Any],
    config_contract: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not config_contract["inferred_from_existing_config"]:
        reasons.append("config_contract_not_inferred")
    for check in key_checks:
        reason = _reason_for_check(check)
        if reason is not None:
            reasons.append(reason)
    for key in parsed.duplicate_key_names:
        reasons.append(f"duplicate_key_name: {key}")
    if parsed.malformed_line_count:
        reasons.append(f"malformed_line_count: {parsed.malformed_line_count}")
    if config_build_check["status"] == "build_invalid_redacted":
        category = config_build_check["redacted_error_category"] or "config_build_failed_redacted"
        reasons.append(f"collector_config_build_check: {category}")
    return reasons


def _fix_action(
    *,
    action_id: str,
    action_type: str,
    key: str | None,
    reason: str,
    value_required_from_operator: bool,
) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "action_type": action_type,
        "key": key,
        "reason": reason,
        "value_required_from_operator": value_required_from_operator,
        "value_to_use": None,
        "future_operator_command_not_run": None,
    }


def build_redacted_fix_plan(
    *,
    parsed: ParsedRuntimeEnv,
    key_checks: list[dict[str, Any]],
    config_build_check: Mapping[str, Any],
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for check in key_checks:
        issue_code = check["issue_code"]
        if issue_code is None:
            continue
        key = check["key"]
        if issue_code in {"missing_required_key", "empty_required_key"}:
            action_type = "set_missing_key"
            needs_value = True
        elif issue_code in {
            "invalid_integer",
            "invalid_boolean",
            "invalid_path_format",
            "unsupported_value_shape",
        }:
            action_type = "replace_invalid_value"
            needs_value = True
        else:
            action_type = "manual_review"
            needs_value = False
        action_id = f"{action_type}.{key.lower()}"
        if action_id in seen_ids:
            continue
        seen_ids.add(action_id)
        plan.append(
            _fix_action(
                action_id=action_id,
                action_type=action_type,
                key=key,
                reason=issue_code,
                value_required_from_operator=needs_value,
            )
        )

    for key in parsed.duplicate_key_names:
        action_id = f"remove_duplicate_key.{key.lower()}"
        plan.append(
            _fix_action(
                action_id=action_id,
                action_type="remove_duplicate_key",
                key=key,
                reason="duplicate_key_name",
                value_required_from_operator=False,
            )
        )

    if parsed.malformed_line_count:
        plan.append(
            _fix_action(
                action_id="manual_review.malformed_lines",
                action_type="manual_review",
                key=None,
                reason="malformed_line_count",
                value_required_from_operator=False,
            )
        )

    if config_build_check["status"] == "build_invalid_redacted":
        plan.append(
            _fix_action(
                action_id="manual_review.collector_config_build_check",
                action_type="manual_review",
                key=None,
                reason=config_build_check["redacted_error_category"]
                or "config_build_failed_redacted",
                value_required_from_operator=False,
            )
        )
    return plan


def _base_report(
    *,
    config_contract: Mapping[str, Any],
    runtime_env_path: str | None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_name": CONTRACT_NAME,
        "contract_status": "runtime_env_read_blocked",
        "recommended_next_slice": "defer_manual_review",
        "prior_result": dict(PRIOR_RESULT),
        "runtime_env_inspection": {
            "runtime_env_path_provided": runtime_env_path is not None,
            "runtime_env_read": False,
            "runtime_env_values_printed": False,
            "secret_values_printed": False,
            "raw_values_in_output": False,
            "line_count": None,
            "parsed_key_count": None,
            "duplicate_key_names": [],
            "malformed_line_count": 0,
        },
        "config_contract": dict(config_contract),
        "redacted_key_checks": [],
        "collector_config_build_check": {
            "attempted": False,
            "status": "not_attempted",
            "exception_class": None,
            "redacted_error_category": None,
            "raw_exception_message_included": False,
        },
        "diagnostic_reasons": [],
        "redacted_fix_plan": [],
        "stop_conditions": [
            "no_auth_rerun",
            "no_auth_wrapper_execution",
            "no_runtime_env_edit",
            "no_runtime_env_value_printing",
            "no_secret_value_printing",
            "no_tdlib_client_or_session_creation",
            "no_telegram_network_contact",
            "no_collector_notifier_rollout_start",
            "no_db_redis_alembic",
            "no_docker_systemd_change",
            "no_source_build_or_package_mutation",
        ],
        "boundary_check": "pass",
    }
    report.update(SAFETY_FLAGS)
    return report


def _finalize_status(
    *,
    report: dict[str, Any],
    parsed: ParsedRuntimeEnv,
    key_checks: list[dict[str, Any]],
    config_build_check: Mapping[str, Any],
    config_contract: Mapping[str, Any],
) -> None:
    reasons = build_diagnostic_reasons(
        parsed=parsed,
        key_checks=key_checks,
        config_build_check=config_build_check,
        config_contract=config_contract,
    )
    report["diagnostic_reasons"] = reasons
    report["redacted_fix_plan"] = build_redacted_fix_plan(
        parsed=parsed,
        key_checks=key_checks,
        config_build_check=config_build_check,
    )

    if not config_contract["inferred_from_existing_config"]:
        report["contract_status"] = "runtime_env_invalid_diagnostic_inconclusive"
        report["recommended_next_slice"] = "defer_manual_review"
        return

    invalid_key_checks = [check for check in key_checks if check["issue_code"] is not None]
    has_shape_issue = bool(
        invalid_key_checks
        or parsed.duplicate_key_names
        or parsed.malformed_line_count
        or config_build_check["status"] == "build_invalid_redacted"
    )
    if has_shape_issue:
        report["contract_status"] = "runtime_env_invalid_diagnostic_ready"
        report["recommended_next_slice"] = "tdlib_auth_runtime_env_operator_fix_plan"
        return

    if config_build_check["status"] == "build_valid":
        report["contract_status"] = "runtime_env_shape_appears_valid"
        report["recommended_next_slice"] = "tdlib_auth_operator_execution_rerun_after_fix"
        return

    report["contract_status"] = "runtime_env_invalid_diagnostic_inconclusive"
    report["recommended_next_slice"] = "defer_manual_review"


def generate_report(
    *,
    repo_root: Path | None = None,
    runtime_env_path: str | Path | None = None,
    runtime_env_text: str | None = None,
    config_source_text: str | None = None,
    config_build_checker: ConfigBuildChecker | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
) -> dict[str, Any]:
    repo_root = repo_root or default_repo_root()
    if config_source_text is None:
        config_source_text, source_files_inspected = _read_config_source(repo_root)
    else:
        source_files_inspected = [CONFIG_SOURCE_LABEL_INJECTED]
    config_contract = infer_config_contract(
        config_source_text,
        source_files_inspected=source_files_inspected,
    )

    path_text = str(runtime_env_path) if runtime_env_path is not None else None
    if runtime_env_text is not None and path_text is None:
        path_text = "<injected runtime env text>"
    report = _base_report(config_contract=config_contract, runtime_env_path=path_text)

    if runtime_env_text is None and runtime_env_path is None:
        report["diagnostic_reasons"] = ["runtime_env_path_not_provided"]
        return report

    if runtime_env_text is None:
        reader = runtime_env_reader or _read_runtime_env_file
        try:
            runtime_env_text = reader(runtime_env_path)  # type: ignore[arg-type]
        except OSError:
            report["contract_status"] = "runtime_env_path_missing"
            report["recommended_next_slice"] = "defer_manual_review"
            report["diagnostic_reasons"] = ["runtime_env_path_missing_or_unreadable"]
            return report

    parsed = parse_runtime_env_text(runtime_env_text)
    report["runtime_env_inspection"] = {
        "runtime_env_path_provided": True,
        "runtime_env_read": True,
        "runtime_env_values_printed": False,
        "secret_values_printed": False,
        "raw_values_in_output": False,
        "line_count": parsed.line_count,
        "parsed_key_count": parsed.parsed_key_count,
        "duplicate_key_names": parsed.duplicate_key_names,
        "malformed_line_count": parsed.malformed_line_count,
    }

    key_checks = build_key_checks(parsed, config_contract)
    config_build_check = run_collector_config_build_check(
        parsed.values,
        key_checks,
        config_build_checker=config_build_checker,
    )
    report["redacted_key_checks"] = key_checks
    report["collector_config_build_check"] = config_build_check
    _finalize_status(
        report=report,
        parsed=parsed,
        key_checks=key_checks,
        config_build_check=config_build_check,
        config_contract=config_contract,
    )
    return report


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root) if args.repo_root else default_repo_root()
    report = generate_report(repo_root=repo_root, runtime_env_path=args.runtime_env_path)
    print(render_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
