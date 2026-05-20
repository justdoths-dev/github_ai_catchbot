from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan_v1"
CONTRACT_NAME = "dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan"

INPUT_SCHEMA_VERSION = (
    "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan_v1"
)
INPUT_CONTRACT_NAME = (
    "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan"
)

DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

INPUT_CONTRACT_STATUSES = {
    "runtime_env_invalid_diagnostic_ready",
    "runtime_env_invalid_diagnostic_inconclusive",
    "runtime_env_shape_appears_valid",
    "runtime_env_path_missing",
    "runtime_env_read_blocked",
}

INPUT_RECOMMENDED_NEXT_SLICES = {
    "tdlib_auth_runtime_env_operator_fix_plan",
    "tdlib_auth_operator_execution_rerun_after_fix",
    "defer_manual_review",
}

OUTPUT_CONTRACT_STATUSES = {
    "runtime_env_operator_fix_plan_ready",
    "runtime_env_operator_fix_plan_inconclusive",
    "diagnostic_json_missing",
    "diagnostic_json_unsafe",
    "diagnostic_not_ready",
    "runtime_env_shape_already_valid",
}

OUTPUT_RECOMMENDED_NEXT_SLICES = {
    "tdlib_auth_runtime_env_operator_fix_execution",
    "tdlib_auth_operator_execution_rerun_after_fix",
    "defer_manual_review",
}

ACTION_TYPES = {
    "set_missing_key",
    "replace_invalid_value",
    "remove_duplicate_key",
    "manual_review",
}

VALUE_REQUIRED_ACTIONS = {"set_missing_key", "replace_invalid_value"}

SAFETY_FLAGS = {
    "fix_executed": False,
    "runtime_env_read": False,
    "runtime_env_modified": False,
    "runtime_env_values_printed": False,
    "secret_values_printed": False,
    "auth_wrapper_executed": False,
    "tdlib_auth_attempted": False,
    "tdlib_auth_completed": False,
    "telegram_connected": False,
    "session_state_created_or_reused": False,
    "manual_intervention_required": False,
    "telegram_login_code_or_2fa_requested": False,
    "collector_main_used": False,
    "collector_service_used": False,
    "collector_runtime_used": False,
    "live_collector_started": False,
    "app_runtime_started": False,
    "notifier_transport_enabled": False,
    "production_rollout_performed": False,
    "database_connected": False,
    "redis_connected": False,
    "alembic_run": False,
    "docker_or_systemd_changed": False,
    "source_build_attempted": False,
    "package_manager_mutation_attempted": False,
}

FORBIDDEN_SECRET_PATTERNS = {
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
        r"\b\s*=\s*\S+",
        re.IGNORECASE,
    ),
    "postgresql_url": re.compile(r"\bpostgresql(?:\+psycopg)?://", re.IGNORECASE),
    "redis_url": re.compile(r"\bredis://", re.IGNORECASE),
    "private_invite_link": re.compile(
        r"https?://(?:t|telegram)\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+",
        re.IGNORECASE,
    ),
}

SENSITIVE_ASSIGNMENT_KEYS = {
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TELEGRAM_LOGIN_CODE",
    "LOGIN_CODE",
    "AUTH_CODE",
    "TELEGRAM_2FA_PASSWORD",
    "TWO_FACTOR_PASSWORD",
    "2FA_PASSWORD",
    "PASSWORD",
    "DATABASE_URL",
    "REDIS_URL",
}

SAFE_SENSITIVE_METADATA_VALUES = {
    "TELEGRAM_API_HASH_FILE",
    "TELEGRAM_2FA_PASSWORD_FILE",
    "TDLIB_DB_ENCRYPTION_KEY_FILE",
    "secret_like_redacted",
    "url_like_redacted",
    "opaque_present_redacted",
    "redacted",
}

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce a redacted operator fix plan from the redacted TDLib auth "
            "runtime env diagnostic JSON. This command reads only the explicit "
            "diagnostic JSON, reads no runtime.env file, writes no runtime.env "
            "file, runs no auth, and starts no runtime services."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--diagnostic-json",
        default=None,
        help="Explicit path to a redacted diagnostic JSON report.",
    )
    parser.add_argument(
        "--runtime-env-path",
        default=DEFAULT_RUNTIME_ENV_PATH,
        help=(
            "Runtime env target path string for planning text only. The file is "
            "not read or modified."
        ),
    )
    return parser


def _base_report(
    *,
    diagnostic_json_path: str | None,
    runtime_env_path: str,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_name": CONTRACT_NAME,
        "contract_status": "diagnostic_json_missing",
        "recommended_next_slice": "defer_manual_review",
        "diagnostic_input": {
            "diagnostic_json_path_provided": diagnostic_json_path is not None,
            "diagnostic_json_read": False,
            "diagnostic_contract_status": None,
            "diagnostic_recommended_next_slice": None,
            "diagnostic_boundary_check": None,
            "diagnostic_values_safe": False,
        },
        "runtime_env_target": {
            "runtime_env_path": runtime_env_path,
            "runtime_env_read": False,
            "runtime_env_modified": False,
            "runtime_env_values_printed": False,
            "secret_values_printed": False,
        },
        "issue_summary": {
            "missing_required_keys": [],
            "empty_required_keys": [],
            "invalid_format_keys": [],
            "duplicate_key_names": [],
            "malformed_line_count": None,
            "manual_review_required": True,
        },
        "selected_plan": {
            "plan_kind": "operator_runtime_env_fix_plan",
            "actions": [],
            "pre_fix_backup_instruction_not_run": (
                "NOT RUN / FUTURE SLICE ONLY: before any approved fix execution, "
                f"create a private operator-side backup of {runtime_env_path} "
                "without displaying file contents or values."
            ),
            "fix_execution_boundary": (
                "Fix execution is a separate approved slice. This plan authorizes "
                "only key-level future instructions and no runtime.env value "
                "collection, reading, editing, auth execution, or service startup."
            ),
            "post_fix_validation_commands_not_run": [
                (
                    "NOT RUN / FUTURE SLICE ONLY: rerun the redacted runtime-env "
                    "diagnostic tool against the target runtime env path and write "
                    "its redacted JSON to the operator-owned temporary output path."
                ),
                (
                    "NOT RUN / FUTURE SLICE ONLY: assert the redacted diagnostic "
                    "output contract, safety booleans, and boundary_check=pass."
                ),
                (
                    "NOT RUN / FUTURE SLICE ONLY: if the shape appears valid, "
                    "request a separate tdlib_auth_operator_execution_rerun_after_fix "
                    "slice."
                ),
            ],
            "post_fix_next_slice_not_run": (
                "tdlib_auth_operator_execution_rerun_after_fix only after a "
                "separate approved fix execution and a shape-valid redacted "
                "diagnostic result."
            ),
        },
        "operator_warnings": [
            (
                "Do not paste runtime.env values or secrets into ChatGPT, Codex, "
                "GitHub, markdown, shell history, logs, or review bundles."
            ),
            (
                "All actual replacement values must come from private operator "
                "input only during a later approved fix execution slice."
            ),
            (
                "This plan may name keys and issue categories only. It must not "
                "contain actual values or runnable replacement commands."
            ),
        ],
        "stop_conditions": [
            "diagnostic_json_missing_or_unreadable",
            "diagnostic_json_unsafe",
            "diagnostic_contract_mismatch",
            "diagnostic_boundary_check_not_pass",
            "diagnostic_not_ready_for_operator_fix_plan",
            "operator_value_needed_in_current_slice",
            "request_to_read_or_print_runtime_env_values",
            "request_to_run_auth_or_start_collector_notifier_rollout",
            "request_to_connect_db_redis_or_run_alembic",
            "request_to_change_docker_systemd_build_or_packages",
        ],
        "unsafe_diagnostic_reasons": [],
        "boundary_check": "pass",
    }
    report.update(SAFETY_FLAGS)
    return report


def _read_json_file(path_text: str) -> tuple[Any | None, str | None]:
    try:
        text = Path(path_text).read_text(encoding="utf-8")
    except OSError:
        return None, "diagnostic_json_missing_or_unreadable"
    try:
        return json.loads(text), None
    except json.JSONDecodeError:
        return None, "diagnostic_json_invalid_json"


def _serialized(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _contains_true_key(data: Any, key: str) -> bool:
    if isinstance(data, Mapping):
        for item_key, value in data.items():
            if item_key == key and value is True:
                return True
            if _contains_true_key(value, key):
                return True
    elif isinstance(data, list):
        return any(_contains_true_key(item, key) for item in data)
    return False


def _value_looks_sensitive_assignment(value: Any) -> bool:
    if value in {None, "", False}:
        return False
    if not isinstance(value, str):
        return True
    stripped = value.strip()
    if not stripped:
        return False
    if stripped in SAFE_SENSITIVE_METADATA_VALUES or stripped.endswith("_FILE"):
        return False
    if stripped.startswith("<") and stripped.endswith(">"):
        return False
    return True


def _contains_sensitive_assignment(data: Any) -> bool:
    if isinstance(data, Mapping):
        for key, value in data.items():
            key_text = str(key).upper()
            if key_text in SENSITIVE_ASSIGNMENT_KEYS and _value_looks_sensitive_assignment(value):
                return True
            if _contains_sensitive_assignment(value):
                return True
    elif isinstance(data, list):
        return any(_contains_sensitive_assignment(item) for item in data)
    return False


def _unsafe_reasons(data: Any) -> list[str]:
    reasons: list[str] = []
    if not isinstance(data, Mapping):
        return ["diagnostic_json_root_not_object"]

    if data.get("schema_version") != INPUT_SCHEMA_VERSION:
        reasons.append("diagnostic_schema_version_mismatch")
    if data.get("contract_name") != INPUT_CONTRACT_NAME:
        reasons.append("diagnostic_contract_name_mismatch")
    if data.get("boundary_check") != "pass":
        reasons.append("diagnostic_boundary_check_not_pass")

    for key in ("runtime_env_values_printed", "secret_values_printed", "raw_values_in_output"):
        if _contains_true_key(data, key):
            reasons.append(f"{key}_true")

    if _contains_sensitive_assignment(data):
        reasons.append("sensitive_key_value_assignment_present")

    text = _serialized(data)
    for name, pattern in FORBIDDEN_SECRET_PATTERNS.items():
        if pattern.search(text):
            reasons.append(name)

    for action in _as_list(data.get("redacted_fix_plan")):
        if isinstance(action, Mapping) and action.get("value_to_use") is not None:
            reasons.append("diagnostic_fix_action_value_to_use_not_null")

    return sorted(set(reasons))


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    key = value.strip()
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
        return key
    return None


def _safe_text(value: Any, *, fallback: str) -> str:
    if not isinstance(value, str):
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9_.: -]+", "_", value.strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:160] or fallback


def _action_id(action_type: str, key: str | None, reason: str) -> str:
    suffix = key.lower() if key is not None else _safe_text(reason, fallback="manual_review")
    suffix = re.sub(r"[^a-z0-9_.:-]+", "_", suffix.lower()).strip("_")
    return f"{action_type}.{suffix or 'manual_review'}"


def _instruction(action_type: str, key: str | None, reason: str) -> str:
    if action_type == "set_missing_key" and key is not None:
        return (
            f"NOT RUN / FUTURE SLICE ONLY: Set {key} using an "
            "operator-provided private value in a separate approved fix "
            "execution slice."
        )
    if action_type == "replace_invalid_value" and key is not None:
        return (
            f"NOT RUN / FUTURE SLICE ONLY: Replace {key} using an "
            "operator-provided private value in a separate approved fix "
            "execution slice."
        )
    if action_type == "remove_duplicate_key" and key is not None:
        return (
            f"NOT RUN / FUTURE SLICE ONLY: Remove duplicate {key} entries only "
            "after the operator decides which key entry is authoritative."
        )
    return (
        "NOT RUN / FUTURE SLICE ONLY: Manually review the redacted issue "
        f"category {reason} without collecting or displaying values."
    )


def _plan_action(action: Mapping[str, Any]) -> dict[str, Any]:
    raw_type = action.get("action_type")
    action_type = raw_type if isinstance(raw_type, str) and raw_type in ACTION_TYPES else "manual_review"
    key = _safe_key(action.get("key"))
    if action_type != "manual_review" and key is None:
        action_type = "manual_review"
    reason = _safe_text(action.get("reason"), fallback="redacted_diagnostic_issue")
    value_required = action_type in VALUE_REQUIRED_ACTIONS
    return {
        "action_id": _safe_text(
            action.get("action_id"),
            fallback=_action_id(action_type, key, reason),
        ),
        "action_type": action_type,
        "key": key if action_type != "manual_review" else key,
        "reason": reason,
        "value_required_from_operator": value_required,
        "value_to_use": None,
        "permitted_value_source": (
            "operator_private_input_only" if value_required else "not_applicable"
        ),
        "future_operator_instruction_not_run": _instruction(action_type, key, reason),
    }


def _extract_plan_actions(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None, str]] = set()
    for item in _as_list(data.get("redacted_fix_plan")):
        if not isinstance(item, Mapping):
            continue
        action = _plan_action(item)
        dedupe_key = (action["action_type"], action["key"], action["reason"])
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        actions.append(action)
    return actions


def _issue_summary(data: Mapping[str, Any], actions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    missing: set[str] = set()
    empty: set[str] = set()
    invalid: set[str] = set()
    duplicate: set[str] = set()
    malformed_line_count: int | None = None

    for check in _as_list(data.get("redacted_key_checks")):
        if not isinstance(check, Mapping):
            continue
        key = _safe_key(check.get("key"))
        issue_code = check.get("issue_code")
        if key is None or not isinstance(issue_code, str):
            continue
        if issue_code == "missing_required_key":
            missing.add(key)
        elif issue_code == "empty_required_key":
            empty.add(key)
        elif issue_code:
            invalid.add(key)

    inspection = data.get("runtime_env_inspection")
    if isinstance(inspection, Mapping):
        for key in _as_list(inspection.get("duplicate_key_names")):
            safe = _safe_key(key)
            if safe is not None:
                duplicate.add(safe)
        raw_count = inspection.get("malformed_line_count")
        if isinstance(raw_count, int) and raw_count >= 0:
            malformed_line_count = raw_count

    for action in actions:
        key = action.get("key")
        if not isinstance(key, str):
            continue
        if action["action_type"] == "set_missing_key":
            reason = str(action.get("reason", ""))
            if reason == "empty_required_key":
                empty.add(key)
            else:
                missing.add(key)
        elif action["action_type"] == "replace_invalid_value":
            invalid.add(key)
        elif action["action_type"] == "remove_duplicate_key":
            duplicate.add(key)

    return {
        "missing_required_keys": sorted(missing),
        "empty_required_keys": sorted(empty),
        "invalid_format_keys": sorted(invalid),
        "duplicate_key_names": sorted(duplicate),
        "malformed_line_count": malformed_line_count,
        "manual_review_required": any(
            action.get("action_type") == "manual_review" for action in actions
        ),
    }


def _update_diagnostic_input(report: dict[str, Any], data: Mapping[str, Any]) -> None:
    report["diagnostic_input"] = {
        "diagnostic_json_path_provided": True,
        "diagnostic_json_read": True,
        "diagnostic_contract_status": data.get("contract_status")
        if isinstance(data.get("contract_status"), str)
        else None,
        "diagnostic_recommended_next_slice": data.get("recommended_next_slice")
        if isinstance(data.get("recommended_next_slice"), str)
        else None,
        "diagnostic_boundary_check": data.get("boundary_check")
        if isinstance(data.get("boundary_check"), str)
        else None,
        "diagnostic_values_safe": True,
    }


def generate_report(
    *,
    diagnostic_json_path: str | Path | None = None,
    runtime_env_path: str = DEFAULT_RUNTIME_ENV_PATH,
) -> dict[str, Any]:
    path_text = str(diagnostic_json_path) if diagnostic_json_path is not None else None
    report = _base_report(diagnostic_json_path=path_text, runtime_env_path=runtime_env_path)
    if path_text is None:
        return report

    data, read_error = _read_json_file(path_text)
    if read_error == "diagnostic_json_missing_or_unreadable":
        return report
    if read_error is not None:
        report["contract_status"] = "diagnostic_json_unsafe"
        report["unsafe_diagnostic_reasons"] = [read_error]
        return report

    unsafe_reasons = _unsafe_reasons(data)
    if unsafe_reasons:
        if isinstance(data, Mapping):
            _update_diagnostic_input(report, data)
        report["diagnostic_input"]["diagnostic_values_safe"] = False
        report["contract_status"] = "diagnostic_json_unsafe"
        report["recommended_next_slice"] = "defer_manual_review"
        report["unsafe_diagnostic_reasons"] = unsafe_reasons
        return report

    assert isinstance(data, Mapping)
    _update_diagnostic_input(report, data)
    diagnostic_status = report["diagnostic_input"]["diagnostic_contract_status"]
    diagnostic_next = report["diagnostic_input"]["diagnostic_recommended_next_slice"]
    actions = _extract_plan_actions(data)
    report["selected_plan"]["actions"] = actions
    report["issue_summary"] = _issue_summary(data, actions)

    if (
        diagnostic_status == "runtime_env_invalid_diagnostic_ready"
        and diagnostic_next == "tdlib_auth_runtime_env_operator_fix_plan"
    ):
        if actions:
            report["contract_status"] = "runtime_env_operator_fix_plan_ready"
            report["recommended_next_slice"] = "tdlib_auth_runtime_env_operator_fix_execution"
            return report
        report["contract_status"] = "runtime_env_operator_fix_plan_inconclusive"
        report["recommended_next_slice"] = "defer_manual_review"
        report["issue_summary"]["manual_review_required"] = True
        return report

    if diagnostic_status == "runtime_env_shape_appears_valid":
        report["contract_status"] = "runtime_env_shape_already_valid"
        report["recommended_next_slice"] = "tdlib_auth_operator_execution_rerun_after_fix"
        report["issue_summary"]["manual_review_required"] = False
        return report

    if diagnostic_status in INPUT_CONTRACT_STATUSES and diagnostic_next in INPUT_RECOMMENDED_NEXT_SLICES:
        report["contract_status"] = "diagnostic_not_ready"
        report["recommended_next_slice"] = "defer_manual_review"
        report["issue_summary"]["manual_review_required"] = True
        return report

    report["contract_status"] = "diagnostic_not_ready"
    report["recommended_next_slice"] = "defer_manual_review"
    report["issue_summary"]["manual_review_required"] = True
    return report


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = generate_report(
        diagnostic_json_path=args.diagnostic_json,
        runtime_env_path=args.runtime_env_path,
    )
    if report["contract_status"] not in OUTPUT_CONTRACT_STATUSES:
        raise AssertionError(report["contract_status"])
    if report["recommended_next_slice"] not in OUTPUT_RECOMMENDED_NEXT_SLICES:
        raise AssertionError(report["recommended_next_slice"])
    print(render_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
