from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from typing import Any

from .delivery_gate_preflight import (
    RECOMMENDED_FLAG_PATCH_KEYS,
    SCHEMA_VERSION as PREFLIGHT_SCHEMA_VERSION,
)


SCHEMA_VERSION = "delivery_gate_preflight_invocation_proof_v1"
SUPPORTED_OUTPUT_FORMAT = "json"
SUPPORTED_MODES = frozenset({"restricted", "full"})
SUPPORTED_GATE_STATUSES = frozenset({"pass", "warn", "fail"})

DELIVERY_GATE_PREFLIGHT_INVOCATION_INVALID_JSON = "delivery_gate_preflight_invocation_invalid_json"
DELIVERY_GATE_PREFLIGHT_INVOCATION_SCHEMA_MISMATCH = "delivery_gate_preflight_invocation_schema_mismatch"
DELIVERY_GATE_PREFLIGHT_INVOCATION_AUTHORITY_MISSING = "delivery_gate_preflight_invocation_authority_missing"
DELIVERY_GATE_PREFLIGHT_INVOCATION_AUTHORITY_OPENED = "delivery_gate_preflight_invocation_authority_opened"
DELIVERY_GATE_PREFLIGHT_INVOCATION_FLAG_PATCH_INVALID = "delivery_gate_preflight_invocation_flag_patch_invalid"
DELIVERY_GATE_PREFLIGHT_INVOCATION_SENSITIVE_OUTPUT_DETECTED = (
    "delivery_gate_preflight_invocation_sensitive_output_detected"
)
DELIVERY_GATE_PREFLIGHT_INVOCATION_COMMAND_FAILED_WITHOUT_JSON = (
    "delivery_gate_preflight_invocation_command_failed_without_json"
)
DELIVERY_GATE_PREFLIGHT_INVOCATION_GATE_STATUS_MISMATCH = "delivery_gate_preflight_invocation_gate_status_mismatch"
DELIVERY_GATE_PREFLIGHT_INVOCATION_UNSUPPORTED_REQUIRED_GATE_STATUS = (
    "delivery_gate_preflight_invocation_unsupported_required_gate_status"
)
DELIVERY_GATE_PREFLIGHT_INVOCATION_UNSUPPORTED_MODE = "delivery_gate_preflight_invocation_unsupported_mode"
DELIVERY_GATE_PREFLIGHT_INVOCATION_UNSUPPORTED_OUTPUT = "delivery_gate_preflight_invocation_unsupported_output"

AUTHORITY = {
    "telegram_called": False,
    "openai_called": False,
    "github_called": False,
    "redis_mutation": False,
    "workers_started": False,
    "database_mutation": False,
    "production_db_write": False,
    "env_file_mutated": False,
    "feature_flags_applied": False,
    "alembic_or_ddl_ran": False,
    "subprocess_started": False,
    "shell_invoked": False,
}
PREFLIGHT_REQUIRED_AUTHORITY_KEYS = tuple(key for key in AUTHORITY if key not in {"subprocess_started", "shell_invoked"})
SENSITIVE_SENTINELS = (
    "DATABASE_URL",
    "REDIS_URL",
    "TELEGRAM_BOT_TOKEN",
    "OPENAI_API_KEY",
    "GITHUB_TOKEN",
    "password",
    "Traceback",
    "RuntimeError",
    "raw exception",
)


@dataclass(frozen=True, slots=True)
class CapturedPreflightInvocation:
    exit_code: int | None
    stdout: str
    stderr: str = ""
    command_raised: bool = False


PreflightInvoker = Callable[[list[str]], Awaitable[CapturedPreflightInvocation]]


async def run_delivery_gate_preflight_invocation_proof(
    *,
    mode: str,
    output: str,
    require_gate_status: str | None = None,
    operator_review_passed: bool = False,
    invoke_preflight: PreflightInvoker | None = None,
    emit_json: Callable[[str], None] = print,
) -> int:
    if invoke_preflight is None:
        invoke_preflight = _invoke_delivery_gate_preflight_cli
    report = await _build_invocation_proof_report(
        mode=mode,
        output=output,
        require_gate_status=require_gate_status,
        operator_review_passed=operator_review_passed,
        invoke_preflight=invoke_preflight,
    )
    emit_json(_to_json(report))
    return 0 if report["proof_status"] == "pass" else 1


async def _build_invocation_proof_report(
    *,
    mode: str,
    output: str,
    require_gate_status: str | None,
    operator_review_passed: bool,
    invoke_preflight: PreflightInvoker,
) -> dict[str, Any]:
    if output != SUPPORTED_OUTPUT_FORMAT:
        return _base_payload(
            mode=mode,
            proof_reason_codes=[DELIVERY_GATE_PREFLIGHT_INVOCATION_UNSUPPORTED_OUTPUT],
        )
    if mode not in SUPPORTED_MODES:
        return _base_payload(
            mode=mode,
            proof_reason_codes=[DELIVERY_GATE_PREFLIGHT_INVOCATION_UNSUPPORTED_MODE],
        )
    if require_gate_status is not None and require_gate_status not in SUPPORTED_GATE_STATUSES:
        return _base_payload(
            mode=mode,
            proof_reason_codes=[DELIVERY_GATE_PREFLIGHT_INVOCATION_UNSUPPORTED_REQUIRED_GATE_STATUS],
        )

    captured = await invoke_preflight(_preflight_argv(mode=mode, operator_review_passed=operator_review_passed))
    captured_output = f"{captured.stdout}\n{captured.stderr}"
    output_sanitized = _output_is_sanitized(captured_output)
    json_valid, parsed = _parse_json(captured.stdout)
    report = parsed if isinstance(parsed, dict) else {}

    reason_codes: list[str] = []
    if not output_sanitized:
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_SENSITIVE_OUTPUT_DETECTED)
    if captured.command_raised and not json_valid:
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_COMMAND_FAILED_WITHOUT_JSON)
    elif not json_valid:
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_INVALID_JSON)

    safe_report = report if output_sanitized and json_valid else {}
    if output_sanitized and json_valid:
        _validate_preflight_report(
            report=safe_report,
            require_gate_status=require_gate_status,
            reason_codes=reason_codes,
        )

    return _base_payload(
        mode=mode,
        preflight_exit_code=captured.exit_code,
        preflight_gate_status=_string_or_none(safe_report.get("gate_status")),
        preflight_blocking_reason_codes=_string_list_or_empty(safe_report.get("blocking_reason_codes")),
        preflight_warning_reason_codes=_string_list_or_empty(safe_report.get("warning_reason_codes")),
        preflight_schema_version=_string_or_none(safe_report.get("schema_version")),
        preflight_json_valid=json_valid,
        preflight_authority_all_false=_authority_all_false(safe_report.get("authority")),
        preflight_recommended_flag_patch_keys_valid=_recommended_flag_patch_keys_valid(
            safe_report.get("recommended_flag_patch")
        ),
        preflight_output_sanitized=output_sanitized,
        preflight_report=safe_report,
        proof_reason_codes=_dedupe_stable(reason_codes),
    )


async def _invoke_delivery_gate_preflight_cli(argv: list[str]) -> CapturedPreflightInvocation:
    from . import main as maintenance_main

    stdout = StringIO()
    stderr = StringIO()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = await maintenance_main._run(argv)
    except Exception:
        return CapturedPreflightInvocation(
            exit_code=None,
            stdout=stdout.getvalue(),
            stderr=stderr.getvalue(),
            command_raised=True,
        )
    return CapturedPreflightInvocation(
        exit_code=exit_code,
        stdout=stdout.getvalue(),
        stderr=stderr.getvalue(),
        command_raised=False,
    )


def _preflight_argv(*, mode: str, operator_review_passed: bool) -> list[str]:
    argv = ["delivery-gate-preflight", "--mode", mode, "--output", "json"]
    if operator_review_passed:
        argv.append("--operator-review-passed")
    return argv


def _validate_preflight_report(
    *,
    report: dict[str, Any],
    require_gate_status: str | None,
    reason_codes: list[str],
) -> None:
    if report.get("schema_version") != PREFLIGHT_SCHEMA_VERSION:
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_SCHEMA_MISMATCH)
    authority = report.get("authority")
    if not isinstance(authority, dict):
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_AUTHORITY_MISSING)
    elif not _authority_all_false(authority):
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_AUTHORITY_OPENED)
    if not _recommended_flag_patch_keys_valid(report.get("recommended_flag_patch")):
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_FLAG_PATCH_INVALID)

    gate_status = report.get("gate_status")
    if require_gate_status is not None and gate_status != require_gate_status:
        reason_codes.append(DELIVERY_GATE_PREFLIGHT_INVOCATION_GATE_STATUS_MISMATCH)


def _base_payload(
    *,
    mode: str,
    preflight_exit_code: int | None = None,
    preflight_gate_status: str | None = None,
    preflight_blocking_reason_codes: list[str] | None = None,
    preflight_warning_reason_codes: list[str] | None = None,
    preflight_schema_version: str | None = None,
    preflight_json_valid: bool = False,
    preflight_authority_all_false: bool = False,
    preflight_recommended_flag_patch_keys_valid: bool = False,
    preflight_output_sanitized: bool = True,
    preflight_report: dict[str, Any] | None = None,
    proof_reason_codes: list[str] | None = None,
) -> dict[str, Any]:
    reason_codes = proof_reason_codes or []
    return {
        "schema_version": SCHEMA_VERSION,
        "proof_status": "fail" if reason_codes else "pass",
        "proof_reason_codes": reason_codes,
        "mode": mode,
        "preflight_exit_code": preflight_exit_code,
        "preflight_gate_status": preflight_gate_status,
        "preflight_blocking_reason_codes": preflight_blocking_reason_codes or [],
        "preflight_warning_reason_codes": preflight_warning_reason_codes or [],
        "preflight_schema_version": preflight_schema_version,
        "preflight_json_valid": preflight_json_valid,
        "preflight_authority_all_false": preflight_authority_all_false,
        "preflight_recommended_flag_patch_keys_valid": preflight_recommended_flag_patch_keys_valid,
        "preflight_output_sanitized": preflight_output_sanitized,
        "preflight_report": preflight_report or {},
        "authority": dict(AUTHORITY),
    }


def _parse_json(raw: str) -> tuple[bool, object | None]:
    try:
        return True, json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return False, None


def _authority_all_false(authority: object) -> bool:
    if not isinstance(authority, dict):
        return False
    for key in PREFLIGHT_REQUIRED_AUTHORITY_KEYS:
        if authority.get(key) is not False:
            return False
    for key in AUTHORITY:
        if authority.get(key) is True:
            return False
    return True


def _recommended_flag_patch_keys_valid(value: object) -> bool:
    return isinstance(value, dict) and tuple(value) == RECOMMENDED_FLAG_PATCH_KEYS


def _output_is_sanitized(value: str) -> bool:
    return not any(sentinel in value for sentinel in SENSITIVE_SENTINELS)


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _string_list_or_empty(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _dedupe_stable(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, indent=2, sort_keys=True)
