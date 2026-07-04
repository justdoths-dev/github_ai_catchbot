from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.maintenance.systemd_rollout import (  # noqa: E402
    SERVICE_NAME,
    TARGET_MAINTENANCE_WORKER,
    SystemdContextProofRequest,
    SystemdDiagnosticRequest,
    SystemdRolloutRequest,
    run_systemd_context_proof,
    run_systemd_diagnostic,
    run_systemd_rollout,
)


SCHEMA_VERSION = "restricted_systemd_install_context_runner_v1"
RUNNER_NAME = "bounded_restricted_systemd_install_context_runner"
SUPPORTED_MODES = {"plan", "install", "context-proof", "diagnose", "rollback"}
READ_ONLY_MODES = {"plan", "context-proof", "diagnose"}


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Run a bounded maintenance user-systemd install/context/rollback step with sanitized JSON output.",
        add_help=False,
    )
    parser.add_argument("--mode", metavar="plan|install|context-proof|diagnose|rollback", required=True)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--python-executable", required=True)
    parser.add_argument("--runtime-env-file", required=True)
    parser.add_argument("--systemd-user-dir", required=True)
    parser.add_argument("--confirm-install", action="store_true")
    parser.add_argument("--i-understand-this-writes-user-systemd-unit", action="store_true")
    parser.add_argument("--confirm-rollback", action="store_true")
    parser.add_argument("--i-understand-this-removes-user-systemd-unit", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(_render_json(_blocked_report(str(exc), mode="argument_error")))
        return 1

    mode = str(args.mode)
    try:
        report = run(args)
    except Exception:
        report = _blocked_report("runner_error", mode=mode)
    sys.stdout.write(_render_json(report))
    return 0 if report["ok"] is True else 1


def run(args: argparse.Namespace) -> dict[str, Any]:
    mode = str(args.mode)
    validation_error = _cli_validation_error(args)
    if validation_error is not None:
        return _blocked_report(validation_error, mode=mode)

    paths = _request_paths(args)

    if mode == "context-proof":
        request = SystemdContextProofRequest(
            target=TARGET_MAINTENANCE_WORKER,
            repo_root=paths["repo_root"],
            python_executable=paths["python_executable"],
            runtime_env_file=paths["runtime_env_file"],
            systemd_user_dir=paths["systemd_user_dir"],
            service_name=SERVICE_NAME,
            timer_name=None,
        )
        lower_report = run_systemd_context_proof(request)
        return _final_report(mode=mode, lower_report=lower_report, lower_report_kind="context_proof")

    if mode == "diagnose":
        request = SystemdDiagnosticRequest(
            target=TARGET_MAINTENANCE_WORKER,
            runtime_env_file=paths["runtime_env_file"],
            systemd_user_dir=paths["systemd_user_dir"],
            service_name=SERVICE_NAME,
        )
        lower_report = run_systemd_diagnostic(request)
        return _final_report(mode=mode, lower_report=lower_report, lower_report_kind="diagnostic")

    request = SystemdRolloutRequest(
        mode=mode,
        target=TARGET_MAINTENANCE_WORKER,
        confirm_install=mode == "install",
        confirm_start=False,
        confirm_rollback=mode == "rollback",
        repo_root=paths["repo_root"],
        python_executable=paths["python_executable"],
        runtime_env_file=paths["runtime_env_file"],
        systemd_user_dir=paths["systemd_user_dir"],
        service_name=SERVICE_NAME,
        timer_name=None,
        dry_run=mode == "plan",
    )
    lower_report = run_systemd_rollout(request)
    return _final_report(mode=mode, lower_report=lower_report, lower_report_kind="rollout")


def _cli_validation_error(args: argparse.Namespace) -> str | None:
    mode = str(args.mode)
    if mode not in SUPPORTED_MODES:
        return "mode_not_allowed"

    install_confirmed = bool(args.confirm_install) and bool(args.i_understand_this_writes_user_systemd_unit)
    rollback_confirmed = bool(args.confirm_rollback) and bool(args.i_understand_this_removes_user_systemd_unit)
    any_write_confirmation = any(
        (
            bool(args.confirm_install),
            bool(args.i_understand_this_writes_user_systemd_unit),
            bool(args.confirm_rollback),
            bool(args.i_understand_this_removes_user_systemd_unit),
        )
    )

    if mode in READ_ONLY_MODES and any_write_confirmation:
        return "write_confirmation_not_allowed_for_read_only_mode"
    if mode == "install":
        if bool(args.confirm_rollback) or bool(args.i_understand_this_removes_user_systemd_unit):
            return "rollback_confirmation_not_allowed_for_install_mode"
        if not install_confirmed:
            return "install_confirmation_flags_missing"
    if mode == "rollback":
        if bool(args.confirm_install) or bool(args.i_understand_this_writes_user_systemd_unit):
            return "install_confirmation_not_allowed_for_rollback_mode"
        if not rollback_confirmed:
            return "rollback_confirmation_flags_missing"

    for attr in ("repo_root", "python_executable", "runtime_env_file", "systemd_user_dir"):
        if not Path(str(getattr(args, attr))).expanduser().is_absolute():
            return f"{attr}_not_absolute"
    return None


def _request_paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "repo_root": Path(str(args.repo_root)).expanduser(),
        "python_executable": Path(str(args.python_executable)).expanduser(),
        "runtime_env_file": Path(str(args.runtime_env_file)).expanduser(),
        "systemd_user_dir": Path(str(args.systemd_user_dir)).expanduser(),
    }


def _final_report(*, mode: str, lower_report: Any, lower_report_kind: str) -> dict[str, Any]:
    lower = _dataclass_dict(lower_report)
    status = _safe_status(lower.get("status"))
    reason_code = _safe_reason_code(lower.get("reason_code"))
    ok = status == "pass"
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "ok": ok,
        "status": status,
        "reason_code": reason_code,
        "target_exit_states": _target_exit_states(),
        "systemd_plan_or_readback": _systemd_plan_or_readback(
            mode=mode,
            lower=lower,
            lower_report_kind=lower_report_kind,
        ),
        "authority": _authority(mode=mode, dispatched=True),
        "redactions_applied": _redactions_applied(lower),
        "open_gates": _open_gates(),
        "completion_claims": _completion_claims(),
        "recommended_next_operator_action": _recommended_next_operator_action(mode, status, reason_code),
    }


def _blocked_report(reason_code: str, *, mode: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "ok": False,
        "status": "blocked",
        "reason_code": _safe_reason_code(reason_code),
        "target_exit_states": _target_exit_states(),
        "systemd_plan_or_readback": {
            "lower_report_kind": None,
            "lower_schema_version": None,
            "lower_status": "not_dispatched",
            "lower_reason_code": _safe_reason_code(reason_code),
            "target_bucket": "known_maintenance_worker",
            "service_name_bucket": "known_maintenance_worker_service",
            "raw_paths_omitted": True,
            "raw_unit_content_omitted": True,
        },
        "authority": _authority(mode=mode, dispatched=False),
        "redactions_applied": _redactions_applied({}),
        "open_gates": _open_gates(),
        "completion_claims": _completion_claims(),
        "recommended_next_operator_action": _recommended_next_operator_action(
            mode,
            "blocked",
            _safe_reason_code(reason_code),
        ),
    }


def _systemd_plan_or_readback(*, mode: str, lower: dict[str, Any], lower_report_kind: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "lower_report_kind": lower_report_kind,
        "lower_schema_version": _safe_scalar(lower.get("schema_version")),
        "lower_status": _safe_status(lower.get("status")),
        "lower_reason_code": _safe_reason_code(lower.get("reason_code")),
        "target_bucket": "known_maintenance_worker",
        "service_name_bucket": "known_maintenance_worker_service",
        "raw_paths_omitted": True,
        "raw_unit_content_omitted": True,
    }

    if lower_report_kind == "rollout":
        result.update(
            {
                "unit_plan_created": bool(lower.get("unit_plan_created")),
                "timer_unit_planned": bool(lower.get("timer_name")),
                "install_attempted_by_lower_runner": bool(lower.get("install_attempted")),
                "enable_attempted_by_lower_runner": bool(lower.get("enable_attempted")),
                "start_attempted_by_lower_runner": bool(lower.get("start_attempted")),
                "rollback_attempted_by_lower_runner": bool(lower.get("rollback_attempted")),
                "proof_attempted_by_lower_runner": bool(lower.get("proof_attempted")),
                "service_file_present": bool(lower.get("service_file_present")),
                "timer_file_present": bool(lower.get("timer_file_present")),
                "service_enabled": bool(lower.get("service_enabled")),
                "service_active": bool(lower.get("service_active")),
                "rollback_plan_available": bool(lower.get("rollback_plan_available")),
            }
        )
    elif lower_report_kind == "context_proof":
        result.update(
            {
                "service_name_matches_expected": bool(lower.get("service_name_matches_expected")),
                "service_file_present": bool(lower.get("service_file_present")),
                "service_enabled": bool(lower.get("service_enabled")),
                "service_active": bool(lower.get("service_active")),
                "unit_load_state_bucket": _safe_scalar(lower.get("unit_load_state")),
                "unit_file_state_bucket": _safe_scalar(lower.get("unit_file_state")),
                "exec_start_matches_expected": bool(lower.get("exec_start_matches_expected")),
                "working_directory_matches_expected": bool(lower.get("working_directory_matches_expected")),
                "environment_file_matches_expected": bool(lower.get("environment_file_matches_expected")),
                "restart_policy_matches_expected": bool(lower.get("restart_policy_matches_expected")),
                "restart_sec_matches_expected": bool(lower.get("restart_sec_matches_expected")),
                "expected_python_executable_present": bool(lower.get("expected_python_executable_present")),
                "expected_repo_root_present": bool(lower.get("expected_repo_root_present")),
                "expected_runtime_env_file_present": bool(lower.get("expected_runtime_env_file_present")),
                "unit_context_matches_expected": bool(lower.get("unit_context_matches_expected")),
                "mismatched_context_field_buckets": _safe_string_list(lower.get("mismatched_context_fields")),
            }
        )
    elif lower_report_kind == "diagnostic":
        result.update(
            {
                "service_file_present": bool(lower.get("service_file_present")),
                "service_enabled": bool(lower.get("service_enabled")),
                "service_active": bool(lower.get("service_active")),
                "load_state_bucket": _safe_scalar(lower.get("load_state")),
                "active_state_bucket": _safe_scalar(lower.get("active_state")),
                "sub_state_bucket": _safe_scalar(lower.get("sub_state")),
                "result_bucket": _safe_scalar(lower.get("result")),
                "exec_main_code_bucket": _safe_int_bucket(lower.get("exec_main_code")),
                "exec_main_status_bucket": _safe_int_bucket(lower.get("exec_main_status")),
                "restart_count_bucket": _restart_count_bucket(lower.get("n_restarts")),
                "unit_file_state_bucket": _safe_scalar(lower.get("unit_file_state")),
                "current_invocation_fingerprint_present": bool(lower.get("current_invocation_fingerprint")),
                "restart_likely": bool(lower.get("restart_likely")),
                "exited_after_start_likely": bool(lower.get("exited_after_start_likely")),
                "raw_invocation_id_omitted": bool(lower.get("raw_invocation_id_omitted", True)),
            }
        )
    result["worker_start_exposed"] = mode == "start"
    return result


def _authority(*, mode: str, dispatched: bool) -> dict[str, bool]:
    install_write = dispatched and mode == "install"
    rollback_write = dispatched and mode == "rollback"
    return {
        "systemd_write_attempted": install_write or rollback_write,
        "systemd_start_attempted": False,
        "systemd_stop_attempted": rollback_write,
        "systemd_enable_attempted": install_write,
        "systemd_disable_attempted": rollback_write,
        "unit_file_write_attempted": install_write,
        "unit_file_remove_attempted": rollback_write,
        "daemon_reload_attempted": install_write or rollback_write,
        "worker_started": False,
        "redis_consume_attempted": False,
        "redis_ack_attempted": False,
        "redis_xadd_attempted": False,
        "redis_group_mutation_attempted": False,
        "db_read_attempted": False,
        "db_write_attempted": False,
        "telegram_attempted": False,
        "openai_attempted": False,
        "github_attempted": False,
        "x_attempted": False,
        "web_attempted": False,
        "docker_attempted": False,
        "migration_attempted": False,
        "runtime_env_values_read": False,
        "secrets_output": False,
    }


def _target_exit_states() -> dict[str, bool]:
    return {
        "RESTRICTED_SYSTEMD_INSTALL_RUNNER_CODE_REVIEW_PASS": True,
        "SYSTEMD_CONTEXT_DIAGNOSTIC_READBACK_RUNNER_CODE_REVIEW_PASS": True,
        "SYSTEMD_ROLLBACK_RUNNER_CODE_REVIEW_PASS": True,
    }


def _open_gates() -> dict[str, bool]:
    return {
        "AUTHORITY_OPEN": True,
        "ROLLOUT_OPEN": True,
        "PRODUCTION_ROLLOUT_OPEN": True,
        "FULL_ALWAYS_ON_COLLECTOR_WORKER_OPEN": True,
        "ACTUAL_WORKER_START_OPEN": True,
        "ACTUAL_REDIS_CONSUME_ACK_OPEN": True,
        "ACTUAL_TELEGRAM_SEND_OPEN": True,
    }


def _completion_claims() -> dict[str, bool]:
    return {
        "PRODUCT_COMPLETE_CLOSED": False,
        "final_bot_complete": False,
        "one_hundred_percent_complete": False,
        "production_rollout_complete": False,
        "actual_worker_start_complete": False,
        "actual_redis_consume_ack_complete": False,
        "actual_telegram_send_complete": False,
    }


def _redactions_applied(lower: dict[str, Any]) -> dict[str, bool]:
    return {
        "raw_service_unit_content_omitted": True,
        "raw_exec_start_omitted": True,
        "raw_working_directory_omitted": True,
        "raw_environment_file_path_omitted": True,
        "raw_repo_path_omitted": True,
        "raw_runtime_env_path_omitted": True,
        "runtime_env_values_omitted": True,
        "db_redis_urls_omitted": True,
        "tokens_omitted": True,
        "raw_systemctl_stderr_omitted": True,
        "raw_exception_bodies_omitted": True,
        "raw_invocation_id_omitted": True,
        "raw_source_queue_stream_dedupe_ids_omitted": True,
        "lower_level_redactions_consumed": True,
    }


def _recommended_next_operator_action(mode: str, status: str, reason_code: str | None) -> str:
    if status != "pass":
        if reason_code == "mode_not_allowed":
            return "do_not_use_start_mode_request_separate_reviewed_start_slice"
        return "resolve_blocked_reason_before_retrying_same_bounded_mode"
    if mode == "plan":
        return "review_sanitized_plan_then_run_install_with_both_install_confirmations"
    if mode == "install":
        return "run_context_proof_then_diagnose_before_any_separate_start_review"
    if mode == "context-proof":
        return "run_diagnose_and_keep_worker_start_closed"
    if mode == "diagnose":
        return "request_separate_reviewed_worker_start_authority_if_readback_is_acceptable"
    if mode == "rollback":
        return "confirm_unit_absent_and_keep_production_rollout_open"
    return "stop_and_request_review"


def _dataclass_dict(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return dict(value)
    return {}


def _safe_status(value: Any) -> str:
    text = _safe_scalar(value)
    if text in {"pass", "blocked", "failed", "not_dispatched"}:
        return text
    return "failed"


def _safe_reason_code(value: Any) -> str | None:
    if value is None:
        return None
    return _safe_scalar(value) or "redacted_reason"


def _safe_scalar(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if all(char.isalnum() or char in {"-", "_"} for char in text):
        return text
    return "redacted"


def _safe_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [safe for item in value if (safe := _safe_scalar(item)) is not None]


def _safe_int_bucket(value: Any) -> str | None:
    if value is None:
        return None
    try:
        integer = int(value)
    except (TypeError, ValueError):
        return "non_integer"
    if integer == 0:
        return "zero"
    if integer > 0:
        return "nonzero"
    return "negative"


def _restart_count_bucket(value: Any) -> str | None:
    if value is None:
        return None
    try:
        integer = int(value)
    except (TypeError, ValueError):
        return "non_integer"
    if integer <= 0:
        return "zero"
    if integer == 1:
        return "one"
    return "multiple"


def _render_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
