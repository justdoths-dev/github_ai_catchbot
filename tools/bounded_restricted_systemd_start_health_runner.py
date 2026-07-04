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
    SystemdDiagnosticRequest,
    SystemdRolloutRequest,
    run_systemd_diagnostic,
    run_systemd_rollout,
)


SCHEMA_VERSION = "restricted_systemd_start_health_runner_v1"
RUNNER_NAME = "bounded_restricted_systemd_start_health_runner"
SUPPORTED_MODES = {"start", "proof", "diagnose", "rollback"}
READ_ONLY_MODES = {"proof", "diagnose"}


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Run a bounded maintenance user-systemd start/health/rollback handoff with sanitized JSON output.",
        add_help=False,
    )
    parser.add_argument("--mode", required=True)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--python-executable", required=True)
    parser.add_argument("--runtime-env-file", required=True)
    parser.add_argument("--systemd-user-dir", required=True)
    parser.add_argument("--confirm-start", action="store_true")
    parser.add_argument("--i-understand-this-starts-maintenance-worker", action="store_true")
    parser.add_argument("--confirm-rollback", action="store_true")
    parser.add_argument("--i-understand-this-stops-and-removes-user-systemd-unit", action="store_true")
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

    if mode == "diagnose":
        request = SystemdDiagnosticRequest(
            target=TARGET_MAINTENANCE_WORKER,
            runtime_env_file=paths["runtime_env_file"],
            systemd_user_dir=paths["systemd_user_dir"],
            service_name=SERVICE_NAME,
        )
        try:
            lower_report = run_systemd_diagnostic(request)
        except Exception:
            return _failed_dispatch_report(mode=mode, lower_report_kind="diagnostic")
        return _final_report(mode=mode, lower_report=lower_report, lower_report_kind="diagnostic")

    request = SystemdRolloutRequest(
        mode=mode,
        target=TARGET_MAINTENANCE_WORKER,
        confirm_install=False,
        confirm_start=mode == "start",
        confirm_rollback=mode == "rollback",
        repo_root=paths["repo_root"],
        python_executable=paths["python_executable"],
        runtime_env_file=paths["runtime_env_file"],
        systemd_user_dir=paths["systemd_user_dir"],
        service_name=SERVICE_NAME,
        timer_name=None,
        dry_run=mode == "proof",
    )
    try:
        lower_report = run_systemd_rollout(request)
    except Exception:
        return _failed_dispatch_report(mode=mode, lower_report_kind="rollout")
    return _final_report(mode=mode, lower_report=lower_report, lower_report_kind="rollout")


def _cli_validation_error(args: argparse.Namespace) -> str | None:
    mode = str(args.mode)
    if mode not in SUPPORTED_MODES:
        return "mode_not_allowed"

    start_flags = (
        bool(args.confirm_start),
        bool(args.i_understand_this_starts_maintenance_worker),
    )
    rollback_flags = (
        bool(args.confirm_rollback),
        bool(args.i_understand_this_stops_and_removes_user_systemd_unit),
    )
    start_confirmed = all(start_flags)
    rollback_confirmed = all(rollback_flags)

    if mode in READ_ONLY_MODES and any((*start_flags, *rollback_flags)):
        return "confirmation_not_allowed_for_read_only_mode"
    if mode == "start":
        if any(rollback_flags):
            return "rollback_confirmation_not_allowed_for_start_mode"
        if not start_confirmed:
            return "start_confirmation_flags_missing"
    if mode == "rollback":
        if any(start_flags):
            return "start_confirmation_not_allowed_for_rollback_mode"
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
        "systemd_start_or_health_readback": _systemd_start_or_health_readback(
            mode=mode,
            lower=lower,
            lower_report_kind=lower_report_kind,
        ),
        "authority": _authority(mode=mode, dispatched=True, lower=lower, lower_report_kind=lower_report_kind),
        "redactions_applied": _redactions_applied(),
        "open_gates": _open_gates(),
        "completion_claims": _completion_claims(),
        "recommended_next_operator_action": _recommended_next_operator_action(mode, status, reason_code),
    }


def _blocked_report(reason_code: str, *, mode: str) -> dict[str, Any]:
    safe_reason = _safe_reason_code(reason_code)
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "ok": False,
        "status": "blocked",
        "reason_code": safe_reason,
        "systemd_start_or_health_readback": _not_dispatched_readback(safe_reason),
        "authority": _authority(mode=mode, dispatched=False, lower={}, lower_report_kind=None),
        "redactions_applied": _redactions_applied(),
        "open_gates": _open_gates(),
        "completion_claims": _completion_claims(),
        "recommended_next_operator_action": _recommended_next_operator_action(mode, "blocked", safe_reason),
    }


def _failed_dispatch_report(*, mode: str, lower_report_kind: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "ok": False,
        "status": "failed",
        "reason_code": "runner_error",
        "systemd_start_or_health_readback": {
            "lower_report_kind": lower_report_kind,
            "lower_schema_version": None,
            "lower_status": "failed",
            "lower_reason_code": "runner_error",
            "target_bucket": "known_maintenance_worker",
            "service_name_bucket": "known_maintenance_worker_service",
            "health_readback_source": "not_available",
            "raw_paths_omitted": True,
            "raw_unit_content_omitted": True,
            "raw_systemctl_output_omitted": True,
        },
        "authority": _authority(mode=mode, dispatched=True, lower={}, lower_report_kind=lower_report_kind),
        "redactions_applied": _redactions_applied(),
        "open_gates": _open_gates(),
        "completion_claims": _completion_claims(),
        "recommended_next_operator_action": _recommended_next_operator_action(mode, "failed", "runner_error"),
    }


def _not_dispatched_readback(reason_code: str | None) -> dict[str, Any]:
    return {
        "lower_report_kind": None,
        "lower_schema_version": None,
        "lower_status": "not_dispatched",
        "lower_reason_code": reason_code,
        "target_bucket": "known_maintenance_worker",
        "service_name_bucket": "known_maintenance_worker_service",
        "health_readback_source": "not_dispatched",
        "raw_paths_omitted": True,
        "raw_unit_content_omitted": True,
        "raw_systemctl_output_omitted": True,
    }


def _systemd_start_or_health_readback(
    *,
    mode: str,
    lower: dict[str, Any],
    lower_report_kind: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "lower_report_kind": lower_report_kind,
        "lower_schema_version": _safe_scalar(lower.get("schema_version")),
        "lower_status": _safe_status(lower.get("status")),
        "lower_reason_code": _safe_reason_code(lower.get("reason_code")),
        "target_bucket": "known_maintenance_worker",
        "service_name_bucket": "known_maintenance_worker_service",
        "health_readback_source": _health_readback_source(mode, lower_report_kind),
        "raw_paths_omitted": True,
        "raw_unit_content_omitted": True,
        "raw_systemctl_output_omitted": True,
    }

    if lower_report_kind == "rollout":
        result.update(
            {
                "unit_plan_created": bool(lower.get("unit_plan_created")),
                "timer_unit_planned": bool(lower.get("timer_name")),
                "start_attempted_by_rollout_service": bool(lower.get("start_attempted")),
                "proof_attempted_by_rollout_service": bool(lower.get("proof_attempted")),
                "rollback_attempted_by_rollout_service": bool(lower.get("rollback_attempted")),
                "install_attempted_by_rollout_service": bool(lower.get("install_attempted")),
                "enable_attempted_by_rollout_service": bool(lower.get("enable_attempted")),
                "service_file_present": bool(lower.get("service_file_present")),
                "timer_file_present": bool(lower.get("timer_file_present")),
                "service_enabled": bool(lower.get("service_enabled")),
                "service_active": bool(lower.get("service_active")),
                "rollback_plan_available": bool(lower.get("rollback_plan_available")),
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
    return result


def _health_readback_source(mode: str, lower_report_kind: str | None) -> str:
    if lower_report_kind == "diagnostic":
        return "rollout_diagnostic_readback"
    if mode == "start":
        return "rollout_service_start_stability_readback"
    if mode == "proof":
        return "rollout_service_proof_readback"
    if mode == "rollback":
        return "rollout_service_rollback_readback"
    return "not_available"


def _authority(
    *,
    mode: str,
    dispatched: bool,
    lower: dict[str, Any],
    lower_report_kind: str | None,
) -> dict[str, bool]:
    rollout = lower_report_kind == "rollout"
    diagnostic = lower_report_kind == "diagnostic"
    start_attempted = rollout and bool(lower.get("start_attempted"))
    rollback_attempted = rollout and bool(lower.get("rollback_attempted"))
    service_active = bool(lower.get("service_active")) if rollout or diagnostic else False
    start_dispatched = dispatched and mode == "start"
    return {
        "systemd_start_attempted": start_attempted,
        "systemd_stop_attempted": rollback_attempted,
        "systemd_disable_attempted": rollback_attempted,
        "unit_file_remove_attempted": rollback_attempted,
        "daemon_reload_attempted": rollback_attempted,
        "worker_process_start_attempted": start_attempted,
        "worker_process_active_after_stability_readback": service_active,
        "direct_runner_db_client_constructed": False,
        "direct_runner_redis_client_constructed": False,
        "direct_runner_telegram_client_constructed": False,
        "direct_runner_openai_client_constructed": False,
        "direct_runner_github_client_constructed": False,
        "direct_runner_x_client_constructed": False,
        "direct_runner_web_client_constructed": False,
        "runtime_env_values_read_by_runner": False,
        "secrets_output": False,
        "downstream_runtime_authority_may_open": start_dispatched,
    }


def _redactions_applied() -> dict[str, bool]:
    return {
        "raw_repo_path_omitted": True,
        "raw_python_executable_path_omitted": True,
        "raw_runtime_env_path_omitted": True,
        "raw_systemd_user_dir_omitted": True,
        "raw_service_unit_content_omitted": True,
        "raw_exec_start_omitted": True,
        "raw_working_directory_omitted": True,
        "raw_environment_file_omitted": True,
        "runtime_env_values_omitted": True,
        "raw_systemctl_stdout_omitted": True,
        "raw_systemctl_stderr_omitted": True,
        "raw_exception_bodies_omitted": True,
        "raw_invocation_id_omitted": True,
        "raw_urls_omitted": True,
        "tokens_omitted": True,
        "db_redis_urls_omitted": True,
        "raw_source_text_omitted": True,
        "raw_queue_ids_omitted": True,
        "raw_dedupe_keys_omitted": True,
        "raw_origin_remote_url_omitted": True,
        "lower_level_redactions_consumed": True,
    }


def _open_gates() -> dict[str, bool]:
    return {
        "AUTHORITY_OPEN": True,
        "ROLLOUT_OPEN": True,
        "PRODUCTION_ROLLOUT_OPEN": True,
        "ACTUAL_REDIS_CONSUME_ACK_OPEN": True,
        "ACTUAL_TELEGRAM_SEND_OPEN": True,
        "FULL_LIVE_COLLECTOR_OPEN": True,
    }


def _completion_claims() -> dict[str, bool]:
    return {
        "PRODUCT_COMPLETE_CLOSED": False,
        "final_bot_complete": False,
        "one_hundred_percent_complete": False,
        "production_rollout_complete": False,
        "actual_telegram_send_complete": False,
    }


def _recommended_next_operator_action(mode: str, status: str, reason_code: str | None) -> str:
    if status != "pass":
        if reason_code == "start_confirmation_flags_missing":
            return "review_then_retry_start_with_both_start_confirmations"
        if reason_code == "rollback_confirmation_flags_missing":
            return "review_then_retry_rollback_with_both_rollback_confirmations"
        if reason_code == "mode_not_allowed":
            return "use_install_context_runner_for_install_plan_context_or_stop"
        return "resolve_blocked_or_failed_reason_before_retrying_same_bounded_mode"
    if mode == "start":
        return "run_proof_then_diagnose_to_capture_post_start_health_readback"
    if mode == "proof":
        return "run_diagnose_if_additional_failure_classification_is_needed"
    if mode == "diagnose":
        return "review_redacted_health_readback_before_any_next_rollout_step"
    if mode == "rollback":
        return "confirm_rollback_readback_and_keep_production_rollout_open"
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
