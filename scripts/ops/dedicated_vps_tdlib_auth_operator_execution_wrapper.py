from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Awaitable, Callable, Sequence


REPORT_TYPE = "dedicated_vps_tdlib_auth_operator_execution_wrapper_v1"
RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
MISSING_NEXT_SLICE = "dedicated_vps_tdlib_auth_entrypoint_implementation"
AVAILABLE_NEXT_SLICE = "dedicated_vps_tdlib_auth_operator_execution"
DEFAULT_TDLIB_AUTH_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_AUTH_MAX_AUTHORIZATION_UPDATES = 120
MAX_TDLIB_AUTH_RECEIVE_TIMEOUT_SEC = 5.0
MAX_TDLIB_AUTH_MAX_AUTHORIZATION_UPDATES = 600
MAX_TDLIB_AUTH_RECEIVE_BUDGET_SECONDS = 600.0

COLLECTOR_SOURCE_FILES = (
    "src/services/collector_telegram/auth_entrypoint.py",
    "src/services/collector_telegram/auth_fsm.py",
    "src/services/collector_telegram/tdlib_client.py",
    "src/services/collector_telegram/runtime.py",
    "src/services/collector_telegram/service.py",
    "src/services/collector_telegram/main.py",
)

AUTH_ONLY_ENTRYPOINT_FILE = "src/services/collector_telegram/auth_entrypoint.py"
AUTH_ONLY_ENTRYPOINT_LABEL = "src.services.collector_telegram.auth_entrypoint"

AUTH_ONLY_ENTRYPOINT_MARKERS = (
    "AUTH_ONLY_ENTRYPOINT_LABEL",
    "TDLibAuthOnlyRunner",
    "TDLibAuthOnlyResult",
    "tdlib_auth_operator_execution",
    "auth_only_entrypoint",
    "run_tdlib_auth_only",
    "run_auth_only",
    "tdlib_auth_main",
    "authenticate_tdlib",
)

RUNTIME_ENTRYPOINT_MARKERS = (
    "CollectorTelegramService",
    "CollectorRuntime",
    "service.run",
    "asyncio.run(_run())",
)

SIDE_EFFECT_FLAGS = (
    "runtime_env_read",
    "runtime_env_values_printed",
    "secret_values_printed",
    "tdlib_auth_attempted",
    "tdlib_auth_completed",
    "manual_intervention_required",
    "telegram_connected",
    "session_state_created_or_reused",
    "db_connected",
    "redis_connected",
    "database_connected",
    "alembic_run",
    "app_runtime_started",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "collector_main_used",
    "collector_service_used",
    "collector_runtime_used",
    "systemd_or_docker_changed",
    "files_mutated",
    "network_called",
)

RealTransportFactory = Callable[[], Any]
AuthRunner = Callable[..., Awaitable[Any]]
RuntimeEnvReader = Callable[[str | Path], dict[str, str]]


@dataclass(frozen=True, slots=True)
class WrapperResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReceiveBudgetValidation:
    receive_timeout_sec: float | None
    max_authorization_updates: int | None
    receive_budget_seconds: float | None
    errors: tuple[str, ...]


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_repo_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _load_auth_entrypoint_module(repo_root: Path) -> ModuleType:
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)
    from src.services.collector_telegram import auth_entrypoint

    return auth_entrypoint


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def _parse_receive_budget(
    receive_timeout_sec_value: object,
    max_authorization_updates_value: object,
) -> ReceiveBudgetValidation:
    errors: list[str] = []
    receive_timeout_sec: float | None = None
    max_authorization_updates: int | None = None
    receive_budget_seconds: float | None = None

    if isinstance(receive_timeout_sec_value, bool):
        errors.append("receive_timeout_sec.invalid")
    else:
        try:
            parsed_timeout = float(receive_timeout_sec_value)
        except (TypeError, ValueError):
            errors.append("receive_timeout_sec.invalid")
        else:
            if not math.isfinite(parsed_timeout):
                errors.append("receive_timeout_sec.not_finite")
            else:
                receive_timeout_sec = parsed_timeout
                if not 0 < receive_timeout_sec <= MAX_TDLIB_AUTH_RECEIVE_TIMEOUT_SEC:
                    errors.append("receive_timeout_sec.out_of_bounds")

    if isinstance(max_authorization_updates_value, bool):
        errors.append("max_authorization_updates.invalid")
    elif isinstance(max_authorization_updates_value, int):
        max_authorization_updates = max_authorization_updates_value
    elif isinstance(max_authorization_updates_value, str):
        stripped_updates = max_authorization_updates_value.strip()
        if re.fullmatch(r"[+-]?\d+", stripped_updates):
            max_authorization_updates = int(stripped_updates)
        else:
            errors.append("max_authorization_updates.invalid")
    else:
        errors.append("max_authorization_updates.invalid")

    if max_authorization_updates is not None and (
        not 1 <= max_authorization_updates <= MAX_TDLIB_AUTH_MAX_AUTHORIZATION_UPDATES
    ):
        errors.append("max_authorization_updates.out_of_bounds")

    if receive_timeout_sec is not None and max_authorization_updates is not None:
        candidate_budget_seconds = receive_timeout_sec * max_authorization_updates
        if math.isfinite(candidate_budget_seconds):
            receive_budget_seconds = candidate_budget_seconds
            if not errors and receive_budget_seconds > MAX_TDLIB_AUTH_RECEIVE_BUDGET_SECONDS:
                errors.append("receive_budget_seconds.out_of_bounds")
        elif not errors:
            errors.append("receive_budget_seconds.out_of_bounds")

    return ReceiveBudgetValidation(
        receive_timeout_sec=receive_timeout_sec,
        max_authorization_updates=max_authorization_updates,
        receive_budget_seconds=receive_budget_seconds,
        errors=tuple(errors),
    )


def _inspect_auth_only_entrypoint(repo_root: Path) -> dict[str, Any]:
    inspected: list[str] = []
    missing_files: list[str] = []
    marker_hits: list[dict[str, str]] = []

    for relative in COLLECTOR_SOURCE_FILES:
        path = repo_root / relative
        text = _read_repo_text(path)
        if not text:
            missing_files.append(relative)
            continue
        inspected.append(relative)
        lowered = text.lower()
        for marker in AUTH_ONLY_ENTRYPOINT_MARKERS:
            if marker.lower() in lowered:
                marker_hits.append({"file": relative, "marker": marker})

    main_text = _read_repo_text(repo_root / "src/services/collector_telegram/main.py")
    main_is_runtime_entrypoint = all(marker in main_text for marker in RUNTIME_ENTRYPOINT_MARKERS)

    auth_entrypoint_text = _read_repo_text(repo_root / AUTH_ONLY_ENTRYPOINT_FILE)
    auth_entrypoint_has_markers = bool(auth_entrypoint_text) and all(
        marker in auth_entrypoint_text
        for marker in (
            "TDLibAuthOnlyRunner",
            "TDLibAuthOnlyResult",
            "run_tdlib_auth_only_once",
        )
    )
    auth_entrypoint_imports_runtime = any(
        marker in auth_entrypoint_text
        for marker in (
            "CollectorTelegramService",
            "CollectorRuntime",
            "from .service import",
            "from .runtime import",
            "asyncio.run(",
        )
    )

    # Auth-building components alone are not a standalone operator entrypoint.
    # The dedicated auth_entrypoint module is acceptable only if it stays
    # separate from collector runtime/service/main.
    available_hits = [
        hit
        for hit in marker_hits
        if hit["file"] == AUTH_ONLY_ENTRYPOINT_FILE
    ]
    if auth_entrypoint_has_markers and available_hits and not auth_entrypoint_imports_runtime:
        return {
            "auth_only_entrypoint_status": "available",
            "selected_entrypoint": AUTH_ONLY_ENTRYPOINT_LABEL,
            "entrypoint_evidence": available_hits,
            "inspected_source_files": inspected,
            "missing_source_files": missing_files,
            "collector_main_runtime_entrypoint": main_is_runtime_entrypoint,
            "auth_entrypoint_imports_runtime": auth_entrypoint_imports_runtime,
        }

    return {
        "auth_only_entrypoint_status": "missing",
        "selected_entrypoint": None,
        "entrypoint_evidence": marker_hits,
        "inspected_source_files": inspected,
        "missing_source_files": missing_files,
        "collector_main_runtime_entrypoint": main_is_runtime_entrypoint,
        "auth_entrypoint_imports_runtime": auth_entrypoint_imports_runtime,
    }


def generate_report(
    repo_root: Path | None = None,
    *,
    approved_tdlib_auth_operator_execution: bool = False,
    runtime_env_path: str | Path = RUNTIME_ENV_PATH,
    tdlib_auth_receive_timeout_sec: object = DEFAULT_TDLIB_AUTH_RECEIVE_TIMEOUT_SEC,
    tdlib_auth_max_authorization_updates: object = DEFAULT_TDLIB_AUTH_MAX_AUTHORIZATION_UPDATES,
    real_transport_factory: RealTransportFactory | None = None,
    auth_runner: AuthRunner | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
) -> WrapperResult:
    repo_root = repo_root or default_repo_root()
    inspection = _inspect_auth_only_entrypoint(repo_root)
    status = inspection["auth_only_entrypoint_status"]
    available = status == "available"

    checks_failed: list[str] = []
    failures: list[dict[str, str]] = []
    blocked_reason: str | None = None
    receive_budget = _parse_receive_budget(
        tdlib_auth_receive_timeout_sec,
        tdlib_auth_max_authorization_updates,
    )
    receive_timeout_sec = receive_budget.receive_timeout_sec
    max_authorization_updates = receive_budget.max_authorization_updates
    receive_budget_seconds = receive_budget.receive_budget_seconds

    report: dict[str, Any] = {
        "report_type": REPORT_TYPE,
        "contract_status": "approval_required",
        "approval_required": not approved_tdlib_auth_operator_execution,
        "approved_execution_requested": approved_tdlib_auth_operator_execution,
        "auth_only_entrypoint_status": status,
        "selected_entrypoint": inspection["selected_entrypoint"],
        "runtime_env_path": str(runtime_env_path),
        "tdlib_auth_receive_timeout_sec": receive_timeout_sec,
        "tdlib_auth_max_authorization_updates": max_authorization_updates,
        "tdlib_auth_receive_budget_seconds": receive_budget_seconds,
        "checks_failed": checks_failed,
        "failures": failures,
        "likely_next_slice": AVAILABLE_NEXT_SLICE if available else MISSING_NEXT_SLICE,
        "blocked_reason": blocked_reason,
        "entrypoint_assessment": {
            "collector_main_runtime_entrypoint": inspection["collector_main_runtime_entrypoint"],
            "auth_entrypoint_imports_runtime": inspection["auth_entrypoint_imports_runtime"],
            "entrypoint_evidence": inspection["entrypoint_evidence"],
            "inspected_source_files": inspection["inspected_source_files"],
            "missing_source_files": inspection["missing_source_files"],
            "telegram_bot_token_used_for_tdlib_auth": False,
        },
        "tdlib_parameters_shape_guard": {
            "checked": False,
            "valid": False,
            "errors": [],
        },
        "tdlib_parameters_semantic_guard": {
            "checked": False,
            "valid": False,
            "errors": [],
        },
    }
    for flag in SIDE_EFFECT_FLAGS:
        report[flag] = False

    def fail(check: str, message: str, *, contract_status: str, reason: str | None = None) -> WrapperResult:
        checks_failed.append(check)
        failures.append({"check": check, "message": message})
        report["contract_status"] = contract_status
        report["blocked_reason"] = reason
        return WrapperResult(exit_code=1, report=report)

    if receive_budget.errors:
        report["tdlib_auth_receive_budget_errors"] = list(receive_budget.errors)
        return fail(
            "tdlib_auth_receive_budget.invalid",
            "Approved TDLib auth receive budget override is outside bounded limits.",
            contract_status="blocked_tdlib_auth_receive_budget_invalid",
            reason="tdlib_auth_receive_budget_invalid",
        )

    if not available:
        return fail(
            "auth_only_entrypoint.missing",
            (
                "No safe standalone TDLib-auth-only entrypoint exists in the "
                "current repository."
            ),
            contract_status="blocked",
            reason="auth_only_entrypoint_missing",
        )

    if not approved_tdlib_auth_operator_execution:
        return fail(
            "approval.required",
            "TDLib auth operator execution requires separate explicit approval.",
            contract_status="approval_required",
        )

    assert receive_timeout_sec is not None
    assert max_authorization_updates is not None

    auth_module = _load_auth_entrypoint_module(repo_root)
    from src.services.collector_telegram.tdlib_client import (
        build_set_tdlib_parameters_payload,
        tdlib_parameters_semantic_errors,
        tdlib_parameters_shape_errors,
    )

    env_reader = runtime_env_reader or parse_runtime_env_file
    try:
        runtime_env = env_reader(runtime_env_path)
    except OSError:
        return fail(
            "runtime_env.unreadable",
            "Approved TDLib auth execution could not read runtime.env inside Python.",
            contract_status="blocked_runtime_env_unreadable",
            reason="runtime_env_unreadable",
        )
    report["runtime_env_read"] = True

    try:
        config = auth_module.CollectorTelegramConfig.from_env(runtime_env)
    except Exception as exc:
        return fail(
            "runtime_env.invalid",
            f"Approved TDLib auth execution could not build collector config: {type(exc).__name__}.",
            contract_status="blocked_runtime_env_invalid",
            reason="runtime_env_invalid",
        )

    parameter_payload = build_set_tdlib_parameters_payload(config)
    parameter_errors = tdlib_parameters_shape_errors(parameter_payload)
    report["tdlib_parameters_shape_guard"] = {
        "checked": True,
        "valid": not parameter_errors,
        "errors": list(parameter_errors),
    }
    if parameter_errors:
        return fail(
            "tdlib_parameters.invalid",
            (
                "Approved TDLib auth execution blocked before TDLib invocation "
                "because the redacted setTdlibParameters payload shape is invalid."
            ),
            contract_status="blocked_tdlib_parameters_invalid",
            reason="tdlib_parameters_invalid",
        )

    parameter_semantic_errors = tdlib_parameters_semantic_errors(parameter_payload)
    report["tdlib_parameters_semantic_guard"] = {
        "checked": True,
        "valid": not parameter_semantic_errors,
        "errors": list(parameter_semantic_errors),
    }
    if parameter_semantic_errors:
        return fail(
            "tdlib_parameters.semantic_invalid",
            (
                "Approved TDLib auth execution blocked before TDLib invocation "
                "because the redacted setTdlibParameters payload semantics are invalid."
            ),
            contract_status="blocked_tdlib_parameters_semantic_invalid",
            reason="tdlib_parameters_semantic_invalid",
        )

    transport_factory = real_transport_factory or auth_module.build_real_tdlib_transport
    try:
        transport = transport_factory()
    except Exception:
        return fail(
            "tdlib.real_transport_missing",
            "Approved TDLib auth execution is blocked because the real tdjson transport is unavailable.",
            contract_status="blocked_real_transport_missing",
            reason="blocked_real_transport_missing",
        )

    try:
        config.ensure_runtime_dirs()
        report["session_state_created_or_reused"] = True
    except OSError:
        return fail(
            "tdlib.session_state_unavailable",
            "Approved TDLib auth execution could not create or reuse TDLib session directories.",
            contract_status="blocked_session_state_unavailable",
            reason="session_state_unavailable",
        )

    runner = auth_runner or auth_module.run_tdlib_auth_only_once
    try:
        auth_result = asyncio.run(
            runner(
                config,
                transport=transport,
                receive_timeout_sec=receive_timeout_sec,
                max_authorization_updates=max_authorization_updates,
            )
        )
    except Exception as exc:
        return fail(
            "auth_only_entrypoint.unhandled_error",
            f"Auth-only entrypoint returned an unhandled error: {type(exc).__name__}.",
            contract_status="auth_only_entrypoint_error",
            reason="auth_only_entrypoint_error",
        )

    auth_payload = auth_result.to_redacted_dict()
    report["auth_only_entrypoint_status"] = auth_payload["auth_entrypoint_status"]
    report["tdlib_auth_attempted"] = bool(auth_payload["tdlib_auth_attempted"])
    report["tdlib_auth_completed"] = bool(auth_payload["tdlib_auth_completed"])
    report["manual_intervention_required"] = bool(auth_payload["manual_intervention_required"])
    report["telegram_connected"] = bool(auth_payload["telegram_connected"])
    report["runtime_env_values_printed"] = bool(auth_payload["runtime_env_values_printed"])
    report["secret_values_printed"] = bool(auth_payload["secret_values_printed"])
    report["database_connected"] = bool(auth_payload["database_connected"])
    report["db_connected"] = bool(auth_payload["database_connected"])
    report["redis_connected"] = bool(auth_payload["redis_connected"])
    report["alembic_run"] = bool(auth_payload["alembic_run"])
    report["app_runtime_started"] = bool(auth_payload["app_runtime_started"])
    report["live_collector_started"] = bool(auth_payload["live_collector_started"])
    report["notifier_transport_enabled"] = bool(auth_payload["notifier_transport_enabled"])
    report["production_rollout_performed"] = bool(auth_payload["production_rollout_performed"])
    report["collector_main_used"] = bool(auth_payload["collector_main_imported"])
    report["collector_runtime_used"] = bool(auth_payload["collector_runtime_started"])
    report["network_called"] = bool(auth_payload["tdlib_auth_attempted"])
    report["auth_only_entrypoint_result"] = auth_payload

    if auth_payload["tdlib_auth_completed"]:
        report["contract_status"] = "tdlib_auth_completed"
        return WrapperResult(exit_code=0, report=report)
    if auth_payload["manual_intervention_required"]:
        report["contract_status"] = "manual_intervention_required"
        return WrapperResult(exit_code=1, report=report)

    checks_failed.append("auth_only_entrypoint.not_completed")
    failures.append(
        {
            "check": "auth_only_entrypoint.not_completed",
            "message": "Auth-only entrypoint did not complete TDLib authorization.",
        }
    )
    report["contract_status"] = "auth_only_entrypoint_not_completed"

    return WrapperResult(exit_code=1, report=report)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Report the safe TDLib auth operator execution wrapper decision. "
            "Default mode reads repository source text only and performs no auth."
        )
    )
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--runtime-env-path", default=RUNTIME_ENV_PATH)
    parser.add_argument("--approved-tdlib-auth-operator-execution", action="store_true")
    parser.add_argument(
        "--tdlib-auth-receive-timeout-sec",
        default=DEFAULT_TDLIB_AUTH_RECEIVE_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-auth-max-authorization-updates",
        default=DEFAULT_TDLIB_AUTH_MAX_AUTHORIZATION_UPDATES,
    )
    return parser


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, allow_nan=False, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root) if args.repo_root else default_repo_root()
    result = generate_report(
        repo_root=repo_root,
        approved_tdlib_auth_operator_execution=args.approved_tdlib_auth_operator_execution,
        runtime_env_path=args.runtime_env_path,
        tdlib_auth_receive_timeout_sec=args.tdlib_auth_receive_timeout_sec,
        tdlib_auth_max_authorization_updates=args.tdlib_auth_max_authorization_updates,
    )

    if args.format == "json":
        print(render_json(result.report))
    else:
        print(f"contract_status={result.report['contract_status']}")
        print(f"auth_only_entrypoint_status={result.report['auth_only_entrypoint_status']}")
        print(f"likely_next_slice={result.report['likely_next_slice']}")
        print(f"tdlib_auth_receive_timeout_sec={result.report['tdlib_auth_receive_timeout_sec']}")
        print(f"tdlib_auth_max_authorization_updates={result.report['tdlib_auth_max_authorization_updates']}")
        print(f"tdlib_auth_receive_budget_seconds={result.report['tdlib_auth_receive_budget_seconds']}")
        for failure in result.report["failures"]:
            print(f"- {failure['check']}: {failure['message']}")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
