from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_tdlib_auth_operator_execution_wrapper_v1"
RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
MISSING_NEXT_SLICE = "dedicated_vps_tdlib_auth_entrypoint_implementation"
AVAILABLE_NEXT_SLICE = "dedicated_vps_tdlib_auth_operator_execution"

COLLECTOR_SOURCE_FILES = (
    "src/services/collector_telegram/auth_fsm.py",
    "src/services/collector_telegram/tdlib_client.py",
    "src/services/collector_telegram/runtime.py",
    "src/services/collector_telegram/service.py",
    "src/services/collector_telegram/main.py",
)

AUTH_ONLY_ENTRYPOINT_MARKERS = (
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
    "tdlib_auth_attempted",
    "tdlib_auth_completed",
    "telegram_connected",
    "session_state_created_or_reused",
    "database_connected",
    "redis_connected",
    "alembic_run",
    "app_runtime_started",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "files_mutated",
    "network_called",
)


@dataclass(frozen=True, slots=True)
class WrapperResult:
    exit_code: int
    report: dict[str, Any]


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_repo_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


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

    # Conservative decision: auth-building components are not a standalone
    # operator entrypoint, and collector main remains runtime-bound.
    available_hits = [
        hit
        for hit in marker_hits
        if hit["file"] != "src/services/collector_telegram/main.py"
    ]
    if available_hits and not main_is_runtime_entrypoint:
        return {
            "auth_only_entrypoint_status": "available",
            "selected_entrypoint": available_hits[0]["file"],
            "entrypoint_evidence": available_hits,
            "inspected_source_files": inspected,
            "missing_source_files": missing_files,
            "collector_main_runtime_entrypoint": main_is_runtime_entrypoint,
        }

    return {
        "auth_only_entrypoint_status": "missing",
        "selected_entrypoint": None,
        "entrypoint_evidence": marker_hits,
        "inspected_source_files": inspected,
        "missing_source_files": missing_files,
        "collector_main_runtime_entrypoint": main_is_runtime_entrypoint,
    }


def generate_report(
    repo_root: Path | None = None,
    *,
    approved_tdlib_auth_operator_execution: bool = False,
) -> WrapperResult:
    repo_root = repo_root or default_repo_root()
    inspection = _inspect_auth_only_entrypoint(repo_root)
    status = inspection["auth_only_entrypoint_status"]
    available = status == "available"

    checks_failed: list[str] = []
    failures: list[dict[str, str]] = []
    if not available:
        checks_failed.append("auth_only_entrypoint.missing")
        failures.append(
            {
                "check": "auth_only_entrypoint.missing",
                "message": (
                    "No safe standalone TDLib-auth-only entrypoint exists in the "
                    "current repository."
                ),
            }
        )
    elif not approved_tdlib_auth_operator_execution:
        checks_failed.append("approval.required")
        failures.append(
            {
                "check": "approval.required",
                "message": (
                    "TDLib auth operator execution requires separate explicit approval."
                ),
            }
        )

    report: dict[str, Any] = {
        "report_type": REPORT_TYPE,
        "contract_status": "approval_required" if available else "blocked",
        "approval_required": True,
        "approved_execution_requested": approved_tdlib_auth_operator_execution,
        "auth_only_entrypoint_status": status,
        "selected_entrypoint": inspection["selected_entrypoint"],
        "runtime_env_path": RUNTIME_ENV_PATH,
        "checks_failed": checks_failed,
        "failures": failures,
        "likely_next_slice": AVAILABLE_NEXT_SLICE if available else MISSING_NEXT_SLICE,
        "entrypoint_assessment": {
            "collector_main_runtime_entrypoint": inspection["collector_main_runtime_entrypoint"],
            "entrypoint_evidence": inspection["entrypoint_evidence"],
            "inspected_source_files": inspection["inspected_source_files"],
            "missing_source_files": inspection["missing_source_files"],
            "telegram_bot_token_used_for_tdlib_auth": False,
        },
    }
    for flag in SIDE_EFFECT_FLAGS:
        report[flag] = False

    return WrapperResult(exit_code=1 if failures else 0, report=report)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Report the safe TDLib auth operator execution wrapper decision. "
            "Default mode reads repository source text only and performs no auth."
        )
    )
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--approved-tdlib-auth-operator-execution", action="store_true")
    return parser


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root) if args.repo_root else default_repo_root()
    result = generate_report(
        repo_root=repo_root,
        approved_tdlib_auth_operator_execution=args.approved_tdlib_auth_operator_execution,
    )

    if args.format == "json":
        print(render_json(result.report))
    else:
        print(f"contract_status={result.report['contract_status']}")
        print(f"auth_only_entrypoint_status={result.report['auth_only_entrypoint_status']}")
        print(f"likely_next_slice={result.report['likely_next_slice']}")
        for failure in result.report["failures"]:
            print(f"- {failure['check']}: {failure['message']}")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
