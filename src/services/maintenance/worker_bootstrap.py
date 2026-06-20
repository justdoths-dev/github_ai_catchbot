from __future__ import annotations

import asyncio
import importlib
import json
from datetime import datetime, timezone
from pathlib import Path


FATAL_REPORT_SCHEMA_VERSION = "maintenance_worker_runtime_fatal_report_v1"
WORKER_RUNTIME_FATAL_REPORT_PATH = (
    Path(__file__).resolve().parents[3] / "state/maintenance/worker-runtime-fatal-report.json"
)
MAINTENANCE_MAIN_MODULE = "src.services.maintenance.main"

_BOOTSTRAP_REASON_CODES = {
    "worker_bootstrap_import_error",
    "worker_bootstrap_main_error",
}
_BOOTSTRAP_PHASES = {
    "bootstrap_import",
    "bootstrap_main",
}


def build_worker_bootstrap_fatal_report(
    *,
    reason_code: str,
    phase: str,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    created_at = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return {
        "schema_version": FATAL_REPORT_SCHEMA_VERSION,
        "reason_code": _safe_reason_code(reason_code),
        "phase": _safe_phase(phase),
        "crashed_task": None,
        "unexpected_return_task": None,
        "cleanup_completed": None,
        "tasks_started": [],
        "broad_worker_run_started": False,
        "created_at_utc": created_at.isoformat().replace("+00:00", "Z"),
        "redactions_applied": {
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "raw_exception_body_omitted": True,
            "traceback_omitted": True,
            "redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "report_path_omitted": True,
            "systemd_stdout_stderr_omitted": True,
            "journal_output_omitted": True,
        },
        "report_path_omitted": True,
    }


def write_worker_bootstrap_fatal_report(
    *,
    reason_code: str,
    phase: str,
    report_path: Path | None = None,
) -> dict[str, object]:
    report = build_worker_bootstrap_fatal_report(reason_code=reason_code, phase=phase)
    path = report_path or WORKER_RUNTIME_FATAL_REPORT_PATH
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    except OSError:
        pass
    return report


async def run_bootstrap(
    *,
    import_module=importlib.import_module,
    report_path: Path | None = None,
) -> int:
    try:
        maintenance_main = import_module(MAINTENANCE_MAIN_MODULE)
    except Exception:
        write_worker_bootstrap_fatal_report(
            reason_code="worker_bootstrap_import_error",
            phase="bootstrap_import",
            report_path=report_path,
        )
        return 1

    try:
        exit_code = int(await maintenance_main._run(["worker"]))
    except Exception:
        write_worker_bootstrap_fatal_report(
            reason_code="worker_bootstrap_main_error",
            phase="bootstrap_main",
            report_path=report_path,
        )
        return 1
    if exit_code != 0 and not _fatal_report_exists(report_path):
        write_worker_bootstrap_fatal_report(
            reason_code="worker_bootstrap_main_error",
            phase="bootstrap_main",
            report_path=report_path,
        )
    return exit_code


def _safe_reason_code(value: object) -> str:
    if isinstance(value, str) and value in _BOOTSTRAP_REASON_CODES:
        return value
    return "worker_bootstrap_main_error"


def _safe_phase(value: object) -> str:
    if isinstance(value, str) and value in _BOOTSTRAP_PHASES:
        return value
    return "bootstrap_main"


def _fatal_report_exists(report_path: Path | None = None) -> bool:
    path = report_path or WORKER_RUNTIME_FATAL_REPORT_PATH
    try:
        return path.is_file()
    except OSError:
        return False


def main() -> None:
    raise SystemExit(asyncio.run(run_bootstrap()))


if __name__ == "__main__":
    main()
