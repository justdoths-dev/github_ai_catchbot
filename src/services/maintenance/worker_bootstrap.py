from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


FATAL_REPORT_SCHEMA_VERSION = "maintenance_worker_runtime_fatal_report_v1"
REPO_ROOT = Path(__file__).resolve().parents[3]
WORKER_RUNTIME_FATAL_REPORT_PATH = REPO_ROOT / "state/maintenance/worker-runtime-fatal-report.json"
MAINTENANCE_PACKAGE_MODULE = "src.services.maintenance"
MAINTENANCE_MAIN_MODULE = "src.services.maintenance.main"
MAINTENANCE_REPOSITORIES_MODULE = "src.services.maintenance.repositories"
EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR = 21
EXIT_WORKER_BOOTSTRAP_MAIN_ERROR = 22
EXIT_WORKER_BOOTSTRAP_REPORT_WRITE_FAILED = 23
BOOTSTRAP_SPEC_READY_STAGES = frozenset(
    {
        "maintenance_package_ready",
        "maintenance_repositories_spec_ready",
    }
)
BOOTSTRAP_IMPORT_STAGE_SEQUENCE = (
    ("bootstrap_repo_root_path_ready", None),
    ("stdlib_ready", "json"),
    ("maintenance_package_ready", MAINTENANCE_PACKAGE_MODULE),
    ("maintenance_package_init_import", MAINTENANCE_PACKAGE_MODULE),
    ("maintenance_config_import", "src.services.maintenance.config"),
    ("maintenance_redis_streams_import", "src.services.maintenance.redis_streams"),
    ("maintenance_repositories_import", None),
    ("maintenance_repositories_spec_ready", MAINTENANCE_REPOSITORIES_MODULE),
    ("maintenance_repositories_sqlalchemy_import", "sqlalchemy"),
    ("maintenance_repositories_outbox_eligibility_import", "src.services.outbox_relay.eligibility"),
    ("maintenance_repositories_models_import", "src.services.maintenance.models"),
    ("maintenance_repositories_delivery_retry_import", "src.services.maintenance.delivery_retry"),
    ("maintenance_repositories_delivery_replay_import", "src.services.maintenance.delivery_replay"),
    ("maintenance_repositories_retry_policy_import", "src.services.maintenance.retry_policy"),
    ("maintenance_repositories_module_import", MAINTENANCE_REPOSITORIES_MODULE),
    ("maintenance_service_import", "src.services.maintenance.service"),
    ("maintenance_worker_import", "src.services.maintenance.worker"),
    ("maintenance_main_import", MAINTENANCE_MAIN_MODULE),
)
BOOTSTRAP_IMPORT_STAGE_LABELS = tuple(stage for stage, _module_name in BOOTSTRAP_IMPORT_STAGE_SEQUENCE)
BOOTSTRAP_IMPORT_STAGE_REASON_CODES = {
    "repo_root_path_unavailable",
    "stage_import_error",
    "stage_dependency_unavailable",
    "stage_module_unavailable",
    "stage_spec_unavailable",
}
INVOCATION_ID_ENV_KEY = "INVOCATION_ID"
INVOCATION_ID_HEX_LENGTH = 32
INVOCATION_FINGERPRINT_LENGTH = 16
BOOTSTRAP_EXIT_STATUS_REASON_CODES = {
    EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR: "worker_bootstrap_import_error",
    EXIT_WORKER_BOOTSTRAP_MAIN_ERROR: "worker_bootstrap_main_error",
    EXIT_WORKER_BOOTSTRAP_REPORT_WRITE_FAILED: "worker_bootstrap_report_write_failed",
}

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
    import_stage: str | None = None,
    import_stage_status: str | None = None,
    import_stage_reason_code: str | None = None,
    import_stage_index: int | None = None,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    created_at = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    safe_import_stage = _safe_import_stage(import_stage)
    invocation_fingerprint = _current_invocation_fingerprint()
    return {
        "schema_version": FATAL_REPORT_SCHEMA_VERSION,
        "reason_code": _safe_reason_code(reason_code),
        "phase": _safe_phase(phase),
        "import_stage": safe_import_stage,
        "import_stage_status": _safe_import_stage_status(import_stage_status),
        "import_stage_reason_code": _safe_import_stage_reason_code(import_stage_reason_code),
        "import_stage_index": _safe_import_stage_index(safe_import_stage, import_stage_index),
        "crashed_task": None,
        "unexpected_return_task": None,
        "cleanup_completed": None,
        "tasks_started": [],
        "broad_worker_run_started": False,
        "created_at_utc": created_at.isoformat().replace("+00:00", "Z"),
        "invocation_fingerprint": invocation_fingerprint,
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
            "raw_invocation_id_omitted": True,
        },
        "report_path_omitted": True,
        "raw_invocation_id_omitted": True,
    }


def write_worker_bootstrap_fatal_report(
    *,
    reason_code: str,
    phase: str,
    import_stage: str | None = None,
    import_stage_status: str | None = None,
    import_stage_reason_code: str | None = None,
    import_stage_index: int | None = None,
    report_path: Path | None = None,
) -> bool:
    report = build_worker_bootstrap_fatal_report(
        reason_code=reason_code,
        phase=phase,
        import_stage=import_stage,
        import_stage_status=import_stage_status,
        import_stage_reason_code=import_stage_reason_code,
        import_stage_index=import_stage_index,
    )
    path = report_path or WORKER_RUNTIME_FATAL_REPORT_PATH
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    except OSError:
        return False
    return True


async def run_bootstrap(
    *,
    import_module=importlib.import_module,
    find_spec=importlib.util.find_spec,
    report_path: Path | None = None,
) -> int:
    _ensure_repo_root_on_sys_path()
    maintenance_main, import_failure = _import_maintenance_main_with_stage_classifier(import_module, find_spec)
    if import_failure is not None:
        if not write_worker_bootstrap_fatal_report(
            reason_code="worker_bootstrap_import_error",
            phase="bootstrap_import",
            import_stage=import_failure["import_stage"],
            import_stage_status="failed",
            import_stage_reason_code=import_failure["import_stage_reason_code"],
            import_stage_index=import_failure["import_stage_index"],
            report_path=report_path,
        ):
            return EXIT_WORKER_BOOTSTRAP_REPORT_WRITE_FAILED
        return EXIT_WORKER_BOOTSTRAP_IMPORT_ERROR

    try:
        exit_code = int(await maintenance_main._run(["worker"]))
    except Exception:
        if not write_worker_bootstrap_fatal_report(
            reason_code="worker_bootstrap_main_error",
            phase="bootstrap_main",
            report_path=report_path,
        ):
            return EXIT_WORKER_BOOTSTRAP_REPORT_WRITE_FAILED
        return EXIT_WORKER_BOOTSTRAP_MAIN_ERROR
    if exit_code != 0 and not _fatal_report_exists(report_path):
        if not write_worker_bootstrap_fatal_report(
            reason_code="worker_bootstrap_main_error",
            phase="bootstrap_main",
            report_path=report_path,
        ):
            return EXIT_WORKER_BOOTSTRAP_REPORT_WRITE_FAILED
        return EXIT_WORKER_BOOTSTRAP_MAIN_ERROR
    return exit_code


def _safe_reason_code(value: object) -> str:
    if isinstance(value, str) and value in _BOOTSTRAP_REASON_CODES:
        return value
    return "worker_bootstrap_main_error"


def _safe_phase(value: object) -> str:
    if isinstance(value, str) and value in _BOOTSTRAP_PHASES:
        return value
    return "bootstrap_main"


def _safe_import_stage(value: object) -> str | None:
    if isinstance(value, str) and value in BOOTSTRAP_IMPORT_STAGE_LABELS:
        return value
    return None


def _safe_import_stage_status(value: object) -> str | None:
    if value == "failed":
        return "failed"
    return None


def _safe_import_stage_reason_code(value: object) -> str | None:
    if isinstance(value, str) and value in BOOTSTRAP_IMPORT_STAGE_REASON_CODES:
        return value
    return None


def _safe_import_stage_index(import_stage: str | None, value: object) -> int | None:
    if import_stage is None or not isinstance(value, int):
        return None
    expected_index = BOOTSTRAP_IMPORT_STAGE_LABELS.index(import_stage)
    if value == expected_index:
        return value
    return None


def _import_maintenance_main_with_stage_classifier(
    import_module,
    find_spec,
) -> tuple[object | None, dict[str, object] | None]:
    maintenance_main = None
    for index, (stage, module_name) in enumerate(BOOTSTRAP_IMPORT_STAGE_SEQUENCE):
        if stage == "bootstrap_repo_root_path_ready":
            if not REPO_ROOT.is_dir():
                return None, _import_stage_failure(
                    import_stage=stage,
                    import_stage_reason_code="repo_root_path_unavailable",
                    import_stage_index=index,
                )
            continue
        if module_name is None:
            continue
        if stage in BOOTSTRAP_SPEC_READY_STAGES:
            try:
                spec = find_spec(module_name)
            except Exception:
                return None, _import_stage_failure(
                    import_stage=stage,
                    import_stage_reason_code="stage_import_error",
                    import_stage_index=index,
                )
            if spec is None:
                return None, _import_stage_failure(
                    import_stage=stage,
                    import_stage_reason_code="stage_spec_unavailable",
                    import_stage_index=index,
                )
            continue
        try:
            module = import_module(module_name)
        except Exception as exc:
            return None, _import_stage_failure(
                import_stage=stage,
                import_stage_reason_code=_import_failure_reason_code(exc, target_module_name=module_name),
                import_stage_index=index,
            )
        if module_name == MAINTENANCE_MAIN_MODULE:
            maintenance_main = module
    return maintenance_main, None


def _import_stage_failure(
    *,
    import_stage: str,
    import_stage_reason_code: str,
    import_stage_index: int,
) -> dict[str, object]:
    return {
        "import_stage": _safe_import_stage(import_stage),
        "import_stage_reason_code": _safe_import_stage_reason_code(import_stage_reason_code),
        "import_stage_index": _safe_import_stage_index(_safe_import_stage(import_stage), import_stage_index),
    }


def _fatal_report_exists(report_path: Path | None = None) -> bool:
    path = report_path or WORKER_RUNTIME_FATAL_REPORT_PATH
    try:
        return path.is_file()
    except OSError:
        return False


def _ensure_repo_root_on_sys_path() -> None:
    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)


def _import_failure_reason_code(exc: Exception, *, target_module_name: str) -> str:
    if isinstance(exc, ModuleNotFoundError):
        missing_name = getattr(exc, "name", None)
        if missing_name == target_module_name:
            return "stage_module_unavailable"
        if isinstance(missing_name, str) and missing_name:
            return "stage_dependency_unavailable"
    return "stage_import_error"


def _current_invocation_fingerprint() -> str | None:
    return _invocation_fingerprint(os.environ.get(INVOCATION_ID_ENV_KEY))


def _invocation_fingerprint(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if len(normalized) != INVOCATION_ID_HEX_LENGTH:
        return None
    if not all(char in "0123456789abcdef" for char in normalized):
        return None
    return hashlib.sha256(normalized.encode("ascii")).hexdigest()[:INVOCATION_FINGERPRINT_LENGTH]


def main() -> None:
    raise SystemExit(asyncio.run(run_bootstrap()))


if __name__ == "__main__":
    main()
