from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from pathlib import Path
from typing import Any, Callable, Sequence


REPORT_TYPE = "dedicated_vps_baseline_preflight_v1"
SUCCESS_NOTE = "Dedicated VPS baseline preflight success does not authorize live ingest or production rollout."
MIN_SUPPORTED_PYTHON = (3, 12)

CORE_REPO_PATHS: tuple[tuple[str, str], ...] = (
    ("pyproject_present", "pyproject.toml"),
    ("docs_project_source_present", "docs/project-source"),
    ("scripts_ops_present", "scripts/ops"),
    ("ops_pipeline_runbooks_present", "ops/pipeline/runbooks"),
    ("tests_present", "tests"),
)

RUNTIME_DIRECTORY_LABELS = (
    "app_dir",
    "state_dir",
    "tdlib_state_parent",
    "blob_cache_parent",
    "logs_parent",
    "secrets_parent",
    "backups_parent",
)

DEPLOYMENT_TOPOLOGY = {
    "expected_deployment_topology": "dedicated_vps",
    "shared_with_trading_bot": False,
    "trading_bot_repo_inspected": False,
    "trading_bot_paths_touched": False,
}

REDACTION = {
    "env_values_printed": False,
    "secret_values_printed": False,
    "raw_paths_printed": False,
    "hostname_printed": False,
    "username_printed": False,
    "home_path_printed": False,
    "ip_address_printed": False,
    "provider_metadata_printed": False,
}

SIDE_EFFECTS = {
    "tdlib_started": False,
    "telegram_called": False,
    "db_connection_attempted": False,
    "redis_connection_attempted": False,
    "external_network_attempted": False,
    "docker_invoked": False,
    "systemd_invoked": False,
    "services_started": False,
    "collector_started": False,
    "env_or_feature_flags_mutated": False,
    "production_files_created": False,
    "trading_bot_repo_inspected": False,
    "trading_bot_paths_touched": False,
}

AUTHORIZATION = {
    "live_ingest_authorized": False,
    "production_rollout_authorized": False,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Emit a metadata-only dedicated VPS baseline preflight report. "
            "Default schema mode is CI-safe and does not inspect host paths, "
            "environment values, services, databases, Redis, Telegram, Docker, "
            "systemd, or network connectivity."
        )
    )
    parser.add_argument(
        "--format",
        choices=("json",),
        default="json",
        help="Output format. Only json is supported.",
    )
    parser.add_argument(
        "--mode",
        choices=("schema", "current-host"),
        default="schema",
        help="schema validates the report contract; current-host inspects redacted local metadata only.",
    )
    return parser


def _not_checked() -> str:
    return "not_checked"


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _python_major_minor(version_info: tuple[int, int] | sys.version_info) -> str:
    return f"{version_info[0]}.{version_info[1]}"


def _python_supported(version_info: tuple[int, int] | sys.version_info) -> bool:
    return (version_info[0], version_info[1]) >= MIN_SUPPORTED_PYTHON


def _status(passed: bool) -> str:
    return "passed" if passed else "failed"


def _add_failure(
    checks_failed: list[str],
    failures: list[dict[str, str]],
    *,
    check: str,
    reason_code: str,
) -> None:
    checks_failed.append(reason_code)
    failures.append({"check": check, "reason_code": reason_code})


def _schema_host_metadata() -> dict[str, Any]:
    return {
        "system": _not_checked(),
        "machine": _not_checked(),
        "python_major_minor": _not_checked(),
        "is_posix": _not_checked(),
    }


def _current_host_metadata(
    *,
    platform_system: Callable[[], str] = platform.system,
    platform_machine: Callable[[], str] = platform.machine,
    version_info: tuple[int, int] | sys.version_info = sys.version_info,
    os_name: str = os.name,
) -> dict[str, Any]:
    return {
        "system": platform_system() or "unknown",
        "machine": platform_machine() or "unknown",
        "python_major_minor": _python_major_minor(version_info),
        "is_posix": os_name == "posix",
    }


def _schema_repo_metadata() -> dict[str, Any]:
    return {
        "repo_root_detected": _not_checked(),
        **{field: _not_checked() for field, _ in CORE_REPO_PATHS},
    }


def _current_repo_metadata(repo_root: Path | None) -> dict[str, Any]:
    repo_root_detected = bool(repo_root is not None and repo_root.exists() and repo_root.is_dir())
    metadata: dict[str, Any] = {"repo_root_detected": repo_root_detected}
    for field, relative_path in CORE_REPO_PATHS:
        metadata[field] = bool(repo_root_detected and (repo_root / relative_path).exists())
    return metadata


def _schema_venv_metadata() -> dict[str, Any]:
    return {
        "venv_dir_present": _not_checked(),
        "venv_python_present": _not_checked(),
        "running_inside_venv": _not_checked(),
        "python_version_supported": _not_checked(),
    }


def _current_venv_metadata(
    *,
    repo_root: Path | None,
    version_info: tuple[int, int] | sys.version_info = sys.version_info,
    prefix: str = sys.prefix,
    base_prefix: str | None = getattr(sys, "base_prefix", None),
) -> dict[str, Any]:
    venv_dir = repo_root / "venv" if repo_root is not None else None
    venv_dir_present = bool(venv_dir is not None and venv_dir.exists() and venv_dir.is_dir())
    venv_python_present = False
    if venv_dir is not None:
        candidates = (venv_dir / "bin" / "python", venv_dir / "Scripts" / "python.exe")
        venv_python_present = any(path.exists() and path.is_file() for path in candidates)

    return {
        "venv_dir_present": venv_dir_present,
        "venv_python_present": venv_python_present,
        "running_inside_venv": bool(base_prefix is not None and prefix != base_prefix),
        "python_version_supported": _python_supported(version_info),
    }


def _schema_runtime_directory_metadata() -> dict[str, dict[str, Any]]:
    return {
        label: {
            "status": "not_checked",
            "reason_code": "schema_mode_does_not_inspect_host_paths",
        }
        for label in RUNTIME_DIRECTORY_LABELS
    }


def _current_runtime_directory_metadata(
    *,
    repo_root: Path | None,
    path_writable: Callable[[Path, int], bool] = os.access,
) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    repo_root_detected = bool(repo_root is not None and repo_root.exists() and repo_root.is_dir())
    metadata["app_dir"] = {
        "status": _status(repo_root_detected),
        "exists": repo_root_detected,
        "parent_writable_by_process": bool(
            repo_root is not None and repo_root.parent.exists() and path_writable(repo_root.parent, os.W_OK)
        ),
    }
    for label in RUNTIME_DIRECTORY_LABELS:
        if label == "app_dir":
            continue
        metadata[label] = {
            "status": "not_applicable",
            "reason_code": "no_concrete_path_configured_for_metadata_only_check",
        }
    return metadata


def _evaluate_failures(
    *,
    mode: str,
    repo_metadata: dict[str, Any],
    venv_metadata: dict[str, Any],
) -> tuple[list[str], list[dict[str, str]]]:
    checks_failed: list[str] = []
    failures: list[dict[str, str]] = []
    if mode != "current-host":
        return checks_failed, failures

    if not repo_metadata["repo_root_detected"]:
        _add_failure(
            checks_failed,
            failures,
            check="repo_metadata.repo_root_detected",
            reason_code="repo_root_not_detected",
        )
    for field, _ in CORE_REPO_PATHS:
        if not repo_metadata[field]:
            _add_failure(
                checks_failed,
                failures,
                check=f"repo_metadata.{field}",
                reason_code=f"{field}_missing",
            )

    if not venv_metadata["python_version_supported"]:
        _add_failure(
            checks_failed,
            failures,
            check="venv_metadata.python_version_supported",
            reason_code="python_version_below_3_12",
        )
    if not venv_metadata["venv_dir_present"]:
        _add_failure(
            checks_failed,
            failures,
            check="venv_metadata.venv_dir_present",
            reason_code="venv_dir_missing",
        )
    if not venv_metadata["venv_python_present"]:
        _add_failure(
            checks_failed,
            failures,
            check="venv_metadata.venv_python_present",
            reason_code="venv_python_missing",
        )
    return checks_failed, failures


def generate_report(
    *,
    mode: str = "schema",
    repo_root: Path | None = None,
    platform_system: Callable[[], str] = platform.system,
    platform_machine: Callable[[], str] = platform.machine,
    version_info: tuple[int, int] | sys.version_info = sys.version_info,
    os_name: str = os.name,
    prefix: str = sys.prefix,
    base_prefix: str | None = getattr(sys, "base_prefix", None),
    path_writable: Callable[[Path, int], bool] = os.access,
) -> dict[str, Any]:
    if mode not in {"schema", "current-host"}:
        raise ValueError(f"unsupported mode: {mode}")

    resolved_repo_root = repo_root if repo_root is not None else _default_repo_root()
    if mode == "schema":
        host_metadata = _schema_host_metadata()
        repo_metadata = _schema_repo_metadata()
        venv_metadata = _schema_venv_metadata()
        runtime_directory_metadata = _schema_runtime_directory_metadata()
    else:
        host_metadata = _current_host_metadata(
            platform_system=platform_system,
            platform_machine=platform_machine,
            version_info=version_info,
            os_name=os_name,
        )
        repo_metadata = _current_repo_metadata(resolved_repo_root)
        venv_metadata = _current_venv_metadata(
            repo_root=resolved_repo_root,
            version_info=version_info,
            prefix=prefix,
            base_prefix=base_prefix,
        )
        runtime_directory_metadata = _current_runtime_directory_metadata(
            repo_root=resolved_repo_root,
            path_writable=path_writable,
        )

    checks_failed, failures = _evaluate_failures(
        mode=mode,
        repo_metadata=repo_metadata,
        venv_metadata=venv_metadata,
    )

    return {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "mode": mode,
        "checks_failed": checks_failed,
        "failures": failures,
        "deployment_topology": dict(DEPLOYMENT_TOPOLOGY),
        "host_metadata": host_metadata,
        "repo_metadata": repo_metadata,
        "venv_metadata": venv_metadata,
        "runtime_directory_metadata": runtime_directory_metadata,
        "redaction": dict(REDACTION),
        "side_effects": dict(SIDE_EFFECTS),
        "authorization": dict(AUTHORIZATION),
        "notes": [SUCCESS_NOTE],
    }


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = generate_report(mode=args.mode)
    sys.stdout.write(render_json(report))
    sys.stdout.write("\n")
    return 1 if report["checks_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
