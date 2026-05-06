from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPORT_TYPE = "collector_restricted_environment_inventory_v1"
SUCCESS_NOTE = "Inventory success does not authorize live ingest or production rollout."


@dataclass(frozen=True, slots=True)
class EnvSpec:
    name: str
    category: str
    kind: str
    required: bool = True


COMMON_REQUIRED: tuple[EnvSpec, ...] = (
    EnvSpec("APP_ENV", "required", "mode"),
    EnvSpec("DATABASE_URL", "required", "env_value"),
    EnvSpec("REDIS_URL", "required", "env_value"),
)

COLLECTOR_REQUIRED: tuple[EnvSpec, ...] = (
    EnvSpec("COLLECTOR_MODE", "required", "mode"),
    EnvSpec("TELEGRAM_API_ID", "required", "env_value"),
    EnvSpec("TELEGRAM_API_HASH_FILE", "required", "secret_file_path"),
    EnvSpec("TELEGRAM_PHONE_NUMBER", "required", "env_value"),
    EnvSpec("TELEGRAM_2FA_PASSWORD_FILE", "required", "secret_file_path"),
    EnvSpec("TDLIB_STATE_DIR", "required", "non_secret_path"),
    EnvSpec("TDLIB_FILES_DIR", "required", "non_secret_path"),
    EnvSpec("TDLIB_DB_ENCRYPTION_KEY_FILE", "required", "secret_file_path"),
    EnvSpec("COLLECTOR_SINGLETON_LOCK_PATH", "required_with_default", "non_secret_path", required=False),
)

OPTIONAL_TUNING: tuple[EnvSpec, ...] = (
    EnvSpec("RECONCILE_INTERVAL_SEC", "optional", "tuning", required=False),
    EnvSpec("RECONCILE_BACKFILL_LIMIT", "optional", "tuning", required=False),
    EnvSpec("WARM_BACKFILL_LIMIT", "optional", "tuning", required=False),
    EnvSpec("HISTORY_PAGE_LIMIT", "optional", "tuning", required=False),
    EnvSpec("STARTUP_PROBE_TIMEOUT_SEC", "optional", "tuning", required=False),
    EnvSpec("STARTUP_WARM_BACKFILL_ENABLED", "optional", "tuning", required=False),
)

FILE_PATH_ENV_NAMES = {
    spec.name for spec in COLLECTOR_REQUIRED if spec.kind == "secret_file_path"
}
TDLIB_DIR_ENV_NAMES = {"TDLIB_STATE_DIR", "TDLIB_FILES_DIR"}
SINGLETON_LOCK_ENV_NAME = "COLLECTOR_SINGLETON_LOCK_PATH"

REDACTION = {
    "env_values_printed": False,
    "secret_values_printed": False,
    "secret_file_contents_read": False,
    "raw_paths_printed": False,
    "database_url_printed": False,
    "redis_url_printed": False,
    "phone_number_printed": False,
}

SIDE_EFFECTS = {
    "tdlib_started": False,
    "telegram_called": False,
    "db_connection_attempted": False,
    "redis_connection_attempted": False,
    "external_network_attempted": False,
    "docker_invoked": False,
    "systemd_invoked": False,
    "env_or_feature_flags_mutated": False,
    "production_files_created": False,
    "singleton_lock_acquired": False,
}

AUTHORIZATION = {
    "live_ingest_authorized": False,
    "production_rollout_authorized": False,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect collector restricted-environment inventory metadata. "
            "Default schema mode is CI-safe and does not require production env values. "
            "Current-env mode checks name presence and path metadata only; it never prints "
            "values, raw paths, secret file contents, or starts runtime systems."
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
        choices=("schema", "current-env"),
        default="schema",
        help="schema is CI-safe; current-env inspects current process env name presence and path metadata only.",
    )
    return parser


def _path_has_value(environ: Mapping[str, str], name: str) -> bool:
    return bool((environ.get(name) or "").strip())


def _inventory_entry(spec: EnvSpec, *, mode: str, environ: Mapping[str, str]) -> dict[str, Any]:
    if mode == "schema":
        present = False
        status = "not_checked"
    else:
        present = _path_has_value(environ, spec.name)
        status = "present" if present else "missing"
        if spec.name == SINGLETON_LOCK_ENV_NAME and not present and _path_has_value(environ, "TDLIB_STATE_DIR"):
            status = "not_applicable"

    entry: dict[str, Any] = {
        "present": present,
        "status": status,
        "category": spec.category,
        "kind": spec.kind,
    }
    if spec.name == SINGLETON_LOCK_ENV_NAME and status == "not_applicable":
        entry["reason"] = "collector config can derive a default lock path from TDLIB_STATE_DIR"
    return entry


def _file_path_metadata(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    parent = path.parent
    return {
        "checked": True,
        "path_present": bool(path_value.strip()),
        "exists": path.exists(),
        "is_file": path.is_file(),
        "readable": os.access(path, os.R_OK),
        "parent_exists": parent.exists(),
    }


def _tdlib_dir_metadata(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    parent = path.parent
    exists = path.exists()
    is_dir = path.is_dir()
    parent_exists = parent.exists()
    if exists and is_dir:
        writable_or_creatable = os.access(path, os.W_OK)
    elif not exists and parent_exists:
        writable_or_creatable = os.access(parent, os.W_OK)
    else:
        writable_or_creatable = False

    return {
        "checked": True,
        "path_present": bool(path_value.strip()),
        "exists": exists,
        "is_dir": is_dir,
        "parent_exists": parent_exists,
        "writable_or_creatable": writable_or_creatable,
    }


def _singleton_lock_metadata(path_value: str) -> dict[str, Any]:
    parent = Path(path_value).parent
    return {
        "checked": True,
        "path_present": bool(path_value.strip()),
        "parent_exists": parent.exists(),
        "parent_writable": os.access(parent, os.W_OK),
    }


def _unchecked_path_metadata() -> dict[str, Any]:
    return {"checked": False, "path_present": False}


def _build_path_metadata(mode: str, environ: Mapping[str, str]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for name in sorted(FILE_PATH_ENV_NAMES):
        value = (environ.get(name) or "").strip()
        metadata[name] = _file_path_metadata(value) if mode == "current-env" and value else _unchecked_path_metadata()

    for name in sorted(TDLIB_DIR_ENV_NAMES):
        value = (environ.get(name) or "").strip()
        metadata[name] = _tdlib_dir_metadata(value) if mode == "current-env" and value else _unchecked_path_metadata()

    value = (environ.get(SINGLETON_LOCK_ENV_NAME) or "").strip()
    metadata[SINGLETON_LOCK_ENV_NAME] = (
        _singleton_lock_metadata(value) if mode == "current-env" and value else _unchecked_path_metadata()
    )
    return metadata


def _add_failure(
    checks_failed: list[str],
    failures: list[dict[str, str]],
    *,
    check: str,
    env_name: str,
    reason_code: str,
    message: str,
) -> None:
    checks_failed.append(reason_code)
    failures.append(
        {
            "check": check,
            "env_name": env_name,
            "reason_code": reason_code,
            "message": message,
        }
    )


def _evaluate_failures(
    *,
    mode: str,
    inventory: dict[str, dict[str, dict[str, Any]]],
    path_metadata: dict[str, dict[str, Any]],
) -> tuple[list[str], list[dict[str, str]]]:
    checks_failed: list[str] = []
    failures: list[dict[str, str]] = []
    if mode != "current-env":
        return checks_failed, failures

    for name, entry in inventory["required"].items():
        if name == SINGLETON_LOCK_ENV_NAME:
            continue
        if entry["status"] == "missing":
            _add_failure(
                checks_failed,
                failures,
                check="env_presence.required_name_present",
                env_name=name,
                reason_code=f"{name}.missing",
                message="Required collector environment variable is missing.",
            )

    for name in FILE_PATH_ENV_NAMES:
        metadata = path_metadata[name]
        if not metadata["checked"]:
            continue
        for field in ("path_present", "exists", "is_file", "readable", "parent_exists"):
            if not metadata[field]:
                _add_failure(
                    checks_failed,
                    failures,
                    check=f"path_metadata.{name}.{field}",
                    env_name=name,
                    reason_code=f"{name}.{field}_false",
                    message="Secret file path metadata check failed.",
                )

    for name in TDLIB_DIR_ENV_NAMES:
        metadata = path_metadata[name]
        if not metadata["checked"]:
            continue
        for field in ("path_present", "parent_exists", "writable_or_creatable"):
            if not metadata[field]:
                _add_failure(
                    checks_failed,
                    failures,
                    check=f"path_metadata.{name}.{field}",
                    env_name=name,
                    reason_code=f"{name}.{field}_false",
                    message="TDLib directory path metadata check failed.",
                )
        if metadata["exists"] and not metadata["is_dir"]:
            _add_failure(
                checks_failed,
                failures,
                check=f"path_metadata.{name}.is_dir",
                env_name=name,
                reason_code=f"{name}.is_dir_false",
                message="TDLib path exists but is not a directory.",
            )

    metadata = path_metadata[SINGLETON_LOCK_ENV_NAME]
    if metadata["checked"]:
        for field in ("path_present", "parent_exists", "parent_writable"):
            if not metadata[field]:
                _add_failure(
                    checks_failed,
                    failures,
                    check=f"path_metadata.{SINGLETON_LOCK_ENV_NAME}.{field}",
                    env_name=SINGLETON_LOCK_ENV_NAME,
                    reason_code=f"{SINGLETON_LOCK_ENV_NAME}.{field}_false",
                    message="Singleton lock parent metadata check failed.",
                )

    return checks_failed, failures


def generate_report(
    *,
    mode: str = "schema",
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if mode not in {"schema", "current-env"}:
        raise ValueError(f"unsupported mode: {mode}")

    source = os.environ if environ is None else environ
    required_specs = (*COMMON_REQUIRED, *COLLECTOR_REQUIRED)
    inventory = {
        "required": {
            spec.name: _inventory_entry(spec, mode=mode, environ=source)
            for spec in required_specs
        },
        "optional": {
            spec.name: _inventory_entry(spec, mode=mode, environ=source)
            for spec in OPTIONAL_TUNING
        },
    }
    path_metadata = _build_path_metadata(mode, source)
    checks_failed, failures = _evaluate_failures(
        mode=mode,
        inventory=inventory,
        path_metadata=path_metadata,
    )

    return {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "mode": mode,
        "checks_failed": checks_failed,
        "failures": failures,
        "inventory": inventory,
        "path_metadata": path_metadata,
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
