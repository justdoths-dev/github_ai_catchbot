from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


REPORT_TYPE = "collector_sensitive_path_permission_v1"
SUCCESS_NOTE = "Permission check success does not authorize live ingest or production rollout."

SECRET_FILE_ENV_NAMES: tuple[str, ...] = (
    "TELEGRAM_API_HASH_FILE",
    "TELEGRAM_2FA_PASSWORD_FILE",
    "TDLIB_DB_ENCRYPTION_KEY_FILE",
)
TDLIB_DIR_ENV_NAMES: tuple[str, ...] = (
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
)
SINGLETON_LOCK_ENV_NAME = "COLLECTOR_SINGLETON_LOCK_PATH"

REDACTION = {
    "env_values_printed": False,
    "secret_values_printed": False,
    "secret_file_contents_read": False,
    "raw_paths_printed": False,
    "uid_gid_printed": False,
    "mode_bits_printed": False,
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
    "secret_file_contents_read": False,
}

AUTHORIZATION = {
    "live_ingest_authorized": False,
    "production_rollout_authorized": False,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect collector sensitive path permission metadata. Default schema mode "
            "is CI-safe and does not inspect real environment values. Current-env mode "
            "checks opaque path metadata only; it never prints values, raw paths, uid/gid, "
            "mode bits, secret contents, or starts runtime systems."
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
        help="schema is CI-safe; current-env inspects current process env names as opaque paths.",
    )
    return parser


def _blank_secret_file_entry(*, checked: bool, path_present: bool = False) -> dict[str, Any]:
    return {
        "checked": checked,
        "path_present": path_present,
        "exists": False,
        "is_file": False,
        "is_symlink": False,
        "readable_by_process": False,
        "owner_readable": False,
        "group_readable": False,
        "world_readable": False,
        "owner_writable": False,
        "group_writable": False,
        "world_writable": False,
        "unsafe_world_readable": False,
        "unsafe_group_writable": False,
        "unsafe_world_writable": False,
        "permission_status": "not_checked" if not checked else "missing",
    }


def _blank_tdlib_dir_entry(*, checked: bool, path_present: bool = False) -> dict[str, Any]:
    return {
        "checked": checked,
        "path_present": path_present,
        "exists": False,
        "is_dir": False,
        "is_symlink": False,
        "parent_exists": False,
        "writable_by_process_or_parent": False,
        "world_writable": False,
        "parent_world_writable": False,
        "unsafe_world_writable": False,
        "permission_status": "not_checked" if not checked else "missing",
    }


def _blank_singleton_entry(*, checked: bool, path_present: bool = False) -> dict[str, Any]:
    return {
        "checked": checked,
        "path_present": path_present,
        "parent_exists": False,
        "parent_writable_by_process": False,
        "parent_world_writable": False,
        "unsafe_parent_world_writable": False,
        "permission_status": "not_checked" if not checked else "missing",
    }


def _has_mode(mode: int, bit: int) -> bool:
    return bool(mode & bit)


def _stat_path(path: Path) -> os.stat_result | None:
    try:
        return path.stat()
    except OSError:
        return None


def _lstat_path(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except OSError:
        return None


def _permission_status_for_secret(entry: Mapping[str, Any]) -> str:
    if not entry["path_present"] or not entry["exists"]:
        return "missing"
    if entry["is_symlink"]:
        return "symlink"
    if not entry["is_file"]:
        return "not_file"
    if not entry["readable_by_process"]:
        return "unreadable"
    if entry["world_readable"]:
        return "world_readable"
    if entry["group_writable"]:
        return "group_writable"
    if entry["world_writable"]:
        return "world_writable"
    return "ok"


def _secret_file_permissions(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    lstat_result = _lstat_path(path)
    stat_result = _stat_path(path)
    mode = stat_result.st_mode if stat_result is not None else 0
    exists = stat_result is not None
    is_symlink = lstat_result is not None and stat.S_ISLNK(lstat_result.st_mode)
    entry = {
        "checked": True,
        "path_present": bool(path_value.strip()),
        "exists": exists,
        "is_file": stat.S_ISREG(mode) if exists else False,
        "is_symlink": is_symlink,
        "readable_by_process": os.access(path, os.R_OK) if exists else False,
        "owner_readable": _has_mode(mode, stat.S_IRUSR),
        "group_readable": _has_mode(mode, stat.S_IRGRP),
        "world_readable": _has_mode(mode, stat.S_IROTH),
        "owner_writable": _has_mode(mode, stat.S_IWUSR),
        "group_writable": _has_mode(mode, stat.S_IWGRP),
        "world_writable": _has_mode(mode, stat.S_IWOTH),
        "unsafe_world_readable": _has_mode(mode, stat.S_IROTH),
        "unsafe_group_writable": _has_mode(mode, stat.S_IWGRP),
        "unsafe_world_writable": _has_mode(mode, stat.S_IWOTH),
        "permission_status": "not_checked",
    }
    entry["permission_status"] = _permission_status_for_secret(entry)
    return entry


def _permission_status_for_tdlib(entry: Mapping[str, Any]) -> str:
    if not entry["path_present"]:
        return "missing"
    if entry["is_symlink"]:
        return "symlink"
    if entry["exists"] and not entry["is_dir"]:
        return "not_dir"
    if not entry["parent_exists"]:
        return "missing"
    if not entry["writable_by_process_or_parent"]:
        return "unwritable"
    if entry["world_writable"] or entry["parent_world_writable"]:
        return "world_writable"
    return "ok"


def _tdlib_dir_permissions(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    parent = path.parent
    lstat_result = _lstat_path(path)
    stat_result = _stat_path(path)
    parent_stat = _stat_path(parent)
    mode = stat_result.st_mode if stat_result is not None else 0
    parent_mode = parent_stat.st_mode if parent_stat is not None else 0
    exists = stat_result is not None
    is_dir = stat.S_ISDIR(mode) if exists else False
    parent_exists = parent_stat is not None
    is_symlink = lstat_result is not None and stat.S_ISLNK(lstat_result.st_mode)
    if exists and is_dir:
        writable_by_process_or_parent = os.access(path, os.W_OK)
    elif not exists and parent_exists:
        writable_by_process_or_parent = os.access(parent, os.W_OK)
    else:
        writable_by_process_or_parent = False
    entry = {
        "checked": True,
        "path_present": bool(path_value.strip()),
        "exists": exists,
        "is_dir": is_dir,
        "is_symlink": is_symlink,
        "parent_exists": parent_exists,
        "writable_by_process_or_parent": writable_by_process_or_parent,
        "world_writable": _has_mode(mode, stat.S_IWOTH),
        "parent_world_writable": _has_mode(parent_mode, stat.S_IWOTH),
        "unsafe_world_writable": _has_mode(mode, stat.S_IWOTH) or _has_mode(parent_mode, stat.S_IWOTH),
        "permission_status": "not_checked",
    }
    entry["permission_status"] = _permission_status_for_tdlib(entry)
    return entry


def _permission_status_for_singleton(entry: Mapping[str, Any]) -> str:
    if not entry["path_present"]:
        return "missing"
    if not entry["parent_exists"]:
        return "missing"
    if not entry["parent_writable_by_process"]:
        return "unwritable"
    if entry["parent_world_writable"]:
        return "world_writable"
    return "ok"


def _singleton_parent_permissions(path_value: str) -> dict[str, Any]:
    parent = Path(path_value).parent
    parent_stat = _stat_path(parent)
    parent_exists = parent_stat is not None
    parent_mode = parent_stat.st_mode if parent_stat is not None else 0
    entry = {
        "checked": True,
        "path_present": bool(path_value.strip()),
        "parent_exists": parent_exists,
        "parent_writable_by_process": os.access(parent, os.W_OK) if parent_exists else False,
        "parent_world_writable": _has_mode(parent_mode, stat.S_IWOTH),
        "unsafe_parent_world_writable": _has_mode(parent_mode, stat.S_IWOTH),
        "permission_status": "not_checked",
    }
    entry["permission_status"] = _permission_status_for_singleton(entry)
    return entry


def _env_value(environ: Mapping[str, str], name: str) -> str:
    return (environ.get(name) or "").strip()


def _add_failure(
    checks_failed: list[str],
    failures: list[dict[str, str]],
    *,
    env_name: str,
    reason_code: str,
) -> None:
    checks_failed.append(reason_code)
    failures.append({"env_name": env_name, "reason_code": reason_code})


def _build_permissions(
    *,
    mode: str,
    environ: Mapping[str, str],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    secret_file_permissions: dict[str, dict[str, Any]] = {}
    tdlib_path_permissions: dict[str, dict[str, Any]] = {}
    singleton_lock_parent_permissions: dict[str, dict[str, Any]] = {}

    for name in SECRET_FILE_ENV_NAMES:
        value = _env_value(environ, name)
        if mode == "schema":
            secret_file_permissions[name] = _blank_secret_file_entry(checked=False)
        elif value:
            secret_file_permissions[name] = _secret_file_permissions(value)
        else:
            secret_file_permissions[name] = _blank_secret_file_entry(checked=True)

    for name in TDLIB_DIR_ENV_NAMES:
        value = _env_value(environ, name)
        if mode == "schema":
            tdlib_path_permissions[name] = _blank_tdlib_dir_entry(checked=False)
        elif value:
            tdlib_path_permissions[name] = _tdlib_dir_permissions(value)
        else:
            tdlib_path_permissions[name] = _blank_tdlib_dir_entry(checked=True)

    value = _env_value(environ, SINGLETON_LOCK_ENV_NAME)
    if mode == "schema":
        singleton_lock_parent_permissions[SINGLETON_LOCK_ENV_NAME] = _blank_singleton_entry(checked=False)
    elif value:
        singleton_lock_parent_permissions[SINGLETON_LOCK_ENV_NAME] = _singleton_parent_permissions(value)
    elif _env_value(environ, "TDLIB_STATE_DIR"):
        entry = _blank_singleton_entry(checked=False)
        entry["permission_status"] = "not_applicable"
        singleton_lock_parent_permissions[SINGLETON_LOCK_ENV_NAME] = entry
    else:
        singleton_lock_parent_permissions[SINGLETON_LOCK_ENV_NAME] = _blank_singleton_entry(checked=True)

    return secret_file_permissions, tdlib_path_permissions, singleton_lock_parent_permissions


def _evaluate_failures(
    *,
    mode: str,
    secret_file_permissions: Mapping[str, Mapping[str, Any]],
    tdlib_path_permissions: Mapping[str, Mapping[str, Any]],
    singleton_lock_parent_permissions: Mapping[str, Mapping[str, Any]],
) -> tuple[list[str], list[dict[str, str]]]:
    checks_failed: list[str] = []
    failures: list[dict[str, str]] = []
    if mode != "current-env":
        return checks_failed, failures

    for name, entry in secret_file_permissions.items():
        status = str(entry["permission_status"])
        if status != "ok":
            _add_failure(
                checks_failed,
                failures,
                env_name=name,
                reason_code=f"{name}.{status}",
            )

    for name, entry in tdlib_path_permissions.items():
        status = str(entry["permission_status"])
        if status != "ok":
            _add_failure(
                checks_failed,
                failures,
                env_name=name,
                reason_code=f"{name}.{status}",
            )

    entry = singleton_lock_parent_permissions[SINGLETON_LOCK_ENV_NAME]
    status = str(entry["permission_status"])
    if status not in {"ok", "not_applicable"}:
        _add_failure(
            checks_failed,
            failures,
            env_name=SINGLETON_LOCK_ENV_NAME,
            reason_code=f"{SINGLETON_LOCK_ENV_NAME}.{status}",
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
    (
        secret_file_permissions,
        tdlib_path_permissions,
        singleton_lock_parent_permissions,
    ) = _build_permissions(mode=mode, environ=source)
    checks_failed, failures = _evaluate_failures(
        mode=mode,
        secret_file_permissions=secret_file_permissions,
        tdlib_path_permissions=tdlib_path_permissions,
        singleton_lock_parent_permissions=singleton_lock_parent_permissions,
    )

    return {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "mode": mode,
        "checks_failed": checks_failed,
        "failures": failures,
        "secret_file_permissions": secret_file_permissions,
        "tdlib_path_permissions": tdlib_path_permissions,
        "singleton_lock_parent_permissions": singleton_lock_parent_permissions,
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
