from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS = (
    "APP_ENV",
    "COLLECTOR_MODE",
    "DATABASE_URL",
    "REDIS_URL",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_API_HASH_FILE",
    "TELEGRAM_PHONE_NUMBER",
    "TELEGRAM_2FA_PASSWORD",
    "TELEGRAM_2FA_PASSWORD_FILE",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_DB_ENCRYPTION_KEY_FILE",
    "RECONCILE_INTERVAL_SEC",
    "RECONCILE_BACKFILL_LIMIT",
    "WARM_BACKFILL_LIMIT",
    "HISTORY_PAGE_LIMIT",
    "COLLECTOR_SINGLETON_LOCK_PATH",
    "STARTUP_PROBE_TIMEOUT_SEC",
    "STARTUP_WARM_BACKFILL_ENABLED",
    "LOG_LEVEL",
)

COLLECTOR_RUNTIME_ENV_REQUIRED_KEYS = (
    "APP_ENV",
    "COLLECTOR_MODE",
    "DATABASE_URL",
    "TELEGRAM_API_ID",
    "TELEGRAM_PHONE_NUMBER",
    "TDLIB_STATE_DIR",
)

COLLECTOR_RUNTIME_ENV_REQUIRED_GROUPS = (
    ("TELEGRAM_API_HASH", "TELEGRAM_API_HASH_FILE"),
    ("TDLIB_DB_ENCRYPTION_KEY", "TDLIB_DB_ENCRYPTION_KEY_FILE"),
)

COLLECTOR_RUNTIME_ENV_FORBIDDEN_SOURCE_KEYS = (
    "ENABLE_NOTIFICATION_SEND",
    "ENABLE_REPLAY_TO_PROD_DB",
    "GITHUB_PRIVATE_KEY",
    "GITHUB_PRIVATE_KEY_FILE",
    "GITHUB_TOKEN",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "OPENAI_API_KEY",
    "OPENAI_API_KEY_FILE",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_BOT_TOKEN_FILE",
    "TELEGRAM_OPERATOR_CHAT_ID",
    "X_BEARER_TOKEN",
    "X_BEARER_TOKEN_FILE",
)


_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_KEYS = frozenset(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
_FORBIDDEN_KEYS = frozenset(COLLECTOR_RUNTIME_ENV_FORBIDDEN_SOURCE_KEYS)


@dataclass(frozen=True, slots=True)
class CollectorRuntimeEnvOverlayResult:
    status: str
    reason_code: str
    child_overlay: Mapping[str, str] = field(default_factory=dict)
    ignored_unknown_keys: tuple[str, ...] = ()
    ignored_forbidden_keys: tuple[str, ...] = ()
    missing_required_keys: tuple[str, ...] = ()
    missing_required_groups: tuple[str, ...] = ()
    duplicate_keys: tuple[str, ...] = ()
    invalid_line_numbers: tuple[int, ...] = ()
    file_permission_checked: bool = False
    file_permission_mode: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "pass"

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "collector_runtime_env_overlay_v1",
            "status": self.status,
            "reason_code": self.reason_code,
            "source_runtime_env_allows_extra_keys": True,
            "source_unknown_keys_ignored": True,
            "source_forbidden_keys_ignored": True,
            "child_overlay_only": True,
            "child_overlay_allowed_keys": list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS),
            "child_overlay_keys": sorted(self.child_overlay),
            "child_overlay_rejects_unknown_keys": True,
            "child_overlay_rejects_forbidden_keys": True,
            "ignored_unknown_keys": list(self.ignored_unknown_keys),
            "ignored_forbidden_keys": list(self.ignored_forbidden_keys),
            "missing_required_keys": list(self.missing_required_keys),
            "missing_required_groups": list(self.missing_required_groups),
            "duplicate_keys": list(self.duplicate_keys),
            "invalid_line_numbers": list(self.invalid_line_numbers),
            "runtime_env_values_printed": False,
            "runtime_env_file_contents_printed": False,
            "runtime_env_file_path_printed": False,
            "file_permission_checked": self.file_permission_checked,
            "file_permission_mode": self.file_permission_mode,
        }


def build_collector_runtime_env_overlay(runtime_env_file: str | os.PathLike[str]) -> CollectorRuntimeEnvOverlayResult:
    path = Path(runtime_env_file)
    try:
        file_status = _runtime_env_file_status(path)
    except OSError:
        return _blocked("runtime_env_file_unreadable")
    if file_status is not None:
        return file_status

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return _blocked("runtime_env_file_unreadable")

    parsed: dict[str, str] = {}
    duplicate_keys: set[str] = set()
    invalid_line_numbers: list[int] = []
    for line_number, raw_line in enumerate(lines, start=1):
        parsed_line = _parse_env_line(raw_line)
        if parsed_line is None:
            continue
        if parsed_line == (None, None):
            invalid_line_numbers.append(line_number)
            continue
        key, value = parsed_line
        assert key is not None
        assert value is not None
        if key in parsed:
            duplicate_keys.add(key)
            continue
        parsed[key] = value

    if invalid_line_numbers:
        return _blocked(
            "invalid_runtime_env_line",
            invalid_line_numbers=tuple(invalid_line_numbers),
            file_permission_checked=True,
            file_permission_mode=_safe_mode(path),
        )
    if duplicate_keys:
        return _blocked(
            "duplicate_runtime_env_keys",
            duplicate_keys=tuple(sorted(duplicate_keys)),
            file_permission_checked=True,
            file_permission_mode=_safe_mode(path),
        )

    ignored_forbidden_keys = tuple(sorted(key for key in parsed if key in _FORBIDDEN_KEYS))
    ignored_unknown_keys = tuple(sorted(key for key in parsed if key not in _ALLOWED_KEYS and key not in _FORBIDDEN_KEYS))
    child_overlay = {key: value for key, value in parsed.items() if key in _ALLOWED_KEYS}

    overlay_error = _child_overlay_key_error(child_overlay)
    if overlay_error is not None:
        return _blocked(
            overlay_error,
            ignored_unknown_keys=ignored_unknown_keys,
            ignored_forbidden_keys=ignored_forbidden_keys,
            file_permission_checked=True,
            file_permission_mode=_safe_mode(path),
        )

    missing_required_keys = tuple(
        key for key in COLLECTOR_RUNTIME_ENV_REQUIRED_KEYS if not child_overlay.get(key, "").strip()
    )
    missing_required_groups = tuple(
        "|".join(group)
        for group in COLLECTOR_RUNTIME_ENV_REQUIRED_GROUPS
        if not any(child_overlay.get(key, "").strip() for key in group)
    )
    if missing_required_keys:
        return _blocked(
            "missing_required_collector_runtime_env_keys",
            child_overlay=child_overlay,
            ignored_unknown_keys=ignored_unknown_keys,
            ignored_forbidden_keys=ignored_forbidden_keys,
            missing_required_keys=missing_required_keys,
            missing_required_groups=missing_required_groups,
            file_permission_checked=True,
            file_permission_mode=_safe_mode(path),
        )
    if missing_required_groups:
        return _blocked(
            "missing_required_collector_runtime_env_groups",
            child_overlay=child_overlay,
            ignored_unknown_keys=ignored_unknown_keys,
            ignored_forbidden_keys=ignored_forbidden_keys,
            missing_required_groups=missing_required_groups,
            file_permission_checked=True,
            file_permission_mode=_safe_mode(path),
        )

    return CollectorRuntimeEnvOverlayResult(
        status="pass",
        reason_code="collector_runtime_env_overlay_ready",
        child_overlay=child_overlay,
        ignored_unknown_keys=ignored_unknown_keys,
        ignored_forbidden_keys=ignored_forbidden_keys,
        file_permission_checked=True,
        file_permission_mode=_safe_mode(path),
    )


def _runtime_env_file_status(path: Path) -> CollectorRuntimeEnvOverlayResult | None:
    if not path.is_file():
        return _blocked("runtime_env_file_missing")
    file_stat = path.stat()
    mode = stat.S_IMODE(file_stat.st_mode)
    if os.name != "nt":
        group_permissions = mode & stat.S_IRWXG
        other_permissions = mode & stat.S_IRWXO
        permissions_too_open = bool(other_permissions)
        if group_permissions:
            group_read_only = group_permissions == stat.S_IRGRP
            process_groups = {os.getegid(), *os.getgroups()} if group_read_only else set()
            permissions_too_open = permissions_too_open or file_stat.st_gid not in process_groups
        if permissions_too_open:
            return _blocked(
                "runtime_env_file_permissions_too_open",
                file_permission_checked=True,
                file_permission_mode=f"{mode:04o}",
            )
    return None


def _parse_env_line(raw_line: str) -> tuple[str | None, str | None] | None:
    line = raw_line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):
        line = line[len("export ") :].lstrip()
    if "=" not in line:
        return (None, None)
    key, value = line.split("=", 1)
    key = key.strip()
    if not key or not _ENV_KEY_RE.fullmatch(key):
        return (None, None)
    value = value.strip()
    if value.startswith(("'", '"')):
        if len(value) < 2 or value[-1] != value[0]:
            return (None, None)
        value = value[1:-1]
    return key, value


def _child_overlay_key_error(child_overlay: Mapping[str, str]) -> str | None:
    if any(key not in _ALLOWED_KEYS for key in child_overlay):
        return "child_overlay_unknown_keys"
    if any(key in _FORBIDDEN_KEYS for key in child_overlay):
        return "child_overlay_forbidden_keys"
    return None


def _blocked(
    reason_code: str,
    *,
    child_overlay: Mapping[str, str] | None = None,
    ignored_unknown_keys: tuple[str, ...] = (),
    ignored_forbidden_keys: tuple[str, ...] = (),
    missing_required_keys: tuple[str, ...] = (),
    missing_required_groups: tuple[str, ...] = (),
    duplicate_keys: tuple[str, ...] = (),
    invalid_line_numbers: tuple[int, ...] = (),
    file_permission_checked: bool = False,
    file_permission_mode: str | None = None,
) -> CollectorRuntimeEnvOverlayResult:
    return CollectorRuntimeEnvOverlayResult(
        status="blocked",
        reason_code=reason_code,
        child_overlay=dict(child_overlay or {}),
        ignored_unknown_keys=ignored_unknown_keys,
        ignored_forbidden_keys=ignored_forbidden_keys,
        missing_required_keys=missing_required_keys,
        missing_required_groups=missing_required_groups,
        duplicate_keys=duplicate_keys,
        invalid_line_numbers=invalid_line_numbers,
        file_permission_checked=file_permission_checked,
        file_permission_mode=file_permission_mode,
    )


def _safe_mode(path: Path) -> str | None:
    try:
        return f"{stat.S_IMODE(path.stat().st_mode):04o}"
    except OSError:
        return None


__all__ = [
    "COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS",
    "COLLECTOR_RUNTIME_ENV_FORBIDDEN_SOURCE_KEYS",
    "COLLECTOR_RUNTIME_ENV_REQUIRED_GROUPS",
    "COLLECTOR_RUNTIME_ENV_REQUIRED_KEYS",
    "CollectorRuntimeEnvOverlayResult",
    "build_collector_runtime_env_overlay",
]
