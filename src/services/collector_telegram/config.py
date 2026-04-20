"""Configuration loading and validation for the collector bootstrap."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Self

from .exceptions import CollectorTelegramConfigError
from .models import CollectorEnvironment, CollectorMode

_ALLOWED_APP_ENVS = {env.value for env in CollectorEnvironment}
_ALLOWED_MODES = {mode.value for mode in CollectorMode}
_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})


def _read_text_file(path_str: str, *, field_name: str) -> str:
    path = Path(path_str).expanduser()
    if not path.exists():
        raise CollectorTelegramConfigError(f"{field_name} file does not exist: {path}")
    if not path.is_file():
        raise CollectorTelegramConfigError(f"{field_name} path is not a file: {path}")

    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise CollectorTelegramConfigError(f"{field_name} file is empty: {path}")
    return value


def _read_secret(
    *,
    env_name: str,
    allow_empty: bool = False,
    default: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> str | None:
    env_map = os.environ if environ is None else environ
    file_env_name = f"{env_name}_FILE"
    file_value = env_map.get(file_env_name)
    direct_value = env_map.get(env_name)

    value: str | None
    if file_value:
        value = _read_text_file(file_value, field_name=file_env_name)
    else:
        value = direct_value if direct_value is not None else default

    if value is None:
        return None

    value = value.strip()
    if not value and not allow_empty:
        raise CollectorTelegramConfigError(f"{env_name} is empty")
    return value or None


def _read_required(env_name: str, *, environ: Mapping[str, str] | None = None) -> str:
    env_map = os.environ if environ is None else environ
    value = env_map.get(env_name)
    if value is None or not value.strip():
        raise CollectorTelegramConfigError(f"Missing required environment variable: {env_name}")
    return value.strip()


def _read_required_int(env_name: str, *, environ: Mapping[str, str] | None = None) -> int:
    raw = _read_required(env_name, environ=environ)
    try:
        return int(raw)
    except ValueError as exc:
        raise CollectorTelegramConfigError(f"{env_name} must be an integer: {raw}") from exc


def _read_int(env_name: str, *, default: int, environ: Mapping[str, str] | None = None) -> int:
    env_map = os.environ if environ is None else environ
    raw = env_map.get(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise CollectorTelegramConfigError(f"{env_name} must be an integer: {raw}") from exc


@dataclass(slots=True, frozen=True)
class CollectorTelegramConfig:
    app_env: CollectorEnvironment
    database_url: str
    redis_url: str | None
    collector_mode: CollectorMode

    telegram_api_id: int
    telegram_api_hash: str
    telegram_phone_number: str
    telegram_2fa_password: str | None

    tdlib_state_dir: str
    tdlib_files_dir: str
    tdlib_db_encryption_key: str

    reconcile_interval_sec: int
    reconcile_backfill_limit: int
    warm_backfill_limit: int
    history_page_limit: int

    log_level: str = "INFO"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Self:
        env_map = dict(os.environ) if environ is None else dict(environ)
        app_env = (env_map.get("APP_ENV", CollectorEnvironment.DEV.value) or CollectorEnvironment.DEV.value).strip().lower()
        collector_mode = (env_map.get("COLLECTOR_MODE", CollectorMode.REPLAY.value) or CollectorMode.REPLAY.value).strip().lower()
        tdlib_state_dir = _read_required("TDLIB_STATE_DIR", environ=env_map)

        config = cls(
            app_env=CollectorEnvironment(app_env),
            database_url=_read_required("DATABASE_URL", environ=env_map),
            redis_url=(env_map.get("REDIS_URL") or "").strip() or None,
            collector_mode=CollectorMode(collector_mode),
            telegram_api_id=_read_required_int("TELEGRAM_API_ID", environ=env_map),
            telegram_api_hash=_read_secret(env_name="TELEGRAM_API_HASH", environ=env_map) or "",
            telegram_phone_number=_read_required("TELEGRAM_PHONE_NUMBER", environ=env_map),
            telegram_2fa_password=_read_secret(
                env_name="TELEGRAM_2FA_PASSWORD",
                allow_empty=True,
                default=None,
                environ=env_map,
            ),
            tdlib_state_dir=tdlib_state_dir,
            tdlib_files_dir=(env_map.get("TDLIB_FILES_DIR") or "").strip() or tdlib_state_dir,
            tdlib_db_encryption_key=_read_secret(env_name="TDLIB_DB_ENCRYPTION_KEY", environ=env_map) or "",
            reconcile_interval_sec=_read_int("RECONCILE_INTERVAL_SEC", default=300, environ=env_map),
            reconcile_backfill_limit=_read_int("RECONCILE_BACKFILL_LIMIT", default=50, environ=env_map),
            warm_backfill_limit=_read_int("WARM_BACKFILL_LIMIT", default=30, environ=env_map),
            history_page_limit=_read_int("HISTORY_PAGE_LIMIT", default=50, environ=env_map),
            log_level=(env_map.get("LOG_LEVEL") or "INFO").strip().upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.app_env.value not in _ALLOWED_APP_ENVS:
            raise CollectorTelegramConfigError(f"APP_ENV must be one of {_ALLOWED_APP_ENVS}, got: {self.app_env}")

        if self.collector_mode.value not in _ALLOWED_MODES:
            raise CollectorTelegramConfigError(
                f"COLLECTOR_MODE must be one of {_ALLOWED_MODES}, got: {self.collector_mode}"
            )

        if self.app_env is CollectorEnvironment.PROD and self.collector_mode is not CollectorMode.LIVE:
            raise CollectorTelegramConfigError("prod environment requires COLLECTOR_MODE=live")

        if self.app_env in {CollectorEnvironment.DEV, CollectorEnvironment.TEST} and self.collector_mode is CollectorMode.LIVE:
            raise CollectorTelegramConfigError("dev/test environment must not use COLLECTOR_MODE=live")

        if self.reconcile_interval_sec <= 0:
            raise CollectorTelegramConfigError("RECONCILE_INTERVAL_SEC must be > 0")

        if self.reconcile_backfill_limit <= 0 or self.reconcile_backfill_limit > 100:
            raise CollectorTelegramConfigError("RECONCILE_BACKFILL_LIMIT must be between 1 and 100")

        if self.warm_backfill_limit <= 0 or self.warm_backfill_limit > 100:
            raise CollectorTelegramConfigError("WARM_BACKFILL_LIMIT must be between 1 and 100")

        if self.history_page_limit <= 0 or self.history_page_limit > 100:
            raise CollectorTelegramConfigError("HISTORY_PAGE_LIMIT must be between 1 and 100")

        if not self.telegram_api_hash:
            raise CollectorTelegramConfigError("TELEGRAM_API_HASH must be configured")

        if not self.tdlib_db_encryption_key:
            raise CollectorTelegramConfigError("TDLIB_DB_ENCRYPTION_KEY must be configured")

        if self.log_level not in _VALID_LOG_LEVELS:
            raise CollectorTelegramConfigError(
                f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}"
            )

        tdlib_state_dir = Path(self.tdlib_state_dir)
        tdlib_files_dir = Path(self.tdlib_files_dir)
        if tdlib_state_dir == tdlib_files_dir:
            raise CollectorTelegramConfigError("TDLIB_STATE_DIR and TDLIB_FILES_DIR must be distinct")

    def redacted(self) -> dict[str, object]:
        return {
            "app_env": self.app_env.value,
            "collector_mode": self.collector_mode.value,
            "has_database_url": bool(self.database_url),
            "has_redis_url": bool(self.redis_url),
            "telegram_api_id": self.telegram_api_id,
            "has_telegram_api_hash": bool(self.telegram_api_hash),
            "has_telegram_phone_number": bool(self.telegram_phone_number),
            "has_telegram_2fa_password": bool(self.telegram_2fa_password),
            "tdlib_state_dir": self.tdlib_state_dir,
            "tdlib_files_dir": self.tdlib_files_dir,
            "has_tdlib_db_encryption_key": bool(self.tdlib_db_encryption_key),
            "reconcile_interval_sec": self.reconcile_interval_sec,
            "reconcile_backfill_limit": self.reconcile_backfill_limit,
            "warm_backfill_limit": self.warm_backfill_limit,
            "history_page_limit": self.history_page_limit,
            "log_level": self.log_level,
        }
