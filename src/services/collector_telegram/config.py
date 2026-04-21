from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .exceptions import ConfigurationError
from .models import CollectorEnvironment, CollectorMode


_ALLOWED_APP_ENVS = {"prod", "dev", "test"}
_ALLOWED_MODES = {"live", "replay"}

EnvMapping = Mapping[str, str]


def _read_text_file(path_str: str, *, field_name: str) -> str:
    path = Path(path_str)
    if not path.exists():
        raise ConfigurationError(f"{field_name} file does not exist: {path}")
    if not path.is_file():
        raise ConfigurationError(f"{field_name} path is not a file: {path}")

    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ConfigurationError(f"{field_name} file is empty: {path}")
    return value


def _env_get(env: EnvMapping | None, name: str, default: str | None = None) -> str | None:
    if env is None:
        value = os.getenv(name)
    else:
        value = env.get(name, default)

    if value is None:
        return default
    return str(value)


def _read_secret(
    env: EnvMapping | None,
    *,
    env_name: str,
    allow_empty: bool = False,
    default: str | None = None,
) -> str | None:
    file_env_name = f"{env_name}_FILE"
    file_value = _env_get(env, file_env_name)
    direct_value = _env_get(env, env_name)

    value: str | None
    if file_value:
        value = _read_text_file(file_value, field_name=file_env_name)
    else:
        value = direct_value if direct_value is not None else default

    if value is None:
        return None

    if not value and not allow_empty:
        raise ConfigurationError(f"{env_name} is empty")
    return value


def _read_required(env: EnvMapping | None, env_name: str) -> str:
    value = _env_get(env, env_name)
    if value is None or not value.strip():
        raise ConfigurationError(f"Missing required environment variable: {env_name}")
    return value.strip()


def _read_required_int(env: EnvMapping | None, env_name: str) -> int:
    raw = _read_required(env, env_name)
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


def _read_int(env: EnvMapping | None, env_name: str, *, default: int) -> int:
    raw = _env_get(env, env_name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


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
    def from_env(cls, env: EnvMapping | None = None) -> "CollectorTelegramConfig":
        raw_app_env = (_env_get(env, "APP_ENV", "dev") or "dev").strip().lower()
        raw_collector_mode = (_env_get(env, "COLLECTOR_MODE", "replay") or "replay").strip().lower()

        if raw_app_env not in _ALLOWED_APP_ENVS:
            raise ConfigurationError(f"APP_ENV must be one of {_ALLOWED_APP_ENVS}, got: {raw_app_env}")
        if raw_collector_mode not in _ALLOWED_MODES:
            raise ConfigurationError(
                f"COLLECTOR_MODE must be one of {_ALLOWED_MODES}, got: {raw_collector_mode}"
            )

        tdlib_state_dir = _read_required(env, "TDLIB_STATE_DIR")
        tdlib_files_dir = (_env_get(env, "TDLIB_FILES_DIR") or "").strip() or tdlib_state_dir

        config = cls(
            app_env=CollectorEnvironment(raw_app_env),
            database_url=_read_required(env, "DATABASE_URL"),
            redis_url=((_env_get(env, "REDIS_URL") or "").strip() or None),
            collector_mode=CollectorMode(raw_collector_mode),
            telegram_api_id=_read_required_int(env, "TELEGRAM_API_ID"),
            telegram_api_hash=_read_secret(env, env_name="TELEGRAM_API_HASH"),
            telegram_phone_number=_read_required(env, "TELEGRAM_PHONE_NUMBER"),
            telegram_2fa_password=_read_secret(
                env,
                env_name="TELEGRAM_2FA_PASSWORD",
                allow_empty=True,
                default=None,
            ),
            tdlib_state_dir=tdlib_state_dir,
            tdlib_files_dir=tdlib_files_dir,
            tdlib_db_encryption_key=_read_secret(env, env_name="TDLIB_DB_ENCRYPTION_KEY"),
            reconcile_interval_sec=_read_int(env, "RECONCILE_INTERVAL_SEC", default=300),
            reconcile_backfill_limit=_read_int(env, "RECONCILE_BACKFILL_LIMIT", default=50),
            warm_backfill_limit=_read_int(env, "WARM_BACKFILL_LIMIT", default=30),
            history_page_limit=_read_int(env, "HISTORY_PAGE_LIMIT", default=50),
            log_level=(_env_get(env, "LOG_LEVEL", "INFO") or "INFO").strip().upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.app_env == CollectorEnvironment.PROD and self.collector_mode != CollectorMode.LIVE:
            raise ConfigurationError("prod environment requires COLLECTOR_MODE=live")

        if self.app_env in {CollectorEnvironment.DEV, CollectorEnvironment.TEST} and self.collector_mode == CollectorMode.LIVE:
            raise ConfigurationError("dev/test environment must not use COLLECTOR_MODE=live")

        if self.reconcile_interval_sec <= 0:
            raise ConfigurationError("RECONCILE_INTERVAL_SEC must be > 0")

        if self.reconcile_backfill_limit <= 0 or self.reconcile_backfill_limit > 100:
            raise ConfigurationError("RECONCILE_BACKFILL_LIMIT must be between 1 and 100")

        if self.warm_backfill_limit <= 0 or self.warm_backfill_limit > 100:
            raise ConfigurationError("WARM_BACKFILL_LIMIT must be between 1 and 100")

        if self.history_page_limit <= 0 or self.history_page_limit > 100:
            raise ConfigurationError("HISTORY_PAGE_LIMIT must be between 1 and 100")

        if not self.telegram_api_hash:
            raise ConfigurationError("TELEGRAM_API_HASH must be configured")

        if not self.tdlib_db_encryption_key:
            raise ConfigurationError("TDLIB_DB_ENCRYPTION_KEY must be configured")

    def ensure_runtime_dirs(self) -> None:
        Path(self.tdlib_state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.tdlib_files_dir).mkdir(parents=True, exist_ok=True)

    def redacted(self) -> dict[str, object]:
        return {
            "app_env": str(self.app_env),
            "collector_mode": str(self.collector_mode),
            "has_database_url": bool(self.database_url),
            "has_redis_url": bool(self.redis_url),
            "telegram_api_id": self.telegram_api_id,
            "telegram_phone_number": self.telegram_phone_number,
            "has_telegram_api_hash": bool(self.telegram_api_hash),
            "has_telegram_2fa_password": bool(self.telegram_2fa_password),
            "has_tdlib_db_encryption_key": bool(self.tdlib_db_encryption_key),
            "tdlib_state_dir": self.tdlib_state_dir,
            "tdlib_files_dir": self.tdlib_files_dir,
            "reconcile_interval_sec": self.reconcile_interval_sec,
            "reconcile_backfill_limit": self.reconcile_backfill_limit,
            "warm_backfill_limit": self.warm_backfill_limit,
            "history_page_limit": self.history_page_limit,
            "log_level": self.log_level,
        }