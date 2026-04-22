from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

from .exceptions import ConfigurationError
from .models import AppEnv, CollectorMode


_ALLOWED_APP_ENVS = {"prod", "dev", "test"}
_ALLOWED_MODES = {"live", "replay"}


def _env_get(env: Mapping[str, str], name: str, default: str | None = None) -> str | None:
    value = env.get(name)
    if value is None:
        return default
    return value


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


def _read_secret(
    env: Mapping[str, str],
    *,
    env_name: str,
    allow_empty: bool = False,
    default: str | None = None,
) -> str | None:
    file_env_name = f"{env_name}_FILE"
    file_value = _env_get(env, file_env_name)
    direct_value = _env_get(env, env_name)

    value: str | None
    if file_value is not None and file_value.strip():
        value = _read_text_file(file_value.strip(), field_name=file_env_name)
    else:
        value = direct_value if direct_value is not None else default

    if value is None:
        return None

    value = value.strip() if isinstance(value, str) else value
    if value == "" and not allow_empty:
        raise ConfigurationError(f"{env_name} is empty")
    return value


def _read_required(env: Mapping[str, str], env_name: str) -> str:
    value = _env_get(env, env_name)
    if value is None or not value.strip():
        raise ConfigurationError(f"Missing required environment variable: {env_name}")
    return value.strip()


def _read_required_int(env: Mapping[str, str], env_name: str) -> int:
    raw = _read_required(env, env_name)
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


def _read_int(env: Mapping[str, str], env_name: str, *, default: int) -> int:
    raw = _env_get(env, env_name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{env_name} must be an integer: {raw}") from exc


def _read_bool(env: Mapping[str, str], env_name: str, *, default: bool) -> bool:
    raw = _env_get(env, env_name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in {"0", "false", "no"}


def _redact_url_secret(url: str | None) -> str | None:
    if not url:
        return url

    parsed = urlsplit(url)
    if parsed.username is None and parsed.password is None:
        return url

    hostname = parsed.hostname or ""
    if parsed.port is not None:
        hostname = f"{hostname}:{parsed.port}"

    if parsed.username:
        netloc = f"{parsed.username}:***@{hostname}"
    else:
        netloc = f"***@{hostname}"

    return urlunsplit(
        (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
    )


@dataclass(slots=True, frozen=True)
class CollectorTelegramConfig:
    app_env: AppEnv
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

    singleton_lock_path: str = ""
    startup_probe_timeout_sec: int = 30
    startup_warm_backfill_enabled: bool = True

    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if not self.singleton_lock_path.strip():
            object.__setattr__(
                self,
                "singleton_lock_path",
                str(Path(self.tdlib_state_dir) / "collector-live.lock"),
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "CollectorTelegramConfig":
        effective_env: Mapping[str, str]
        if env is None:
            effective_env = os.environ
        else:
            effective_env = env

        app_env = (_env_get(effective_env, "APP_ENV", "dev") or "dev").strip().lower()
        collector_mode = (_env_get(effective_env, "COLLECTOR_MODE", "replay") or "replay").strip().lower()
        tdlib_state_dir = _read_required(effective_env, "TDLIB_STATE_DIR")

        config = cls(
            app_env=app_env,  # type: ignore[arg-type]
            database_url=_read_required(effective_env, "DATABASE_URL"),
            redis_url=(_env_get(effective_env, "REDIS_URL") or "").strip() or None,
            collector_mode=collector_mode,  # type: ignore[arg-type]
            telegram_api_id=_read_required_int(effective_env, "TELEGRAM_API_ID"),
            telegram_api_hash=_read_secret(effective_env, env_name="TELEGRAM_API_HASH"),
            telegram_phone_number=_read_required(effective_env, "TELEGRAM_PHONE_NUMBER"),
            telegram_2fa_password=_read_secret(
                effective_env,
                env_name="TELEGRAM_2FA_PASSWORD",
                allow_empty=True,
                default=None,
            ),
            tdlib_state_dir=tdlib_state_dir,
            tdlib_files_dir=(
                (_env_get(effective_env, "TDLIB_FILES_DIR") or "").strip() or tdlib_state_dir
            ),
            tdlib_db_encryption_key=_read_secret(
                effective_env,
                env_name="TDLIB_DB_ENCRYPTION_KEY",
            ),
            reconcile_interval_sec=_read_int(effective_env, "RECONCILE_INTERVAL_SEC", default=300),
            reconcile_backfill_limit=_read_int(effective_env, "RECONCILE_BACKFILL_LIMIT", default=50),
            warm_backfill_limit=_read_int(effective_env, "WARM_BACKFILL_LIMIT", default=30),
            history_page_limit=_read_int(effective_env, "HISTORY_PAGE_LIMIT", default=50),
            singleton_lock_path=(
                (_env_get(effective_env, "COLLECTOR_SINGLETON_LOCK_PATH") or "").strip()
                or str(Path(tdlib_state_dir) / "collector-live.lock")
            ),
            startup_probe_timeout_sec=_read_int(effective_env, "STARTUP_PROBE_TIMEOUT_SEC", default=30),
            startup_warm_backfill_enabled=_read_bool(
                effective_env,
                "STARTUP_WARM_BACKFILL_ENABLED",
                default=True,
            ),
            log_level=(_env_get(effective_env, "LOG_LEVEL", "INFO") or "INFO").strip().upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.app_env not in _ALLOWED_APP_ENVS:
            raise ConfigurationError(f"APP_ENV must be one of {_ALLOWED_APP_ENVS}, got: {self.app_env}")

        if self.collector_mode not in _ALLOWED_MODES:
            raise ConfigurationError(
                f"COLLECTOR_MODE must be one of {_ALLOWED_MODES}, got: {self.collector_mode}"
            )

        if self.app_env == "prod" and self.collector_mode != "live":
            raise ConfigurationError("prod environment requires COLLECTOR_MODE=live")

        if self.app_env in {"dev", "test"} and self.collector_mode == "live":
            raise ConfigurationError("dev/test environment must not use COLLECTOR_MODE=live")

        if self.reconcile_interval_sec <= 0:
            raise ConfigurationError("RECONCILE_INTERVAL_SEC must be > 0")

        if self.reconcile_backfill_limit <= 0 or self.reconcile_backfill_limit > 100:
            raise ConfigurationError("RECONCILE_BACKFILL_LIMIT must be between 1 and 100")

        if self.warm_backfill_limit <= 0 or self.warm_backfill_limit > 100:
            raise ConfigurationError("WARM_BACKFILL_LIMIT must be between 1 and 100")

        if self.history_page_limit <= 0 or self.history_page_limit > 100:
            raise ConfigurationError("HISTORY_PAGE_LIMIT must be between 1 and 100")

        if self.startup_probe_timeout_sec <= 0:
            raise ConfigurationError("STARTUP_PROBE_TIMEOUT_SEC must be > 0")

        if not self.telegram_api_hash:
            raise ConfigurationError("TELEGRAM_API_HASH must be configured")

        if not self.tdlib_db_encryption_key:
            raise ConfigurationError("TDLIB_DB_ENCRYPTION_KEY must be configured")

        if not self.singleton_lock_path.strip():
            raise ConfigurationError("COLLECTOR_SINGLETON_LOCK_PATH must not be empty")

    def ensure_runtime_dirs(self) -> None:
        Path(self.tdlib_state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.tdlib_files_dir).mkdir(parents=True, exist_ok=True)
        Path(self.singleton_lock_path).parent.mkdir(parents=True, exist_ok=True)

    def redacted(self) -> dict[str, Any]:
        return {
            "app_env": self.app_env,
            "collector_mode": self.collector_mode,
            "telegram_api_id": self.telegram_api_id,
            "telegram_phone_number": self.telegram_phone_number,
            "tdlib_state_dir": self.tdlib_state_dir,
            "tdlib_files_dir": self.tdlib_files_dir,
            "reconcile_interval_sec": self.reconcile_interval_sec,
            "reconcile_backfill_limit": self.reconcile_backfill_limit,
            "warm_backfill_limit": self.warm_backfill_limit,
            "history_page_limit": self.history_page_limit,
            "singleton_lock_path": self.singleton_lock_path,
            "startup_probe_timeout_sec": self.startup_probe_timeout_sec,
            "startup_warm_backfill_enabled": self.startup_warm_backfill_enabled,
            "log_level": self.log_level,
            "has_database_url": bool(self.database_url),
            "has_redis_url": bool(self.redis_url),
            "has_telegram_api_hash": bool(self.telegram_api_hash),
            "has_telegram_2fa_password": bool(self.telegram_2fa_password),
            "has_tdlib_db_encryption_key": bool(self.tdlib_db_encryption_key),
        }
