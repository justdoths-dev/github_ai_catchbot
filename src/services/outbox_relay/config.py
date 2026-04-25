from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


class OutboxRelayConfigurationError(ValueError):
    """Raised when outbox-relay configuration is invalid."""


def _env_get(env: Mapping[str, str], name: str, default: str | None = None) -> str | None:
    value = env.get(name)
    if value is None:
        return default
    return value


@dataclass(slots=True, frozen=True)
class OutboxRelayConfig:
    app_env: str
    database_url: str
    redis_url: str
    poll_interval_ms: int
    batch_size: int
    xadd_maxlen: int | None
    log_level: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "OutboxRelayConfig":
        effective_env = os.environ if env is None else env

        database_url = (_env_get(effective_env, "DATABASE_URL", "") or "").strip()
        redis_url = (_env_get(effective_env, "REDIS_URL", "") or "").strip()
        if not database_url:
            raise OutboxRelayConfigurationError("DATABASE_URL is required")
        if not redis_url:
            raise OutboxRelayConfigurationError("REDIS_URL is required")

        poll_interval_ms = int(
            (_env_get(effective_env, "OUTBOX_RELAY_POLL_INTERVAL_MS", "1000") or "1000").strip()
        )
        batch_size = int((_env_get(effective_env, "OUTBOX_RELAY_BATCH_SIZE", "100") or "100").strip())
        xadd_maxlen_raw = (_env_get(effective_env, "OUTBOX_RELAY_XADD_MAXLEN", "10000") or "10000").strip()
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None

        cfg = cls(
            app_env=(_env_get(effective_env, "APP_ENV", "dev") or "dev").strip().lower(),
            database_url=database_url,
            redis_url=redis_url,
            poll_interval_ms=poll_interval_ms,
            batch_size=batch_size,
            xadd_maxlen=xadd_maxlen,
            log_level=(_env_get(effective_env, "LOG_LEVEL", "INFO") or "INFO").strip().upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.poll_interval_ms <= 0:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_POLL_INTERVAL_MS must be > 0")
        if self.batch_size <= 0:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_BATCH_SIZE must be > 0")
        if self.batch_size > 1000:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_BATCH_SIZE must be <= 1000")
        if self.xadd_maxlen is not None and self.xadd_maxlen <= 0:
            raise OutboxRelayConfigurationError("OUTBOX_RELAY_XADD_MAXLEN must be > 0 when set")
