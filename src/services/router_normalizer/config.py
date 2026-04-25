from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


class RouterNormalizerConfigurationError(ValueError):
    """Raised when router-normalizer configuration is invalid."""


def _env_get(env: Mapping[str, str], name: str, default: str | None = None) -> str | None:
    value = env.get(name)
    if value is None:
        return default
    return value


@dataclass(slots=True, frozen=True)
class RouterNormalizerConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    block_ms: int
    batch_size: int
    normalizer_version: str
    short_url_allowlist: tuple[str, ...]
    short_url_hop_limit: int
    short_url_timeout_seconds: float
    log_level: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "RouterNormalizerConfig":
        effective_env = os.environ if env is None else env
        database_url = (_env_get(effective_env, "DATABASE_URL", "") or "").strip()
        redis_url = (_env_get(effective_env, "REDIS_URL", "") or "").strip()
        if not database_url:
            raise RouterNormalizerConfigurationError("DATABASE_URL is required")
        if not redis_url:
            raise RouterNormalizerConfigurationError("REDIS_URL is required")

        allowlist_raw = (
            _env_get(
                effective_env,
                "ROUTER_NORMALIZER_SHORT_URL_ALLOWLIST",
                "bit.ly,t.co,tinyurl.com,ow.ly,lnkd.in,buff.ly,goo.gl",
            )
            or ""
        )
        cfg = cls(
            app_env=(_env_get(effective_env, "APP_ENV", "dev") or "dev").strip().lower(),
            database_url=database_url,
            redis_url=redis_url,
            queue_name=(_env_get(effective_env, "ROUTER_NORMALIZER_QUEUE", "q.source.normalize") or "").strip(),
            consumer_group=(
                _env_get(effective_env, "ROUTER_NORMALIZER_CONSUMER_GROUP", "router-normalizer") or ""
            ).strip(),
            consumer_name=(
                _env_get(effective_env, "ROUTER_NORMALIZER_CONSUMER_NAME", "router-normalizer-1") or ""
            ).strip(),
            block_ms=int((_env_get(effective_env, "ROUTER_NORMALIZER_BLOCK_MS", "5000") or "5000").strip()),
            batch_size=int((_env_get(effective_env, "ROUTER_NORMALIZER_BATCH_SIZE", "10") or "10").strip()),
            normalizer_version=(
                _env_get(effective_env, "ROUTER_NORMALIZER_VERSION", "router-normalizer-v1") or ""
            ).strip(),
            short_url_allowlist=tuple(
                host.strip().lower() for host in allowlist_raw.split(",") if host.strip()
            ),
            short_url_hop_limit=int(
                (_env_get(effective_env, "ROUTER_NORMALIZER_SHORT_URL_HOP_LIMIT", "3") or "3").strip()
            ),
            short_url_timeout_seconds=float(
                (_env_get(effective_env, "ROUTER_NORMALIZER_SHORT_URL_TIMEOUT_SECONDS", "2.0") or "2.0").strip()
            ),
            log_level=(_env_get(effective_env, "LOG_LEVEL", "INFO") or "INFO").strip().upper(),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.queue_name:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_QUEUE is required")
        if not self.consumer_group:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_CONSUMER_GROUP is required")
        if not self.consumer_name:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_CONSUMER_NAME is required")
        if self.block_ms <= 0:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_BLOCK_MS must be > 0")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_BATCH_SIZE must be between 1 and 100")
        if not self.normalizer_version:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_VERSION is required")
        if self.short_url_hop_limit <= 0:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_SHORT_URL_HOP_LIMIT must be > 0")
        if self.short_url_timeout_seconds <= 0:
            raise RouterNormalizerConfigurationError("ROUTER_NORMALIZER_SHORT_URL_TIMEOUT_SECONDS must be > 0")
