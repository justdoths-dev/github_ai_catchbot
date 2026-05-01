from __future__ import annotations

import os
from dataclasses import dataclass


class MaintenanceConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class MaintenanceConfig:
    app_env: str
    database_url: str
    redis_url: str
    maintenance_queue_name: str
    maintenance_consumer_group: str
    maintenance_consumer_name: str
    replay_queue_name: str
    replay_consumer_group: str
    replay_consumer_name: str
    batch_size: int
    block_ms: int
    retry_scan_poll_sec: int
    delivery_retry_max_attempts: int
    enable_delivery_retry_promotion: bool
    enable_replay_to_prod_db: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "MaintenanceConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        try:
            cfg = cls(
                app_env=_read("APP_ENV", "dev").lower(),
                database_url=_read("DATABASE_URL"),
                redis_url=_read("REDIS_URL"),
                maintenance_queue_name=_read("MAINTENANCE_QUEUE_NAME", "q.maintenance"),
                maintenance_consumer_group=_read("MAINTENANCE_CONSUMER_GROUP", "maintenance"),
                maintenance_consumer_name=_read("MAINTENANCE_CONSUMER_NAME", "maintenance-1"),
                replay_queue_name=_read("REPLAY_QUEUE_NAME", "q.replay"),
                replay_consumer_group=_read("REPLAY_CONSUMER_GROUP", "maintenance-replay"),
                replay_consumer_name=_read("REPLAY_CONSUMER_NAME", "maintenance-replay-1"),
                batch_size=int(_read("MAINTENANCE_BATCH_SIZE", "50")),
                block_ms=int(_read("MAINTENANCE_BLOCK_MS", "5000")),
                retry_scan_poll_sec=int(_read("MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC", "30")),
                delivery_retry_max_attempts=int(
                    _read("DELIVERY_RETRY_MAX_ATTEMPTS", _read("NOTIFICATION_RETRY_MAX_ATTEMPTS", "3"))
                ),
                enable_delivery_retry_promotion=_bool_env(
                    _read("MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION", "false")
                ),
                enable_replay_to_prod_db=_bool_env(_read("ENABLE_REPLAY_TO_PROD_DB", "false")),
                log_level=_read("LOG_LEVEL", "INFO").upper(),
            )
        except ValueError as exc:
            raise MaintenanceConfigurationError(str(exc)) from exc
        cfg.validate()
        return cfg

    @property
    def is_prod(self) -> bool:
        return self.app_env in {"prod", "production"}

    def replay_dispatch_allowed(self) -> bool:
        return self.app_env in {"dev", "test", "replay"} or (self.is_prod and self.enable_replay_to_prod_db)

    def validate(self) -> None:
        if not self.database_url:
            raise MaintenanceConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise MaintenanceConfigurationError("REDIS_URL is required")
        if not self.maintenance_queue_name:
            raise MaintenanceConfigurationError("MAINTENANCE_QUEUE_NAME must not be empty")
        if not self.replay_queue_name:
            raise MaintenanceConfigurationError("REPLAY_QUEUE_NAME must not be empty")
        if self.batch_size < 1 or self.batch_size > 500:
            raise MaintenanceConfigurationError("MAINTENANCE_BATCH_SIZE must be between 1 and 500")
        if self.block_ms <= 0:
            raise MaintenanceConfigurationError("MAINTENANCE_BLOCK_MS must be > 0")
        if self.retry_scan_poll_sec <= 0:
            raise MaintenanceConfigurationError("MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC must be > 0")
        if self.delivery_retry_max_attempts <= 0:
            raise MaintenanceConfigurationError("DELIVERY_RETRY_MAX_ATTEMPTS must be > 0")


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
