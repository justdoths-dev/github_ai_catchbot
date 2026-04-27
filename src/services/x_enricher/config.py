from __future__ import annotations

import os
from dataclasses import dataclass


class XEnricherConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class XEnricherConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    x_api_base_url: str
    x_bearer_token: str
    request_timeout_sec: float
    request_max_ids: int
    depth_budget_default: int
    log_level: str

    @classmethod
    def from_env(cls) -> "XEnricherConfig":
        def read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        config = cls(
            app_env=read("APP_ENV", "dev").lower(),
            database_url=read("DATABASE_URL"),
            redis_url=read("REDIS_URL"),
            queue_name=read("X_ENRICHER_QUEUE_NAME", "q.artifact.enrich.x"),
            consumer_group=read("X_ENRICHER_CONSUMER_GROUP", "x-enricher"),
            consumer_name=read("X_ENRICHER_CONSUMER_NAME", "x-enricher-1"),
            batch_size=int(read("X_ENRICHER_BATCH_SIZE", "10")),
            block_ms=int(read("X_ENRICHER_BLOCK_MS", "5000")),
            x_api_base_url=read("X_API_BASE_URL", read("X_BASE_URL", "https://api.x.com")),
            x_bearer_token=read("X_BEARER_TOKEN"),
            request_timeout_sec=float(read("X_ENRICHER_REQUEST_TIMEOUT_SEC", read("X_REQUEST_TIMEOUT_SEC", "10"))),
            request_max_ids=int(read("X_ENRICHER_REQUEST_MAX_IDS", read("X_REQUEST_MAX_IDS", "100"))),
            depth_budget_default=int(read("X_DEPTH_BUDGET_DEFAULT", "1")),
            log_level=read("LOG_LEVEL", "INFO").upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.database_url:
            raise XEnricherConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise XEnricherConfigurationError("REDIS_URL is required")
        if not self.x_bearer_token:
            raise XEnricherConfigurationError("X_BEARER_TOKEN is required")
        if not self.queue_name:
            raise XEnricherConfigurationError("X_ENRICHER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise XEnricherConfigurationError("X_ENRICHER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise XEnricherConfigurationError("X_ENRICHER_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise XEnricherConfigurationError("X_ENRICHER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise XEnricherConfigurationError("X_ENRICHER_BLOCK_MS must be > 0")
        if not self.x_api_base_url.startswith("https://"):
            raise XEnricherConfigurationError("X_API_BASE_URL must start with https://")
        if self.request_timeout_sec <= 0:
            raise XEnricherConfigurationError("X_ENRICHER_REQUEST_TIMEOUT_SEC must be > 0")
        if self.request_max_ids <= 0 or self.request_max_ids > 100:
            raise XEnricherConfigurationError("X_ENRICHER_REQUEST_MAX_IDS must be between 1 and 100")
        if self.depth_budget_default != 1:
            raise XEnricherConfigurationError("v0.1 only supports X_DEPTH_BUDGET_DEFAULT=1")
