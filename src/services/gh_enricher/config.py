from __future__ import annotations

import os
from dataclasses import dataclass


class GhEnricherConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class GhEnricherConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    github_api_base_url: str
    github_app_id: str | None
    github_installation_id: str | None
    github_private_key: str | None
    request_timeout_sec: float
    sample_max_files: int
    sample_excerpt_chars: int
    max_file_bytes: int
    stale_after_sec: int
    log_level: str

    @classmethod
    def from_env(cls) -> "GhEnricherConfig":
        def read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        config = cls(
            app_env=read("APP_ENV", "dev").lower(),
            database_url=read("DATABASE_URL"),
            redis_url=read("REDIS_URL"),
            queue_name=read("GH_ENRICHER_QUEUE_NAME", "q.artifact.enrich.github"),
            consumer_group=read("GH_ENRICHER_CONSUMER_GROUP", "gh-enricher"),
            consumer_name=read("GH_ENRICHER_CONSUMER_NAME", "gh-enricher-1"),
            batch_size=int(read("GH_ENRICHER_BATCH_SIZE", "10")),
            block_ms=int(read("GH_ENRICHER_BLOCK_MS", "5000")),
            github_api_base_url=read("GITHUB_API_BASE_URL", "https://api.github.com"),
            github_app_id=read("GITHUB_APP_ID") or None,
            github_installation_id=read("GITHUB_INSTALLATION_ID") or None,
            github_private_key=read("GITHUB_PRIVATE_KEY") or None,
            request_timeout_sec=float(read("GH_ENRICHER_REQUEST_TIMEOUT_SEC", "10")),
            sample_max_files=int(read("GH_ENRICHER_SAMPLE_MAX_FILES", "20")),
            sample_excerpt_chars=int(read("GH_ENRICHER_SAMPLE_EXCERPT_CHARS", "1200")),
            max_file_bytes=int(read("GH_ENRICHER_MAX_FILE_BYTES", "131072")),
            stale_after_sec=int(read("GH_ENRICHER_STALE_AFTER_SEC", "21600")),
            log_level=read("LOG_LEVEL", "INFO").upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.database_url:
            raise GhEnricherConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise GhEnricherConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise GhEnricherConfigurationError("GH_ENRICHER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise GhEnricherConfigurationError("GH_ENRICHER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise GhEnricherConfigurationError("GH_ENRICHER_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise GhEnricherConfigurationError("GH_ENRICHER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_BLOCK_MS must be > 0")
        if self.request_timeout_sec <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_REQUEST_TIMEOUT_SEC must be > 0")
        if self.sample_max_files <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_SAMPLE_MAX_FILES must be > 0")
        if self.sample_excerpt_chars <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_SAMPLE_EXCERPT_CHARS must be > 0")
        if self.max_file_bytes <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_MAX_FILE_BYTES must be > 0")
        if self.stale_after_sec <= 0:
            raise GhEnricherConfigurationError("GH_ENRICHER_STALE_AFTER_SEC must be > 0")
