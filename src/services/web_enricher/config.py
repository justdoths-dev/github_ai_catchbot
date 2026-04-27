from __future__ import annotations

import os
from dataclasses import dataclass


class WebEnricherConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class WebEnricherConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    request_timeout_sec: float
    max_redirects: int
    max_bytes: int
    excerpt_chars: int
    max_outbound_links: int
    user_agent: str
    content_type_allowlist: tuple[str, ...]
    log_level: str

    @classmethod
    def from_env(cls) -> "WebEnricherConfig":
        def read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        allowlist = tuple(
            part.strip().lower()
            for part in read(
                "WEB_FETCH_CONTENT_TYPE_ALLOWLIST",
                "text/html,application/xhtml+xml,text/plain,text/markdown",
            ).split(",")
            if part.strip()
        )
        config = cls(
            app_env=read("APP_ENV", "dev").lower(),
            database_url=read("DATABASE_URL"),
            redis_url=read("REDIS_URL"),
            queue_name=read("WEB_ENRICHER_QUEUE_NAME", "q.artifact.enrich.web"),
            consumer_group=read("WEB_ENRICHER_CONSUMER_GROUP", "web-enricher"),
            consumer_name=read("WEB_ENRICHER_CONSUMER_NAME", "web-enricher-1"),
            batch_size=int(read("WEB_ENRICHER_BATCH_SIZE", "10")),
            block_ms=int(read("WEB_ENRICHER_BLOCK_MS", "5000")),
            request_timeout_sec=float(read("WEB_FETCH_TIMEOUT_SEC", "6")),
            max_redirects=int(read("WEB_FETCH_MAX_REDIRECTS", "4")),
            max_bytes=int(read("WEB_FETCH_MAX_BYTES", "262144")),
            excerpt_chars=int(read("WEB_FETCH_EXCERPT_CHARS", "1600")),
            max_outbound_links=int(read("WEB_FETCH_MAX_OUTBOUND_LINKS", "50")),
            user_agent=read("WEB_FETCH_USER_AGENT", "catchbot-web-enricher/0.1"),
            content_type_allowlist=allowlist,
            log_level=read("LOG_LEVEL", "INFO").upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.database_url:
            raise WebEnricherConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise WebEnricherConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise WebEnricherConfigurationError("WEB_ENRICHER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise WebEnricherConfigurationError("WEB_ENRICHER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise WebEnricherConfigurationError("WEB_ENRICHER_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise WebEnricherConfigurationError("WEB_ENRICHER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise WebEnricherConfigurationError("WEB_ENRICHER_BLOCK_MS must be > 0")
        if self.request_timeout_sec <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_TIMEOUT_SEC must be > 0")
        if self.max_redirects < 0 or self.max_redirects > 10:
            raise WebEnricherConfigurationError("WEB_FETCH_MAX_REDIRECTS must be between 0 and 10")
        if self.max_bytes <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_MAX_BYTES must be > 0")
        if self.excerpt_chars <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_EXCERPT_CHARS must be > 0")
        if self.max_outbound_links <= 0:
            raise WebEnricherConfigurationError("WEB_FETCH_MAX_OUTBOUND_LINKS must be > 0")
        if not self.user_agent:
            raise WebEnricherConfigurationError("WEB_FETCH_USER_AGENT must not be empty")
        if not self.content_type_allowlist:
            raise WebEnricherConfigurationError("WEB_FETCH_CONTENT_TYPE_ALLOWLIST must not be empty")
