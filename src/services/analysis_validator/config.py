from __future__ import annotations

import os
from dataclasses import dataclass


class AnalysisValidatorConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class AnalysisValidatorConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    max_headline_chars: int
    max_summary_chars: int
    max_text_items: int
    log_level: str

    @classmethod
    def from_env(cls) -> "AnalysisValidatorConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        try:
            cfg = cls(
                app_env=_read("APP_ENV", "dev").lower(),
                database_url=_read("DATABASE_URL"),
                redis_url=_read("REDIS_URL"),
                queue_name=_read("ANALYSIS_VALIDATOR_QUEUE_NAME", "q.analysis.validate"),
                consumer_group=_read("ANALYSIS_VALIDATOR_CONSUMER_GROUP", "analysis-validator"),
                consumer_name=_read("ANALYSIS_VALIDATOR_CONSUMER_NAME", "analysis-validator-1"),
                batch_size=int(_read("ANALYSIS_VALIDATOR_BATCH_SIZE", "20")),
                block_ms=int(_read("ANALYSIS_VALIDATOR_BLOCK_MS", "5000")),
                max_headline_chars=int(_read("ANALYSIS_VALIDATOR_MAX_HEADLINE_CHARS", "200")),
                max_summary_chars=int(_read("ANALYSIS_VALIDATOR_MAX_SUMMARY_CHARS", "1200")),
                max_text_items=int(_read("ANALYSIS_VALIDATOR_MAX_TEXT_ITEMS", "10")),
                log_level=_read("LOG_LEVEL", "INFO").upper(),
            )
        except ValueError as exc:
            raise AnalysisValidatorConfigurationError(str(exc)) from exc
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise AnalysisValidatorConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise AnalysisValidatorConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_CONSUMER_NAME must not be empty")
        if self.batch_size < 1 or self.batch_size > 100:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_BLOCK_MS must be > 0")
        if self.max_headline_chars <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_MAX_HEADLINE_CHARS must be > 0")
        if self.max_summary_chars <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_MAX_SUMMARY_CHARS must be > 0")
        if self.max_text_items <= 0:
            raise AnalysisValidatorConfigurationError("ANALYSIS_VALIDATOR_MAX_TEXT_ITEMS must be > 0")
