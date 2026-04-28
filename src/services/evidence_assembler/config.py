from __future__ import annotations

import os
from dataclasses import dataclass


class EvidenceAssemblerConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class EvidenceAssemblerConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    bundle_profile_version: str
    enable_text_idea: bool
    enable_reroot: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "EvidenceAssemblerConfig":
        def read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        config = cls(
            app_env=read("APP_ENV", "dev").lower(),
            database_url=read("DATABASE_URL"),
            redis_url=read("REDIS_URL"),
            queue_name=read("EVIDENCE_ASSEMBLER_QUEUE_NAME", "q.candidate.bundle"),
            consumer_group=read("EVIDENCE_ASSEMBLER_CONSUMER_GROUP", "evidence-assembler"),
            consumer_name=read("EVIDENCE_ASSEMBLER_CONSUMER_NAME", "evidence-assembler-1"),
            batch_size=int(read("EVIDENCE_ASSEMBLER_BATCH_SIZE", "10")),
            block_ms=int(read("EVIDENCE_ASSEMBLER_BLOCK_MS", "5000")),
            bundle_profile_version=read("EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION", "bundle_profile_v1"),
            enable_text_idea=read("EVIDENCE_ASSEMBLER_ENABLE_TEXT_IDEA", "true").lower()
            not in {"0", "false", "no"},
            enable_reroot=read("EVIDENCE_ASSEMBLER_ENABLE_REROOT", "true").lower() not in {"0", "false", "no"},
            log_level=read("LOG_LEVEL", "INFO").upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.database_url:
            raise EvidenceAssemblerConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise EvidenceAssemblerConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_BLOCK_MS must be > 0")
        if not self.bundle_profile_version:
            raise EvidenceAssemblerConfigurationError("EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION must not be empty")
