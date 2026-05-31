from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


class JudgeOpenAIConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class JudgeOpenAIConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    openai_api_key: str = field(repr=False)
    openai_project: str | None
    request_timeout_sec: float
    max_output_tokens: int | None
    enable_prompt_guard_preflight: bool
    log_level: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "JudgeOpenAIConfig":
        source = os.environ if env is None else env

        def read(name: str, default: str = "") -> str:
            return str(source.get(name, default)).strip()

        if read("OPENAI_API_KEY"):
            raise JudgeOpenAIConfigurationError(
                "direct OPENAI_API_KEY is not supported; use OPENAI_API_KEY_FILE"
            )
        api_key_file = read("OPENAI_API_KEY_FILE")
        api_key = _read_secret_file(api_key_file)

        max_output_tokens_raw = read("JUDGE_MAX_OUTPUT_TOKENS")
        config = cls(
            app_env=read("APP_ENV", "dev").lower(),
            database_url=read("DATABASE_URL"),
            redis_url=read("REDIS_URL"),
            queue_name=read("JUDGE_OPENAI_QUEUE_NAME", "q.analysis.judge"),
            consumer_group=read("JUDGE_OPENAI_CONSUMER_GROUP", "judge-openai"),
            consumer_name=read("JUDGE_OPENAI_CONSUMER_NAME", "judge-openai-1"),
            batch_size=_parse_positive_int(
                "JUDGE_OPENAI_BATCH_SIZE",
                read("JUDGE_OPENAI_BATCH_SIZE", "10"),
            ),
            block_ms=_parse_positive_int(
                "JUDGE_OPENAI_BLOCK_MS",
                read("JUDGE_OPENAI_BLOCK_MS", "5000"),
            ),
            openai_api_key=api_key,
            openai_project=read("OPENAI_PROJECT") or None,
            request_timeout_sec=_parse_positive_float(
                "JUDGE_OPENAI_REQUEST_TIMEOUT_SEC",
                read("JUDGE_OPENAI_REQUEST_TIMEOUT_SEC", "60"),
            ),
            max_output_tokens=(
                _parse_positive_int("JUDGE_MAX_OUTPUT_TOKENS", max_output_tokens_raw)
                if max_output_tokens_raw
                else None
            ),
            enable_prompt_guard_preflight=_parse_bool(read("ENABLE_PROMPT_GUARD_PREFLIGHT", "false")),
            log_level=read("LOG_LEVEL", "INFO").upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.database_url:
            raise JudgeOpenAIConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise JudgeOpenAIConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_BLOCK_MS must be > 0")
        if not self.openai_api_key:
            raise JudgeOpenAIConfigurationError("OPENAI_API_KEY_FILE is required")
        if self.request_timeout_sec <= 0:
            raise JudgeOpenAIConfigurationError("JUDGE_OPENAI_REQUEST_TIMEOUT_SEC must be > 0")
        if self.max_output_tokens is not None and self.max_output_tokens <= 0:
            raise JudgeOpenAIConfigurationError("JUDGE_MAX_OUTPUT_TOKENS must be > 0 when set")


def _read_secret_file(raw_path: str) -> str:
    if not raw_path:
        raise JudgeOpenAIConfigurationError("OPENAI_API_KEY_FILE is required")

    path = Path(raw_path)
    try:
        if not path.is_file():
            raise JudgeOpenAIConfigurationError("OPENAI_API_KEY_FILE must reference a regular file")
        value = path.read_text(encoding="utf-8").strip()
    except JudgeOpenAIConfigurationError:
        raise
    except OSError:
        raise JudgeOpenAIConfigurationError("OPENAI_API_KEY_FILE could not be read") from None

    if not value:
        raise JudgeOpenAIConfigurationError("OPENAI_API_KEY_FILE is empty")
    return value


def _parse_positive_int(name: str, raw_value: str) -> int:
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        raise JudgeOpenAIConfigurationError(f"{name} must be an integer") from None


def _parse_positive_float(name: str, raw_value: str) -> float:
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        raise JudgeOpenAIConfigurationError(f"{name} must be a number") from None


def _parse_bool(raw_value: str) -> bool:
    return raw_value.lower() not in {"", "0", "false", "no"}
