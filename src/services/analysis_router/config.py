from __future__ import annotations

import os
from dataclasses import dataclass


class AnalysisRouterConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class AnalysisRouterConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    enable_model_escalation: bool
    default_model: str
    escalation_model: str
    default_reasoning_effort: str
    escalation_reasoning_effort: str
    github_prompt_version: str
    x_prompt_version: str
    text_idea_prompt_version: str
    judge_schema_version: str
    policy_version: str
    log_level: str

    @classmethod
    def from_env(cls) -> "AnalysisRouterConfig":
        def read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        config = cls(
            app_env=read("APP_ENV", "dev").lower(),
            database_url=read("DATABASE_URL"),
            redis_url=read("REDIS_URL"),
            queue_name=read("ANALYSIS_ROUTER_QUEUE_NAME", "q.analysis.route"),
            consumer_group=read("ANALYSIS_ROUTER_CONSUMER_GROUP", "analysis-router"),
            consumer_name=read("ANALYSIS_ROUTER_CONSUMER_NAME", "analysis-router-1"),
            batch_size=int(read("ANALYSIS_ROUTER_BATCH_SIZE", "20")),
            block_ms=int(read("ANALYSIS_ROUTER_BLOCK_MS", "5000")),
            enable_model_escalation=read("ENABLE_MODEL_ESCALATION", "false").lower()
            not in {"0", "false", "no"},
            default_model=read("JUDGE_DEFAULT_MODEL", "gpt-5.4-mini"),
            escalation_model=read("JUDGE_ESCALATION_MODEL", "gpt-5.4"),
            default_reasoning_effort=read("JUDGE_REASONING_EFFORT_DEFAULT", "low"),
            escalation_reasoning_effort=read("JUDGE_REASONING_EFFORT_ESCALATION", "medium"),
            github_prompt_version=read("JUDGE_PROMPT_VERSION_GITHUB", "judge_github_primary_v1"),
            x_prompt_version=read("JUDGE_PROMPT_VERSION_X", "judge_x_primary_v1"),
            text_idea_prompt_version=read(
                "JUDGE_PROMPT_VERSION_TEXT_IDEA",
                "judge_text_idea_primary_v1",
            ),
            judge_schema_version=read("JUDGE_SCHEMA_VERSION", "judge_output_v1"),
            policy_version=read("VERDICT_POLICY_VERSION", "verdict_policy_v1"),
            log_level=read("LOG_LEVEL", "INFO").upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.database_url:
            raise AnalysisRouterConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise AnalysisRouterConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_CONSUMER_NAME must not be empty")
        if self.batch_size <= 0 or self.batch_size > 100:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise AnalysisRouterConfigurationError("ANALYSIS_ROUTER_BLOCK_MS must be > 0")
        if not self.default_model:
            raise AnalysisRouterConfigurationError("JUDGE_DEFAULT_MODEL must not be empty")
        if not self.escalation_model:
            raise AnalysisRouterConfigurationError("JUDGE_ESCALATION_MODEL must not be empty")
        if not self.default_reasoning_effort:
            raise AnalysisRouterConfigurationError("JUDGE_REASONING_EFFORT_DEFAULT must not be empty")
        if not self.escalation_reasoning_effort:
            raise AnalysisRouterConfigurationError("JUDGE_REASONING_EFFORT_ESCALATION must not be empty")
        if not self.github_prompt_version:
            raise AnalysisRouterConfigurationError("JUDGE_PROMPT_VERSION_GITHUB must not be empty")
        if not self.x_prompt_version:
            raise AnalysisRouterConfigurationError("JUDGE_PROMPT_VERSION_X must not be empty")
        if not self.text_idea_prompt_version:
            raise AnalysisRouterConfigurationError("JUDGE_PROMPT_VERSION_TEXT_IDEA must not be empty")
        if not self.judge_schema_version:
            raise AnalysisRouterConfigurationError("JUDGE_SCHEMA_VERSION must not be empty")
        if not self.policy_version:
            raise AnalysisRouterConfigurationError("VERDICT_POLICY_VERSION must not be empty")
