from __future__ import annotations

from typing import cast

from .config import AnalysisRouterConfig
from .models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, JudgeProfile, JudgeRouteDecision


ALLOWED_JUDGE_PROFILES: frozenset[str] = frozenset(
    {
        "github_primary",
        "x_primary",
        "text_idea_primary",
    }
)


class AnalysisRoutingPolicy:
    def __init__(self, config: AnalysisRouterConfig) -> None:
        self._config = config

    def decide(
        self,
        *,
        job: AnalysisRequestedJob,
        current_bundle_id,
        bundle: BundleRouteRecord | None,
        shape: BundleShapeStats | None,
    ) -> JudgeRouteDecision:
        if current_bundle_id is None or current_bundle_id != job.bundle_id:
            return JudgeRouteDecision(action="noop")

        if bundle is None:
            return JudgeRouteDecision(action="refresh", refresh_reason="bundle_missing")

        if not bundle.ready_for_analysis:
            return JudgeRouteDecision(action="refresh", refresh_reason="bundle_not_ready")

        profile = (job.judge_profile or "").strip()
        if not profile:
            return JudgeRouteDecision(action="refresh", refresh_reason="bundle_profile_missing")

        if shape is None or shape.member_count <= 0:
            return JudgeRouteDecision(action="refresh", refresh_reason="bundle_members_missing")

        if profile not in ALLOWED_JUDGE_PROFILES:
            return JudgeRouteDecision(action="noop")

        prompt_version = self._prompt_version_for_profile(profile)
        schema_version = self._config.judge_schema_version
        policy_version = self._config.policy_version
        prompt_cache_key = f"judge:{profile}:{prompt_version}:{schema_version}:{policy_version}"
        use_escalation = self._use_escalation(job=job, bundle=bundle, shape=shape)

        return JudgeRouteDecision(
            action="judge",
            judge_profile=cast(JudgeProfile, profile),
            model=self._config.escalation_model if use_escalation else self._config.default_model,
            reasoning_effort=(
                self._config.escalation_reasoning_effort
                if use_escalation
                else self._config.default_reasoning_effort
            ),
            prompt_version=prompt_version,
            schema_version=schema_version,
            policy_version=policy_version,
            prompt_cache_key=prompt_cache_key,
        )

    def _use_escalation(
        self,
        *,
        job: AnalysisRequestedJob,
        bundle: BundleRouteRecord,
        shape: BundleShapeStats,
    ) -> bool:
        if not self._config.enable_model_escalation or not job.escalation_allowed:
            return False
        return (
            bundle.reroot_count > 0
            or shape.supporting_count >= 3
            or bundle.token_budget_profile in {"large", "xlarge"}
        )

    def _prompt_version_for_profile(self, profile: str) -> str:
        if profile == "github_primary":
            return self._config.github_prompt_version
        if profile == "x_primary":
            return self._config.x_prompt_version
        return self._config.text_idea_prompt_version
