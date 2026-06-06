from __future__ import annotations

from uuid import uuid4

import pytest

from services.analysis_router.config import AnalysisRouterConfig
from services.analysis_router.models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats
from services.analysis_router.routing_policy import AnalysisRoutingPolicy


def _config() -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        enable_model_escalation=False,
        default_model="gpt-5.4-mini",
        escalation_model="gpt-5.4",
        default_reasoning_effort="low",
        escalation_reasoning_effort="medium",
        github_prompt_version="judge_github_primary_v1",
        x_prompt_version="judge_x_primary_v1",
        text_idea_prompt_version="judge_text_idea_primary_v1",
        judge_schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        log_level="INFO",
    )


def _job(*, bundle_id, judge_profile: str | None) -> AnalysisRequestedJob:
    return AnalysisRequestedJob(
        trigger_event_id=uuid4(),
        event_type="analysis.requested.v1",
        candidate_group_id=uuid4(),
        bundle_id=bundle_id,
        judge_profile=judge_profile,
        escalation_allowed=True,
    )


def _bundle(*, bundle_id, ready_for_analysis: bool = True) -> BundleRouteRecord:
    return BundleRouteRecord(
        bundle_id=bundle_id,
        candidate_group_id=uuid4(),
        bundle_profile_version="bundle_profile_v1",
        reroot_count=0,
        ready_for_analysis=ready_for_analysis,
        token_budget_profile="small",
    )


def _decide(*, judge_profile: str | None, ready_for_analysis: bool = True):
    bundle_id = uuid4()
    job = _job(bundle_id=bundle_id, judge_profile=judge_profile)
    return AnalysisRoutingPolicy(_config()).decide(
        job=job,
        current_bundle_id=bundle_id,
        bundle=_bundle(bundle_id=bundle_id, ready_for_analysis=ready_for_analysis),
        shape=BundleShapeStats(member_count=1, supporting_count=0),
    )


@pytest.mark.parametrize(
    ("judge_profile", "prompt_version"),
    [
        ("github_primary", "judge_github_primary_v1"),
        ("x_primary", "judge_x_primary_v1"),
        ("text_idea_primary", "judge_text_idea_primary_v1"),
    ],
)
def test_valid_profile_ready_bundle_produces_judge_route(judge_profile: str, prompt_version: str) -> None:
    decision = _decide(judge_profile=judge_profile)

    assert decision.action == "judge"
    assert decision.judge_profile == judge_profile
    assert decision.model == "gpt-5.4-mini"
    assert decision.reasoning_effort == "low"
    assert decision.prompt_version == prompt_version
    assert decision.schema_version == "judge_output_v1"
    assert decision.policy_version == "verdict_policy_v1"


def test_invalid_judge_profile_rejects_route() -> None:
    decision = _decide(judge_profile="web_primary")

    assert decision.action == "noop"
    assert decision.model is None
    assert decision.prompt_cache_key is None


def test_not_ready_bundle_rejects_route() -> None:
    decision = _decide(judge_profile="github_primary", ready_for_analysis=False)

    assert decision.action == "refresh"
    assert decision.refresh_reason == "bundle_not_ready"
    assert decision.model is None


def test_prompt_cache_key_is_deterministic_for_profile_and_versions() -> None:
    first = _decide(judge_profile="github_primary")
    second = _decide(judge_profile="github_primary")

    assert first.prompt_cache_key == second.prompt_cache_key
    assert (
        first.prompt_cache_key
        == "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
    )
