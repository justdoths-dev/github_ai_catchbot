from __future__ import annotations

from uuid import uuid4

from services.analysis_router.config import AnalysisRouterConfig
from services.analysis_router.models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats
from services.analysis_router.routing_policy import AnalysisRoutingPolicy


def _config(*, escalation: bool = False) -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        enable_model_escalation=escalation,
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


def _job(
    *,
    bundle_id=None,
    profile: str | None = "github_primary",
    escalation_allowed: bool = True,
) -> AnalysisRequestedJob:
    return AnalysisRequestedJob(
        trigger_event_id=uuid4(),
        event_type="analysis.requested.v1",
        candidate_group_id=uuid4(),
        bundle_id=bundle_id or uuid4(),
        judge_profile=profile,
        escalation_allowed=escalation_allowed,
    )


def _bundle(*, bundle_id, ready: bool = True, reroot_count: int = 0, token_budget_profile: str = "small"):
    return BundleRouteRecord(
        bundle_id=bundle_id,
        candidate_group_id=uuid4(),
        bundle_profile_version="bundle_profile_v1",
        reroot_count=reroot_count,
        ready_for_analysis=ready,
        token_budget_profile=token_budget_profile,
    )


def _decide(
    *,
    escalation: bool = False,
    profile: str | None = "github_primary",
    ready: bool = True,
    reroot_count: int = 0,
    supporting_count: int = 0,
    token_budget_profile: str = "small",
    escalation_allowed: bool = True,
):
    job = _job(profile=profile, escalation_allowed=escalation_allowed)
    return AnalysisRoutingPolicy(_config(escalation=escalation)).decide(
        job=job,
        current_bundle_id=job.bundle_id,
        bundle=_bundle(
            bundle_id=job.bundle_id,
            ready=ready,
            reroot_count=reroot_count,
            token_budget_profile=token_budget_profile,
        ),
        shape=BundleShapeStats(member_count=1 + supporting_count, supporting_count=supporting_count),
    )


def test_default_routing_policy_chooses_mini_low() -> None:
    decision = _decide()

    assert decision.action == "judge"
    assert decision.model == "gpt-5.4-mini"
    assert decision.reasoning_effort == "low"


def test_escalation_disabled_forces_default_route() -> None:
    decision = _decide(escalation=False, reroot_count=1, supporting_count=3, token_budget_profile="xlarge")

    assert decision.action == "judge"
    assert decision.model == "gpt-5.4-mini"
    assert decision.reasoning_effort == "low"


def test_escalation_enabled_and_reroot_count_chooses_escalation_route() -> None:
    decision = _decide(escalation=True, reroot_count=1)

    assert decision.model == "gpt-5.4"
    assert decision.reasoning_effort == "medium"


def test_escalation_enabled_and_supporting_count_chooses_escalation_route() -> None:
    decision = _decide(escalation=True, supporting_count=3)

    assert decision.model == "gpt-5.4"
    assert decision.reasoning_effort == "medium"


def test_escalation_enabled_and_large_token_budget_chooses_escalation_route() -> None:
    large = _decide(escalation=True, token_budget_profile="large")
    xlarge = _decide(escalation=True, token_budget_profile="xlarge")

    assert large.model == "gpt-5.4"
    assert large.reasoning_effort == "medium"
    assert xlarge.model == "gpt-5.4"
    assert xlarge.reasoning_effort == "medium"


def test_evidence_insufficiency_routes_to_refresh_not_escalation() -> None:
    decision = _decide(escalation=True, ready=False, reroot_count=1)

    assert decision.action == "refresh"
    assert decision.refresh_reason == "bundle_not_ready"
    assert decision.model is None


def test_prompt_cache_key_format() -> None:
    decision = _decide(profile="github_primary")

    assert (
        decision.prompt_cache_key
        == "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
    )


def test_unknown_profile_rejected_noop() -> None:
    assert _decide(profile="unknown").action == "noop"


def test_idea_primary_rejected_noop() -> None:
    assert _decide(profile="idea_primary").action == "noop"


def test_web_primary_rejected_noop() -> None:
    assert _decide(profile="web_primary").action == "noop"
