from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.policy_engine.config import PolicyEngineConfig
from services.policy_engine.models import (
    AnalysisPolicyJob,
    BundlePolicyContext,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
)
from services.policy_engine.service import PolicyEngineService
from services.policy_engine.verdict_policy import GITHUB_PRIMARY_TYPES, VerdictPolicy, normalize_scores_for_policy


def _github_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 70,
        "practical_usefulness": 75,
        "evidence_strength": 60,
        "hype_penalty": 20,
        "confidence": 70,
        "code_quality": 70,
        "maintenance_signal": 60,
    }
    scores.update(overrides)
    return scores


def _early_tool_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 45,
        "practical_usefulness": 35,
        "evidence_strength": 15,
        "hype_penalty": 20,
        "confidence": 20,
        "code_quality": 35,
        "specificity": 45,
        "reproducibility_signal": 10,
        "maintenance_signal": 10,
    }
    scores.update(overrides)
    return scores


def _live_like_0_to_10_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 5,
        "practical_usefulness": 7,
        "evidence_strength": 7,
        "hype_penalty": 2,
        "confidence": 7,
        "code_quality": 7,
        "maintenance_signal": 7,
        "specificity": 8,
        "reproducibility_signal": 6,
    }
    scores.update(overrides)
    return scores


def test_github_primary_inspect_now_happy_path() -> None:
    decision = VerdictPolicy().evaluate(scores=_github_scores(), current_primary_artifact_type="github_repo")

    assert decision.verdict == "inspect_now"
    assert decision.reason_codes == ["policy_threshold_inspect_now"]


def test_github_code_quality_below_65_blocks_inspect_now() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_github_scores(code_quality=64),
        current_primary_artifact_type="github_repo",
    )

    assert decision.verdict == "later"
    assert decision.reason_codes == ["policy_threshold_later"]


def test_github_evidence_strength_below_50_blocks_inspect_now() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_github_scores(evidence_strength=49),
        current_primary_artifact_type="github_repo",
    )

    assert decision.verdict == "later"
    assert decision.reason_codes == ["policy_threshold_later"]


@pytest.mark.parametrize("artifact_type", sorted(GITHUB_PRIMARY_TYPES))
def test_github_primary_early_concrete_tool_maps_to_later(artifact_type: str) -> None:
    decision = VerdictPolicy().evaluate(
        scores=_early_tool_scores(),
        current_primary_artifact_type=artifact_type,
    )

    assert decision.verdict == "later"
    assert decision.reason_codes == ["policy_threshold_early_github_tool_later"]


def test_github_primary_early_tool_high_hype_stays_skip() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_early_tool_scores(hype_penalty=70),
        current_primary_artifact_type="github_repo",
    )

    assert decision.verdict == "skip"
    assert decision.reason_codes == ["policy_threshold_skip"]


def test_github_primary_early_tool_low_practical_usefulness_stays_skip() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_early_tool_scores(practical_usefulness=34),
        current_primary_artifact_type="github_repo",
    )

    assert decision.verdict == "skip"
    assert decision.reason_codes == ["policy_threshold_skip"]


def test_github_primary_early_tool_requires_two_concrete_signals() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_early_tool_scores(specificity=44, reproducibility_signal=34, maintenance_signal=19),
        current_primary_artifact_type="github_repo",
    )

    assert decision.verdict == "skip"
    assert decision.reason_codes == ["policy_threshold_skip"]


def test_live_like_0_to_10_scores_normalize_for_later_model_verdict_and_do_not_skip() -> None:
    normalized, changed = normalize_scores_for_policy(
        _live_like_0_to_10_scores(),
        model_proposed_verdict="later",
    )

    assert changed is True
    assert normalized["practical_usefulness"] == 70
    assert normalized["evidence_strength"] == 70
    assert normalized["confidence"] == 70
    assert normalized["code_quality"] == 70
    decision = VerdictPolicy().evaluate(scores=normalized, current_primary_artifact_type="github_repo")
    assert decision.verdict in {"later", "inspect_now"}


@pytest.mark.parametrize("model_proposed_verdict", ["skip", None])
def test_skip_or_missing_model_verdict_does_not_normalize(model_proposed_verdict: str | None) -> None:
    scores = _live_like_0_to_10_scores()

    normalized, changed = normalize_scores_for_policy(
        scores,
        model_proposed_verdict=model_proposed_verdict,
    )

    assert changed is False
    assert normalized == scores
    assert normalized is not scores


def test_mixed_scale_with_score_above_10_does_not_normalize() -> None:
    scores = _live_like_0_to_10_scores(confidence=70)

    normalized, changed = normalize_scores_for_policy(
        scores,
        model_proposed_verdict="later",
    )

    assert changed is False
    assert normalized == scores
    assert normalized["practical_usefulness"] == 7
    assert normalized["confidence"] == 70


def test_policy_engine_service_persists_normalized_scores_before_threshold_reasons() -> None:
    candidate_group_id = uuid4()
    judge_run_id = uuid4()
    judge_output_id = uuid4()
    bundle_id = uuid4()
    service = PolicyEngineService(_policy_config(), repository=object())

    analysis, evaluation = service._build_analysis(
        job=AnalysisPolicyJob(
            trigger_event_id=uuid4(),
            event_type="analysis.policy.apply.v1",
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
        ),
        judge_run=JudgeRunPolicyContext(
            judge_run_id=judge_run_id,
            bundle_id=bundle_id,
            prompt_version="judge_github_primary_v1",
            policy_version="verdict_policy_v1",
            status="succeeded",
        ),
        judge_output=JudgeOutputPolicyContext(
            judge_output_id=judge_output_id,
            judge_run_id=judge_run_id,
            candidate_group_id=candidate_group_id,
            payload_json={
                "scores": _live_like_0_to_10_scores(),
                "reason_codes": ["judge_output_validated"],
            },
            model_proposed_verdict="later",
            model_confidence_band="high",
            created_at=datetime.now(timezone.utc),
            judge_schema_version="judge_output_v1",
        ),
        bundle=BundlePolicyContext(
            bundle_id=bundle_id,
            candidate_group_id=candidate_group_id,
            current_primary_artifact_id=uuid4(),
            current_primary_artifact_type="github_repo",
            created_at=datetime.now(timezone.utc),
        ),
    )

    assert analysis.scores_json["practical_usefulness"] == 70
    assert analysis.scores_json["specificity"] == 80
    assert analysis.verdict in {"later", "inspect_now"}
    assert evaluation.verdict == analysis.verdict
    assert analysis.reason_codes_json[:3] == [
        "judge_output_validated",
        "policy_score_scale_normalized_0_10_to_0_100",
        "policy_threshold_inspect_now",
    ]
    assert analysis.reason_codes_json[-1] == "policy_overrode_model_verdict"


def _policy_config() -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url="unused",
        redis_url="unused",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=12345,
        enable_later_delivery=True,
        enable_silent_later=True,
        enable_notification_send=True,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )
