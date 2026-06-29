from __future__ import annotations

import pytest

from services.policy_engine.verdict_policy import VerdictPolicy


def _text_scores(**overrides: int) -> dict[str, int]:
    scores = {
        "novelty": 70,
        "practical_usefulness": 74,
        "evidence_strength": 58,
        "hype_penalty": 30,
        "confidence": 72,
        "specificity": 65,
        "reproducibility_signal": 50,
    }
    scores.update(overrides)
    return scores


def _early_tool_like_scores(**overrides: int) -> dict[str, int]:
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


@pytest.mark.parametrize("artifact_type", ["x_post", "web_article", "text_idea"])
def test_text_like_primary_inspect_now_happy_path(artifact_type: str) -> None:
    decision = VerdictPolicy().evaluate(scores=_text_scores(), current_primary_artifact_type=artifact_type)

    assert decision.verdict == "inspect_now"


def test_specificity_below_60_blocks_inspect_now() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_text_scores(specificity=59),
        current_primary_artifact_type="x_post",
    )

    assert decision.verdict == "later"


def test_later_thresholds_are_stable() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_text_scores(practical_usefulness=45, evidence_strength=30, confidence=35, specificity=0),
        current_primary_artifact_type="x_post",
    )

    assert decision.verdict == "later"


def test_skip_thresholds_are_stable() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_text_scores(practical_usefulness=44, evidence_strength=30, confidence=35, specificity=65),
        current_primary_artifact_type="x_post",
    )

    assert decision.verdict == "skip"


@pytest.mark.parametrize("artifact_type", ["web_article", "text_idea"])
def test_non_github_text_like_early_tool_scores_stay_skip(artifact_type: str) -> None:
    decision = VerdictPolicy().evaluate(
        scores=_early_tool_like_scores(),
        current_primary_artifact_type=artifact_type,
    )

    assert decision.verdict == "skip"
    assert decision.reason_codes == ["policy_threshold_skip"]
