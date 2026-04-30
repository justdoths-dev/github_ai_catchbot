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
