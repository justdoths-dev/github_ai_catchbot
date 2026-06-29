from __future__ import annotations

import pytest

from services.policy_engine.verdict_policy import GITHUB_PRIMARY_TYPES, VerdictPolicy


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
