from __future__ import annotations

from services.policy_engine.verdict_policy import VerdictPolicy


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


def test_github_evidence_strength_below_50_blocks_inspect_now() -> None:
    decision = VerdictPolicy().evaluate(
        scores=_github_scores(evidence_strength=49),
        current_primary_artifact_type="github_repo",
    )

    assert decision.verdict == "later"
