from __future__ import annotations

import pytest

from services.judge_openai.prompt_library import PromptLibrary, UnsupportedJudgeProfileError


@pytest.mark.parametrize("profile", ["github_primary", "x_primary", "text_idea_primary"])
def test_prompt_library_renders_supported_profiles(profile: str) -> None:
    prompt = PromptLibrary().render(judge_profile=profile, prompt_version="judge_output_v1")

    assert profile in prompt
    assert "Use only the provided CandidateEvidenceBundle context" in prompt
    assert "Do not browse, search, fetch, call tools" in prompt
    assert "Evaluate negative-first" in prompt
    assert "Do not invent comparables" in prompt
    assert "Scores must be integers on a 0-100 scale, not a 0-10 scale" in prompt
    assert "use 70 for strong, not 7; use 45 for moderate, not 4.5/5" in prompt
    assert "Do not decide final verdict or delivery_decision" in prompt


def test_github_primary_prompt_allows_comparison_gap_without_fabrication() -> None:
    prompt = PromptLibrary().render(
        judge_profile="github_primary",
        prompt_version="judge_github_primary_v1",
    )

    assert "Only include comparables when supported by the provided CandidateEvidenceBundle" in prompt
    assert "Do not use latent/general knowledge to fill comparables" in prompt
    assert "Do not invent comparables" in prompt
    assert "leave comparables=[]" in prompt
    assert "comparison_gap or insufficient_comparables" in prompt


def test_github_primary_prompt_no_longer_hard_requires_comparables_for_later_or_inspect() -> None:
    prompt = PromptLibrary().render(
        judge_profile="github_primary",
        prompt_version="judge_github_primary_v1",
    )

    assert "later or inspect_now requires" not in prompt
    assert "choose model_proposed_verdict=skip" not in prompt
    assert "comparables strengthen evidence but are not mandatory" in prompt
    assert "primary bundle evidence is strong and specific" in prompt


def test_github_primary_prompt_treats_missing_comparables_as_score_penalty_not_veto() -> None:
    prompt = PromptLibrary().render(
        judge_profile="github_primary",
        prompt_version="judge_github_primary_v1",
    )

    assert "no reliable comparables" in prompt
    assert "do not force skip solely for that reason" in prompt
    assert "lowering evidence_strength and/or confidence" in prompt
    assert "evidence_limitations_ko" in prompt


@pytest.mark.parametrize("profile", ["idea_primary", "web_primary", "unknown_primary"])
def test_prompt_library_rejects_unsupported_profiles(profile: str) -> None:
    with pytest.raises(UnsupportedJudgeProfileError):
        PromptLibrary().render(judge_profile=profile, prompt_version="judge_output_v1")
