from __future__ import annotations

import pytest

from services.judge_openai.prompt_library import PromptLibrary, UnsupportedJudgeProfileError


@pytest.mark.parametrize("profile", ["github_primary", "x_primary", "text_idea_primary"])
def test_prompt_library_renders_supported_profiles(profile: str) -> None:
    prompt = PromptLibrary().render(judge_profile=profile, prompt_version="judge_output_v1")

    assert profile in prompt
    assert "CandidateEvidenceBundle" in prompt
    assert "Do not browse" in prompt
    assert "Do not compute the final verdict" in prompt


def test_prompt_library_rejects_unknown_profile() -> None:
    with pytest.raises(UnsupportedJudgeProfileError):
        PromptLibrary().render(judge_profile="web_primary", prompt_version="judge_output_v1")
