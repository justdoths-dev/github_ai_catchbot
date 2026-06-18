from __future__ import annotations


class UnsupportedJudgeProfileError(ValueError):
    pass


class UnsupportedPromptVersionError(ValueError):
    pass


class PromptLibrary:
    _SUPPORTED = {"github_primary", "x_primary", "text_idea_primary"}
    _SUPPORTED_PROMPT_VERSIONS = {
        "github_primary": frozenset(
            {
                "judge_github_primary_v1",
                "judge_output_v1",
                "judge_prompt_v1",
            }
        ),
        "x_primary": frozenset(
            {
                "judge_x_primary_v1",
                "judge_output_v1",
                "judge_prompt_v1",
            }
        ),
        "text_idea_primary": frozenset(
            {
                "judge_text_idea_primary_v1",
                "judge_output_v1",
                "judge_prompt_v1",
            }
        ),
    }

    def render(self, *, judge_profile: str, prompt_version: str) -> str:
        if judge_profile not in self._SUPPORTED:
            raise UnsupportedJudgeProfileError(f"unsupported judge_profile: {judge_profile}")
        if prompt_version not in self._SUPPORTED_PROMPT_VERSIONS[judge_profile]:
            raise UnsupportedPromptVersionError(f"unsupported prompt_version: {prompt_version}")

        profile_guidance = {
            "github_primary": (
                "Profile: github_primary. Evaluate GitHub-primary evidence. Focus on code quality "
                "signals, maintenance signals, wrapper risk, adoption quality, and bundle-supported comparables."
            ),
            "x_primary": (
                "Profile: x_primary. Evaluate X-post-primary evidence. Focus on specificity, "
                "reproducibility, hype risk, and whether linked artifacts carry the actual value."
            ),
            "text_idea_primary": (
                "Profile: text_idea_primary. Evaluate a text-idea-primary candidate. Focus on "
                "procedural specificity, execution realism, anti-hype skepticism, and commonness."
            ),
        }[judge_profile]

        return "\n\n".join(
            [
                self._common_prefix(),
                f"Prompt version: {prompt_version}",
                profile_guidance,
            ]
        )

    @staticmethod
    def _common_prefix() -> str:
        return "\n".join(
            [
                "You are the stage-6 OpenAI judge for a precision-first GitHub/X catch-bot.",
                "Return only strict judge_output_v1 JSON matching the supplied schema.",
                "Use only the provided CandidateEvidenceBundle context.",
                "Do not browse, search, fetch, call tools, or assume facts outside the bundle.",
                "Evaluate negative-first: identify why the candidate may not be worth attention before upside.",
                "Do not invent comparables, repo activity, social proof, dates, or evidence.",
                "Only include comparables when supported by the provided CandidateEvidenceBundle.",
                "Do not use latent/general knowledge to fill comparables.",
                "Do not use latent/general knowledge to fill comparables.",
                "If no reliable comparables are available, set comparables=[] and include comparison_gap or insufficient_comparables in reason_codes or evidence_limitations_ko.",
                "If evidence is weak, reflect that in scores, red flags, limitations, and confidence.",
                "Do not decide final verdict or delivery_decision; downstream deterministic services do that later.",
            ]
        )
