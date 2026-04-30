from __future__ import annotations

from typing import Any

from .models import Verdict, VerdictDecision


GITHUB_PRIMARY_TYPES = {
    "github_repo",
    "github_subpath",
    "github_repo_page",
    "github_gist",
}

TEXT_LIKE_PRIMARY_TYPES = {
    "x_post",
    "web_article",
    "text_idea",
}


class VerdictPolicy:
    def evaluate(
        self,
        *,
        scores: dict[str, Any],
        current_primary_artifact_type: str | None,
    ) -> VerdictDecision:
        practical = _score(scores, "practical_usefulness")
        evidence = _score(scores, "evidence_strength")
        confidence = _score(scores, "confidence")
        hype = _score(scores, "hype_penalty")
        code_quality = _score(scores, "code_quality")
        specificity = _score(scores, "specificity")

        if (
            practical >= 70
            and evidence >= 50
            and confidence >= 60
            and hype < 70
            and self._primary_gate(
                artifact_type=current_primary_artifact_type,
                code_quality=code_quality,
                specificity=specificity,
            )
        ):
            return VerdictDecision(verdict="inspect_now", reason_codes=["policy_threshold_inspect_now"])

        if practical >= 45 and evidence >= 30 and confidence >= 35:
            return VerdictDecision(verdict="later", reason_codes=["policy_threshold_later"])

        return VerdictDecision(verdict="skip", reason_codes=["policy_threshold_skip"])

    @staticmethod
    def _primary_gate(
        *,
        artifact_type: str | None,
        code_quality: int,
        specificity: int,
    ) -> bool:
        if artifact_type in GITHUB_PRIMARY_TYPES:
            return code_quality >= 65
        if artifact_type in TEXT_LIKE_PRIMARY_TYPES:
            return specificity >= 60
        return False


def _score(scores: dict[str, Any], key: str) -> int:
    value = scores.get(key)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def reconcile_model_verdict(
    *,
    model_proposed_verdict: str | None,
    final_verdict: Verdict,
    reason_codes: list[str],
) -> tuple[bool, list[str]]:
    if not model_proposed_verdict or model_proposed_verdict == final_verdict:
        return True, reason_codes
    return False, [*reason_codes, "policy_overrode_model_verdict"]
