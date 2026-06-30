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
        reproducibility = _score(scores, "reproducibility_signal")
        maintenance = _score(scores, "maintenance_signal")

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

        if self._early_github_tool_gate(
            artifact_type=current_primary_artifact_type,
            practical=practical,
            evidence=evidence,
            confidence=confidence,
            hype=hype,
            code_quality=code_quality,
            specificity=specificity,
            reproducibility=reproducibility,
            maintenance=maintenance,
        ):
            return VerdictDecision(verdict="later", reason_codes=["policy_threshold_early_github_tool_later"])

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

    @staticmethod
    def _early_github_tool_gate(
        *,
        artifact_type: str | None,
        practical: int,
        evidence: int,
        confidence: int,
        hype: int,
        code_quality: int,
        specificity: int,
        reproducibility: int,
        maintenance: int,
    ) -> bool:
        if artifact_type not in GITHUB_PRIMARY_TYPES:
            return False
        if hype >= 70 or practical < 35 or evidence < 15 or confidence < 20:
            return False
        concrete_signal_count = sum(
            [
                code_quality >= 35,
                specificity >= 45,
                reproducibility >= 35,
                maintenance >= 20,
            ]
        )
        return concrete_signal_count >= 2


def normalize_scores_for_policy(
    scores: dict[str, Any],
    *,
    model_proposed_verdict: str | None,
) -> tuple[dict[str, Any], bool]:
    normalized = dict(scores)
    if model_proposed_verdict not in {"later", "inspect_now"}:
        return normalized, False

    numeric_keys: list[str] = []
    for key, value in scores.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            numeric_keys.append(key)

    if not numeric_keys:
        return normalized, False
    if any(not 0 <= scores[key] <= 10 for key in numeric_keys):
        return normalized, False

    for key in numeric_keys:
        normalized[key] = scores[key] * 10
    return normalized, True


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
    if not model_proposed_verdict:
        return True, reason_codes
    if model_proposed_verdict == final_verdict:
        return True, reason_codes
    return False, [*reason_codes, "policy_overrode_model_verdict"]
