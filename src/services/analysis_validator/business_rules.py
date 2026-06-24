from __future__ import annotations

from numbers import Real
from typing import Any

from .models import BundleValidationContext, ValidationDecision


_GITHUB_PRIMARY_TYPES = {"github_repo", "github_subpath", "github_repo_page", "github_gist"}
_TRUNCATION_FINISH_REASONS = {"incomplete", "max_output_tokens", "output_truncated", "truncated"}


class AnalysisValidatorBusinessRules:
    def evaluate_control_flow(
        self,
        *,
        payload: dict[str, Any],
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> ValidationDecision:
        if refusal_detected or payload.get("output_kind") == "refusal":
            return ValidationDecision(
                action="refused",
                reason_code="model_refusal",
                transition_to_state="analysis_refused",
            )
        if self._is_truncated(finish_reason):
            return ValidationDecision(
                action="failed_retryable",
                reason_code="analysis_failed_truncation",
                transition_to_state="analysis_failed_truncation",
            )
        return ValidationDecision(action="forward_policy")

    def validate_semantics(
        self,
        *,
        payload: dict[str, Any],
        bundle: BundleValidationContext,
    ) -> ValidationDecision:
        skeptical_take = payload.get("skeptical_take_ko")
        if not isinstance(skeptical_take, str) or not skeptical_take.strip():
            return self._semantic("validator_missing_skeptical_take")

        reason_codes = payload.get("reason_codes")
        if not isinstance(reason_codes, list) or len(reason_codes) == 0:
            return self._semantic("validator_missing_reason_codes")

        scores = payload.get("scores")
        if not isinstance(scores, dict):
            return self._semantic("validator_schema_invalid")
        if self._has_out_of_range_numeric_score(scores):
            return self._semantic("validator_score_range_invalid")

        comparables = payload.get("comparables")
        verdict = payload.get("model_proposed_verdict")
        if (
            bundle.current_primary_artifact_type in _GITHUB_PRIMARY_TYPES
            and isinstance(comparables, list)
            and len(comparables) == 0
        ):
            comparables_decision = self._validate_github_no_comparables(
                confidence_band=payload.get("model_confidence_band"),
                scores=scores,
                verdict=verdict,
            )
            if comparables_decision is not None:
                return comparables_decision

        if verdict == "inspect_now":
            evidence_strength = self._score(scores, "evidence_strength")
            confidence = self._score(scores, "confidence")
            hype_penalty = self._score(scores, "hype_penalty")
            if evidence_strength is not None and evidence_strength < 50:
                return self._semantic("validator_inspect_now_evidence_too_low")
            if confidence is not None and confidence < 60:
                return self._semantic("validator_inspect_now_confidence_too_low")
            if hype_penalty is not None and hype_penalty >= 70:
                return self._semantic("validator_inspect_now_hype_too_high")

        return ValidationDecision(
            action="forward_policy",
            reason_code="validator_passed",
            transition_to_state="analysis_validated",
        )

    @staticmethod
    def _score(scores: dict[str, Any], field: str) -> int | None:
        value = scores.get(field)
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    @staticmethod
    def _has_out_of_range_numeric_score(scores: dict[str, Any]) -> bool:
        for value in scores.values():
            if isinstance(value, bool) or not isinstance(value, Real):
                continue
            if value < 0 or value > 100:
                return True
        return False

    @staticmethod
    def _is_truncated(finish_reason: str | None) -> bool:
        if not finish_reason:
            return False
        normalized = finish_reason.strip().lower()
        return normalized in _TRUNCATION_FINISH_REASONS or "truncat" in normalized

    @staticmethod
    def _semantic(reason_code: str) -> ValidationDecision:
        return ValidationDecision(
            action="failed_terminal",
            reason_code=reason_code,
            transition_to_state="analysis_failed_semantic",
        )

    def _validate_github_no_comparables(
        self,
        *,
        confidence_band: Any,
        scores: dict[str, Any],
        verdict: Any,
    ) -> ValidationDecision | None:
        confidence = self._score(scores, "confidence")

        if verdict == "skip":
            return None
        if verdict == "inspect_now" or confidence_band == "high" or (
            confidence is not None and confidence >= 60
        ):
            return self._semantic("validator_github_comparables_required_for_high_action")
        return self._semantic("validator_missing_github_comparables")
