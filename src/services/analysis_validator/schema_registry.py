from __future__ import annotations

from typing import Any

from .models import ValidationDecision


_EXPECTED_SCHEMA_VERSION = "judge_output_v1"
_REQUIRED_TOP_LEVEL_FIELDS = {
    "judge_schema_version",
    "candidate_group_id",
    "headline",
    "summary_one_line_ko",
    "skeptical_take_ko",
    "why_it_might_matter_ko",
    "comparables",
    "scores",
    "reason_codes",
    "red_flags_ko",
    "evidence_limitations_ko",
    "recommended_action_ko",
    "freshness_note_ko",
    "model_proposed_verdict",
    "model_confidence_band",
}
_REQUIRED_SCORE_FIELDS = {
    "novelty",
    "practical_usefulness",
    "evidence_strength",
    "hype_penalty",
    "confidence",
    "code_quality",
    "maintenance_signal",
    "specificity",
    "reproducibility_signal",
}
_REQUIRED_INTEGER_SCORE_FIELDS = {
    "novelty",
    "practical_usefulness",
    "evidence_strength",
    "hype_penalty",
    "confidence",
}
_OPTIONAL_NULLABLE_SCORE_FIELDS = {
    "code_quality",
    "maintenance_signal",
    "specificity",
    "reproducibility_signal",
}
_ARRAY_FIELDS = {
    "comparables",
    "reason_codes",
    "red_flags_ko",
    "evidence_limitations_ko",
}
_STRING_FIELDS = {
    "judge_schema_version",
    "candidate_group_id",
    "headline",
    "summary_one_line_ko",
    "skeptical_take_ko",
    "why_it_might_matter_ko",
    "recommended_action_ko",
    "freshness_note_ko",
}
_VERDICTS = {"inspect_now", "later", "skip"}
_CONFIDENCE_BANDS = {"low", "medium", "high"}


class JudgeOutputSchemaRegistry:
    def __init__(
        self,
        *,
        max_headline_chars: int,
        max_summary_chars: int,
        max_text_items: int,
    ) -> None:
        self._max_headline_chars = max_headline_chars
        self._max_summary_chars = max_summary_chars
        self._max_text_items = max_text_items

    def validate(self, payload: dict[str, Any]) -> ValidationDecision:
        if not isinstance(payload, dict):
            return self._schema_invalid()

        if set(payload) != _REQUIRED_TOP_LEVEL_FIELDS:
            return self._schema_invalid()

        for field in _STRING_FIELDS:
            if not isinstance(payload.get(field), str):
                return self._schema_invalid()
        if payload["judge_schema_version"] != _EXPECTED_SCHEMA_VERSION:
            return self._schema_invalid()

        if len(payload["headline"]) > self._max_headline_chars:
            return self._schema_invalid()
        if len(payload["summary_one_line_ko"]) > self._max_summary_chars:
            return self._schema_invalid()

        for field in _ARRAY_FIELDS:
            if not self._is_string_list(payload.get(field)):
                return self._schema_invalid()
            if len(payload[field]) > self._max_text_items:
                return self._schema_invalid()

        verdict = payload.get("model_proposed_verdict")
        if verdict is not None and verdict not in _VERDICTS:
            return self._schema_invalid()
        confidence_band = payload.get("model_confidence_band")
        if confidence_band is not None and confidence_band not in _CONFIDENCE_BANDS:
            return self._schema_invalid()

        scores = payload.get("scores")
        if not isinstance(scores, dict) or set(scores) != _REQUIRED_SCORE_FIELDS:
            return self._schema_invalid()
        for field in _REQUIRED_INTEGER_SCORE_FIELDS:
            if not self._valid_score(scores.get(field), allow_null=False):
                return self._score_invalid()
        for field in _OPTIONAL_NULLABLE_SCORE_FIELDS:
            if not self._valid_score(scores.get(field), allow_null=True):
                return self._score_invalid()

        return ValidationDecision(action="forward_policy")

    @staticmethod
    def _is_string_list(value: Any) -> bool:
        return isinstance(value, list) and all(isinstance(item, str) for item in value)

    @staticmethod
    def _valid_score(value: Any, *, allow_null: bool) -> bool:
        if value is None:
            return allow_null
        return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 100

    @staticmethod
    def _schema_invalid() -> ValidationDecision:
        return ValidationDecision(
            action="failed_terminal",
            reason_code="validator_schema_invalid",
            transition_to_state="analysis_failed_schema",
        )

    @staticmethod
    def _score_invalid() -> ValidationDecision:
        return ValidationDecision(
            action="failed_terminal",
            reason_code="validator_score_range_invalid",
            transition_to_state="analysis_failed_schema",
        )
