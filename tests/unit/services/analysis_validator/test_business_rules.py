from __future__ import annotations

from uuid import uuid4

import pytest

from services.analysis_validator.business_rules import AnalysisValidatorBusinessRules
from services.analysis_validator.models import BundleValidationContext
from tests.unit.services.analysis_validator.test_schema_registry import valid_payload


def _bundle(artifact_type: str | None = "web_article") -> BundleValidationContext:
    return BundleValidationContext(
        bundle_id=uuid4(),
        candidate_group_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        current_primary_artifact_type=artifact_type,
    )


def _live_terminal_no_comparables_payload() -> dict:
    payload = valid_payload()
    payload["comparables"] = []
    payload["scores"].update(
        {
            "novelty": 22,
            "practical_usefulness": 58,
            "evidence_strength": 45,
            "hype_penalty": 35,
            "confidence": 45,
            "code_quality": None,
            "maintenance_signal": None,
        }
    )
    payload["reason_codes"] = [
        "repo_scope_unclear",
        "low_evidence_strength",
        "limited_usage_signal",
        "maintenance_signal_unknown",
        "defer_until_more_evidence",
    ]
    payload["evidence_limitations_ko"] = ["only limited public bundle evidence was available"]
    payload["recommended_action_ko"] = "review later after more evidence"
    payload["model_proposed_verdict"] = "later"
    payload["model_confidence_band"] = "low"
    return payload


def test_business_rules_pass_valid_payload() -> None:
    payload = valid_payload()
    payload["comparables"] = []

    decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=_bundle())

    assert decision.action == "forward_policy"
    assert decision.reason_code == "validator_passed"


def test_business_rules_reject_empty_skeptical_take() -> None:
    payload = valid_payload()
    payload["skeptical_take_ko"] = " "

    decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=_bundle())

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_missing_skeptical_take"


def test_business_rules_reject_empty_reason_codes() -> None:
    payload = valid_payload()
    payload["reason_codes"] = []

    decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=_bundle())

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_missing_reason_codes"


def test_business_rules_reject_score_outside_0_to_100() -> None:
    payload = valid_payload()
    payload["scores"]["evidence_strength"] = 101

    decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=_bundle())

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_score_range_invalid"


def test_business_rules_reject_inspect_now_with_evidence_strength_below_50() -> None:
    payload = valid_payload()
    payload["model_proposed_verdict"] = "inspect_now"
    payload["scores"]["evidence_strength"] = 49

    decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=_bundle())

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_inspect_now_evidence_too_low"


def test_business_rules_reject_inspect_now_with_confidence_below_60() -> None:
    payload = valid_payload()
    payload["model_proposed_verdict"] = "inspect_now"
    payload["scores"]["confidence"] = 59

    decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=_bundle())

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_inspect_now_confidence_too_low"


def test_business_rules_reject_inspect_now_with_hype_penalty_70_or_higher() -> None:
    payload = valid_payload()
    payload["model_proposed_verdict"] = "inspect_now"
    payload["scores"]["hype_penalty"] = 70

    decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=_bundle())

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_inspect_now_hype_too_high"


def test_business_rules_allow_github_no_comparables_skip_to_reach_policy_suppress() -> None:
    payload = _live_terminal_no_comparables_payload()
    payload["scores"].update(
        {
            "novelty": 2,
            "practical_usefulness": 1,
            "evidence_strength": 1,
            "hype_penalty": 0,
            "confidence": 9,
            "code_quality": None,
            "maintenance_signal": None,
            "specificity": 1,
            "reproducibility_signal": None,
        }
    )
    payload["model_proposed_verdict"] = "skip"
    payload["model_confidence_band"] = "high"

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "forward_policy"
    assert decision.reason_code == "validator_passed"


@pytest.mark.parametrize("shape", ["missing", "null"])
def test_business_rules_allow_github_missing_or_null_comparables_skip_to_reach_policy_suppress(shape: str) -> None:
    payload = _live_terminal_no_comparables_payload()
    payload["scores"].update(
        {
            "novelty": 2,
            "practical_usefulness": 1,
            "evidence_strength": 1,
            "hype_penalty": 0,
            "confidence": 9,
            "code_quality": None,
            "maintenance_signal": None,
            "specificity": 1,
            "reproducibility_signal": None,
        }
    )
    payload["model_proposed_verdict"] = "skip"
    payload["model_confidence_band"] = "high"
    if shape == "missing":
        payload.pop("comparables")
    else:
        payload["comparables"] = None

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "forward_policy"
    assert decision.reason_code == "validator_passed"


def test_business_rules_reject_github_no_comparables_later_even_with_comparison_gap_marker() -> None:
    payload = _live_terminal_no_comparables_payload()
    payload["reason_codes"].extend(["comparison_gap", "insufficient_comparables"])
    payload["evidence_limitations_ko"] = ["comparison_gap"]

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_missing_github_comparables"


@pytest.mark.parametrize("shape", ["missing", "null"])
def test_business_rules_reject_github_missing_or_null_comparables_later(shape: str) -> None:
    payload = _live_terminal_no_comparables_payload()
    payload["reason_codes"].extend(["comparison_gap", "insufficient_comparables"])
    payload["evidence_limitations_ko"] = ["comparison_gap"]
    if shape == "missing":
        payload.pop("comparables")
    else:
        payload["comparables"] = None

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_missing_github_comparables"


def test_business_rules_non_github_no_comparables_later_still_forwards_policy() -> None:
    payload = _live_terminal_no_comparables_payload()
    payload["reason_codes"].extend(["comparison_gap", "insufficient_comparables"])
    payload["evidence_limitations_ko"] = ["comparison_gap"]

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("web_article"),
    )

    assert decision.action == "forward_policy"
    assert decision.reason_code == "validator_passed"


def test_business_rules_reject_github_no_comparables_high_confidence_even_with_gap_marker() -> None:
    payload = _live_terminal_no_comparables_payload()
    payload["reason_codes"].append("comparison_gap")
    payload["model_confidence_band"] = "high"

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_github_comparables_required_for_high_action"


def test_business_rules_reject_github_no_comparables_inspect_now_even_with_gap_marker() -> None:
    payload = _live_terminal_no_comparables_payload()
    payload["reason_codes"].append("comparison_gap")
    payload["model_proposed_verdict"] = "inspect_now"

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_github_comparables_required_for_high_action"


def test_business_rules_reject_github_no_comparables_without_comparison_gap_marker() -> None:
    payload = _live_terminal_no_comparables_payload()

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_missing_github_comparables"


def test_business_rules_reject_github_no_comparables_high_action_even_with_comparison_gap() -> None:
    payload = valid_payload()
    payload["comparables"] = []
    payload["reason_codes"] = ["comparison_gap"]
    payload["scores"].update(
        {
            "evidence_strength": 80,
            "confidence": 85,
            "hype_penalty": 10,
        }
    )
    payload["model_proposed_verdict"] = "inspect_now"
    payload["model_confidence_band"] = "high"

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_github_comparables_required_for_high_action"


def test_refusal_branch_produces_analysis_refused_and_no_policy_handoff_decision() -> None:
    decision = AnalysisValidatorBusinessRules().evaluate_control_flow(
        payload={"output_kind": "refusal"},
        finish_reason="completed",
        refusal_detected=False,
    )

    assert decision.action == "refused"
    assert decision.transition_to_state == "analysis_refused"
    assert decision.reason_code == "model_refusal"


def test_truncation_finish_reason_produces_failed_retryable_decision() -> None:
    decision = AnalysisValidatorBusinessRules().evaluate_control_flow(
        payload=valid_payload(),
        finish_reason="max_output_tokens",
        refusal_detected=False,
    )

    assert decision.action == "failed_retryable"
    assert decision.transition_to_state == "analysis_failed_truncation"
    assert decision.reason_code == "analysis_failed_truncation"
