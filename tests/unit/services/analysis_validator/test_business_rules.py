from __future__ import annotations

from uuid import uuid4

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


def test_business_rules_reject_github_family_primary_with_empty_comparables() -> None:
    payload = valid_payload()
    payload["comparables"] = []

    decision = AnalysisValidatorBusinessRules().validate_semantics(
        payload=payload,
        bundle=_bundle("github_repo"),
    )

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_missing_github_comparables"


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
