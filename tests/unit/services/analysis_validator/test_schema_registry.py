from __future__ import annotations

from uuid import uuid4

from services.analysis_validator.schema_registry import JudgeOutputSchemaRegistry


def _registry() -> JudgeOutputSchemaRegistry:
    return JudgeOutputSchemaRegistry(max_headline_chars=200, max_summary_chars=1200, max_text_items=10)


def valid_payload() -> dict:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(uuid4()),
        "headline": "Useful repository",
        "summary_one_line_ko": "short summary",
        "skeptical_take_ko": "needs more evidence before acting",
        "why_it_might_matter_ko": "could help workflow automation",
        "comparables": ["existing-tool"],
        "scores": {
            "novelty": 61,
            "practical_usefulness": 72,
            "evidence_strength": 66,
            "hype_penalty": 20,
            "confidence": 70,
            "code_quality": 55,
            "maintenance_signal": 52,
            "specificity": 64,
            "reproducibility_signal": None,
        },
        "reason_codes": ["repo_has_clear_scope"],
        "red_flags_ko": ["production use is still unclear"],
        "evidence_limitations_ko": ["only public docs were checked"],
        "recommended_action_ko": "review later",
        "freshness_note_ko": "recent activity needs verification",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def test_schema_registry_accepts_corrected_locked_score_contract() -> None:
    decision = _registry().validate(valid_payload())

    assert decision.action == "forward_policy"


def test_schema_registry_rejects_missing_required_locked_score_fields() -> None:
    payload = valid_payload()
    del payload["scores"]["maintenance_signal"]

    decision = _registry().validate(payload)

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_schema_invalid"


def test_schema_registry_rejects_old_score_only_payload() -> None:
    payload = valid_payload()
    payload["scores"] = {
        "novelty": 61,
        "usefulness": 72,
        "evidence_strength": 66,
        "hype_penalty": 20,
        "confidence": 70,
        "execution_risk": 40,
    }

    decision = _registry().validate(payload)

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_schema_invalid"


def test_schema_registry_enforces_score_range() -> None:
    payload = valid_payload()
    payload["scores"]["novelty"] = 101

    decision = _registry().validate(payload)

    assert decision.action == "failed_terminal"
    assert decision.reason_code == "validator_score_range_invalid"
