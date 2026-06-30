from __future__ import annotations

from services.judge_openai.request_shape import build_judge_output_schema
from services.judge_openai.service import JudgeOpenAIService


def test_judge_output_schema_uses_locked_score_names_and_0_to_100_range() -> None:
    schema = JudgeOpenAIService.judge_output_schema()
    scores = schema["properties"]["scores"]

    assert scores["additionalProperties"] is False
    assert scores["required"] == [
        "novelty",
        "practical_usefulness",
        "evidence_strength",
        "hype_penalty",
        "confidence",
        "code_quality",
        "maintenance_signal",
        "specificity",
        "reproducibility_signal",
    ]
    assert set(scores["properties"]) == set(scores["required"])

    common_scores = [
        "novelty",
        "practical_usefulness",
        "evidence_strength",
        "hype_penalty",
        "confidence",
    ]
    for name in common_scores:
        assert scores["properties"][name]["type"] == "integer"
        assert scores["properties"][name]["minimum"] == 0
        assert scores["properties"][name]["maximum"] == 100
        assert "0-100 scale" in scores["properties"][name]["description"]
        assert "not a 0-10 scale" in scores["properties"][name]["description"]

    nullable_scores = [
        "code_quality",
        "maintenance_signal",
        "specificity",
        "reproducibility_signal",
    ]
    for name in nullable_scores:
        assert scores["properties"][name]["type"] == ["integer", "null"]
        assert scores["properties"][name]["minimum"] == 0
        assert scores["properties"][name]["maximum"] == 100
        assert "0-100 scale" in scores["properties"][name]["description"]
        assert "not a 0-10 scale" in scores["properties"][name]["description"]

    assert "0-100 scoring" in scores["description"]
    assert "not 0-10 scoring" in scores["description"]


def test_judge_output_schema_rejects_old_score_contract_names() -> None:
    scores = JudgeOpenAIService.judge_output_schema()["properties"]["scores"]

    assert "usefulness" not in scores["required"]
    assert "execution_risk" not in scores["required"]
    assert "usefulness" not in scores["properties"]
    assert "execution_risk" not in scores["properties"]
    assert scores["additionalProperties"] is False


def test_judge_output_schema_documents_no_fabricated_comparables_contract() -> None:
    properties = JudgeOpenAIService.judge_output_schema()["properties"]

    assert "supported by the provided CandidateEvidenceBundle" in properties["comparables"]["description"]
    assert "do not use latent/general knowledge" in properties["comparables"]["description"]
    assert "unsupported comparables" in properties["model_proposed_verdict"]["description"]
    assert "comparison_gap or insufficient_comparables" in properties["reason_codes"]["description"]


def test_judge_output_schema_no_longer_encodes_comparables_as_automatic_skip() -> None:
    properties = JudgeOpenAIService.judge_output_schema()["properties"]
    comparables_description = properties["comparables"]["description"]
    verdict_description = properties["model_proposed_verdict"]["description"]

    assert "use [] only with conservative skip" not in comparables_description
    assert "later or inspect_now requires" not in verdict_description
    assert "no reliable comparables are available, emit skip" not in verdict_description
    assert "may be proposed without comparables" in verdict_description


def test_judge_output_schema_guides_no_reliable_comparables_to_limitation_and_penalty() -> None:
    properties = JudgeOpenAIService.judge_output_schema()["properties"]
    reason_description = properties["reason_codes"]["description"]
    verdict_description = properties["model_proposed_verdict"]["description"]

    assert "comparison_gap or insufficient_comparables" in reason_description
    assert "not an automatic veto" in reason_description
    assert "reduce evidence_strength" in verdict_description
    assert "and/or confidence" in verdict_description


def test_judge_output_schema_shape_remains_strict_and_compatible() -> None:
    schema = build_judge_output_schema()
    properties = schema["properties"]
    scores = properties["scores"]

    assert schema["additionalProperties"] is False
    assert scores["additionalProperties"] is False
    assert set(schema["required"]) >= {
        "comparables",
        "scores",
        "reason_codes",
        "model_proposed_verdict",
    }
    assert set(scores["required"]) >= {
        "practical_usefulness",
        "evidence_strength",
        "confidence",
        "hype_penalty",
        "code_quality",
        "specificity",
    }
    assert properties["model_proposed_verdict"]["enum"] == ["inspect_now", "later", "skip", None]
