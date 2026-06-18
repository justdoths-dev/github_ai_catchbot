from __future__ import annotations

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
        assert scores["properties"][name] == {"type": "integer", "minimum": 0, "maximum": 100}

    nullable_scores = [
        "code_quality",
        "maintenance_signal",
        "specificity",
        "reproducibility_signal",
    ]
    for name in nullable_scores:
        assert scores["properties"][name] == {"type": ["integer", "null"], "minimum": 0, "maximum": 100}


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
    assert "use [] when no reliable comparables are available" in properties["comparables"]["description"]
    assert "comparison_gap or insufficient_comparables" in properties["reason_codes"]["description"]
