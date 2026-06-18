from __future__ import annotations

import json
import time

from services.judge_openai.response_mapper import OpenAIResponseMapper


def test_response_mapper_parses_valid_structured_json_from_output_text() -> None:
    payload = {"model_proposed_verdict": "later", "model_confidence_band": "medium"}
    response = {
        "id": "resp_1",
        "status": "completed",
        "output_text": json.dumps(payload),
    }

    result = OpenAIResponseMapper().parse(response, started_monotonic=time.monotonic())

    assert result.payload_json == payload
    assert result.refusal_text is None
    assert result.finish_reason == "completed"
    assert result.raw_response_id == "resp_1"


def test_response_mapper_preserves_conservative_empty_comparables_without_defaults() -> None:
    payload = {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": "candidate-1",
        "headline": "Useful but early repository",
        "summary_one_line_ko": "short summary",
        "skeptical_take_ko": "comparison evidence is unavailable",
        "why_it_might_matter_ko": "could help workflow automation",
        "comparables": [],
        "scores": {
            "novelty": 35,
            "practical_usefulness": 40,
            "evidence_strength": 1,
            "hype_penalty": 20,
            "confidence": 20,
            "code_quality": None,
            "maintenance_signal": None,
            "specificity": 20,
            "reproducibility_signal": None,
        },
        "reason_codes": ["comparison_gap"],
        "red_flags_ko": ["comparison evidence is missing"],
        "evidence_limitations_ko": ["comparison_gap"],
        "recommended_action_ko": "review later",
        "freshness_note_ko": "freshness unknown",
        "model_proposed_verdict": "later",
        "model_confidence_band": "low",
    }
    response = {
        "status": "completed",
        "output_text": json.dumps(payload),
    }

    result = OpenAIResponseMapper().parse(response, started_monotonic=time.monotonic())

    assert result.payload_json == payload
    assert result.payload_json["comparables"] == []
    assert "comparison_gap" in result.payload_json["reason_codes"]


def test_response_mapper_extracts_refusal_text_from_output_blocks() -> None:
    response = {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [
                    {"type": "refusal", "refusal": "I cannot comply."},
                ],
            }
        ],
    }

    result = OpenAIResponseMapper().parse(response, started_monotonic=time.monotonic())

    assert result.payload_json is None
    assert result.refusal_text == "I cannot comply."
    assert result.refusal_detected is True


def test_response_mapper_handles_invalid_json_as_missing_structured_payload() -> None:
    response = {
        "status": "completed",
        "output_text": "{not-valid-json",
    }

    result = OpenAIResponseMapper().parse(response, started_monotonic=time.monotonic())

    assert result.payload_json is None
    assert result.refusal_text is None
    assert result.has_structured_payload is False
    assert result.finish_reason == "completed"


def test_response_mapper_extracts_usage_fields_and_latency() -> None:
    started = time.monotonic() - 0.05
    response = {
        "usage": {
            "input_tokens": 100,
            "input_tokens_details": {"cached_tokens": 80},
            "output_tokens": 25,
            "output_tokens_details": {"reasoning_tokens": 7},
        }
    }

    result = OpenAIResponseMapper().parse(response, started_monotonic=started)

    assert result.usage.input_tokens == 100
    assert result.usage.cached_input_tokens == 80
    assert result.usage.output_tokens == 25
    assert result.usage.reasoning_tokens == 7
    assert result.usage.latency_ms is not None
    assert result.usage.latency_ms >= 1


def test_response_mapper_builds_refusal_envelope() -> None:
    envelope = OpenAIResponseMapper().build_refusal_envelope(
        candidate_group_id="candidate-1",
        schema_version="judge_output_v1",
        refusal_text="No.",
    )

    assert envelope == {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": "candidate-1",
        "output_kind": "refusal",
        "refusal_text": "No.",
    }
