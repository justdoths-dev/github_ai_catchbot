from __future__ import annotations

import asyncio
import json

import pytest

from src.services.judge_openai.restricted_judge_canary import (
    CANDIDATE_GROUP_ID,
    DEFAULT_MAX_REQUESTS,
    RestrictedOpenAIJudgeCanaryConfig,
    RestrictedOpenAIJudgeCanaryRequestBudget,
    run_restricted_openai_judge_canary,
)


API_KEY = "sentinel_openai_api_key_value"
RAW_PROMPT_SENTINEL = "Synthetic CandidateEvidenceBundle JSON"
GENERATED_TEXT = "sentinel generated korean analysis text"
RAW_EXCEPTION = "sentinel raw openai exception detail"


class FakeOpenAIJudgeClient:
    def __init__(self, *, response: object | None = None, error: BaseException | None = None) -> None:
        self.response = response if response is not None else _response()
        self.error = error
        self.calls: list[dict] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


class FakeOpenAIException(Exception):
    def __init__(
        self,
        message: str = RAW_EXCEPTION,
        *,
        status_code: int | str | None = None,
        code: str | None = None,
        error_type: str | None = None,
        body: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.type = error_type
        self.body = body


class RateLimitError(FakeOpenAIException):
    pass


class AuthenticationError(FakeOpenAIException):
    pass


class APITimeoutError(FakeOpenAIException):
    pass


def _valid_payload(**overrides) -> dict:
    payload = {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": CANDIDATE_GROUP_ID,
        "headline": "Synthetic developer workflow tool",
        "summary_one_line_ko": GENERATED_TEXT,
        "skeptical_take_ko": GENERATED_TEXT,
        "why_it_might_matter_ko": GENERATED_TEXT,
        "comparables": [],
        "scores": {
            "novelty": 25,
            "practical_usefulness": 30,
            "evidence_strength": 20,
            "hype_penalty": 15,
            "confidence": 35,
            "code_quality": 40,
            "maintenance_signal": None,
            "specificity": 30,
            "reproducibility_signal": None,
        },
        "reason_codes": ["synthetic_canary_fixture"],
        "red_flags_ko": [GENERATED_TEXT],
        "evidence_limitations_ko": [GENERATED_TEXT],
        "recommended_action_ko": GENERATED_TEXT,
        "freshness_note_ko": GENERATED_TEXT,
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }
    payload.update(overrides)
    return payload


def _response(payload: dict | None = None, *, usage: dict | None = None) -> dict:
    return {
        "status": "completed",
        "output_text": json.dumps(payload if payload is not None else _valid_payload()),
        "usage": usage
        if usage is not None
        else {
            "input_tokens": 101,
            "input_tokens_details": {"cached_tokens": 7},
            "output_tokens": 88,
            "output_tokens_details": {"reasoning_tokens": 13},
        },
    }


def _refusal_response() -> dict:
    return {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [{"type": "refusal", "refusal": GENERATED_TEXT}],
            }
        ],
    }


def _config(**overrides) -> RestrictedOpenAIJudgeCanaryConfig:
    values = {
        "operator_approved": True,
        "allow_network": True,
        "api_key": API_KEY,
    }
    values.update(overrides)
    return RestrictedOpenAIJudgeCanaryConfig(**values)


def _run(
    config: RestrictedOpenAIJudgeCanaryConfig,
    client: FakeOpenAIJudgeClient,
    *,
    budget: RestrictedOpenAIJudgeCanaryRequestBudget | None = None,
) -> dict:
    result = asyncio.run(run_restricted_openai_judge_canary(config, client=client, request_budget=budget))
    return result.to_sanitized_dict()


def _render(report: dict) -> str:
    return json.dumps(report, sort_keys=True)


@pytest.mark.parametrize(
    ("config", "expected_code"),
    [
        (_config(operator_approved=False), "operator_approval_missing"),
        (_config(allow_network=False), "network_not_allowed"),
        (_config(api_key=""), "credential_missing"),
        (_config(model="gpt-5.3"), "model_not_allowed"),
        (_config(reasoning_effort="high"), "reasoning_effort_not_allowed"),
        (_config(fixture_profile="x_primary_minimal"), "fixture_profile_invalid"),
        (_config(max_requests=0), "request_cap_invalid"),
        (_config(max_requests=2), "request_cap_invalid"),
        (_config(max_output_tokens=0), "output_token_cap_invalid"),
        (_config(max_input_chars=1), "input_char_cap_invalid"),
    ],
)
def test_precondition_failures_block_before_network(config: RestrictedOpenAIJudgeCanaryConfig, expected_code: str):
    client = FakeOpenAIJudgeClient()

    report = _run(config, client)

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == expected_code
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_fake_success_returns_sanitized_metadata_only() -> None:
    client = FakeOpenAIJudgeClient()

    report = _run(_config(), client)
    text = _render(report)

    assert report["canary_name"] == "restricted_openai_judge_canary"
    assert report["mode"] == "restricted_live_judge"
    assert report["model"] == "gpt-5.4-mini"
    assert report["reasoning_effort"] == "low"
    assert report["prompt_version"] == "restricted_openai_judge_canary_prompt_v1"
    assert report["schema_version"] == "judge_output_v1"
    assert report["policy_version"] == "verdict_policy_v1"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert report["max_requests"] == DEFAULT_MAX_REQUESTS
    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["error_code"] is None
    assert report["finish_reason"] == "completed"
    assert report["refusal_detected"] is False
    assert report["schema_valid"] is True
    assert report["required_field_count"] == 15
    assert report["observed_model_proposed_verdict"] == "later"
    assert report["observed_confidence_band"] == "medium"
    assert report["input_tokens"] == 101
    assert report["cached_input_tokens"] == 7
    assert report["output_tokens"] == 88
    assert report["reasoning_tokens"] == 13
    assert all(value is False for value in report["side_effects"].values())
    assert client.calls[0]["model"] == "gpt-5.4-mini"
    assert client.calls[0]["reasoning_effort"] == "low"
    assert client.calls[0]["max_output_tokens"] == 900
    assert client.calls[0]["prompt_cache_key"] is None
    assert client.calls[0]["json_schema"]["additionalProperties"] is False
    assert client.calls[0]["json_schema"]["properties"]["model_proposed_verdict"]["enum"] == [
        "inspect_now",
        "later",
        "skip",
    ]
    assert API_KEY not in text
    assert "Authorization" not in text
    assert RAW_PROMPT_SENTINEL not in text
    assert "raw request" not in text.lower()
    assert "output_text" not in text
    assert GENERATED_TEXT not in text


def test_fake_refusal_maps_to_openai_refusal_without_refusal_text_leakage() -> None:
    client = FakeOpenAIJudgeClient(response=_refusal_response())

    report = _run(_config(), client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["ok"] is False
    assert report["error_code"] == "openai_refusal"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert report["refusal_detected"] is True
    assert GENERATED_TEXT not in text


def test_fake_schema_invalid_output_maps_to_openai_schema_invalid() -> None:
    payload = _valid_payload(model_proposed_verdict="deliver_now")
    client = FakeOpenAIJudgeClient(response=_response(payload))

    report = _run(_config(), client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["error_code"] == "openai_schema_invalid"
    assert report["schema_valid"] is False
    assert report["required_field_count"] == 15
    assert report["observed_model_proposed_verdict"] is None
    assert "deliver_now" not in text


@pytest.mark.parametrize(
    "response",
    [
        {"status": "completed", "output_text": "not json " + GENERATED_TEXT},
        {"status": "completed", "output_text": json.dumps(["not", "an", "object"])},
        object(),
    ],
)
def test_fake_malformed_response_maps_to_openai_response_invalid(response: object) -> None:
    client = FakeOpenAIJudgeClient(response=response)

    report = _run(_config(), client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["error_code"] == "openai_response_invalid"
    assert report["schema_valid"] is False
    assert GENERATED_TEXT not in text


@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        (RateLimitError(status_code=429), "openai_rate_limited"),
        (AuthenticationError(status_code=401), "openai_auth_failed"),
        (AuthenticationError(status_code=403), "openai_auth_failed"),
        (
            FakeOpenAIException(
                status_code=429,
                code="insufficient_quota",
                body={"error": {"code": "insufficient_quota"}},
            ),
            "openai_quota_or_billing",
        ),
        (FakeOpenAIException(status_code=400, code="bad_request"), "openai_bad_request"),
        (FakeOpenAIException(status_code=500), "openai_transient_error"),
        (APITimeoutError(code="timeout"), "openai_transient_error"),
        (TimeoutError(RAW_EXCEPTION), "openai_transient_error"),
    ],
)
def test_openai_exceptions_are_bucketed_without_exception_detail(error: BaseException, expected_code: str) -> None:
    client = FakeOpenAIJudgeClient(error=error)

    report = _run(_config(), client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["ok"] is False
    assert report["error_code"] == expected_code
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert RAW_EXCEPTION not in text
    assert API_KEY not in text
    assert "exception_detail_omitted" in report["redactions_applied"]


def test_request_cap_exceeded_fails_safely_without_client_call() -> None:
    client = FakeOpenAIJudgeClient()
    budget = RestrictedOpenAIJudgeCanaryRequestBudget(max_requests=0)

    report = _run(_config(), client, budget=budget)

    assert report["status"] == "blocked"
    assert report["error_code"] == "request_cap_exceeded"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_token_usage_metadata_is_numeric_and_sanitized() -> None:
    client = FakeOpenAIJudgeClient(
        response=_response(
            usage={
                "input_tokens": "12",
                "input_tokens_details": {"cached_tokens": "3"},
                "output_tokens": "9",
                "output_tokens_details": {"reasoning_tokens": "4"},
            }
        )
    )

    report = _run(_config(), client)
    text = _render(report)

    assert report["status"] == "pass"
    assert report["input_tokens"] == 12
    assert report["cached_input_tokens"] == 3
    assert report["output_tokens"] == 9
    assert report["reasoning_tokens"] == 4
    assert API_KEY not in text
    assert GENERATED_TEXT not in text
