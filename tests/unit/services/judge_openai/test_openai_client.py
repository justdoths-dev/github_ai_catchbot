from __future__ import annotations

from services.judge_openai.openai_client import OpenAIJudgeClient


def test_openai_client_builds_responses_api_request_without_tools() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema={"type": "object", "properties": {}, "additionalProperties": False},
        max_output_tokens=500,
        prompt_cache_key="judge:github:v1",
    )

    assert request["model"] == "gpt-5.4-mini"
    assert request["reasoning"] == {"effort": "low"}
    assert request["input"][0]["role"] == "developer"
    assert request["input"][1]["role"] == "user"
    assert request["text"]["format"]["type"] == "json_schema"
    assert request["text"]["format"]["strict"] is True
    assert request["tools"] == []
    assert request["max_output_tokens"] == 500
    assert request["prompt_cache_key"] == "judge:github:v1"


def test_openai_client_omits_missing_prompt_cache_key_for_legacy_events() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema={"type": "object", "properties": {}, "additionalProperties": False},
        max_output_tokens=500,
        prompt_cache_key=None,
    )

    assert "prompt_cache_key" not in request
