from __future__ import annotations

import pytest

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


@pytest.mark.asyncio
async def test_openai_client_can_use_injected_fake_without_sdk_import_or_network() -> None:
    class FakeResponses:
        def __init__(self) -> None:
            self.request = None

        async def create(self, **request):
            self.request = request
            return {"status": "completed", "output_text": "{}"}

    class FakeClient:
        def __init__(self) -> None:
            self.responses = FakeResponses()

    fake_client = FakeClient()
    client = OpenAIJudgeClient(
        api_key="unused",
        project=None,
        timeout_sec=1,
        client=fake_client,
    )

    response = await client.create_structured_response(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema={"type": "object", "properties": {}, "additionalProperties": False},
        max_output_tokens=None,
        prompt_cache_key=None,
    )

    assert response == {"status": "completed", "output_text": "{}"}
    assert fake_client.responses.request is not None
    assert fake_client.responses.request["tools"] == []
