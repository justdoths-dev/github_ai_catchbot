from __future__ import annotations

import pytest

from services.judge_openai.openai_client import (
    OpenAIJudgeClient,
    OpenAIPermanentError,
    OpenAIRequestShapeError,
    OpenAITransientError,
)
from services.judge_openai.request_shape import (
    summarize_responses_request_shape,
    validate_responses_request_shape,
)
from services.judge_openai.service import JudgeOpenAIService


def test_openai_client_builds_responses_api_request_without_tools() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema={
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
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
    assert "prompt_cache_key" not in request


def test_openai_client_current_judge_schema_passes_request_shape_diagnostic() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=500,
        prompt_cache_key="judge:github:v1",
    )

    summary = summarize_responses_request_shape(request)

    assert summary["request_shape_valid_bucket"] == "one"
    assert summary["request_shape_issue_count_bucket"] == "zero"
    assert summary["request_shape_issue_buckets"] == []
    assert summary["model_bucket"] == "locked_hot_path"
    assert summary["reasoning_effort_bucket"] == "low"
    assert summary["top_level_request_key_presence_buckets"]["max_output_tokens"] == "one"
    assert summary["top_level_request_key_presence_buckets"]["prompt_cache_key"] == "zero"
    assert summary["optional_null_field_count_bucket"] == "zero"
    assert summary["optional_null_field_name_buckets"] == []
    assert summary["text_format_type_bucket"] == "json_schema"
    assert summary["text_format_json_schema_bucket"] == "one"
    assert summary["json_schema_strict_bucket"] == "one"
    assert summary["strict_schema_bucket"] == "one"
    assert summary["tools_count_bucket"] == "zero"
    assert summary["tools_bucket"] == "zero"
    assert summary["max_output_tokens_presence_bucket"] == "one"
    assert summary["max_output_tokens_null_bucket"] == "zero"
    assert summary["prompt_cache_key_presence_bucket"] == "zero"
    assert summary["openai_call_attempted"] is False
    assert summary["openai_key_file_read_bucket"] == "zero"
    assert summary["database_write_attempted"] is False
    assert summary["redis_write_attempted"] is False
    assert summary["raw_values_emitted"] is False


def test_request_shape_diagnostic_detects_injected_prompt_cache_key() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=500,
        prompt_cache_key="judge:github:v1",
    )
    request["prompt_cache_key"] = "judge:github:v1"

    diagnostic = validate_responses_request_shape(request)
    summary = summarize_responses_request_shape(request)

    assert diagnostic.valid
    assert summary["top_level_request_key_presence_buckets"]["prompt_cache_key"] == "one"
    assert summary["prompt_cache_key_presence_bucket"] == "one"


def test_openai_client_omits_missing_prompt_cache_key_for_legacy_events() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema={
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
        max_output_tokens=500,
        prompt_cache_key=None,
    )

    assert "prompt_cache_key" not in request


def test_openai_client_omits_none_optional_generation_controls() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema={
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
        max_output_tokens=None,
        prompt_cache_key=None,
    )

    summary = summarize_responses_request_shape(request)

    assert "max_output_tokens" not in request
    assert "prompt_cache_key" not in request
    assert summary["top_level_request_key_presence_buckets"]["max_output_tokens"] == "zero"
    assert summary["top_level_request_key_presence_buckets"]["prompt_cache_key"] == "zero"
    assert summary["max_output_tokens_presence_bucket"] == "zero"
    assert summary["max_output_tokens_null_bucket"] == "zero"
    assert summary["prompt_cache_key_presence_bucket"] == "zero"
    assert summary["optional_null_field_count_bucket"] == "zero"
    assert summary["optional_null_field_name_buckets"] == []


@pytest.mark.parametrize(
    ("exception_name", "status_code", "expected_error", "expected_safe_code"),
    [
        ("RateLimitError", None, OpenAITransientError, "openai_retryable_rate_limited"),
        ("OtherError", 429, OpenAITransientError, "openai_retryable_rate_limited"),
        ("APITimeoutError", None, OpenAITransientError, "openai_retryable_timeout"),
        ("APIConnectionError", None, OpenAITransientError, "openai_retryable_connection"),
        ("InternalServerError", 500, OpenAITransientError, "openai_retryable_server_error"),
        ("AuthenticationError", None, OpenAIPermanentError, "openai_permanent_auth"),
        ("OtherError", 401, OpenAIPermanentError, "openai_permanent_auth"),
        ("PermissionDeniedError", None, OpenAIPermanentError, "openai_permanent_permission"),
        ("OtherError", 403, OpenAIPermanentError, "openai_permanent_permission"),
        ("BadRequestError", None, OpenAIPermanentError, "openai_permanent_bad_request"),
        ("OtherError", 400, OpenAIPermanentError, "openai_permanent_bad_request"),
        ("NotFoundError", None, OpenAIPermanentError, "openai_permanent_not_found"),
        ("OtherError", 404, OpenAIPermanentError, "openai_permanent_not_found"),
        ("ConflictError", 409, OpenAIPermanentError, "openai_permanent_client_error"),
        ("UnexpectedError", None, OpenAITransientError, "openai_retryable_unknown"),
    ],
)
def test_openai_client_classifies_provider_errors_to_safe_codes(
    exception_name: str,
    status_code: int | None,
    expected_error: type[Exception],
    expected_safe_code: str,
) -> None:
    exc_cls = type(exception_name, (Exception,), {})
    exc = exc_cls("private provider response body")
    if status_code is not None:
        exc.status_code = status_code

    with pytest.raises(expected_error) as exc_info:
        OpenAIJudgeClient._raise_mapped_exception(exc)

    assert exc_info.value.safe_code == expected_safe_code
    assert str(exc_info.value) == expected_safe_code
    assert "private provider response body" not in str(exc_info.value)


def test_openai_error_safe_code_rejects_raw_caller_value() -> None:
    retryable = OpenAITransientError(
        "private provider response body",
        safe_code="private provider response body",
    )
    permanent = OpenAIPermanentError(
        "private provider response body",
        safe_code="private provider response body",
    )

    assert retryable.safe_code == "openai_retryable_unknown"
    assert permanent.safe_code == "openai_permanent_unknown"


def test_request_shape_validator_rejects_injected_optional_null_fields() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="developer",
        user_context="user",
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=500,
        prompt_cache_key="judge:github:v1",
    )
    request["max_output_tokens"] = None
    request["prompt_cache_key"] = None

    diagnostic = validate_responses_request_shape(request)
    summary = summarize_responses_request_shape(request)

    assert not diagnostic.valid
    assert "max_output_tokens.null" in diagnostic.issue_codes
    assert "prompt_cache_key.null" in diagnostic.issue_codes
    assert summary["request_shape_valid_bucket"] == "zero"
    assert summary["optional_null_field_count_bucket"] == "multiple"
    assert summary["optional_null_field_name_buckets"] == [
        "max_output_tokens",
        "prompt_cache_key",
    ]
    assert summary["max_output_tokens_null_bucket"] == "one"


def test_request_shape_diagnostic_flags_unsupported_parameters_without_raw_text() -> None:
    request = OpenAIJudgeClient.build_request(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="private developer prompt should not be reported",
        user_context="private user context should not be reported",
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=500,
        prompt_cache_key="judge:github:v1",
    )
    request["temperature"] = 0

    summary = summarize_responses_request_shape(request)
    rendered = repr(summary)

    assert summary["request_shape_valid_bucket"] == "zero"
    assert summary["request_shape_issue_buckets"] == ["top_level.unsupported_parameter"]
    assert "private developer prompt" not in rendered
    assert "private user context" not in rendered


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
        json_schema={"type": "object", "properties": {}, "required": [], "additionalProperties": False},
        max_output_tokens=None,
        prompt_cache_key=None,
    )

    assert response == {"status": "completed", "output_text": "{}"}
    assert fake_client.responses.request is not None
    assert fake_client.responses.request["tools"] == []


@pytest.mark.asyncio
async def test_openai_client_rejects_bad_schema_before_sdk_call() -> None:
    class FakeResponses:
        def __init__(self) -> None:
            self.calls = 0

        async def create(self, **request):
            self.calls += 1
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

    with pytest.raises(OpenAIRequestShapeError) as exc_info:
        await client.create_structured_response(
            model="gpt-5.4-mini",
            reasoning_effort="low",
            developer_prompt="developer",
            user_context="user",
            json_schema={
                "anyOf": [
                    {"type": "object", "properties": {}, "required": [], "additionalProperties": False}
                ]
            },
            max_output_tokens=None,
            prompt_cache_key=None,
        )

    assert fake_client.responses.calls == 0
    assert str(exc_info.value) == "invalid_request_shape"
    assert "schema.root_not_object" in exc_info.value.issue_codes
    assert "schema.root_anyof" in exc_info.value.issue_codes
