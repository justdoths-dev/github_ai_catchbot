from __future__ import annotations

import importlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_judge_openai_sdk_client_compatibility_diagnostic.py"
)
PRIVATE_MODEL_VALUE = "private-model-value"
PRIVATE_PROMPT_VALUE = "diagnostic developer prompt placeholder"
PRIVATE_CONTEXT_VALUE = "diagnostic user context placeholder"
PLACEHOLDER_KEY_VALUE = "-".join(("sk", "local", "diagnostic", "placeholder"))


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_sdk_client_compatibility_diagnostic"
    )


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_report_passes_with_no_network_mock_sdk_path() -> None:
    report = _module().generate_report(sdk_loader=_fake_sdk_loader())

    assert report["contract_status"] == "judge_openai_sdk_client_compatibility_diagnostic_passed"
    assert report["sdk_import_bucket"] == "one"
    assert report["sdk_version_present_bucket"] == "one"
    assert report["async_openai_constructor_bucket"] == "one"
    assert report["responses_resource_bucket"] == "one"
    assert report["responses_create_callable_bucket"] == "one"
    assert report["mock_transport_invoked_bucket"] == "one"
    assert report["network_transport_blocked_bucket"] == "one"
    assert report["http_method_bucket"] == "post"
    assert report["responses_endpoint_bucket"] == "one"
    assert report["authorization_header_placeholder_bucket"] == "one"
    assert report["authorization_header_raw_emitted"] is False
    assert report["serialized_request_shape_valid_bucket"] == "one"
    assert report["serialized_request_shape_issue_count_bucket"] == "zero"
    assert report["serialized_request_shape_issue_buckets"] == []
    assert report["model_bucket"] == "locked_hot_path"
    assert report["reasoning_effort_bucket"] == "low"
    assert report["text_format_json_schema_bucket"] == "one"
    assert report["strict_schema_bucket"] == "one"
    assert report["tools_bucket"] == "zero"
    assert report["prompt_cache_key_present_bucket"] == "zero"
    if "prompt_cache_key_presence_bucket" in report:
        assert report["prompt_cache_key_presence_bucket"] == "zero"
    assert report["max_output_tokens_present_bucket"] == "one"
    assert report["openai_call_attempted"] is False
    assert report["live_openai_call_attempted"] is False
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["runtime_env_read"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False
    assert report["analysis_validator_started"] is False
    assert report["policy_engine_started"] is False
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []


def test_invalid_local_request_path_fails_with_sanitized_issue_codes() -> None:
    report = _module().generate_report(model=PRIVATE_MODEL_VALUE, sdk_loader=_fake_sdk_loader())
    rendered = json.dumps(report, sort_keys=True)

    assert report["contract_status"] == (
        "blocked_judge_openai_sdk_client_compatibility_diagnostic_failed"
    )
    assert report["mock_transport_invoked_bucket"] == "one"
    assert report["serialized_request_shape_valid_bucket"] == "zero"
    assert report["serialized_request_shape_issue_count_bucket"] == "one"
    assert report["serialized_request_shape_issue_buckets"] == ["model.outside_locked_set"]
    assert report["model_bucket"] == "other"
    assert report["checks_failed"] == ["serialized_request_shape.invalid"]
    assert PRIVATE_MODEL_VALUE not in rendered
    assert PRIVATE_PROMPT_VALUE not in rendered
    assert PRIVATE_CONTEXT_VALUE not in rendered
    assert PLACEHOLDER_KEY_VALUE not in rendered


def test_missing_sdk_fails_sanitized_without_traceback() -> None:
    report = _module().generate_report(sdk_loader=lambda: _module().SdkImports(None, None, False))
    rendered = json.dumps(report, sort_keys=True)

    assert report["contract_status"] == (
        "blocked_judge_openai_sdk_client_compatibility_diagnostic_failed"
    )
    assert report["sdk_import_bucket"] == "zero"
    assert report["async_openai_constructor_bucket"] == "zero"
    assert report["responses_resource_bucket"] == "zero"
    assert report["responses_create_callable_bucket"] == "zero"
    assert report["mock_transport_invoked_bucket"] == "zero"
    assert report["checks_failed"] == ["sdk.import_unavailable"]
    assert "Traceback" not in rendered
    assert PLACEHOLDER_KEY_VALUE not in rendered


def test_responses_create_unavailable_fails_sanitized() -> None:
    report = _module().generate_report(sdk_loader=_fake_sdk_loader(with_create=False))

    assert report["contract_status"] == (
        "blocked_judge_openai_sdk_client_compatibility_diagnostic_failed"
    )
    assert report["sdk_import_bucket"] == "one"
    assert report["async_openai_constructor_bucket"] == "one"
    assert report["responses_resource_bucket"] == "one"
    assert report["responses_create_callable_bucket"] == "zero"
    assert report["mock_transport_invoked_bucket"] == "zero"
    assert report["checks_failed"] == ["responses.create_unavailable"]


def test_cli_outputs_json_without_raw_runtime_or_secret_values() -> None:
    private_key = "private-" + "openai-" + "api-" + "key"
    private_key_file = "/private/" + "openai/" + "key/" + "file"
    private_database_url = "postgresql://" + "private"
    private_redis_url = "redis://" + "private"
    env = os.environ.copy()
    env["OPENAI_API_KEY"] = private_key
    env["OPENAI_API_KEY_FILE"] = private_key_file
    env["DATABASE_URL"] = private_database_url
    env["REDIS_URL"] = private_redis_url

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    report = json.loads(result.stdout)
    installed_sdk_available = (
        importlib.util.find_spec("openai") is not None
        and importlib.util.find_spec("httpx") is not None
    )
    if installed_sdk_available:
        assert result.returncode == 0
        assert report["contract_status"] == (
            "judge_openai_sdk_client_compatibility_diagnostic_passed"
        )
        assert report["mock_transport_invoked_bucket"] == "one"
        assert report["serialized_request_shape_valid_bucket"] == "one"
        assert report["serialized_request_shape_issue_count_bucket"] == "zero"
        assert report["checks_failed"] == []
    else:
        assert result.returncode == 1
        assert report["contract_status"] == (
            "blocked_judge_openai_sdk_client_compatibility_diagnostic_failed"
        )
        assert report["sdk_import_bucket"] == "zero"
        assert report["checks_failed"] == ["sdk.import_unavailable"]
    assert report["openai_call_attempted"] is False
    assert report["live_openai_call_attempted"] is False
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["runtime_env_read"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["raw_values_emitted"] is False
    combined_output = result.stdout + result.stderr
    assert private_key not in combined_output
    assert private_key_file not in combined_output
    assert private_database_url not in combined_output
    assert private_redis_url not in combined_output
    assert PLACEHOLDER_KEY_VALUE not in combined_output


def _fake_sdk_loader(*, with_create: bool = True):
    def loader():
        return _module().SdkImports(
            async_openai=_fake_async_openai_class(with_create=with_create),
            httpx=_FakeHttpx,
            sdk_version_present=True,
        )

    return loader


def _fake_async_openai_class(*, with_create: bool):
    class FakeAsyncOpenAI:
        def __init__(
            self,
            *,
            api_key: str,
            organization: str | None,
            project: str | None,
            base_url: str,
            http_client: Any,
            max_retries: int,
        ) -> None:
            self._api_key = api_key
            self._http_client = http_client
            self.responses = _FakeResponses(api_key, http_client) if with_create else object()

        async def aclose(self) -> None:
            return None

    return FakeAsyncOpenAI


class _FakeResponses:
    def __init__(self, api_key: str, http_client: Any) -> None:
        self._api_key = api_key
        self._http_client = http_client

    async def create(self, **request: Any) -> dict[str, Any]:
        outbound = _FakeRequest(
            method="POST",
            path="/v1/responses",
            headers={"authorization": f"Bearer {self._api_key}"},
            content=json.dumps(request).encode("utf-8"),
        )
        response = self._http_client.transport.handler(outbound)
        return json.loads(response.content.decode("utf-8"))


class _FakeHttpx:
    class MockTransport:
        def __init__(self, handler: Any) -> None:
            self.handler = handler

    class AsyncClient:
        def __init__(self, *, transport: Any, timeout: float, trust_env: bool) -> None:
            self.transport = transport
            self.trust_env = trust_env

        async def aclose(self) -> None:
            return None

    class Response:
        def __init__(self, status_code: int, *, json: dict[str, Any], **kwargs: Any) -> None:
            self.status_code = status_code
            self.content = __import__("json").dumps(json).encode("utf-8")


class _FakeRequest:
    def __init__(
        self,
        *,
        method: str,
        path: str,
        headers: dict[str, str],
        content: bytes,
    ) -> None:
        self.method = method
        self.url = _FakeUrl(path)
        self.headers = headers
        self.content = content


class _FakeUrl:
    def __init__(self, path: str) -> None:
        self.path = path
