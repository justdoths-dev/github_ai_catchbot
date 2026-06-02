from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.services.judge_openai.openai_client import OpenAIJudgeClient
from src.services.judge_openai.request_shape import (
    LOCKED_HOT_PATH_MODEL,
    summarize_responses_request_shape,
)
from src.services.judge_openai.service import JudgeOpenAIService


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_sdk_client_compatibility_diagnostic"
REPORT_TYPE = "judge_openai_sdk_client_compatibility_diagnostic_v1"
STATUS_PASSED = "judge_openai_sdk_client_compatibility_diagnostic_passed"
STATUS_FAILED = "blocked_judge_openai_sdk_client_compatibility_diagnostic_failed"
DEFAULT_REASONING_EFFORT = "low"
DEFAULT_PROMPT_CACHE_KEY = "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1"
PLACEHOLDER_API_KEY_PARTS = ("sk", "local", "diagnostic", "placeholder")
PLACEHOLDER_ORGANIZATION = "org_local_diagnostic_placeholder"
PLACEHOLDER_PROJECT = "proj_local_diagnostic_placeholder"
LOCAL_BASE_URL_PARTS = ("https://", "local", ".invalid", "/v1")


@dataclass(slots=True, frozen=True)
class SdkImports:
    async_openai: Any | None
    httpx: Any | None
    sdk_version_present: bool


@dataclass(slots=True)
class CapturedRequest:
    invoked: bool = False
    method: str | None = None
    path: str | None = None
    authorization_matches_placeholder: bool = False
    json_body: Mapping[str, Any] | None = None
    issue_code: str | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="No-network judge-openai SDK/client compatibility diagnostic."
    )
    parser.add_argument("--model", default=LOCKED_HOT_PATH_MODEL)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    parser.add_argument("--max-output-tokens", type=int, default=500)
    parser.add_argument("--omit-prompt-cache-key", action="store_true")
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def generate_report(
    *,
    model: str = LOCKED_HOT_PATH_MODEL,
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    max_output_tokens: int | None = 500,
    prompt_cache_key: str | None = DEFAULT_PROMPT_CACHE_KEY,
    sdk_loader: Callable[[], SdkImports] | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        _generate_report_async(
            model=model,
            reasoning_effort=reasoning_effort,
            max_output_tokens=max_output_tokens,
            prompt_cache_key=prompt_cache_key,
            sdk_loader=sdk_loader or _load_sdk,
        )
    )


async def _generate_report_async(
    *,
    model: str,
    reasoning_effort: str,
    max_output_tokens: int | None,
    prompt_cache_key: str | None,
    sdk_loader: Callable[[], SdkImports],
) -> dict[str, Any]:
    request = OpenAIJudgeClient.build_request(
        model=model,
        reasoning_effort=reasoning_effort,
        developer_prompt="diagnostic developer prompt placeholder",
        user_context="diagnostic user context placeholder",
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=max_output_tokens,
        prompt_cache_key=prompt_cache_key,
    )
    report = _base_report()
    checks_failed: list[str] = []

    try:
        sdk = sdk_loader()
    except Exception:
        checks_failed.append("sdk.import_unavailable")
        _finalize_report(report, checks_failed=checks_failed)
        return report

    report["sdk_import_bucket"] = "one" if sdk.async_openai is not None and sdk.httpx is not None else "zero"
    report["sdk_version_present_bucket"] = "one" if sdk.sdk_version_present else "zero"
    if sdk.async_openai is None or sdk.httpx is None:
        checks_failed.append("sdk.import_unavailable")
        _finalize_report(report, checks_failed=checks_failed)
        return report

    captured = CapturedRequest()
    try:
        transport = sdk.httpx.MockTransport(_mock_handler(captured, sdk.httpx))
        async_client = sdk.httpx.AsyncClient(transport=transport, timeout=1.0, trust_env=False)
    except Exception:
        checks_failed.append("mock_transport.unavailable")
        _finalize_report(report, checks_failed=checks_failed)
        return report

    client = None
    try:
        client = sdk.async_openai(
            api_key=_placeholder_api_key(),
            organization=PLACEHOLDER_ORGANIZATION,
            project=PLACEHOLDER_PROJECT,
            base_url=_local_base_url(),
            http_client=async_client,
            max_retries=0,
        )
        report["async_openai_constructor_bucket"] = "one"
    except Exception:
        checks_failed.append("async_openai.constructor")
        await _safe_aclose(async_client)
        _finalize_report(report, checks_failed=checks_failed)
        return report

    responses = getattr(client, "responses", None)
    create = getattr(responses, "create", None)
    report["responses_resource_bucket"] = "one" if responses is not None else "zero"
    report["responses_create_callable_bucket"] = "one" if callable(create) else "zero"
    if not callable(create):
        checks_failed.append("responses.create_unavailable")
        await _safe_aclose(client)
        await _safe_aclose(async_client)
        _finalize_report(report, checks_failed=checks_failed)
        return report

    try:
        await create(**request)
        report["sdk_response_parse_bucket"] = "one"
    except Exception:
        report["sdk_response_parse_bucket"] = "zero"
        checks_failed.append("responses.create_or_parse")
    finally:
        await _safe_aclose(client)
        await _safe_aclose(async_client)

    _merge_captured_request_report(report, captured)
    if captured.issue_code is not None:
        checks_failed.append(captured.issue_code)
    if not captured.invoked:
        checks_failed.append("mock_transport.not_invoked")
    if captured.method != "POST":
        checks_failed.append("http.method")
    if captured.path != "/v1/responses":
        checks_failed.append("responses.endpoint")
    if not captured.authorization_matches_placeholder:
        checks_failed.append("authorization.placeholder_header")
    if captured.json_body is None:
        checks_failed.append("serialized_body.missing")
    else:
        serialized_summary = summarize_responses_request_shape(captured.json_body)
        _merge_serialized_shape_report(report, serialized_summary)
        if serialized_summary["request_shape_valid_bucket"] != "one":
            checks_failed.append("serialized_request_shape.invalid")

    _finalize_report(report, checks_failed=checks_failed)
    return report


def _load_sdk() -> SdkImports:
    try:
        import httpx  # type: ignore[import-not-found]
        import openai  # type: ignore[import-not-found]
        from openai import AsyncOpenAI  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return SdkImports(async_openai=None, httpx=None, sdk_version_present=False)
    return SdkImports(
        async_openai=AsyncOpenAI,
        httpx=httpx,
        sdk_version_present=bool(getattr(openai, "__version__", None)),
    )


def _mock_handler(captured: CapturedRequest, httpx_module: Any) -> Callable[[Any], Any]:
    def handler(request: Any) -> Any:
        captured.invoked = True
        captured.method = str(getattr(request, "method", "")).upper() or None
        captured.path = str(getattr(getattr(request, "url", None), "path", "")) or None
        headers = getattr(request, "headers", {})
        authorization = headers.get("authorization") if hasattr(headers, "get") else None
        captured.authorization_matches_placeholder = authorization == f"Bearer {_placeholder_api_key()}"
        try:
            body = getattr(request, "content", b"")
            if isinstance(body, str):
                body = body.encode("utf-8")
            parsed = json.loads(body.decode("utf-8"))
            if isinstance(parsed, Mapping):
                captured.json_body = parsed
            else:
                captured.issue_code = "serialized_body.not_object"
        except Exception:
            captured.issue_code = "serialized_body.json_invalid"
        response_body = _fake_response_body(captured.json_body)
        try:
            return httpx_module.Response(200, json=response_body, request=request)
        except TypeError:
            return httpx_module.Response(200, json=response_body)

    return handler


def _fake_response_body(request_body: Mapping[str, Any] | None) -> dict[str, Any]:
    model = request_body.get("model") if request_body is not None else LOCKED_HOT_PATH_MODEL
    reasoning = request_body.get("reasoning") if request_body is not None else {"effort": "low"}
    text = request_body.get("text") if request_body is not None else {"format": {"type": "text"}}
    return {
        "id": "resp_local_diagnostic",
        "object": "response",
        "created_at": 0,
        "status": "completed",
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "max_output_tokens": request_body.get("max_output_tokens") if request_body is not None else None,
        "metadata": {},
        "model": model,
        "output": [],
        "parallel_tool_calls": False,
        "previous_response_id": None,
        "reasoning": reasoning,
        "store": False,
        "temperature": None,
        "text": text,
        "tool_choice": "none",
        "tools": [],
        "top_p": None,
        "truncation": "disabled",
        "usage": None,
        "user": None,
    }


def _placeholder_api_key() -> str:
    return "-".join(PLACEHOLDER_API_KEY_PARTS)


def _local_base_url() -> str:
    return "".join(LOCAL_BASE_URL_PARTS)


async def _safe_aclose(value: Any) -> None:
    close = getattr(value, "aclose", None)
    if close is None:
        close = getattr(value, "close", None)
    if close is None:
        return
    try:
        result = close()
        if hasattr(result, "__await__"):
            await result
    except Exception:
        return


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_FAILED,
        "sdk_import_bucket": "zero",
        "sdk_version_present_bucket": "zero",
        "async_openai_constructor_bucket": "zero",
        "responses_resource_bucket": "zero",
        "responses_create_callable_bucket": "zero",
        "mock_transport_invoked_bucket": "zero",
        "network_transport_blocked_bucket": "one",
        "http_method_bucket": "zero",
        "responses_endpoint_bucket": "zero",
        "authorization_header_placeholder_bucket": "zero",
        "authorization_header_raw_emitted": False,
        "serialized_request_shape_valid_bucket": "zero",
        "serialized_request_shape_issue_count_bucket": "zero",
        "serialized_request_shape_issue_buckets": [],
        "model_bucket": "zero",
        "reasoning_effort_bucket": "zero",
        "text_format_json_schema_bucket": "zero",
        "strict_schema_bucket": "zero",
        "tools_bucket": "zero",
        "prompt_cache_key_present_bucket": "zero",
        "max_output_tokens_present_bucket": "zero",
        "sdk_response_parse_bucket": "zero",
        "openai_call_attempted": False,
        "live_openai_call_attempted": False,
        "openai_key_file_read_bucket": "zero",
        "runtime_env_read": False,
        "database_write_attempted": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "analysis_validator_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _merge_captured_request_report(report: dict[str, Any], captured: CapturedRequest) -> None:
    report["mock_transport_invoked_bucket"] = "one" if captured.invoked else "zero"
    report["http_method_bucket"] = _method_bucket(captured.method)
    report["responses_endpoint_bucket"] = "one" if captured.path == "/v1/responses" else "zero"
    report["authorization_header_placeholder_bucket"] = (
        "one" if captured.authorization_matches_placeholder else "zero"
    )


def _merge_serialized_shape_report(report: dict[str, Any], summary: Mapping[str, Any]) -> None:
    report["serialized_request_shape_valid_bucket"] = summary["request_shape_valid_bucket"]
    report["serialized_request_shape_issue_count_bucket"] = summary["request_shape_issue_count_bucket"]
    report["serialized_request_shape_issue_buckets"] = list(summary["request_shape_issue_buckets"])
    report["model_bucket"] = summary["model_bucket"]
    report["reasoning_effort_bucket"] = summary["reasoning_effort_bucket"]
    report["text_format_json_schema_bucket"] = summary["text_format_json_schema_bucket"]
    report["strict_schema_bucket"] = summary["strict_schema_bucket"]
    report["tools_bucket"] = summary["tools_bucket"]
    report["prompt_cache_key_present_bucket"] = summary["prompt_cache_key_present_bucket"]
    report["max_output_tokens_present_bucket"] = summary["max_output_tokens_present_bucket"]


def _finalize_report(report: dict[str, Any], *, checks_failed: list[str]) -> None:
    unique_checks = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = unique_checks
    report["contract_status"] = STATUS_PASSED if not unique_checks else STATUS_FAILED


def _method_bucket(method: str | None) -> str:
    if method is None:
        return "zero"
    return "post" if method == "POST" else "other"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = generate_report(
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        max_output_tokens=args.max_output_tokens,
        prompt_cache_key=None if args.omit_prompt_cache_key else DEFAULT_PROMPT_CACHE_KEY,
    )
    print(json.dumps(report, sort_keys=True))
    return 0 if report["contract_status"] == STATUS_PASSED else 1


if __name__ == "__main__":
    raise SystemExit(main())
