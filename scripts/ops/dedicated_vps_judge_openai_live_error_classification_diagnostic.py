from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.services.judge_openai.openai_client import OpenAIJudgeClient  # noqa: E402
from src.services.judge_openai.request_shape import (  # noqa: E402
    LOCKED_HOT_PATH_MODEL,
    summarize_responses_request_shape,
)
from src.services.judge_openai.service import JudgeOpenAIService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_live_error_classification_diagnostic"
REPORT_TYPE = "judge_openai_live_error_classification_diagnostic_v1"

STATUS_PREFLIGHT_PASSED = "judge_openai_live_error_classification_preflight_passed"
STATUS_LIVE_SUCCEEDED = "judge_openai_live_error_classification_approved_live_succeeded"
STATUS_NOT_APPROVED = "blocked_judge_openai_live_error_classification_not_approved"
STATUS_PREFLIGHT_FAILED = "blocked_judge_openai_live_error_classification_preflight_failed"
STATUS_LIVE_FAILED = "blocked_judge_openai_live_error_classification_live_failed"
STATUS_RAW_VALUE_EMISSION = "blocked_judge_openai_live_error_classification_raw_value_emission"

DEFAULT_REASONING_EFFORT = "low"
DEFAULT_PROMPT_CACHE_KEY = "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1"
DEFAULT_MAX_OUTPUT_TOKENS = 500
DEFAULT_TIMEOUT_SECONDS = 10.0

DIAGNOSTIC_DEVELOPER_PROMPT = "diagnostic judge-output schema probe"
DIAGNOSTIC_USER_CONTEXT = "diagnostic candidate evidence placeholder"

OPENAI_ENV_KEYS = frozenset({"OPENAI_API_KEY", "OPENAI_API_KEY_FILE", "OPENAI_PROJECT"})
SCHEMA_HINT_RE = re.compile(r"(json_schema|schema|response_format|structured)", re.IGNORECASE)
MODEL_HINT_RE = re.compile(r"(model|deployment)", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SdkImports:
    async_openai: Any | None


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
SdkLoader = Callable[[], SdkImports]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated judge-openai live error classification diagnostic. "
            "Default mode is local-only and makes no live OpenAI call."
        )
    )
    parser.add_argument("--approve-live-openai", action="store_true")
    parser.add_argument("--approve-key-read", action="store_true")
    parser.add_argument("--max-live-calls", type=int, default=0)
    parser.add_argument("--runtime-env-path")
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def generate_report(
    *,
    approve_live_openai: bool = False,
    approve_key_read: bool = False,
    max_live_calls: int = 0,
    runtime_env_path: str | Path | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    sdk_loader: SdkLoader | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            approve_live_openai=approve_live_openai,
            approve_key_read=approve_key_read,
            max_live_calls=max_live_calls,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            sdk_loader=sdk_loader,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


async def generate_report_async(
    *,
    approve_live_openai: bool = False,
    approve_key_read: bool = False,
    max_live_calls: int = 0,
    runtime_env_path: str | Path | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    sdk_loader: SdkLoader | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report(
        approve_live_openai=approve_live_openai,
        approve_key_read=approve_key_read,
        max_live_calls=max_live_calls,
    )
    raw_values = _raw_values(forbidden_raw_values)
    raw_values.update(_raw_values(DIAGNOSTIC_DEVELOPER_PROMPT, DIAGNOSTIC_USER_CONTEXT))

    request = _build_diagnostic_request()
    request_summary = summarize_responses_request_shape(request)
    _merge_request_shape_report(report, request_summary)
    if request_summary["request_shape_valid_bucket"] != "one":
        _set_status(report, STATUS_PREFLIGHT_FAILED, "request_shape.invalid")
        return _finalize(report, raw_values, exit_code=1)

    if not _has_full_live_approval(
        approve_live_openai=approve_live_openai,
        approve_key_read=approve_key_read,
        max_live_calls=max_live_calls,
    ):
        if not approve_live_openai and not approve_key_read and max_live_calls == 0:
            _set_status(report, STATUS_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_all")
        return _finalize(report, raw_values, exit_code=1)

    values: Mapping[str, str]
    if runtime_env_path is not None:
        raw_values.update(_raw_values(runtime_env_path))
        try:
            values = (
                runtime_env_reader(runtime_env_path)
                if runtime_env_reader is not None
                else parse_runtime_env_file(runtime_env_path)
            )
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_LIVE_FAILED, "runtime_env.read")
            return _finalize(report, raw_values, exit_code=1)
    else:
        values = _load_process_openai_values()

    raw_values.update(_raw_values(*(values.get(key, "") for key in OPENAI_ENV_KEYS)))
    key_material = _resolve_key_material(report=report, values=values, raw_values=raw_values)
    if key_material is None:
        return _finalize(report, raw_values, exit_code=1)
    api_key, project = key_material

    try:
        sdk = (sdk_loader or _load_sdk)()
    except Exception:
        _set_status(report, STATUS_LIVE_FAILED, "sdk.import_unavailable")
        return _finalize(report, raw_values, exit_code=1)

    report["sdk_import_bucket"] = "one" if sdk.async_openai is not None else "zero"
    if sdk.async_openai is None:
        _set_status(report, STATUS_LIVE_FAILED, "sdk.import_unavailable")
        return _finalize(report, raw_values, exit_code=1)

    client = None
    try:
        client = sdk.async_openai(
            api_key=api_key,
            project=project or None,
            timeout=DEFAULT_TIMEOUT_SECONDS,
            max_retries=0,
        )
        report["async_openai_constructor_bucket"] = "one"
    except Exception:
        _set_status(report, STATUS_LIVE_FAILED, "async_openai.constructor")
        return _finalize(report, raw_values, exit_code=1)

    responses = getattr(client, "responses", None)
    create = getattr(responses, "create", None)
    report["responses_create_callable_bucket"] = "one" if callable(create) else "zero"
    if not callable(create):
        await _safe_aclose(client)
        _set_status(report, STATUS_LIVE_FAILED, "responses.create_unavailable")
        return _finalize(report, raw_values, exit_code=1)

    started = time.monotonic()
    try:
        report["openai_call_attempted"] = True
        report["live_openai_call_attempted"] = True
        report["live_openai_call_attempted_bucket"] = "one"
        response = await create(**request)
        report["live_openai_call_completed_bucket"] = "one"
        report["live_result_class_bucket"] = "success"
        report["http_status_bucket"] = "2xx"
        report["response_parse_bucket"] = "one"
        report["structured_output_observed_bucket"] = _structured_output_bucket(response)
        report["usage_present_bucket"] = _usage_present_bucket(response)
        report["latency_ms_present_bucket"] = "one" if time.monotonic() >= started else "zero"
        _set_status(report, STATUS_LIVE_SUCCEEDED)
        return _finalize(report, raw_values, exit_code=0)
    except Exception as exc:
        classification = _classify_openai_exception(exc)
        report.update(classification)
        report["live_openai_call_completed_bucket"] = "zero"
        report["latency_ms_present_bucket"] = "one" if time.monotonic() >= started else "zero"
        _set_status(report, STATUS_LIVE_FAILED, f"openai.live_call.{report['live_result_class_bucket']}")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        await _safe_aclose(client)


def _build_diagnostic_request() -> dict[str, Any]:
    return OpenAIJudgeClient.build_request(
        model=LOCKED_HOT_PATH_MODEL,
        reasoning_effort=DEFAULT_REASONING_EFFORT,
        developer_prompt=DIAGNOSTIC_DEVELOPER_PROMPT,
        user_context=DIAGNOSTIC_USER_CONTEXT,
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
        prompt_cache_key=DEFAULT_PROMPT_CACHE_KEY,
    )


def _load_sdk() -> SdkImports:
    try:
        from openai import AsyncOpenAI  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return SdkImports(async_openai=None)
    return SdkImports(async_openai=AsyncOpenAI)


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if key in OPENAI_ENV_KEYS:
            values[key] = _strip_optional_quotes(raw_value)
    return values


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _load_process_openai_values() -> dict[str, str]:
    return {key: os.environ.get(key, "") for key in OPENAI_ENV_KEYS}


def _resolve_key_material(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str | None] | None:
    direct_key = str(values.get("OPENAI_API_KEY", "")).strip()
    key_file = str(values.get("OPENAI_API_KEY_FILE", "")).strip()
    project = str(values.get("OPENAI_PROJECT", "")).strip()

    report["openai_project_present_bucket"] = "one" if project else "zero"
    if direct_key and key_file:
        report["openai_key_source_bucket"] = "both_conflict"
        _set_status(report, STATUS_LIVE_FAILED, "openai_key.source_conflict")
        return None
    if direct_key:
        report["openai_key_source_bucket"] = "env"
        report["openai_key_read_bucket"] = "one"
        return direct_key, project or None
    if key_file:
        report["openai_key_source_bucket"] = "file"
        try:
            key_value = Path(key_file).read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            _set_status(report, STATUS_LIVE_FAILED, "openai_key_file.read")
            return None
        raw_values.update(_raw_values(key_value))
        report["openai_key_file_read_bucket"] = "one"
        if not key_value:
            report["openai_key_source_bucket"] = "missing"
            _set_status(report, STATUS_LIVE_FAILED, "openai_key.missing")
            return None
        report["openai_key_read_bucket"] = "one"
        return key_value, project or None

    report["openai_key_source_bucket"] = "missing"
    _set_status(report, STATUS_LIVE_FAILED, "openai_key.missing")
    return None


def _classify_openai_exception(exc: Exception) -> dict[str, Any]:
    status_code = getattr(exc, "status_code", None)
    status_int = status_code if isinstance(status_code, int) else None
    name = type(exc).__name__
    hint = _exception_hint(exc)

    result_bucket = "other_sanitized"
    error_type_bucket = "other"

    if name == "APITimeoutError":
        result_bucket = "timeout"
        error_type_bucket = "api_timeout_error"
    elif name == "APIConnectionError":
        result_bucket = "api_connection_error"
        error_type_bucket = "api_connection_error"
    elif name == "RateLimitError" or status_int == 429:
        result_bucket = "rate_limit"
        error_type_bucket = "rate_limit_error"
    elif name == "AuthenticationError" or status_int == 401:
        result_bucket = "authentication"
        error_type_bucket = "authentication_error"
    elif name == "PermissionDeniedError" or status_int == 403:
        result_bucket = "model_access" if MODEL_HINT_RE.search(hint) else "permission"
        error_type_bucket = "permission_error"
    elif name == "NotFoundError" or status_int == 404:
        result_bucket = "model_access" if MODEL_HINT_RE.search(hint) else "invalid_request"
        error_type_bucket = "invalid_request_error"
    elif name in {"BadRequestError", "UnprocessableEntityError"} or status_int in {400, 422}:
        result_bucket = "schema_rejected" if SCHEMA_HINT_RE.search(hint) else "invalid_request"
        error_type_bucket = "invalid_request_error"
    elif name == "InternalServerError" or (status_int is not None and status_int >= 500):
        result_bucket = "api_status_error"
        error_type_bucket = "server_error"
    elif name == "APIStatusError" or (status_int is not None and status_int >= 400):
        result_bucket = "api_status_error"
        error_type_bucket = _status_error_type_bucket(status_int)

    return {
        "live_result_class_bucket": result_bucket,
        "http_status_bucket": _http_status_bucket(status_int),
        "openai_error_type_bucket": error_type_bucket,
        "response_parse_bucket": "zero",
        "structured_output_observed_bucket": "zero",
        "usage_present_bucket": "zero",
    }


def _exception_hint(exc: Exception) -> str:
    parts: list[str] = [type(exc).__name__]
    for attr in ("type", "code"):
        value = getattr(exc, attr, None)
        if isinstance(value, str):
            parts.append(value)
    return " ".join(parts)


def _status_error_type_bucket(status_code: int | None) -> str:
    if status_code == 401:
        return "authentication_error"
    if status_code == 403:
        return "permission_error"
    if status_code == 429:
        return "rate_limit_error"
    if status_code is not None and status_code >= 500:
        return "server_error"
    if status_code is not None and status_code >= 400:
        return "invalid_request_error"
    return "other"


def _http_status_bucket(status_code: int | None) -> str:
    if status_code is None:
        return "zero"
    if 200 <= status_code <= 299:
        return "2xx"
    if status_code in {400, 401, 403, 404, 408, 409, 422, 429}:
        return str(status_code)
    if 500 <= status_code <= 599:
        return "5xx"
    return "other"


def _structured_output_bucket(response: Any) -> str:
    output_text = _read_response_value(response, "output_text")
    if isinstance(output_text, str) and output_text.strip():
        return "one"
    output = _read_response_value(response, "output")
    if isinstance(output, Sequence) and not isinstance(output, (str, bytes)) and output:
        return "one"
    return "zero"


def _usage_present_bucket(response: Any) -> str:
    usage = _read_response_value(response, "usage")
    return "one" if usage is not None else "zero"


def _read_response_value(response: Any, key: str) -> Any:
    if isinstance(response, Mapping):
        return response.get(key)
    return getattr(response, key, None)


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


def _base_report(
    *,
    approve_live_openai: bool,
    approve_key_read: bool,
    max_live_calls: int,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_APPROVED,
        "approval_live_openai_bucket": "one" if approve_live_openai else "zero",
        "approval_key_read_bucket": "one" if approve_key_read else "zero",
        "max_live_calls_bucket": _bucket_count(max_live_calls),
        "runtime_env_read": False,
        "openai_key_source_bucket": "zero",
        "openai_key_read_bucket": "zero",
        "openai_key_file_read_bucket": "zero",
        "openai_project_present_bucket": "zero",
        "request_shape_valid_bucket": "zero",
        "request_shape_issue_count_bucket": "zero",
        "request_shape_issue_buckets": [],
        "sdk_import_bucket": "zero",
        "async_openai_constructor_bucket": "zero",
        "responses_create_callable_bucket": "zero",
        "openai_call_attempted": False,
        "live_openai_call_attempted": False,
        "live_openai_call_attempted_bucket": "zero",
        "live_openai_call_completed_bucket": "zero",
        "live_result_class_bucket": "zero",
        "http_status_bucket": "zero",
        "openai_error_type_bucket": "zero",
        "response_parse_bucket": "zero",
        "structured_output_observed_bucket": "zero",
        "usage_present_bucket": "zero",
        "latency_ms_present_bucket": "zero",
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


def _merge_request_shape_report(report: dict[str, Any], summary: Mapping[str, Any]) -> None:
    report["request_shape_valid_bucket"] = summary["request_shape_valid_bucket"]
    report["request_shape_issue_count_bucket"] = summary["request_shape_issue_count_bucket"]
    report["request_shape_issue_buckets"] = list(summary["request_shape_issue_buckets"])


def _has_full_live_approval(
    *,
    approve_live_openai: bool,
    approve_key_read: bool,
    max_live_calls: int,
) -> bool:
    return approve_live_openai and approve_key_read and max_live_calls == 1


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def _raw_values(*values: object) -> set[str]:
    raw: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value)
        if len(text) >= 6:
            raw.add(text)
    return raw


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    public_literals = {
        SCHEMA_VERSION,
        SCRIPT_NAME,
        REPORT_TYPE,
        STATUS_PREFLIGHT_PASSED,
        STATUS_LIVE_SUCCEEDED,
        STATUS_NOT_APPROVED,
        STATUS_PREFLIGHT_FAILED,
        STATUS_LIVE_FAILED,
        STATUS_RAW_VALUE_EMISSION,
    }
    return any(value not in public_literals and value in rendered for value in raw_values)


def _finalize(report: dict[str, Any], raw_values: set[str], *, exit_code: int) -> ScriptResult:
    if _report_contains_raw_values(report, {value for value in raw_values if len(value) >= 6}):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = generate_report(
        approve_live_openai=args.approve_live_openai,
        approve_key_read=args.approve_key_read,
        max_live_calls=args.max_live_calls,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
