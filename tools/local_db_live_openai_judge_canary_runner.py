from __future__ import annotations

import argparse
import copy
import inspect
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from tools import local_db_restricted_openai_judge_canary_runner as restricted_runner


SCHEMA_VERSION = "local_db_live_openai_judge_canary_v1"
DIAGNOSTIC_SCHEMA_VERSION = "sanitized_openai_responses_request_contract_diagnostic_v1"
DEFAULT_OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
ALLOWED_OPENAI_API_KEY_ENVS = frozenset({DEFAULT_OPENAI_API_KEY_ENV})
ALLOWED_OPENAI_MODELS = frozenset({"gpt-5.4-mini"})
ALLOWED_REASONING_EFFORTS = frozenset({"none", "minimal", "low", "medium", "high", "xhigh"})
ALLOWED_OPENAI_REQUEST_TOP_LEVEL_KEYS = frozenset(
    {
        "model",
        "reasoning",
        "input",
        "text",
        "tools",
        "max_output_tokens",
        "prompt_cache_key",
    }
)
DEFAULT_MAX_OUTPUT_TOKENS = 800
HARD_MAX_OUTPUT_TOKENS = 1200
OPENAI_STRUCTURED_OUTPUT_MAX_OBJECT_DEPTH = 10
OPENAI_STRUCTURED_OUTPUT_MAX_PROPERTY_COUNT = 5000
OPENAI_SCHEMA_SUBSET_ISSUE_SAMPLE_LIMIT = 20
OPENAI_REQUEST_DIAGNOSTIC_CAPTURED = "openai_request_diagnostic_captured"
OPENAI_STRUCTURED_OUTPUT_SCHEMA_KEYWORDS = frozenset(
    {
        "$defs",
        "$ref",
        "additionalProperties",
        "anyOf",
        "const",
        "description",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "format",
        "items",
        "maxItems",
        "maximum",
        "minItems",
        "minimum",
        "multipleOf",
        "pattern",
        "properties",
        "required",
        "title",
        "type",
    }
)
FORBIDDEN_LIVE_REQUEST_TOKENS = tuple(
    dict.fromkeys(
        (
            *restricted_runner.FORBIDDEN_REQUEST_TOKENS,
            "authorization",
            "client_secret",
            "private_key",
            "openai_api_key",
            "openai-api-key",
            "web_search_preview",
            "file_search_call",
            "function_call",
            '"type":"function"',
            "tool_choice",
        )
    )
)
SECRET_LIKE_PATTERNS = (
    re.compile(r"sk-[a-z0-9_-]{12,}", re.IGNORECASE),
    re.compile(r"gh[pousr]_[a-z0-9_]{12,}", re.IGNORECASE),
    re.compile(r"postgres(?:ql)?(?:\+psycopg)?://", re.IGNORECASE),
    re.compile(r"bearer\s+[a-z0-9._-]{8,}", re.IGNORECASE),
    re.compile(r"-----begin [a-z ]*private key-----", re.IGNORECASE),
)
SIDE_EFFECT_FALSE_KEYS = (
    "telegram_called",
    "live_github_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
    "alembic_or_ddl_ran",
    "analysis_created",
    "notification_created",
)
OPENAI_QUOTA_OR_BILLING_MARKERS = (
    "insufficient_quota",
    "quota_exceeded",
    "billing_hard_limit",
    "billing_hard_limit_reached",
    "billing_not_active",
    "billing",
    "payment_required",
)
OPENAI_AUTHENTICATION_MARKERS = (
    "authenticationerror",
    "authentication_error",
    "invalid_api_key",
    "incorrect_api_key",
    "expired_api_key",
    "missing_api_key",
)
OPENAI_PERMISSION_MARKERS = (
    "permissiondeniederror",
    "permission_denied",
    "access_denied",
    "forbidden",
)
OPENAI_RATE_LIMIT_MARKERS = (
    "ratelimiterror",
    "rate_limit_error",
    "rate_limit_exceeded",
    "rate_limited",
)
OPENAI_MODEL_UNAVAILABLE_MARKERS = (
    "notfounderror",
    "not_found",
    "model_not_found",
    "model_not_available",
    "model_unavailable",
)
OPENAI_BAD_REQUEST_MARKERS = (
    "badrequesterror",
    "bad_request",
    "invalidrequesterror",
    "invalid_request_error",
)
OPENAI_TIMEOUT_MARKERS = (
    "apitimeouterror",
    "timeout",
    "timedout",
    "timed_out",
)
OPENAI_CONNECTION_MARKERS = (
    "apiconnectionerror",
    "connectionerror",
    "connection_error",
    "connecterror",
)
OPENAI_SERVER_ERROR_MARKERS = (
    "internalservererror",
    "servererror",
    "server_error",
    "serviceunavailableerror",
)

LiveClientFactory = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(slots=True)
class OpenAIRequestAudit:
    request_seen: bool = False
    request_shape_valid: bool = False
    model_allowed: bool = False
    tools_disabled: bool = False
    strict_schema_valid: bool = False
    max_output_tokens_capped: bool = False
    sensitive_content_absent: bool = False
    openai_client_created: bool = False
    live_openai_called: bool = False
    error_code: str | None = None


class LiveOpenAIResponsesClientAdapter:
    def __init__(
        self,
        *,
        api_key: str,
        live_client_factory: LiveClientFactory | None,
        max_output_tokens: int,
        sensitive_values: Sequence[str],
    ) -> None:
        self.responses = self
        self.audit = OpenAIRequestAudit()
        self._api_key = api_key
        self._live_client_factory = live_client_factory
        self._max_output_tokens = max_output_tokens
        self._sensitive_values = tuple(value for value in sensitive_values if value)
        self._client: Any | None = None

    def create(self, **request: Any) -> Any:
        prepared, checks = prepare_openai_responses_request(
            request,
            max_output_tokens=self._max_output_tokens,
            sensitive_values=self._sensitive_values,
        )
        self.audit = checks
        if checks.error_code is not None:
            raise RuntimeError(checks.error_code)

        client = self._ensure_client()
        responses = getattr(client, "responses", None)
        create = getattr(responses, "create", None)
        if not callable(create):
            create = getattr(client, "create", None)
        if not callable(create):
            self.audit.error_code = "openai_client_create_unavailable"
            raise RuntimeError("openai_client_create_unavailable")

        self.audit.live_openai_called = True
        try:
            response = create(**prepared)
        except Exception as exc:  # noqa: BLE001 - never expose live exception text.
            classified_code = classify_openai_responses_create_exception(exc)
            self.audit.error_code = classified_code
            raise RuntimeError(classified_code) from None
        if inspect.isawaitable(response):
            return _await_safely(response, self.audit)
        return response

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            self._client = self._create_client()
        except RuntimeError:
            raise
        except Exception:  # noqa: BLE001 - keep factory/SDK failure sanitized.
            self.audit.error_code = "openai_client_factory_failed"
            raise RuntimeError("openai_client_factory_failed") from None
        self.audit.openai_client_created = True
        return self._client

    def _create_client(self) -> Any:
        if self._live_client_factory is not None:
            return _call_live_client_factory(self._live_client_factory, api_key=self._api_key)
        try:
            from openai import OpenAI
        except Exception:  # noqa: BLE001 - stable operator failure code only.
            self.audit.error_code = "openai_sdk_missing"
            raise RuntimeError("openai_sdk_missing") from None
        try:
            return OpenAI(api_key=self._api_key)
        except Exception:  # noqa: BLE001 - do not expose SDK/env details.
            self.audit.error_code = "openai_client_factory_failed"
            raise RuntimeError("openai_client_factory_failed") from None


class SanitizedOpenAIRequestDiagnosticClientAdapter:
    def __init__(
        self,
        *,
        max_output_tokens: int,
        sensitive_values: Sequence[str],
    ) -> None:
        self.responses = self
        self.audit = OpenAIRequestAudit()
        self.diagnostic: dict[str, Any] | None = None
        self._max_output_tokens = max_output_tokens
        self._sensitive_values = tuple(value for value in sensitive_values if value)

    def create(self, **request: Any) -> Any:
        prepared, checks = prepare_openai_responses_request(
            request,
            max_output_tokens=self._max_output_tokens,
            sensitive_values=self._sensitive_values,
        )
        self.audit = checks
        self.diagnostic = build_sanitized_openai_request_diagnostic(
            prepared,
            max_output_tokens=self._max_output_tokens,
            preparation_error_code=checks.error_code,
        )
        raise RuntimeError(OPENAI_REQUEST_DIAGNOSTIC_CAPTURED)


async def _await_safely(awaitable: Any, audit: OpenAIRequestAudit) -> Any:
    try:
        return await awaitable
    except Exception as exc:  # noqa: BLE001 - never expose live response/transport details.
        classified_code = classify_openai_responses_create_exception(exc)
        audit.error_code = classified_code
        raise RuntimeError(classified_code) from None


def classify_openai_responses_create_exception(exc: Exception) -> str:
    markers = _openai_exception_markers(exc)
    status_code = _openai_exception_status_code(exc)

    if _marker_matches(markers, OPENAI_QUOTA_OR_BILLING_MARKERS):
        return "openai_quota_or_billing_failed"
    if _marker_matches(markers, OPENAI_AUTHENTICATION_MARKERS) or status_code == 401:
        return "openai_authentication_failed"
    if _marker_matches(markers, OPENAI_PERMISSION_MARKERS) or status_code == 403:
        return "openai_permission_denied"
    if _marker_matches(markers, OPENAI_RATE_LIMIT_MARKERS) or status_code == 429:
        return "openai_rate_limited"
    if _marker_matches(markers, OPENAI_MODEL_UNAVAILABLE_MARKERS) or status_code == 404:
        return "openai_model_not_found_or_unavailable"
    if _marker_matches(markers, OPENAI_BAD_REQUEST_MARKERS) or status_code == 400:
        return "openai_bad_request"
    if status_code == 402:
        return "openai_quota_or_billing_failed"
    if _marker_matches(markers, OPENAI_TIMEOUT_MARKERS) or status_code == 408:
        return "openai_timeout"
    if _marker_matches(markers, OPENAI_CONNECTION_MARKERS):
        return "openai_connection_failed"
    if _marker_matches(markers, OPENAI_SERVER_ERROR_MARKERS) or (
        status_code is not None and 500 <= status_code <= 599
    ):
        return "openai_server_error"
    return "openai_responses_create_failed"


def _openai_exception_markers(exc: Exception) -> tuple[str, ...]:
    markers: list[str] = []
    for cls in type(exc).__mro__:
        if cls is object:
            continue
        markers.append(cls.__name__)
    markers.extend(_openai_exception_code_type_values(exc))
    return tuple(normalized for value in markers if (normalized := _safe_marker(value)))


def _openai_exception_code_type_values(exc: Exception) -> tuple[str, ...]:
    values: list[str] = []
    for field in ("code", "type"):
        value = _safe_getattr(exc, field)
        if isinstance(value, str):
            values.append(value)

    body = _safe_getattr(exc, "body")
    if isinstance(body, Mapping):
        values.extend(_mapping_code_type_values(body))
        nested_error = body.get("error")
        if isinstance(nested_error, Mapping):
            values.extend(_mapping_code_type_values(nested_error))
    return tuple(values)


def _mapping_code_type_values(value: Mapping[str, Any]) -> tuple[str, ...]:
    values: list[str] = []
    for field in ("code", "type"):
        field_value = value.get(field)
        if isinstance(field_value, str):
            values.append(field_value)
    return tuple(values)


def _openai_exception_status_code(exc: Exception) -> int | None:
    for field in ("status_code", "status"):
        value = _safe_getattr(exc, field)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _safe_getattr(value: Any, name: str) -> Any:
    try:
        return getattr(value, name)
    except Exception:  # noqa: BLE001 - live SDK fields must not break classification.
        return None


def _safe_marker(value: str) -> str | None:
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    return normalized or None


def _marker_matches(markers: Sequence[str], expected: Sequence[str]) -> bool:
    return any(token in marker for marker in markers for token in expected)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an operator-approved local/test DB one-shot live OpenAI judge "
            "canary through the closed restricted judge runner."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--source-fixture", required=True)
    parser.add_argument("--github-snapshot-fixture", required=True)
    parser.add_argument("--replay-namespace", required=True)
    parser.add_argument("--confirm-local-test-db", action="store_true")
    parser.add_argument("--allow-live-openai", action="store_true")
    parser.add_argument("--confirm-live-openai-call", action="store_true")
    parser.add_argument("--openai-api-key-env", default=DEFAULT_OPENAI_API_KEY_ENV)
    parser.add_argument("--max-output-tokens", type=int, default=DEFAULT_MAX_OUTPUT_TOKENS)
    parser.add_argument("--print-sanitized-openai-request-diagnostic", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    live_client_factory: LiveClientFactory | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    root = repo_root or _repo_root()
    report = _base_report()
    checks_failed: list[str] = []
    diagnostic_requested = bool(getattr(args, "print_sanitized_openai_request_diagnostic", False))
    if diagnostic_requested:
        report["sanitized_openai_request_diagnostic_requested"] = True

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    if effective_env.get("APP_ENV", "").strip().lower() != "test":
        checks_failed.append("app_env_test_required")

    if not diagnostic_requested:
        if not args.allow_live_openai:
            checks_failed.append("allow_live_openai_required")
        if not args.confirm_live_openai_call:
            checks_failed.append("confirm_live_openai_call_required")

    api_key_env = str(args.openai_api_key_env or "")
    api_key_env_allowed = api_key_env in ALLOWED_OPENAI_API_KEY_ENVS
    report["openai_api_key_env_allowed"] = api_key_env_allowed
    if not api_key_env_allowed:
        checks_failed.append("openai_api_key_env_disallowed")

    max_output_tokens = getattr(args, "max_output_tokens", DEFAULT_MAX_OUTPUT_TOKENS)
    if not isinstance(max_output_tokens, int) or isinstance(max_output_tokens, bool) or max_output_tokens < 1:
        checks_failed.append("max_output_tokens_invalid")
    elif max_output_tokens > HARD_MAX_OUTPUT_TOKENS:
        checks_failed.append("max_output_tokens_above_hard_limit")

    namespace_ok, namespace_failures = (
        restricted_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.validate_replay_namespace(
            args.replay_namespace
        )
    )
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = restricted_runner.validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    try:
        restricted_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.load_source_fixture(
            Path(args.source_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - stable sanitized flag only.
        checks_failed.append("source_fixture_load_failed")

    try:
        restricted_runner.analysis_router_runner.evidence_bundle_runner.github_snapshot_runner.load_github_snapshot_fixture(
            Path(args.github_snapshot_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - stable sanitized flag only.
        checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if not namespace_ok:
        checks_failed.append("precondition_missing")
        return _finish(report, checks_failed)

    if diagnostic_requested:
        return _run_sanitized_openai_request_diagnostic(
            args,
            env=effective_env,
            repo_root=root,
            report=report,
            max_output_tokens=int(max_output_tokens),
        )

    api_key = effective_env.get(api_key_env, "")
    if not isinstance(api_key, str) or not api_key.strip():
        checks_failed.append("openai_api_key_missing")
        return _finish(report, checks_failed)
    report["openai_api_key_present"] = True
    report["live_openai_authority_confirmed"] = True

    adapter = LiveOpenAIResponsesClientAdapter(
        api_key=api_key,
        live_client_factory=live_client_factory,
        max_output_tokens=int(max_output_tokens),
        sensitive_values=(api_key, args.database_url),
    )
    restricted_args = argparse.Namespace(
        database_url=args.database_url,
        source_fixture=args.source_fixture,
        github_snapshot_fixture=args.github_snapshot_fixture,
        replay_namespace=args.replay_namespace,
        confirm_local_test_db=True,
    )

    report["restricted_judge_canary_delegated"] = True
    try:
        delegated = restricted_runner.run(
            restricted_args,
            env=effective_env,
            openai_client=adapter,
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - never echo delegated runtime details.
        checks_failed.append("restricted_judge_canary_failed")
        return _finish(report, checks_failed)

    _merge_delegated_report(report, delegated.report, adapter.audit)
    delegated_failures = list(delegated.report.get("checks_failed") or [])
    if adapter.audit.error_code is not None:
        checks_failed.append(adapter.audit.error_code)
        delegated_failures = [failure for failure in delegated_failures if failure != "RuntimeError"]
    checks_failed.extend(delegated_failures)
    if delegated.exit_code != 0 or delegated.report.get("status") != "pass":
        if not checks_failed:
            checks_failed.append("restricted_judge_canary_failed")
        return _finish(report, checks_failed)

    checks_failed.extend(_proof_flag_failures(report))
    return _finish(report, checks_failed)


def _run_sanitized_openai_request_diagnostic(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str],
    repo_root: Path,
    report: dict[str, Any],
    max_output_tokens: int,
) -> RunnerResult:
    adapter = SanitizedOpenAIRequestDiagnosticClientAdapter(
        max_output_tokens=max_output_tokens,
        sensitive_values=(args.database_url,),
    )
    restricted_args = argparse.Namespace(
        database_url=args.database_url,
        source_fixture=args.source_fixture,
        github_snapshot_fixture=args.github_snapshot_fixture,
        replay_namespace=args.replay_namespace,
        confirm_local_test_db=True,
    )

    report["restricted_judge_canary_delegated"] = True
    try:
        delegated = restricted_runner.run(
            restricted_args,
            env=env,
            openai_client=adapter,
            repo_root=repo_root,
        )
    except Exception:  # noqa: BLE001 - diagnostic output must stay sanitized.
        if adapter.diagnostic is None:
            return _finish(report, ["restricted_judge_canary_failed"])
        delegated = restricted_runner.RunnerResult(exit_code=1, report={})

    _merge_delegated_report(report, delegated.report, adapter.audit)
    if adapter.diagnostic is None:
        delegated_failures = [
            failure
            for failure in list(delegated.report.get("checks_failed") or [])
            if failure != "RuntimeError"
        ]
        checks_failed = delegated_failures or ["openai_request_diagnostic_not_captured"]
        return _finish(report, checks_failed)

    report["openai_request_diagnostic"] = adapter.diagnostic
    checks_failed = [adapter.audit.error_code] if adapter.audit.error_code is not None else []
    return _finish(report, checks_failed)


def prepare_openai_responses_request(
    request: Mapping[str, Any],
    *,
    max_output_tokens: int,
    sensitive_values: Sequence[str],
) -> tuple[dict[str, Any], OpenAIRequestAudit]:
    audit = OpenAIRequestAudit(request_seen=True)
    prepared = copy.deepcopy(dict(request))

    audit.model_allowed = prepared.get("model") in ALLOWED_OPENAI_MODELS
    if not audit.model_allowed:
        audit.error_code = "openai_request_model_disallowed"
        return prepared, audit

    audit.tools_disabled = prepared.get("tools") == []
    if not audit.tools_disabled:
        audit.error_code = "openai_request_tools_not_disabled"
        return prepared, audit

    audit.strict_schema_valid = _strict_judge_output_schema_valid(prepared)
    if not audit.strict_schema_valid:
        audit.error_code = "openai_request_schema_not_strict"
        return prepared, audit

    raw_max = prepared.get("max_output_tokens")
    if raw_max is None:
        prepared["max_output_tokens"] = max_output_tokens
    elif not isinstance(raw_max, int) or isinstance(raw_max, bool) or raw_max < 1:
        audit.error_code = "openai_request_max_output_tokens_invalid"
        return prepared, audit
    elif raw_max > max_output_tokens:
        prepared["max_output_tokens"] = max_output_tokens
    audit.max_output_tokens_capped = prepared.get("max_output_tokens") <= max_output_tokens
    if not audit.max_output_tokens_capped:
        audit.error_code = "openai_request_max_output_tokens_uncapped"
        return prepared, audit

    audit.sensitive_content_absent = not _contains_forbidden_live_request_content(
        prepared,
        sensitive_values=sensitive_values,
    )
    if not audit.sensitive_content_absent:
        audit.error_code = "openai_request_sensitive_content_rejected"
        return prepared, audit

    audit.request_shape_valid = restricted_runner.openai_responses_request_shape_valid(prepared)
    if not audit.request_shape_valid:
        audit.error_code = "openai_responses_request_shape_invalid"
        return prepared, audit

    return prepared, audit


def build_sanitized_openai_request_diagnostic(
    prepared_request: Mapping[str, Any],
    *,
    max_output_tokens: int,
    preparation_error_code: str | None = None,
) -> dict[str, Any]:
    request = dict(prepared_request)
    model = request.get("model")
    model_allowed = model in ALLOWED_OPENAI_MODELS
    tools = request.get("tools")
    text_format = _mapping(_mapping(request.get("text")).get("format"))
    schema = _mapping(text_format.get("schema"))
    required = schema.get("required")
    properties = _mapping(schema.get("properties"))
    input_items = request.get("input")
    reasoning = _mapping(request.get("reasoning"))
    max_output_value = request.get("max_output_tokens")
    schema_subset_issues = validate_openai_structured_output_schema_subset(schema)

    unsupported_top_level_keys = set(request) - ALLOWED_OPENAI_REQUEST_TOP_LEVEL_KEYS
    diagnostic: dict[str, Any] = {
        "schema_version": DIAGNOSTIC_SCHEMA_VERSION,
        "request_preparation_error_code": preparation_error_code,
        "top_level_keys": _diagnostic_key_list(request.keys()),
        "unsupported_top_level_keys": _diagnostic_key_list(unsupported_top_level_keys),
        "unsupported_top_level_key_count": len(unsupported_top_level_keys),
        "model_allowed": model_allowed,
        "model": model if isinstance(model, str) and model_allowed else None,
        "input_item_count": len(input_items) if isinstance(input_items, list) else 0,
        "input_items": _input_item_diagnostics(input_items),
        "input_text_content_present": _input_text_content_present(input_items),
        "tools_count": len(tools) if isinstance(tools, list) else None,
        "tools_empty": tools == [],
        "max_output_tokens": max_output_value if _positive_int(max_output_value) else None,
        "max_output_tokens_cap": max_output_tokens,
        "max_output_tokens_cap_status": _max_output_tokens_cap_status(
            max_output_value,
            max_output_tokens=max_output_tokens,
        ),
        "reasoning_keys": _diagnostic_key_list(reasoning.keys()),
        "reasoning_effort": _diagnostic_reasoning_effort(reasoning.get("effort")),
        "text_format_keys": _diagnostic_key_list(text_format.keys()),
        "text_format_type": _diagnostic_scalar(text_format.get("type")),
        "text_format_name": _diagnostic_scalar(text_format.get("name")),
        "text_format_strict": text_format.get("strict") if isinstance(text_format.get("strict"), bool) else None,
        "json_schema_top_level_keys": _diagnostic_key_list(schema.keys()),
        "json_schema_required_count": len(required) if isinstance(required, list) else None,
        "json_schema_property_keys": _diagnostic_key_list(properties.keys()),
        "json_schema_subset_issue_count": len(schema_subset_issues),
        "json_schema_subset_issue_codes": _diagnostic_key_list(
            issue.get("issue_code") for issue in schema_subset_issues
        ),
        "json_schema_subset_issues": schema_subset_issues[:OPENAI_SCHEMA_SUBSET_ISSUE_SAMPLE_LIMIT],
    }
    return diagnostic


def validate_openai_structured_output_schema_subset(schema: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    object_property_count = 0

    def add_issue(issue_code: str, schema_path: str, *, key: Any | None = None) -> None:
        issue: dict[str, Any] = {
            "issue_code": issue_code,
            "schema_path": schema_path,
        }
        if key is not None:
            issue["key"] = _diagnostic_key_name(key)
        issues.append(issue)

    def visit(node: Any, path: str, *, object_depth: int, root: bool = False, property_key: Any | None = None) -> None:
        nonlocal object_property_count
        if not isinstance(node, Mapping):
            if root:
                add_issue("root_not_object", path)
            return

        for raw_key in sorted(node, key=str):
            if raw_key not in OPENAI_STRUCTURED_OUTPUT_SCHEMA_KEYWORDS:
                add_issue(
                    "unsupported_schema_keyword",
                    _schema_path_join(path, raw_key),
                    key=raw_key,
                )

        if root:
            if node.get("type") != "object":
                add_issue("root_not_object", path)
            if "anyOf" in node:
                add_issue("root_anyof_disallowed", path)

        if not root and "type" not in node and "anyOf" not in node and "$ref" not in node:
            add_issue("property_missing_type", path, key=property_key)

        schema_types = _schema_type_names(node.get("type"))
        if "object" in schema_types:
            current_object_depth = object_depth + 1
            if current_object_depth > OPENAI_STRUCTURED_OUTPUT_MAX_OBJECT_DEPTH:
                add_issue("excessive_nesting_depth_candidate", path)

            properties = node.get("properties")
            required = node.get("required")

            if "properties" not in node:
                add_issue("object_missing_properties", path)
                property_items: list[tuple[Any, Any]] = []
            elif not isinstance(properties, Mapping):
                add_issue("properties_not_object", _schema_path_join(path, "properties"))
                property_items = []
            else:
                object_property_count += len(properties)
                property_items = sorted(properties.items(), key=lambda item: str(item[0]))

            required_names: set[str] | None
            if "required" not in node:
                add_issue("object_missing_required", path)
                required_names = None
            elif not isinstance(required, list) or not all(isinstance(item, str) for item in required):
                add_issue("required_not_list", _schema_path_join(path, "required"))
                required_names = None
            else:
                required_names = set(required)

            if node.get("additionalProperties") is not False:
                add_issue("object_missing_additional_properties_false", path)

            if isinstance(properties, Mapping) and required_names is not None:
                property_names = {str(name) for name in properties}
                for missing_required in sorted(property_names - required_names):
                    add_issue(
                        "required_property_mismatch",
                        _schema_path_join(path, "required"),
                        key=missing_required,
                    )
                for unknown_required in sorted(required_names - property_names):
                    add_issue(
                        "required_property_mismatch",
                        _schema_path_join(path, "required"),
                        key=unknown_required,
                    )

            for child_key, child_schema in property_items:
                visit(
                    child_schema,
                    _schema_path_join(path, "properties", child_key),
                    object_depth=current_object_depth,
                    property_key=child_key,
                )

        if "array" in schema_types:
            if "items" not in node:
                add_issue("array_missing_items", path, key=property_key)
            else:
                visit(
                    node.get("items"),
                    _schema_path_join(path, "items"),
                    object_depth=object_depth,
                )

        any_of = node.get("anyOf")
        if isinstance(any_of, list):
            for index, candidate in enumerate(any_of):
                visit(
                    candidate,
                    _schema_path_join(path, "anyOf", index),
                    object_depth=object_depth,
                )

        defs = node.get("$defs")
        if isinstance(defs, Mapping):
            for definition_name, definition_schema in sorted(defs.items(), key=lambda item: str(item[0])):
                visit(
                    definition_schema,
                    _schema_path_join(path, "$defs", definition_name),
                    object_depth=object_depth,
                    property_key=definition_name,
                )

    visit(schema, "", object_depth=0, root=True)
    if object_property_count > OPENAI_STRUCTURED_OUTPUT_MAX_PROPERTY_COUNT:
        issues.append(
            {
                "issue_code": "excessive_property_count_candidate",
                "schema_path": "",
            }
        )
    return issues


def _input_item_diagnostics(input_items: Any) -> list[dict[str, Any]]:
    if not isinstance(input_items, list):
        return []
    diagnostics: list[dict[str, Any]] = []
    for index, item in enumerate(input_items):
        item_mapping = _mapping(item)
        content = item_mapping.get("content")
        content_items = content if isinstance(content, list) else []
        diagnostics.append(
            {
                "index": index,
                "keys": _diagnostic_key_list(item_mapping.keys()),
                "role": _diagnostic_scalar(item_mapping.get("role")),
                "content_count": len(content_items),
                "content_items": _content_item_diagnostics(content_items),
            }
        )
    return diagnostics


def _content_item_diagnostics(content_items: Sequence[Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for index, item in enumerate(content_items):
        item_mapping = _mapping(item)
        text = item_mapping.get("text")
        diagnostics.append(
            {
                "index": index,
                "keys": _diagnostic_key_list(item_mapping.keys()),
                "type": _diagnostic_scalar(item_mapping.get("type")),
                "has_text": isinstance(text, str) and bool(text),
            }
        )
    return diagnostics


def _input_text_content_present(input_items: Any) -> bool:
    if not isinstance(input_items, list):
        return False
    for item in input_items:
        content = _mapping(item).get("content")
        if not isinstance(content, list):
            continue
        for content_item in content:
            content_mapping = _mapping(content_item)
            if content_mapping.get("type") == "input_text":
                text = content_mapping.get("text")
                if isinstance(text, str) and bool(text):
                    return True
    return False


def _diagnostic_key_list(keys: Any) -> list[str]:
    return sorted(dict.fromkeys(_diagnostic_key_name(key) for key in keys))


def _diagnostic_key_name(key: Any) -> str:
    value = str(key)
    lowered = value.lower()
    if any(
        token in lowered
        for token in (
            "authorization",
            "api_key",
            "openai_api_key",
            "access_token",
            "bearer",
            "client_secret",
            "private_key",
            "secret",
            "password",
            "database_url",
            "db_url",
        )
    ):
        return "<redacted_sensitive_key>"
    if len(value) > 80 or "\n" in value or "\r" in value:
        return "<redacted_key>"
    return value


def _diagnostic_scalar(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if len(value) > 80 or "\n" in value or "\r" in value:
        return "<redacted_value>"
    lowered = value.lower()
    if any(pattern.search(value) for pattern in SECRET_LIKE_PATTERNS):
        return "<redacted_value>"
    if any(token in lowered for token in ("authorization", "api_key", "access_token", "client_secret")):
        return "<redacted_value>"
    return value


def _diagnostic_reasoning_effort(value: Any) -> str | None:
    return value if isinstance(value, str) and value in ALLOWED_REASONING_EFFORTS else None


def _schema_type_names(value: Any) -> set[str]:
    if isinstance(value, str):
        return {value}
    if isinstance(value, list):
        return {item for item in value if isinstance(item, str)}
    return set()


def _schema_path_join(path: str, *segments: Any) -> str:
    suffix = "".join(f"/{_json_pointer_escape(_diagnostic_key_name(segment))}" for segment in segments)
    return f"{path}{suffix}"


def _json_pointer_escape(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _max_output_tokens_cap_status(value: Any, *, max_output_tokens: int) -> str:
    if value is None:
        return "missing"
    if not _positive_int(value):
        return "invalid"
    if value > max_output_tokens:
        return "above_cap"
    return "within_cap"


def _strict_judge_output_schema_valid(request: Mapping[str, Any]) -> bool:
    text = _mapping(request.get("text"))
    text_format = _mapping(text.get("format"))
    return (
        text_format.get("type") == "json_schema"
        and text_format.get("name") == "judge_output_v1"
        and text_format.get("strict") is True
        and isinstance(text_format.get("schema"), Mapping)
    )


def _contains_forbidden_live_request_content(
    request: Mapping[str, Any],
    *,
    sensitive_values: Sequence[str],
) -> bool:
    canonical = _canonical_json(request)
    lowered = canonical.lower()
    if any(token in lowered for token in FORBIDDEN_LIVE_REQUEST_TOKENS):
        return True
    for value in sensitive_values:
        if value and value.lower() in lowered:
            return True
    return any(pattern.search(canonical) for pattern in SECRET_LIKE_PATTERNS)


def _call_live_client_factory(factory: LiveClientFactory, *, api_key: str) -> Any:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return factory(api_key=api_key)
    parameters = signature.parameters
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()):
        return factory(api_key=api_key)
    if "api_key" in parameters:
        return factory(api_key=api_key)
    positional = [
        param
        for param in parameters.values()
        if param.kind in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        and param.default is inspect.Parameter.empty
    ]
    if positional:
        return factory(api_key)
    return factory()


def _merge_delegated_report(
    report: dict[str, Any],
    delegated: Mapping[str, Any],
    audit: OpenAIRequestAudit,
) -> None:
    passthrough = (
        "analysis_router_replay_confirmed",
        "judge_call_requested_event_found",
        "judge_run_loaded",
        "evidence_bundle_loaded",
        "judge_request_built",
        "judge_request_uses_bundle_only",
        "openai_structured_output_received",
        "judge_output_created",
        "judge_run_updated",
        "judge_output_ready_event_created",
        *SIDE_EFFECT_FALSE_KEYS,
    )
    for key in passthrough:
        if key in delegated:
            report[key] = delegated[key]

    report["openai_client_created"] = audit.openai_client_created
    report["live_openai_called"] = audit.live_openai_called
    if audit.request_seen:
        report["openai_responses_request_shape_valid"] = audit.request_shape_valid
        report["openai_request_model_allowed"] = audit.model_allowed
        report["openai_request_tools_disabled"] = audit.tools_disabled
        report["openai_request_max_output_tokens_capped"] = audit.max_output_tokens_capped
    else:
        report["openai_responses_request_shape_valid"] = bool(
            delegated.get("openai_responses_request_shape_valid")
        )
        report["openai_request_model_allowed"] = bool(
            delegated.get("openai_responses_request_shape_valid")
        )
        report["openai_request_tools_disabled"] = bool(
            delegated.get("openai_responses_request_shape_valid")
        )

    report["existing_judge_output_reused"] = (
        delegated.get("status") == "pass"
        and delegated.get("judge_output_created") is True
        and audit.request_seen is False
    )


def _proof_flag_failures(report: Mapping[str, Any]) -> list[str]:
    required = [
        "database_url_guard_passed",
        "live_openai_authority_confirmed",
        "openai_api_key_env_allowed",
        "openai_api_key_present",
        "restricted_judge_canary_delegated",
        "analysis_router_replay_confirmed",
        "judge_call_requested_event_found",
        "judge_run_loaded",
        "evidence_bundle_loaded",
        "judge_request_built",
        "judge_request_uses_bundle_only",
        "openai_responses_request_shape_valid",
        "openai_request_model_allowed",
        "openai_request_tools_disabled",
        "openai_structured_output_received",
        "judge_output_created",
        "judge_run_updated",
        "judge_output_ready_event_created",
    ]
    if not report.get("existing_judge_output_reused"):
        required.extend(
            [
                "openai_client_created",
                "openai_request_max_output_tokens_capped",
                "live_openai_called",
            ]
        )
    failures = [f"{key}:missing" for key in required if report.get(key) is not True]
    failures.extend(f"{key}:unexpected" for key in SIDE_EFFECT_FALSE_KEYS if report.get(key) is not False)
    return failures


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "live_openai_authority_confirmed": False,
        "openai_api_key_env_allowed": False,
        "openai_api_key_present": False,
        "openai_client_created": False,
        "restricted_judge_canary_delegated": False,
        "analysis_router_replay_confirmed": False,
        "judge_call_requested_event_found": False,
        "judge_run_loaded": False,
        "evidence_bundle_loaded": False,
        "judge_request_built": False,
        "judge_request_uses_bundle_only": False,
        "openai_responses_request_shape_valid": False,
        "openai_request_model_allowed": False,
        "openai_request_tools_disabled": False,
        "openai_request_max_output_tokens_capped": False,
        "openai_structured_output_received": False,
        "judge_output_created": False,
        "judge_run_updated": False,
        "judge_output_ready_event_created": False,
        "live_openai_called": False,
        "existing_judge_output_reused": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "analysis_created": False,
        "notification_created": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
