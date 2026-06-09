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
DEFAULT_OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
ALLOWED_OPENAI_API_KEY_ENVS = frozenset({DEFAULT_OPENAI_API_KEY_ENV})
ALLOWED_OPENAI_MODELS = frozenset({"gpt-5.4-mini"})
DEFAULT_MAX_OUTPUT_TOKENS = 800
HARD_MAX_OUTPUT_TOKENS = 1200
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
        except Exception:  # noqa: BLE001 - never expose live exception text.
            self.audit.error_code = "openai_responses_create_failed"
            raise RuntimeError("openai_responses_create_failed") from None
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


async def _await_safely(awaitable: Any, audit: OpenAIRequestAudit) -> Any:
    try:
        return await awaitable
    except Exception:  # noqa: BLE001 - never expose live response/transport details.
        audit.error_code = "openai_responses_create_failed"
        raise RuntimeError("openai_responses_create_failed") from None


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

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    if effective_env.get("APP_ENV", "").strip().lower() != "test":
        checks_failed.append("app_env_test_required")

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
