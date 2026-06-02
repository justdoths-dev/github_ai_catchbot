from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

import sqlalchemy as sa


_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.services.judge_openai.context_builder import JudgeContextBuilder  # noqa: E402
from src.services.judge_openai.openai_client import OpenAIJudgeClient  # noqa: E402
from src.services.judge_openai.preflight import NoopModelContextPreflight  # noqa: E402
from src.services.judge_openai.prompt_library import (  # noqa: E402
    PromptLibrary,
    UnsupportedJudgeProfileError,
)
from src.services.judge_openai.repositories import JudgeOpenAIRepository  # noqa: E402
from src.services.judge_openai.request_shape import (  # noqa: E402
    LOCKED_HOT_PATH_MODEL,
    summarize_responses_request_shape,
)
from src.services.judge_openai.service import JudgeOpenAIService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_real_bundle_context_live_diagnostic"
REPORT_TYPE = "judge_openai_real_bundle_context_live_diagnostic_v1"

STATUS_PREFLIGHT_PASSED = "judge_openai_real_bundle_context_live_diagnostic_preflight_passed"
STATUS_DB_PREFLIGHT_PASSED = (
    "judge_openai_real_bundle_context_live_diagnostic_db_read_preflight_passed"
)
STATUS_LIVE_SUCCEEDED = (
    "judge_openai_real_bundle_context_live_diagnostic_approved_live_succeeded"
)
STATUS_NOT_APPROVED = (
    "blocked_judge_openai_real_bundle_context_live_diagnostic_not_approved"
)
STATUS_DB_READ_FAILED = (
    "blocked_judge_openai_real_bundle_context_live_diagnostic_db_read_failed"
)
STATUS_CONTEXT_FAILED = (
    "blocked_judge_openai_real_bundle_context_live_diagnostic_context_failed"
)
STATUS_LIVE_FAILED = (
    "blocked_judge_openai_real_bundle_context_live_diagnostic_live_failed"
)
STATUS_RAW_VALUE_EMISSION = (
    "blocked_judge_openai_real_bundle_context_live_diagnostic_raw_value_emission"
)

REPLAY_PROMPT_SUFFIX = "__replay_live_smoke_v1"
REPLAY_REASON_CODE = "manual_live_smoke_replay"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
EXPECTED_QUEUE_NAME = "q.analysis.judge"
EXPECTED_STAGE_NAME = "judge"
EXPECTED_AGGREGATE_TYPE = "judge_run"

DEFAULT_REASONING_EFFORT = "low"
DEFAULT_MAX_OUTPUT_TOKENS = 500
DEFAULT_PROMPT_CACHE_KEY = "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1"
DEFAULT_TIMEOUT_SECONDS = 10.0
DIAGNOSTIC_DEVELOPER_PROMPT = "diagnostic judge-output schema probe"
DIAGNOSTIC_USER_CONTEXT = "diagnostic candidate evidence placeholder"

DATABASE_ENV_KEYS = frozenset({"DATABASE_URL"})
OPENAI_ENV_KEYS = frozenset({"OPENAI_API_KEY", "OPENAI_API_KEY_FILE", "OPENAI_PROJECT"})
ALLOWED_STATUS_BUCKETS = frozenset(
    {"pending", "running", "succeeded", "failed_terminal", "failed_retryable"}
)
ALLOWED_TARGET_FINISH_REASON_BUCKETS = frozenset(
    {
        "openai_permanent_error",
        "openai_request_shape_invalid",
        "openai_transport_retryable",
        "schema_invalid_after_retry",
    }
)
SCHEMA_HINT_RE = re.compile(r"(json_schema|schema|response_format|structured)", re.IGNORECASE)
MODEL_HINT_RE = re.compile(r"(model|deployment)", re.IGNORECASE)
OPENAI_ERROR_CODE_BUCKET_PATTERNS = (
    ("json_schema", re.compile(r"(json_schema|schema_invalid|schema)", re.IGNORECASE)),
    ("response_format", re.compile(r"response_format", re.IGNORECASE)),
    (
        "unsupported_parameter",
        re.compile(r"(unsupported|unknown|unrecognized).*(parameter|param)", re.IGNORECASE),
    ),
    ("unsupported_value", re.compile(r"(unsupported|invalid).*value", re.IGNORECASE)),
    ("invalid_type", re.compile(r"invalid.*type", re.IGNORECASE)),
    (
        "missing_required_parameter",
        re.compile(r"(missing|required).*(parameter|param)", re.IGNORECASE),
    ),
    ("context_length_exceeded", re.compile(r"context.*length", re.IGNORECASE)),
    ("model_not_found", re.compile(r"model.*not.*found", re.IGNORECASE)),
    ("model_access", re.compile(r"model.*(not.*available|access)", re.IGNORECASE)),
    ("rate_limit", re.compile(r"rate.*limit", re.IGNORECASE)),
    ("insufficient_quota", re.compile(r"insufficient.*quota", re.IGNORECASE)),
    ("invalid_api_key", re.compile(r"invalid.*api.*key", re.IGNORECASE)),
)
OPENAI_ERROR_PARAM_BUCKET_PATTERNS = (
    ("max_output_tokens", re.compile(r"max_output_tokens", re.IGNORECASE)),
    ("prompt_cache_key", re.compile(r"prompt_cache_key", re.IGNORECASE)),
    ("response_format", re.compile(r"response_format", re.IGNORECASE)),
    ("text_format", re.compile(r"text.*format|format", re.IGNORECASE)),
    ("json_schema", re.compile(r"json_schema|schema", re.IGNORECASE)),
    ("input_context", re.compile(r"input|context|message|content", re.IGNORECASE)),
    ("tools", re.compile(r"tools?", re.IGNORECASE)),
    ("model", re.compile(r"model", re.IGNORECASE)),
    ("reasoning", re.compile(r"reasoning", re.IGNORECASE)),
)
OPENAI_ERROR_MESSAGE_HINT_PATTERNS = (
    ("optional_null", re.compile(r"\b(null|none|null value)\b", re.IGNORECASE)),
    (
        "unsupported_parameter",
        re.compile(r"(unsupported|unknown|unrecognized).*(parameter|param)", re.IGNORECASE),
    ),
    ("unsupported_value", re.compile(r"(unsupported|invalid).*(value|type)", re.IGNORECASE)),
    ("model_parameter", MODEL_HINT_RE),
    ("response_format", re.compile(r"response_format|response format", re.IGNORECASE)),
    ("text_format", re.compile(r"text\.format|text format", re.IGNORECASE)),
    ("json_schema", SCHEMA_HINT_RE),
    ("prompt_cache_key", re.compile(r"prompt_cache_key", re.IGNORECASE)),
    ("max_output_tokens", re.compile(r"max_output_tokens|max output tokens", re.IGNORECASE)),
    ("input_context", re.compile(r"\b(input|context|message|content)\b", re.IGNORECASE)),
    ("tools", re.compile(r"\btools?\b", re.IGNORECASE)),
    ("reasoning", re.compile(r"\breasoning\b", re.IGNORECASE)),
)
REQUEST_SHAPE_REPORT_KEYS = (
    "request_shape_valid_bucket",
    "request_shape_issue_count_bucket",
    "request_shape_issue_buckets",
    "top_level_request_key_presence_buckets",
    "optional_null_field_count_bucket",
    "optional_null_field_name_buckets",
    "model_bucket",
    "reasoning_effort_bucket",
    "input_message_count_bucket",
    "text_format_type_bucket",
    "text_format_json_schema_bucket",
    "json_schema_strict_bucket",
    "strict_schema_bucket",
    "tools_count_bucket",
    "tools_bucket",
    "max_output_tokens_presence_bucket",
    "max_output_tokens_present_bucket",
    "max_output_tokens_null_bucket",
    "prompt_cache_key_presence_bucket",
    "prompt_cache_key_present_bucket",
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_REPLAY_LIVE_SMOKE_CANDIDATES_QUERY = """
WITH latest_outbox_per_run AS (
    SELECT DISTINCT ON (jr.judge_run_id)
           jr.judge_run_id,
           jr.bundle_id,
           jr.status AS judge_run_status,
           jr.finish_reason,
           eo.event_id AS judge_call_requested_event_id,
           eo.created_at AS judge_call_requested_created_at,
           COALESCE(jr.finished_at, jr.started_at, eo.created_at) AS recency_at
    FROM judge_runs jr
    JOIN event_outbox eo
      ON eo.event_type = 'judge.call.requested.v1'
     AND eo.aggregate_type = 'judge_run'
     AND eo.aggregate_id = jr.judge_run_id
    WHERE jr.prompt_version LIKE :replay_prompt_like
      AND (
          eo.payload_json->>'replay_reason_code' = :replay_reason_code
          OR jr.prompt_version LIKE :replay_prompt_like
      )
    ORDER BY jr.judge_run_id,
             eo.created_at DESC NULLS LAST,
             eo.event_id DESC
)
SELECT judge_run_id,
       bundle_id,
       judge_run_status,
       finish_reason,
       judge_call_requested_event_id,
       judge_call_requested_created_at,
       recency_at
FROM latest_outbox_per_run
ORDER BY recency_at DESC NULLS LAST,
         judge_call_requested_created_at DESC NULLS LAST,
         judge_run_id DESC
LIMIT :limit
"""
COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM judge_outputs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'judge.output.ready.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
"""
COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
"""
REQUIRED_TABLES = (
    "judge_runs",
    "judge_outputs",
    "event_outbox",
    "candidate_evidence_bundles",
)
TARGET_QUERY_LIMIT = 2


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
SdkLoader = Callable[[], "SdkImports"]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SdkImports:
    async_openai: Any | None


@dataclass(frozen=True, slots=True)
class ReplayLiveSmokeCandidate:
    judge_run_id: UUID
    bundle_id: UUID
    status: str
    finish_reason: str | None
    judge_call_requested_event_id: UUID
    recency_at: Any
    judge_call_requested_created_at: Any


@dataclass(frozen=True, slots=True)
class RealContextBuild:
    request: dict[str, Any]


class _DefaultDatabaseSession:
    def __init__(self, engine: Any, session: Any) -> None:
        self._engine = engine
        self._session = session

    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._session.execute(statement, params or {})

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated judge-openai real bundle context diagnostic. "
            "Default mode is local-only and does not read DB, keys, Redis, or OpenAI."
        )
    )
    parser.add_argument("--approve-db-read", action="store_true")
    parser.add_argument("--approve-key-read", action="store_true")
    parser.add_argument("--approve-live-openai", action="store_true")
    parser.add_argument("--max-live-calls", type=int, default=0)
    parser.add_argument("--runtime-env-path")
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def generate_report(
    *,
    approve_db_read: bool = False,
    approve_key_read: bool = False,
    approve_live_openai: bool = False,
    max_live_calls: int = 0,
    runtime_env_path: str | Path | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    sdk_loader: SdkLoader | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            approve_db_read=approve_db_read,
            approve_key_read=approve_key_read,
            approve_live_openai=approve_live_openai,
            max_live_calls=max_live_calls,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            sdk_loader=sdk_loader,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


async def generate_report_async(
    *,
    approve_db_read: bool = False,
    approve_key_read: bool = False,
    approve_live_openai: bool = False,
    max_live_calls: int = 0,
    runtime_env_path: str | Path | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    sdk_loader: SdkLoader | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report(
        approve_db_read=approve_db_read,
        approve_key_read=approve_key_read,
        approve_live_openai=approve_live_openai,
        max_live_calls=max_live_calls,
    )
    raw_values = _raw_values(*forbidden_raw_values)

    default_request = _build_default_preflight_request()
    raw_values.update(
        _raw_values(DIAGNOSTIC_DEVELOPER_PROMPT, DIAGNOSTIC_USER_CONTEXT)
    )
    _merge_request_shape_report(
        report,
        summarize_responses_request_shape(default_request),
    )
    if report["request_shape_valid_bucket"] != "one":
        _set_status(report, STATUS_CONTEXT_FAILED, "request_shape.invalid")
        return _finalize(report, raw_values, exit_code=1)

    any_approval_or_runtime_arg = (
        approve_db_read
        or approve_key_read
        or approve_live_openai
        or max_live_calls != 0
        or runtime_env_path is not None
    )
    if not any_approval_or_runtime_arg:
        _set_status(report, STATUS_PREFLIGHT_PASSED)
        return _finalize(report, raw_values, exit_code=0)

    live_mode = _has_full_live_approval(
        approve_db_read=approve_db_read,
        approve_key_read=approve_key_read,
        approve_live_openai=approve_live_openai,
        max_live_calls=max_live_calls,
    )
    db_preflight_mode = (
        approve_db_read
        and not approve_key_read
        and not approve_live_openai
        and max_live_calls == 0
    )
    if not live_mode and not db_preflight_mode:
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_all")
        return _finalize(report, raw_values, exit_code=1)
    if runtime_env_path is None:
        _set_status(report, STATUS_NOT_APPROVED, "runtime_env_path.required")
        return _finalize(report, raw_values, exit_code=1)

    raw_values.update(_raw_values(runtime_env_path))
    session: AsyncSessionLike | None = None
    try:
        try:
            values = _read_runtime_env(
                runtime_env_path,
                runtime_env_reader,
                include_openai=live_mode,
            )
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "runtime_env.read")
            return _finalize(report, raw_values, exit_code=1)

        database_url = str(values.get("DATABASE_URL", "")).strip()
        raw_values.update(_raw_values(database_url))
        report["database_configured"] = bool(
            database_url and _database_url_is_supported(database_url)
        )
        if not report["database_configured"]:
            _set_status(report, STATUS_DB_READ_FAILED, "database.config")
            return _finalize(report, raw_values, exit_code=1)

        try:
            session = await _open_database_session(database_url, database_session_factory)
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only = _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
            if not _transaction_read_only_enabled(read_only):
                _set_status(report, STATUS_DB_READ_FAILED, "database.read_only")
                return _finalize(report, raw_values, exit_code=1)
            report["read_only_transaction"] = True
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(report, STATUS_DB_READ_FAILED, "database.required_tables")
                return _finalize(report, raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "database.connection_or_schema")
            return _finalize(report, raw_values, exit_code=1)

        try:
            candidate, ambiguous = await _select_target_candidate(
                session=session,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "candidate.select")
            return _finalize(report, raw_values, exit_code=1)

        if ambiguous:
            report["target_replay_candidate_found_bucket"] = "multiple"
            _set_status(report, STATUS_DB_READ_FAILED, "candidate.ambiguous_recency")
            return _finalize(report, raw_values, exit_code=1)
        if candidate is None:
            report["target_replay_candidate_found_bucket"] = "zero"
            _set_status(report, STATUS_DB_READ_FAILED, "candidate.none")
            return _finalize(report, raw_values, exit_code=1)

        report["target_replay_candidate_found_bucket"] = "one"
        _apply_target_buckets(report, candidate)
        await _apply_target_counts(report=report, session=session, candidate=candidate)

        context = await _build_real_context(
            report=report,
            session=session,
            candidate=candidate,
            raw_values=raw_values,
        )
        if context is None:
            return _finalize(report, raw_values, exit_code=1)

        if not live_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        raw_values.update(_raw_values(*(values.get(key, "") for key in OPENAI_ENV_KEYS)))
        key_material = _resolve_key_material(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if key_material is None:
            return _finalize(report, raw_values, exit_code=1)
        api_key, project = key_material

        return await _run_live_call(
            report=report,
            request=context.request,
            api_key=api_key,
            project=project,
            sdk_loader=sdk_loader,
            raw_values=raw_values,
        )
    except Exception:
        _set_status(report, STATUS_CONTEXT_FAILED, "unexpected")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        if session is not None:
            await _safe_session_close(session)


def _build_default_preflight_request() -> dict[str, Any]:
    return OpenAIJudgeClient.build_request(
        model=LOCKED_HOT_PATH_MODEL,
        reasoning_effort=DEFAULT_REASONING_EFFORT,
        developer_prompt=DIAGNOSTIC_DEVELOPER_PROMPT,
        user_context=DIAGNOSTIC_USER_CONTEXT,
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
        prompt_cache_key=DEFAULT_PROMPT_CACHE_KEY,
    )


async def _build_real_context(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    candidate: ReplayLiveSmokeCandidate,
    raw_values: set[str],
) -> RealContextBuild | None:
    repository = JudgeOpenAIRepository(session)  # read-only methods only
    try:
        judge_run = await repository.load_judge_run(candidate.judge_run_id)
    except Exception:
        _set_status(report, STATUS_CONTEXT_FAILED, "judge_run.load")
        return None
    if judge_run is None:
        _set_status(report, STATUS_CONTEXT_FAILED, "judge_run.missing")
        return None
    raw_values.update(
        _raw_values(
            judge_run.judge_run_id,
            judge_run.bundle_id,
            judge_run.judge_profile,
            judge_run.model,
            judge_run.reasoning_effort,
            judge_run.prompt_version,
            judge_run.prompt_cache_key,
        )
    )
    if judge_run.bundle_id != candidate.bundle_id:
        _set_status(report, STATUS_CONTEXT_FAILED, "judge_run.bundle_mismatch")
        return None

    try:
        bundle = await repository.load_bundle_context(judge_run.bundle_id)
    except Exception:
        _set_status(report, STATUS_CONTEXT_FAILED, "bundle.load")
        return None
    if bundle is None:
        _set_status(report, STATUS_CONTEXT_FAILED, "bundle.missing")
        return None

    report["bundle_context_loaded_bucket"] = "one"
    raw_values.update(
        _collect_raw_strings(
            bundle.bundle_id,
            bundle.candidate_group_id,
            bundle.current_primary_artifact_id,
            bundle.primary_summary,
            bundle.supporting_summaries_json,
            bundle.discovered_links_summary_json,
            bundle.evidence_limitations,
        )
    )
    _apply_bundle_buckets(report, bundle)
    if not bundle.is_structurally_usable():
        _set_status(report, STATUS_CONTEXT_FAILED, "bundle.structurally_unusable")
        return None

    try:
        developer_prompt = PromptLibrary().render(
            judge_profile=judge_run.judge_profile,
            prompt_version=judge_run.prompt_version,
        )
        report["prompt_rendered_bucket"] = "one"
    except UnsupportedJudgeProfileError:
        _set_status(report, STATUS_CONTEXT_FAILED, "prompt.render")
        return None

    try:
        prepared = JudgeContextBuilder(preflight=NoopModelContextPreflight()).build(
            developer_prompt=developer_prompt,
            bundle=bundle,
        )
        report["context_builder_bucket"] = "one"
    except Exception:
        _set_status(report, STATUS_CONTEXT_FAILED, "context_builder.build")
        return None

    raw_values.update(_raw_values(prepared.developer_prompt, prepared.user_context))
    report["developer_prompt_size_bucket"] = _bucket_size_chars(
        len(prepared.developer_prompt)
    )
    report["user_context_size_bucket"] = _bucket_size_chars(len(prepared.user_context))
    request = OpenAIJudgeClient.build_request(
        model=judge_run.model,
        reasoning_effort=judge_run.reasoning_effort,
        developer_prompt=prepared.developer_prompt,
        user_context=prepared.user_context,
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=None,
        prompt_cache_key=judge_run.prompt_cache_key,
    )
    _merge_request_shape_report(report, summarize_responses_request_shape(request))
    if report["request_shape_valid_bucket"] != "one":
        _set_status(report, STATUS_CONTEXT_FAILED, "request_shape.invalid")
        return None

    return RealContextBuild(request=request)


async def _run_live_call(
    *,
    report: dict[str, Any],
    request: Mapping[str, Any],
    api_key: str,
    project: str | None,
    sdk_loader: SdkLoader | None,
    raw_values: set[str],
) -> ScriptResult:
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
        response = await create(**dict(request))
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


def parse_runtime_env_file(path: str | Path, *, include_openai: bool = False) -> dict[str, str]:
    return parse_runtime_env_text(
        Path(path).read_text(encoding="utf-8", errors="replace"),
        include_openai=include_openai,
    )


def parse_runtime_env_text(text: str, *, include_openai: bool = False) -> dict[str, str]:
    allowed_keys = set(DATABASE_ENV_KEYS)
    if include_openai:
        allowed_keys.update(OPENAI_ENV_KEYS)
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if key in allowed_keys:
            values[key] = _strip_optional_quotes(raw_value)
    return values


def _read_runtime_env(
    path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
    *,
    include_openai: bool,
) -> Mapping[str, str]:
    if runtime_env_reader is None:
        return parse_runtime_env_file(path, include_openai=include_openai)
    raw_values = runtime_env_reader(path)
    allowed_keys = set(DATABASE_ENV_KEYS)
    if include_openai:
        allowed_keys.update(OPENAI_ENV_KEYS)
    return {key: str(raw_values.get(key, "")) for key in allowed_keys if key in raw_values}


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _database_url_is_supported(database_url: str) -> bool:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not match:
        return False
    scheme = match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


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
        raw_values.update(_raw_values(key_file))
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


def _load_sdk() -> SdkImports:
    try:
        from openai import AsyncOpenAI  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return SdkImports(async_openai=None)
    return SdkImports(async_openai=AsyncOpenAI)


async def _open_default_database_session(database_url: str) -> AsyncSessionLike:
    from sqlalchemy.ext.asyncio import (  # type: ignore[import-not-found]
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return _DefaultDatabaseSession(engine, session_factory())


async def _open_database_session(
    database_url: str,
    database_session_factory: DatabaseSessionFactory | None,
) -> AsyncSessionLike:
    if database_session_factory is not None:
        return await _maybe_await(database_session_factory(database_url))
    return await _open_default_database_session(database_url)


async def _check_required_tables(session: AsyncSessionLike) -> bool:
    for table in REQUIRED_TABLES:
        available = bool(
            _scalar(
                await _execute(
                    session,
                    TABLE_AVAILABLE_QUERY,
                    {"qualified_table_name": f"public.{table}"},
                )
            )
        )
        if not available:
            return False
    return True


async def _select_target_candidate(
    *,
    session: AsyncSessionLike,
    raw_values: set[str],
) -> tuple[ReplayLiveSmokeCandidate | None, bool]:
    result = await _execute(
        session,
        SELECT_REPLAY_LIVE_SMOKE_CANDIDATES_QUERY,
        {
            "replay_prompt_like": f"%{REPLAY_PROMPT_SUFFIX}",
            "replay_reason_code": REPLAY_REASON_CODE,
            "limit": TARGET_QUERY_LIMIT,
        },
    )
    candidates = [_candidate_from_mapping(row) for row in _rows(result)]
    for candidate in candidates:
        raw_values.update(
            _raw_values(
                candidate.judge_run_id,
                candidate.bundle_id,
                candidate.judge_call_requested_event_id,
            )
        )
    if not candidates:
        return None, False
    if len(candidates) > 1 and _same_recency(candidates[0], candidates[1]):
        return None, True
    return candidates[0], False


def _candidate_from_mapping(row: Mapping[str, Any]) -> ReplayLiveSmokeCandidate:
    return ReplayLiveSmokeCandidate(
        judge_run_id=_coerce_uuid(row["judge_run_id"]),
        bundle_id=_coerce_uuid(row["bundle_id"]),
        status=str(row.get("judge_run_status", "")),
        finish_reason=(
            str(row["finish_reason"]) if row.get("finish_reason") is not None else None
        ),
        judge_call_requested_event_id=_coerce_uuid(row["judge_call_requested_event_id"]),
        recency_at=row.get("recency_at"),
        judge_call_requested_created_at=row.get("judge_call_requested_created_at"),
    )


def _same_recency(first: ReplayLiveSmokeCandidate, second: ReplayLiveSmokeCandidate) -> bool:
    return (
        first.recency_at == second.recency_at
        and first.judge_call_requested_created_at == second.judge_call_requested_created_at
    )


async def _apply_target_counts(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    candidate: ReplayLiveSmokeCandidate,
) -> None:
    params = {"judge_run_id": str(candidate.judge_run_id)}
    output_count = _safe_count(
        _scalar(await _execute(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params))
    )
    ready_count = _safe_count(
        _scalar(await _execute(session, COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY, params))
    )
    call_count = _safe_count(
        _scalar(await _execute(session, COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY, params))
    )
    report["existing_judge_outputs_for_run_bucket"] = _bucket_count(output_count)
    report["existing_judge_output_ready_outbox_for_run_bucket"] = _bucket_count(ready_count)
    report["target_judge_call_requested_outbox_bucket"] = _bucket_count(call_count)


def _apply_target_buckets(report: dict[str, Any], candidate: ReplayLiveSmokeCandidate) -> None:
    report["target_judge_run_status_bucket"] = _judge_run_status_bucket(candidate.status)
    report["target_finish_reason_bucket"] = _finish_reason_bucket(candidate.finish_reason)


def _apply_bundle_buckets(report: dict[str, Any], bundle: Any) -> None:
    report["bundle_structurally_usable_bucket"] = (
        "one" if bundle.is_structurally_usable() else "zero"
    )
    report["evidence_limitations_count_bucket"] = _bucket_count(
        len(bundle.evidence_limitations)
    )
    report["supporting_summary_count_bucket"] = _bucket_count(
        len(bundle.supporting_summaries_json)
    )
    report["discovered_link_count_bucket"] = _bucket_count(
        len(bundle.discovered_links_summary_json)
    )
    report["token_budget_profile_bucket"] = _token_budget_profile_bucket(
        bundle.token_budget_profile
    )


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


async def _execute(
    session: AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(sa.text(statement), params or {})


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "all"):
            return list(mappings.all())
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return result
    return list(result)


def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    if hasattr(result, "scalar_one"):
        return result.scalar_one()
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if isinstance(first, (tuple, list)):
        return first[0] if first else None
    if hasattr(first, "_mapping"):
        return next(iter(first._mapping.values()))
    if isinstance(first, Mapping):
        return next(iter(first.values()))
    return first


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


async def _safe_session_close(session: AsyncSessionLike) -> None:
    try:
        await _maybe_await(session.rollback())
    finally:
        await _maybe_await(session.close())


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


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _judge_run_status_bucket(status: str) -> str:
    normalized = status.strip().lower()
    if normalized in ALLOWED_STATUS_BUCKETS:
        return normalized
    if normalized:
        return "other"
    return "zero"


def _finish_reason_bucket(finish_reason: Any) -> str:
    if finish_reason is None:
        return "zero"
    normalized = str(finish_reason).strip().lower()
    if not normalized:
        return "zero"
    if normalized in ALLOWED_TARGET_FINISH_REASON_BUCKETS:
        return normalized
    return "other_sanitized"


def _token_budget_profile_bucket(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return "zero"
    if normalized in {"small", "medium", "large", "xlarge"}:
        return normalized
    return "other"


def _bucket_size_chars(count: int) -> str:
    if count <= 0:
        return "zero"
    if count <= 4000:
        return "small"
    if count <= 12000:
        return "medium"
    if count <= 30000:
        return "large"
    return "xlarge"


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def _has_full_live_approval(
    *,
    approve_db_read: bool,
    approve_key_read: bool,
    approve_live_openai: bool,
    max_live_calls: int,
) -> bool:
    return (
        approve_db_read
        and approve_key_read
        and approve_live_openai
        and max_live_calls == 1
    )


def _classify_openai_exception(exc: Exception) -> dict[str, Any]:
    status_code = getattr(exc, "status_code", None)
    status_int = status_code if isinstance(status_code, int) else None
    name = type(exc).__name__
    hint = _exception_hint(exc)
    message_hint_buckets = _openai_error_message_hint_buckets(hint)

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
        "openai_error_code_bucket": _openai_error_code_bucket(exc),
        "openai_error_param_bucket": _openai_error_param_bucket(exc),
        "openai_error_message_hint_count_bucket": _bucket_count(
            len(message_hint_buckets)
        ),
        "openai_error_message_hint_buckets": message_hint_buckets,
        "response_parse_bucket": "zero",
        "structured_output_observed_bucket": "zero",
        "usage_present_bucket": "zero",
    }


def _exception_hint(exc: Exception) -> str:
    parts: list[str] = [type(exc).__name__]
    text = str(exc)
    if text:
        parts.append(text)
    for attr in ("type", "code", "param", "message"):
        value = _openai_error_field(exc, attr)
        if value:
            parts.append(value)
    return " ".join(parts)


def _openai_error_code_bucket(exc: Exception) -> str:
    return _bucket_from_patterns(
        _openai_error_field(exc, "code"),
        OPENAI_ERROR_CODE_BUCKET_PATTERNS,
    )


def _openai_error_param_bucket(exc: Exception) -> str:
    return _bucket_from_patterns(
        _openai_error_field(exc, "param"),
        OPENAI_ERROR_PARAM_BUCKET_PATTERNS,
    )


def _openai_error_message_hint_buckets(text: str) -> list[str]:
    return [
        bucket
        for bucket, pattern in OPENAI_ERROR_MESSAGE_HINT_PATTERNS
        if pattern.search(text)
    ]


def _openai_error_field(exc: Exception, field: str) -> str:
    value = getattr(exc, field, None)
    if isinstance(value, str) and value.strip():
        return value.strip()
    body = getattr(exc, "body", None)
    body_value = _mapping_field(body, field)
    if body_value:
        return body_value
    if isinstance(body, Mapping):
        nested_error = body.get("error")
        nested_value = _mapping_field(nested_error, field)
        if nested_value:
            return nested_value
    return ""


def _mapping_field(value: Any, field: str) -> str:
    if not isinstance(value, Mapping):
        return ""
    raw = value.get(field)
    return raw.strip() if isinstance(raw, str) and raw.strip() else ""


def _bucket_from_patterns(
    value: str,
    patterns: Sequence[tuple[str, re.Pattern[str]]],
) -> str:
    if not value:
        return "zero"
    for bucket, pattern in patterns:
        if pattern.search(value):
            return bucket
    return "other_sanitized"


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


def _base_report(
    *,
    approve_db_read: bool,
    approve_key_read: bool,
    approve_live_openai: bool,
    max_live_calls: int,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_APPROVED,
        "approval_db_read_bucket": "one" if approve_db_read else "zero",
        "approval_key_read_bucket": "one" if approve_key_read else "zero",
        "approval_live_openai_bucket": "one" if approve_live_openai else "zero",
        "max_live_calls_bucket": _bucket_count(max_live_calls),
        "runtime_env_read": False,
        "database_configured": False,
        "database_connected": False,
        "read_only_transaction": False,
        "database_write_attempted": False,
        "target_replay_candidate_found_bucket": "zero",
        "target_judge_run_status_bucket": "zero",
        "target_finish_reason_bucket": "zero",
        "target_judge_call_requested_outbox_bucket": "zero",
        "existing_judge_outputs_for_run_bucket": "zero",
        "existing_judge_output_ready_outbox_for_run_bucket": "zero",
        "bundle_context_loaded_bucket": "zero",
        "bundle_structurally_usable_bucket": "zero",
        "prompt_rendered_bucket": "zero",
        "context_builder_bucket": "zero",
        "developer_prompt_size_bucket": "zero",
        "user_context_size_bucket": "zero",
        "evidence_limitations_count_bucket": "zero",
        "supporting_summary_count_bucket": "zero",
        "discovered_link_count_bucket": "zero",
        "token_budget_profile_bucket": "zero",
        "request_shape_valid_bucket": "zero",
        "request_shape_issue_count_bucket": "zero",
        "request_shape_issue_buckets": [],
        "openai_key_source_bucket": "zero",
        "openai_key_read_bucket": "zero",
        "openai_key_file_read_bucket": "zero",
        "openai_project_present_bucket": "zero",
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
        "openai_error_code_bucket": "zero",
        "openai_error_param_bucket": "zero",
        "openai_error_message_hint_count_bucket": "zero",
        "openai_error_message_hint_buckets": [],
        "response_parse_bucket": "zero",
        "structured_output_observed_bucket": "zero",
        "usage_present_bucket": "zero",
        "latency_ms_present_bucket": "zero",
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
    for key in REQUEST_SHAPE_REPORT_KEYS:
        if key not in summary:
            continue
        value = summary[key]
        report[key] = list(value) if isinstance(value, list) else value


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _raw_values(*values: object) -> set[str]:
    raw: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value)
        if len(text) >= 6:
            raw.add(text)
    return raw


def _collect_raw_strings(*values: Any) -> set[str]:
    raw: set[str] = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            raw.update(_collect_raw_strings(*value.values()))
            continue
        if isinstance(value, (list, tuple, set)):
            raw.update(_collect_raw_strings(*value))
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
        STATUS_DB_PREFLIGHT_PASSED,
        STATUS_LIVE_SUCCEEDED,
        STATUS_NOT_APPROVED,
        STATUS_DB_READ_FAILED,
        STATUS_CONTEXT_FAILED,
        STATUS_LIVE_FAILED,
        STATUS_RAW_VALUE_EMISSION,
        REPLAY_PROMPT_SUFFIX,
        REPLAY_REASON_CODE,
        JUDGE_CALL_REQUESTED_EVENT_TYPE,
        JUDGE_OUTPUT_READY_EVENT_TYPE,
        EXPECTED_QUEUE_NAME,
        EXPECTED_STAGE_NAME,
        EXPECTED_AGGREGATE_TYPE,
        "pending",
        "running",
        "succeeded",
        "failed_terminal",
        "failed_retryable",
        "openai_permanent_error",
        "openai_request_shape_invalid",
        "openai_transport_retryable",
        "schema_invalid_after_retry",
        "other_sanitized",
        "zero",
        "one",
        "multiple",
        "small",
        "medium",
        "large",
        "xlarge",
        "other",
        "success",
        "api_status_error",
        "api_connection_error",
        "timeout",
        "rate_limit",
        "authentication",
        "permission",
        "model_access",
        "invalid_request",
        "schema_rejected",
        "2xx",
        "400",
        "401",
        "403",
        "404",
        "408",
        "409",
        "422",
        "429",
        "5xx",
        "env",
        "file",
        "both_conflict",
        "missing",
        "authentication_error",
        "permission_error",
        "invalid_request_error",
        "rate_limit_error",
        "server_error",
        "api_timeout_error",
        "json_schema",
        "response_format",
        "unsupported_parameter",
        "unsupported_value",
        "invalid_type",
        "missing_required_parameter",
        "context_length_exceeded",
        "model_not_found",
        "model_parameter",
        "model_access",
        "rate_limit",
        "insufficient_quota",
        "invalid_api_key",
        "optional_null",
        "max_output_tokens",
        "prompt_cache_key",
        "text_format",
        "input_context",
        "tools",
        "reasoning",
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
        approve_db_read=args.approve_db_read,
        approve_key_read=args.approve_key_read,
        approve_live_openai=args.approve_live_openai,
        max_live_calls=args.max_live_calls,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
