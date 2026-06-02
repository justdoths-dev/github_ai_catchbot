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
from src.services.judge_openai.models import (  # noqa: E402
    BundleJudgeContext,
    JudgeCallJob,
    JudgeRunRecord,
    OpenAIJudgeResult,
)
from src.services.judge_openai.openai_client import (  # noqa: E402
    OpenAIJudgeClient,
    OpenAIPermanentError,
    OpenAIRequestShapeError,
    OpenAITransientError,
)
from src.services.judge_openai.preflight import NoopModelContextPreflight  # noqa: E402
from src.services.judge_openai.prompt_library import (  # noqa: E402
    PromptLibrary,
    UnsupportedJudgeProfileError,
)
from src.services.judge_openai.repositories import JudgeOpenAIRepository  # noqa: E402
from src.services.judge_openai.request_shape import (  # noqa: E402
    summarize_responses_request_shape,
)
from src.services.judge_openai.response_mapper import OpenAIResponseMapper  # noqa: E402
from src.services.judge_openai.service import JudgeOpenAIService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_real_context_service_path_persistence_smoke"
REPORT_TYPE = "judge_openai_real_context_service_path_persistence_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
REPLAY_PROMPT_SUFFIX = "__replay_live_smoke_v1"
REPLAY_REASON_CODE = "manual_live_smoke_replay"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
DEFAULT_TIMEOUT_SECONDS = 60.0

STATUS_PREFLIGHT_PASSED = (
    "judge_openai_real_context_service_path_persistence_smoke_preflight_passed"
)
STATUS_DB_PREFLIGHT_PASSED = (
    "judge_openai_real_context_service_path_persistence_smoke_db_read_preflight_passed"
)
STATUS_DB_WRITE_PASSED = (
    "judge_openai_real_context_service_path_persistence_smoke_approved_db_write_passed"
)
STATUS_NOT_APPROVED = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_not_approved"
)
STATUS_DB_READ_FAILED = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_db_read_failed"
)
STATUS_KEY_NOT_READY = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_key_not_ready"
)
STATUS_CONTEXT_FAILED = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_context_failed"
)
STATUS_DUPLICATE_OUTPUT = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_existing_output"
)
STATUS_DUPLICATE_READY_OUTBOX = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_existing_ready_outbox"
)
STATUS_FORBIDDEN_SIDE_EFFECT = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_forbidden_side_effect"
)
STATUS_LIVE_CALL_FAILED = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_live_call_failed"
)
STATUS_WRITE_FAILED = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_write_failed"
)
STATUS_RAW_VALUE_EMISSION = (
    "blocked_judge_openai_real_context_service_path_persistence_smoke_raw_value_emission"
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
SELECT_PENDING_SERVICE_PATH_CANDIDATES_QUERY = """
SELECT DISTINCT ON (jr.judge_run_id)
       jr.judge_run_id,
       jr.bundle_id,
       jr.status AS judge_run_status,
       jr.finish_reason,
       eo.event_id AS judge_call_requested_event_id,
       eo.created_at AS judge_call_requested_created_at,
       COALESCE(jr.started_at, eo.created_at) AS recency_at
FROM judge_runs jr
JOIN event_outbox eo
  ON eo.event_type = 'judge.call.requested.v1'
 AND eo.aggregate_type = 'judge_run'
 AND eo.aggregate_id = jr.judge_run_id
WHERE jr.status = 'pending'
ORDER BY jr.judge_run_id,
         eo.created_at DESC NULLS LAST,
         eo.event_id DESC
LIMIT :limit
"""
SELECT_FAILED_REPLAY_SERVICE_PATH_CANDIDATES_QUERY = """
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
      AND jr.status IN ('failed_terminal', 'failed_retryable')
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
COUNT_ANALYSES_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM analyses a
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_POLICY_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.policy.apply.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
"""
COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM notification_plans np
JOIN analyses a ON a.analysis_id = np.analysis_id
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
SELECT_JUDGE_RUN_FINISH_STATE_QUERY = """
SELECT status, refusal_detected
FROM judge_runs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""

DATABASE_ENV_KEYS = frozenset({"DATABASE_URL"})
OPENAI_ENV_KEYS = frozenset(
    {
        "OPENAI_API_KEY",
        "OPENAI_API_KEY_FILE",
        "OPENAI_PROJECT",
        "JUDGE_OPENAI_REQUEST_TIMEOUT_SEC",
        "JUDGE_MAX_OUTPUT_TOKENS",
    }
)
PUBLIC_LITERAL_VALUES = frozenset(
    {
        SCRIPT_NAME,
        REPORT_TYPE,
        JUDGE_CALL_REQUESTED_EVENT_TYPE,
        JUDGE_OUTPUT_READY_EVENT_TYPE,
        "judge",
        "judge_run",
        "pending",
        "failed_terminal",
        "failed_retryable",
        "succeeded",
        "gpt-5.4-mini",
        "gpt-5.4",
        "zero",
        "one",
        "multiple",
    }
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


class OpenAIClientLike(Protocol):
    async def create_structured_response(self, **kwargs: Any) -> Any: ...


RuntimeEnvReader = Callable[[str | Path, bool], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
OpenAIClientFactory = Callable[[str, str | None, float], OpenAIClientLike]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ServicePathTarget:
    judge_run_id: UUID
    bundle_id: UUID
    trigger_event_id: UUID
    status: str
    finish_reason: str | None
    target_source: str


@dataclass(frozen=True, slots=True)
class PreparedServicePath:
    target: ServicePathTarget
    job: JudgeCallJob
    judge_run: JudgeRunRecord
    bundle: BundleJudgeContext
    developer_prompt: str
    user_context: str
    request: dict[str, Any]


class _DefaultDatabaseSession:
    def __init__(self, engine: Any, session: Any) -> None:
        self._engine = engine
        self._session = session

    def in_transaction(self) -> bool:
        return bool(self._session.in_transaction())

    def begin(self) -> Any:
        return self._session.begin()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        return await self._session.execute(statement, params or {})

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


class _SingleCallOpenAIClient:
    def __init__(self, inner: OpenAIClientLike) -> None:
        self._inner = inner
        self.live_calls = 0
        self.blocked_second_call = False

    async def create_structured_response(self, **kwargs: Any) -> Any:
        if self.live_calls >= 1:
            self.blocked_second_call = True
            raise OpenAIPermanentError("single_live_call_contract")
        self.live_calls += 1
        return await self._inner.create_structured_response(**kwargs)

    @property
    def attempted_count(self) -> int:
        return self.live_calls + int(self.blocked_second_call)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated judge-openai real-context service-path persistence smoke. "
            "Default mode is local-only and reads no runtime env, DB, key, Redis, or OpenAI."
        )
    )
    parser.add_argument("--approve-db-read", action="store_true")
    parser.add_argument("--approve-db-write", action="store_true")
    parser.add_argument("--approve-key-read", action="store_true")
    parser.add_argument("--approve-live-openai", action="store_true")
    parser.add_argument("--max-live-calls", type=int, default=0)
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report(
    *,
    approve_db_read: bool,
    approve_db_write: bool,
    approve_key_read: bool,
    approve_live_openai: bool,
    max_live_calls: int,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_PREFLIGHT_PASSED,
        "approvals": {
            "db_read": approve_db_read,
            "db_write": approve_db_write,
            "key_read": approve_key_read,
            "live_openai": approve_live_openai,
            "max_live_calls_bucket": _bucket_count(max_live_calls),
        },
        "runtime_env_read": False,
        "database_configured": False,
        "database_connected": False,
        "read_only_transaction": False,
        "database_write_attempted": False,
        "redis_connected": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "target_candidate_found_bucket": "zero",
        "target_source_bucket": "zero",
        "target_judge_run_found_bucket": "zero",
        "target_bundle_found_bucket": "zero",
        "target_judge_run_status_bucket": "zero",
        "target_finish_reason_bucket": "zero",
        "target_judge_call_requested_outbox_bucket": "zero",
        "event_outbox_rehydrated_bucket": "zero",
        "event_type_is_judge_call_requested": False,
        "judge_run_linked": False,
        "bundle_structurally_usable_bucket": "zero",
        "prompt_rendered_bucket": "zero",
        "context_builder_bucket": "zero",
        "stored_prompt_cache_key_presence_bucket": "zero",
        "prompt_cache_key_transport_policy_bucket": "disabled",
        "request_shape_valid_bucket": "zero",
        "request_shape_issue_count_bucket": "zero",
        "request_shape_issue_buckets": [],
        "top_level_request_key_presence_buckets": {},
        "optional_null_field_count_bucket": "zero",
        "optional_null_field_name_buckets": [],
        "max_output_tokens_presence_bucket": "zero",
        "max_output_tokens_null_bucket": "zero",
        "prompt_cache_key_presence_bucket": "zero",
        "text_format_type_bucket": "zero",
        "json_schema_strict_bucket": "zero",
        "tools_count_bucket": "zero",
        "existing_judge_outputs_for_run_bucket": "zero",
        "existing_judge_output_ready_outbox_for_run_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "judge_run_updated_bucket": "zero",
        "judge_output_ready_outbox_written_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "notification_rows_written_bucket": "zero",
        "q_analysis_policy_published": False,
        "analysis_validator_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "direct_openai_api_key_present": False,
        "openai_key_file_configured": False,
        "openai_key_read_bucket": "zero",
        "openai_key_file_read_bucket": "zero",
        "openai_project_present_bucket": "zero",
        "openai_call_attempted": False,
        "live_openai_call_attempted": False,
        "live_openai_call_attempted_bucket": "zero",
        "live_openai_call_completed_bucket": "zero",
        "live_result_class_bucket": "zero",
        "http_status_bucket": "zero",
        "response_parse_bucket": "zero",
        "structured_output_observed_bucket": "zero",
        "usage_present_bucket": "zero",
        "service_path_adapter_used": True,
        "production_handle_job_called": False,
        "production_persistence_methods_reused": True,
        "raw_values_emitted": False,
        "checks_failed": [],
    }


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


def _bucket_size_chars(count: int) -> str:
    if count <= 0:
        return "zero"
    if count < 2048:
        return "small"
    if count < 12000:
        return "medium"
    return "large"


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


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
        if key not in allowed_keys:
            continue
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            values[key] = _strip_optional_quotes(raw_value)
    return values


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


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


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


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "first"):
            return mappings.first()
        rows = list(mappings.all())
        return rows[0] if rows else None
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if hasattr(first, "_mapping"):
        return first._mapping
    return first if isinstance(first, Mapping) else None


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


def _coerce_uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _raw_values(*values: Any) -> set[str]:
    raw: set[str] = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            raw.update(_raw_values(*value.values()))
            continue
        if isinstance(value, (list, tuple, set)):
            raw.update(_raw_values(*value))
            continue
        text = str(value)
        if len(text) >= 6 and text not in PUBLIC_LITERAL_VALUES:
            raw.add(text)
    return raw


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values if len(value) >= 6)


async def _count_query(
    session: AsyncSessionLike,
    query: str,
    params: dict[str, Any] | None = None,
) -> int:
    return _safe_count(_scalar(await _execute(session, query, params or {})))


async def _select_target_candidate(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    raw_values: set[str],
) -> tuple[ServicePathTarget | None, str | None]:
    pending_rows = _rows(
        await _execute(
            session,
            SELECT_PENDING_SERVICE_PATH_CANDIDATES_QUERY,
            {"limit": 2},
        )
    )
    if len(pending_rows) > 1:
        report["target_candidate_found_bucket"] = "multiple"
        report["target_source_bucket"] = "pending_candidate"
        raw_values.update(_raw_values(pending_rows))
        return None, "candidate.pending_ambiguous"
    if len(pending_rows) == 1:
        report["target_candidate_found_bucket"] = "one"
        report["target_source_bucket"] = "pending_candidate"
        return _target_from_row(pending_rows[0], target_source="pending_candidate", raw_values=raw_values), None

    failed_rows = _rows(
        await _execute(
            session,
            SELECT_FAILED_REPLAY_SERVICE_PATH_CANDIDATES_QUERY,
            {
                "limit": 2,
                "replay_prompt_like": f"%{REPLAY_PROMPT_SUFFIX}",
                "replay_reason_code": REPLAY_REASON_CODE,
            },
        )
    )
    if len(failed_rows) > 1:
        report["target_candidate_found_bucket"] = "multiple"
        report["target_source_bucket"] = "failed_replay_real_context"
        raw_values.update(_raw_values(failed_rows))
        return None, "candidate.failed_replay_ambiguous"
    if len(failed_rows) == 1:
        report["target_candidate_found_bucket"] = "one"
        report["target_source_bucket"] = "failed_replay_real_context"
        return _target_from_row(
            failed_rows[0],
            target_source="failed_replay_real_context",
            raw_values=raw_values,
        ), None

    report["target_candidate_found_bucket"] = "zero"
    return None, "candidate.none"


def _target_from_row(
    row: Any,
    *,
    target_source: str,
    raw_values: set[str],
) -> ServicePathTarget | None:
    mapping = row._mapping if hasattr(row, "_mapping") else row
    if not isinstance(mapping, Mapping):
        return None
    judge_run_id = _coerce_uuid(mapping.get("judge_run_id"))
    bundle_id = _coerce_uuid(mapping.get("bundle_id"))
    trigger_event_id = _coerce_uuid(mapping.get("judge_call_requested_event_id"))
    raw_values.update(_raw_values(judge_run_id, bundle_id, trigger_event_id))
    if judge_run_id is None or bundle_id is None or trigger_event_id is None:
        return None
    return ServicePathTarget(
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        trigger_event_id=trigger_event_id,
        status=str(mapping.get("judge_run_status") or ""),
        finish_reason=str(mapping.get("finish_reason")) if mapping.get("finish_reason") else None,
        target_source=target_source,
    )


async def _prepare_service_path(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    target: ServicePathTarget,
    raw_values: set[str],
) -> PreparedServicePath | None:
    repository = JudgeOpenAIRepository(session)

    job = await repository.load_job_by_trigger_event_id(target.trigger_event_id)
    report["event_outbox_rehydrated_bucket"] = "one" if job is not None else "zero"
    report["event_type_is_judge_call_requested"] = bool(
        job and job.event_type == JUDGE_CALL_REQUESTED_EVENT_TYPE
    )
    if job is None or job.event_type != JUDGE_CALL_REQUESTED_EVENT_TYPE:
        _set_status(report, STATUS_CONTEXT_FAILED, "event_outbox.job")
        return None

    judge_run = await repository.load_judge_run(job.judge_run_id)
    report["target_judge_run_found_bucket"] = "one" if judge_run is not None else "zero"
    if judge_run is None:
        _set_status(report, STATUS_CONTEXT_FAILED, "judge_run.missing")
        return None
    report["target_judge_run_status_bucket"] = judge_run.status
    report["target_finish_reason_bucket"] = target.finish_reason or "zero"
    report["judge_run_linked"] = (
        job.judge_run_id == target.judge_run_id
        and judge_run.judge_run_id == target.judge_run_id
        and job.bundle_id == target.bundle_id
        and judge_run.bundle_id == target.bundle_id
    )
    raw_values.update(
        _raw_values(
            job.judge_run_id,
            job.bundle_id,
            job.trigger_event_id,
            judge_run.judge_run_id,
            judge_run.bundle_id,
            judge_run.prompt_cache_key,
        )
    )
    if not report["judge_run_linked"]:
        _set_status(report, STATUS_CONTEXT_FAILED, "judge_run.link")
        return None
    if _job_conflicts_with_run(job, judge_run):
        _set_status(report, STATUS_CONTEXT_FAILED, "judge_run.locked_config")
        return None

    params = {"judge_run_id": str(target.judge_run_id)}
    call_requested_count = await _count_query(
        session,
        COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY,
        params,
    )
    report["target_judge_call_requested_outbox_bucket"] = _bucket_count(call_requested_count)

    output_count = await _count_query(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params)
    report["existing_judge_outputs_for_run_bucket"] = _bucket_count(output_count)
    if output_count:
        _set_status(report, STATUS_DUPLICATE_OUTPUT, "judge_outputs.existing")
        return None

    ready_count = await _count_query(session, COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY, params)
    report["existing_judge_output_ready_outbox_for_run_bucket"] = _bucket_count(ready_count)
    if ready_count:
        _set_status(report, STATUS_DUPLICATE_READY_OUTBOX, "event_outbox.ready_existing")
        return None

    analysis_count = await _count_query(session, COUNT_ANALYSES_FOR_RUN_QUERY, params)
    policy_count = await _count_query(session, COUNT_POLICY_OUTBOX_FOR_RUN_QUERY, params)
    notification_count = await _count_query(session, COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY, params)
    report["analysis_rows_written_bucket"] = _bucket_count(analysis_count)
    report["notification_rows_written_bucket"] = _bucket_count(notification_count)
    report["q_analysis_policy_published"] = policy_count > 0
    if analysis_count or policy_count or notification_count:
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "downstream.side_effect")
        return None

    bundle = await repository.load_bundle_context(judge_run.bundle_id)
    report["target_bundle_found_bucket"] = "one" if bundle is not None else "zero"
    if bundle is None:
        _set_status(report, STATUS_CONTEXT_FAILED, "bundle.missing")
        return None
    raw_values.update(
        _raw_values(
            bundle.bundle_id,
            bundle.candidate_group_id,
            bundle.current_primary_artifact_id,
            bundle.primary_summary,
            bundle.supporting_summaries_json,
            bundle.discovered_links_summary_json,
            bundle.evidence_limitations,
        )
    )
    report["bundle_structurally_usable_bucket"] = (
        "one" if bundle.is_structurally_usable() else "zero"
    )
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
    report["developer_prompt_size_bucket"] = _bucket_size_chars(len(prepared.developer_prompt))
    report["user_context_size_bucket"] = _bucket_size_chars(len(prepared.user_context))
    report["stored_prompt_cache_key_presence_bucket"] = (
        "one" if judge_run.prompt_cache_key else "zero"
    )
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
    report["prompt_cache_key_transport_policy_bucket"] = (
        "disabled" if "prompt_cache_key" not in request else "enabled"
    )
    if report["request_shape_valid_bucket"] != "one":
        _set_status(report, STATUS_CONTEXT_FAILED, "request_shape.invalid")
        return None

    return PreparedServicePath(
        target=target,
        job=job,
        judge_run=judge_run,
        bundle=bundle,
        developer_prompt=prepared.developer_prompt,
        user_context=prepared.user_context,
        request=request,
    )


def _job_conflicts_with_run(job: JudgeCallJob, judge_run: JudgeRunRecord) -> bool:
    return bool(
        job.model != judge_run.model
        or job.reasoning_effort != judge_run.reasoning_effort
        or job.prompt_version != judge_run.prompt_version
        or job.prompt_cache_key != judge_run.prompt_cache_key
    )


def _merge_request_shape_report(report: dict[str, Any], shape: Mapping[str, Any]) -> None:
    for key in (
        "request_shape_valid_bucket",
        "request_shape_issue_count_bucket",
        "request_shape_issue_buckets",
        "top_level_request_key_presence_buckets",
        "optional_null_field_count_bucket",
        "optional_null_field_name_buckets",
        "max_output_tokens_presence_bucket",
        "max_output_tokens_null_bucket",
        "prompt_cache_key_presence_bucket",
        "text_format_type_bucket",
        "json_schema_strict_bucket",
        "tools_count_bucket",
    ):
        if key in shape:
            report[key] = shape[key]


def _read_runtime_env(
    *,
    path: str | Path,
    include_openai: bool,
    runtime_env_reader: RuntimeEnvReader | None,
) -> Mapping[str, str]:
    if runtime_env_reader is not None:
        return runtime_env_reader(path, include_openai)
    return parse_runtime_env_file(path, include_openai=include_openai)


def _extract_database_url(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> str | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    raw_values.update(_raw_values(database_url))
    report["database_configured"] = bool(database_url and _database_url_is_supported(database_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_DB_READ_FAILED, "database.config")
        return None
    return database_url


def _resolve_key_material(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str | None, float] | None:
    direct_key = str(values.get("OPENAI_API_KEY", "")).strip()
    key_file = str(values.get("OPENAI_API_KEY_FILE", "")).strip()
    project = str(values.get("OPENAI_PROJECT", "")).strip() or None
    report["direct_openai_api_key_present"] = bool(direct_key)
    report["openai_key_file_configured"] = bool(key_file)
    report["openai_project_present_bucket"] = "one" if project else "zero"
    raw_values.update(_raw_values(direct_key, key_file, project))
    if direct_key:
        _set_status(report, STATUS_KEY_NOT_READY, "openai_key.direct_env_unsupported")
        return None
    if not key_file:
        _set_status(report, STATUS_KEY_NOT_READY, "openai_key.file_missing")
        return None

    try:
        api_key = Path(key_file).read_text(encoding="utf-8").strip()
    except OSError:
        _set_status(report, STATUS_KEY_NOT_READY, "openai_key.file_read")
        return None
    raw_values.update(_raw_values(api_key))
    if not api_key:
        _set_status(report, STATUS_KEY_NOT_READY, "openai_key.empty")
        return None
    report["openai_key_file_read_bucket"] = "one"
    report["openai_key_read_bucket"] = "one"

    timeout = DEFAULT_TIMEOUT_SECONDS
    raw_timeout = str(values.get("JUDGE_OPENAI_REQUEST_TIMEOUT_SEC", "")).strip()
    if raw_timeout:
        try:
            timeout = float(raw_timeout)
        except ValueError:
            _set_status(report, STATUS_KEY_NOT_READY, "openai.timeout")
            return None
    if timeout <= 0:
        _set_status(report, STATUS_KEY_NOT_READY, "openai.timeout")
        return None
    return api_key, project, timeout


def _build_real_openai_client(
    api_key: str,
    project: str | None,
    timeout_sec: float,
) -> OpenAIClientLike:
    return OpenAIJudgeClient(api_key=api_key, project=project, timeout_sec=timeout_sec)


async def _call_openai_once(
    *,
    report: dict[str, Any],
    prepared: PreparedServicePath,
    client: _SingleCallOpenAIClient,
) -> OpenAIJudgeResult | None:
    started = time.monotonic()
    try:
        report["openai_call_attempted"] = True
        report["live_openai_call_attempted"] = True
        response = await client.create_structured_response(
            model=prepared.judge_run.model,
            reasoning_effort=prepared.judge_run.reasoning_effort,
            developer_prompt=prepared.developer_prompt,
            user_context=prepared.user_context,
            json_schema=JudgeOpenAIService.judge_output_schema(),
            max_output_tokens=None,
            prompt_cache_key=prepared.judge_run.prompt_cache_key,
        )
    except OpenAIRequestShapeError:
        report["live_openai_call_attempted_bucket"] = _bucket_count(client.attempted_count)
        report["live_openai_call_completed_bucket"] = "zero"
        report["live_result_class_bucket"] = "request_shape_invalid"
        _set_status(report, STATUS_LIVE_CALL_FAILED, "openai.request_shape")
        return None
    except OpenAITransientError:
        report["live_openai_call_attempted_bucket"] = _bucket_count(client.attempted_count)
        report["live_openai_call_completed_bucket"] = "zero"
        report["live_result_class_bucket"] = "retryable"
        _set_status(report, STATUS_LIVE_CALL_FAILED, "openai.retryable")
        return None
    except OpenAIPermanentError:
        report["live_openai_call_attempted_bucket"] = _bucket_count(client.attempted_count)
        report["live_openai_call_completed_bucket"] = "zero"
        report["live_result_class_bucket"] = "permanent"
        _set_status(report, STATUS_LIVE_CALL_FAILED, "openai.permanent")
        return None
    except Exception:
        report["live_openai_call_attempted_bucket"] = _bucket_count(client.attempted_count)
        report["live_openai_call_completed_bucket"] = "zero"
        report["live_result_class_bucket"] = "unexpected"
        _set_status(report, STATUS_LIVE_CALL_FAILED, "openai.unexpected")
        return None

    report["live_openai_call_attempted_bucket"] = _bucket_count(client.attempted_count)
    report["live_openai_call_completed_bucket"] = "one"
    report["live_result_class_bucket"] = "success"
    report["http_status_bucket"] = "2xx"
    result = OpenAIResponseMapper().parse(response, started_monotonic=started)
    report["response_parse_bucket"] = "one"
    report["structured_output_observed_bucket"] = "one" if result.has_structured_payload else "zero"
    report["usage_present_bucket"] = (
        "one"
        if any(
            value is not None
            for value in (
                result.usage.input_tokens,
                result.usage.output_tokens,
                result.usage.cached_input_tokens,
                result.usage.reasoning_tokens,
            )
        )
        else "zero"
    )
    if not result.has_structured_payload:
        _set_status(report, STATUS_LIVE_CALL_FAILED, "openai.structured_output_missing")
        return None
    return result


async def _persist_success(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    prepared: PreparedServicePath,
    result: OpenAIJudgeResult,
) -> None:
    repository = JudgeOpenAIRepository(session)
    payload_json = result.payload_json
    if payload_json is None:
        _set_status(report, STATUS_LIVE_CALL_FAILED, "openai.structured_output_missing")
        return
    proposed_verdict = payload_json.get("model_proposed_verdict")
    confidence_band = payload_json.get("model_confidence_band")

    report["database_write_attempted"] = True
    async with repository.transaction():
        judge_output_id = await repository.insert_judge_output(
            judge_run_id=prepared.judge_run.judge_run_id,
            candidate_group_id=prepared.bundle.candidate_group_id,
            judge_schema_version=prepared.judge_run.schema_version,
            payload_json=payload_json,
            model_proposed_verdict=proposed_verdict if isinstance(proposed_verdict, str) else None,
            model_confidence_band=confidence_band if isinstance(confidence_band, str) else None,
        )
        await repository.finish_judge_run(
            judge_run_id=prepared.judge_run.judge_run_id,
            status="succeeded",
            usage=result.usage,
            finish_reason=result.finish_reason,
            refusal_detected=result.refusal_detected,
        )
        await repository.insert_judge_output_ready_outbox(
            judge_run_id=prepared.judge_run.judge_run_id,
            judge_output_id=judge_output_id,
            finish_reason=result.finish_reason,
            refusal_detected=result.refusal_detected,
        )

    await session.commit()
    params = {"judge_run_id": str(prepared.judge_run.judge_run_id)}
    output_count = await _count_query(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params)
    ready_count = await _count_query(session, COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY, params)
    analysis_count = await _count_query(session, COUNT_ANALYSES_FOR_RUN_QUERY, params)
    policy_count = await _count_query(session, COUNT_POLICY_OUTBOX_FOR_RUN_QUERY, params)
    notification_count = await _count_query(session, COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY, params)
    run_state = _first_mapping(
        await _execute(session, SELECT_JUDGE_RUN_FINISH_STATE_QUERY, params)
    )
    report["judge_outputs_written_bucket"] = _bucket_count(output_count)
    report["judge_output_ready_outbox_written_bucket"] = _bucket_count(ready_count)
    report["analysis_rows_written_bucket"] = _bucket_count(analysis_count)
    report["notification_rows_written_bucket"] = _bucket_count(notification_count)
    report["q_analysis_policy_published"] = policy_count > 0
    report["judge_run_updated_bucket"] = "one" if _run_succeeded(run_state) else "zero"


def _run_succeeded(row: Mapping[str, Any] | None) -> bool:
    return bool(row and str(row.get("status")) == "succeeded")


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["live_openai_call_attempted_bucket"] == "one"
        and report["live_openai_call_completed_bucket"] == "one"
        and report["structured_output_observed_bucket"] == "one"
        and report["judge_outputs_written_bucket"] == "one"
        and report["judge_run_updated_bucket"] == "one"
        and report["judge_output_ready_outbox_written_bucket"] == "one"
        and report["analysis_rows_written_bucket"] == "zero"
        and report["notification_rows_written_bucket"] == "zero"
        and not report["q_analysis_policy_published"]
        and not report["analysis_validator_started"]
        and not report["policy_engine_started"]
        and not report["notifier_started"]
        and not report["telegram_send_attempted"]
        and not report["redis_write_attempted"]
    )


def _has_full_live_approval(
    *,
    approve_db_read: bool,
    approve_db_write: bool,
    approve_key_read: bool,
    approve_live_openai: bool,
    max_live_calls: int,
) -> bool:
    return bool(
        approve_db_read
        and approve_db_write
        and approve_key_read
        and approve_live_openai
        and max_live_calls == 1
    )


async def generate_report_async(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    approve_key_read: bool = False,
    approve_live_openai: bool = False,
    max_live_calls: int = 0,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    openai_client_factory: OpenAIClientFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report(
        approve_db_read=approve_db_read,
        approve_db_write=approve_db_write,
        approve_key_read=approve_key_read,
        approve_live_openai=approve_live_openai,
        max_live_calls=max_live_calls,
    )
    raw_values = _raw_values(*forbidden_raw_values)
    raw_values.update(_raw_values(runtime_env_path))

    any_approval = approve_db_read or approve_db_write or approve_key_read or approve_live_openai
    if not any_approval and max_live_calls == 0:
        _merge_request_shape_report(
            report,
            summarize_responses_request_shape(
                OpenAIJudgeClient.build_request(
                    model="gpt-5.4-mini",
                    reasoning_effort="low",
                    developer_prompt="default local-only shape probe",
                    user_context="default local-only shape probe",
                    json_schema=JudgeOpenAIService.judge_output_schema(),
                    max_output_tokens=None,
                    prompt_cache_key="default:local:cache:key",
                )
            ),
        )
        report["stored_prompt_cache_key_presence_bucket"] = "one"
        report["prompt_cache_key_transport_policy_bucket"] = "disabled"
        _set_status(report, STATUS_PREFLIGHT_PASSED)
        return _finalize(report, raw_values, exit_code=0)

    db_preflight_mode = (
        approve_db_read
        and not approve_db_write
        and not approve_key_read
        and not approve_live_openai
        and max_live_calls == 0
    )
    live_mode = _has_full_live_approval(
        approve_db_read=approve_db_read,
        approve_db_write=approve_db_write,
        approve_key_read=approve_key_read,
        approve_live_openai=approve_live_openai,
        max_live_calls=max_live_calls,
    )
    if not db_preflight_mode and not live_mode:
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_mode")
        return _finalize(report, raw_values, exit_code=1)

    session: AsyncSessionLike | None = None
    committed = False
    try:
        try:
            values = _read_runtime_env(
                path=runtime_env_path,
                include_openai=False,
                runtime_env_reader=runtime_env_reader,
            )
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "runtime_env.read")
            return _finalize(report, raw_values, exit_code=1)

        database_url = _extract_database_url(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if database_url is None:
            return _finalize(report, raw_values, exit_code=1)

        try:
            session = await _open_database_session(database_url, database_session_factory)
            if db_preflight_mode:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only = _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
                if not _transaction_read_only_enabled(read_only):
                    _set_status(report, STATUS_DB_READ_FAILED, "database.read_only")
                    return _finalize(report, raw_values, exit_code=1)
                report["read_only_transaction"] = True
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "database.connection")
            return _finalize(report, raw_values, exit_code=1)

        target, target_check = await _select_target_candidate(
            report=report,
            session=session,
            raw_values=raw_values,
        )
        if target is None:
            _set_status(report, STATUS_DB_READ_FAILED, target_check or "candidate.none")
            return _finalize(report, raw_values, exit_code=1)
        prepared = await _prepare_service_path(
            report=report,
            session=session,
            target=target,
            raw_values=raw_values,
        )
        if prepared is None:
            return _finalize(report, raw_values, exit_code=1)

        if db_preflight_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        try:
            openai_values = _read_runtime_env(
                path=runtime_env_path,
                include_openai=True,
                runtime_env_reader=runtime_env_reader,
            )
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_KEY_NOT_READY, "runtime_env.key_read")
            return _finalize(report, raw_values, exit_code=1)
        key_material = _resolve_key_material(
            report=report,
            values=openai_values,
            raw_values=raw_values,
        )
        if key_material is None:
            return _finalize(report, raw_values, exit_code=1)
        api_key, project, timeout_sec = key_material

        inner_client = (
            openai_client_factory(api_key, project, timeout_sec)
            if openai_client_factory is not None
            else _build_real_openai_client(api_key, project, timeout_sec)
        )
        guarded_client = _SingleCallOpenAIClient(inner_client)
        result = await _call_openai_once(
            report=report,
            prepared=prepared,
            client=guarded_client,
        )
        if result is None:
            return _finalize(report, raw_values, exit_code=1)
        try:
            await _persist_success(
                report=report,
                session=session,
                prepared=prepared,
                result=result,
            )
            committed = True
        except Exception:
            _set_status(report, STATUS_WRITE_FAILED, "db_write.persistence")
            return _finalize(report, raw_values, exit_code=1)

        if not _approved_execution_succeeded(report):
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)
        _set_status(report, STATUS_DB_WRITE_PASSED)
        return _finalize(report, raw_values, exit_code=0)
    except Exception:
        _set_status(report, STATUS_CONTEXT_FAILED, "unexpected")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        if session is not None:
            if not committed:
                await _maybe_await(session.rollback())
            await _maybe_await(session.close())


def _finalize(report: dict[str, Any], raw_values: set[str], *, exit_code: int) -> ScriptResult:
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


def generate_report(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    approve_key_read: bool = False,
    approve_live_openai: bool = False,
    max_live_calls: int = 0,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    openai_client_factory: OpenAIClientFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            approve_db_read=approve_db_read,
            approve_db_write=approve_db_write,
            approve_key_read=approve_key_read,
            approve_live_openai=approve_live_openai,
            max_live_calls=max_live_calls,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            openai_client_factory=openai_client_factory,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = generate_report(
        approve_db_read=args.approve_db_read,
        approve_db_write=args.approve_db_write,
        approve_key_read=args.approve_key_read,
        approve_live_openai=args.approve_live_openai,
        max_live_calls=args.max_live_calls,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
