from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

import sqlalchemy as sa


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_live_call_result_audit"
REPORT_TYPE = "judge_openai_live_call_result_audit_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_SCAN_LIMIT = 100
MAX_SCAN_LIMIT = 250
TARGET_QUERY_LIMIT = 2

REPLAY_PROMPT_SUFFIX = "__replay_live_smoke_v1"
REPLAY_REASON_CODE = "manual_live_smoke_replay"
EXPECTED_QUEUE_NAME = "q.analysis.judge"
EXPECTED_STAGE_NAME = "judge"
EXPECTED_AGGREGATE_TYPE = "judge_run"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
POLICY_APPLY_EVENT_TYPE = "analysis.policy.apply.v1"
NOTIFICATION_PLAN_EVENT_TYPE = "notification.plan.created.v1"
ALLOWED_REDIS_THIN_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}
ALLOWED_FINISH_REASON_CODES = {
    "completed",
    "incomplete",
    "max_output_tokens",
    "output_truncated",
    "truncated",
    "prompt_missing",
    "bundle_missing",
    "bundle_invalid",
    "unsupported_judge_profile",
    "openai_request_shape_invalid",
    "openai_transport_retryable",
    "openai_permanent_error",
    "schema_invalid_after_retry",
    "analysis_failed_truncation",
    "model_refusal",
    "validator_missing_skeptical_take",
    "validator_missing_reason_codes",
    "validator_schema_invalid",
    "validator_missing_github_comparables",
    "validator_inspect_now_evidence_too_low",
    "validator_inspect_now_confidence_too_low",
    "validator_inspect_now_hype_too_high",
    "validator_passed",
}
SCHEMA_FAILURE_KEYWORDS = ("schema", "structured", "json_schema", "invalid_json")
PRECONDITION_FAILURE_KEYWORDS = ("prompt", "bundle", "precondition", "missing", "unsupported")
API_TRANSPORT_FAILURE_KEYWORDS = (
    "openai",
    "api",
    "transport",
    "timeout",
    "network",
    "rate_limit",
    "rate-limit",
    "429",
    "500",
    "502",
    "503",
    "504",
)
RESPONSE_MAPPING_FAILURE_KEYWORDS = (
    "parse",
    "parser",
    "mapping",
    "mapper",
    "decode",
    "response_map",
    "response mapping",
)

STATUS_PASSED = "judge_openai_live_call_result_audit_passed"
STATUS_NO_CANDIDATE = "blocked_judge_openai_live_call_result_audit_no_candidate"
STATUS_AMBIGUOUS_CANDIDATE = (
    "blocked_judge_openai_live_call_result_audit_ambiguous_candidate"
)
STATUS_NOT_READY = "blocked_judge_openai_live_call_result_audit_not_ready"
STATUS_RAW_VALUE_EMISSION = (
    "blocked_judge_openai_live_call_result_audit_raw_value_emission"
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
           jr.schema_retry_count,
           jr.refusal_detected,
           jr.started_at,
           jr.finished_at,
           jr.finish_reason,
           jr.input_tokens,
           jr.cached_input_tokens,
           jr.output_tokens,
           jr.reasoning_tokens,
           jr.latency_ms,
           eo.event_id AS judge_call_requested_event_id,
           eo.status AS judge_call_requested_status,
           eo.fail_count AS judge_call_requested_fail_count,
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
       schema_retry_count,
       refusal_detected,
       started_at,
       finished_at,
       finish_reason,
       input_tokens,
       cached_input_tokens,
       output_tokens,
       reasoning_tokens,
       latency_ms,
       judge_call_requested_event_id,
       judge_call_requested_status,
       judge_call_requested_fail_count,
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
COUNT_PUBLISHED_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'judge.output.ready.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
  AND status = 'published'::outbox_status_enum
"""
COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
"""
SELECT_JUDGE_CALL_REQUESTED_OUTBOX_STATUS_BUCKETS_QUERY = """
SELECT status, COUNT(*) AS status_count
FROM event_outbox
WHERE event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
GROUP BY status
ORDER BY status
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
COUNT_PUBLISHED_POLICY_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.policy.apply.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
  AND status = 'published'::outbox_status_enum
"""
COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM notification_plans np
JOIN analyses a ON a.analysis_id = np.analysis_id
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_PUBLISHED_NOTIFICATION_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox eo
JOIN notification_plans np
  ON eo.aggregate_type = 'notification_plan'
 AND eo.aggregate_id = np.notification_plan_id
JOIN analyses a ON a.analysis_id = np.analysis_id
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
  AND eo.event_type = 'notification.plan.created.v1'
  AND eo.status = 'published'::outbox_status_enum
"""
SELECT_EVENT_SUMMARY_BY_ID_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, status
FROM event_outbox
WHERE event_id = CAST(:event_id AS uuid)
"""
REQUIRED_TABLES = (
    "judge_runs",
    "judge_outputs",
    "event_outbox",
    "candidate_evidence_bundles",
    "analyses",
    "notification_plans",
)


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


class RedisClientLike(Protocol):
    async def ping(self) -> Any: ...
    async def xrevrange(self, name: str, count: int | None = None) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayLiveSmokeCandidate:
    judge_run_id: UUID
    bundle_id: UUID
    status: str
    schema_retry_count: int
    refusal_detected: bool
    started_at: Any
    finished_at: Any
    finish_reason: str | None
    input_tokens: Any
    cached_input_tokens: Any
    output_tokens: Any
    reasoning_tokens: Any
    latency_ms: Any
    judge_call_requested_event_id: UUID
    judge_call_requested_status: str
    judge_call_requested_fail_count: int
    recency_at: Any
    judge_call_requested_created_at: Any


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
            "Read-only result audit for the replay live-smoke judge-openai candidate. "
            "The script inspects PostgreSQL and Redis only, emits sanitized JSON, "
            "and has no approval or write mode."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--scan-limit",
        type=_bounded_positive_int_named("scan-limit", upper_bound=MAX_SCAN_LIMIT),
        default=DEFAULT_SCAN_LIMIT,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _bounded_positive_int_named(field_name: str, *, upper_bound: int) -> Callable[[str], int]:
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a positive integer"
            ) from exc
        if value <= 0 or value > upper_bound:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be between 1 and {upper_bound}"
            )
        return value

    return parse


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_READY,
        "runtime_env_read": False,
        "database_configured": False,
        "redis_configured": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "replay_live_smoke_candidate_found_bucket": "zero",
        "replay_judge_run_status_bucket": "zero",
        "replay_judge_run_pending_bucket": "zero",
        "replay_judge_run_terminal_bucket": "zero",
        "replay_judge_run_retryable_bucket": "zero",
        "replay_judge_run_active_bucket": "zero",
        "finish_reason_bucket": "zero",
        "finish_reason_present_bucket": "zero",
        "started_at_present_bucket": "zero",
        "finished_at_present_bucket": "zero",
        "latency_ms_present_bucket": "zero",
        "input_tokens_present_bucket": "zero",
        "output_tokens_present_bucket": "zero",
        "reasoning_tokens_present_bucket": "zero",
        "cached_input_tokens_present_bucket": "zero",
        "schema_retry_count_bucket": "zero",
        "refusal_detected_bucket": "zero",
        "judge_outputs_for_run_bucket": "zero",
        "judge_output_ready_outbox_for_run_bucket": "zero",
        "judge_call_requested_outbox_for_run_bucket": "zero",
        "judge_call_requested_outbox_status_bucket": "zero",
        "q_analysis_judge_active_candidate_bucket": "zero",
        "q_analysis_judge_scanned_bucket": "zero",
        "analysis_rows_for_run_bucket": "zero",
        "policy_outbox_for_run_bucket": "zero",
        "notification_rows_for_run_bucket": "zero",
        "openai_call_attempted": False,
        "openai_key_file_read_bucket": "zero",
        "database_write_attempted": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "analysis_validator_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "q_analysis_validate_published": False,
        "q_analysis_policy_published": False,
        "q_notification_send_published": False,
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


def _bucket_bool(value: bool) -> str:
    return "one" if value else "zero"


def _bucket_present(value: Any) -> str:
    if value is None:
        return "zero"
    if isinstance(value, str) and not value.strip():
        return "zero"
    return "one"


def _finish_reason_bucket(finish_reason: Any) -> str:
    if finish_reason is None:
        return "zero"
    normalized = str(finish_reason).strip().lower()
    if not normalized:
        return "zero"
    if normalized in ALLOWED_FINISH_REASON_CODES:
        return normalized
    if any(keyword in normalized for keyword in SCHEMA_FAILURE_KEYWORDS):
        return "schema_failure"
    if any(keyword in normalized for keyword in PRECONDITION_FAILURE_KEYWORDS):
        return "precondition_failure"
    if any(keyword in normalized for keyword in API_TRANSPORT_FAILURE_KEYWORDS):
        return "api_or_transport_failure"
    if any(keyword in normalized for keyword in RESPONSE_MAPPING_FAILURE_KEYWORDS):
        return "response_mapping_failure"
    return "other_sanitized"


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _read_runtime_env(
    path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
) -> Mapping[str, str]:
    if runtime_env_reader is not None:
        return runtime_env_reader(path)
    return parse_runtime_env_file(path)


def _database_url_is_supported(database_url: str) -> bool:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not match:
        return False
    scheme = match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _redis_url_is_supported(redis_url: str) -> bool:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", redis_url)
    return bool(match and match.group(1).lower() in {"redis", "rediss", "unix"})


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str] | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    raw_values.update(_collect_raw_strings(database_url, redis_url))

    report["database_configured"] = bool(database_url and _database_url_is_supported(database_url))
    report["redis_configured"] = bool(redis_url and _redis_url_is_supported(redis_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_NOT_READY, "database.config")
        return None
    if not report["redis_configured"]:
        _set_status(report, STATUS_NOT_READY, "redis.config")
        return None
    return database_url, redis_url


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


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


async def _open_default_redis_client(redis_url: str) -> RedisClientLike:
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    return Redis.from_url(redis_url, decode_responses=True)


async def _open_redis_client(
    redis_url: str,
    redis_client_factory: RedisClientFactory | None,
) -> RedisClientLike:
    if redis_client_factory is not None:
        return await _maybe_await(redis_client_factory(redis_url))
    return await _open_default_redis_client(redis_url)


async def _close_database_session(session: AsyncSessionLike | None) -> None:
    if session is not None:
        await _maybe_await(session.close())


async def _close_redis_client(client: Any | None) -> None:
    if client is None:
        return
    close = getattr(client, "aclose", None) or getattr(client, "close", None)
    if close is not None:
        await _maybe_await(close())


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


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _uuid_or_none(value: Any) -> UUID | None:
    try:
        return _coerce_uuid(value)
    except (TypeError, ValueError, AttributeError):
        return None


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _redis_entry_fields(entry: Any) -> tuple[str | None, dict[str, str]]:
    if isinstance(entry, Mapping):
        return None, {_decode_text(k): _decode_text(v) for k, v in entry.items()}
    if isinstance(entry, (tuple, list)) and len(entry) >= 2 and isinstance(entry[1], Mapping):
        return _decode_text(entry[0]), {_decode_text(k): _decode_text(v) for k, v in entry[1].items()}
    return None, {}


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
        STATUS_PASSED,
        STATUS_NO_CANDIDATE,
        STATUS_AMBIGUOUS_CANDIDATE,
        STATUS_NOT_READY,
        STATUS_RAW_VALUE_EMISSION,
        "pending",
        "running",
        "succeeded",
        "failed_terminal",
        "failed_retryable",
        "published",
        "failed",
        "completed",
        "incomplete",
        "max_output_tokens",
        "output_truncated",
        "truncated",
        "prompt_missing",
        "bundle_missing",
        "bundle_invalid",
        "unsupported_judge_profile",
        "openai_transport_retryable",
        "openai_permanent_error",
        "schema_invalid_after_retry",
        "analysis_failed_truncation",
        "model_refusal",
        "validator_missing_skeptical_take",
        "validator_missing_reason_codes",
        "validator_schema_invalid",
        "validator_missing_github_comparables",
        "validator_inspect_now_evidence_too_low",
        "validator_inspect_now_confidence_too_low",
        "validator_inspect_now_hype_too_high",
        "validator_passed",
        "schema_failure",
        "precondition_failure",
        "api_or_transport_failure",
        "response_mapping_failure",
        "other_sanitized",
        EXPECTED_STAGE_NAME,
        EXPECTED_AGGREGATE_TYPE,
        EXPECTED_QUEUE_NAME,
        JUDGE_CALL_REQUESTED_EVENT_TYPE,
        JUDGE_OUTPUT_READY_EVENT_TYPE,
        POLICY_APPLY_EVENT_TYPE,
        NOTIFICATION_PLAN_EVENT_TYPE,
        "zero",
        "one",
        "multiple",
        "other",
    }
    return any(value not in public_literals and value in rendered for value in raw_values)


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


def _candidate_from_mapping(row: Mapping[str, Any]) -> ReplayLiveSmokeCandidate:
    return ReplayLiveSmokeCandidate(
        judge_run_id=_coerce_uuid(row["judge_run_id"]),
        bundle_id=_coerce_uuid(row["bundle_id"]),
        status=str(row["judge_run_status"]),
        schema_retry_count=int(row.get("schema_retry_count", 0) or 0),
        refusal_detected=bool(row.get("refusal_detected", False)),
        started_at=row.get("started_at"),
        finished_at=row.get("finished_at"),
        finish_reason=(
            str(row["finish_reason"]) if row.get("finish_reason") is not None else None
        ),
        input_tokens=row.get("input_tokens"),
        cached_input_tokens=row.get("cached_input_tokens"),
        output_tokens=row.get("output_tokens"),
        reasoning_tokens=row.get("reasoning_tokens"),
        latency_ms=row.get("latency_ms"),
        judge_call_requested_event_id=_coerce_uuid(row["judge_call_requested_event_id"]),
        judge_call_requested_status=str(row.get("judge_call_requested_status", "")),
        judge_call_requested_fail_count=int(
            row.get("judge_call_requested_fail_count", 0) or 0
        ),
        recency_at=row.get("recency_at"),
        judge_call_requested_created_at=row.get("judge_call_requested_created_at"),
    )


def _same_recency(first: ReplayLiveSmokeCandidate, second: ReplayLiveSmokeCandidate) -> bool:
    return (
        first.recency_at == second.recency_at
        and first.judge_call_requested_created_at == second.judge_call_requested_created_at
    )


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
            _collect_raw_strings(
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


def _judge_run_status_bucket(status: str) -> str:
    normalized = status.strip().lower()
    if normalized in {"pending", "running", "succeeded", "failed_terminal", "failed_retryable"}:
        return normalized
    return "other"


def _apply_judge_run_status(report: dict[str, Any], candidate: ReplayLiveSmokeCandidate) -> None:
    status = _judge_run_status_bucket(candidate.status)
    report["replay_judge_run_status_bucket"] = status
    report["replay_judge_run_pending_bucket"] = _bucket_bool(status == "pending")
    report["replay_judge_run_terminal_bucket"] = _bucket_bool(
        status in {"succeeded", "failed_terminal"}
    )
    report["replay_judge_run_retryable_bucket"] = _bucket_bool(
        status in {"failed_retryable", "running"}
    )
    report["replay_judge_run_active_bucket"] = _bucket_bool(status in {"pending", "running"})
    report["finish_reason_bucket"] = _finish_reason_bucket(candidate.finish_reason)
    report["finish_reason_present_bucket"] = _bucket_present(candidate.finish_reason)
    report["started_at_present_bucket"] = _bucket_present(candidate.started_at)
    report["finished_at_present_bucket"] = _bucket_present(candidate.finished_at)
    report["latency_ms_present_bucket"] = _bucket_present(candidate.latency_ms)
    report["input_tokens_present_bucket"] = _bucket_present(candidate.input_tokens)
    report["output_tokens_present_bucket"] = _bucket_present(candidate.output_tokens)
    report["reasoning_tokens_present_bucket"] = _bucket_present(candidate.reasoning_tokens)
    report["cached_input_tokens_present_bucket"] = _bucket_present(
        candidate.cached_input_tokens
    )
    report["schema_retry_count_bucket"] = _bucket_count(candidate.schema_retry_count)
    report["refusal_detected_bucket"] = _bucket_bool(candidate.refusal_detected)


async def _count_query(
    session: AsyncSessionLike,
    query: str,
    params: dict[str, Any],
) -> int:
    return _safe_count(_scalar(await _execute(session, query, params)))


async def _status_bucket_for_judge_call_requested(
    session: AsyncSessionLike,
    judge_run_id: UUID,
) -> str:
    result = await _execute(
        session,
        SELECT_JUDGE_CALL_REQUESTED_OUTBOX_STATUS_BUCKETS_QUERY,
        {"judge_run_id": str(judge_run_id)},
    )
    rows = _rows(result)
    total = sum(_safe_count(row.get("status_count") if isinstance(row, Mapping) else 0) for row in rows)
    if total <= 0:
        return "zero"
    if total > 1 or len(rows) != 1:
        return "multiple"
    row = rows[0]
    status = str(row.get("status", "")) if isinstance(row, Mapping) else ""
    normalized = status.strip().lower()
    if normalized in {"pending", "published", "failed"}:
        return normalized
    return "other"


async def _apply_db_counts(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    candidate: ReplayLiveSmokeCandidate,
) -> None:
    params = {"judge_run_id": str(candidate.judge_run_id)}
    output_count = await _count_query(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params)
    ready_count = await _count_query(
        session,
        COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY,
        params,
    )
    ready_published_count = await _count_query(
        session,
        COUNT_PUBLISHED_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY,
        params,
    )
    judge_call_count = await _count_query(
        session,
        COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY,
        params,
    )
    analysis_count = await _count_query(session, COUNT_ANALYSES_FOR_RUN_QUERY, params)
    policy_count = await _count_query(session, COUNT_POLICY_OUTBOX_FOR_RUN_QUERY, params)
    policy_published_count = await _count_query(
        session,
        COUNT_PUBLISHED_POLICY_OUTBOX_FOR_RUN_QUERY,
        params,
    )
    notification_count = await _count_query(
        session,
        COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY,
        params,
    )
    notification_published_count = await _count_query(
        session,
        COUNT_PUBLISHED_NOTIFICATION_OUTBOX_FOR_RUN_QUERY,
        params,
    )

    report["judge_outputs_for_run_bucket"] = _bucket_count(output_count)
    report["judge_output_ready_outbox_for_run_bucket"] = _bucket_count(ready_count)
    report["judge_call_requested_outbox_for_run_bucket"] = _bucket_count(judge_call_count)
    report["judge_call_requested_outbox_status_bucket"] = (
        await _status_bucket_for_judge_call_requested(session, candidate.judge_run_id)
    )
    report["analysis_rows_for_run_bucket"] = _bucket_count(analysis_count)
    report["policy_outbox_for_run_bucket"] = _bucket_count(policy_count)
    report["notification_rows_for_run_bucket"] = _bucket_count(notification_count)
    report["q_analysis_validate_published"] = ready_published_count > 0
    report["q_analysis_policy_published"] = policy_published_count > 0
    report["q_notification_send_published"] = notification_published_count > 0


async def _load_event_summary(
    session: AsyncSessionLike,
    event_id: UUID,
    raw_values: set[str],
) -> Mapping[str, Any] | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_EVENT_SUMMARY_BY_ID_QUERY,
            {"event_id": str(event_id)},
        )
    )
    if row is not None:
        raw_values.update(
            _collect_raw_strings(
                row.get("event_id"),
                row.get("aggregate_id"),
            )
        )
    return row


async def _scan_q_analysis_judge(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    candidate: ReplayLiveSmokeCandidate,
    raw_values: set[str],
    scan_limit: int,
) -> None:
    entries = await _maybe_await(
        redis_client.xrevrange(EXPECTED_QUEUE_NAME, count=scan_limit)
    )
    entries = entries or []
    report["q_analysis_judge_scanned_bucket"] = _bucket_count(len(entries))
    active_count = 0
    for entry in entries:
        entry_id, fields = _redis_entry_fields(entry)
        raw_values.update(_collect_raw_strings(entry_id, fields))
        if set(fields) != ALLOWED_REDIS_THIN_FIELDS:
            continue
        if fields.get("stage_name") != EXPECTED_STAGE_NAME:
            continue
        if fields.get("root_object_type") != EXPECTED_AGGREGATE_TYPE:
            continue
        root_object_id = _uuid_or_none(fields.get("root_object_id"))
        trigger_event_id = _uuid_or_none(fields.get("trigger_event_id"))
        if root_object_id != candidate.judge_run_id or trigger_event_id is None:
            continue
        event = await _load_event_summary(session, trigger_event_id, raw_values)
        if event is None:
            continue
        if str(event.get("event_type")) != JUDGE_CALL_REQUESTED_EVENT_TYPE:
            continue
        if str(event.get("aggregate_type")) != EXPECTED_AGGREGATE_TYPE:
            continue
        if _uuid_or_none(event.get("aggregate_id")) != candidate.judge_run_id:
            continue
        if str(event.get("status")) != "pending":
            continue
        if _judge_run_status_bucket(candidate.status) != "pending":
            continue
        if report["judge_outputs_for_run_bucket"] != "zero":
            continue
        if report["judge_output_ready_outbox_for_run_bucket"] != "zero":
            continue
        active_count += 1
    report["q_analysis_judge_active_candidate_bucket"] = _bucket_count(active_count)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    if scan_limit <= 0 or scan_limit > MAX_SCAN_LIMIT:
        _set_status(report, STATUS_NOT_READY, "scan_limit.out_of_bounds")
        return ScriptResult(exit_code=1, report=report)

    session: AsyncSessionLike | None = None
    redis_client: RedisClientLike | None = None
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}
    raw_values.add(str(runtime_env_path))

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "runtime_env.read")
            return _finalize(report, raw_values, exit_code=1)

        runtime_config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return _finalize(report, raw_values, exit_code=1)
        database_url, redis_url = runtime_config

        try:
            session = await _open_database_session(database_url, database_session_factory)
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only = _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
            if not _transaction_read_only_enabled(read_only):
                _set_status(report, STATUS_NOT_READY, "database.read_only")
                return _finalize(report, raw_values, exit_code=1)
            report["read_only_transaction"] = True
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(report, STATUS_NOT_READY, "database.required_tables")
                return _finalize(report, raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_NOT_READY, "database.connection_or_schema")
            return _finalize(report, raw_values, exit_code=1)

        try:
            redis_client = await _open_redis_client(redis_url, redis_client_factory)
            await _maybe_await(redis_client.ping())
            report["redis_connected"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "redis.connection")
            return _finalize(report, raw_values, exit_code=1)

        candidate, ambiguous = await _select_target_candidate(
            session=session,
            raw_values=raw_values,
        )
        if ambiguous:
            report["replay_live_smoke_candidate_found_bucket"] = "multiple"
            _set_status(report, STATUS_AMBIGUOUS_CANDIDATE, "candidate.ambiguous_recency")
            return _finalize(report, raw_values, exit_code=1)
        if candidate is None:
            _set_status(report, STATUS_NO_CANDIDATE, "candidate.none")
            return _finalize(report, raw_values, exit_code=1)

        report["replay_live_smoke_candidate_found_bucket"] = "one"
        raw_values.update(
            _collect_raw_strings(
                candidate.judge_run_id,
                candidate.bundle_id,
                candidate.judge_call_requested_event_id,
                candidate.finish_reason,
            )
        )
        _apply_judge_run_status(report, candidate)
        await _apply_db_counts(report=report, session=session, candidate=candidate)
        await _scan_q_analysis_judge(
            report=report,
            session=session,
            redis_client=redis_client,
            candidate=candidate,
            raw_values=raw_values,
            scan_limit=scan_limit,
        )

        _set_status(report, STATUS_PASSED)
        return _finalize(report, raw_values, exit_code=0)
    except Exception:
        _set_status(report, STATUS_NOT_READY, "unexpected")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        if session is not None:
            await _maybe_await(session.rollback())
            await _close_database_session(session)
        await _close_redis_client(redis_client)


def _finalize(report: dict[str, Any], raw_values: set[str], *, exit_code: int) -> ScriptResult:
    if _report_contains_raw_values(report, {value for value in raw_values if len(value) >= 6}):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            scan_limit=scan_limit,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        scan_limit=args.scan_limit,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
