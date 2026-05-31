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


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_call_requested_route_publish_post_publish_read_only_audit"
REPORT_TYPE = "judge_call_requested_route_publish_post_publish_read_only_audit_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_PUBLISHED_SCAN_LIMIT = 2
MAX_PUBLISHED_SCAN_LIMIT = 20
EXPECTED_EVENT_TYPE = "judge.call.requested.v1"
EXPECTED_AGGREGATE_TYPE = "judge_run"
EXPECTED_QUEUE_NAME = "q.analysis.judge"
EXPECTED_STAGE_NAME = "judge"
REQUIRED_PAYLOAD_FIELDS = {
    "judge_run_id",
    "bundle_id",
    "model",
    "reasoning_effort",
    "prompt_version",
    "prompt_cache_key",
}
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
FORBIDDEN_REDIS_FIELD_TOKENS = (
    "payload",
    "payload_json",
    "source",
    "raw",
    "text",
    "caption",
    "url",
    "message",
    "database_url",
    "redis_url",
    "secret",
    "token",
    "password",
    "api_key",
    "apikey",
    "body",
    "stderr",
)
REQUIRED_TABLES = (
    "event_outbox",
    "job_attempts",
    "judge_runs",
    "candidate_evidence_bundles",
    "judge_outputs",
    "analyses",
    "notification_plans",
)
SIDE_EFFECT_REPORT_FIELDS = (
    "recent_judge_output_written_bucket",
    "recent_judge_output_ready_outbox_written_bucket",
    "recent_analysis_written_bucket",
    "recent_policy_side_effect_bucket",
    "recent_notification_plan_written_bucket",
    "judge_openai_started",
    "analysis_validator_started",
    "policy_engine_started",
    "notifier_started",
    "telegram_send_attempted",
    "source_tables_mutation_performed",
    "telegram_raw_updates_mutation_performed",
    "candidate_mutation_performed",
    "artifact_registry_mutation_performed",
    "artifact_snapshot_mutation_performed",
    "evidence_bundle_mutation_performed",
    "docker_or_systemd_changed",
    "alembic_run",
    "external_network_attempted",
    "raw_values_emitted",
)

STATUS_PASSED = "judge_call_requested_route_publish_post_publish_read_only_audit_passed"
STATUS_NOT_READY = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_not_ready"
)
STATUS_MISSING_PUBLISHED = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_missing_published_event"
)
STATUS_AMBIGUOUS_PUBLISHED = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_ambiguous_published_event"
)
STATUS_INVALID_PUBLISHED = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_invalid_published_event"
)
STATUS_PENDING_REMAINING = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_pending_event_remaining"
)
STATUS_MISSING_JOB_ATTEMPT = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_missing_job_attempt"
)
STATUS_INVALID_REDIS_STREAM = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_invalid_redis_stream"
)
STATUS_INVALID_JUDGE_RUN = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_invalid_judge_run"
)
STATUS_INVALID_BUNDLE = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_invalid_bundle"
)
STATUS_DOWNSTREAM_SIDE_EFFECT = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_downstream_side_effect"
)
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"
STATUS_RAW_VALUE_EMISSION = (
    "blocked_judge_call_requested_route_publish_post_publish_read_only_audit_raw_value_emission"
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_RECENT_PUBLISHED_JUDGE_CALL_REQUESTED_QUERY = """
SELECT
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    published_at,
    created_at
FROM event_outbox
WHERE status = 'published'::outbox_status_enum
  AND event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
ORDER BY published_at DESC NULLS LAST, created_at DESC, event_id DESC
LIMIT :limit
"""
COUNT_PENDING_JUDGE_CALL_REQUESTED_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE status = 'pending'::outbox_status_enum
  AND event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
"""
COUNT_SUCCEEDED_JUDGE_ROUTE_JOB_ATTEMPT_QUERY = """
SELECT COUNT(*)
FROM job_attempts
WHERE queue_name = 'q.analysis.judge'
  AND stage_name = 'judge'
  AND root_object_type = 'judge_run'
  AND root_object_id = CAST(:judge_run_id AS uuid)
  AND attempt_status = 'succeeded'::job_attempt_status_enum
"""
SELECT_JUDGE_RUN_STATE_QUERY = """
SELECT
    judge_run_id,
    bundle_id,
    model,
    reasoning_effort,
    prompt_version,
    prompt_cache_key
FROM judge_runs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""
SELECT_BUNDLE_READY_QUERY = """
SELECT bundle_id, ready_for_analysis
FROM candidate_evidence_bundles
WHERE bundle_id = CAST(:bundle_id AS uuid)
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
COUNT_ANALYSES_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM analyses a
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_POLICY_SIDE_EFFECTS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.policy.apply.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
"""
COUNT_NOTIFICATION_PLANS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM notification_plans np
JOIN analyses a ON a.analysis_id = np.analysis_id
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""


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
    async def exists(self, name: str) -> Any: ...
    async def xlen(self, name: str) -> Any: ...
    async def xrevrange(self, name: str, count: int | None = None) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PublishedEventRow:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str
    published_at: Any
    created_at: Any


@dataclass(frozen=True, slots=True)
class JudgeRunState:
    judge_run_id: UUID
    bundle_id: UUID
    model: str
    reasoning_effort: str
    prompt_version: str
    prompt_cache_key: str | None


@dataclass(frozen=True, slots=True)
class BundleState:
    bundle_id: UUID
    ready_for_analysis: bool


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.outbox_relay.repositories import _sql  # noqa: E402


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only post-publish audit for the judge.call.requested.v1 route "
            "publish handoff to q.analysis.judge. The script inspects only DB and "
            "Redis state, emits sanitized JSON, and performs no writes."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--published-scan-limit",
        type=_bounded_positive_int_named(
            "published-scan-limit",
            upper_bound=MAX_PUBLISHED_SCAN_LIMIT,
        ),
        default=DEFAULT_PUBLISHED_SCAN_LIMIT,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_READY,
        "runtime_env_read": False,
        "database_connected": False,
        "read_only_transaction": False,
        "redis_connected": False,
        "recent_judge_call_requested_published_bucket": "zero",
        "recent_judge_route_job_attempt_succeeded_bucket": "zero",
        "pending_judge_call_requested_bucket": "zero",
        "q_analysis_judge_stream_exists": False,
        "q_analysis_judge_length_bucket": "zero",
        "latest_redis_entry_shape_valid_bucket": "zero",
        "latest_redis_entry_stage_judge_bucket": "zero",
        "latest_redis_entry_root_judge_run_bucket": "zero",
        "judge_run_linked_bucket": "zero",
        "bundle_ready_for_analysis_bucket": "zero",
        "recent_judge_output_written_bucket": "zero",
        "recent_judge_output_ready_outbox_written_bucket": "zero",
        "recent_analysis_written_bucket": "zero",
        "recent_policy_side_effect_bucket": "zero",
        "recent_notification_plan_written_bucket": "zero",
        "judge_openai_started": False,
        "analysis_validator_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "candidate_mutation_performed": False,
        "artifact_registry_mutation_performed": False,
        "artifact_snapshot_mutation_performed": False,
        "evidence_bundle_mutation_performed": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "external_network_attempted": False,
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


def _bucket_one_zero(value: bool) -> str:
    return "one" if value else "zero"


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
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
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not scheme_match:
        return False
    scheme = scheme_match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _redis_url_is_supported(redis_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", redis_url)
    if not scheme_match:
        return False
    return scheme_match.group(1).lower() in {"redis", "rediss", "unix"}


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
    return await session.execute(_sql(statement), params or {})


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
    if isinstance(first, Mapping):
        return first
    return None


def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
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


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return _coerce_uuid(value)
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _payload_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _payload_uuid(payload: Mapping[str, Any], key: str) -> UUID | None:
    value = _payload_str(payload, key)
    if value is None:
        return None
    return _uuid_or_none(value)


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_stream_fields(fields: Mapping[Any, Any]) -> dict[str, str]:
    return {_decode_text(key): _decode_text(value) for key, value in fields.items()}


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str] | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    if database_url:
        raw_values.add(database_url)
    if redis_url:
        raw_values.add(redis_url)
    if not database_url:
        _set_status(report, STATUS_NOT_READY, "database.url_missing")
        return None
    if not _database_url_is_supported(database_url):
        _set_status(report, STATUS_NOT_READY, "database.url_unsupported")
        return None
    if not redis_url:
        _set_status(report, STATUS_NOT_READY, "redis.url_missing")
        return None
    if not _redis_url_is_supported(redis_url):
        _set_status(report, STATUS_NOT_READY, "redis.url_unsupported")
        return None
    return database_url, redis_url


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    if not side_effect_flags:
        return
    for field in SIDE_EFFECT_REPORT_FIELDS:
        if bool(side_effect_flags.get(field, False)):
            if isinstance(report[field], str):
                report[field] = "one"
            else:
                report[field] = True


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    for field in SIDE_EFFECT_REPORT_FIELDS:
        value = report[field]
        if isinstance(value, str):
            if value != "zero":
                return True
            continue
        if bool(value):
            return True
    return False


def _raw_values_from_event_rows(rows: Sequence[PublishedEventRow]) -> set[str]:
    values: set[str] = set()
    for row in rows:
        values.update({str(row.event_id), str(row.aggregate_id), row.dedupe_key})
        values.add(json.dumps(row.payload_json, sort_keys=True, default=str))
        for _key, value in row.payload_json.items():
            if isinstance(value, str) and value:
                values.add(value)
    return {value for value in values if len(value) >= 6}


def _raw_values_from_redis_entry(entry_id: Any, fields: Mapping[str, str]) -> set[str]:
    public_contract_values = {
        EXPECTED_STAGE_NAME,
        EXPECTED_AGGREGATE_TYPE,
        EXPECTED_QUEUE_NAME,
    }
    values = {_decode_text(entry_id)}
    values.update(
        str(value)
        for value in fields.values()
        if str(value) and str(value) not in public_contract_values
    )
    return {value for value in values if len(value) >= 6}


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values)


def _published_event_from_mapping(row: Mapping[str, Any]) -> PublishedEventRow:
    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return PublishedEventRow(
        event_id=_coerce_uuid(row["event_id"]),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=_coerce_uuid(row["aggregate_id"]),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        published_at=row.get("published_at"),
        created_at=row.get("created_at"),
    )


async def _fetch_recent_published_events(
    session: AsyncSessionLike,
    *,
    limit: int,
) -> list[PublishedEventRow]:
    result = await _execute(
        session,
        SELECT_RECENT_PUBLISHED_JUDGE_CALL_REQUESTED_QUERY,
        {"limit": limit},
    )
    return [_published_event_from_mapping(row) for row in _rows(result)]


async def _load_judge_run(
    session: AsyncSessionLike,
    judge_run_id: UUID,
) -> JudgeRunState | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_JUDGE_RUN_STATE_QUERY,
            {"judge_run_id": str(judge_run_id)},
        )
    )
    if row is None:
        return None
    return JudgeRunState(
        judge_run_id=_coerce_uuid(row["judge_run_id"]),
        bundle_id=_coerce_uuid(row["bundle_id"]),
        model=str(row["model"]),
        reasoning_effort=str(row["reasoning_effort"]),
        prompt_version=str(row["prompt_version"]),
        prompt_cache_key=(
            str(row["prompt_cache_key"]) if row["prompt_cache_key"] is not None else None
        ),
    )


async def _load_bundle(
    session: AsyncSessionLike,
    bundle_id: UUID,
) -> BundleState | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_BUNDLE_READY_QUERY,
            {"bundle_id": str(bundle_id)},
        )
    )
    if row is None:
        return None
    return BundleState(
        bundle_id=_coerce_uuid(row["bundle_id"]),
        ready_for_analysis=bool(row["ready_for_analysis"]),
    )


async def _count_query(
    session: AsyncSessionLike,
    query: str,
    params: dict[str, Any] | None = None,
) -> int:
    return _safe_count(_scalar(await _execute(session, query, params or {})))


def validate_redis_thin_payload_shape(fields: Mapping[str, Any]) -> bool:
    keys = set(fields)
    if keys != ALLOWED_REDIS_THIN_FIELDS:
        return False
    return not any(_is_forbidden_redis_field(key) for key in keys)


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_REDIS_FIELD_TOKENS)


async def _validate_published_event(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    row: PublishedEventRow,
) -> tuple[bool, str, UUID | None, UUID | None]:
    payload = row.payload_json
    judge_run_id = _payload_uuid(payload, "judge_run_id")
    bundle_id = _payload_uuid(payload, "bundle_id")
    model = _payload_str(payload, "model")
    reasoning_effort = _payload_str(payload, "reasoning_effort")
    prompt_version = _payload_str(payload, "prompt_version")
    prompt_cache_key = _payload_str(payload, "prompt_cache_key")

    if row.event_type != EXPECTED_EVENT_TYPE:
        return False, "event_outbox.event_type", judge_run_id, bundle_id
    if row.aggregate_type != EXPECTED_AGGREGATE_TYPE:
        return False, "event_outbox.aggregate_type", judge_run_id, bundle_id
    if row.status != "published":
        return False, "event_outbox.status", judge_run_id, bundle_id
    missing_payload_fields = [
        field
        for field in sorted(REQUIRED_PAYLOAD_FIELDS)
        if _payload_str(payload, field) is None
    ]
    if missing_payload_fields:
        return False, f"payload.{missing_payload_fields[0]}", judge_run_id, bundle_id
    if judge_run_id is None:
        return False, "payload.judge_run_id", judge_run_id, bundle_id
    if bundle_id is None:
        return False, "payload.bundle_id", judge_run_id, bundle_id
    if row.aggregate_id != judge_run_id:
        return False, "aggregate.judge_run_id", judge_run_id, bundle_id

    judge_run = await _load_judge_run(session, judge_run_id)
    if judge_run is None:
        return False, "judge_run.exists", judge_run_id, bundle_id
    if judge_run.judge_run_id != judge_run_id:
        return False, "judge_run.judge_run_id", judge_run_id, bundle_id
    if judge_run.bundle_id != bundle_id:
        return False, "judge_run.bundle_id", judge_run_id, bundle_id
    if judge_run.model != model:
        return False, "judge_run.model", judge_run_id, bundle_id
    if judge_run.reasoning_effort != reasoning_effort:
        return False, "judge_run.reasoning_effort", judge_run_id, bundle_id
    if judge_run.prompt_version != prompt_version:
        return False, "judge_run.prompt_version", judge_run_id, bundle_id
    if judge_run.prompt_cache_key != prompt_cache_key:
        return False, "judge_run.prompt_cache_key", judge_run_id, bundle_id
    report["judge_run_linked_bucket"] = "one"

    bundle = await _load_bundle(session, bundle_id)
    if bundle is None:
        return False, "bundle.exists", judge_run_id, bundle_id
    if bundle.bundle_id != bundle_id:
        return False, "bundle.bundle_id", judge_run_id, bundle_id
    if not bundle.ready_for_analysis:
        return False, "bundle.ready_for_analysis", judge_run_id, bundle_id
    report["bundle_ready_for_analysis_bucket"] = "one"
    return True, "", judge_run_id, bundle_id


def _status_for_published_validation_check(check: str) -> str:
    if check.startswith("judge_run."):
        return STATUS_INVALID_JUDGE_RUN
    if check.startswith("bundle."):
        return STATUS_INVALID_BUNDLE
    return STATUS_INVALID_PUBLISHED


async def _inspect_redis_stream(
    *,
    report: dict[str, Any],
    redis_client: RedisClientLike,
    row: PublishedEventRow,
    judge_run_id: UUID,
    raw_values: set[str],
) -> tuple[bool, str]:
    await _maybe_await(redis_client.ping())
    exists_count = _safe_count(await _maybe_await(redis_client.exists(EXPECTED_QUEUE_NAME)))
    report["q_analysis_judge_stream_exists"] = exists_count > 0
    if exists_count <= 0:
        return False, "redis.stream_missing"

    stream_length = _safe_count(await _maybe_await(redis_client.xlen(EXPECTED_QUEUE_NAME)))
    report["q_analysis_judge_length_bucket"] = _bucket_count(stream_length)
    if stream_length != 1:
        return False, "redis.stream_length_not_one"

    entries = await _maybe_await(
        redis_client.xrevrange(EXPECTED_QUEUE_NAME, count=1)
    )
    if not entries:
        return False, "redis.stream_entry_missing"
    entry_id, raw_fields = entries[0]
    fields = _decode_stream_fields(raw_fields)
    raw_values.update(_raw_values_from_redis_entry(entry_id, fields))

    shape_valid = validate_redis_thin_payload_shape(fields)
    report["latest_redis_entry_shape_valid_bucket"] = _bucket_one_zero(shape_valid)
    if not shape_valid:
        return False, "redis.entry_shape"

    stage_valid = fields.get("stage_name") == EXPECTED_STAGE_NAME
    report["latest_redis_entry_stage_judge_bucket"] = _bucket_one_zero(stage_valid)
    if not stage_valid:
        return False, "redis.entry_stage"

    root_valid = fields.get("root_object_type") == EXPECTED_AGGREGATE_TYPE
    report["latest_redis_entry_root_judge_run_bucket"] = _bucket_one_zero(root_valid)
    if not root_valid:
        return False, "redis.entry_root_type"

    if fields.get("root_object_id") != str(judge_run_id):
        return False, "redis.entry_root_object_id"
    if fields.get("trigger_event_id") != str(row.event_id):
        return False, "redis.entry_trigger_event_id"
    return True, ""


async def _validate_downstream_absence(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    judge_run_id: UUID,
) -> tuple[bool, str]:
    params = {"judge_run_id": str(judge_run_id)}

    judge_output_count = await _count_query(
        session,
        COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY,
        params,
    )
    report["recent_judge_output_written_bucket"] = _bucket_count(judge_output_count)
    if judge_output_count:
        return False, "downstream.judge_outputs"

    judge_output_ready_count = await _count_query(
        session,
        COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY,
        params,
    )
    report["recent_judge_output_ready_outbox_written_bucket"] = _bucket_count(
        judge_output_ready_count
    )
    if judge_output_ready_count:
        return False, "downstream.judge_output_ready_outbox"

    analysis_count = await _count_query(
        session,
        COUNT_ANALYSES_FOR_RUN_QUERY,
        params,
    )
    report["recent_analysis_written_bucket"] = _bucket_count(analysis_count)
    if analysis_count:
        return False, "downstream.analysis"

    policy_count = await _count_query(
        session,
        COUNT_POLICY_SIDE_EFFECTS_FOR_RUN_QUERY,
        params,
    )
    report["recent_policy_side_effect_bucket"] = _bucket_count(policy_count)
    if policy_count:
        return False, "downstream.policy"

    notification_count = await _count_query(
        session,
        COUNT_NOTIFICATION_PLANS_FOR_RUN_QUERY,
        params,
    )
    report["recent_notification_plan_written_bucket"] = _bucket_count(notification_count)
    if notification_count:
        return False, "downstream.notification_plan"

    return True, ""


async def _run_audit_checks(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    published_scan_limit: int,
    raw_values: set[str],
) -> ScriptResult | None:
    rows = await _fetch_recent_published_events(session, limit=published_scan_limit)
    raw_values.update(_raw_values_from_event_rows(rows))
    report["recent_judge_call_requested_published_bucket"] = _bucket_count(len(rows))
    if not rows:
        _set_status(report, STATUS_MISSING_PUBLISHED, "event_outbox.no_published_judge_call_request")
        return ScriptResult(exit_code=1, report=report)
    if len(rows) != 1:
        _set_status(report, STATUS_AMBIGUOUS_PUBLISHED, "event_outbox.published_count_not_one")
        return ScriptResult(exit_code=1, report=report)

    row = rows[0]
    valid, check, judge_run_id, _bundle_id = await _validate_published_event(
        report=report,
        session=session,
        row=row,
    )
    if not valid or judge_run_id is None:
        _set_status(report, _status_for_published_validation_check(check), check)
        return ScriptResult(exit_code=1, report=report)

    pending_count = await _count_query(session, COUNT_PENDING_JUDGE_CALL_REQUESTED_QUERY)
    report["pending_judge_call_requested_bucket"] = _bucket_count(pending_count)
    if pending_count:
        _set_status(report, STATUS_PENDING_REMAINING, "event_outbox.pending_judge_call_request")
        return ScriptResult(exit_code=1, report=report)

    job_attempt_count = await _count_query(
        session,
        COUNT_SUCCEEDED_JUDGE_ROUTE_JOB_ATTEMPT_QUERY,
        {"judge_run_id": str(judge_run_id)},
    )
    report["recent_judge_route_job_attempt_succeeded_bucket"] = _bucket_count(
        job_attempt_count
    )
    if job_attempt_count != 1:
        _set_status(report, STATUS_MISSING_JOB_ATTEMPT, "job_attempts.succeeded_count_not_one")
        return ScriptResult(exit_code=1, report=report)

    redis_ok, redis_check = await _inspect_redis_stream(
        report=report,
        redis_client=redis_client,
        row=row,
        judge_run_id=judge_run_id,
        raw_values=raw_values,
    )
    if not redis_ok:
        _set_status(report, STATUS_INVALID_REDIS_STREAM, redis_check)
        return ScriptResult(exit_code=1, report=report)

    downstream_ok, downstream_check = await _validate_downstream_absence(
        report=report,
        session=session,
        judge_run_id=judge_run_id,
    )
    if not downstream_ok:
        _set_status(report, STATUS_DOWNSTREAM_SIDE_EFFECT, downstream_check)
        return ScriptResult(exit_code=1, report=report)

    return None


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    published_scan_limit: int = DEFAULT_PUBLISHED_SCAN_LIMIT,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
        return ScriptResult(exit_code=1, report=report)

    if published_scan_limit <= 0 or published_scan_limit > MAX_PUBLISHED_SCAN_LIMIT:
        _set_status(report, STATUS_NOT_READY, "published_scan_limit.out_of_bounds")
        return ScriptResult(exit_code=1, report=report)

    session: AsyncSessionLike | None = None
    redis_client: RedisClientLike | None = None
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
            raw_values.add(str(runtime_env_path))
        except Exception:
            _set_status(report, STATUS_NOT_READY, "runtime_env.read")
            return ScriptResult(exit_code=1, report=report)

        runtime_config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return ScriptResult(exit_code=1, report=report)
        database_url, redis_url = runtime_config

        try:
            session = await _open_database_session(database_url, database_session_factory)
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only_value = _scalar(
                await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY)
            )
            report["read_only_transaction"] = _transaction_read_only_enabled(
                read_only_value
            )
            if not report["read_only_transaction"]:
                _set_status(report, STATUS_NOT_READY, "database.read_only_transaction")
                return ScriptResult(exit_code=1, report=report)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(report, STATUS_NOT_READY, "database.required_tables")
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(report, STATUS_NOT_READY, "database.connection_or_schema")
            return ScriptResult(exit_code=1, report=report)

        try:
            redis_client = await _open_redis_client(redis_url, redis_client_factory)
            await _maybe_await(redis_client.ping())
            report["redis_connected"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "redis.connection")
            return ScriptResult(exit_code=1, report=report)

        audit_result = await _run_audit_checks(
            report=report,
            session=session,
            redis_client=redis_client,
            published_scan_limit=published_scan_limit,
            raw_values=raw_values,
        )
        if audit_result is not None:
            if _report_contains_raw_values(report, raw_values):
                report["raw_values_emitted"] = True
                _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
                return ScriptResult(exit_code=1, report=report)
            return audit_result

        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
            return ScriptResult(exit_code=1, report=report)

        _set_status(report, STATUS_PASSED)
        return ScriptResult(exit_code=0, report=report)
    except Exception:
        _set_status(report, STATUS_NOT_READY, "unexpected")
        return ScriptResult(exit_code=1, report=report)
    finally:
        if session is not None:
            await _maybe_await(session.rollback())
        await _close_redis_client(redis_client)
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    published_scan_limit: int = DEFAULT_PUBLISHED_SCAN_LIMIT,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            published_scan_limit=published_scan_limit,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            side_effect_flags=side_effect_flags,
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
        published_scan_limit=args.published_scan_limit,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
