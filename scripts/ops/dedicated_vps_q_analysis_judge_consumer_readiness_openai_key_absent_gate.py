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
SCRIPT_NAME = "dedicated_vps_q_analysis_judge_consumer_readiness_openai_key_absent_gate"
REPORT_TYPE = "q_analysis_judge_consumer_readiness_openai_key_absent_gate_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
EXPECTED_EVENT_TYPE = "judge.call.requested.v1"
EXPECTED_AGGREGATE_TYPE = "judge_run"
EXPECTED_QUEUE_NAME = "q.analysis.judge"
EXPECTED_STAGE_NAME = "judge"
EXPECTED_JUDGE_RUN_STATUS = "pending"
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
    "judge_outputs_written_bucket",
    "judge_output_ready_outbox_written_bucket",
    "analyses_written_bucket",
    "analysis_policy_apply_outbox_written_bucket",
    "notification_plans_written_bucket",
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

STATUS_PASSED = "q_analysis_judge_consumer_readiness_passed_openai_key_absent"
STATUS_NOT_READY = "blocked_q_analysis_judge_consumer_readiness_not_ready"
STATUS_OPENAI_KEY_CONFIGURED = (
    "blocked_q_analysis_judge_consumer_readiness_openai_key_configured"
)
STATUS_INVALID_REDIS_STREAM = (
    "blocked_q_analysis_judge_consumer_readiness_invalid_redis_stream"
)
STATUS_MISSING_EVENT = "blocked_q_analysis_judge_consumer_readiness_missing_event_outbox"
STATUS_INVALID_EVENT = "blocked_q_analysis_judge_consumer_readiness_invalid_event_outbox"
STATUS_INVALID_JUDGE_RUN = "blocked_q_analysis_judge_consumer_readiness_invalid_judge_run"
STATUS_INVALID_BUNDLE = "blocked_q_analysis_judge_consumer_readiness_invalid_bundle"
STATUS_MISSING_JOB_ATTEMPT = (
    "blocked_q_analysis_judge_consumer_readiness_missing_route_publish_job_attempt"
)
STATUS_DOWNSTREAM_SIDE_EFFECT = (
    "blocked_q_analysis_judge_consumer_readiness_downstream_side_effect"
)
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"
STATUS_RAW_VALUE_EMISSION = "blocked_q_analysis_judge_consumer_readiness_raw_value_emission"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_EVENT_OUTBOX_BY_ID_QUERY = """
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
WHERE event_id = CAST(:event_id AS uuid)
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
    prompt_cache_key,
    status
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
class EventOutboxRow:
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
    status: str


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only readiness gate for the q.analysis.judge consumer handoff "
            "when the OpenAI API key is intentionally absent. The script inspects "
            "only DB and Redis state, emits sanitized JSON, and performs no writes."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_READY,
        "expected_success_meaning": (
            "q.analysis.judge consumer readiness passed, and execution is "
            "intentionally blocked because OpenAI API key is absent."
        ),
        "runtime_env_read": False,
        "database_connected": False,
        "read_only_transaction": False,
        "redis_connected": False,
        "q_analysis_judge_stream_exists": False,
        "q_analysis_judge_length_bucket": "zero",
        "redis_entry_shape_valid_bucket": "zero",
        "redis_entry_stage_judge_bucket": "zero",
        "redis_entry_root_judge_run_bucket": "zero",
        "redis_entry_trigger_event_id_valid_bucket": "zero",
        "event_outbox_rehydrated_bucket": "zero",
        "judge_call_requested_published_bucket": "zero",
        "judge_run_linked_bucket": "zero",
        "judge_run_pending_bucket": "zero",
        "bundle_ready_for_analysis_bucket": "zero",
        "route_publish_job_attempt_succeeded_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "judge_output_ready_outbox_written_bucket": "zero",
        "analyses_written_bucket": "zero",
        "analysis_policy_apply_outbox_written_bucket": "zero",
        "notification_plans_written_bucket": "zero",
        "openai_api_key_configured": False,
        "openai_api_key_file_configured": False,
        "openai_execution_blocked_by_missing_key": False,
        "openai_call_attempted": False,
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
    openai_api_key = str(values.get("OPENAI_API_KEY", "")).strip()
    openai_api_key_file = str(values.get("OPENAI_API_KEY_FILE", "")).strip()
    if database_url:
        raw_values.add(database_url)
    if redis_url:
        raw_values.add(redis_url)
    if openai_api_key:
        raw_values.add(openai_api_key)
    if openai_api_key_file:
        raw_values.add(openai_api_key_file)
    report["openai_api_key_configured"] = bool(openai_api_key)
    report["openai_api_key_file_configured"] = bool(openai_api_key_file)
    report["openai_execution_blocked_by_missing_key"] = not (
        openai_api_key or openai_api_key_file
    )
    if openai_api_key or openai_api_key_file:
        _set_status(report, STATUS_OPENAI_KEY_CONFIGURED, "openai.key_configured")
        return None
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


def _raw_values_from_event_row(row: EventOutboxRow) -> set[str]:
    values = {
        str(row.event_id),
        str(row.aggregate_id),
        row.dedupe_key,
        json.dumps(row.payload_json, sort_keys=True, default=str),
    }
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


def _event_outbox_row_from_mapping(row: Mapping[str, Any]) -> EventOutboxRow:
    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return EventOutboxRow(
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


async def _load_event_outbox_row(
    session: AsyncSessionLike,
    event_id: UUID,
) -> EventOutboxRow | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_EVENT_OUTBOX_BY_ID_QUERY,
            {"event_id": str(event_id)},
        )
    )
    if row is None:
        return None
    return _event_outbox_row_from_mapping(row)


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
        status=str(row["status"]),
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


async def _inspect_redis_work_item(
    *,
    report: dict[str, Any],
    redis_client: RedisClientLike,
    raw_values: set[str],
) -> tuple[bool, str, dict[str, str] | None]:
    await _maybe_await(redis_client.ping())
    exists_count = _safe_count(await _maybe_await(redis_client.exists(EXPECTED_QUEUE_NAME)))
    report["q_analysis_judge_stream_exists"] = exists_count > 0
    if exists_count <= 0:
        return False, "redis.stream_missing", None

    stream_length = _safe_count(await _maybe_await(redis_client.xlen(EXPECTED_QUEUE_NAME)))
    report["q_analysis_judge_length_bucket"] = _bucket_count(stream_length)
    if stream_length != 1:
        return False, "redis.stream_length_not_one", None

    entries = await _maybe_await(redis_client.xrevrange(EXPECTED_QUEUE_NAME, count=1))
    if len(entries) != 1:
        return False, "redis.stream_entry_count_not_one", None
    entry_id, raw_fields = entries[0]
    fields = _decode_stream_fields(raw_fields)
    raw_values.update(_raw_values_from_redis_entry(entry_id, fields))

    shape_valid = validate_redis_thin_payload_shape(fields)
    report["redis_entry_shape_valid_bucket"] = _bucket_one_zero(shape_valid)
    if not shape_valid:
        return False, "redis.entry_shape", None

    stage_valid = fields.get("stage_name") == EXPECTED_STAGE_NAME
    report["redis_entry_stage_judge_bucket"] = _bucket_one_zero(stage_valid)
    if not stage_valid:
        return False, "redis.entry_stage", None

    root_valid = fields.get("root_object_type") == EXPECTED_AGGREGATE_TYPE
    report["redis_entry_root_judge_run_bucket"] = _bucket_one_zero(root_valid)
    if not root_valid:
        return False, "redis.entry_root_type", None

    trigger_event_id = _uuid_or_none(fields.get("trigger_event_id"))
    root_object_id = _uuid_or_none(fields.get("root_object_id"))
    job_id = _uuid_or_none(fields.get("job_id"))
    report["redis_entry_trigger_event_id_valid_bucket"] = _bucket_one_zero(
        trigger_event_id is not None
    )
    if trigger_event_id is None:
        return False, "redis.entry_trigger_event_id", None
    if root_object_id is None:
        return False, "redis.entry_root_object_id", None
    if job_id != trigger_event_id:
        return False, "redis.entry_job_id", None
    return True, "", fields


async def _validate_event_outbox(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    fields: Mapping[str, str],
    raw_values: set[str],
) -> tuple[bool, str, EventOutboxRow | None, UUID | None, UUID | None]:
    trigger_event_id = _coerce_uuid(fields["trigger_event_id"])
    root_object_id = _coerce_uuid(fields["root_object_id"])
    row = await _load_event_outbox_row(session, trigger_event_id)
    if row is None:
        return False, "event_outbox.exists", None, root_object_id, None
    raw_values.update(_raw_values_from_event_row(row))
    report["event_outbox_rehydrated_bucket"] = "one"

    payload = row.payload_json
    judge_run_id = _payload_uuid(payload, "judge_run_id")
    bundle_id = _payload_uuid(payload, "bundle_id")
    missing_payload_fields = [
        field
        for field in sorted(REQUIRED_PAYLOAD_FIELDS)
        if _payload_str(payload, field) is None
    ]

    if row.event_type != EXPECTED_EVENT_TYPE:
        return False, "event_outbox.event_type", row, judge_run_id, bundle_id
    if row.aggregate_type != EXPECTED_AGGREGATE_TYPE:
        return False, "event_outbox.aggregate_type", row, judge_run_id, bundle_id
    if row.status != "published":
        return False, "event_outbox.status", row, judge_run_id, bundle_id
    report["judge_call_requested_published_bucket"] = "one"
    if missing_payload_fields:
        return False, f"payload.{missing_payload_fields[0]}", row, judge_run_id, bundle_id
    if judge_run_id is None:
        return False, "payload.judge_run_id", row, judge_run_id, bundle_id
    if bundle_id is None:
        return False, "payload.bundle_id", row, judge_run_id, bundle_id
    if row.aggregate_id != judge_run_id:
        return False, "aggregate.judge_run_id", row, judge_run_id, bundle_id
    if root_object_id != judge_run_id:
        return False, "redis.event_outbox_root_object_id", row, judge_run_id, bundle_id
    return True, "", row, judge_run_id, bundle_id


async def _validate_judge_run_and_bundle(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    row: EventOutboxRow,
    judge_run_id: UUID,
    bundle_id: UUID,
) -> tuple[bool, str]:
    payload = row.payload_json
    judge_run = await _load_judge_run(session, judge_run_id)
    if judge_run is None:
        return False, "judge_run.exists"
    if judge_run.judge_run_id != judge_run_id:
        return False, "judge_run.judge_run_id"
    if judge_run.bundle_id != bundle_id:
        return False, "judge_run.bundle_id"
    if judge_run.model != _payload_str(payload, "model"):
        return False, "judge_run.model"
    if judge_run.reasoning_effort != _payload_str(payload, "reasoning_effort"):
        return False, "judge_run.reasoning_effort"
    if judge_run.prompt_version != _payload_str(payload, "prompt_version"):
        return False, "judge_run.prompt_version"
    if judge_run.prompt_cache_key != _payload_str(payload, "prompt_cache_key"):
        return False, "judge_run.prompt_cache_key"
    report["judge_run_linked_bucket"] = "one"
    if judge_run.status != EXPECTED_JUDGE_RUN_STATUS:
        return False, "judge_run.status"
    report["judge_run_pending_bucket"] = "one"

    bundle = await _load_bundle(session, bundle_id)
    if bundle is None:
        return False, "bundle.exists"
    if bundle.bundle_id != bundle_id:
        return False, "bundle.bundle_id"
    if not bundle.ready_for_analysis:
        return False, "bundle.ready_for_analysis"
    report["bundle_ready_for_analysis_bucket"] = "one"
    return True, ""


def _status_for_event_validation_check(check: str) -> str:
    if check == "event_outbox.exists":
        return STATUS_MISSING_EVENT
    return STATUS_INVALID_EVENT


def _status_for_judge_validation_check(check: str) -> str:
    if check.startswith("bundle."):
        return STATUS_INVALID_BUNDLE
    return STATUS_INVALID_JUDGE_RUN


async def _validate_route_publish_job_attempt(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    judge_run_id: UUID,
) -> tuple[bool, str]:
    job_attempt_count = await _count_query(
        session,
        COUNT_SUCCEEDED_JUDGE_ROUTE_JOB_ATTEMPT_QUERY,
        {"judge_run_id": str(judge_run_id)},
    )
    report["route_publish_job_attempt_succeeded_bucket"] = _bucket_count(
        job_attempt_count
    )
    if job_attempt_count != 1:
        return False, "job_attempts.succeeded_count_not_one"
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
    report["judge_outputs_written_bucket"] = _bucket_count(judge_output_count)
    if judge_output_count:
        return False, "downstream.judge_outputs"

    judge_output_ready_count = await _count_query(
        session,
        COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY,
        params,
    )
    report["judge_output_ready_outbox_written_bucket"] = _bucket_count(
        judge_output_ready_count
    )
    if judge_output_ready_count:
        return False, "downstream.judge_output_ready_outbox"

    analysis_count = await _count_query(
        session,
        COUNT_ANALYSES_FOR_RUN_QUERY,
        params,
    )
    report["analyses_written_bucket"] = _bucket_count(analysis_count)
    if analysis_count:
        return False, "downstream.analyses"

    policy_count = await _count_query(
        session,
        COUNT_POLICY_SIDE_EFFECTS_FOR_RUN_QUERY,
        params,
    )
    report["analysis_policy_apply_outbox_written_bucket"] = _bucket_count(policy_count)
    if policy_count:
        return False, "downstream.analysis_policy_apply_outbox"

    notification_count = await _count_query(
        session,
        COUNT_NOTIFICATION_PLANS_FOR_RUN_QUERY,
        params,
    )
    report["notification_plans_written_bucket"] = _bucket_count(notification_count)
    if notification_count:
        return False, "downstream.notification_plans"

    return True, ""


async def _run_readiness_checks(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    raw_values: set[str],
) -> ScriptResult | None:
    redis_ok, redis_check, fields = await _inspect_redis_work_item(
        report=report,
        redis_client=redis_client,
        raw_values=raw_values,
    )
    if not redis_ok or fields is None:
        _set_status(report, STATUS_INVALID_REDIS_STREAM, redis_check)
        return ScriptResult(exit_code=1, report=report)

    event_ok, event_check, row, judge_run_id, bundle_id = await _validate_event_outbox(
        report=report,
        session=session,
        fields=fields,
        raw_values=raw_values,
    )
    if not event_ok or row is None or judge_run_id is None or bundle_id is None:
        _set_status(report, _status_for_event_validation_check(event_check), event_check)
        return ScriptResult(exit_code=1, report=report)

    judge_ok, judge_check = await _validate_judge_run_and_bundle(
        report=report,
        session=session,
        row=row,
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
    )
    if not judge_ok:
        _set_status(report, _status_for_judge_validation_check(judge_check), judge_check)
        return ScriptResult(exit_code=1, report=report)

    job_attempt_ok, job_attempt_check = await _validate_route_publish_job_attempt(
        report=report,
        session=session,
        judge_run_id=judge_run_id,
    )
    if not job_attempt_ok:
        _set_status(report, STATUS_MISSING_JOB_ATTEMPT, job_attempt_check)
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
            if _report_contains_raw_values(report, raw_values):
                report["raw_values_emitted"] = True
                _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
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

        readiness_result = await _run_readiness_checks(
            report=report,
            session=session,
            redis_client=redis_client,
            raw_values=raw_values,
        )
        if readiness_result is not None:
            if _report_contains_raw_values(report, raw_values):
                report["raw_values_emitted"] = True
                _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
                return ScriptResult(exit_code=1, report=report)
            return readiness_result

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
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
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
    result = generate_report(runtime_env_path=args.runtime_env_path)
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
