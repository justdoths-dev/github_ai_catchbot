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
SCRIPT_NAME = "dedicated_vps_analysis_requested_analysis_router_consume_smoke"
REPORT_TYPE = "analysis_requested_analysis_router_consume_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_CONSUMER_GROUP = "analysis-router-consume-smoke"
DEFAULT_CONSUMER_NAME = "analysis-router-consume-smoke-1"
EXPECTED_QUEUE_NAME = "q.analysis.route"
EXPECTED_STAGE_NAME = "analysis_route"
EXPECTED_ROOT_OBJECT_TYPE = "candidate_group"
EXPECTED_EVENT_TYPE = "analysis.requested.v1"
EXPECTED_EVENT_STATUS = "published"
EXPECTED_DOWNSTREAM_EVENT_TYPE = "judge.call.requested.v1"
EXPECTED_DOWNSTREAM_AGGREGATE_TYPE = "judge_run"
DEFAULT_MODEL = "gpt-5.4-mini"
ESCALATION_MODEL = "gpt-5.4"
DEFAULT_REASONING_EFFORT = "low"
ESCALATION_REASONING_EFFORT = "medium"
JUDGE_SCHEMA_VERSION = "judge_output_v1"
POLICY_VERSION = "verdict_policy_v1"
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
    "candidate_group_proposals",
    "candidate_evidence_bundles",
    "candidate_evidence_members",
    "judge_runs",
)
SIDE_EFFECT_REPORT_FIELDS = (
    "judge_openai_started",
    "judge_output_written_bucket",
    "analysis_validator_started",
    "analysis_written_bucket",
    "policy_engine_started",
    "notification_plan_written_bucket",
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

STATUS_READY = "analysis_requested_analysis_router_consume_smoke_ready"
STATUS_CONSUMED = "analysis_requested_analysis_router_consume_smoke_consumed"
STATUS_NOT_READY = "blocked_analysis_requested_analysis_router_consume_smoke_not_ready"
STATUS_MISSING_APPROVAL = (
    "blocked_analysis_requested_analysis_router_consume_smoke_missing_approval"
)
STATUS_NO_STREAM_ENTRY = (
    "blocked_analysis_requested_analysis_router_consume_smoke_no_stream_entry"
)
STATUS_INVALID_REDIS_ENTRY = (
    "blocked_analysis_requested_analysis_router_consume_smoke_invalid_redis_entry"
)
STATUS_INVALID_EVENT = (
    "blocked_analysis_requested_analysis_router_consume_smoke_invalid_event"
)
STATUS_INVALID_BUNDLE = (
    "blocked_analysis_requested_analysis_router_consume_smoke_invalid_bundle"
)
STATUS_INVALID_JUDGE_PROFILE = (
    "blocked_analysis_requested_analysis_router_consume_smoke_invalid_judge_profile"
)
STATUS_DB_WRITE_FAILED = (
    "blocked_analysis_requested_analysis_router_consume_smoke_db_write_failed"
)
STATUS_REDIS_ACK_FAILED = (
    "blocked_analysis_requested_analysis_router_consume_smoke_redis_ack_failed"
)
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"
STATUS_RAW_VALUE_EMISSION = (
    "blocked_analysis_requested_analysis_router_consume_smoke_raw_value_emission"
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_ANALYSIS_REQUESTED_EVENT_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json, status
FROM event_outbox
WHERE event_id = CAST(:event_id AS uuid)
"""
SELECT_CANDIDATE_GROUP_STATE_QUERY = """
SELECT candidate_group_id, current_bundle_id
FROM candidate_group_proposals
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""
SELECT_BUNDLE_ROUTE_STATE_QUERY = """
SELECT
    bundle_id,
    candidate_group_id,
    bundle_profile_version,
    reroot_count,
    ready_for_analysis,
    token_budget_profile,
    created_at
FROM candidate_evidence_bundles
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""
COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY = """
SELECT
    COUNT(*) AS member_count,
    COUNT(*) FILTER (WHERE member_role = 'supporting') AS supporting_count
FROM candidate_evidence_members
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""
INSERT_JUDGE_RUN_QUERY = """
INSERT INTO judge_runs (
    bundle_id,
    judge_profile,
    model,
    reasoning_effort,
    prompt_version,
    schema_version,
    policy_version,
    prompt_cache_key,
    status
) VALUES (
    CAST(:bundle_id AS uuid),
    :judge_profile,
    :model,
    :reasoning_effort,
    :prompt_version,
    :schema_version,
    :policy_version,
    :prompt_cache_key,
    'pending'
)
ON CONFLICT ON CONSTRAINT uq_judge_runs_bundle_prompt_model_effort
DO NOTHING
RETURNING judge_run_id
"""
SELECT_EXISTING_JUDGE_RUN_QUERY = """
SELECT judge_run_id
FROM judge_runs
WHERE bundle_id = CAST(:bundle_id AS uuid)
  AND prompt_version = :prompt_version
  AND model = :model
  AND reasoning_effort = :reasoning_effort
LIMIT 1
"""
SELECT_JUDGE_CALL_OUTBOX_QUERY = """
SELECT event_id
FROM event_outbox
WHERE event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
  AND status = 'pending'::outbox_status_enum
LIMIT 1
"""
INSERT_JUDGE_CALL_OUTBOX_QUERY = """
INSERT INTO event_outbox (
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    created_at
) VALUES (
    'judge.call.requested.v1',
    'judge_run',
    CAST(:judge_run_id AS uuid),
    :dedupe_key,
    CAST(:payload_json AS jsonb),
    'pending'::outbox_status_enum,
    now()
)
ON CONFLICT (dedupe_key) DO NOTHING
RETURNING event_id
"""


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


class RedisClientLike(Protocol):
    async def ping(self) -> Any: ...
    async def xlen(self, name: str) -> Any: ...
    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> Any: ...

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = False,
    ) -> Any: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ConsumeApprovals:
    analysis_router_consume: bool
    judge_run_write: bool
    judge_call_requested_outbox_write: bool
    redis_ack: bool

    @property
    def all_granted(self) -> bool:
        return (
            self.analysis_router_consume
            and self.judge_run_write
            and self.judge_call_requested_outbox_write
            and self.redis_ack
        )

    @property
    def any_granted(self) -> bool:
        return (
            self.analysis_router_consume
            or self.judge_run_write
            or self.judge_call_requested_outbox_write
            or self.redis_ack
        )

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.analysis_router_consume:
            checks.append("approval.analysis_router_consume")
        if not self.judge_run_write:
            checks.append("approval.judge_run_write")
        if not self.judge_call_requested_outbox_write:
            checks.append("approval.judge_call_requested_outbox_write")
        if not self.redis_ack:
            checks.append("approval.redis_ack")
        return checks


@dataclass(frozen=True, slots=True)
class StreamEntry:
    message_id: str
    fields: dict[str, str]


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    database_url: str
    redis_url: str
    consumer_group: str
    consumer_name: str
    enable_model_escalation: bool


@dataclass(frozen=True, slots=True)
class AnalysisRequestedEvent:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]
    status: str


@dataclass(frozen=True, slots=True)
class CandidateGroupState:
    candidate_group_id: UUID
    current_bundle_id: UUID | None


@dataclass(frozen=True, slots=True)
class BundleRouteState:
    bundle_id: UUID
    candidate_group_id: UUID
    bundle_profile_version: str
    reroot_count: int
    ready_for_analysis: bool
    token_budget_profile: str | None
    created_at: Any


@dataclass(frozen=True, slots=True)
class EvidenceMemberCounts:
    member_count: int
    supporting_count: int


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.analysis_router.config import AnalysisRouterConfig  # noqa: E402
from src.services.analysis_router.models import (  # noqa: E402
    AnalysisRequestedJob,
    BundleRouteRecord,
    BundleShapeStats,
)
from src.services.analysis_router.routing_policy import (  # noqa: E402
    ALLOWED_JUDGE_PROFILES,
    AnalysisRoutingPolicy,
)


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

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bounded analysis.requested.v1 analysis-router consume smoke. "
            "Default mode validates one q.analysis.route entry and the "
            "PostgreSQL rehydration path without DB writes or Redis ack."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--consumer-group", default=DEFAULT_CONSUMER_GROUP)
    parser.add_argument("--consumer-name", default=DEFAULT_CONSUMER_NAME)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--approved-analysis-router-consume", action="store_true")
    parser.add_argument("--approved-judge-run-write", action="store_true")
    parser.add_argument(
        "--approved-judge-call-requested-outbox-write",
        action="store_true",
    )
    parser.add_argument("--approved-redis-ack", action="store_true")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_READY,
        "runtime_env_read": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "q_analysis_route_entry_found_bucket": "zero",
        "redis_entry_shape_valid_bucket": "zero",
        "analysis_requested_event_rehydrated_bucket": "zero",
        "analysis_requested_event_valid_bucket": "zero",
        "candidate_group_current_bundle_match_bucket": "zero",
        "bundle_ready_for_analysis_bucket": "zero",
        "candidate_evidence_member_found_bucket": "zero",
        "judge_profile_allowed_bucket": "zero",
        "analysis_router_decision_bucket": "zero",
        "judge_run_write_planned_bucket": "zero",
        "judge_call_requested_outbox_write_planned_bucket": "zero",
        "redis_ack_planned_bucket": "zero",
        "judge_run_write_attempted": False,
        "judge_run_written_bucket": "zero",
        "judge_run_reused_bucket": "zero",
        "judge_call_requested_outbox_write_attempted": False,
        "judge_call_requested_outbox_written_bucket": "zero",
        "judge_call_requested_outbox_reused_bucket": "zero",
        "redis_ack_attempted": False,
        "redis_ack_succeeded_bucket": "zero",
        "judge_openai_started": False,
        "judge_output_written_bucket": "zero",
        "analysis_validator_started": False,
        "analysis_written_bucket": "zero",
        "policy_engine_started": False,
        "notification_plan_written_bucket": "zero",
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


def _bool_env(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


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


def _sql(statement: str) -> Any:
    import sqlalchemy as sa

    return sa.text(statement)


async def _execute(
    session: AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(_sql(statement), params or {})


async def _scalar(result: Any) -> Any:
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


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if hasattr(result, "mappings"):
        return list(result.mappings().all())
    if isinstance(result, list):
        return result
    return list(result)


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    mappings = result.mappings() if hasattr(result, "mappings") else None
    if mappings is not None:
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


async def _check_required_tables(session: AsyncSessionLike) -> bool:
    for table in REQUIRED_TABLES:
        available = bool(
            await _scalar(
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
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _payload_json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return value if isinstance(value, dict) else {}


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


def _runtime_config_from_values(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
    consumer_group: str,
    consumer_name: str,
) -> RuntimeConfig | None:
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
    if not consumer_group.strip():
        _set_status(report, STATUS_NOT_READY, "redis.consumer_group_missing")
        return None
    if not consumer_name.strip():
        _set_status(report, STATUS_NOT_READY, "redis.consumer_name_missing")
        return None
    return RuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        consumer_group=consumer_group.strip(),
        consumer_name=consumer_name.strip(),
        enable_model_escalation=_bool_env(values.get("ENABLE_MODEL_ESCALATION")),
    )


def _analysis_router_config(runtime_config: RuntimeConfig) -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="ops-smoke",
        database_url=runtime_config.database_url,
        redis_url=runtime_config.redis_url,
        queue_name=EXPECTED_QUEUE_NAME,
        consumer_group=runtime_config.consumer_group,
        consumer_name=runtime_config.consumer_name,
        batch_size=1,
        block_ms=1,
        enable_model_escalation=runtime_config.enable_model_escalation,
        default_model=DEFAULT_MODEL,
        escalation_model=ESCALATION_MODEL,
        default_reasoning_effort=DEFAULT_REASONING_EFFORT,
        escalation_reasoning_effort=ESCALATION_REASONING_EFFORT,
        github_prompt_version="judge_github_primary_v1",
        x_prompt_version="judge_x_primary_v1",
        text_idea_prompt_version="judge_text_idea_primary_v1",
        judge_schema_version=JUDGE_SCHEMA_VERSION,
        policy_version=POLICY_VERSION,
        log_level="INFO",
    )


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    if not side_effect_flags:
        return
    for field in SIDE_EFFECT_REPORT_FIELDS:
        value = side_effect_flags.get(field, False)
        if field.endswith("_bucket"):
            report[field] = "one" if value else report[field]
        elif bool(value):
            report[field] = True


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    for field in SIDE_EFFECT_REPORT_FIELDS:
        value = report[field]
        if isinstance(value, str) and value != "zero":
            return True
        if isinstance(value, bool) and value:
            return True
    return False


def _approval_block_status(report: dict[str, Any], approvals: ConsumeApprovals) -> None:
    _set_status(report, STATUS_MISSING_APPROVAL)
    for check in approvals.missing_checks():
        _set_status(report, report["contract_status"], check)


def _decode_value(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, str]:
    return {_decode_value(key): _decode_value(value) for key, value in fields.items()}


def _stream_entries_from_xrange(raw: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for message_id, fields in raw or []:
        entries.append(StreamEntry(message_id=_decode_value(message_id), fields=_decode_fields(fields)))
    return entries


def _stream_entries_from_xreadgroup(raw: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for _stream_name, stream_entries in raw or []:
        for message_id, fields in stream_entries:
            entries.append(
                StreamEntry(message_id=_decode_value(message_id), fields=_decode_fields(fields))
            )
    return entries


async def _peek_one_stream_entry(redis: RedisClientLike) -> tuple[int, StreamEntry | None]:
    length = _safe_count(await redis.xlen(EXPECTED_QUEUE_NAME))
    raw_entries = await redis.xrange(EXPECTED_QUEUE_NAME, min="-", max="+", count=2)
    entries = _stream_entries_from_xrange(raw_entries)
    return max(length, len(entries)), entries[0] if entries else None


async def _consume_one_stream_entry(
    redis: RedisClientLike,
    *,
    consumer_group: str,
    consumer_name: str,
) -> tuple[int, StreamEntry | None]:
    length = _safe_count(await redis.xlen(EXPECTED_QUEUE_NAME))
    try:
        await redis.xgroup_create(
            EXPECTED_QUEUE_NAME,
            consumer_group,
            id="0",
            mkstream=False,
        )
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    raw_entries = await redis.xreadgroup(
        consumer_group,
        consumer_name,
        {EXPECTED_QUEUE_NAME: ">"},
        count=1,
        block=1,
    )
    entries = _stream_entries_from_xreadgroup(raw_entries)
    return max(length, len(entries)), entries[0] if entries else None


def validate_redis_entry_shape(fields: Mapping[str, Any]) -> tuple[bool, str]:
    keys = set(fields)
    if keys != ALLOWED_REDIS_THIN_FIELDS:
        return False, "redis.fields"
    if any(_is_forbidden_redis_field(key) for key in keys):
        return False, "redis.business_payload"
    if str(fields.get("stage_name", "")).strip() != EXPECTED_STAGE_NAME:
        return False, "redis.stage_name"
    if str(fields.get("root_object_type", "")).strip() != EXPECTED_ROOT_OBJECT_TYPE:
        return False, "redis.root_object_type"
    if not str(fields.get("trigger_event_id", "")).strip():
        return False, "redis.trigger_event_id"
    for required in ("job_id", "root_object_id", "idempotency_key"):
        if not str(fields.get(required, "")).strip():
            return False, f"redis.{required}"
    return True, ""


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_REDIS_FIELD_TOKENS)


async def _load_analysis_requested_event(
    session: AsyncSessionLike,
    trigger_event_id: UUID,
) -> AnalysisRequestedEvent | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_ANALYSIS_REQUESTED_EVENT_QUERY,
            {"event_id": str(trigger_event_id)},
        )
    )
    if row is None:
        return None
    aggregate_id = _uuid_or_none(row["aggregate_id"])
    if aggregate_id is None:
        return None
    return AnalysisRequestedEvent(
        event_id=_coerce_uuid(row["event_id"]),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=aggregate_id,
        payload_json=_payload_json(row["payload_json"]),
        status=str(row["status"]),
    )


async def _load_candidate_group_state(
    session: AsyncSessionLike,
    candidate_group_id: UUID,
) -> CandidateGroupState | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_CANDIDATE_GROUP_STATE_QUERY,
            {"candidate_group_id": str(candidate_group_id)},
        )
    )
    if row is None:
        return None
    return CandidateGroupState(
        candidate_group_id=_coerce_uuid(row["candidate_group_id"]),
        current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
    )


async def _load_bundle_route_state(
    session: AsyncSessionLike,
    bundle_id: UUID,
) -> BundleRouteState | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_BUNDLE_ROUTE_STATE_QUERY,
            {"bundle_id": str(bundle_id)},
        )
    )
    if row is None:
        return None
    return BundleRouteState(
        bundle_id=_coerce_uuid(row["bundle_id"]),
        candidate_group_id=_coerce_uuid(row["candidate_group_id"]),
        bundle_profile_version=str(row["bundle_profile_version"]),
        reroot_count=_safe_count(row["reroot_count"]),
        ready_for_analysis=bool(row["ready_for_analysis"]),
        token_budget_profile=(
            str(row["token_budget_profile"]) if row["token_budget_profile"] else None
        ),
        created_at=row["created_at"],
    )


async def _load_evidence_member_counts(
    session: AsyncSessionLike,
    bundle_id: UUID,
) -> EvidenceMemberCounts:
    row = _first_mapping(
        await _execute(
            session,
            COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY,
            {"bundle_id": str(bundle_id)},
        )
    )
    if row is None:
        return EvidenceMemberCounts(member_count=0, supporting_count=0)
    return EvidenceMemberCounts(
        member_count=_safe_count(row["member_count"]),
        supporting_count=_safe_count(row["supporting_count"]),
    )


async def _validate_rehydrated_event(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    trigger_event_id: UUID,
    raw_values: set[str],
) -> tuple[
    AnalysisRequestedJob | None,
    BundleRouteState | None,
    EvidenceMemberCounts | None,
    str,
]:
    event = await _load_analysis_requested_event(session, trigger_event_id)
    if event is None:
        return None, None, None, "event_outbox.rehydrated"
    report["analysis_requested_event_rehydrated_bucket"] = "one"
    _add_raw_values_from_event(
        raw_values=raw_values,
        event_id=event.event_id,
        candidate_group_id=_uuid_or_none(event.payload_json.get("candidate_group_id")),
        bundle_id=_uuid_or_none(event.payload_json.get("bundle_id")),
        payload=event.payload_json,
    )

    payload = event.payload_json
    candidate_group_id = _payload_uuid(payload, "candidate_group_id")
    bundle_id = _payload_uuid(payload, "bundle_id")
    judge_profile = _payload_str(payload, "judge_profile")

    if event.event_type != EXPECTED_EVENT_TYPE:
        return None, None, None, "event_outbox.event_type"
    if event.status != EXPECTED_EVENT_STATUS:
        return None, None, None, "event_outbox.status"
    if event.aggregate_type != EXPECTED_ROOT_OBJECT_TYPE:
        return None, None, None, "event_outbox.aggregate_type"
    if candidate_group_id is None:
        return None, None, None, "payload.candidate_group_id"
    if bundle_id is None:
        return None, None, None, "payload.bundle_id"
    if "escalation_allowed" not in payload:
        return None, None, None, "payload.escalation_allowed"
    if event.aggregate_id != candidate_group_id:
        return None, None, None, "event_outbox.aggregate_id"
    report["analysis_requested_event_valid_bucket"] = "one"

    if judge_profile not in ALLOWED_JUDGE_PROFILES:
        return None, None, None, "judge_profile.allowed"
    report["judge_profile_allowed_bucket"] = "one"

    candidate_group = await _load_candidate_group_state(session, candidate_group_id)
    if candidate_group is None:
        return None, None, None, "candidate_group.exists"
    if candidate_group.current_bundle_id != bundle_id:
        return None, None, None, "candidate_group.current_bundle_id"
    report["candidate_group_current_bundle_match_bucket"] = "one"

    bundle = await _load_bundle_route_state(session, bundle_id)
    if bundle is None:
        return None, None, None, "bundle.exists"
    if bundle.candidate_group_id != candidate_group_id:
        return None, None, None, "bundle.candidate_group_id"
    if not bundle.ready_for_analysis:
        return None, None, None, "bundle.ready_for_analysis"
    report["bundle_ready_for_analysis_bucket"] = "one"

    counts = await _load_evidence_member_counts(session, bundle_id)
    report["candidate_evidence_member_found_bucket"] = _bucket_count(counts.member_count)
    if counts.member_count <= 0:
        return None, None, None, "bundle.candidate_evidence_members"

    return (
        AnalysisRequestedJob(
            trigger_event_id=trigger_event_id,
            event_type=event.event_type,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
            judge_profile=judge_profile,
            escalation_allowed=bool(payload.get("escalation_allowed", False)),
        ),
        bundle,
        counts,
        "",
    )


def _status_for_validation_check(check: str) -> str:
    if check == "judge_profile.allowed":
        return STATUS_INVALID_JUDGE_PROFILE
    if check.startswith("redis."):
        return STATUS_INVALID_REDIS_ENTRY
    if check.startswith("event_outbox.") or check.startswith("payload."):
        return STATUS_INVALID_EVENT
    if check.startswith("candidate_group.") or check.startswith("bundle."):
        return STATUS_INVALID_BUNDLE
    return STATUS_NOT_READY


def decide_analysis_route(
    *,
    runtime_config: RuntimeConfig,
    job: AnalysisRequestedJob,
    bundle: BundleRouteState,
    counts: EvidenceMemberCounts,
) -> Any:
    policy = AnalysisRoutingPolicy(_analysis_router_config(runtime_config))
    return policy.decide(
        job=job,
        current_bundle_id=job.bundle_id,
        bundle=BundleRouteRecord(
            bundle_id=bundle.bundle_id,
            candidate_group_id=bundle.candidate_group_id,
            bundle_profile_version=bundle.bundle_profile_version,
            reroot_count=bundle.reroot_count,
            ready_for_analysis=bundle.ready_for_analysis,
            token_budget_profile=bundle.token_budget_profile,
            created_at=bundle.created_at,
        ),
        shape=BundleShapeStats(
            member_count=counts.member_count,
            supporting_count=counts.supporting_count,
        ),
    )


async def _get_or_create_judge_run(
    session: AsyncSessionLike,
    *,
    job: AnalysisRequestedJob,
    decision: Any,
) -> tuple[UUID, bool]:
    params = {
        "bundle_id": str(job.bundle_id),
        "judge_profile": decision.judge_profile or "",
        "model": decision.model or "",
        "reasoning_effort": decision.reasoning_effort or "",
        "prompt_version": decision.prompt_version or "",
        "schema_version": decision.schema_version or "",
        "policy_version": decision.policy_version or "",
        "prompt_cache_key": decision.prompt_cache_key or "",
    }
    inserted = await _scalar(await _execute(session, INSERT_JUDGE_RUN_QUERY, params))
    if inserted:
        return _coerce_uuid(inserted), True
    existing = await _scalar(
        await _execute(
            session,
            SELECT_EXISTING_JUDGE_RUN_QUERY,
            {
                "bundle_id": str(job.bundle_id),
                "prompt_version": decision.prompt_version or "",
                "model": decision.model or "",
                "reasoning_effort": decision.reasoning_effort or "",
            },
        )
    )
    if existing is None:
        raise RuntimeError("judge_run postcondition missing")
    return _coerce_uuid(existing), False


async def _get_or_create_judge_call_outbox(
    session: AsyncSessionLike,
    *,
    judge_run_id: UUID,
    job: AnalysisRequestedJob,
    decision: Any,
) -> tuple[UUID, bool]:
    existing = await _scalar(
        await _execute(
            session,
            SELECT_JUDGE_CALL_OUTBOX_QUERY,
            {"judge_run_id": str(judge_run_id)},
        )
    )
    if existing:
        return _coerce_uuid(existing), False

    payload = {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(job.bundle_id),
        "model": decision.model or "",
        "reasoning_effort": decision.reasoning_effort or "",
        "prompt_version": decision.prompt_version or "",
        "prompt_cache_key": decision.prompt_cache_key or "",
    }
    inserted = await _scalar(
        await _execute(
            session,
            INSERT_JUDGE_CALL_OUTBOX_QUERY,
            {
                "judge_run_id": str(judge_run_id),
                "dedupe_key": f"judge-call:{judge_run_id}",
                "payload_json": json.dumps(payload, sort_keys=True),
            },
        )
    )
    if inserted:
        return _coerce_uuid(inserted), True

    existing_after_conflict = await _scalar(
        await _execute(
            session,
            SELECT_JUDGE_CALL_OUTBOX_QUERY,
            {"judge_run_id": str(judge_run_id)},
        )
    )
    if existing_after_conflict is None:
        raise RuntimeError("judge.call.requested.v1 postcondition missing")
    return _coerce_uuid(existing_after_conflict), False


def _add_raw_values_from_stream(entry: StreamEntry, raw_values: set[str]) -> None:
    raw_values.add(entry.message_id)
    for value in entry.fields.values():
        _add_sensitive_raw_value(value, raw_values)


def _add_raw_values_from_event(
    *,
    raw_values: set[str],
    event_id: UUID,
    candidate_group_id: UUID | None,
    bundle_id: UUID | None,
    payload: Mapping[str, Any],
) -> None:
    raw_values.add(str(event_id))
    if candidate_group_id is not None:
        raw_values.add(str(candidate_group_id))
    if bundle_id is not None:
        raw_values.add(str(bundle_id))
    raw_values.add(json.dumps(payload, sort_keys=True, default=str))
    for value in payload.values():
        if isinstance(value, str):
            _add_sensitive_raw_value(value, raw_values)


def _add_sensitive_raw_value(value: str, raw_values: set[str]) -> None:
    stripped = value.strip()
    if len(stripped) < 6:
        return
    lowered = stripped.lower()
    if _uuid_or_none(stripped) is not None:
        raw_values.add(stripped)
    elif lowered.startswith(("http://", "https://", "postgresql://", "postgresql+", "redis://", "rediss://")):
        raw_values.add(stripped)
    elif any(token in lowered for token in ("secret", "token", "password", "credential", "private")):
        raw_values.add(stripped)


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value and value in rendered for value in raw_values)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


async def _generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    consumer_group: str = DEFAULT_CONSUMER_GROUP,
    consumer_name: str = DEFAULT_CONSUMER_NAME,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    raw_values: set[str] = {str(value) for value in forbidden_raw_values if str(value)}
    approvals = approvals or ConsumeApprovals(
        analysis_router_consume=False,
        judge_run_write=False,
        judge_call_requested_outbox_write=False,
        redis_ack=False,
    )
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
        return ScriptResult(exit_code=1, report=report)
    if approvals.any_granted and not approvals.all_granted:
        _approval_block_status(report, approvals)
        return ScriptResult(exit_code=1, report=report)

    session: AsyncSessionLike | None = None
    redis: RedisClientLike | None = None
    db_committed = False
    try:
        values = _read_runtime_env(runtime_env_path, runtime_env_reader)
        report["runtime_env_read"] = True
        runtime_config = _runtime_config_from_values(
            report=report,
            values=values,
            raw_values=raw_values,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
        )
        if runtime_config is None:
            return ScriptResult(exit_code=1, report=report)

        session = await _open_database_session(
            runtime_config.database_url,
            database_session_factory,
        )
        report["database_connected"] = True
        redis = await _open_redis_client(runtime_config.redis_url, redis_client_factory)
        await redis.ping()
        report["redis_connected"] = True

        if not approvals.all_granted:
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only_raw = await _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
            report["read_only_transaction"] = _transaction_read_only_enabled(read_only_raw)
            if not report["read_only_transaction"]:
                _set_status(report, STATUS_NOT_READY, "database.read_only_transaction")
                return ScriptResult(exit_code=1, report=report)

        await _execute(session, SELECT_ONE_QUERY)
        if not await _check_required_tables(session):
            _set_status(report, STATUS_NOT_READY, "database.required_tables")
            return ScriptResult(exit_code=1, report=report)

        if approvals.all_granted:
            entry_count, entry = await _consume_one_stream_entry(
                redis,
                consumer_group=runtime_config.consumer_group,
                consumer_name=runtime_config.consumer_name,
            )
        else:
            entry_count, entry = await _peek_one_stream_entry(redis)
        report["q_analysis_route_entry_found_bucket"] = _bucket_count(entry_count)
        if entry is None:
            _set_status(report, STATUS_NO_STREAM_ENTRY, "redis.stream_entry")
            return ScriptResult(exit_code=1, report=report)
        _add_raw_values_from_stream(entry, raw_values)

        redis_shape_valid, redis_shape_check = validate_redis_entry_shape(entry.fields)
        if not redis_shape_valid:
            _set_status(report, STATUS_INVALID_REDIS_ENTRY, redis_shape_check)
            return ScriptResult(exit_code=1, report=report)
        report["redis_entry_shape_valid_bucket"] = "one"

        trigger_event_id = _uuid_or_none(entry.fields.get("trigger_event_id"))
        if trigger_event_id is None:
            _set_status(report, STATUS_INVALID_REDIS_ENTRY, "redis.trigger_event_id")
            return ScriptResult(exit_code=1, report=report)

        job, bundle, counts, validation_check = await _validate_rehydrated_event(
            report=report,
            session=session,
            trigger_event_id=trigger_event_id,
            raw_values=raw_values,
        )
        if job is None or bundle is None or counts is None:
            _set_status(
                report,
                _status_for_validation_check(validation_check),
                validation_check,
            )
            return ScriptResult(exit_code=1, report=report)
        decision = decide_analysis_route(
            runtime_config=runtime_config,
            job=job,
            bundle=bundle,
            counts=counts,
        )
        if decision.action != "judge":
            _set_status(report, STATUS_INVALID_BUNDLE, f"analysis_router.{decision.action}")
            return ScriptResult(exit_code=1, report=report)
        report["analysis_router_decision_bucket"] = "one"
        report["judge_run_write_planned_bucket"] = "one"
        report["judge_call_requested_outbox_write_planned_bucket"] = "one"
        report["redis_ack_planned_bucket"] = "one"

        if not approvals.all_granted:
            if _report_contains_raw_values(report, raw_values):
                report["raw_values_emitted"] = True
                _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
                return ScriptResult(exit_code=1, report=report)
            _set_status(report, STATUS_READY)
            return ScriptResult(exit_code=0, report=report)

        try:
            report["judge_run_write_attempted"] = True
            judge_run_id, judge_run_created = await _get_or_create_judge_run(
                session,
                job=job,
                decision=decision,
            )
            raw_values.add(str(judge_run_id))
            if judge_run_created:
                report["judge_run_written_bucket"] = "one"
            else:
                report["judge_run_reused_bucket"] = "one"

            report["judge_call_requested_outbox_write_attempted"] = True
            outbox_event_id, outbox_created = await _get_or_create_judge_call_outbox(
                session,
                judge_run_id=judge_run_id,
                job=job,
                decision=decision,
            )
            raw_values.add(str(outbox_event_id))
            if outbox_created:
                report["judge_call_requested_outbox_written_bucket"] = "one"
            else:
                report["judge_call_requested_outbox_reused_bucket"] = "one"

            if _report_contains_raw_values(report, raw_values):
                report["raw_values_emitted"] = True
                _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
                await session.rollback()
                return ScriptResult(exit_code=1, report=report)

            await session.commit()
            db_committed = True
        except Exception:
            _set_status(report, STATUS_DB_WRITE_FAILED, "database.write")
            if session is not None:
                await session.rollback()
            return ScriptResult(exit_code=1, report=report)

        report["redis_ack_attempted"] = True
        try:
            acked = _safe_count(
                await redis.xack(
                    EXPECTED_QUEUE_NAME,
                    runtime_config.consumer_group,
                    entry.message_id,
                )
            )
        except Exception:
            _set_status(report, STATUS_REDIS_ACK_FAILED, "redis.ack")
            return ScriptResult(exit_code=1, report=report)
        report["redis_ack_succeeded_bucket"] = _bucket_count(acked)
        if acked != 1:
            _set_status(report, STATUS_REDIS_ACK_FAILED, "redis.ack")
            return ScriptResult(exit_code=1, report=report)
        _set_status(report, STATUS_CONSUMED)
        return ScriptResult(exit_code=0, report=report)
    except Exception:
        _set_status(report, STATUS_NOT_READY, "runtime.unhandled")
        if session is not None and not db_committed:
            await session.rollback()
        return ScriptResult(exit_code=1, report=report)
    finally:
        if session is not None and not db_committed:
            await session.rollback()
        await _close_database_session(session)
        await _close_redis_client(redis)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    consumer_group: str = DEFAULT_CONSUMER_GROUP,
    consumer_name: str = DEFAULT_CONSUMER_NAME,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        _generate_report_async(
            runtime_env_path=runtime_env_path,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    approvals = ConsumeApprovals(
        analysis_router_consume=bool(args.approved_analysis_router_consume),
        judge_run_write=bool(args.approved_judge_run_write),
        judge_call_requested_outbox_write=bool(
            args.approved_judge_call_requested_outbox_write
        ),
        redis_ack=bool(args.approved_redis_ack),
    )
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        consumer_group=args.consumer_group,
        consumer_name=args.consumer_name,
        approvals=approvals,
    )
    sys.stdout.write(render_json(result.report) + "\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
