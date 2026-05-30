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
SCRIPT_NAME = "dedicated_vps_x_enricher_requested_event_rehydrate_readiness_probe"
REPORT_TYPE = "x_enricher_requested_event_rehydrate_readiness_probe_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_STREAM_ENTRIES = 20
MAX_STREAM_ENTRIES_HARD_LIMIT = 100
EXPECTED_STREAM_NAME = "q.artifact.enrich.x"
EXPECTED_STAGE_NAME = "enrich_x"
EXPECTED_EVENT_TYPE = "artifact.enrich.requested.v1"
EXPECTED_PROVIDER_ROUTE = "x"
EXPECTED_ARTIFACT_TYPE = "x_post"
DEFAULT_REFRESH_MODE = "standard"
DEFAULT_DEPTH_BUDGET = 1
ALLOWED_ROOT_OBJECT_TYPES = {"candidate_group", "artifact"}
REQUIRED_THIN_FIELDS = {
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
)
PUBLIC_REPORT_LABEL_VALUES = {
    EXPECTED_EVENT_TYPE,
    EXPECTED_PROVIDER_ROUTE,
    EXPECTED_ARTIFACT_TYPE,
    EXPECTED_STAGE_NAME,
    DEFAULT_REFRESH_MODE,
    "published",
    "candidate_group",
    "artifact",
    "ready",
    "partial_ready",
    "low_evidence",
}
SIDE_EFFECT_REPORT_FIELDS = (
    "redis_ack_attempted",
    "redis_group_mutation_attempted",
    "redis_publish_attempted",
    "database_write_attempted",
    "external_network_attempted",
    "source_tables_mutation_performed",
    "telegram_raw_updates_mutation_performed",
    "registry_mutation_performed",
    "downstream_enricher_started",
    "evidence_assembler_started",
    "judge_policy_notifier_started",
    "docker_or_systemd_changed",
    "alembic_run",
    "raw_values_emitted",
)

STATUS_READY = "x_enricher_requested_event_rehydrate_readiness_probe_ready"
STATUS_NO_STREAM_ENTRY = (
    "blocked_x_enricher_requested_event_rehydrate_readiness_probe_no_x_stream_entry"
)
STATUS_INVALID_THIN_PAYLOAD = (
    "blocked_x_enricher_requested_event_rehydrate_readiness_probe_invalid_thin_payload"
)
STATUS_EVENT_REHYDRATE_FAILED = (
    "blocked_x_enricher_requested_event_rehydrate_readiness_probe_event_rehydrate_failed"
)
STATUS_INVALID_EVENT_CONTRACT = (
    "blocked_x_enricher_requested_event_rehydrate_readiness_probe_invalid_event_contract"
)
STATUS_INVALID_ARTIFACT = (
    "blocked_x_enricher_requested_event_rehydrate_readiness_probe_invalid_artifact"
)
STATUS_MISSING_TOKEN = (
    "blocked_x_enricher_requested_event_rehydrate_readiness_probe_missing_x_bearer_token"
)
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"

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
    created_at,
    published_at
FROM event_outbox
WHERE event_id = CAST(:event_id AS uuid)
LIMIT 1
"""
SELECT_ARTIFACT_BY_ID_QUERY = """
SELECT
    artifact_id,
    artifact_type,
    canonical_id,
    canonical_url,
    normalized_host,
    artifact_key_json,
    current_snapshot_id,
    current_status
FROM artifact_registry
WHERE artifact_id = CAST(:artifact_id AS uuid)
LIMIT 1
"""
COUNT_CANDIDATE_GROUP_ARTIFACT_MEMBERSHIP_QUERY = """
SELECT COUNT(*)
FROM candidate_group_proposals cgp
JOIN candidate_group_members cgm
  ON cgm.candidate_group_id = cgp.candidate_group_id
WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
  AND cgm.artifact_id = CAST(:artifact_id AS uuid)
"""
COUNT_ARTIFACT_ANY_CANDIDATE_GROUP_MEMBERSHIP_QUERY = """
SELECT COUNT(*)
FROM candidate_group_members cgm
JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = cgm.candidate_group_id
WHERE cgm.artifact_id = CAST(:artifact_id AS uuid)
"""
REQUIRED_TABLES = (
    "event_outbox",
    "artifact_registry",
    "candidate_group_proposals",
    "candidate_group_members",
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

    async def xlen(self, name: str) -> Any: ...

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> Any: ...

    async def xrevrange(
        self,
        name: str,
        max: str = "+",
        min: str = "-",
        count: int | None = None,
    ) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    database_url: str
    redis_url: str
    x_bearer_token_present: bool
    x_base_url_valid: bool


@dataclass(frozen=True, slots=True)
class StreamEntry:
    stream_id: str
    fields: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ThinPayloadContract:
    entry: StreamEntry
    shape_valid: bool
    stage_valid: bool
    root_valid: bool
    trigger_event_id: UUID | None
    root_object_type: str | None
    root_object_id: UUID | None
    checks_failed: list[str]

    @property
    def valid(self) -> bool:
        return self.shape_valid and self.stage_valid and self.root_valid


@dataclass(frozen=True, slots=True)
class EventRecord:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str


@dataclass(frozen=True, slots=True)
class EventContract:
    artifact_id: UUID
    payload_candidate_group_id: UUID | None
    refresh_mode: str
    depth_budget: int


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    artifact_id: UUID
    artifact_type: str
    canonical_id: str
    canonical_url: Any
    normalized_host: Any
    artifact_key_json: Any
    current_snapshot_id: Any
    current_status: Any


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
            "No-network X enricher readiness probe. It inspects the published "
            "q.artifact.enrich.x thin work item, rehydrates event_outbox and "
            "artifact/candidate relations, and emits only sanitized JSON."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--max-stream-entries",
        type=_bounded_positive_int_named(
            "max-stream-entries",
            upper_bound=MAX_STREAM_ENTRIES_HARD_LIMIT,
        ),
        default=DEFAULT_MAX_STREAM_ENTRIES,
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
        "contract_status": STATUS_EVENT_REHYDRATE_FAILED,
        "runtime_env_read": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "x_stream_exists": False,
        "x_stream_length_bucket": "zero",
        "target_stream_entry_found_bucket": "zero",
        "thin_payload_shape_valid_bucket": "zero",
        "thin_payload_stage_valid_bucket": "zero",
        "thin_payload_root_valid_bucket": "zero",
        "event_outbox_rehydrate_succeeded_bucket": "zero",
        "provider_route_x_bucket": "zero",
        "artifact_type_x_post_bucket": "zero",
        "artifact_registry_rehydrate_succeeded_bucket": "zero",
        "artifact_canonical_x_post_bucket": "zero",
        "candidate_membership_valid_bucket": "zero",
        "x_bearer_token_present_bucket": "missing",
        "x_base_url_valid_bucket": "not_configured",
        "redis_ack_attempted": False,
        "redis_group_mutation_attempted": False,
        "redis_publish_attempted": False,
        "database_write_attempted": False,
        "external_network_attempted": False,
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "registry_mutation_performed": False,
        "downstream_enricher_started": False,
        "evidence_assembler_started": False,
        "judge_policy_notifier_started": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
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


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> RuntimeConfig | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    token = str(values.get("X_BEARER_TOKEN", "")).strip()
    x_base_url = str(values.get("X_BASE_URL", "")).strip()
    for raw in (database_url, redis_url, token, x_base_url):
        if raw:
            raw_values.add(raw)
    if not database_url:
        _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "runtime.database_url_missing")
        return None
    if not _database_url_is_supported(database_url):
        _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "runtime.database_url_unsupported")
        return None
    if not redis_url:
        _set_status(report, STATUS_NO_STREAM_ENTRY, "runtime.redis_url_missing")
        return None
    if not _redis_url_is_supported(redis_url):
        _set_status(report, STATUS_NO_STREAM_ENTRY, "runtime.redis_url_unsupported")
        return None

    token_present = bool(token)
    report["x_bearer_token_present_bucket"] = "present" if token_present else "missing"
    if x_base_url:
        x_base_url_valid = x_base_url.startswith("https://")
        report["x_base_url_valid_bucket"] = "valid" if x_base_url_valid else "invalid"
    else:
        x_base_url_valid = True
        report["x_base_url_valid_bucket"] = "not_configured"
    return RuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        x_bearer_token_present=token_present,
        x_base_url_valid=x_base_url_valid,
    )


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _sql(statement: str) -> Any:
    from sqlalchemy import text  # type: ignore[import-not-found]

    return text(statement)


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


async def _close_redis_client(redis_client: RedisClientLike | None) -> None:
    if redis_client is None:
        return
    close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
    if close is not None:
        await _maybe_await(close())


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
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "first"):
            row = mappings.first()
            return row
        rows = mappings.all()
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


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _json_loads(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


async def _inspect_target_stream(
    *,
    redis_client: RedisClientLike,
    max_stream_entries: int,
    report: dict[str, Any],
    raw_values: set[str],
) -> ThinPayloadContract | None:
    await _maybe_await(redis_client.ping())
    report["redis_connected"] = True
    stream_length = _safe_count(await _maybe_await(redis_client.xlen(EXPECTED_STREAM_NAME)))
    report["x_stream_exists"] = stream_length > 0
    report["x_stream_length_bucket"] = _bucket_count(stream_length)
    if stream_length <= 0:
        _set_status(report, STATUS_NO_STREAM_ENTRY, "redis.x_stream_entry_missing")
        return None

    xrevrange = getattr(redis_client, "xrevrange", None)
    if xrevrange is not None:
        raw_entries = await _maybe_await(
            xrevrange(EXPECTED_STREAM_NAME, max="+", min="-", count=max_stream_entries)
        )
    else:
        raw_entries = await _maybe_await(
            redis_client.xrange(EXPECTED_STREAM_NAME, min="-", max="+", count=max_stream_entries)
        )
    entries = _decode_stream_entries(raw_entries)
    if not entries:
        _set_status(report, STATUS_NO_STREAM_ENTRY, "redis.x_stream_entry_missing")
        return None

    valid_contracts: list[ThinPayloadContract] = []
    shape_valid_count = 0
    stage_valid_count = 0
    root_valid_count = 0
    checks_failed: list[str] = []
    for entry in entries:
        raw_values.add(entry.stream_id)
        _collect_raw_values_from_stream_fields(entry.fields, raw_values)
        contract = _validate_thin_payload_entry(entry)
        if contract.shape_valid:
            shape_valid_count += 1
        if contract.stage_valid:
            stage_valid_count += 1
        if contract.root_valid:
            root_valid_count += 1
        checks_failed.extend(contract.checks_failed)
        if contract.valid:
            valid_contracts.append(contract)

    report["thin_payload_shape_valid_bucket"] = _bucket_count(shape_valid_count)
    report["thin_payload_stage_valid_bucket"] = _bucket_count(stage_valid_count)
    report["thin_payload_root_valid_bucket"] = _bucket_count(root_valid_count)

    if len(valid_contracts) == 1:
        report["target_stream_entry_found_bucket"] = "one"
        return valid_contracts[0]
    if len(valid_contracts) > 1:
        _set_status(report, STATUS_INVALID_THIN_PAYLOAD, "redis.target_stream_entry_duplicate")
        return None

    _set_status(report, STATUS_INVALID_THIN_PAYLOAD)
    for check in checks_failed or ["redis.thin_payload_shape"]:
        _set_status(report, report["contract_status"], check)
    return None


def _decode_stream_entries(raw_entries: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for message_id, fields in raw_entries or []:
        decoded_id = message_id.decode("utf-8") if isinstance(message_id, bytes) else str(message_id)
        entries.append(StreamEntry(stream_id=decoded_id, fields=_decode_fields(fields)))
    return entries


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        decoded_value = value.decode("utf-8") if isinstance(value, bytes) else value
        decoded[decoded_key] = decoded_value
    return decoded


def _validate_thin_payload_entry(entry: StreamEntry) -> ThinPayloadContract:
    checks_failed: list[str] = []
    keys = set(entry.fields)
    shape_valid = keys == REQUIRED_THIN_FIELDS
    if not shape_valid:
        checks_failed.append("redis.thin_payload_shape")
    if any(_is_forbidden_redis_field(key) for key in keys):
        shape_valid = False
        checks_failed.append("redis.thin_payload_forbidden_field")

    for key in (
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "trigger_event_id",
    ):
        if not str(entry.fields.get(key, "")).strip():
            shape_valid = False
            checks_failed.append(f"redis.{key}_missing")

    trigger_event_id: UUID | None = None
    root_object_id: UUID | None = None
    try:
        trigger_event_id = _coerce_uuid(entry.fields.get("trigger_event_id"))
    except (TypeError, ValueError):
        shape_valid = False
        checks_failed.append("redis.trigger_event_id_invalid")
    try:
        root_object_id = _coerce_uuid(entry.fields.get("root_object_id"))
    except (TypeError, ValueError):
        shape_valid = False
        checks_failed.append("redis.root_object_id_invalid")

    stage_valid = str(entry.fields.get("stage_name", "")) == EXPECTED_STAGE_NAME
    if not stage_valid:
        checks_failed.append("redis.stage_name_mismatch")
    root_object_type = str(entry.fields.get("root_object_type", ""))
    root_valid = root_object_type in ALLOWED_ROOT_OBJECT_TYPES and root_object_id is not None
    if root_object_type not in ALLOWED_ROOT_OBJECT_TYPES:
        checks_failed.append("redis.root_object_type_mismatch")

    return ThinPayloadContract(
        entry=entry,
        shape_valid=shape_valid,
        stage_valid=stage_valid,
        root_valid=root_valid,
        trigger_event_id=trigger_event_id,
        root_object_type=root_object_type if root_object_type else None,
        root_object_id=root_object_id,
        checks_failed=checks_failed,
    )


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_REDIS_FIELD_TOKENS)


def _collect_raw_values_from_stream_fields(fields: Mapping[str, Any], raw_values: set[str]) -> None:
    for key in (
        "job_id",
        "root_object_id",
        "idempotency_key",
        "pipeline_run_id",
        "not_before",
        "trigger_event_id",
    ):
        value = fields.get(key)
        if value is not None:
            raw_values.add(str(value))


async def _load_event_record(
    *,
    session: AsyncSessionLike,
    trigger_event_id: UUID,
    report: dict[str, Any],
    raw_values: set[str],
) -> EventRecord | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_EVENT_OUTBOX_BY_ID_QUERY,
            {"event_id": str(trigger_event_id)},
        )
    )
    if row is None:
        _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "event_outbox.row_missing")
        return None
    payload = _json_loads(row["payload_json"]) or {}
    if not isinstance(payload, dict):
        payload = {}
    event = EventRecord(
        event_id=_coerce_uuid(row["event_id"]),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=_coerce_uuid(row["aggregate_id"]),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload,
        status=str(row["status"]),
    )
    report["event_outbox_rehydrate_succeeded_bucket"] = "one"
    _collect_raw_values_from_event(event, raw_values)
    return event


def _validate_event_contract(
    *,
    event: EventRecord,
    thin: ThinPayloadContract,
    report: dict[str, Any],
) -> EventContract | None:
    payload = event.payload_json
    provider_route = _payload_str(payload, "provider_route")
    artifact_type = _payload_str(payload, "artifact_type")
    if provider_route == EXPECTED_PROVIDER_ROUTE:
        report["provider_route_x_bucket"] = "one"
    if artifact_type == EXPECTED_ARTIFACT_TYPE:
        report["artifact_type_x_post_bucket"] = "one"

    if event.event_type != EXPECTED_EVENT_TYPE:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "event_outbox.event_type")
        return None
    if event.status != "published":
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "event_outbox.status")
        return None
    if event.aggregate_type not in ALLOWED_ROOT_OBJECT_TYPES:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "event_outbox.aggregate_type")
        return None
    if thin.root_object_type != event.aggregate_type or thin.root_object_id != event.aggregate_id:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "event_outbox.thin_root_mismatch")
        return None
    if provider_route != EXPECTED_PROVIDER_ROUTE:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "payload.provider_route")
        return None
    if artifact_type != EXPECTED_ARTIFACT_TYPE:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "payload.artifact_type")
        return None

    artifact_id = _payload_uuid(payload, "artifact_id")
    if artifact_id is None:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "payload.artifact_id")
        return None
    if event.aggregate_type == "artifact" and event.aggregate_id != artifact_id:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "event_outbox.artifact_aggregate")
        return None

    payload_candidate_group_id = _payload_uuid(payload, "candidate_group_id")
    refresh_mode = _payload_str(payload, "refresh_mode") or DEFAULT_REFRESH_MODE
    depth_budget = _payload_int(payload.get("depth_budget"), default=DEFAULT_DEPTH_BUDGET)
    if depth_budget != DEFAULT_DEPTH_BUDGET:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "payload.depth_budget")
        return None
    if not refresh_mode:
        _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "payload.refresh_mode")
        return None

    return EventContract(
        artifact_id=artifact_id,
        payload_candidate_group_id=payload_candidate_group_id,
        refresh_mode=refresh_mode,
        depth_budget=depth_budget,
    )


async def _load_artifact_record(
    *,
    session: AsyncSessionLike,
    artifact_id: UUID,
    report: dict[str, Any],
    raw_values: set[str],
) -> ArtifactRecord | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_ARTIFACT_BY_ID_QUERY,
            {"artifact_id": str(artifact_id)},
        )
    )
    if row is None:
        _set_status(report, STATUS_INVALID_ARTIFACT, "artifact_registry.row_missing")
        return None
    artifact = ArtifactRecord(
        artifact_id=_coerce_uuid(row["artifact_id"]),
        artifact_type=str(row["artifact_type"]),
        canonical_id=str(row["canonical_id"]),
        canonical_url=row["canonical_url"],
        normalized_host=row["normalized_host"],
        artifact_key_json=_json_loads(row["artifact_key_json"]),
        current_snapshot_id=row["current_snapshot_id"],
        current_status=row["current_status"],
    )
    report["artifact_registry_rehydrate_succeeded_bucket"] = "one"
    _collect_raw_values_from_artifact(artifact, raw_values)
    if artifact.artifact_type != EXPECTED_ARTIFACT_TYPE:
        _set_status(report, STATUS_INVALID_ARTIFACT, "artifact_registry.artifact_type")
        return None
    post_id = extract_x_post_id_from_canonical_id(artifact.canonical_id)
    if post_id is None:
        _set_status(report, STATUS_INVALID_ARTIFACT, "artifact_registry.canonical_id")
        return None
    raw_values.add(post_id)
    report["artifact_canonical_x_post_bucket"] = "one"
    return artifact


async def _validate_candidate_membership(
    *,
    session: AsyncSessionLike,
    event: EventRecord,
    contract: EventContract,
    report: dict[str, Any],
) -> bool:
    artifact_id = contract.artifact_id
    payload_group_id = contract.payload_candidate_group_id
    if event.aggregate_type == "candidate_group":
        if payload_group_id is not None and payload_group_id != event.aggregate_id:
            _set_status(report, STATUS_INVALID_ARTIFACT, "candidate.payload_candidate_group_mismatch")
            return False
        if await _candidate_group_contains_artifact(
            session=session,
            candidate_group_id=event.aggregate_id,
            artifact_id=artifact_id,
        ):
            report["candidate_membership_valid_bucket"] = "one"
            return True
        _set_status(report, STATUS_INVALID_ARTIFACT, "candidate.membership")
        return False

    if payload_group_id is not None:
        if await _candidate_group_contains_artifact(
            session=session,
            candidate_group_id=payload_group_id,
            artifact_id=artifact_id,
        ):
            report["candidate_membership_valid_bucket"] = "one"
            return True
        _set_status(report, STATUS_INVALID_ARTIFACT, "candidate.payload_candidate_group_mismatch")
        return False

    if await _artifact_has_any_candidate_group(session=session, artifact_id=artifact_id):
        report["candidate_membership_valid_bucket"] = "one"
        return True
    _set_status(report, STATUS_INVALID_ARTIFACT, "candidate.membership")
    return False


async def _candidate_group_contains_artifact(
    *,
    session: AsyncSessionLike,
    candidate_group_id: UUID,
    artifact_id: UUID,
) -> bool:
    value = await _scalar(
        await _execute(
            session,
            COUNT_CANDIDATE_GROUP_ARTIFACT_MEMBERSHIP_QUERY,
            {
                "candidate_group_id": str(candidate_group_id),
                "artifact_id": str(artifact_id),
            },
        )
    )
    return _safe_count(value) > 0


async def _artifact_has_any_candidate_group(
    *,
    session: AsyncSessionLike,
    artifact_id: UUID,
) -> bool:
    value = await _scalar(
        await _execute(
            session,
            COUNT_ARTIFACT_ANY_CANDIDATE_GROUP_MEMBERSHIP_QUERY,
            {"artifact_id": str(artifact_id)},
        )
    )
    return _safe_count(value) > 0


def _payload_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _payload_uuid(payload: Mapping[str, Any], key: str) -> UUID | None:
    raw = _payload_str(payload, key)
    if raw is None:
        return None
    try:
        return _coerce_uuid(raw)
    except ValueError:
        return None


def _payload_int(value: Any, *, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def extract_x_post_id_from_canonical_id(canonical_id: str) -> str | None:
    if not canonical_id.startswith("x:post:"):
        return None
    post_id = canonical_id.split("x:post:", 1)[1]
    return post_id or None


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    if not side_effect_flags:
        return
    for field in SIDE_EFFECT_REPORT_FIELDS:
        if bool(side_effect_flags.get(field, False)):
            report[field] = True


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    return any(bool(report[field]) for field in SIDE_EFFECT_REPORT_FIELDS)


def _collect_raw_values_from_event(event: EventRecord, raw_values: set[str]) -> None:
    raw_values.update(
        {
            str(event.event_id),
            str(event.aggregate_id),
            event.dedupe_key,
            json.dumps(event.payload_json, sort_keys=True, default=str),
        }
    )
    _collect_raw_json_values(event.payload_json, raw_values)


def _collect_raw_values_from_artifact(artifact: ArtifactRecord, raw_values: set[str]) -> None:
    raw_values.update(
        {
            str(artifact.artifact_id),
            artifact.canonical_id,
            str(artifact.canonical_url or ""),
            str(artifact.normalized_host or ""),
            json.dumps(artifact.artifact_key_json, sort_keys=True, default=str),
            str(artifact.current_snapshot_id or ""),
            str(artifact.current_status or ""),
        }
    )


def _collect_raw_json_values(value: Any, raw_values: set[str]) -> None:
    if isinstance(value, Mapping):
        for nested in value.values():
            _collect_raw_json_values(nested, raw_values)
        return
    if isinstance(value, list):
        for nested in value:
            _collect_raw_json_values(nested, raw_values)
        return
    if value is not None:
        raw_values.add(str(value))


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(
        value in rendered
        for value in raw_values
        if len(value) >= 6 and value not in PUBLIC_REPORT_LABEL_VALUES
    )


def _finish_result(
    *,
    exit_code: int,
    report: dict[str, Any],
    raw_values: set[str],
) -> ScriptResult:
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_stream_entries: int = DEFAULT_MAX_STREAM_ENTRIES,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
        return _finish_result(exit_code=1, report=report, raw_values=raw_values)

    if max_stream_entries <= 0 or max_stream_entries > MAX_STREAM_ENTRIES_HARD_LIMIT:
        _set_status(report, STATUS_NO_STREAM_ENTRY, "max_stream_entries.out_of_bounds")
        return _finish_result(exit_code=1, report=report, raw_values=raw_values)

    session: AsyncSessionLike | None = None
    redis_client: RedisClientLike | None = None

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
            raw_values.add(str(runtime_env_path))
        except Exception:
            _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "runtime_env.read")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        runtime_config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            session = await _open_database_session(
                runtime_config.database_url,
                database_session_factory,
            )
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only_value = await _scalar(
                await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY)
            )
            report["read_only_transaction"] = _transaction_read_only_enabled(read_only_value)
            if not report["read_only_transaction"]:
                _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "database.read_only_transaction")
                return _finish_result(exit_code=1, report=report, raw_values=raw_values)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "database.required_tables")
                return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        except Exception:
            _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "database.connection_or_schema")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            redis_client = await _open_redis_client(runtime_config.redis_url, redis_client_factory)
            thin_contract = await _inspect_target_stream(
                redis_client=redis_client,
                max_stream_entries=max_stream_entries,
                report=report,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_NO_STREAM_ENTRY, "redis.connection_or_stream_inspection")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if thin_contract is None:
            exit_code = 0 if report["contract_status"] == STATUS_READY else 1
            return _finish_result(exit_code=exit_code, report=report, raw_values=raw_values)
        if thin_contract.trigger_event_id is None:
            _set_status(report, STATUS_INVALID_THIN_PAYLOAD, "redis.trigger_event_id_invalid")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            event = await _load_event_record(
                session=session,
                trigger_event_id=thin_contract.trigger_event_id,
                report=report,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "event_outbox.rehydrate")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if event is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        event_contract = _validate_event_contract(
            event=event,
            thin=thin_contract,
            report=report,
        )
        if event_contract is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            artifact = await _load_artifact_record(
                session=session,
                artifact_id=event_contract.artifact_id,
                report=report,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_INVALID_ARTIFACT, "artifact_registry.rehydrate")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if artifact is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        if not await _validate_candidate_membership(
            session=session,
            event=event,
            contract=event_contract,
            report=report,
        ):
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        if not runtime_config.x_base_url_valid:
            _set_status(report, STATUS_INVALID_EVENT_CONTRACT, "runtime.x_base_url")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if not runtime_config.x_bearer_token_present:
            _set_status(report, STATUS_MISSING_TOKEN, "runtime.x_bearer_token_missing")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        _set_status(report, STATUS_READY)
        return _finish_result(exit_code=0, report=report, raw_values=raw_values)
    except Exception:
        if session is not None:
            await _maybe_await(session.rollback())
        _set_status(report, STATUS_EVENT_REHYDRATE_FAILED, "unexpected")
        return _finish_result(exit_code=1, report=report, raw_values=raw_values)
    finally:
        if session is not None:
            await _maybe_await(session.rollback())
        await _close_redis_client(redis_client)
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_stream_entries: int = DEFAULT_MAX_STREAM_ENTRIES,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            max_stream_entries=max_stream_entries,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        max_stream_entries=args.max_stream_entries,
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
