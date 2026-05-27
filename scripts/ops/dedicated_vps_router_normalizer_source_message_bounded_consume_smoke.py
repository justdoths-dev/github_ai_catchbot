from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import UUID, uuid4


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_router_normalizer_source_message_bounded_consume_smoke"
REPORT_TYPE = "router_normalizer_source_message_bounded_consume_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
EXPECTED_STREAM_NAME = "q.source.normalize"
EXPECTED_STAGE_NAME = "normalize"
EXPECTED_ROOT_OBJECT_TYPE = "source_message"
DEFAULT_CONSUMER_GROUP = "router-normalizer-source-message-bounded-consume-smoke"
DEFAULT_CONSUMER_NAME = "router-normalizer-source-message-bounded-consume-smoke-1"
DEFAULT_BLOCK_MS = 100
DEFAULT_NORMALIZER_VERSION = "router-normalizer-v1"
SOURCE_MESSAGE_EVENT_TYPES = (
    "source_message.created.v1",
    "source_message.edited.v1",
    "source_message.deleted.v1",
    "source_message.reconciled.v1",
)
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
FORBIDDEN_FIELD_TOKENS = (
    "payload",
    "raw",
    "text",
    "caption",
    "message_text",
    "database_url",
    "redis_url",
    "secret",
    "token",
    "password",
    "api_key",
    "apikey",
)
SIDE_EFFECT_REPORT_FIELDS = (
    "source_tables_mutation_performed",
    "telegram_raw_updates_mutation_performed",
    "registry_mutation_performed",
    "downstream_service_started",
    "external_network_attempted",
    "docker_or_systemd_changed",
    "alembic_run",
    "raw_values_emitted",
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_EVENT_OUTBOX_QUERY = """
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
"""
SELECT_SOURCE_MESSAGE_QUERY = """
SELECT
    source_message_id,
    current_version_no,
    text_body,
    caption_text,
    text_surface,
    entities_json,
    url_surface_json,
    raw_message_json,
    deleted_at
FROM source_messages
WHERE source_message_id = CAST(:source_message_id AS uuid)
"""
SELECT_SOURCE_VERSION_QUERY = """
SELECT
    source_message_id,
    version_no,
    text_surface,
    entities_json,
    raw_message_json
FROM source_message_versions
WHERE source_message_id = CAST(:source_message_id AS uuid)
  AND version_no = :version_no
"""
REQUIRED_READ_TABLES = ("event_outbox", "source_messages", "source_message_versions")
REQUIRED_WRITE_TABLES = (
    "normalization_runs",
    "normalization_suppression_traces",
    "artifact_registry",
    "artifact_observations",
    "candidate_group_proposals",
    "candidate_group_members",
    "event_outbox",
)


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
        id: str = "$",
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
NormalizerRunner = Callable[[Any, Any, AsyncSessionLike], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ConsumeApprovals:
    router_normalizer_consume_smoke: bool
    redis_consumer_group: bool
    normalization_write: bool
    artifact_candidate_write: bool
    event_outbox_write: bool
    redis_ack: bool

    @property
    def all_granted(self) -> bool:
        return (
            self.router_normalizer_consume_smoke
            and self.redis_consumer_group
            and self.normalization_write
            and self.artifact_candidate_write
            and self.event_outbox_write
            and self.redis_ack
        )

    @property
    def any_granted(self) -> bool:
        return (
            self.router_normalizer_consume_smoke
            or self.redis_consumer_group
            or self.normalization_write
            or self.artifact_candidate_write
            or self.event_outbox_write
            or self.redis_ack
        )

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.router_normalizer_consume_smoke:
            checks.append("approval.router_normalizer_consume_smoke")
        if not self.redis_consumer_group:
            checks.append("approval.redis_consumer_group")
        if not self.normalization_write:
            checks.append("approval.normalization_write")
        if not self.artifact_candidate_write:
            checks.append("approval.artifact_candidate_write")
        if not self.event_outbox_write:
            checks.append("approval.event_outbox_write")
        if not self.redis_ack:
            checks.append("approval.redis_ack")
        return checks


@dataclass(frozen=True, slots=True)
class SelectedStreamMessage:
    stream_id: str
    fields: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ThinMessageContract:
    valid: bool
    trigger_event_id: UUID | None
    root_object_id: UUID | None
    checks_failed: list[str]


@dataclass(frozen=True, slots=True)
class RehydratedInputs:
    outbox_event: Any
    source_snapshot: Any
    version_snapshot: Any | None
    requested_version: int | None


@dataclass(frozen=True, slots=True)
class NormalizationPlan:
    signal_detected: bool
    candidate_eligible: bool
    artifact_count: int
    candidate_group_count: int
    suppression_count: int


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.router_normalizer.canonicalizer import (  # noqa: E402
    build_text_idea_artifact,
    canonicalize_resolved_urls,
)
from src.services.router_normalizer.config import RouterNormalizerConfig  # noqa: E402
from src.services.router_normalizer.models import (  # noqa: E402
    OutboxEventRow,
    RedisNormalizeMessage,
    ResolvedUrl,
    SourceMessageSnapshot,
)
from src.services.router_normalizer.redis_streams import RedisStreamsConsumer  # noqa: E402
from src.services.router_normalizer.repositories import RouterNormalizerRepository  # noqa: E402
from src.services.router_normalizer.service import (  # noqa: E402
    RouterNormalizerService,
    _with_inferred_repo_anchors,
)
from src.services.router_normalizer.text_surfaces import build_text_surfaces  # noqa: E402
from src.services.router_normalizer.trigger_rules import evaluate_triggers  # noqa: E402
from src.services.router_normalizer.url_extraction import extract_urls  # noqa: E402


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


class _NoNetworkShortUrlResolver:
    async def resolve(self, url: Any) -> ResolvedUrl:
        return ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=_strip_url_fragment(url.observed_url),
            resolved_url=None,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status="network_disabled",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bounded one-shot router-normalizer consume smoke for q.source.normalize. "
            "Default mode is read-only and inspects one thin Redis Stream message "
            "without creating a consumer group, writing PostgreSQL, or acking Redis."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--approved-router-normalizer-consume-smoke", action="store_true")
    parser.add_argument("--approved-redis-consumer-group", action="store_true")
    parser.add_argument("--approved-normalization-write", action="store_true")
    parser.add_argument("--approved-artifact-candidate-write", action="store_true")
    parser.add_argument("--approved-event-outbox-write", action="store_true")
    parser.add_argument("--approved-redis-ack", action="store_true")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "redis_stream_exists": False,
        "selected_stream_messages_bucket": "zero",
        "thin_payload_shape_valid_bucket": "zero",
        "event_outbox_rehydrate_succeeded_bucket": "zero",
        "source_message_rehydrate_succeeded_bucket": "zero",
        "source_version_rehydrate_succeeded_bucket": "zero",
        "normalization_plan_signal_detected_bucket": "zero",
        "normalization_plan_candidate_eligible_bucket": "zero",
        "normalization_write_attempted": False,
        "normalization_runs_written_bucket": "zero",
        "suppression_traces_written_bucket": "zero",
        "artifacts_written_bucket": "zero",
        "artifact_observations_written_bucket": "zero",
        "candidate_groups_written_bucket": "zero",
        "candidate_members_written_bucket": "zero",
        "enrich_outbox_events_written_bucket": "zero",
        "redis_consumer_group_mutation_attempted": False,
        "redis_ack_attempted": False,
        "redis_ack_succeeded_bucket": "zero",
        "redis_ack_failure_class": None,
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "registry_mutation_performed": False,
        "downstream_service_started": False,
        "external_network_attempted": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "raw_values_emitted": False,
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


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


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


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "first"):
            return mappings.first()
        if hasattr(mappings, "all"):
            rows = list(mappings.all())
            return rows[0] if rows else None
    if hasattr(result, "fetchall"):
        rows = list(result.fetchall())
        return rows[0] if rows else None
    if isinstance(result, list):
        return result[0] if result else None
    return None


async def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    row = _first_mapping(result)
    if not row:
        return None
    if hasattr(row, "_mapping"):
        return next(iter(row._mapping.values()))
    if isinstance(row, Mapping):
        return next(iter(row.values()))
    if isinstance(row, (tuple, list)):
        return row[0] if row else None
    return row


async def _check_required_tables(session: AsyncSessionLike, tables: Sequence[str]) -> bool:
    for table in tables:
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


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _coerce_optional_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return None


def _decode_stream_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        decoded_value = value.decode("utf-8") if isinstance(value, bytes) else value
        decoded[decoded_key] = decoded_value
    return decoded


def _decode_stream_entries(raw_entries: Any) -> list[SelectedStreamMessage]:
    messages: list[SelectedStreamMessage] = []
    for message_id, fields in raw_entries or []:
        messages.append(
            SelectedStreamMessage(
                stream_id=message_id.decode("utf-8") if isinstance(message_id, bytes) else str(message_id),
                fields=_decode_stream_fields(fields),
            )
        )
    return messages


async def _inspect_stream(redis_client: RedisClientLike) -> SelectedStreamMessage | None:
    stream_length = int(await redis_client.xlen(EXPECTED_STREAM_NAME) or 0)
    if stream_length <= 0:
        return None
    entries = _decode_stream_entries(
        await redis_client.xrange(EXPECTED_STREAM_NAME, min="-", max="+", count=1)
    )
    return entries[0] if entries else None


def _validate_thin_message(fields: Mapping[str, Any]) -> ThinMessageContract:
    checks_failed: list[str] = []
    keys = set(fields)
    missing = REQUIRED_THIN_FIELDS - keys
    extra = keys - REQUIRED_THIN_FIELDS
    if missing or extra:
        checks_failed.append("redis.thin_payload_shape")
    if any(_is_forbidden_field_name(key) for key in keys):
        checks_failed.append("redis.thin_payload_forbidden_field")

    trigger_event_id: UUID | None = None
    root_object_id: UUID | None = None
    try:
        trigger_event_id = _coerce_uuid(fields.get("trigger_event_id"))
    except (TypeError, ValueError):
        checks_failed.append("redis.trigger_event_id_invalid")
    try:
        root_object_id = _coerce_uuid(fields.get("root_object_id"))
    except (TypeError, ValueError):
        checks_failed.append("redis.root_object_id_invalid")

    if str(fields.get("stage_name", "")) != EXPECTED_STAGE_NAME:
        checks_failed.append("redis.stage_name_mismatch")
    if str(fields.get("root_object_type", "")) != EXPECTED_ROOT_OBJECT_TYPE:
        checks_failed.append("redis.root_object_type_mismatch")

    return ThinMessageContract(
        valid=not checks_failed,
        trigger_event_id=trigger_event_id,
        root_object_id=root_object_id,
        checks_failed=checks_failed,
    )


def _is_forbidden_field_name(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_FIELD_TOKENS)


async def _load_event_outbox(session: AsyncSessionLike, event_id: UUID) -> OutboxEventRow | None:
    row = _first_mapping(await _execute(session, SELECT_EVENT_OUTBOX_QUERY, {"event_id": str(event_id)}))
    if row is None:
        return None
    payload = _json_loads(row["payload_json"]) or {}
    return OutboxEventRow(
        event_id=_coerce_uuid(row["event_id"]),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=_coerce_uuid(row["aggregate_id"]),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        created_at=row["created_at"],
    )


async def _load_source_message(
    session: AsyncSessionLike,
    source_message_id: UUID,
) -> SourceMessageSnapshot | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_SOURCE_MESSAGE_QUERY,
            {"source_message_id": str(source_message_id)},
        )
    )
    if row is None:
        return None
    return SourceMessageSnapshot(
        source_message_id=_coerce_uuid(row["source_message_id"]),
        source_version_no=int(row["current_version_no"]),
        text_body=row["text_body"],
        caption_text=row["caption_text"],
        text_surface=row["text_surface"],
        entities_json=_json_loads(row["entities_json"]),
        url_surface_json=_json_loads(row["url_surface_json"]),
        raw_message_json=_json_loads(row["raw_message_json"]) or {},
        deleted_at=_coerce_optional_datetime(row["deleted_at"]),
    )


async def _load_source_message_version(
    session: AsyncSessionLike,
    *,
    source_message_id: UUID,
    version_no: int,
) -> SourceMessageSnapshot | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_SOURCE_VERSION_QUERY,
            {"source_message_id": str(source_message_id), "version_no": version_no},
        )
    )
    if row is None:
        return None
    return SourceMessageSnapshot(
        source_message_id=_coerce_uuid(row["source_message_id"]),
        source_version_no=int(row["version_no"]),
        text_body=None,
        caption_text=None,
        text_surface=row["text_surface"],
        entities_json=_json_loads(row["entities_json"]),
        url_surface_json=None,
        raw_message_json=_json_loads(row["raw_message_json"]) or {},
    )


def _source_message_id_from_event(outbox_event: OutboxEventRow) -> UUID:
    payload_source_id = outbox_event.payload_json.get("source_message_id")
    if isinstance(payload_source_id, str) and payload_source_id.strip():
        return UUID(payload_source_id)
    return outbox_event.aggregate_id


def _requested_version_from_event(outbox_event: OutboxEventRow) -> int | None:
    raw = outbox_event.payload_json.get("current_version_no")
    if raw is None:
        raw = outbox_event.payload_json.get("source_version_no")
    if raw is None:
        return None
    return int(raw)


async def _rehydrate_inputs(
    *,
    session: AsyncSessionLike,
    trigger_event_id: UUID,
    root_object_id: UUID,
    report: dict[str, Any],
    raw_values: set[str],
) -> RehydratedInputs | None:
    outbox_event = await _load_event_outbox(session, trigger_event_id)
    if outbox_event is None:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "event_outbox.rehydrate_missing",
        )
        return None
    report["event_outbox_rehydrate_succeeded_bucket"] = "one"
    _collect_raw_values_from_outbox_event(outbox_event, raw_values)

    if outbox_event.event_type not in SOURCE_MESSAGE_EVENT_TYPES:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "event_outbox.event_type_mismatch",
        )
        return None
    if outbox_event.aggregate_type != EXPECTED_ROOT_OBJECT_TYPE:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "event_outbox.aggregate_type_mismatch",
        )
        return None

    source_message_id = _source_message_id_from_event(outbox_event)
    if source_message_id != root_object_id:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "event_outbox.root_object_mismatch",
        )
        return None
    source_snapshot = await _load_source_message(session, source_message_id)
    if source_snapshot is None:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "source_message.rehydrate_missing",
        )
        return None
    report["source_message_rehydrate_succeeded_bucket"] = "one"
    _collect_raw_values_from_source_snapshot(source_snapshot, raw_values)

    requested_version = _requested_version_from_event(outbox_event)
    version_snapshot = None
    if requested_version is None:
        report["source_version_rehydrate_succeeded_bucket"] = "not_requested"
    else:
        version_snapshot = await _load_source_message_version(
            session,
            source_message_id=source_message_id,
            version_no=requested_version,
        )
        if version_snapshot is None:
            _set_status(
                report,
                "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                "source_version.rehydrate_missing",
            )
            return None
        report["source_version_rehydrate_succeeded_bucket"] = "one"
        _collect_raw_values_from_source_snapshot(version_snapshot, raw_values)

    return RehydratedInputs(
        outbox_event=outbox_event,
        source_snapshot=source_snapshot,
        version_snapshot=version_snapshot,
        requested_version=requested_version,
    )


def _snapshot_for_planning(inputs: RehydratedInputs) -> SourceMessageSnapshot:
    if inputs.source_snapshot.deleted_at is not None:
        return inputs.source_snapshot
    if inputs.version_snapshot is not None:
        return inputs.version_snapshot
    return inputs.source_snapshot


async def _build_normalization_plan(snapshot: SourceMessageSnapshot) -> NormalizationPlan:
    if snapshot.deleted_at is not None:
        return NormalizationPlan(
            signal_detected=False,
            candidate_eligible=False,
            artifact_count=0,
            candidate_group_count=0,
            suppression_count=1,
        )
    surfaces = build_text_surfaces(snapshot)
    extracted_urls = extract_urls(snapshot, surfaces)
    resolver = _NoNetworkShortUrlResolver()
    resolved_urls = [await resolver.resolve(url) for url in extracted_urls]
    artifacts = _with_inferred_repo_anchors(canonicalize_resolved_urls(resolved_urls))
    evaluation = evaluate_triggers(surfaces, artifacts)
    if evaluation.candidate_eligible and not artifacts:
        artifacts = [build_text_idea_artifact(surfaces)]
    return NormalizationPlan(
        signal_detected=evaluation.signal_detected,
        candidate_eligible=evaluation.candidate_eligible,
        artifact_count=len(artifacts),
        candidate_group_count=_planned_candidate_group_count(artifacts) if evaluation.candidate_eligible else 0,
        suppression_count=0 if evaluation.candidate_eligible else len(evaluation.reason_codes),
    )


def _planned_candidate_group_count(artifacts: Sequence[Any]) -> int:
    primary_ids: set[str] = set()
    for artifact in artifacts:
        if (
            artifact.artifact_type in {"github_subpath", "github_repo_page"}
            and artifact.inferred_repo is not None
        ):
            primary_ids.add(artifact.inferred_repo.canonical_id)
        else:
            primary_ids.add(artifact.canonical_id)
    return len(primary_ids)


def _apply_plan_to_report(report: dict[str, Any], plan: NormalizationPlan) -> None:
    report["normalization_plan_signal_detected_bucket"] = "one" if plan.signal_detected else "zero"
    report["normalization_plan_candidate_eligible_bucket"] = "one" if plan.candidate_eligible else "zero"


def _build_config(
    *,
    database_url: str,
    redis_url: str,
    consumer_group: str | None = None,
    consumer_name: str | None = None,
) -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="bounded-smoke",
        database_url=database_url,
        redis_url=redis_url,
        queue_name=EXPECTED_STREAM_NAME,
        consumer_group=consumer_group or DEFAULT_CONSUMER_GROUP,
        consumer_name=consumer_name or DEFAULT_CONSUMER_NAME,
        block_ms=DEFAULT_BLOCK_MS,
        batch_size=1,
        normalizer_version=DEFAULT_NORMALIZER_VERSION,
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="CRITICAL",
    )


def _quiet_logger() -> logging.Logger:
    logger = logging.getLogger("router-normalizer-source-message-bounded-consume-smoke")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


async def _default_normalizer_runner(config: RouterNormalizerConfig, message: Any, session: AsyncSessionLike) -> Any:
    service = RouterNormalizerService(
        config,
        repository=RouterNormalizerRepository(session),
        short_url_resolver=_NoNetworkShortUrlResolver(),
        logger=_quiet_logger(),
    )
    return await service.process_stream_message(message)


async def _consume_and_write(
    *,
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    selected_message: SelectedStreamMessage,
    database_url: str,
    redis_url: str,
    normalizer_runner: NormalizerRunner | None,
    report: dict[str, Any],
    raw_values: set[str],
) -> bool:
    if not await _check_required_tables(session, REQUIRED_WRITE_TABLES):
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "database.required_write_tables",
        )
        return False

    smoke_consumer_suffix = uuid4().hex
    config = _build_config(
        database_url=database_url,
        redis_url=redis_url,
        consumer_group=f"{DEFAULT_CONSUMER_GROUP}-{smoke_consumer_suffix}",
        consumer_name=f"{DEFAULT_CONSUMER_NAME}-{smoke_consumer_suffix}",
    )
    consumer = RedisStreamsConsumer(
        redis_client,
        queue_name=config.queue_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        batch_size=1,
    )
    report["redis_consumer_group_mutation_attempted"] = True
    await consumer.ensure_group()
    messages = await consumer.read_batch()
    if len(messages) != 1:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "redis.consumer_read_count",
        )
        return False
    message_id, message = messages[0]
    raw_values.add(str(message_id))
    if str(message_id) != selected_message.stream_id:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "redis.consumer_selected_message_mismatch",
        )
        return False

    runner = normalizer_runner or _default_normalizer_runner
    try:
        report["normalization_write_attempted"] = True
        result = await _maybe_await(runner(config, message, session))
        await session.commit()
    except Exception:
        await session.rollback()
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_write_failed",
            "database.normalizer_write",
        )
        return False

    _apply_write_result_to_report(report, result)
    report["redis_ack_attempted"] = True
    try:
        await consumer.ack(message_id)
    except Exception as exc:
        report["redis_ack_failure_class"] = type(exc).__name__
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_ack_failed_after_commit",
            "redis.ack",
        )
        return False
    report["redis_ack_succeeded_bucket"] = "one"
    _set_status(report, "router_normalizer_source_message_bounded_consume_smoke_consumed")
    return True


def _apply_write_result_to_report(report: dict[str, Any], result: Any) -> None:
    report["normalization_runs_written_bucket"] = "one"
    artifact_count = max(int(getattr(result, "artifact_count", 0) or 0), 0)
    candidate_group_count = max(int(getattr(result, "candidate_group_count", 0) or 0), 0)
    suppression_count = len(getattr(result, "suppression_reason_codes", []) or [])
    report["artifacts_written_bucket"] = _bucket_count(artifact_count)
    report["artifact_observations_written_bucket"] = _bucket_count(artifact_count)
    report["candidate_groups_written_bucket"] = _bucket_count(candidate_group_count)
    report["candidate_members_written_bucket"] = _bucket_count(candidate_group_count)
    report["enrich_outbox_events_written_bucket"] = _bucket_count(artifact_count if candidate_group_count else 0)
    report["suppression_traces_written_bucket"] = _bucket_count(suppression_count)


def _approval_block_status(report: dict[str, Any], approvals: ConsumeApprovals) -> ScriptResult:
    if not approvals.any_granted:
        _set_status(report, "router_normalizer_source_message_bounded_consume_smoke_ready")
        return ScriptResult(exit_code=0, report=report)
    _set_status(report, "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready")
    for check in approvals.missing_checks():
        _set_status(report, report["contract_status"], check)
    return ScriptResult(exit_code=1, report=report)


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
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "database.url_missing",
        )
        return None
    if not _database_url_is_supported(database_url):
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "database.url_unsupported",
        )
        return None
    if not redis_url:
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "redis.url_missing",
        )
        return None
    if not _redis_url_is_supported(redis_url):
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "redis.url_unsupported",
        )
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
            report[field] = True


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    return any(bool(report[field]) for field in SIDE_EFFECT_REPORT_FIELDS)


def _collect_raw_values_from_outbox_event(event: OutboxEventRow, raw_values: set[str]) -> None:
    raw_values.update(
        {
            str(event.event_id),
            str(event.aggregate_id),
            event.dedupe_key,
            json.dumps(event.payload_json, sort_keys=True, default=str),
        }
    )
    for key in ("source_message_id", "aggregate_id", "event_id", "message_text", "text_body", "url"):
        value = event.payload_json.get(key)
        if isinstance(value, str):
            raw_values.add(value)


def _collect_raw_values_from_source_snapshot(snapshot: SourceMessageSnapshot, raw_values: set[str]) -> None:
    raw_values.add(str(snapshot.source_message_id))
    for value in (snapshot.text_body, snapshot.caption_text, snapshot.text_surface):
        if isinstance(value, str):
            raw_values.add(value)
    raw_values.add(json.dumps(snapshot.raw_message_json, sort_keys=True, default=str))
    raw_values.add(json.dumps(snapshot.entities_json, sort_keys=True, default=str))
    raw_values.add(json.dumps(snapshot.url_surface_json, sort_keys=True, default=str))


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values if len(value) >= 6)


def _strip_url_fragment(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    return parsed._replace(fragment="").geturl()


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    normalizer_runner: NormalizerRunner | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    approvals = approvals or ConsumeApprovals(False, False, False, False, False, False)
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, "blocked_forbidden_side_effect_detected", "side_effect.forbidden")
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
            _set_status(
                report,
                "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                "runtime_env.read",
            )
            return ScriptResult(exit_code=1, report=report)

        runtime_config = _extract_runtime_config(report=report, values=values, raw_values=raw_values)
        if runtime_config is None:
            return ScriptResult(exit_code=1, report=report)
        database_url, redis_url = runtime_config

        try:
            session = await _open_database_session(database_url, database_session_factory)
            if not approvals.all_granted:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only_value = await _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
                report["read_only_transaction"] = _transaction_read_only_enabled(read_only_value)
                if not report["read_only_transaction"]:
                    _set_status(
                        report,
                        "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                        "database.read_only_transaction",
                    )
                    return ScriptResult(exit_code=1, report=report)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session, REQUIRED_READ_TABLES):
                _set_status(
                    report,
                    "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                    "database.required_read_tables",
                )
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                "database.connection_or_schema",
            )
            return ScriptResult(exit_code=1, report=report)

        try:
            redis_client = await _open_redis_client(redis_url, redis_client_factory)
            await _maybe_await(redis_client.ping())
            report["redis_connected"] = True
            selected_message = await _inspect_stream(redis_client)
        except Exception:
            _set_status(
                report,
                "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                "redis.connection_or_stream_read",
            )
            return ScriptResult(exit_code=1, report=report)

        if selected_message is None:
            _set_status(report, "router_normalizer_source_message_bounded_consume_smoke_no_stream_message")
            return ScriptResult(exit_code=0, report=report)
        report["redis_stream_exists"] = True
        report["selected_stream_messages_bucket"] = "one"
        raw_values.add(selected_message.stream_id)

        thin_contract = _validate_thin_message(selected_message.fields)
        if not thin_contract.valid:
            for check in thin_contract.checks_failed:
                _set_status(
                    report,
                    "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                    check,
                )
            return ScriptResult(exit_code=1, report=report)
        report["thin_payload_shape_valid_bucket"] = "one"
        if thin_contract.trigger_event_id is None or thin_contract.root_object_id is None:
            _set_status(
                report,
                "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
                "redis.thin_payload_uuid",
            )
            return ScriptResult(exit_code=1, report=report)
        raw_values.update({str(thin_contract.trigger_event_id), str(thin_contract.root_object_id)})

        inputs = await _rehydrate_inputs(
            session=session,
            trigger_event_id=thin_contract.trigger_event_id,
            root_object_id=thin_contract.root_object_id,
            report=report,
            raw_values=raw_values,
        )
        if inputs is None:
            return ScriptResult(exit_code=1, report=report)

        plan = await _build_normalization_plan(_snapshot_for_planning(inputs))
        _apply_plan_to_report(report, plan)

        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, "blocked_forbidden_side_effect_detected", "output.raw_values")
            return ScriptResult(exit_code=1, report=report)

        if not approvals.all_granted:
            return _approval_block_status(report, approvals)

        ok = await _consume_and_write(
            session=session,
            redis_client=redis_client,
            selected_message=selected_message,
            database_url=database_url,
            redis_url=redis_url,
            normalizer_runner=normalizer_runner,
            report=report,
            raw_values=raw_values,
        )
        if _forbidden_side_effect_detected(report):
            _set_status(report, "blocked_forbidden_side_effect_detected", "side_effect.forbidden")
            return ScriptResult(exit_code=1, report=report)
        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, "blocked_forbidden_side_effect_detected", "output.raw_values")
            return ScriptResult(exit_code=1, report=report)
        return ScriptResult(exit_code=0 if ok else 1, report=report)
    except Exception:
        if session is not None:
            await session.rollback()
        _set_status(
            report,
            "blocked_router_normalizer_source_message_bounded_consume_smoke_not_ready",
            "unexpected",
        )
        return ScriptResult(exit_code=1, report=report)
    finally:
        if session is not None and not approvals.all_granted:
            await session.rollback()
        await _close_redis_client(redis_client)
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    normalizer_runner: NormalizerRunner | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            normalizer_runner=normalizer_runner,
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
        approvals=ConsumeApprovals(
            router_normalizer_consume_smoke=args.approved_router_normalizer_consume_smoke,
            redis_consumer_group=args.approved_redis_consumer_group,
            normalization_write=args.approved_normalization_write,
            artifact_candidate_write=args.approved_artifact_candidate_write,
            event_outbox_write=args.approved_event_outbox_write,
            redis_ack=args.approved_redis_ack,
        ),
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
