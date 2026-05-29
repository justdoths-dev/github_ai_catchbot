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
from urllib.parse import urlparse
from uuid import UUID, uuid4


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_router_normalizer_candidate_source_event_targeted_consume_smoke"
REPORT_TYPE = "router_normalizer_candidate_source_event_targeted_consume_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_SOURCE_ROWS = 50
MAX_SOURCE_ROWS_HARD_LIMIT = 200
DEFAULT_MAX_STREAM_ENTRIES = 100
MAX_STREAM_ENTRIES_HARD_LIMIT = 1000
EXPECTED_STREAM_NAME = "q.source.normalize"
EXPECTED_STAGE_NAME = "normalize"
EXPECTED_ROOT_OBJECT_TYPE = "source_message"
DEFAULT_CONSUMER_GROUP = "router-normalizer-candidate-source-event-targeted-consume-smoke"
DEFAULT_CONSUMER_NAME = "router-normalizer-candidate-source-event-targeted-consume-smoke-1"
DEFAULT_BLOCK_MS = 100
DEFAULT_NORMALIZER_VERSION = "router-normalizer-v1"
MAX_REDIS_STREAM_SEQUENCE = 18_446_744_073_709_551_615
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

STATUS_READY = "router_normalizer_candidate_source_event_targeted_consume_smoke_ready"
STATUS_CONSUMED = "router_normalizer_candidate_source_event_targeted_consume_smoke_consumed"
STATUS_ALREADY_CONSUMED = (
    "router_normalizer_candidate_source_event_targeted_consume_smoke_already_consumed"
)
STATUS_BLOCKED_NOT_READY = (
    "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_not_ready"
)
STATUS_NO_CANDIDATE = (
    "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_no_candidate_source"
)
STATUS_NO_EVENT = (
    "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_no_published_source_event"
)
STATUS_NO_TARGET = (
    "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_no_target_stream_entry"
)
STATUS_DELIVERY_MISMATCH = (
    "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_target_delivery_mismatch"
)
STATUS_NOT_CANDIDATE = (
    "blocked_router_normalizer_candidate_source_event_targeted_consume_smoke_not_candidate"
)
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_PUBLISHED_SOURCE_EVENT_QUERY = """
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
WHERE event_type = ANY(CAST(:event_types AS text[]))
  AND status::text = 'published'
  AND aggregate_type::text = 'source_message'
  AND aggregate_id = CAST(:source_message_id AS uuid)
ORDER BY published_at DESC NULLS LAST, created_at DESC NULLS LAST, event_id DESC
LIMIT 1
"""
COUNT_EXISTING_NORMALIZATION_RUNS_QUERY = """
SELECT COUNT(*)
FROM normalization_runs
WHERE source_message_id = CAST(:source_message_id AS uuid)
  AND source_version_no = :source_version_no
  AND normalizer_version = :normalizer_version
"""
COUNT_EXISTING_CANDIDATE_GROUPS_QUERY = """
SELECT COUNT(*)
FROM candidate_group_proposals
WHERE source_message_id = CAST(:source_message_id AS uuid)
  AND source_version_no = :source_version_no
"""
COUNT_EXISTING_ENRICH_OUTBOX_QUERY = """
SELECT COUNT(*)
FROM event_outbox eo
WHERE eo.event_type = 'artifact.enrich.requested.v1'
  AND (
    (
      eo.aggregate_type::text = 'candidate_group'
      AND eo.aggregate_id IN (
        SELECT cgp.candidate_group_id
        FROM candidate_group_proposals cgp
        WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
          AND cgp.source_version_no = :source_version_no
      )
    )
    OR eo.payload_json->>'candidate_group_id' IN (
      SELECT cgp.candidate_group_id::text
      FROM candidate_group_proposals cgp
      WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
        AND cgp.source_version_no = :source_version_no
    )
    OR (
      eo.aggregate_type::text = 'artifact'
      AND eo.aggregate_id IN (
        SELECT cgm.artifact_id
        FROM candidate_group_members cgm
        JOIN candidate_group_proposals cgp
          ON cgp.candidate_group_id = cgm.candidate_group_id
        WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
          AND cgp.source_version_no = :source_version_no
      )
    )
    OR eo.payload_json->>'artifact_id' IN (
      SELECT cgm.artifact_id::text
      FROM candidate_group_members cgm
      JOIN candidate_group_proposals cgp
        ON cgp.candidate_group_id = cgm.candidate_group_id
      WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
        AND cgp.source_version_no = :source_version_no
    )
  )
"""
COUNT_ARTIFACTS_FOR_SOURCE_VERSION_QUERY = """
SELECT COUNT(DISTINCT artifact_id)
FROM artifact_observations
WHERE source_message_id = CAST(:source_message_id AS uuid)
  AND source_version_no = :source_version_no
"""
COUNT_ARTIFACT_OBSERVATIONS_FOR_SOURCE_VERSION_QUERY = """
SELECT COUNT(*)
FROM artifact_observations
WHERE source_message_id = CAST(:source_message_id AS uuid)
  AND source_version_no = :source_version_no
"""
COUNT_CANDIDATE_MEMBERS_FOR_SOURCE_VERSION_QUERY = """
SELECT COUNT(*)
FROM candidate_group_members cgm
JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = cgm.candidate_group_id
WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
  AND cgp.source_version_no = :source_version_no
"""
COUNT_SUPPRESSION_TRACES_FOR_SOURCE_VERSION_QUERY = """
SELECT COUNT(*)
FROM normalization_suppression_traces nst
JOIN normalization_runs nr
  ON nr.normalization_run_id = nst.normalization_run_id
WHERE nr.source_message_id = CAST(:source_message_id AS uuid)
  AND nr.source_version_no = :source_version_no
  AND nr.normalizer_version = :normalizer_version
"""
REQUIRED_READ_TABLES = (
    "source_messages",
    "source_message_versions",
    "event_outbox",
    "normalization_runs",
    "candidate_group_proposals",
)
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

    async def xrevrange(
        self,
        name: str,
        max: str = "+",
        min: str = "-",
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
ShortUrlResolverFactory = Callable[[], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ConsumeApprovals:
    targeted_router_normalizer_consume_smoke: bool
    redis_targeted_consumer_group: bool
    normalization_write: bool
    artifact_candidate_write: bool
    event_outbox_write: bool
    targeted_redis_ack: bool

    @property
    def all_granted(self) -> bool:
        return (
            self.targeted_router_normalizer_consume_smoke
            and self.redis_targeted_consumer_group
            and self.normalization_write
            and self.artifact_candidate_write
            and self.event_outbox_write
            and self.targeted_redis_ack
        )

    @property
    def any_granted(self) -> bool:
        return (
            self.targeted_router_normalizer_consume_smoke
            or self.redis_targeted_consumer_group
            or self.normalization_write
            or self.artifact_candidate_write
            or self.event_outbox_write
            or self.targeted_redis_ack
        )

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.targeted_router_normalizer_consume_smoke:
            checks.append("approval.targeted_router_normalizer_consume_smoke")
        if not self.redis_targeted_consumer_group:
            checks.append("approval.redis_targeted_consumer_group")
        if not self.normalization_write:
            checks.append("approval.normalization_write")
        if not self.artifact_candidate_write:
            checks.append("approval.artifact_candidate_write")
        if not self.event_outbox_write:
            checks.append("approval.event_outbox_write")
        if not self.targeted_redis_ack:
            checks.append("approval.targeted_redis_ack")
        return checks


@dataclass(frozen=True, slots=True)
class CandidateSelection:
    source_row: Any
    plan: Any


@dataclass(frozen=True, slots=True)
class PublishedSourceEvent:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str


@dataclass(frozen=True, slots=True)
class StreamEntry:
    stream_id: str
    fields: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ThinTargetContract:
    shape_valid: bool
    stage_valid: bool
    root_valid: bool
    trigger_event_id: UUID | None
    root_object_id: UUID | None
    checks_failed: list[str]

    @property
    def valid(self) -> bool:
        return self.shape_valid and self.stage_valid and self.root_valid


@dataclass(frozen=True, slots=True)
class ExistingConsumedCounts:
    normalization_runs: int
    candidate_groups: int
    enrich_outbox_events: int

    @property
    def safely_consumed(self) -> bool:
        return (
            self.normalization_runs > 0
            and self.candidate_groups > 0
            and self.enrich_outbox_events > 0
        )


@dataclass(frozen=True, slots=True)
class WriteProofCounts:
    normalization_runs: int
    suppression_traces: int
    artifacts: int
    artifact_observations: int
    candidate_groups: int
    candidate_members: int
    enrich_outbox_events: int

    @property
    def safely_consumed(self) -> bool:
        return (
            self.normalization_runs > 0
            and self.candidate_groups > 0
            and self.enrich_outbox_events > 0
        )


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import dedicated_vps_router_normalizer_candidate_path_readiness_probe as candidate_probe  # noqa: E402
from scripts.ops import dedicated_vps_router_normalizer_source_message_bounded_consume_smoke as consume_smoke  # noqa: E402
from src.services.router_normalizer.config import RouterNormalizerConfig  # noqa: E402
from src.services.router_normalizer.models import RedisNormalizeMessage  # noqa: E402
from src.services.router_normalizer.repositories import RouterNormalizerRepository  # noqa: E402
from src.services.router_normalizer.service import RouterNormalizerService  # noqa: E402


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
            "Targeted dedicated-VPS router-normalizer consume smoke. It correlates "
            "a candidate-producing source row to a published source event and then "
            "to the exact q.source.normalize Redis stream entry by trigger_event_id."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--max-source-rows",
        type=_bounded_positive_int_named(
            "max-source-rows",
            upper_bound=MAX_SOURCE_ROWS_HARD_LIMIT,
        ),
        default=DEFAULT_MAX_SOURCE_ROWS,
    )
    parser.add_argument(
        "--max-stream-entries",
        type=_bounded_positive_int_named(
            "max-stream-entries",
            upper_bound=MAX_STREAM_ENTRIES_HARD_LIMIT,
        ),
        default=DEFAULT_MAX_STREAM_ENTRIES,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--approved-targeted-router-normalizer-consume-smoke", action="store_true")
    parser.add_argument("--approved-redis-targeted-consumer-group", action="store_true")
    parser.add_argument("--approved-normalization-write", action="store_true")
    parser.add_argument("--approved-artifact-candidate-write", action="store_true")
    parser.add_argument("--approved-event-outbox-write", action="store_true")
    parser.add_argument("--approved-targeted-redis-ack", action="store_true")
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
        "contract_status": STATUS_BLOCKED_NOT_READY,
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "candidate_source_rows_scanned_bucket": "zero",
        "candidate_source_selected_bucket": "zero",
        "candidate_plan_signal_detected_bucket": "zero",
        "candidate_plan_candidate_eligible_bucket": "zero",
        "candidate_plan_artifacts_bucket": "zero",
        "candidate_plan_candidate_groups_bucket": "zero",
        "candidate_plan_github_route_bucket": "zero",
        "candidate_plan_x_route_bucket": "zero",
        "candidate_plan_web_route_bucket": "zero",
        "candidate_plan_text_idea_bucket": "zero",
        "published_source_event_selected_bucket": "zero",
        "target_stream_entry_found_bucket": "zero",
        "target_stream_shape_valid_bucket": "zero",
        "target_stream_stage_valid_bucket": "zero",
        "target_stream_root_valid_bucket": "zero",
        "redis_group_mutation_attempted": False,
        "targeted_stream_delivery_attempted": False,
        "targeted_stream_delivery_succeeded_bucket": "zero",
        "delivered_target_match_bucket": "zero",
        "event_outbox_rehydrate_succeeded_bucket": "zero",
        "source_message_rehydrate_succeeded_bucket": "zero",
        "source_version_rehydrate_succeeded_bucket": "zero",
        "normalization_write_attempted": False,
        "normalization_runs_written_bucket": "zero",
        "suppression_traces_written_bucket": "zero",
        "artifacts_written_bucket": "zero",
        "artifact_observations_written_bucket": "zero",
        "candidate_groups_written_bucket": "zero",
        "candidate_members_written_bucket": "zero",
        "enrich_outbox_events_written_bucket": "zero",
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
    return consume_smoke.parse_runtime_env_text(text)


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def _read_runtime_env(
    path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
) -> Mapping[str, str]:
    if runtime_env_reader is not None:
        return runtime_env_reader(path)
    return parse_runtime_env_file(path)


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
    return consume_smoke._first_mapping(result)


async def _scalar(result: Any) -> Any:
    return await consume_smoke._scalar(result)


def _json_loads(value: Any) -> Any:
    return consume_smoke._json_loads(value)


def _coerce_uuid(value: Any) -> UUID:
    return consume_smoke._coerce_uuid(value)


def _database_url_is_supported(database_url: str) -> bool:
    return consume_smoke._database_url_is_supported(database_url)


def _redis_url_is_supported(redis_url: str) -> bool:
    return consume_smoke._redis_url_is_supported(redis_url)


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return consume_smoke._transaction_read_only_enabled(raw_value)


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


async def _select_candidate_source(
    *,
    session: AsyncSessionLike,
    max_source_rows: int,
    resolver_factory: ShortUrlResolverFactory | None,
    report: dict[str, Any],
    raw_values: set[str],
) -> CandidateSelection | None:
    rows = await candidate_probe._load_source_rows(
        session=session,
        max_source_rows=max_source_rows,
    )
    report["candidate_source_rows_scanned_bucket"] = _bucket_count(len(rows))
    for row in rows:
        _collect_raw_values_from_source_snapshot(row.current_snapshot, raw_values)
        if row.version_snapshot is not None:
            _collect_raw_values_from_source_snapshot(row.version_snapshot, raw_values)
        snapshot = candidate_probe._snapshot_for_planning(row)
        plan = await candidate_probe._build_planning_result(snapshot, resolver_factory)
        if (
            plan.candidate_eligible
            and plan.signal_detected
            and (plan.artifact_count > 0 or plan.candidate_group_count > 0)
        ):
            report["candidate_source_selected_bucket"] = "one"
            _apply_candidate_plan_to_report(report, plan)
            return CandidateSelection(source_row=row, plan=plan)
    _set_status(report, STATUS_NO_CANDIDATE, "candidate_source.none")
    return None


def _apply_candidate_plan_to_report(report: dict[str, Any], plan: Any) -> None:
    report["candidate_plan_signal_detected_bucket"] = "one" if plan.signal_detected else "zero"
    report["candidate_plan_candidate_eligible_bucket"] = (
        "one" if plan.candidate_eligible else "zero"
    )
    report["candidate_plan_artifacts_bucket"] = _bucket_count(int(plan.artifact_count or 0))
    report["candidate_plan_candidate_groups_bucket"] = _bucket_count(
        int(plan.candidate_group_count or 0)
    )
    report["candidate_plan_github_route_bucket"] = "one" if plan.has_github_route else "zero"
    report["candidate_plan_x_route_bucket"] = "one" if plan.has_x_route else "zero"
    report["candidate_plan_web_route_bucket"] = "one" if plan.has_web_route else "zero"
    report["candidate_plan_text_idea_bucket"] = "one" if plan.text_idea_only else "zero"


async def _load_published_source_event(
    *,
    session: AsyncSessionLike,
    source_message_id: UUID,
    report: dict[str, Any],
    raw_values: set[str],
) -> PublishedSourceEvent | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_PUBLISHED_SOURCE_EVENT_QUERY,
            {
                "event_types": list(SOURCE_MESSAGE_EVENT_TYPES),
                "source_message_id": str(source_message_id),
            },
        )
    )
    if row is None:
        _set_status(report, STATUS_NO_EVENT, "event_outbox.published_source_event_missing")
        return None
    payload = _json_loads(row["payload_json"]) or {}
    event = PublishedSourceEvent(
        event_id=_coerce_uuid(row["event_id"]),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=_coerce_uuid(row["aggregate_id"]),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
    )
    report["published_source_event_selected_bucket"] = "one"
    report["event_outbox_rehydrate_succeeded_bucket"] = "one"
    _collect_raw_values_from_published_event(event, raw_values)
    return event


async def _load_existing_consumed_counts(
    *,
    session: AsyncSessionLike,
    source_message_id: UUID,
    source_version_no: int,
) -> ExistingConsumedCounts:
    proof = await _load_write_proof_counts(
        session=session,
        source_message_id=source_message_id,
        source_version_no=source_version_no,
    )
    return ExistingConsumedCounts(
        normalization_runs=proof.normalization_runs,
        candidate_groups=proof.candidate_groups,
        enrich_outbox_events=proof.enrich_outbox_events,
    )


async def _load_write_proof_counts(
    *,
    session: AsyncSessionLike,
    source_message_id: UUID,
    source_version_no: int,
) -> WriteProofCounts:
    params = {
        "source_message_id": str(source_message_id),
        "source_version_no": source_version_no,
        "normalizer_version": DEFAULT_NORMALIZER_VERSION,
    }
    normalization_runs = int(
        await _scalar(
            await _execute(session, COUNT_EXISTING_NORMALIZATION_RUNS_QUERY, params)
        )
        or 0
    )
    candidate_groups = int(
        await _scalar(
            await _execute(session, COUNT_EXISTING_CANDIDATE_GROUPS_QUERY, params)
        )
        or 0
    )
    enrich_outbox = int(
        await _scalar(await _execute(session, COUNT_EXISTING_ENRICH_OUTBOX_QUERY, params))
        or 0
    )
    artifacts = await _count_artifacts_for_source_version(
        session=session,
        source_message_id=source_message_id,
        source_version_no=source_version_no,
    )
    artifact_observations = int(
        await _scalar(
            await _execute(session, COUNT_ARTIFACT_OBSERVATIONS_FOR_SOURCE_VERSION_QUERY, params)
        )
        or 0
    )
    candidate_members = int(
        await _scalar(
            await _execute(session, COUNT_CANDIDATE_MEMBERS_FOR_SOURCE_VERSION_QUERY, params)
        )
        or 0
    )
    suppression_traces = int(
        await _scalar(
            await _execute(session, COUNT_SUPPRESSION_TRACES_FOR_SOURCE_VERSION_QUERY, params)
        )
        or 0
    )
    return WriteProofCounts(
        normalization_runs=normalization_runs,
        suppression_traces=max(suppression_traces, 0),
        artifacts=artifacts,
        artifact_observations=max(artifact_observations, 0),
        candidate_groups=candidate_groups,
        candidate_members=max(candidate_members, 0),
        enrich_outbox_events=max(enrich_outbox, 0),
    )


async def _count_artifacts_for_source_version(
    *,
    session: AsyncSessionLike,
    source_message_id: UUID,
    source_version_no: int,
) -> int:
    return max(
        int(
            await _scalar(
                await _execute(
                    session,
                    COUNT_ARTIFACTS_FOR_SOURCE_VERSION_QUERY,
                    {
                        "source_message_id": str(source_message_id),
                        "source_version_no": source_version_no,
                    },
                )
            )
            or 0
        ),
        0,
    )


async def _scan_target_stream_entry(
    *,
    redis_client: RedisClientLike,
    event_id: UUID,
    max_stream_entries: int,
    report: dict[str, Any],
    raw_values: set[str],
) -> StreamEntry | None:
    stream_length = int(await redis_client.xlen(EXPECTED_STREAM_NAME) or 0)
    if stream_length <= 0:
        return None
    xrevrange = getattr(redis_client, "xrevrange", None)
    if xrevrange is not None:
        raw_entries = await xrevrange(
            EXPECTED_STREAM_NAME,
            max="+",
            min="-",
            count=max_stream_entries,
        )
    else:
        raw_entries = await redis_client.xrange(
            EXPECTED_STREAM_NAME,
            min="-",
            max="+",
            count=max_stream_entries,
        )
    matches: list[StreamEntry] = []
    for entry in _decode_stream_entries(raw_entries):
        trigger = str(entry.fields.get("trigger_event_id", ""))
        if trigger == str(event_id):
            matches.append(entry)
            raw_values.add(entry.stream_id)
            _collect_raw_values_from_stream_fields(entry.fields, raw_values)
    if not matches:
        return None
    if len(matches) > 1:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "redis.target_stream_entry_duplicate")
        return None
    report["target_stream_entry_found_bucket"] = "one"
    return matches[0]


def _decode_stream_entries(raw_entries: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for message_id, fields in raw_entries or []:
        entries.append(
            StreamEntry(
                stream_id=message_id.decode("utf-8") if isinstance(message_id, bytes) else str(message_id),
                fields=_decode_fields(fields),
            )
        )
    return entries


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        decoded_value = value.decode("utf-8") if isinstance(value, bytes) else value
        decoded[decoded_key] = decoded_value
    return decoded


def _validate_target_stream_entry(
    *,
    entry: StreamEntry,
    event_id: UUID,
    source_message_id: UUID,
    report: dict[str, Any],
) -> ThinTargetContract:
    checks_failed: list[str] = []
    keys = set(entry.fields)
    missing = REQUIRED_THIN_FIELDS - keys
    extra = keys - REQUIRED_THIN_FIELDS
    shape_valid = not missing and not extra
    if any(_is_forbidden_field_name(key) for key in keys):
        shape_valid = False
        checks_failed.append("redis.thin_payload_forbidden_field")

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
    if not shape_valid:
        checks_failed.append("redis.thin_payload_shape")

    stage_valid = str(entry.fields.get("stage_name", "")) == EXPECTED_STAGE_NAME
    if not stage_valid:
        checks_failed.append("redis.stage_name_mismatch")

    root_valid = (
        str(entry.fields.get("root_object_type", "")) == EXPECTED_ROOT_OBJECT_TYPE
        and root_object_id == source_message_id
    )
    if str(entry.fields.get("root_object_type", "")) != EXPECTED_ROOT_OBJECT_TYPE:
        checks_failed.append("redis.root_object_type_mismatch")
    if root_object_id is not None and root_object_id != source_message_id:
        checks_failed.append("redis.root_object_id_mismatch")
    if trigger_event_id is not None and trigger_event_id != event_id:
        checks_failed.append("redis.trigger_event_id_mismatch")
        shape_valid = False

    report["target_stream_shape_valid_bucket"] = "one" if shape_valid else "zero"
    report["target_stream_stage_valid_bucket"] = "one" if stage_valid else "zero"
    report["target_stream_root_valid_bucket"] = "one" if root_valid else "zero"

    return ThinTargetContract(
        shape_valid=shape_valid,
        stage_valid=stage_valid,
        root_valid=root_valid,
        trigger_event_id=trigger_event_id,
        root_object_id=root_object_id,
        checks_failed=checks_failed,
    )


def _is_forbidden_field_name(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_FIELD_TOKENS)


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


async def _deliver_target_entry(
    *,
    redis_client: RedisClientLike,
    target_entry: StreamEntry,
    event_id: UUID,
    report: dict[str, Any],
    raw_values: set[str],
) -> tuple[str, RedisNormalizeMessage] | None:
    group_name = f"{DEFAULT_CONSUMER_GROUP}-{uuid4().hex}"
    consumer_name = f"{DEFAULT_CONSUMER_NAME}-{uuid4().hex}"
    previous_id = previous_redis_stream_id(target_entry.stream_id)
    raw_values.update({group_name, consumer_name, previous_id})

    report["redis_group_mutation_attempted"] = True
    try:
        await redis_client.xgroup_create(
            EXPECTED_STREAM_NAME,
            group_name,
            id=previous_id,
            mkstream=False,
        )
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise
        xgroup_setid = getattr(redis_client, "xgroup_setid", None)
        if xgroup_setid is None:
            raise
        await _maybe_await(xgroup_setid(EXPECTED_STREAM_NAME, group_name, previous_id))

    report["targeted_stream_delivery_attempted"] = True
    raw = await redis_client.xreadgroup(
        group_name,
        consumer_name,
        {EXPECTED_STREAM_NAME: ">"},
        count=1,
        block=DEFAULT_BLOCK_MS,
    )
    delivered = _decode_xreadgroup_entries(raw)
    if len(delivered) != 1:
        await _cleanup_group_if_possible(redis_client, group_name)
        _set_status(report, STATUS_DELIVERY_MISMATCH, "redis.targeted_delivery_count")
        return None
    delivered_id, fields = delivered[0]
    raw_values.add(delivered_id)
    _collect_raw_values_from_stream_fields(fields, raw_values)
    delivered_event = str(fields.get("trigger_event_id", ""))
    if delivered_id != target_entry.stream_id or delivered_event != str(event_id):
        await _cleanup_group_if_possible(redis_client, group_name)
        _set_status(report, STATUS_DELIVERY_MISMATCH, "redis.targeted_delivery_mismatch")
        return None
    report["targeted_stream_delivery_succeeded_bucket"] = "one"
    report["delivered_target_match_bucket"] = "one"
    message = RedisNormalizeMessage.from_stream_fields(fields)
    return group_name, message


def _decode_xreadgroup_entries(raw: Any) -> list[tuple[str, dict[str, Any]]]:
    delivered: list[tuple[str, dict[str, Any]]] = []
    for _stream_name, entries in raw or []:
        for message_id, fields in entries:
            decoded_id = message_id.decode("utf-8") if isinstance(message_id, bytes) else str(message_id)
            delivered.append((decoded_id, _decode_fields(fields)))
    return delivered


async def _cleanup_group_if_possible(redis_client: RedisClientLike, group_name: str) -> None:
    destroy = getattr(redis_client, "xgroup_destroy", None)
    if destroy is not None:
        await _maybe_await(destroy(EXPECTED_STREAM_NAME, group_name))


def previous_redis_stream_id(stream_id: str) -> str:
    if not re.fullmatch(r"\d+-\d+", stream_id):
        raise ValueError("invalid redis stream id")
    millisecond_text, sequence_text = stream_id.split("-", 1)
    milliseconds = int(millisecond_text)
    sequence = int(sequence_text)
    if sequence > 0:
        return f"{milliseconds}-{sequence - 1}"
    if milliseconds > 0:
        return f"{milliseconds - 1}-{MAX_REDIS_STREAM_SEQUENCE}"
    return "0-0"


def _build_config(
    *,
    database_url: str,
    redis_url: str,
    consumer_group: str,
) -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="targeted-candidate-smoke",
        database_url=database_url,
        redis_url=redis_url,
        queue_name=EXPECTED_STREAM_NAME,
        consumer_group=consumer_group,
        consumer_name=DEFAULT_CONSUMER_NAME,
        block_ms=DEFAULT_BLOCK_MS,
        batch_size=1,
        normalizer_version=DEFAULT_NORMALIZER_VERSION,
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="CRITICAL",
    )


async def _default_normalizer_runner(
    config: RouterNormalizerConfig,
    message: RedisNormalizeMessage,
    session: AsyncSessionLike,
) -> Any:
    service = RouterNormalizerService(
        config,
        repository=RouterNormalizerRepository(session),
        short_url_resolver=consume_smoke._NoNetworkShortUrlResolver(),
        logger=consume_smoke._quiet_logger(),
    )
    return await service.process_stream_message(message)


async def _consume_and_write(
    *,
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    target_entry: StreamEntry,
    selected: CandidateSelection,
    published_event: PublishedSourceEvent,
    source_message_id: UUID,
    source_version_no: int,
    database_url: str,
    redis_url: str,
    normalizer_runner: NormalizerRunner | None,
    report: dict[str, Any],
    raw_values: set[str],
) -> bool:
    if not await _check_required_tables(session, REQUIRED_WRITE_TABLES):
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.required_write_tables")
        return False
    if not selected.plan.candidate_eligible:
        _set_status(report, STATUS_NOT_CANDIDATE, "candidate_plan.not_candidate")
        return False

    delivered = await _deliver_target_entry(
        redis_client=redis_client,
        target_entry=target_entry,
        event_id=published_event.event_id,
        report=report,
        raw_values=raw_values,
    )
    if delivered is None:
        return False
    group_name, message = delivered
    if message.root_object_id != str(source_message_id) or message.trigger_event_id != str(
        published_event.event_id
    ):
        await _cleanup_group_if_possible(redis_client, group_name)
        _set_status(report, STATUS_DELIVERY_MISMATCH, "redis.delivered_message_mismatch")
        return False

    runner = normalizer_runner or _default_normalizer_runner
    config = _build_config(
        database_url=database_url,
        redis_url=redis_url,
        consumer_group=group_name,
    )
    try:
        report["normalization_write_attempted"] = True
        result = await _maybe_await(runner(config, message, session))
        if not bool(getattr(result, "candidate_eligible", True)):
            await session.rollback()
            _set_status(report, STATUS_NOT_CANDIDATE, "normalizer_result.not_candidate")
            return False
    except Exception:
        await session.rollback()
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.normalizer_write")
        return False

    try:
        await _apply_write_result_to_report(
            report=report,
            result=result,
            session=session,
            source_message_id=source_message_id,
            source_version_no=source_version_no,
        )
        await session.commit()
    except Exception:
        await session.rollback()
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.write_proof_or_commit")
        return False
    report["redis_ack_attempted"] = True
    try:
        await redis_client.xack(EXPECTED_STREAM_NAME, group_name, target_entry.stream_id)
    except Exception as exc:
        report["redis_ack_failure_class"] = type(exc).__name__
        _set_status(report, STATUS_BLOCKED_NOT_READY, "redis.ack")
        return False
    report["redis_ack_succeeded_bucket"] = "one"
    _set_status(report, STATUS_CONSUMED)
    return True


async def _apply_write_result_to_report(
    *,
    report: dict[str, Any],
    result: Any,
    session: AsyncSessionLike,
    source_message_id: UUID,
    source_version_no: int,
) -> None:
    _ = result
    proof = await _load_write_proof_counts(
        session=session,
        source_message_id=source_message_id,
        source_version_no=source_version_no,
    )
    report["normalization_runs_written_bucket"] = _bucket_count(proof.normalization_runs)
    report["suppression_traces_written_bucket"] = _bucket_count(proof.suppression_traces)
    report["artifacts_written_bucket"] = _bucket_count(proof.artifacts)
    report["artifact_observations_written_bucket"] = _bucket_count(proof.artifact_observations)
    report["candidate_groups_written_bucket"] = _bucket_count(proof.candidate_groups)
    report["candidate_members_written_bucket"] = _bucket_count(proof.candidate_members)
    report["enrich_outbox_events_written_bucket"] = _bucket_count(proof.enrich_outbox_events)
    if (
        proof.normalization_runs <= 0
        or proof.artifacts <= 0
        or proof.artifact_observations <= 0
        or proof.candidate_groups <= 0
        or proof.candidate_members <= 0
        or proof.enrich_outbox_events <= 0
    ):
        raise RuntimeError("write proof counts did not confirm persisted candidate flow")


def _approval_block_status(report: dict[str, Any], approvals: ConsumeApprovals) -> ScriptResult:
    if not approvals.any_granted:
        _set_status(report, STATUS_READY)
        return ScriptResult(exit_code=0, report=report)
    _set_status(report, STATUS_BLOCKED_NOT_READY)
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
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.url_missing")
        return None
    if not _database_url_is_supported(database_url):
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.url_unsupported")
        return None
    if not redis_url:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "redis.url_missing")
        return None
    if not _redis_url_is_supported(redis_url):
        _set_status(report, STATUS_BLOCKED_NOT_READY, "redis.url_unsupported")
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


def _collect_raw_values_from_published_event(event: PublishedSourceEvent, raw_values: set[str]) -> None:
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


def _collect_raw_values_from_source_snapshot(snapshot: Any, raw_values: set[str]) -> None:
    consume_smoke._collect_raw_values_from_source_snapshot(snapshot, raw_values)


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values if len(value) >= 6)


def _strip_url_fragment(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    return parsed._replace(fragment="").geturl()


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_source_rows: int = DEFAULT_MAX_SOURCE_ROWS,
    max_stream_entries: int = DEFAULT_MAX_STREAM_ENTRIES,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    normalizer_runner: NormalizerRunner | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    approvals = approvals or ConsumeApprovals(False, False, False, False, False, False)
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
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
            _set_status(report, STATUS_BLOCKED_NOT_READY, "runtime_env.read")
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
                    _set_status(report, STATUS_BLOCKED_NOT_READY, "database.read_only_transaction")
                    return ScriptResult(exit_code=1, report=report)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session, REQUIRED_READ_TABLES):
                _set_status(report, STATUS_BLOCKED_NOT_READY, "database.required_read_tables")
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.connection_or_schema")
            return ScriptResult(exit_code=1, report=report)

        selected = await _select_candidate_source(
            session=session,
            max_source_rows=max_source_rows,
            resolver_factory=short_url_resolver_factory,
            report=report,
            raw_values=raw_values,
        )
        if selected is None:
            return ScriptResult(exit_code=1, report=report)
        source_message_id = selected.source_row.current_snapshot.source_message_id
        source_version_no = candidate_probe._snapshot_for_planning(
            selected.source_row
        ).source_version_no
        report["source_message_rehydrate_succeeded_bucket"] = "one"
        report["source_version_rehydrate_succeeded_bucket"] = (
            "one" if selected.source_row.version_snapshot is not None else "not_requested"
        )

        published_event = await _load_published_source_event(
            session=session,
            source_message_id=source_message_id,
            report=report,
            raw_values=raw_values,
        )
        if published_event is None:
            return ScriptResult(exit_code=1, report=report)

        try:
            redis_client = await _open_redis_client(redis_url, redis_client_factory)
            await _maybe_await(redis_client.ping())
            report["redis_connected"] = True
            target_entry = await _scan_target_stream_entry(
                redis_client=redis_client,
                event_id=published_event.event_id,
                max_stream_entries=max_stream_entries,
                report=report,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "redis.connection_or_stream_read")
            return ScriptResult(exit_code=1, report=report)

        if target_entry is None:
            if report["checks_failed"]:
                return ScriptResult(exit_code=1, report=report)
            consumed_counts = await _load_existing_consumed_counts(
                session=session,
                source_message_id=source_message_id,
                source_version_no=source_version_no,
            )
            if consumed_counts.safely_consumed:
                _set_status(report, STATUS_ALREADY_CONSUMED)
                return ScriptResult(exit_code=0, report=report)
            _set_status(report, STATUS_NO_TARGET, "redis.target_stream_entry_missing")
            return ScriptResult(exit_code=1, report=report)

        contract = _validate_target_stream_entry(
            entry=target_entry,
            event_id=published_event.event_id,
            source_message_id=source_message_id,
            report=report,
        )
        if not contract.valid:
            for check in contract.checks_failed:
                _set_status(report, STATUS_BLOCKED_NOT_READY, check)
            return ScriptResult(exit_code=1, report=report)

        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "output.raw_values")
            return ScriptResult(exit_code=1, report=report)

        if not approvals.all_granted:
            return _approval_block_status(report, approvals)

        ok = await _consume_and_write(
            session=session,
            redis_client=redis_client,
            target_entry=target_entry,
            selected=selected,
            published_event=published_event,
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            database_url=database_url,
            redis_url=redis_url,
            normalizer_runner=normalizer_runner,
            report=report,
            raw_values=raw_values,
        )
        if _forbidden_side_effect_detected(report):
            _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
            return ScriptResult(exit_code=1, report=report)
        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "output.raw_values")
            return ScriptResult(exit_code=1, report=report)
        return ScriptResult(exit_code=0 if ok else 1, report=report)
    except Exception:
        if session is not None:
            await session.rollback()
        _set_status(report, STATUS_BLOCKED_NOT_READY, "unexpected")
        return ScriptResult(exit_code=1, report=report)
    finally:
        if session is not None and not approvals.all_granted:
            await session.rollback()
        await _close_redis_client(redis_client)
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_source_rows: int = DEFAULT_MAX_SOURCE_ROWS,
    max_stream_entries: int = DEFAULT_MAX_STREAM_ENTRIES,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    normalizer_runner: NormalizerRunner | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            max_source_rows=max_source_rows,
            max_stream_entries=max_stream_entries,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            short_url_resolver_factory=short_url_resolver_factory,
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
        max_source_rows=args.max_source_rows,
        max_stream_entries=args.max_stream_entries,
        approvals=ConsumeApprovals(
            targeted_router_normalizer_consume_smoke=(
                args.approved_targeted_router_normalizer_consume_smoke
            ),
            redis_targeted_consumer_group=args.approved_redis_targeted_consumer_group,
            normalization_write=args.approved_normalization_write,
            artifact_candidate_write=args.approved_artifact_candidate_write,
            event_outbox_write=args.approved_event_outbox_write,
            targeted_redis_ack=args.approved_targeted_redis_ack,
        ),
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
