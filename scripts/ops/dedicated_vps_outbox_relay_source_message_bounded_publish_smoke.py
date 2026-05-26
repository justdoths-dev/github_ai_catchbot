from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_outbox_relay_source_message_bounded_publish_smoke"
REPORT_TYPE = "outbox_relay_source_message_bounded_publish_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_EVENTS = 1
MAX_EVENTS_HARD_LIMIT = 3
EXPECTED_QUEUE_NAME = "q.source.normalize"
EXPECTED_STAGE_NAME = "normalize"
EXPECTED_AGGREGATE_TYPE = "source_message"
SOURCE_MESSAGE_EVENT_TYPES = (
    "source_message.created.v1",
    "source_message.edited.v1",
    "source_message.deleted.v1",
    "source_message.reconciled.v1",
)
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
    "raw",
    "text",
    "caption",
    "message",
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
    "router_normalizer_started",
    "notifier_started",
    "docker_or_systemd_changed",
    "alembic_run",
    "raw_values_emitted",
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
COUNT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE status = 'pending'::outbox_status_enum
  AND event_type = ANY(CAST(:event_types AS text[]))
"""
SELECT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY = """
SELECT
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    fail_count,
    created_at
FROM event_outbox
WHERE status = 'pending'::outbox_status_enum
  AND event_type = ANY(CAST(:event_types AS text[]))
ORDER BY created_at ASC, event_id ASC
LIMIT :limit
"""
REQUIRED_TABLES = ("event_outbox", "job_attempts")


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def close(self) -> None: ...


class PublisherLike(Protocol):
    async def publish(self, route: Any, message: Any) -> str: ...

    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
PublisherFactory = Callable[[str, int | None], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PublishApprovals:
    outbox_relay_publish_smoke: bool
    redis_publish: bool
    event_outbox_status_update: bool
    job_attempt_write: bool

    @property
    def all_granted(self) -> bool:
        return (
            self.outbox_relay_publish_smoke
            and self.redis_publish
            and self.event_outbox_status_update
            and self.job_attempt_write
        )

    @property
    def any_granted(self) -> bool:
        return (
            self.outbox_relay_publish_smoke
            or self.redis_publish
            or self.event_outbox_status_update
            or self.job_attempt_write
        )

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.outbox_relay_publish_smoke:
            checks.append("approval.outbox_relay_publish_smoke")
        if not self.redis_publish:
            checks.append("approval.redis_publish")
        if not self.event_outbox_status_update:
            checks.append("approval.event_outbox_status_update")
        if not self.job_attempt_write:
            checks.append("approval.job_attempt_write")
        return checks


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.outbox_relay.config import OutboxRelayConfig  # noqa: E402
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage  # noqa: E402
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher  # noqa: E402
from src.services.outbox_relay.repositories import OutboxRelayRepository, _sql  # noqa: E402
from src.services.outbox_relay.routing import (  # noqa: E402
    OutboxRouteResolver,
    UnsupportedOutboxEventTypeError,
)
from src.services.outbox_relay.service import OutboxRelayService  # noqa: E402


class SourceMessageOutboxRelayRepository(OutboxRelayRepository):
    async def fetch_pending_batch(self, *, limit: int) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(SELECT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY),
            {"event_types": list(SOURCE_MESSAGE_EVENT_TYPES), "limit": limit},
        )
        rows: list[OutboxEventRow] = []
        for raw_row in result.mappings().all():
            payload = raw_row["payload_json"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            rows.append(
                OutboxEventRow(
                    event_id=_coerce_uuid(raw_row["event_id"]),
                    event_type=str(raw_row["event_type"]),
                    aggregate_type=str(raw_row["aggregate_type"]),
                    aggregate_id=_coerce_uuid(raw_row["aggregate_id"]),
                    dedupe_key=str(raw_row["dedupe_key"]),
                    payload_json=payload if isinstance(payload, dict) else {},
                    status=str(raw_row["status"]),
                    fail_count=int(raw_row["fail_count"]),
                    created_at=raw_row["created_at"],
                )
            )
        return rows


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


class _DefaultRedisPublisher:
    def __init__(self, client: Any, publisher: RedisStreamsPublisher) -> None:
        self._client = client
        self._publisher = publisher

    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        return await self._publisher.publish(route, message)

    async def close(self) -> None:
        close = getattr(self._client, "aclose", None) or getattr(self._client, "close", None)
        if close is not None:
            await _maybe_await(close())


class _NoopRepository:
    async def fetch_pending_batch(self, *, limit: int) -> list[OutboxEventRow]:
        raise AssertionError("message builder repository should not be used")


class _NoopPublisher:
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        raise AssertionError("message builder publisher should not be used")


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
            "Bounded source_message.* outbox-relay publish smoke. The default "
            "mode reads runtime configuration and checks pending source outbox "
            "routing without Redis or event_outbox/job_attempt mutation."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--max-events",
        type=_bounded_positive_int_named("max-events", upper_bound=MAX_EVENTS_HARD_LIMIT),
        default=DEFAULT_MAX_EVENTS,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--approved-outbox-relay-publish-smoke", action="store_true")
    parser.add_argument("--approved-redis-publish", action="store_true")
    parser.add_argument("--approved-event-outbox-status-update", action="store_true")
    parser.add_argument("--approved-job-attempt-write", action="store_true")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "pending_source_outbox_events_bucket": "zero",
        "selected_outbox_events_bucket": "zero",
        "route_q_source_normalize_bucket": "zero",
        "route_stage_normalize_bucket": "zero",
        "redis_publish_attempted": False,
        "redis_publish_succeeded_bucket": "zero",
        "redis_mutation_performed": False,
        "event_outbox_status_update_attempted": False,
        "event_outbox_published_bucket": "zero",
        "event_outbox_failed_bucket": "zero",
        "job_attempt_insert_attempted": False,
        "job_attempt_succeeded_bucket": "zero",
        "job_attempt_failed_bucket": "zero",
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "registry_mutation_performed": False,
        "downstream_service_started": False,
        "router_normalizer_started": False,
        "notifier_started": False,
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


async def _open_default_publisher(redis_url: str, maxlen: int | None) -> PublisherLike:
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    client = Redis.from_url(redis_url, decode_responses=True)
    return _DefaultRedisPublisher(client, RedisStreamsPublisher(client, maxlen=maxlen))


async def _open_publisher(
    redis_url: str,
    maxlen: int | None,
    publisher_factory: PublisherFactory | None,
) -> PublisherLike:
    if publisher_factory is not None:
        return await _maybe_await(publisher_factory(redis_url, maxlen))
    return await _open_default_publisher(redis_url, maxlen)


async def _close_database_session(session: AsyncSessionLike | None) -> None:
    if session is not None:
        await _maybe_await(session.close())


async def _close_publisher(publisher: PublisherLike | None) -> None:
    if publisher is not None:
        close = getattr(publisher, "close", None)
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


async def _count_pending_source_outbox_events(session: AsyncSessionLike) -> int:
    value = await _scalar(
        await _execute(
            session,
            COUNT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY,
            {"event_types": list(SOURCE_MESSAGE_EVENT_TYPES)},
        )
    )
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


def _message_builder_config() -> OutboxRelayConfig:
    return OutboxRelayConfig(
        app_env="test",
        database_url="postgresql+psycopg://unused",
        redis_url="redis://unused",
        poll_interval_ms=1000,
        batch_size=1,
        xadd_maxlen=10000,
        log_level="INFO",
    )


def build_redis_queued_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    service = OutboxRelayService(
        _message_builder_config(),
        repository=_NoopRepository(),
        publisher=_NoopPublisher(),
        route_resolver=OutboxRouteResolver(),
    )
    return service._build_stream_message(row, route)


def validate_redis_thin_payload_shape(fields: Mapping[str, Any]) -> bool:
    keys = set(fields)
    if keys != ALLOWED_REDIS_THIN_FIELDS:
        return False
    return not any(_is_forbidden_redis_field(key) for key in keys)


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_REDIS_FIELD_TOKENS)


def _raw_values_from_rows(rows: Sequence[OutboxEventRow]) -> set[str]:
    values: set[str] = set()
    for row in rows:
        values.update({str(row.event_id), str(row.aggregate_id), row.dedupe_key})
        values.add(json.dumps(row.payload_json, sort_keys=True, default=str))
        for key, value in row.payload_json.items():
            if isinstance(key, str) and key:
                values.add(key)
            if isinstance(value, str) and value:
                values.add(value)
    return {value for value in values if len(value) >= 6}


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values)


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


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str, int | None] | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    xadd_maxlen_raw = str(values.get("OUTBOX_RELAY_XADD_MAXLEN", "10000")).strip()
    if database_url:
        raw_values.add(database_url)
    if redis_url:
        raw_values.add(redis_url)
    if not database_url:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "database.url_missing",
        )
        return None
    if not _database_url_is_supported(database_url):
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "database.url_unsupported",
        )
        return None
    if not redis_url:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "redis.url_missing",
        )
        return None
    if not _redis_url_is_supported(redis_url):
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "redis.url_unsupported",
        )
        return None
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "redis.xadd_maxlen_invalid",
        )
        return None
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "redis.xadd_maxlen_invalid",
        )
        return None
    return database_url, redis_url, xadd_maxlen


def _approval_block_status(
    report: dict[str, Any],
    approvals: PublishApprovals,
) -> None:
    if not approvals.any_granted:
        _set_status(report, "outbox_relay_source_message_bounded_publish_smoke_ready")
        return
    _set_status(
        report,
        "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
    )
    for check in approvals.missing_checks():
        _set_status(report, report["contract_status"], check)


def _evaluate_rows_for_publish(
    *,
    report: dict[str, Any],
    rows: Sequence[OutboxEventRow],
    route_resolver: Any,
) -> tuple[list[tuple[OutboxEventRow, QueueRoute, RedisQueuedMessage]], set[str], bool]:
    planned: list[tuple[OutboxEventRow, QueueRoute, RedisQueuedMessage]] = []
    raw_values = _raw_values_from_rows(rows)
    route_queue_ok_count = 0
    route_stage_ok_count = 0
    contract_mismatch = False

    report["selected_outbox_events_bucket"] = _bucket_count(len(rows))
    for row in rows:
        if row.event_type not in SOURCE_MESSAGE_EVENT_TYPES:
            contract_mismatch = True
            continue
        if row.aggregate_type != EXPECTED_AGGREGATE_TYPE:
            contract_mismatch = True
            continue
        try:
            route = route_resolver.resolve(row)
        except UnsupportedOutboxEventTypeError:
            contract_mismatch = True
            continue

        if route.queue_name == EXPECTED_QUEUE_NAME:
            route_queue_ok_count += 1
        else:
            contract_mismatch = True

        if route.stage_name == EXPECTED_STAGE_NAME:
            route_stage_ok_count += 1
        else:
            contract_mismatch = True

        message = build_redis_queued_message(row, route)
        fields = message.as_stream_fields()
        if not validate_redis_thin_payload_shape(fields):
            contract_mismatch = True
            continue
        planned.append((row, route, message))

    report["route_q_source_normalize_bucket"] = _bucket_count(route_queue_ok_count)
    report["route_stage_normalize_bucket"] = _bucket_count(route_stage_ok_count)
    if contract_mismatch or len(planned) != len(rows):
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "outbox_relay.source_message_route_contract",
        )
        return planned, raw_values, False
    return planned, raw_values, True


async def _record_publish_failure(
    *,
    repository: SourceMessageOutboxRelayRepository,
    row: OutboxEventRow,
    route: QueueRoute,
    error_code: str,
    report: dict[str, Any],
) -> None:
    report["event_outbox_status_update_attempted"] = True
    await repository.mark_failed(event_id=row.event_id, error_text=error_code)
    report["event_outbox_failed_bucket"] = "one"
    report["job_attempt_insert_attempted"] = True
    await repository.insert_job_attempt(
        stage_name=route.stage_name,
        queue_name=route.queue_name,
        root_object_type=row.aggregate_type,
        root_object_id=row.aggregate_id,
        attempt_status="failed_retryable",
        error_code=error_code,
    )
    report["job_attempt_failed_bucket"] = "one"


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_events: int = DEFAULT_MAX_EVENTS,
    approvals: PublishApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    publisher_factory: PublisherFactory | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    approvals = approvals or PublishApprovals(False, False, False, False)
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, "blocked_forbidden_side_effect_detected", "side_effect.forbidden")
        return ScriptResult(exit_code=1, report=report)

    if max_events <= 0 or max_events > MAX_EVENTS_HARD_LIMIT:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "max_events.out_of_bounds",
        )
        return ScriptResult(exit_code=1, report=report)

    session: AsyncSessionLike | None = None
    publisher: PublisherLike | None = None
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
        except Exception:
            _set_status(
                report,
                "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
                "runtime_env.read",
            )
            return ScriptResult(exit_code=1, report=report)

        runtime_config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return ScriptResult(exit_code=1, report=report)
        database_url, redis_url, xadd_maxlen = runtime_config

        try:
            session = await _open_database_session(database_url, database_session_factory)
            if not approvals.all_granted:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only_value = await _scalar(
                    await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY)
                )
                report["read_only_transaction"] = _transaction_read_only_enabled(
                    read_only_value
                )
                if not report["read_only_transaction"]:
                    _set_status(
                        report,
                        "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
                        "database.read_only_transaction",
                    )
                    return ScriptResult(exit_code=1, report=report)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(
                    report,
                    "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
                    "database.required_tables",
                )
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
                "database.connection_or_schema",
            )
            return ScriptResult(exit_code=1, report=report)

        pending_count = await _count_pending_source_outbox_events(session)
        report["pending_source_outbox_events_bucket"] = _bucket_count(pending_count)
        if pending_count <= 0:
            _set_status(
                report,
                "outbox_relay_source_message_bounded_publish_smoke_no_pending_events",
            )
            return ScriptResult(exit_code=0, report=report)

        repository = SourceMessageOutboxRelayRepository(session)
        rows = await repository.fetch_pending_batch(limit=max_events)
        if not rows:
            _set_status(
                report,
                "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
                "event_outbox.selected_empty",
            )
            return ScriptResult(exit_code=1, report=report)

        resolver = route_resolver or OutboxRouteResolver()
        planned, row_raw_values, rows_ready = _evaluate_rows_for_publish(
            report=report,
            rows=rows,
            route_resolver=resolver,
        )
        raw_values.update(row_raw_values)
        raw_values.add(str(runtime_env_path))
        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, "blocked_forbidden_side_effect_detected", "output.raw_values")
            return ScriptResult(exit_code=1, report=report)
        if not rows_ready:
            return ScriptResult(exit_code=1, report=report)

        if not approvals.all_granted:
            _approval_block_status(report, approvals)
            return ScriptResult(
                exit_code=0 if not approvals.any_granted else 1,
                report=report,
            )

        try:
            publisher = await _open_publisher(redis_url, xadd_maxlen, publisher_factory)
            report["redis_connected"] = True
        except Exception:
            _set_status(
                report,
                "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
                "redis.connection",
            )
            return ScriptResult(exit_code=1, report=report)

        publish_success_count = 0
        published_count = 0
        job_success_count = 0
        for row, route, message in planned:
            try:
                report["redis_publish_attempted"] = True
                redis_message_id = await publisher.publish(route, message)
                if redis_message_id:
                    raw_values.add(str(redis_message_id))
                publish_success_count += 1
                report["redis_publish_succeeded_bucket"] = _bucket_count(publish_success_count)
                report["redis_mutation_performed"] = True

                report["event_outbox_status_update_attempted"] = True
                await repository.mark_published(
                    event_id=row.event_id,
                    published_at=datetime.now(timezone.utc),
                )
                published_count += 1
                report["event_outbox_published_bucket"] = _bucket_count(published_count)

                report["job_attempt_insert_attempted"] = True
                await repository.insert_job_attempt(
                    stage_name=route.stage_name,
                    queue_name=route.queue_name,
                    root_object_type=row.aggregate_type,
                    root_object_id=row.aggregate_id,
                    attempt_status="succeeded",
                    error_code=None,
                )
                job_success_count += 1
                report["job_attempt_succeeded_bucket"] = _bucket_count(job_success_count)
            except Exception as exc:
                error_code = type(exc).__name__
                try:
                    await _record_publish_failure(
                        repository=repository,
                        row=row,
                        route=route,
                        error_code=error_code,
                        report=report,
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    _set_status(
                        report,
                        "blocked_outbox_relay_source_message_bounded_publish_smoke_publish_failed",
                        "event_outbox.failure_status_update",
                    )
                    return ScriptResult(exit_code=1, report=report)
                _set_status(
                    report,
                    "blocked_outbox_relay_source_message_bounded_publish_smoke_publish_failed",
                    "redis.publish",
                )
                return ScriptResult(exit_code=1, report=report)

        await session.commit()

        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, "blocked_forbidden_side_effect_detected", "output.raw_values")
            return ScriptResult(exit_code=1, report=report)
        if _forbidden_side_effect_detected(report):
            _set_status(report, "blocked_forbidden_side_effect_detected", "side_effect.forbidden")
            return ScriptResult(exit_code=1, report=report)
        if published_count > 0:
            _set_status(report, "outbox_relay_source_message_bounded_publish_smoke_published")
            return ScriptResult(exit_code=0, report=report)

        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "event_outbox.published_zero",
        )
        return ScriptResult(exit_code=1, report=report)
    except Exception:
        if session is not None:
            await session.rollback()
        _set_status(
            report,
            "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready",
            "unexpected",
        )
        return ScriptResult(exit_code=1, report=report)
    finally:
        if session is not None and not approvals.all_granted:
            await session.rollback()
        await _close_publisher(publisher)
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_events: int = DEFAULT_MAX_EVENTS,
    approvals: PublishApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    publisher_factory: PublisherFactory | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            max_events=max_events,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            publisher_factory=publisher_factory,
            route_resolver=route_resolver,
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
        max_events=args.max_events,
        approvals=PublishApprovals(
            outbox_relay_publish_smoke=args.approved_outbox_relay_publish_smoke,
            redis_publish=args.approved_redis_publish,
            event_outbox_status_update=args.approved_event_outbox_status_update,
            job_attempt_write=args.approved_job_attempt_write,
        ),
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
