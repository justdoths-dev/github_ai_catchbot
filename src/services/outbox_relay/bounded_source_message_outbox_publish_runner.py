from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local fallback for static validation
    sa = None

from .models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from .redis_streams import RedisStreamsPublisher
from .repositories import AsyncSessionLike, OutboxRelayRepository
from .routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError

SCHEMA_VERSION = "bounded_source_message_outbox_publish_v1"
RUNNER_NAME = "bounded_source_message_outbox_publish_runner"
MODE = "source_message_outbox_one_shot_publish"
QUEUE_NAME = "q.source.normalize"
STAGE_NAME = "normalize"
ROOT_OBJECT_TYPE = "source_message"
DEFAULT_XADD_MAXLEN = 10000
DEFAULT_MAX_EVENTS = 1
HARD_MAX_EVENTS = 1
SOURCE_MESSAGE_EVENT_TYPES = frozenset(
    {
        "source_message.created.v1",
        "source_message.edited.v1",
        "source_message.deleted.v1",
        "source_message.reconciled.v1",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedSourceMessageOutboxPublishConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_publish: bool = False
    allow_database_write: bool = False
    event_id: UUID | None = None
    source_message_id: UUID | None = None
    max_events: int = DEFAULT_MAX_EVENTS


@dataclass(frozen=True, slots=True)
class BoundedSourceMessagePublishRuntimeConfig:
    database_url: str
    redis_url: str
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedSourceMessageOutboxPublishState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    redis_publisher_created: bool = False
    redis_publish_attempted: bool = False
    event_outbox_status_write_attempted: bool = False
    job_attempt_insert_attempted: bool = False

    @property
    def database_write_attempted(self) -> bool:
        return self.event_outbox_status_write_attempted or self.job_attempt_insert_attempted


class BoundedSourceMessageOutboxPublishError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _PublishResultReady(Exception):
    pass


class BoundedSourceMessageOutboxRepository(Protocol):
    async def fetch_target_events(
        self,
        *,
        event_id: UUID | None,
        source_message_id: UUID | None,
        limit: int,
    ) -> list[OutboxEventRow]: ...

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None: ...
    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None: ...
    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None: ...


class RedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


@dataclass(frozen=True, slots=True)
class BoundedSourceMessageRepositoryHandle:
    repository: BoundedSourceMessageOutboxRepository
    close: Callable[[bool], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedSourceMessageRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedSourceMessageRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedSourceMessagePublishRuntimeConfig,
        state: BoundedSourceMessageOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedSourceMessageRepositoryHandle: ...


class BoundedSourceMessageRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedSourceMessagePublishRuntimeConfig,
        state: BoundedSourceMessageOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedSourceMessageRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedSourceMessageOutboxPublishResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedSourceMessageOutboxPublishConfig
    state: BoundedSourceMessageOutboxPublishState = field(default_factory=BoundedSourceMessageOutboxPublishState)
    target_event_id_suffix: str | None = None
    target_source_message_id_suffix: str | None = None
    events_seen: int = 0
    events_published_count: int = 0
    job_attempts_inserted_count: int = 0
    queue_name: str = QUEUE_NAME
    stage_name: str = STAGE_NAME
    selected_event_status: str | None = None
    selected_event_type: str | None = None
    selected_aggregate_type: str | None = None
    event_outbox_marked_published: bool = False
    event_outbox_marked_failed: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_event_id_suffix": self.target_event_id_suffix,
            "target_source_message_id_suffix": self.target_source_message_id_suffix,
            "events_seen": self.events_seen,
            "events_published_count": self.events_published_count,
            "job_attempts_inserted_count": self.job_attempts_inserted_count,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_publish_allowed": self.config.allow_redis_publish,
                "database_write_allowed": self.config.allow_database_write,
                "max_events": self.config.max_events,
            },
            "selected_event_status": self.selected_event_status,
            "selected_event_type": self.selected_event_type,
            "selected_aggregate_type": self.selected_aggregate_type,
            "event_outbox_marked_published": self.event_outbox_marked_published,
            "event_outbox_marked_failed": self.event_outbox_marked_failed,
            "redactions_applied": [
                "full_event_id_omitted",
                "full_source_message_id_omitted",
                "idempotency_key_omitted",
                "payload_json_omitted",
                "database_url_omitted",
                "redis_url_omitted",
                "redis_message_id_omitted",
                "exception_detail_omitted",
            ],
            "side_effects": {
                "redis_mutation": self.events_published_count > 0,
                "db_write": self.state.database_write_attempted,
                "telegram_send_called": False,
                "telegram_read_called": False,
                "openai/github/x/web": False,
                "notification_table_write": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd/docker/alembic": False,
            },
        }


class SqlAlchemyBoundedSourceMessageOutboxRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._relay_repository = OutboxRelayRepository(session)

    async def fetch_target_events(
        self,
        *,
        event_id: UUID | None,
        source_message_id: UUID | None,
        limit: int,
    ) -> list[OutboxEventRow]:
        if event_id is not None:
            statement = """
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
                WHERE event_id = CAST(:event_id AS uuid)
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            params: dict[str, Any] = {"event_id": str(event_id), "limit": limit}
        elif source_message_id is not None:
            statement = """
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
                WHERE aggregate_id = CAST(:source_message_id AS uuid)
                  AND status = 'pending'::outbox_status_enum
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            params = {"source_message_id": str(source_message_id), "limit": limit}
        else:  # pragma: no cover - guarded before repository calls
            raise BoundedSourceMessageOutboxPublishError("target_missing")

        result = await self._session.execute(_sql(statement), params)
        return [_row_from_mapping(row) for row in result.mappings().all()]

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        await self._relay_repository.mark_published(event_id=event_id, published_at=published_at)

    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None:
        await self._relay_repository.mark_failed(event_id=event_id, error_text=error_text)

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        await self._relay_repository.insert_job_attempt(
            stage_name=stage_name,
            queue_name=queue_name,
            root_object_type=root_object_type,
            root_object_id=root_object_id,
            attempt_status=attempt_status,
            error_code=error_code,
        )


def load_bounded_source_message_publish_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedSourceMessagePublishRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedSourceMessageOutboxPublishError("database_url_missing")
    if not redis_url:
        raise BoundedSourceMessageOutboxPublishError("redis_url_missing")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError as exc:
        raise BoundedSourceMessageOutboxPublishError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedSourceMessageOutboxPublishError("runtime_config_error")
    return BoundedSourceMessagePublishRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        xadd_maxlen=xadd_maxlen,
    )


async def build_default_bounded_source_message_repository(
    runtime_config: BoundedSourceMessagePublishRuntimeConfig,
    state: BoundedSourceMessageOutboxPublishState,
    logger: logging.Logger,
) -> BoundedSourceMessageRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedSourceMessageOutboxRepository(session)

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedSourceMessageRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_source_message_redis_publisher(
    runtime_config: BoundedSourceMessagePublishRuntimeConfig,
    state: BoundedSourceMessageOutboxPublishState,
    logger: logging.Logger,
) -> BoundedSourceMessageRedisPublisherHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_publisher_created = True
    publisher = RedisStreamsPublisher(redis_client, maxlen=runtime_config.xadd_maxlen)

    async def close() -> None:
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedSourceMessageRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_source_message_outbox_publish(
    config: BoundedSourceMessageOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedSourceMessagePublishRuntimeConfig] = (
        load_bounded_source_message_publish_runtime_config
    ),
    repository_builder: BoundedSourceMessageRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedSourceMessageRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedSourceMessageOutboxPublishResult:
    state = BoundedSourceMessageOutboxPublishState()
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if (config.event_id is None) == (config.source_message_id is None):
        error_code = "target_missing" if config.event_id is None else "target_conflict"
        return _result("blocked", error_code, config=config, state=state)
    if config.max_events != HARD_MAX_EVENTS:
        return _result("blocked", "max_events_must_be_one", config=config, state=state)
    if not config.allow_runtime_config:
        return _result("blocked", "runtime_config_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedSourceMessageOutboxPublishError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)

    repository_handle: BoundedSourceMessageRepositoryHandle | None = None
    publisher_handle: BoundedSourceMessageRedisPublisherHandle | None = None
    commit_repository = False
    result: BoundedSourceMessageOutboxPublishResult | None = None
    try:
        repository_handle = await (repository_builder or build_default_bounded_source_message_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        rows = await repository.fetch_target_events(
            event_id=config.event_id,
            source_message_id=config.source_message_id,
            limit=HARD_MAX_EVENTS + 1,
        )
        events_seen = len(rows)
        if events_seen == 0:
            result = _result("blocked", "target_event_not_found", config=config, state=state, events_seen=0)
            raise _PublishResultReady
        if events_seen > HARD_MAX_EVENTS:
            result = _result(
                "blocked",
                "target_event_count_exceeded",
                config=config,
                state=state,
                events_seen=events_seen,
            )
            raise _PublishResultReady

        row = rows[0]
        if row.status != "pending":
            result = _result(
                "blocked",
                "target_event_not_pending",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
            )
            raise _PublishResultReady
        if row.aggregate_type != ROOT_OBJECT_TYPE or row.event_type not in SOURCE_MESSAGE_EVENT_TYPES:
            result = _result(
                "blocked",
                "source_message_event_contract_mismatch",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
            )
            raise _PublishResultReady

        try:
            route = (route_resolver or OutboxRouteResolver()).resolve(row)
        except UnsupportedOutboxEventTypeError:
            result = _result(
                "blocked",
                "unsupported_event_type",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
            )
            raise _PublishResultReady
        if route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            result = _result(
                "blocked",
                "route_not_allowed",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
            )
            raise _PublishResultReady
        if not config.allow_redis_publish:
            result = _result(
                "blocked",
                "redis_publish_not_allowed",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
            )
            raise _PublishResultReady

        publisher_handle = await (redis_publisher_builder or build_default_bounded_source_message_redis_publisher)(
            runtime_config,
            state,
            effective_logger,
        )
        message = _build_stream_message(row, route)
        try:
            state.redis_publish_attempted = True
            redis_message_id = await publisher_handle.publisher.publish(route, message)
        except Exception as exc:
            event_outbox_marked_failed, job_attempts_inserted = await _record_failed_publish(
                repository,
                state,
                row,
            )
            commit_repository = event_outbox_marked_failed or job_attempts_inserted > 0
            result = _result(
                "failed",
                "redis_publish_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                event_outbox_marked_failed=event_outbox_marked_failed,
                job_attempts_inserted_count=job_attempts_inserted,
            )
            raise _PublishResultReady

        try:
            state.event_outbox_status_write_attempted = True
            await repository.mark_published(
                event_id=row.event_id,
                published_at=(clock or _utc_now)(),
            )
            state.job_attempt_insert_attempted = True
            await repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
        except Exception as exc:
            result = _result(
                "failed",
                "database_write_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                events_published_count=1,
            )
            raise _PublishResultReady

        commit_repository = True
        result = _result(
            "published",
            None,
            config=config,
            state=state,
            selected_event=row,
            events_seen=events_seen,
            events_published_count=1 if redis_message_id else 1,
            event_outbox_marked_published=True,
            job_attempts_inserted_count=1,
        )
    except _PublishResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_publish_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )
    finally:
        if publisher_handle is not None:
            try:
                await publisher_handle.close()
            except Exception:
                pass
        if repository_handle is not None:
            try:
                await repository_handle.close(commit_repository)
            except Exception as exc:
                error_code = _repository_close_error_code(commit_repository)
                if result is None:
                    result = _result(
                        "failed",
                        error_code,
                        error_class=_safe_exception_class(exc),
                        config=config,
                        state=state,
                    )
                else:
                    result = replace(
                        result,
                        status="failed",
                        ok=False,
                        error_code=error_code,
                        error_class=_safe_exception_class(exc),
                    )

    assert result is not None
    return result


def run_bounded_source_message_outbox_publish_sync(
    config: BoundedSourceMessageOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedSourceMessagePublishRuntimeConfig] = (
        load_bounded_source_message_publish_runtime_config
    ),
    repository_builder: BoundedSourceMessageRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedSourceMessageRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedSourceMessageOutboxPublishResult:
    return asyncio.run(
        run_bounded_source_message_outbox_publish(
            config,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
            redis_publisher_builder=redis_publisher_builder,
            route_resolver=route_resolver,
            clock=clock,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedSourceMessageOutboxPublishConfig(),
        state=BoundedSourceMessageOutboxPublishState(),
    ).to_sanitized_dict()


async def _record_failed_publish(
    repository: BoundedSourceMessageOutboxRepository,
    state: BoundedSourceMessageOutboxPublishState,
    row: OutboxEventRow,
) -> tuple[bool, int]:
    marked_failed = False
    inserted = 0
    try:
        state.event_outbox_status_write_attempted = True
        await repository.mark_failed(event_id=row.event_id, error_text="redis_publish_failed")
        marked_failed = True
    except Exception:
        return marked_failed, inserted
    try:
        state.job_attempt_insert_attempted = True
        await repository.insert_job_attempt(
            stage_name=STAGE_NAME,
            queue_name=QUEUE_NAME,
            root_object_type=row.aggregate_type,
            root_object_id=row.aggregate_id,
            attempt_status="failed_retryable",
            error_code="redis_publish_failed",
        )
        inserted = 1
    except Exception:
        return marked_failed, inserted
    return marked_failed, inserted


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedSourceMessageOutboxPublishConfig,
    state: BoundedSourceMessageOutboxPublishState,
    error_class: str | None = None,
    selected_event: OutboxEventRow | None = None,
    events_seen: int = 0,
    events_published_count: int = 0,
    event_outbox_marked_published: bool = False,
    event_outbox_marked_failed: bool = False,
    job_attempts_inserted_count: int = 0,
) -> BoundedSourceMessageOutboxPublishResult:
    selected_event_id = selected_event.event_id if selected_event is not None else config.event_id
    selected_source_message_id = (
        selected_event.aggregate_id
        if selected_event is not None and selected_event.aggregate_type == ROOT_OBJECT_TYPE
        else config.source_message_id
    )
    return BoundedSourceMessageOutboxPublishResult(
        status=status,
        ok=status == "published" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        target_event_id_suffix=_optional_id_suffix(selected_event_id),
        target_source_message_id_suffix=_optional_id_suffix(selected_source_message_id),
        events_seen=events_seen,
        events_published_count=events_published_count,
        job_attempts_inserted_count=job_attempts_inserted_count,
        selected_event_status=selected_event.status if selected_event is not None else None,
        selected_event_type=selected_event.event_type if selected_event is not None else None,
        selected_aggregate_type=selected_event.aggregate_type if selected_event is not None else None,
        event_outbox_marked_published=event_outbox_marked_published,
        event_outbox_marked_failed=event_outbox_marked_failed,
    )


def _build_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=str(row.event_id),
    )


def _row_from_mapping(row: Mapping[str, Any]) -> OutboxEventRow:
    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload or {},
        status=str(row["status"]),
        fail_count=int(row["fail_count"]),
        created_at=row["created_at"],
    )


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _repository_close_error_code(commit: bool) -> str:
    return "repository_commit_failed" if commit else "repository_rollback_failed"


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedSourceMessageOutboxPublishConfig",
    "BoundedSourceMessageOutboxPublishError",
    "BoundedSourceMessageOutboxPublishResult",
    "BoundedSourceMessagePublishRuntimeConfig",
    "BoundedSourceMessageRedisPublisherBuilder",
    "BoundedSourceMessageRedisPublisherHandle",
    "BoundedSourceMessageRepositoryBuilder",
    "BoundedSourceMessageRepositoryHandle",
    "DEFAULT_MAX_EVENTS",
    "HARD_MAX_EVENTS",
    "MODE",
    "QUEUE_NAME",
    "ROOT_OBJECT_TYPE",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "SOURCE_MESSAGE_EVENT_TYPES",
    "STAGE_NAME",
    "SqlAlchemyBoundedSourceMessageOutboxRepository",
    "argument_error_report",
    "build_default_bounded_source_message_redis_publisher",
    "build_default_bounded_source_message_repository",
    "load_bounded_source_message_publish_runtime_config",
    "render_sanitized_json",
    "run_bounded_source_message_outbox_publish",
    "run_bounded_source_message_outbox_publish_sync",
]
