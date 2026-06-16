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

SCHEMA_VERSION = "bounded_judge_call_requested_outbox_publish_v1"
RUNNER_NAME = "bounded_judge_call_requested_outbox_publish_runner"
MODE = "judge_call_requested_outbox_one_shot_publish"
EVENT_TYPE = "judge.call.requested.v1"
ROOT_OBJECT_TYPE = "judge_run"
QUEUE_NAME = "q.analysis.judge"
STAGE_NAME = "judge"
DEFAULT_XADD_MAXLEN = 10000
DEFAULT_MAX_EVENTS = 1
HARD_MAX_EVENTS = 1
REQUIRED_PAYLOAD_FIELDS = (
    "judge_run_id",
    "bundle_id",
    "model",
    "reasoning_effort",
    "prompt_version",
    "prompt_cache_key",
)


@dataclass(frozen=True, slots=True)
class BoundedJudgeCallRequestedOutboxPublishConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_redis_publish: bool = False
    allow_database_write: bool = False
    trigger_event_id: UUID | None = None
    trigger_event_suffix: str | None = None
    max_events: int = DEFAULT_MAX_EVENTS


@dataclass(frozen=True, slots=True)
class BoundedJudgeCallRequestedPublishRuntimeConfig:
    database_url: str
    redis_url: str
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(frozen=True, slots=True)
class JudgeRunLocatorRecord:
    judge_run_id: UUID
    bundle_id: UUID
    status: str


@dataclass(slots=True)
class BoundedJudgeCallRequestedOutboxPublishState:
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


class BoundedJudgeCallRequestedOutboxPublishError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _PublishResultReady(Exception):
    pass


class BoundedJudgeCallRequestedOutboxRepository(Protocol):
    async def fetch_target_events(
        self,
        *,
        trigger_event_id: UUID | None,
        trigger_event_suffix: str | None,
        limit: int,
    ) -> list[OutboxEventRow]: ...

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunLocatorRecord | None: ...
    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None: ...
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
class BoundedJudgeCallRequestedRepositoryHandle:
    repository: BoundedJudgeCallRequestedOutboxRepository
    close: Callable[[bool], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedJudgeCallRequestedRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedJudgeCallRequestedRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeCallRequestedPublishRuntimeConfig,
        state: BoundedJudgeCallRequestedOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedJudgeCallRequestedRepositoryHandle: ...


class BoundedJudgeCallRequestedRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeCallRequestedPublishRuntimeConfig,
        state: BoundedJudgeCallRequestedOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedJudgeCallRequestedRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedJudgeCallRequestedOutboxPublishResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedJudgeCallRequestedOutboxPublishConfig
    state: BoundedJudgeCallRequestedOutboxPublishState = field(
        default_factory=BoundedJudgeCallRequestedOutboxPublishState
    )
    selector_type: str | None = None
    target_trigger_event_id_suffix: str | None = None
    target_judge_run_id_suffix: str | None = None
    target_bundle_id_suffix: str | None = None
    redis_message_id_suffix: str | None = None
    events_seen: int = 0
    published_count: int = 0
    event_outbox_status_updated: bool = False
    job_attempts_written_count: int = 0
    queue_name: str | None = None
    stage_name: str | None = None
    selected_event_status: str | None = None
    selected_event_type: str | None = None
    selected_aggregate_type: str | None = None
    judge_run_status: str | None = None
    payload_has_judge_run_id: bool = False
    payload_has_bundle_id: bool = False
    payload_has_model: bool = False
    payload_has_reasoning_effort: bool = False
    payload_has_prompt_version: bool = False
    payload_has_prompt_cache_key: bool = False
    payload_judge_run_id_matches_aggregate: bool = False
    judge_run_found: bool = False
    judge_run_pending: bool = False
    judge_run_bundle_matches_payload: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "selector_type": self.selector_type,
            "target_trigger_event_id_suffix": self.target_trigger_event_id_suffix,
            "target_judge_run_id_suffix": self.target_judge_run_id_suffix,
            "target_bundle_id_suffix": self.target_bundle_id_suffix,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "events_seen": self.events_seen,
            "published_count": self.published_count,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "event_outbox_status_updated": self.event_outbox_status_updated,
            "job_attempts_written_count": self.job_attempts_written_count,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "database_read_allowed": self.config.allow_database_read,
                "redis_publish_allowed": self.config.allow_redis_publish,
                "database_write_allowed": self.config.allow_database_write,
                "max_events": self.config.max_events,
            },
            "selected_event_status": self.selected_event_status,
            "selected_event_type": self.selected_event_type,
            "selected_aggregate_type": self.selected_aggregate_type,
            "judge_run_status": self.judge_run_status,
            "payload_has_judge_run_id": self.payload_has_judge_run_id,
            "payload_has_bundle_id": self.payload_has_bundle_id,
            "payload_has_model": self.payload_has_model,
            "payload_has_reasoning_effort": self.payload_has_reasoning_effort,
            "payload_has_prompt_version": self.payload_has_prompt_version,
            "payload_has_prompt_cache_key": self.payload_has_prompt_cache_key,
            "payload_judge_run_id_matches_aggregate": self.payload_judge_run_id_matches_aggregate,
            "judge_run_found": self.judge_run_found,
            "judge_run_pending": self.judge_run_pending,
            "judge_run_bundle_matches_payload": self.judge_run_bundle_matches_payload,
            "side_effects": {
                "redis_mutation": self.published_count > 0,
                "db_write": self.state.database_write_attempted,
                "queue_consume_called": False,
                "openai_called": False,
                "judge_openai_called": False,
                "analysis_validator_called": False,
                "policy_called": False,
                "notifier_called": False,
                "telegram_send_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "subprocess_called": False,
                "analysis_router_called": False,
                "evidence_assembler_called": False,
                "normalizer_called": False,
                "enricher_called": False,
                "redis_consume_called": False,
            },
            "redactions_applied": {
                "full_trigger_event_id_omitted": True,
                "full_judge_run_id_omitted": True,
                "full_bundle_id_omitted": True,
                "idempotency_key_omitted": True,
                "payload_json_omitted": True,
                "prompt_cache_key_value_omitted": True,
                "prompt_material_omitted": True,
                "bundle_data_omitted": True,
                "raw_text_omitted": True,
                "model_output_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "redis_message_id_truncated": True,
                "exception_detail_omitted": True,
            },
        }


class SqlAlchemyBoundedJudgeCallRequestedOutboxRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._relay_repository = OutboxRelayRepository(session)

    async def fetch_target_events(
        self,
        *,
        trigger_event_id: UUID | None,
        trigger_event_suffix: str | None,
        limit: int,
    ) -> list[OutboxEventRow]:
        if trigger_event_id is not None:
            statement = _SELECT_TARGET_EVENT + """
                WHERE event_id = CAST(:event_id AS uuid)
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            params: dict[str, Any] = {"event_id": str(trigger_event_id), "limit": limit}
        elif trigger_event_suffix is not None:
            statement = _SELECT_TARGET_EVENT + """
                WHERE lower(CAST(event_id AS text)) LIKE :event_suffix_pattern
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            params = {"event_suffix_pattern": f"%{trigger_event_suffix.lower()}", "limit": limit}
        else:  # pragma: no cover - guarded before repository calls
            raise BoundedJudgeCallRequestedOutboxPublishError("target_missing")

        result = await self._session.execute(_sql(statement), params)
        return [_row_from_mapping(row) for row in result.mappings().all()]

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunLocatorRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT judge_run_id, bundle_id, status
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunLocatorRecord(
            judge_run_id=UUID(str(row["judge_run_id"])),
            bundle_id=UUID(str(row["bundle_id"])),
            status=str(row["status"]),
        )

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        await self._relay_repository.mark_published(event_id=event_id, published_at=published_at)

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


def load_bounded_judge_call_requested_publish_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedJudgeCallRequestedPublishRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedJudgeCallRequestedOutboxPublishError("database_url_missing")
    if not redis_url:
        raise BoundedJudgeCallRequestedOutboxPublishError("redis_url_missing")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError as exc:
        raise BoundedJudgeCallRequestedOutboxPublishError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedJudgeCallRequestedOutboxPublishError("runtime_config_error")
    return BoundedJudgeCallRequestedPublishRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        xadd_maxlen=xadd_maxlen,
    )


async def build_default_bounded_judge_call_requested_repository(
    runtime_config: BoundedJudgeCallRequestedPublishRuntimeConfig,
    state: BoundedJudgeCallRequestedOutboxPublishState,
    logger: logging.Logger,
) -> BoundedJudgeCallRequestedRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedJudgeCallRequestedOutboxRepository(session)

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedJudgeCallRequestedRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_judge_call_requested_redis_publisher(
    runtime_config: BoundedJudgeCallRequestedPublishRuntimeConfig,
    state: BoundedJudgeCallRequestedOutboxPublishState,
    logger: logging.Logger,
) -> BoundedJudgeCallRequestedRedisPublisherHandle:
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

    return BoundedJudgeCallRequestedRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_judge_call_requested_outbox_publish(
    config: BoundedJudgeCallRequestedOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeCallRequestedPublishRuntimeConfig] = (
        load_bounded_judge_call_requested_publish_runtime_config
    ),
    repository_builder: BoundedJudgeCallRequestedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeCallRequestedRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeCallRequestedOutboxPublishResult:
    state = BoundedJudgeCallRequestedOutboxPublishState()
    selector_count = _selector_count(config)
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if selector_count != 1:
        error_code = "target_missing" if selector_count == 0 else "target_conflict"
        return _result("blocked", error_code, config=config, state=state)
    if config.trigger_event_suffix is not None and not _is_valid_event_suffix(config.trigger_event_suffix):
        return _result("blocked", "invalid_trigger_event_suffix", config=config, state=state)
    if config.max_events != HARD_MAX_EVENTS:
        return _result("blocked", "max_events_must_be_one", config=config, state=state)
    if not config.allow_runtime_config:
        return _result("blocked", "runtime_config_not_allowed", config=config, state=state)
    if not config.allow_database_read:
        return _result("blocked", "database_read_not_allowed", config=config, state=state)
    if not config.allow_redis_publish:
        return _result("blocked", "redis_publish_not_allowed", config=config, state=state)
    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedJudgeCallRequestedOutboxPublishError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    repository_handle: BoundedJudgeCallRequestedRepositoryHandle | None = None
    publisher_handle: BoundedJudgeCallRequestedRedisPublisherHandle | None = None
    commit_repository = False
    result: BoundedJudgeCallRequestedOutboxPublishResult | None = None
    try:
        repository_handle = await (repository_builder or build_default_bounded_judge_call_requested_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        rows = await repository.fetch_target_events(
            trigger_event_id=config.trigger_event_id,
            trigger_event_suffix=config.trigger_event_suffix,
            limit=HARD_MAX_EVENTS + 1,
        )
        events_seen = len(rows)
        if events_seen == 0:
            result = _result("blocked", "target_event_not_found", config=config, state=state)
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
        payload_flags = _payload_presence_flags(row.payload_json)
        payload_judge_run_id = _payload_uuid(row.payload_json, "judge_run_id")
        payload_bundle_id = _payload_uuid(row.payload_json, "bundle_id")
        judge_run_id_matches = payload_judge_run_id == row.aggregate_id if payload_judge_run_id else False
        if row.event_type != EVENT_TYPE:
            result = _result(
                "blocked",
                "wrong_event_type",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=judge_run_id_matches,
                payload_bundle_id=payload_bundle_id,
            )
            raise _PublishResultReady
        if row.aggregate_type != ROOT_OBJECT_TYPE:
            result = _result(
                "blocked",
                "wrong_aggregate_type",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=judge_run_id_matches,
                payload_bundle_id=payload_bundle_id,
            )
            raise _PublishResultReady
        if not all(payload_flags.values()) or payload_judge_run_id is None or payload_bundle_id is None:
            result = _result(
                "blocked",
                "malformed_event_payload",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=judge_run_id_matches,
                payload_bundle_id=payload_bundle_id,
            )
            raise _PublishResultReady
        if not judge_run_id_matches:
            result = _result(
                "blocked",
                "judge_run_id_mismatch",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=False,
                payload_bundle_id=payload_bundle_id,
            )
            raise _PublishResultReady
        if row.status == "published":
            result = _result(
                "already_published",
                None,
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
            )
            raise _PublishResultReady
        if row.status != "pending":
            result = _result(
                "blocked",
                "target_event_not_pending",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
            )
            raise _PublishResultReady

        judge_run = await repository.load_judge_run(row.aggregate_id)
        if judge_run is None:
            result = _result(
                "blocked",
                "judge_run_missing",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
            )
            raise _PublishResultReady
        if judge_run.status != "pending":
            result = _result(
                "blocked",
                "judge_run_not_pending",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
                judge_run=judge_run,
            )
            raise _PublishResultReady
        if judge_run.bundle_id != payload_bundle_id:
            result = _result(
                "blocked",
                "judge_run_bundle_mismatch",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
                judge_run=judge_run,
            )
            raise _PublishResultReady

        try:
            canonical_route = OutboxRouteResolver().resolve(row)
            route = route_resolver.resolve(row) if route_resolver is not None else canonical_route
        except UnsupportedOutboxEventTypeError:
            result = _result(
                "blocked",
                "unsupported_event_type",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
                judge_run=judge_run,
            )
            raise _PublishResultReady
        if route != canonical_route or route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            result = _result(
                "blocked",
                "route_not_allowed",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
                judge_run=judge_run,
            )
            raise _PublishResultReady

        publisher_handle = await (redis_publisher_builder or build_default_bounded_judge_call_requested_redis_publisher)(
            runtime_config,
            state,
            effective_logger,
        )
        message = _build_stream_message(row, route)
        try:
            state.redis_publish_attempted = True
            redis_message_id = await publisher_handle.publisher.publish(route, message)
        except Exception as exc:
            result = _result(
                "failed",
                "redis_xadd_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
                judge_run=judge_run,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
            )
            raise _PublishResultReady

        event_outbox_status_updated = False
        job_attempts_written_count = 0
        try:
            state.event_outbox_status_write_attempted = True
            await repository.mark_published(
                event_id=row.event_id,
                published_at=(clock or _utc_now)(),
            )
            event_outbox_status_updated = True
            state.job_attempt_insert_attempted = True
            await repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
            job_attempts_written_count = 1
        except Exception as exc:
            result = _result(
                "failed",
                "database_write_failed_after_redis_publish",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                published_count=1,
                event_outbox_status_updated=event_outbox_status_updated,
                job_attempts_written_count=job_attempts_written_count,
                payload_flags=payload_flags,
                payload_judge_run_id_matches=True,
                payload_bundle_id=payload_bundle_id,
                judge_run=judge_run,
                redis_message_id=redis_message_id,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
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
            published_count=1 if redis_message_id else 1,
            event_outbox_status_updated=True,
            job_attempts_written_count=1,
            payload_flags=payload_flags,
            payload_judge_run_id_matches=True,
            payload_bundle_id=payload_bundle_id,
            judge_run=judge_run,
            redis_message_id=redis_message_id,
            queue_name=route.queue_name,
            stage_name=route.stage_name,
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


def run_bounded_judge_call_requested_outbox_publish_sync(
    config: BoundedJudgeCallRequestedOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeCallRequestedPublishRuntimeConfig] = (
        load_bounded_judge_call_requested_publish_runtime_config
    ),
    repository_builder: BoundedJudgeCallRequestedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeCallRequestedRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeCallRequestedOutboxPublishResult:
    return asyncio.run(
        run_bounded_judge_call_requested_outbox_publish(
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
        config=BoundedJudgeCallRequestedOutboxPublishConfig(),
        state=BoundedJudgeCallRequestedOutboxPublishState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedJudgeCallRequestedOutboxPublishConfig,
    state: BoundedJudgeCallRequestedOutboxPublishState,
    error_class: str | None = None,
    selected_event: OutboxEventRow | None = None,
    events_seen: int = 0,
    published_count: int = 0,
    event_outbox_status_updated: bool = False,
    job_attempts_written_count: int = 0,
    payload_flags: Mapping[str, bool] | None = None,
    payload_judge_run_id_matches: bool = False,
    payload_bundle_id: UUID | None = None,
    judge_run: JudgeRunLocatorRecord | None = None,
    redis_message_id: str | None = None,
    queue_name: str | None = None,
    stage_name: str | None = None,
) -> BoundedJudgeCallRequestedOutboxPublishResult:
    flags = dict(payload_flags or {})
    selected_event_id = selected_event.event_id if selected_event is not None else config.trigger_event_id
    selected_judge_run_id = (
        selected_event.aggregate_id
        if selected_event is not None and selected_event.aggregate_type == ROOT_OBJECT_TYPE
        else None
    )
    return BoundedJudgeCallRequestedOutboxPublishResult(
        status=status,
        ok=status in {"published", "already_published"} and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        selector_type=_selector_type(config),
        target_trigger_event_id_suffix=_optional_id_suffix(selected_event_id),
        target_judge_run_id_suffix=_optional_id_suffix(selected_judge_run_id),
        target_bundle_id_suffix=_optional_id_suffix(payload_bundle_id or (judge_run.bundle_id if judge_run else None)),
        redis_message_id_suffix=_redis_message_id_suffix(redis_message_id),
        events_seen=events_seen,
        published_count=published_count,
        event_outbox_status_updated=event_outbox_status_updated,
        job_attempts_written_count=job_attempts_written_count,
        queue_name=queue_name,
        stage_name=stage_name,
        selected_event_status=selected_event.status if selected_event is not None else None,
        selected_event_type=selected_event.event_type if selected_event is not None else None,
        selected_aggregate_type=selected_event.aggregate_type if selected_event is not None else None,
        judge_run_status=judge_run.status if judge_run is not None else None,
        payload_has_judge_run_id=flags.get("judge_run_id", False),
        payload_has_bundle_id=flags.get("bundle_id", False),
        payload_has_model=flags.get("model", False),
        payload_has_reasoning_effort=flags.get("reasoning_effort", False),
        payload_has_prompt_version=flags.get("prompt_version", False),
        payload_has_prompt_cache_key=flags.get("prompt_cache_key", False),
        payload_judge_run_id_matches_aggregate=payload_judge_run_id_matches,
        judge_run_found=judge_run is not None,
        judge_run_pending=judge_run.status == "pending" if judge_run is not None else False,
        judge_run_bundle_matches_payload=judge_run.bundle_id == payload_bundle_id
        if judge_run is not None and payload_bundle_id is not None
        else False,
    )


def _build_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=_payload_string(row.payload_json, "pipeline_run_id"),
        not_before=_payload_string(row.payload_json, "not_before"),
        trigger_event_id=str(row.event_id),
    )


def _payload_presence_flags(payload: Mapping[str, Any]) -> dict[str, bool]:
    return {field_name: _payload_field_present(payload.get(field_name)) for field_name in REQUIRED_PAYLOAD_FIELDS}


def _payload_field_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _payload_string(payload: Mapping[str, Any], field_name: str) -> str | None:
    value = payload.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _payload_uuid(payload: Mapping[str, Any], field_name: str) -> UUID | None:
    try:
        return UUID(str(payload.get(field_name)))
    except (TypeError, ValueError):
        return None


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


def _selector_count(config: BoundedJudgeCallRequestedOutboxPublishConfig) -> int:
    return sum(value is not None for value in (config.trigger_event_id, config.trigger_event_suffix))


def _selector_type(config: BoundedJudgeCallRequestedOutboxPublishConfig) -> str | None:
    if config.trigger_event_id is not None:
        return "trigger_event_id"
    if config.trigger_event_suffix is not None:
        return "trigger_event_suffix"
    return None


def _is_valid_event_suffix(value: str) -> bool:
    stripped = value.strip().lower()
    return 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped)


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _redis_message_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value)
    return normalized[-8:] if len(normalized) > 8 else "present"


def _repository_close_error_code(commit: bool) -> str:
    return "database_commit_failed_after_redis_publish" if commit else "repository_rollback_failed"


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default)).strip()


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


_SELECT_TARGET_EVENT = """
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
"""


__all__ = [
    "BoundedJudgeCallRequestedOutboxPublishConfig",
    "BoundedJudgeCallRequestedOutboxPublishError",
    "BoundedJudgeCallRequestedOutboxPublishResult",
    "BoundedJudgeCallRequestedOutboxPublishState",
    "BoundedJudgeCallRequestedPublishRuntimeConfig",
    "BoundedJudgeCallRequestedRedisPublisherBuilder",
    "BoundedJudgeCallRequestedRedisPublisherHandle",
    "BoundedJudgeCallRequestedRepositoryBuilder",
    "BoundedJudgeCallRequestedRepositoryHandle",
    "JudgeRunLocatorRecord",
    "REQUIRED_PAYLOAD_FIELDS",
    "argument_error_report",
    "load_bounded_judge_call_requested_publish_runtime_config",
    "render_sanitized_json",
    "run_bounded_judge_call_requested_outbox_publish",
    "run_bounded_judge_call_requested_outbox_publish_sync",
]
