from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
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
from .routing import OutboxRouteResolver

SCHEMA_VERSION = "bounded_notification_plan_outbox_publish_v1"
RUNNER_NAME = "bounded_notification_plan_outbox_publish_runner"
MODE = "notification_plan_outbox_one_shot_publish"
EVENT_TYPE = "notification.plan.created.v1"
QUEUE_NAME = "q.notification.send"
STAGE_NAME = "notify"
DEFAULT_XADD_MAXLEN = 10000
REQUIRED_PAYLOAD_FIELDS = (
    "notification_plan_id",
    "analysis_id",
    "candidate_group_id",
    "target_chat_id",
    "material_change_hash",
)


@dataclass(frozen=True, slots=True)
class BoundedNotificationPlanOutboxPublishConfig:
    operator_approved: bool = False
    allow_database_read: bool = False
    allow_redis_write: bool = False
    allow_outbox_status_update: bool = False
    expected_pending_count: int = 1
    target_event_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class BoundedNotificationPlanPublishRuntimeConfig:
    database_url: str
    redis_url: str
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedNotificationPlanOutboxPublishState:
    database_session_opened: bool = False
    database_read_attempted: bool = False
    redis_publisher_created: bool = False
    redis_xadd_attempted: bool = False
    event_outbox_status_update_attempted: bool = False
    job_attempt_insert_attempted: bool = False


class BoundedNotificationPlanOutboxPublishError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class NotificationPlanSendabilityRow:
    notification_plan_id: UUID
    delivery_decision: str
    status: str


class BoundedNotificationPlanOutboxRepository(Protocol):
    async def count_pending_events(self, *, event_type: str) -> int: ...
    async def fetch_oldest_pending_event(self, *, event_type: str) -> OutboxEventRow | None: ...
    async def fetch_event_by_id(self, *, event_id: UUID) -> OutboxEventRow | None: ...
    async def load_notification_plan_sendability(
        self,
        *,
        notification_plan_id: UUID,
    ) -> NotificationPlanSendabilityRow | None: ...
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
class BoundedNotificationPlanRepositoryHandle:
    repository: BoundedNotificationPlanOutboxRepository
    close: Callable[[bool], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedNotificationPlanRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedNotificationPlanRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotificationPlanPublishRuntimeConfig,
        state: BoundedNotificationPlanOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedNotificationPlanRepositoryHandle: ...


class BoundedNotificationPlanRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotificationPlanPublishRuntimeConfig,
        state: BoundedNotificationPlanOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedNotificationPlanRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedNotificationPlanOutboxPublishResult:
    status: str
    ok: bool
    error_code: str | None
    operator_approved: bool
    database_read_allowed: bool
    redis_write_allowed: bool
    outbox_status_update_allowed: bool
    queue_name: str = QUEUE_NAME
    target_event_id_requested: bool = False
    pending_count_observed: int | None = None
    selected_event_present: bool = False
    selected_event_status: str | None = None
    selected_event_id_suffix: str | None = None
    selected_aggregate_type: str | None = None
    selected_aggregate_id_suffix: str | None = None
    payload_has_notification_plan_id: bool = False
    payload_has_analysis_id: bool = False
    payload_has_candidate_group_id: bool = False
    payload_has_target_chat_id: bool = False
    payload_has_material_change_hash: bool = False
    target_notification_plan_present: bool = False
    target_notification_plan_status: str | None = None
    target_notification_plan_delivery_decision: str | None = None
    redis_xadd_count: int = 0
    redis_message_id_present: bool = False
    event_outbox_marked_published: bool = False
    job_attempt_inserted: bool = False
    state: BoundedNotificationPlanOutboxPublishState = field(default_factory=BoundedNotificationPlanOutboxPublishState)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "operator_approved": self.operator_approved,
            "database_read_allowed": self.database_read_allowed,
            "redis_write_allowed": self.redis_write_allowed,
            "outbox_status_update_allowed": self.outbox_status_update_allowed,
            "queue_name": self.queue_name,
            "target_event_id_requested": self.target_event_id_requested,
            "database_read_attempted": self.state.database_read_attempted,
            "pending_count_observed": self.pending_count_observed,
            "selected_event_present": self.selected_event_present,
            "selected_event_status": self.selected_event_status,
            "selected_event_id_suffix": self.selected_event_id_suffix,
            "selected_aggregate_type": self.selected_aggregate_type,
            "selected_aggregate_id_suffix": self.selected_aggregate_id_suffix,
            "payload_has_notification_plan_id": self.payload_has_notification_plan_id,
            "payload_has_analysis_id": self.payload_has_analysis_id,
            "payload_has_candidate_group_id": self.payload_has_candidate_group_id,
            "payload_has_target_chat_id": self.payload_has_target_chat_id,
            "payload_has_material_change_hash": self.payload_has_material_change_hash,
            "target_notification_plan_present": self.target_notification_plan_present,
            "target_notification_plan_status": self.target_notification_plan_status,
            "target_notification_plan_delivery_decision": self.target_notification_plan_delivery_decision,
            "redis_write_attempted": self.state.redis_xadd_attempted,
            "redis_xadd_attempted": self.state.redis_xadd_attempted,
            "redis_xadd_count": self.redis_xadd_count,
            "redis_message_id_present": self.redis_message_id_present,
            "event_outbox_status_update_attempted": self.state.event_outbox_status_update_attempted,
            "event_outbox_marked_published": self.event_outbox_marked_published,
            "job_attempt_inserted": self.job_attempt_inserted,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "redactions_applied": [
                "full_event_id_omitted",
                "full_aggregate_id_omitted",
                "payload_json_omitted",
                "database_url_omitted",
                "redis_url_omitted",
                "redis_message_id_omitted",
                "telegram_token_omitted",
                "exception_detail_omitted",
            ],
            "side_effects": {
                "db_write": self.event_outbox_marked_published or self.job_attempt_inserted,
                "redis_mutation": self.redis_xadd_count > 0,
                "telegram_send_called": False,
                "telegram_edit_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "openai_called": False,
                "github_called": False,
                "x_called": False,
                "web_called": False,
            },
        }


class SqlAlchemyBoundedNotificationPlanOutboxRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._relay_repository = OutboxRelayRepository(session)

    async def count_pending_events(self, *, event_type: str) -> int:
        result = await self._session.execute(
            _sql(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE status = 'pending'::outbox_status_enum
                  AND event_type = :event_type
                """
            ),
            {"event_type": event_type},
        )
        return int(result.scalar_one())

    async def fetch_oldest_pending_event(self, *, event_type: str) -> OutboxEventRow | None:
        result = await self._session.execute(
            _sql(
                """
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
                  AND event_type = :event_type
                ORDER BY created_at ASC, event_id ASC
                LIMIT 1
                """
            ),
            {"event_type": event_type},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return _event_row_from_mapping(row)

    async def fetch_event_by_id(self, *, event_id: UUID) -> OutboxEventRow | None:
        result = await self._session.execute(
            _sql(
                """
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
                LIMIT 1
                """
            ),
            {"event_id": str(event_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return _event_row_from_mapping(row)

    async def load_notification_plan_sendability(
        self,
        *,
        notification_plan_id: UUID,
    ) -> NotificationPlanSendabilityRow | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT
                    notification_plan_id,
                    delivery_decision::text AS delivery_decision,
                    status::text AS status
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return NotificationPlanSendabilityRow(
            notification_plan_id=UUID(str(row["notification_plan_id"])),
            delivery_decision=str(row["delivery_decision"]),
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


def load_bounded_notification_plan_publish_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedNotificationPlanPublishRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedNotificationPlanOutboxPublishError("database_url_missing")
    if not redis_url:
        raise BoundedNotificationPlanOutboxPublishError("redis_url_missing")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedNotificationPlanOutboxPublishError("runtime_config_error")
    return BoundedNotificationPlanPublishRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        xadd_maxlen=xadd_maxlen,
    )


async def build_default_bounded_notification_plan_repository(
    runtime_config: BoundedNotificationPlanPublishRuntimeConfig,
    state: BoundedNotificationPlanOutboxPublishState,
    logger: logging.Logger,
) -> BoundedNotificationPlanRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedNotificationPlanOutboxRepository(session)

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedNotificationPlanRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_notification_plan_redis_publisher(
    runtime_config: BoundedNotificationPlanPublishRuntimeConfig,
    state: BoundedNotificationPlanOutboxPublishState,
    logger: logging.Logger,
) -> BoundedNotificationPlanRedisPublisherHandle:
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

    return BoundedNotificationPlanRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_notification_plan_outbox_publish(
    config: BoundedNotificationPlanOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedNotificationPlanPublishRuntimeConfig] = (
        load_bounded_notification_plan_publish_runtime_config
    ),
    repository_builder: BoundedNotificationPlanRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedNotificationPlanRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationPlanOutboxPublishResult:
    state = BoundedNotificationPlanOutboxPublishState()
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if not config.allow_database_read:
        return _result("blocked", "database_read_not_allowed", config=config, state=state)
    if config.expected_pending_count <= 0:
        return _result("blocked", "expected_pending_count_invalid", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
    except BoundedNotificationPlanOutboxPublishError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    repository_handle: BoundedNotificationPlanRepositoryHandle | None = None
    publisher_handle: BoundedNotificationPlanRedisPublisherHandle | None = None
    commit_repository = False
    try:
        repository_handle = await (repository_builder or build_default_bounded_notification_plan_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        pending_count = await repository.count_pending_events(event_type=EVENT_TYPE)
        if config.target_event_id is None:
            if pending_count != config.expected_pending_count:
                return _result(
                    "blocked",
                    "pending_count_mismatch",
                    config=config,
                    state=state,
                    pending_count_observed=pending_count,
                )
            if pending_count != 1:
                return _result(
                    "blocked",
                    "pending_count_not_one",
                    config=config,
                    state=state,
                    pending_count_observed=pending_count,
                )
            row = await repository.fetch_oldest_pending_event(event_type=EVENT_TYPE)
        else:
            row = await repository.fetch_event_by_id(event_id=config.target_event_id)

        if row is None:
            return _result(
                "blocked",
                "target_event_missing" if config.target_event_id is not None else "selected_event_missing",
                config=config,
                state=state,
                pending_count_observed=pending_count,
            )
        if config.target_event_id is not None:
            target_event_error = _target_event_error(row)
            if target_event_error is not None:
                return _result(
                    "blocked",
                    target_event_error,
                    config=config,
                    state=state,
                    pending_count_observed=pending_count,
                    selected_event=row,
                )

        payload_flags = _payload_presence_flags(row.payload_json)
        if not all(payload_flags.values()):
            return _result(
                "blocked",
                "malformed_event_payload",
                config=config,
                state=state,
                pending_count_observed=pending_count,
                selected_event=row,
                payload_flags=payload_flags,
            )

        target_plan: NotificationPlanSendabilityRow | None = None
        if config.target_event_id is not None:
            notification_plan_id = _notification_plan_id_from_payload(row.payload_json)
            if notification_plan_id is None:
                return _result(
                    "blocked",
                    "target_notification_plan_id_invalid",
                    config=config,
                    state=state,
                    pending_count_observed=pending_count,
                    selected_event=row,
                    payload_flags=payload_flags,
                )
            target_plan = await repository.load_notification_plan_sendability(
                notification_plan_id=notification_plan_id,
            )
            plan_error = _target_notification_plan_error(target_plan)
            if plan_error is not None:
                return _result(
                    "blocked",
                    plan_error,
                    config=config,
                    state=state,
                    pending_count_observed=pending_count,
                    selected_event=row,
                    payload_flags=payload_flags,
                    target_plan=target_plan,
                )

        route = (route_resolver or OutboxRouteResolver()).resolve(row)
        if route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            return _result(
                "blocked",
                "route_not_allowed",
                config=config,
                state=state,
                pending_count_observed=pending_count,
                selected_event=row,
                payload_flags=payload_flags,
                target_plan=target_plan,
            )
        if not config.allow_redis_write:
            return _result(
                "blocked",
                "redis_write_not_allowed",
                config=config,
                state=state,
                pending_count_observed=pending_count,
                selected_event=row,
                payload_flags=payload_flags,
                target_plan=target_plan,
            )

        publisher_handle = await (redis_publisher_builder or build_default_bounded_notification_plan_redis_publisher)(
            runtime_config,
            state,
            effective_logger,
        )
        message = _build_stream_message(row, route)
        try:
            state.redis_xadd_attempted = True
            redis_message_id = await publisher_handle.publisher.publish(route, message)
        except Exception:
            return _result(
                "failed",
                "redis_xadd_failed",
                config=config,
                state=state,
                pending_count_observed=pending_count,
                selected_event=row,
                payload_flags=payload_flags,
                target_plan=target_plan,
            )

        if not config.allow_outbox_status_update:
            return _result(
                "blocked",
                "outbox_status_update_not_allowed",
                config=config,
                state=state,
                pending_count_observed=pending_count,
                selected_event=row,
                payload_flags=payload_flags,
                target_plan=target_plan,
                redis_xadd_count=1,
                redis_message_id_present=bool(redis_message_id),
            )

        try:
            state.event_outbox_status_update_attempted = True
            await repository.mark_published(
                event_id=row.event_id,
                published_at=(clock or _utc_now)(),
            )
        except Exception:
            return _result(
                "failed",
                "event_outbox_status_update_failed",
                config=config,
                state=state,
                pending_count_observed=pending_count,
                selected_event=row,
                payload_flags=payload_flags,
                target_plan=target_plan,
                redis_xadd_count=1,
                redis_message_id_present=bool(redis_message_id),
            )

        try:
            state.job_attempt_insert_attempted = True
            await repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
        except Exception:
            return _result(
                "failed",
                "job_attempt_insert_failed",
                config=config,
                state=state,
                pending_count_observed=pending_count,
                selected_event=row,
                payload_flags=payload_flags,
                target_plan=target_plan,
                redis_xadd_count=1,
                redis_message_id_present=bool(redis_message_id),
            )

        commit_repository = True
        return _result(
            "pass",
            None,
            config=config,
            state=state,
            pending_count_observed=pending_count,
            selected_event=row,
            payload_flags=payload_flags,
            target_plan=target_plan,
            redis_xadd_count=1,
            redis_message_id_present=bool(redis_message_id),
            event_outbox_marked_published=True,
            job_attempt_inserted=True,
        )
    except Exception:
        return _result("failed", "bounded_publish_failed", config=config, state=state)
    finally:
        if publisher_handle is not None:
            try:
                await publisher_handle.close()
            except Exception:
                pass
        if repository_handle is not None:
            try:
                await repository_handle.close(commit_repository)
            except Exception:
                pass


def run_bounded_notification_plan_outbox_publish_sync(
    config: BoundedNotificationPlanOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedNotificationPlanPublishRuntimeConfig] = (
        load_bounded_notification_plan_publish_runtime_config
    ),
    repository_builder: BoundedNotificationPlanRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedNotificationPlanRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationPlanOutboxPublishResult:
    return asyncio.run(
        run_bounded_notification_plan_outbox_publish(
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
        config=BoundedNotificationPlanOutboxPublishConfig(),
        state=BoundedNotificationPlanOutboxPublishState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotificationPlanOutboxPublishConfig,
    state: BoundedNotificationPlanOutboxPublishState,
    pending_count_observed: int | None = None,
    selected_event: OutboxEventRow | None = None,
    payload_flags: Mapping[str, bool] | None = None,
    target_plan: NotificationPlanSendabilityRow | None = None,
    redis_xadd_count: int = 0,
    redis_message_id_present: bool = False,
    event_outbox_marked_published: bool = False,
    job_attempt_inserted: bool = False,
) -> BoundedNotificationPlanOutboxPublishResult:
    flags = dict(payload_flags or {})
    selected = _selected_summary(selected_event)
    return BoundedNotificationPlanOutboxPublishResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        operator_approved=config.operator_approved,
        database_read_allowed=config.allow_database_read,
        redis_write_allowed=config.allow_redis_write,
        outbox_status_update_allowed=config.allow_outbox_status_update,
        target_event_id_requested=config.target_event_id is not None,
        pending_count_observed=pending_count_observed,
        selected_event_present=selected["present"],
        selected_event_status=selected["status"],
        selected_event_id_suffix=selected["event_id_suffix"],
        selected_aggregate_type=selected["aggregate_type"],
        selected_aggregate_id_suffix=selected["aggregate_id_suffix"],
        payload_has_notification_plan_id=flags.get("notification_plan_id", False),
        payload_has_analysis_id=flags.get("analysis_id", False),
        payload_has_candidate_group_id=flags.get("candidate_group_id", False),
        payload_has_target_chat_id=flags.get("target_chat_id", False),
        payload_has_material_change_hash=flags.get("material_change_hash", False),
        target_notification_plan_present=target_plan is not None,
        target_notification_plan_status=target_plan.status if target_plan is not None else None,
        target_notification_plan_delivery_decision=(
            target_plan.delivery_decision if target_plan is not None else None
        ),
        redis_xadd_count=redis_xadd_count,
        redis_message_id_present=redis_message_id_present,
        event_outbox_marked_published=event_outbox_marked_published,
        job_attempt_inserted=job_attempt_inserted,
        state=state,
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


def _payload_presence_flags(payload: Mapping[str, Any]) -> dict[str, bool]:
    return {field_name: _payload_field_present(payload.get(field_name)) for field_name in REQUIRED_PAYLOAD_FIELDS}


def _payload_field_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _notification_plan_id_from_payload(payload: Mapping[str, Any]) -> UUID | None:
    value = payload.get("notification_plan_id")
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _target_event_error(row: OutboxEventRow) -> str | None:
    if row.event_type != EVENT_TYPE:
        return "target_event_type_mismatch"
    if row.status != "pending":
        return "target_event_not_pending"
    return None


def _target_notification_plan_error(row: NotificationPlanSendabilityRow | None) -> str | None:
    if row is None:
        return "target_notification_plan_missing"
    if row.delivery_decision != "send_now":
        return "target_notification_plan_not_send_now"
    if row.status in {"sent", "edited", "suppressed", "failed_terminal"}:
        return "target_notification_plan_terminal"
    if row.status != "planned":
        return "target_notification_plan_not_sendable"
    return None


def _selected_summary(row: OutboxEventRow | None) -> dict[str, Any]:
    if row is None:
        return {
            "present": False,
            "status": None,
            "event_id_suffix": None,
            "aggregate_type": None,
            "aggregate_id_suffix": None,
        }
    return {
        "present": True,
        "status": row.status,
        "event_id_suffix": _id_suffix(row.event_id),
        "aggregate_type": row.aggregate_type,
        "aggregate_id_suffix": _id_suffix(row.aggregate_id),
    }


def _id_suffix(value: UUID | str) -> str:
    return str(value)[-8:]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _event_row_from_mapping(row: Mapping[str, Any]) -> OutboxEventRow:
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


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedNotificationPlanOutboxPublishConfig",
    "BoundedNotificationPlanOutboxPublishError",
    "BoundedNotificationPlanOutboxPublishResult",
    "BoundedNotificationPlanPublishRuntimeConfig",
    "BoundedNotificationPlanRepositoryBuilder",
    "BoundedNotificationPlanRepositoryHandle",
    "BoundedNotificationPlanRedisPublisherBuilder",
    "BoundedNotificationPlanRedisPublisherHandle",
    "EVENT_TYPE",
    "MODE",
    "NotificationPlanSendabilityRow",
    "QUEUE_NAME",
    "REQUIRED_PAYLOAD_FIELDS",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "SqlAlchemyBoundedNotificationPlanOutboxRepository",
    "argument_error_report",
    "build_default_bounded_notification_plan_redis_publisher",
    "build_default_bounded_notification_plan_repository",
    "load_bounded_notification_plan_publish_runtime_config",
    "render_sanitized_json",
    "run_bounded_notification_plan_outbox_publish",
    "run_bounded_notification_plan_outbox_publish_sync",
]
