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

SCHEMA_VERSION = "bounded_delivery_result_outbox_publish_v1"
RUNNER_NAME = "bounded_delivery_result_outbox_publish_runner"
MODE = "delivery_result_outbox_one_shot_publish"
EVENT_TYPE = "notification.delivery.result.v1"
ROOT_OBJECT_TYPE = "notification_plan"
QUEUE_NAME = "q.maintenance"
STAGE_NAME = "maintenance"
DEFAULT_XADD_MAXLEN = 10000
REQUIRED_PAYLOAD_FIELDS = (
    "notification_plan_id",
    "notification_delivery_record_id",
    "delivery_status",
    "attempt_count",
)
THIN_STREAM_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}


@dataclass(frozen=True, slots=True)
class BoundedDeliveryResultOutboxPublishConfig:
    operator_approved: bool = False
    target_event_id: UUID | None = None
    allow_database_read: bool = False
    allow_redis_write: bool = False
    allow_outbox_status_update: bool = False


@dataclass(frozen=True, slots=True)
class BoundedDeliveryResultPublishRuntimeConfig:
    database_url: str
    redis_url: str
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedDeliveryResultOutboxPublishState:
    database_session_opened: bool = False
    database_read_attempted: bool = False
    redis_publisher_created: bool = False
    redis_xadd_attempted: bool = False
    event_outbox_status_update_attempted: bool = False
    job_attempt_insert_attempted: bool = False

    @property
    def database_write_attempted(self) -> bool:
        return self.event_outbox_status_update_attempted or self.job_attempt_insert_attempted


class BoundedDeliveryResultOutboxPublishError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _PublishResultReady(Exception):
    pass


class BoundedDeliveryResultOutboxRepository(Protocol):
    async def fetch_event_by_id(self, *, event_id: UUID) -> OutboxEventRow | None: ...

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
class BoundedDeliveryResultRepositoryHandle:
    repository: BoundedDeliveryResultOutboxRepository
    close: Callable[[bool], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedDeliveryResultRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedDeliveryResultRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedDeliveryResultPublishRuntimeConfig,
        state: BoundedDeliveryResultOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedDeliveryResultRepositoryHandle: ...


class BoundedDeliveryResultRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedDeliveryResultPublishRuntimeConfig,
        state: BoundedDeliveryResultOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedDeliveryResultRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class DeliveryResultPayloadIdentity:
    notification_plan_id: UUID | None
    notification_delivery_record_id: UUID | None
    delivery_status: str | None
    attempt_count: int | None
    presence_flags: Mapping[str, bool]


@dataclass(frozen=True, slots=True)
class BoundedDeliveryResultOutboxPublishResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedDeliveryResultOutboxPublishConfig
    state: BoundedDeliveryResultOutboxPublishState = field(
        default_factory=BoundedDeliveryResultOutboxPublishState
    )
    target_event_id_requested: bool = False
    selected_event_present: bool = False
    selected_event_status: str | None = None
    selected_event_type: str | None = None
    selected_event_id_suffix: str | None = None
    selected_aggregate_type: str | None = None
    selected_aggregate_id_suffix: str | None = None
    payload_has_notification_plan_id: bool = False
    payload_has_notification_delivery_record_id: bool = False
    payload_has_delivery_status: bool = False
    payload_has_attempt_count: bool = False
    payload_notification_plan_id_matches_aggregate: bool = False
    payload_notification_plan_id_suffix: str | None = None
    payload_notification_delivery_record_id_suffix: str | None = None
    payload_delivery_status: str | None = None
    payload_attempt_count_present: bool = False
    queue_name: str | None = None
    stage_name: str | None = None
    redis_xadd_count: int = 0
    redis_message_id_suffix: str | None = None
    event_outbox_marked_published: bool = False
    job_attempt_inserted: bool = False
    thin_stream_fields_valid: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_event_id_requested": self.target_event_id_requested,
            "selected_event_present": self.selected_event_present,
            "selected_event_status": self.selected_event_status,
            "selected_event_type": self.selected_event_type,
            "selected_event_id_suffix": self.selected_event_id_suffix,
            "selected_aggregate_type": self.selected_aggregate_type,
            "selected_aggregate_id_suffix": self.selected_aggregate_id_suffix,
            "payload_has_notification_plan_id": self.payload_has_notification_plan_id,
            "payload_has_notification_delivery_record_id": self.payload_has_notification_delivery_record_id,
            "payload_has_delivery_status": self.payload_has_delivery_status,
            "payload_has_attempt_count": self.payload_has_attempt_count,
            "payload_notification_plan_id_matches_aggregate": self.payload_notification_plan_id_matches_aggregate,
            "payload_notification_plan_id_suffix": self.payload_notification_plan_id_suffix,
            "payload_notification_delivery_record_id_suffix": self.payload_notification_delivery_record_id_suffix,
            "payload_delivery_status": self.payload_delivery_status,
            "payload_attempt_count_present": self.payload_attempt_count_present,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "thin_stream_fields_valid": self.thin_stream_fields_valid,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "redis_xadd_attempted": self.state.redis_xadd_attempted,
            "redis_xadd_count": self.redis_xadd_count,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "event_outbox_status_update_attempted": self.state.event_outbox_status_update_attempted,
            "event_outbox_marked_published": self.event_outbox_marked_published,
            "job_attempt_insert_attempted": self.state.job_attempt_insert_attempted,
            "job_attempt_inserted": self.job_attempt_inserted,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "database_read_allowed": self.config.allow_database_read,
                "redis_write_allowed": self.config.allow_redis_write,
                "outbox_status_update_allowed": self.config.allow_outbox_status_update,
            },
            "side_effects": {
                "redis_mutation": self.redis_xadd_count > 0,
                "db_write": self.event_outbox_marked_published or self.job_attempt_inserted,
                "queue_consume_called": False,
                "redis_ack_called": False,
                "redis_claim_called": False,
                "redis_delete_called": False,
                "redis_group_create_called": False,
                "notifier_called": False,
                "telegram_send_called": False,
                "telegram_edit_called": False,
                "openai_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "subprocess_called": False,
            },
            "redactions_applied": {
                "full_event_id_omitted": True,
                "full_notification_plan_id_omitted": True,
                "full_notification_delivery_record_id_omitted": True,
                "idempotency_key_omitted": True,
                "payload_json_omitted": True,
                "telegram_response_json_omitted": True,
                "telegram_ids_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "redis_message_id_truncated": True,
                "exception_detail_omitted": True,
            },
        }


class SqlAlchemyBoundedDeliveryResultOutboxRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._relay_repository = OutboxRelayRepository(session)

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
        return None if row is None else _event_row_from_mapping(row)

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


def load_bounded_delivery_result_publish_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedDeliveryResultPublishRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedDeliveryResultOutboxPublishError("database_url_missing")
    if not redis_url:
        raise BoundedDeliveryResultOutboxPublishError("redis_url_missing")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError as exc:
        raise BoundedDeliveryResultOutboxPublishError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedDeliveryResultOutboxPublishError("runtime_config_error")
    return BoundedDeliveryResultPublishRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        xadd_maxlen=xadd_maxlen,
    )


async def build_default_bounded_delivery_result_repository(
    runtime_config: BoundedDeliveryResultPublishRuntimeConfig,
    state: BoundedDeliveryResultOutboxPublishState,
    logger: logging.Logger,
) -> BoundedDeliveryResultRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedDeliveryResultOutboxRepository(session)

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedDeliveryResultRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_delivery_result_redis_publisher(
    runtime_config: BoundedDeliveryResultPublishRuntimeConfig,
    state: BoundedDeliveryResultOutboxPublishState,
    logger: logging.Logger,
) -> BoundedDeliveryResultRedisPublisherHandle:
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

    return BoundedDeliveryResultRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_delivery_result_outbox_publish(
    config: BoundedDeliveryResultOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedDeliveryResultPublishRuntimeConfig] = (
        load_bounded_delivery_result_publish_runtime_config
    ),
    repository_builder: BoundedDeliveryResultRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedDeliveryResultRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedDeliveryResultOutboxPublishResult:
    state = BoundedDeliveryResultOutboxPublishState()
    gate_error = _gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
    except BoundedDeliveryResultOutboxPublishError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    repository_handle: BoundedDeliveryResultRepositoryHandle | None = None
    publisher_handle: BoundedDeliveryResultRedisPublisherHandle | None = None
    commit_repository = False
    result: BoundedDeliveryResultOutboxPublishResult | None = None
    try:
        repository_handle = await (repository_builder or build_default_bounded_delivery_result_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        assert config.target_event_id is not None
        row = await repository.fetch_event_by_id(event_id=config.target_event_id)
        if row is None:
            result = _result("blocked", "target_event_missing", config=config, state=state)
            raise _PublishResultReady

        identity = _payload_identity(row)
        selected_kwargs = {"selected_event": row, "identity": identity}
        target_error = _target_event_error(row, identity)
        if target_error is not None:
            result = _blocked_selected(target_error, config, state, **selected_kwargs)
            raise _PublishResultReady

        try:
            canonical_route = OutboxRouteResolver().resolve(row)
            route = route_resolver.resolve(row) if route_resolver is not None else canonical_route
        except UnsupportedOutboxEventTypeError:
            result = _blocked_selected("unsupported_event_type", config, state, **selected_kwargs)
            raise _PublishResultReady
        if route != canonical_route or route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            result = _blocked_selected("route_not_allowed", config, state, route=route, **selected_kwargs)
            raise _PublishResultReady

        message = _build_stream_message(row, route)
        if set(message.as_stream_fields()) != THIN_STREAM_FIELDS:
            result = _blocked_selected("redis_message_shape_invalid", config, state, route=route, **selected_kwargs)
            raise _PublishResultReady

        publisher_handle = await (redis_publisher_builder or build_default_bounded_delivery_result_redis_publisher)(
            runtime_config,
            state,
            effective_logger,
        )
        try:
            state.redis_xadd_attempted = True
            redis_message_id = await publisher_handle.publisher.publish(route, message)
        except Exception as exc:
            result = _result(
                "failed",
                "redis_xadd_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
                selected_event=row,
                identity=identity,
            )
            raise _PublishResultReady

        event_outbox_marked_published = False
        job_attempt_inserted = False
        try:
            state.event_outbox_status_update_attempted = True
            await repository.mark_published(event_id=row.event_id, published_at=(clock or _utc_now)())
            event_outbox_marked_published = True
            state.job_attempt_insert_attempted = True
            await repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
            job_attempt_inserted = True
        except Exception as exc:
            result = _result(
                "failed",
                "database_write_failed_after_redis_publish",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
                selected_event=row,
                identity=identity,
                redis_xadd_count=1,
                redis_message_id=redis_message_id,
                event_outbox_marked_published=event_outbox_marked_published,
                job_attempt_inserted=job_attempt_inserted,
            )
            raise _PublishResultReady

        commit_repository = True
        result = _result(
            "pass",
            None,
            config=config,
            state=state,
            queue_name=route.queue_name,
            stage_name=route.stage_name,
            selected_event=row,
            identity=identity,
            redis_xadd_count=1,
            redis_message_id=redis_message_id,
            event_outbox_marked_published=True,
            job_attempt_inserted=True,
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


def run_bounded_delivery_result_outbox_publish_sync(
    config: BoundedDeliveryResultOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedDeliveryResultPublishRuntimeConfig] = (
        load_bounded_delivery_result_publish_runtime_config
    ),
    repository_builder: BoundedDeliveryResultRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedDeliveryResultRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedDeliveryResultOutboxPublishResult:
    return asyncio.run(
        run_bounded_delivery_result_outbox_publish(
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
        config=BoundedDeliveryResultOutboxPublishConfig(),
        state=BoundedDeliveryResultOutboxPublishState(),
    ).to_sanitized_dict()


def _gate_error(config: BoundedDeliveryResultOutboxPublishConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if config.target_event_id is None:
        return "target_event_id_missing"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_redis_write:
        return "redis_write_not_allowed"
    if not config.allow_outbox_status_update:
        return "outbox_status_update_not_allowed"
    return None


def _target_event_error(row: OutboxEventRow, identity: DeliveryResultPayloadIdentity) -> str | None:
    if row.event_type != EVENT_TYPE:
        return "target_event_type_mismatch"
    if row.status != "pending":
        return "target_event_not_pending"
    if row.aggregate_type != ROOT_OBJECT_TYPE:
        return "target_aggregate_type_mismatch"
    if not all(identity.presence_flags.values()):
        return "malformed_event_payload"
    if (
        identity.notification_plan_id is None
        or identity.notification_delivery_record_id is None
        or identity.delivery_status is None
        or identity.attempt_count is None
        or identity.attempt_count < 0
        or (
            identity.attempt_count == 0
            and not _is_valid_send_disabled_zero_attempt(row, identity)
        )
    ):
        return "malformed_event_payload"
    if identity.notification_plan_id != row.aggregate_id:
        return "payload_notification_plan_id_mismatch"
    return None


def _is_valid_send_disabled_zero_attempt(
    row: OutboxEventRow,
    identity: DeliveryResultPayloadIdentity,
) -> bool:
    payload = row.payload_json if isinstance(row.payload_json, Mapping) else {}
    raw_attempt_count = payload.get("attempt_count")
    return (
        type(raw_attempt_count) is int
        and raw_attempt_count == 0
        and identity.delivery_status == "suppressed"
        and _payload_string(payload, "transport_error_code") == "notification_send_flag_disabled"
    )


def _blocked_selected(
    error_code: str,
    config: BoundedDeliveryResultOutboxPublishConfig,
    state: BoundedDeliveryResultOutboxPublishState,
    *,
    selected_event: OutboxEventRow,
    identity: DeliveryResultPayloadIdentity,
    route: QueueRoute | None = None,
) -> BoundedDeliveryResultOutboxPublishResult:
    return _result(
        "blocked",
        error_code,
        config=config,
        state=state,
        selected_event=selected_event,
        identity=identity,
        queue_name=route.queue_name if route else None,
        stage_name=route.stage_name if route else None,
    )


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedDeliveryResultOutboxPublishConfig,
    state: BoundedDeliveryResultOutboxPublishState,
    error_class: str | None = None,
    selected_event: OutboxEventRow | None = None,
    identity: DeliveryResultPayloadIdentity | None = None,
    queue_name: str | None = None,
    stage_name: str | None = None,
    redis_xadd_count: int = 0,
    redis_message_id: str | None = None,
    event_outbox_marked_published: bool = False,
    job_attempt_inserted: bool = False,
) -> BoundedDeliveryResultOutboxPublishResult:
    flags = dict(identity.presence_flags if identity is not None else {})
    ok = status == "pass" and error_code is None
    return BoundedDeliveryResultOutboxPublishResult(
        status=status,
        ok=ok,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        target_event_id_requested=config.target_event_id is not None,
        selected_event_present=selected_event is not None,
        selected_event_status=selected_event.status if selected_event is not None else None,
        selected_event_type=selected_event.event_type if selected_event is not None else None,
        selected_event_id_suffix=_optional_id_suffix(selected_event.event_id if selected_event else config.target_event_id),
        selected_aggregate_type=selected_event.aggregate_type if selected_event is not None else None,
        selected_aggregate_id_suffix=_optional_id_suffix(selected_event.aggregate_id if selected_event else None),
        payload_has_notification_plan_id=flags.get("notification_plan_id", False),
        payload_has_notification_delivery_record_id=flags.get("notification_delivery_record_id", False),
        payload_has_delivery_status=flags.get("delivery_status", False),
        payload_has_attempt_count=flags.get("attempt_count", False),
        payload_notification_plan_id_matches_aggregate=(
            bool(
                selected_event is not None
                and identity is not None
                and identity.notification_plan_id == selected_event.aggregate_id
            )
        ),
        payload_notification_plan_id_suffix=_optional_id_suffix(
            identity.notification_plan_id if identity is not None else None
        ),
        payload_notification_delivery_record_id_suffix=_optional_id_suffix(
            identity.notification_delivery_record_id if identity is not None else None
        ),
        payload_delivery_status=identity.delivery_status if identity is not None else None,
        payload_attempt_count_present=identity.attempt_count is not None if identity is not None else False,
        queue_name=queue_name,
        stage_name=stage_name,
        redis_xadd_count=redis_xadd_count,
        redis_message_id_suffix=_redis_message_id_suffix(redis_message_id),
        event_outbox_marked_published=event_outbox_marked_published,
        job_attempt_inserted=job_attempt_inserted,
        thin_stream_fields_valid=queue_name == QUEUE_NAME and stage_name == STAGE_NAME and selected_event is not None,
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


def _payload_identity(row: OutboxEventRow) -> DeliveryResultPayloadIdentity:
    payload = row.payload_json if isinstance(row.payload_json, Mapping) else {}
    flags = {field_name: _payload_field_present(payload.get(field_name)) for field_name in REQUIRED_PAYLOAD_FIELDS}
    return DeliveryResultPayloadIdentity(
        notification_plan_id=_payload_uuid(payload, "notification_plan_id"),
        notification_delivery_record_id=_payload_uuid(payload, "notification_delivery_record_id"),
        delivery_status=_payload_string(payload, "delivery_status"),
        attempt_count=_payload_int(payload, "attempt_count"),
        presence_flags=flags,
    )


def _payload_field_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _payload_uuid(payload: Mapping[str, Any], field_name: str) -> UUID | None:
    try:
        return UUID(str(payload.get(field_name)))
    except (TypeError, ValueError, AttributeError):
        return None


def _payload_string(payload: Mapping[str, Any], field_name: str) -> str | None:
    value = payload.get(field_name)
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _payload_int(payload: Mapping[str, Any], field_name: str) -> int | None:
    value = payload.get(field_name)
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed


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


def _env_value(source: Mapping[str, str], key: str, default: str | None = None) -> str | None:
    value = source.get(key, default)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _optional_id_suffix(value: UUID | None) -> str | None:
    if value is None:
        return None
    return str(value).replace("-", "")[-8:]


def _redis_message_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    return cleaned[-8:] if len(cleaned) > 8 else cleaned


def _safe_exception_class(exc: BaseException) -> str:
    text = exc.__class__.__name__
    return text if text.replace("_", "").isalnum() else "Exception"


def _repository_close_error_code(commit_repository: bool) -> str:
    if commit_repository:
        return "database_commit_failed_after_redis_publish"
    return "database_rollback_failed"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedDeliveryResultOutboxPublishConfig",
    "BoundedDeliveryResultOutboxPublishError",
    "BoundedDeliveryResultOutboxPublishResult",
    "BoundedDeliveryResultPublishRuntimeConfig",
    "BoundedDeliveryResultRepositoryBuilder",
    "BoundedDeliveryResultRepositoryHandle",
    "BoundedDeliveryResultRedisPublisherBuilder",
    "BoundedDeliveryResultRedisPublisherHandle",
    "EVENT_TYPE",
    "MODE",
    "QUEUE_NAME",
    "REQUIRED_PAYLOAD_FIELDS",
    "ROOT_OBJECT_TYPE",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "STAGE_NAME",
    "SqlAlchemyBoundedDeliveryResultOutboxRepository",
    "argument_error_report",
    "build_default_bounded_delivery_result_redis_publisher",
    "build_default_bounded_delivery_result_repository",
    "load_bounded_delivery_result_publish_runtime_config",
    "render_sanitized_json",
    "run_bounded_delivery_result_outbox_publish",
    "run_bounded_delivery_result_outbox_publish_sync",
]
