from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Protocol
from uuid import UUID

from ..outbox_relay.models import OutboxEventRow
from ..outbox_relay.redis_streams import RedisStreamsPublisher
from ..outbox_relay.routing import OutboxRouteResolver
from .bounded_notification_send_dry_run_runner import (
    DEFAULT_BLOCK_MS,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_XADD_MAXLEN,
    MAINTENANCE_QUEUE_NAME,
    MAINTENANCE_STAGE_NAME,
    QUEUE_NAME,
    REQUIRED_THIN_QUEUE_FIELDS,
    STAGE_NAME,
    NotificationDurableReadback,
    NotificationSendContext,
    RedisExactNotificationSendConsumer,
    RedisTargetSelection,
    _build_stream_message,
    _durable_readback_from_raw,
    _durable_readback_report,
    _env_value,
    _float_env,
    _int_env,
    _message_suffix,
    _outbox_event_row,
    _safe_exception_class,
    _string_or_none,
    _target_suffix_error,
    _utc_now,
    _uuid_or_none,
    _uuid_suffix,
)
from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .models import DeliveryResult, StreamMessage
from .renderer import NotificationRenderer
from .repositories import NotifierTelegramRepository
from .service import NotifierTelegramService
from .transport import (
    StateTrackingTelegramTransport,
    TelegramBotApiTransport,
    TelegramTransport,
    TelegramTransportConstructionError,
)

SCHEMA_VERSION = "bounded_notification_send_live_runner_v1"
RUNNER_NAME = "bounded_notification_send_live_runner"
DEFAULT_CONSUMER_NAME = RUNNER_NAME


@dataclass(frozen=True, slots=True)
class BoundedNotificationSendLiveConfig:
    mode: str = "preview"
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    allow_redis_read: bool = False
    allow_redis_consume: bool = False
    allow_redis_ack: bool = False
    allow_maintenance_publish: bool = False
    allow_render_write: bool = False
    allow_delivery_record_write: bool = False
    allow_delivery_result_outbox_write: bool = False
    allow_telegram_transport: bool = False
    allow_telegram_send: bool = False
    trigger_event_suffix: str | None = None
    notification_plan_id_suffix: str | None = None
    analysis_id_suffix: str | None = None
    target_chat_id_suffix: str | None = None
    redis_message_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedNotificationSendLiveRuntimeConfig:
    notifier_config: NotifierTelegramConfig
    redis_url: str
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    consumer_name: str = DEFAULT_CONSUMER_NAME
    block_ms: int = DEFAULT_BLOCK_MS
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedNotificationSendLiveState:
    runtime_config_loaded: bool = False
    redis_client_created: bool = False
    redis_read_attempted: bool = False
    redis_consume_attempted: bool = False
    redis_ack_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    render_write_attempted: bool = False
    delivery_record_write_attempted: bool = False
    delivery_result_outbox_write_attempted: bool = False
    database_committed: bool = False
    database_rolled_back: bool = False
    maintenance_redis_publish_attempted: bool = False
    maintenance_outbox_status_update_attempted: bool = False
    maintenance_outbox_status_committed: bool = False
    telegram_transport_constructed: bool = False
    telegram_send_called: bool = False
    telegram_edit_called: bool = False


class BoundedNotificationSendLiveError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class NotificationLiveExecution:
    context: NotificationSendContext
    delivery_result: DeliveryResult
    notification_delivery_record_id: UUID
    delivery_result_event_row: OutboxEventRow
    q_maintenance_message_id: str | None = None
    q_maintenance_marked_published: bool = False
    notifier_owned_write_counts: Mapping[str, int] = field(default_factory=dict)


class BoundedNotificationSendLiveRuntime(Protocol):
    async def inspect_target(self, config: BoundedNotificationSendLiveConfig) -> RedisTargetSelection: ...
    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotificationSendLiveConfig,
    ) -> RedisTargetSelection: ...
    async def load_context(
        self,
        trigger_event_id: UUID,
        config: BoundedNotificationSendLiveConfig,
    ) -> NotificationSendContext: ...
    async def execute_live(
        self,
        trigger_event_id: UUID,
        context: NotificationSendContext,
        config: BoundedNotificationSendLiveConfig,
    ) -> NotificationLiveExecution: ...
    async def readback_final_state(self, execution: NotificationLiveExecution) -> NotificationDurableReadback: ...
    async def publish_maintenance(self, event_row: OutboxEventRow) -> str: ...
    async def mark_delivery_result_published(self, event_id: UUID) -> None: ...
    async def ack(self, message_id: str) -> int: ...
    async def close(self) -> None: ...


class BoundedNotificationSendLiveRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotificationSendLiveRuntimeConfig,
        state: BoundedNotificationSendLiveState,
        logger: logging.Logger,
    ) -> BoundedNotificationSendLiveRuntime: ...


@dataclass(frozen=True, slots=True)
class BoundedNotificationSendLiveResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedNotificationSendLiveConfig
    state: BoundedNotificationSendLiveState = field(default_factory=BoundedNotificationSendLiveState)
    redis_selection: RedisTargetSelection | None = None
    context: NotificationSendContext | None = None
    execution: NotificationLiveExecution | None = None
    durable_readback: NotificationDurableReadback | None = None
    q_maintenance_published: bool = False
    q_maintenance_message_id_present: bool = False
    redis_ack_status: str = "not_attempted"
    redis_acked_count: int = 0
    transport_gate_mode: str = "not_evaluated"

    def to_sanitized_dict(self) -> dict[str, Any]:
        selection = self.redis_selection
        context = self.context or (self.execution.context if self.execution else None)
        execution = self.execution
        delivery_result = execution.delivery_result if execution else None
        readback = self.durable_readback
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "queue_name": QUEUE_NAME,
            "stage_name": selection.message_stage_name if selection else STAGE_NAME,
            "target_redis_message_suffix": _message_suffix(
                self.config.redis_message_suffix
                or (selection.message.message_id if selection and selection.message else None)
            ),
            "trigger_event_suffix": _uuid_suffix(self.config.trigger_event_suffix)
            or _uuid_suffix(context.trigger_event_id if context else None),
            "notification_plan_id_suffix": (
                _uuid_suffix(self.config.notification_plan_id_suffix)
                or _uuid_suffix(context.intent.notification_plan_id if context else None)
            ),
            "analysis_id_suffix": _uuid_suffix(self.config.analysis_id_suffix)
            or _uuid_suffix(context.intent.analysis_id if context else None),
            "target_chat_id_suffix": _chat_suffix(
                self.config.target_chat_id_suffix or (context.intent.target_chat_id if context else None)
            ),
            "target_chat_suffix_verified": bool(
                context is not None
                and self.config.target_chat_id_suffix
                and str(context.intent.target_chat_id).endswith(self.config.target_chat_id_suffix)
                and str(context.intent.target_chat_id) != self.config.target_chat_id_suffix
            ),
            "plan_action": context.plan_action if context else "not_evaluated",
            "render_action": context.render_action if context else "not_evaluated",
            "delivery_action": context.delivery_action if context else "not_evaluated",
            "planned_action": context.planned_action if context else "not_evaluated",
            "delivery_status": delivery_result.delivery_status if delivery_result else context.delivery_status if context else None,
            "attempt_count": delivery_result.attempt_count if delivery_result else None,
            "transport_error_code": _safe_token(delivery_result.transport_error_code if delivery_result else None),
            "transport_error_class": _safe_token(delivery_result.transport_error_class if delivery_result else None),
            "telegram_chat_id_present": delivery_result.telegram_chat_id is not None if delivery_result else False,
            "telegram_message_id_present": delivery_result.telegram_message_id is not None if delivery_result else False,
            "retry_after_seconds_present": delivery_result.retry_after_seconds is not None if delivery_result else False,
            "edited": bool(delivery_result.edited) if delivery_result else False,
            "delivery_result_event_suffix": _uuid_suffix(
                execution.delivery_result_event_row.event_id if execution else None
            ),
            "q_maintenance_published": self.q_maintenance_published,
            "q_maintenance_message_suffix": (
                _message_suffix(execution.q_maintenance_message_id if execution else None)
                if self.q_maintenance_message_id_present
                else None
            ),
            "redis_ack_status": self.redis_ack_status,
            "redis_acked_count": self.redis_acked_count,
            "durable_readback": _durable_readback_report(readback),
            "redis_ack_after_durable_readback": bool(
                readback is not None and readback.ack_safe and self.redis_ack_status == "acked"
            ),
            "transport_gate_mode": self.transport_gate_mode,
            "notifier_owned_write_counts": dict(execution.notifier_owned_write_counts) if execution else {},
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "database_read_allowed": self.config.allow_database_read,
                "database_write_allowed": self.config.allow_database_write,
                "redis_read_allowed": self.config.allow_redis_read,
                "redis_consume_allowed": self.config.allow_redis_consume,
                "redis_ack_allowed": self.config.allow_redis_ack,
                "maintenance_publish_allowed": self.config.allow_maintenance_publish,
                "render_write_allowed": self.config.allow_render_write,
                "delivery_record_write_allowed": self.config.allow_delivery_record_write,
                "delivery_result_outbox_write_allowed": self.config.allow_delivery_result_outbox_write,
                "target_chat_suffix_required": True,
                "telegram_transport_allowed": self.config.allow_telegram_transport,
                "telegram_send_allowed": self.config.allow_telegram_send,
            },
            "side_effects": {
                "runtime_config_loaded": self.state.runtime_config_loaded,
                "redis_read_attempted": self.state.redis_read_attempted,
                "redis_consume_attempted": self.state.redis_consume_attempted,
                "redis_ack_attempted": self.state.redis_ack_attempted,
                "database_session_opened": self.state.database_session_opened,
                "database_read_attempted": self.state.database_read_attempted,
                "database_write_attempted": self.state.database_write_attempted,
                "render_write_attempted": self.state.render_write_attempted,
                "delivery_record_write_attempted": self.state.delivery_record_write_attempted,
                "delivery_result_outbox_write_attempted": self.state.delivery_result_outbox_write_attempted,
                "database_committed": self.state.database_committed,
                "database_rolled_back": self.state.database_rolled_back,
                "maintenance_redis_publish_attempted": self.state.maintenance_redis_publish_attempted,
                "maintenance_outbox_status_update_attempted": self.state.maintenance_outbox_status_update_attempted,
                "maintenance_outbox_status_committed": self.state.maintenance_outbox_status_committed,
                "telegram_transport_constructed": self.state.telegram_transport_constructed,
                "telegram_send_called": self.state.telegram_send_called,
                "telegram_edit_called": self.state.telegram_edit_called,
                "telegram_transport_called": self.state.telegram_send_called or self.state.telegram_edit_called,
                "openai_called": False,
                "github_called": False,
                "x_called": False,
                "web_called": False,
                "subprocess_started": False,
                "docker_or_systemd_called": False,
                "run_forever_started": False,
                "alembic_or_ddl_ran": False,
                "feature_flags_mutated": False,
            },
            "redactions_applied": [
                "full_redis_id_omitted",
                "full_uuid_omitted",
                "database_url_omitted",
                "redis_url_omitted",
                "idempotency_key_omitted",
                "target_chat_id_omitted",
                "rendered_message_text_omitted",
                "telegram_token_omitted",
                "telegram_response_body_omitted",
                "payload_json_omitted",
                "raw_source_text_omitted",
                "exception_detail_omitted",
            ],
        }


class DefaultBoundedNotificationSendLiveRuntime:
    def __init__(
        self,
        *,
        runtime_config: BoundedNotificationSendLiveRuntimeConfig,
        redis_consumer: RedisExactNotificationSendConsumer,
        redis_client: Any,
        session_factory: Any,
        engine: Any,
        state: BoundedNotificationSendLiveState,
        logger: logging.Logger,
        route_resolver: OutboxRouteResolver,
        xadd_maxlen: int | None,
        transport_builder: Callable[
            [NotifierTelegramConfig, BoundedNotificationSendLiveConfig, BoundedNotificationSendLiveState],
            TelegramTransport | None,
        ]
        | None = None,
    ) -> None:
        self._runtime_config = runtime_config
        self._redis_consumer = redis_consumer
        self._redis_client = redis_client
        self._session_factory = session_factory
        self._engine = engine
        self._state = state
        self._logger = logger
        self._route_resolver = route_resolver
        self._publisher = RedisStreamsPublisher(redis_client, maxlen=xadd_maxlen)
        self._transport_builder = transport_builder or _default_transport_builder

    async def inspect_target(self, config: BoundedNotificationSendLiveConfig) -> RedisTargetSelection:
        return await self._redis_consumer.inspect_target(config)  # type: ignore[arg-type]

    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotificationSendLiveConfig,
    ) -> RedisTargetSelection:
        return await self._redis_consumer.consume_target(expected, config)  # type: ignore[arg-type]

    async def load_context(
        self,
        trigger_event_id: UUID,
        config: BoundedNotificationSendLiveConfig,
    ) -> NotificationSendContext:
        from .bounded_notification_send_dry_run_runner import _load_context

        async with self._session_factory() as session:
            self._state.database_session_opened = True
            self._state.database_read_attempted = True
            repository = NotifierTelegramRepository(session)
            renderer = NotificationRenderer(max_message_chars=self._runtime_config.notifier_config.max_message_chars)
            return await _load_context(repository, renderer, trigger_event_id, config)  # type: ignore[arg-type]

    async def execute_live(
        self,
        trigger_event_id: UUID,
        context: NotificationSendContext,
        config: BoundedNotificationSendLiveConfig,
    ) -> NotificationLiveExecution:
        transport = self._transport_builder(self._runtime_config.notifier_config, config, self._state)
        try:
            async with self._session_factory.begin() as session:
                self._state.database_session_opened = True
                self._state.database_read_attempted = True
                base_repository = NotifierTelegramRepository(session)
                repository = _CountingLiveRepository(base_repository, self._state)
                service = NotifierTelegramService(
                    self._runtime_config.notifier_config,
                    repository=repository,  # type: ignore[arg-type]
                    telegram_client=transport,
                    logger=self._logger,
                )
                result = await service.handle_trigger_event(trigger_event_id)
                if result is None:
                    raise BoundedNotificationSendLiveError("notifier_invocation_no_result")
                if repository.delivery_result_event_id is None:
                    raise BoundedNotificationSendLiveError("delivery_result_outbox_insert_failed")
                if repository.notification_delivery_record_id is None:
                    raise BoundedNotificationSendLiveError("notification_delivery_record_readback_failed")
                event_raw = await repository.load_event_outbox(repository.delivery_result_event_id)
                if event_raw is None:
                    raise BoundedNotificationSendLiveError("delivery_result_outbox_readback_failed")
                event_row = _outbox_event_row(event_raw)
                execution = NotificationLiveExecution(
                    context=_context_with_delivery_result(context, result, plan_id=event_row.aggregate_id),
                    delivery_result=result,
                    notification_delivery_record_id=repository.notification_delivery_record_id,
                    delivery_result_event_row=event_row,
                    notifier_owned_write_counts=dict(repository.write_counts),
                )
            self._state.database_committed = True
            return execution
        except Exception:
            self._state.database_rolled_back = True
            raise

    async def readback_final_state(self, execution: NotificationLiveExecution) -> NotificationDurableReadback:
        async with self._session_factory() as session:
            self._state.database_session_opened = True
            self._state.database_read_attempted = True
            repository = NotifierTelegramRepository(session)
            render_hash = execution.context.render_draft.render_hash if execution.context.render_draft else ""
            result = execution.delivery_result
            raw = await repository.load_bounded_notification_send_readback(
                notification_plan_id=execution.context.plan_id,
                analysis_id=execution.context.intent.analysis_id,
                candidate_group_id=execution.context.intent.candidate_group_id,
                target_chat_id=execution.context.intent.target_chat_id,
                dedupe_subject_key=execution.context.intent.dedupe_subject_key,
                material_change_hash=execution.context.intent.material_change_hash,
                render_hash=render_hash,
                notification_delivery_record_id=execution.notification_delivery_record_id,
                delivery_result_event_id=execution.delivery_result_event_row.event_id,
                delivery_status=result.delivery_status,
                telegram_chat_id=result.telegram_chat_id,
                telegram_message_id=result.telegram_message_id,
                attempt_count=result.attempt_count,
                transport_error_code=result.transport_error_code,
                edited=result.edited,
            )
        event_status = _string_or_none(raw.get("delivery_result_event_status"))
        event_row = replace(
            execution.delivery_result_event_row,
            status=event_status or execution.delivery_result_event_row.status,
        )
        route = self._route_resolver.resolve(event_row)
        q_maintenance_route_verified = (
            route.queue_name == MAINTENANCE_QUEUE_NAME and route.stage_name == MAINTENANCE_STAGE_NAME
        )
        maintenance_message = _build_stream_message(event_row, route)
        q_maintenance_message_thin = (
            q_maintenance_route_verified
            and set(maintenance_message.as_stream_fields()) == set(REQUIRED_THIN_QUEUE_FIELDS)
            and "payload_json" not in maintenance_message.as_stream_fields()
        )
        return _durable_readback_from_raw(
            raw,
            q_maintenance_route_verified=q_maintenance_route_verified,
            q_maintenance_message_thin=q_maintenance_message_thin,
        )

    async def publish_maintenance(self, event_row: OutboxEventRow) -> str:
        route = self._route_resolver.resolve(event_row)
        if route.queue_name != MAINTENANCE_QUEUE_NAME or route.stage_name != MAINTENANCE_STAGE_NAME:
            raise BoundedNotificationSendLiveError("q_maintenance_route_not_allowed")
        self._state.maintenance_redis_publish_attempted = True
        return await self._publisher.publish(route, _build_stream_message(event_row, route))

    async def mark_delivery_result_published(self, event_id: UUID) -> None:
        async with self._session_factory.begin() as session:
            self._state.database_session_opened = True
            repository = NotifierTelegramRepository(session)
            self._state.maintenance_outbox_status_update_attempted = True
            await repository.mark_event_outbox_published(event_id=event_id, published_at=_utc_now())
        self._state.maintenance_outbox_status_committed = True

    async def ack(self, message_id: str) -> int:
        return await self._redis_consumer.ack(message_id)

    async def close(self) -> None:
        close = getattr(self._redis_client, "aclose", None) or getattr(self._redis_client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result
        await self._engine.dispose()


class _CountingLiveRepository:
    def __init__(self, wrapped: NotifierTelegramRepository, state: BoundedNotificationSendLiveState) -> None:
        self._wrapped = wrapped
        self._state = state
        self.delivery_result_event_id: UUID | None = None
        self.notification_delivery_record_id: UUID | None = None
        self.write_counts = {
            "notification_plans_insert_calls": 0,
            "notification_renders_insert_calls": 0,
            "notification_delivery_records_insert_calls": 0,
            "notification_plans_status_update_calls": 0,
            "state_transitions_insert_calls": 0,
            "event_outbox_delivery_result_insert_calls": 0,
        }

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    async def insert_notification_plan(self, *args: Any, **kwargs: Any) -> UUID:
        self._state.database_write_attempted = True
        self.write_counts["notification_plans_insert_calls"] += 1
        return await self._wrapped.insert_notification_plan(*args, **kwargs)

    async def insert_notification_render(self, *args: Any, **kwargs: Any) -> UUID | None:
        self._state.database_write_attempted = True
        self._state.render_write_attempted = True
        self.write_counts["notification_renders_insert_calls"] += 1
        return await self._wrapped.insert_notification_render(*args, **kwargs)

    async def insert_delivery_record(self, *args: Any, **kwargs: Any) -> UUID:
        self._state.database_write_attempted = True
        self._state.delivery_record_write_attempted = True
        self.write_counts["notification_delivery_records_insert_calls"] += 1
        self.notification_delivery_record_id = await self._wrapped.insert_delivery_record(*args, **kwargs)
        return self.notification_delivery_record_id

    async def update_plan_status(self, *args: Any, **kwargs: Any) -> None:
        self._state.database_write_attempted = True
        self.write_counts["notification_plans_status_update_calls"] += 1
        await self._wrapped.update_plan_status(*args, **kwargs)

    async def insert_state_transition(self, *args: Any, **kwargs: Any) -> None:
        self._state.database_write_attempted = True
        self.write_counts["state_transitions_insert_calls"] += 1
        await self._wrapped.insert_state_transition(*args, **kwargs)

    async def insert_delivery_result_outbox(self, *args: Any, **kwargs: Any) -> None:
        self._state.database_write_attempted = True
        self._state.delivery_result_outbox_write_attempted = True
        self.write_counts["event_outbox_delivery_result_insert_calls"] += 1
        self.delivery_result_event_id = await self._wrapped.insert_delivery_result_outbox_returning(*args, **kwargs)


async def build_default_bounded_notification_send_live_runtime(
    runtime_config: BoundedNotificationSendLiveRuntimeConfig,
    state: BoundedNotificationSendLiveState,
    logger: logging.Logger,
) -> BoundedNotificationSendLiveRuntime:
    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.notifier_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_client_created = True
    dry_state = _DryStateBridge(state)
    redis_consumer = RedisExactNotificationSendConsumer(
        redis_client,
        queue_name=runtime_config.notifier_config.queue_name,
        consumer_group=runtime_config.consumer_group,
        consumer_name=runtime_config.consumer_name,
        block_ms=runtime_config.block_ms,
        state=dry_state,  # type: ignore[arg-type]
    )
    return DefaultBoundedNotificationSendLiveRuntime(
        runtime_config=runtime_config,
        redis_consumer=redis_consumer,
        redis_client=redis_client,
        session_factory=session_factory,
        engine=engine,
        state=state,
        logger=logger,
        route_resolver=OutboxRouteResolver(),
        xadd_maxlen=runtime_config.xadd_maxlen,
    )


class _DryStateBridge:
    def __init__(self, state: BoundedNotificationSendLiveState) -> None:
        self._live_state = state

    def __setattr__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)
        live_state = getattr(self, "_live_state", None)
        if live_state is not None and hasattr(live_state, name):
            setattr(live_state, name, value)


def _context_with_delivery_result(
    context: NotificationSendContext,
    result: DeliveryResult,
    *,
    plan_id: UUID | None = None,
) -> NotificationSendContext:
    return NotificationSendContext(
        trigger_event_id=context.trigger_event_id,
        event_row=context.event_row,
        intent=context.intent,
        analysis=context.analysis,
        judge_output=context.judge_output,
        candidate=context.candidate,
        plan_id=plan_id or context.plan_id,
        existing_plan_status=context.existing_plan_status,
        plan_action=context.plan_action,
        render_draft=context.render_draft,
        render_action=context.render_action,
        delivery_action=_delivery_action_from_result(result),
        delivery_status=result.delivery_status,
        planned_action="execute_live_delivery",
        error_code=None,
    )


def _delivery_action_from_result(result: DeliveryResult) -> str:
    if result.delivery_status == "sent":
        return "send"
    if result.delivery_status == "edited":
        return "edit"
    if result.transport_error_code == "notification_send_flag_disabled":
        return "suppress_send_disabled_no_transport"
    if result.transport_error_code == "dry_run_skip_transport":
        return "suppress_dry_run_no_transport"
    if result.delivery_status in {"failed_retryable", "failed_terminal"}:
        return "transport_failure"
    return "noop"


def load_bounded_notification_send_live_runtime_config(
    config: BoundedNotificationSendLiveConfig,
    env: Mapping[str, str] | None = None,
) -> BoundedNotificationSendLiveRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedNotificationSendLiveError("database_url_missing")
    if not redis_url:
        raise BoundedNotificationSendLiveError("redis_url_missing")
    try:
        app_env = _env_value(source, "APP_ENV", "dev").lower() or "dev"
        is_prod = app_env in {"prod", "production"}
        notifier_config = NotifierTelegramConfig(
            app_env=app_env,
            database_url=database_url,
            redis_url=redis_url,
            telegram_bot_token=_env_value(source, "TELEGRAM_BOT_TOKEN"),
            queue_name=_env_value(source, "NOTIFIER_TELEGRAM_QUEUE_NAME", QUEUE_NAME) or QUEUE_NAME,
            consumer_group=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
            consumer_name=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_NAME", DEFAULT_CONSUMER_NAME),
            batch_size=1,
            block_ms=_int_env(source, "NOTIFIER_TELEGRAM_BLOCK_MS", DEFAULT_BLOCK_MS),
            dry_run=_bool_env(_env_value(source, "NOTIFIER_TELEGRAM_DRY_RUN", "false" if is_prod else "true")),
            allow_edits=_bool_env(_env_value(source, "NOTIFIER_TELEGRAM_ALLOW_EDITS", "false")),
            enable_notification_send=_bool_env(_env_value(source, "ENABLE_NOTIFICATION_SEND", "false")),
            enable_digest_runtime=_bool_env(_env_value(source, "ENABLE_DIGEST_RUNTIME", "false")),
            max_message_chars=_int_env(source, "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", 3800),
            edit_window_minutes=_int_env(source, "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES", 180),
            telegram_api_base_url=_env_value(source, "TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
            request_timeout_sec=_float_env(source, "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", 10.0),
            log_level=_env_value(source, "LOG_LEVEL", "INFO").upper() or "INFO",
        )
        notifier_config.validate(require_transport_token=False)
    except (NotifierTelegramConfigurationError, ValueError) as exc:
        raise BoundedNotificationSendLiveError("runtime_config_error") from exc
    if notifier_config.queue_name != QUEUE_NAME:
        raise BoundedNotificationSendLiveError("queue_name_not_allowed")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedNotificationSendLiveError("runtime_config_error")
    return BoundedNotificationSendLiveRuntimeConfig(
        notifier_config=notifier_config,
        redis_url=redis_url,
        consumer_group=notifier_config.consumer_group,
        consumer_name=notifier_config.consumer_name,
        block_ms=notifier_config.block_ms,
        xadd_maxlen=xadd_maxlen,
    )


async def run_bounded_notification_send_live(
    config: BoundedNotificationSendLiveConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotificationSendLiveConfig],
        BoundedNotificationSendLiveRuntimeConfig,
    ] = load_bounded_notification_send_live_runtime_config,
    runtime_builder: BoundedNotificationSendLiveRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationSendLiveResult:
    state = BoundedNotificationSendLiveState()
    gate_error = _pre_config_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)
    runtime: BoundedNotificationSendLiveRuntime | None = None
    transport_gate_mode = "not_evaluated"
    try:
        try:
            runtime_config = runtime_config_loader(config)
            state.runtime_config_loaded = True
        except BoundedNotificationSendLiveError as exc:
            return _result("blocked", exc.error_code, config=config, state=state)
        except Exception as exc:
            return _result(
                "blocked",
                "runtime_config_error",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
            )
        telegram_gate_error = _telegram_gate_error(config, runtime_config.notifier_config)
        transport_gate_mode = "live_transport_required" if runtime_config.notifier_config.transport_enabled else "suppressed_by_config"
        if telegram_gate_error is not None:
            return _result(
                "blocked",
                telegram_gate_error,
                config=config,
                state=state,
                transport_gate_mode=transport_gate_mode,
            )
        builder = runtime_builder or build_default_bounded_notification_send_live_runtime
        runtime = await builder(runtime_config, state, logger or logging.getLogger(__name__))
        selection = await runtime.inspect_target(config)
        if selection.message is None:
            return _result(
                "blocked",
                selection.error_code or "redis_target_missing",
                config=config,
                state=state,
                redis_selection=selection,
                transport_gate_mode=transport_gate_mode,
            )
        trigger_event_id = _uuid_or_none(selection.message.fields.get("trigger_event_id"))
        if trigger_event_id is None:
            return _result(
                "blocked",
                "trigger_event_id_invalid",
                config=config,
                state=state,
                redis_selection=selection,
                transport_gate_mode=transport_gate_mode,
            )
        context = await runtime.load_context(trigger_event_id, config)
        if context.error_code is not None:
            return _result(
                "blocked",
                context.error_code,
                config=config,
                state=state,
                redis_selection=selection,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        target_chat_error = _target_chat_context_error(context, config)
        if target_chat_error is not None:
            return _result(
                "blocked",
                target_chat_error,
                config=config,
                state=state,
                redis_selection=selection,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        if not selection.target_is_next:
            return _result(
                "blocked",
                "selected_target_not_next_unconsumed",
                config=config,
                state=state,
                redis_selection=selection,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        if config.mode == "preview":
            return _result(
                "pass",
                None,
                config=config,
                state=state,
                redis_selection=selection,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        consumed = await runtime.consume_target(selection.message, config)
        if consumed.message is None:
            return _result(
                "blocked",
                consumed.error_code or "redis_consume_target_missing",
                config=config,
                state=state,
                redis_selection=consumed,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        try:
            execution = await runtime.execute_live(trigger_event_id, context, config)
        except BoundedNotificationSendLiveError as exc:
            return _result(
                "failed",
                exc.error_code,
                config=config,
                state=state,
                redis_selection=consumed,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        except TelegramTransportConstructionError as exc:
            return _result(
                "failed",
                exc.error_code,
                config=config,
                state=state,
                redis_selection=consumed,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        except Exception as exc:
            return _result(
                "failed",
                "database_commit_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_selection=consumed,
                context=context,
                transport_gate_mode=transport_gate_mode,
            )
        maintenance_message_id: str | None = None
        if execution.delivery_result_event_row.status == "published":
            readback, readback_failure = await _readback_or_failure(
                runtime,
                execution,
                config=config,
                state=state,
                redis_selection=consumed,
                transport_gate_mode=transport_gate_mode,
            )
            if readback_failure is not None:
                return readback_failure
            try:
                ack_count = await runtime.ack(consumed.message.message_id)
            except Exception as exc:
                return _result(
                    "failed",
                    "ack_failed_after_durable_completion",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                    redis_selection=consumed,
                    execution=execution,
                    durable_readback=readback,
                    redis_ack_status="failed_after_durable_completion",
                    transport_gate_mode=transport_gate_mode,
                )
            acked = ack_count == 1
            return _result(
                "pass" if acked else "failed",
                None if acked else "ack_failed_after_durable_completion",
                config=config,
                state=state,
                redis_selection=consumed,
                execution=execution,
                durable_readback=readback,
                redis_ack_status="acked" if acked else "failed_after_durable_completion",
                redis_acked_count=ack_count,
                transport_gate_mode=transport_gate_mode,
            )
        try:
            maintenance_message_id = await runtime.publish_maintenance(execution.delivery_result_event_row)
            await runtime.mark_delivery_result_published(execution.delivery_result_event_row.event_id)
        except BoundedNotificationSendLiveError as exc:
            return _result(
                "failed",
                exc.error_code,
                config=config,
                state=state,
                redis_selection=consumed,
                execution=_with_live_maintenance_message(execution, maintenance_message_id),
                q_maintenance_message_id_present=bool(maintenance_message_id),
                transport_gate_mode=transport_gate_mode,
            )
        except Exception as exc:
            return _result(
                "failed",
                "q_maintenance_publish_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_selection=consumed,
                execution=_with_live_maintenance_message(execution, maintenance_message_id),
                q_maintenance_message_id_present=bool(maintenance_message_id),
                transport_gate_mode=transport_gate_mode,
            )
        execution_with_maintenance = _with_live_maintenance_message(execution, maintenance_message_id, marked=True)
        readback, readback_failure = await _readback_or_failure(
            runtime,
            execution_with_maintenance,
            config=config,
            state=state,
            redis_selection=consumed,
            q_maintenance_published=True,
            q_maintenance_message_id_present=bool(maintenance_message_id),
            transport_gate_mode=transport_gate_mode,
        )
        if readback_failure is not None:
            return readback_failure
        try:
            ack_count = await runtime.ack(consumed.message.message_id)
        except Exception as exc:
            return _result(
                "failed",
                "ack_failed_after_durable_completion",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_selection=consumed,
                execution=execution_with_maintenance,
                durable_readback=readback,
                q_maintenance_published=True,
                q_maintenance_message_id_present=bool(maintenance_message_id),
                redis_ack_status="failed_after_durable_completion",
                transport_gate_mode=transport_gate_mode,
            )
        acked = ack_count == 1
        return _result(
            "pass" if acked else "failed",
            None if acked else "ack_failed_after_durable_completion",
            config=config,
            state=state,
            redis_selection=consumed,
            execution=execution_with_maintenance,
            durable_readback=readback,
            q_maintenance_published=True,
            q_maintenance_message_id_present=bool(maintenance_message_id),
            redis_ack_status="acked" if acked else "failed_after_durable_completion",
            redis_acked_count=ack_count,
            transport_gate_mode=transport_gate_mode,
        )
    except Exception as exc:
        return _result(
            "failed",
            "bounded_notification_send_live_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            transport_gate_mode=transport_gate_mode,
        )
    finally:
        if runtime is not None:
            try:
                await runtime.close()
            except Exception:
                pass


def run_bounded_notification_send_live_sync(
    config: BoundedNotificationSendLiveConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotificationSendLiveConfig],
        BoundedNotificationSendLiveRuntimeConfig,
    ] = load_bounded_notification_send_live_runtime_config,
    runtime_builder: BoundedNotificationSendLiveRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationSendLiveResult:
    return asyncio.run(
        run_bounded_notification_send_live(
            config,
            runtime_config_loader=runtime_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedNotificationSendLiveConfig(),
        state=BoundedNotificationSendLiveState(),
    ).to_sanitized_dict()


def _pre_config_gate_error(config: BoundedNotificationSendLiveConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if config.mode not in {"preview", "execute"}:
        return "mode_not_allowed"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    suffix_error = _target_suffix_error(config)  # type: ignore[arg-type]
    if suffix_error is not None:
        return suffix_error
    if not _valid_chat_suffix(config.target_chat_id_suffix):
        return "target_chat_id_suffix_missing_or_invalid"
    if config.scan_limit < 1 or config.scan_limit > 100:
        return "scan_limit_out_of_range"
    if config.mode == "execute":
        if not config.allow_redis_consume:
            return "redis_consume_not_allowed"
        if not config.allow_database_write:
            return "database_write_not_allowed"
        if not config.allow_render_write:
            return "render_write_not_allowed"
        if not config.allow_delivery_record_write:
            return "delivery_record_write_not_allowed"
        if not config.allow_delivery_result_outbox_write:
            return "delivery_result_outbox_write_not_allowed"
        if not config.allow_maintenance_publish:
            return "maintenance_publish_not_allowed"
        if not config.allow_redis_ack:
            return "redis_ack_not_allowed"
    return None


def _target_chat_context_error(
    context: NotificationSendContext,
    config: BoundedNotificationSendLiveConfig,
) -> str | None:
    suffix = str(config.target_chat_id_suffix or "").strip()
    target = str(context.intent.target_chat_id)
    if not target.endswith(suffix):
        return "target_chat_id_mismatch"
    if target == suffix:
        return "target_chat_id_suffix_must_not_be_full_id"
    return None


async def _readback_or_failure(
    runtime: BoundedNotificationSendLiveRuntime,
    execution: NotificationLiveExecution,
    *,
    config: BoundedNotificationSendLiveConfig,
    state: BoundedNotificationSendLiveState,
    redis_selection: RedisTargetSelection,
    q_maintenance_published: bool = False,
    q_maintenance_message_id_present: bool = False,
    transport_gate_mode: str = "not_evaluated",
) -> tuple[NotificationDurableReadback | None, BoundedNotificationSendLiveResult | None]:
    try:
        readback = await runtime.readback_final_state(execution)
    except BoundedNotificationSendLiveError as exc:
        return None, _result(
            "failed",
            exc.error_code,
            config=config,
            state=state,
            redis_selection=redis_selection,
            execution=execution,
            q_maintenance_published=q_maintenance_published,
            q_maintenance_message_id_present=q_maintenance_message_id_present,
            transport_gate_mode=transport_gate_mode,
        )
    except Exception as exc:
        return None, _result(
            "failed",
            "durable_readback_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            redis_selection=redis_selection,
            execution=execution,
            q_maintenance_published=q_maintenance_published,
            q_maintenance_message_id_present=q_maintenance_message_id_present,
            transport_gate_mode=transport_gate_mode,
        )
    if not readback.ack_safe:
        return readback, _result(
            "failed",
            "durable_readback_failed",
            config=config,
            state=state,
            redis_selection=redis_selection,
            execution=execution,
            durable_readback=readback,
            q_maintenance_published=q_maintenance_published,
            q_maintenance_message_id_present=q_maintenance_message_id_present,
            transport_gate_mode=transport_gate_mode,
        )
    return readback, None


def _telegram_gate_error(
    config: BoundedNotificationSendLiveConfig,
    notifier_config: NotifierTelegramConfig,
) -> str | None:
    if config.mode != "execute" or not notifier_config.transport_enabled:
        return None
    if not config.allow_telegram_transport:
        return "telegram_transport_not_allowed"
    if not config.allow_telegram_send:
        return "telegram_send_not_allowed"
    return None


def _default_transport_builder(
    notifier_config: NotifierTelegramConfig,
    config: BoundedNotificationSendLiveConfig,
    state: BoundedNotificationSendLiveState,
) -> TelegramTransport | None:
    if not notifier_config.transport_enabled:
        return None
    transport = TelegramBotApiTransport.from_config(
        notifier_config,
        allow_telegram_transport=config.allow_telegram_transport,
        allow_telegram_send=config.allow_telegram_send,
    )
    state.telegram_transport_constructed = True
    return StateTrackingTelegramTransport(
        transport,
        on_send=lambda: setattr(state, "telegram_send_called", True),
        on_edit=lambda: setattr(state, "telegram_edit_called", True),
    )


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotificationSendLiveConfig,
    state: BoundedNotificationSendLiveState,
    error_class: str | None = None,
    redis_selection: RedisTargetSelection | None = None,
    context: NotificationSendContext | None = None,
    execution: NotificationLiveExecution | None = None,
    durable_readback: NotificationDurableReadback | None = None,
    q_maintenance_published: bool = False,
    q_maintenance_message_id_present: bool = False,
    redis_ack_status: str = "not_attempted",
    redis_acked_count: int = 0,
    transport_gate_mode: str = "not_evaluated",
) -> BoundedNotificationSendLiveResult:
    return BoundedNotificationSendLiveResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        redis_selection=redis_selection,
        context=context,
        execution=execution,
        durable_readback=durable_readback,
        q_maintenance_published=q_maintenance_published,
        q_maintenance_message_id_present=q_maintenance_message_id_present,
        redis_ack_status=redis_ack_status,
        redis_acked_count=redis_acked_count,
        transport_gate_mode=transport_gate_mode,
    )


def _with_live_maintenance_message(
    execution: NotificationLiveExecution,
    message_id: str | None,
    *,
    marked: bool = False,
) -> NotificationLiveExecution:
    return NotificationLiveExecution(
        context=execution.context,
        delivery_result=execution.delivery_result,
        notification_delivery_record_id=execution.notification_delivery_record_id,
        delivery_result_event_row=execution.delivery_result_event_row,
        q_maintenance_message_id=message_id,
        q_maintenance_marked_published=marked,
        notifier_owned_write_counts=execution.notifier_owned_write_counts,
    )


def _valid_chat_suffix(value: str | None) -> bool:
    text = str(value or "").strip()
    return text.isdigit() and 2 <= len(text) <= 6


def _chat_suffix(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text[-6:]


def _safe_token(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    if text.replace("_", "").replace("-", "").isalnum() and len(text) <= 120:
        lowered = text.lower()
        if not any(marker in lowered for marker in ("token", "secret", "password", "credential", "url")):
            return text
    return "redacted"


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    "BoundedNotificationSendLiveConfig",
    "BoundedNotificationSendLiveRuntimeBuilder",
    "BoundedNotificationSendLiveRuntimeConfig",
    "BoundedNotificationSendLiveState",
    "BoundedNotificationSendLiveError",
    "NotificationLiveExecution",
    "argument_error_report",
    "load_bounded_notification_send_live_runtime_config",
    "render_sanitized_json",
    "run_bounded_notification_send_live",
    "run_bounded_notification_send_live_sync",
]
