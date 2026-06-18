from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

from ..outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from ..outbox_relay.redis_streams import RedisStreamsPublisher
from ..outbox_relay.routing import OutboxRouteResolver
from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    DeliveryResult,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotificationPlanDraft,
    NotificationRenderDraft,
    StreamMessage,
)
from .renderer import NotificationRenderer, RenderInput
from .repositories import NotifierTelegramRepository

SCHEMA_VERSION = "bounded_notification_send_dry_run_runner_v1"
RUNNER_NAME = "bounded_notification_send_dry_run_runner"
QUEUE_NAME = "q.notification.send"
STAGE_NAME = "notify"
ROOT_OBJECT_TYPE = "analysis"
EVENT_TYPE = "notification.plan.created.v1"
DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
MAINTENANCE_QUEUE_NAME = "q.maintenance"
MAINTENANCE_STAGE_NAME = "maintenance"
DRY_RUN_REASON_CODE = "dry_run_skip_transport"
DEFAULT_CONSUMER_GROUP = "notifier-telegram"
DEFAULT_CONSUMER_NAME = RUNNER_NAME
DEFAULT_BLOCK_MS = 1
DEFAULT_SCAN_LIMIT = 10
DEFAULT_XADD_MAXLEN = 10000
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f]{4,12}$")
MESSAGE_ID_SUFFIX_RE = re.compile(r"^[0-9][0-9-]{3,11}$")
FULL_REDIS_STREAM_ID_RE = re.compile(r"^[0-9]{13,}-[0-9]+$")
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_]{1,120}$")
REQUIRED_THIN_QUEUE_FIELDS = (
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
)
FORBIDDEN_REDIS_FIELDS = {
    "message_text",
    "payload_json",
    "scores",
    "judge_output",
    "target_chat_id",
    "raw_text",
    "database_url",
    "redis_url",
}
FORBIDDEN_INTENT_PAYLOAD_KEYS = {
    "message_text",
    "rendered_message_text",
    "telegram_response",
    "telegram_response_json",
    "telegram_api_response",
    "telegram_bot_token",
    "database_url",
    "redis_url",
    "raw_text",
    "scores",
    "judge_output",
}


@dataclass(frozen=True, slots=True)
class BoundedNotificationSendDryRunConfig:
    mode: str = "preview"
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_database_read: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False
    allow_render_write: bool = False
    allow_delivery_record_write: bool = False
    allow_delivery_result_outbox_write: bool = False
    allow_maintenance_outbox_publish: bool = False
    allow_maintenance_redis_publish: bool = False
    trigger_event_suffix: str | None = None
    notification_plan_id_suffix: str | None = None
    analysis_id_suffix: str | None = None
    redis_message_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedNotificationSendDryRunRuntimeConfig:
    notifier_config: NotifierTelegramConfig
    redis_url: str
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    consumer_name: str = DEFAULT_CONSUMER_NAME
    block_ms: int = DEFAULT_BLOCK_MS
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedNotificationSendDryRunState:
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
    telegram_send_called: bool = False
    telegram_edit_called: bool = False


class BoundedNotificationSendDryRunError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class RedisTargetSelection:
    status: str
    error_code: str | None
    message: StreamMessage | None = None
    redis_message_count: int = 0
    group_lag: int | None = None
    group_pending: int | None = None
    message_stage_name: str | None = None
    message_root_object_type: str | None = None
    trigger_event_id_present: bool = False
    analysis_id_present: bool = False
    target_is_next: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class NotificationSendContext:
    trigger_event_id: UUID
    event_row: OutboxEventRow
    intent: NotificationIntentJob
    analysis: AnalysisRenderContext
    judge_output: JudgeOutputRenderContext | None
    candidate: CandidateRenderContext
    plan_id: UUID
    existing_plan_status: str | None
    plan_action: str
    render_draft: NotificationRenderDraft | None
    render_action: str
    delivery_action: str
    delivery_status: str | None
    planned_action: str
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class NotificationDryRunExecution:
    context: NotificationSendContext
    delivery_result: DeliveryResult
    notification_delivery_record_id: UUID
    delivery_result_event_row: OutboxEventRow
    q_maintenance_message_id: str | None = None
    q_maintenance_marked_published: bool = False
    notifier_owned_write_counts: Mapping[str, int] = field(default_factory=dict)


class BoundedNotificationSendDryRunRuntime(Protocol):
    async def inspect_target(self, config: BoundedNotificationSendDryRunConfig) -> RedisTargetSelection: ...
    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotificationSendDryRunConfig,
    ) -> RedisTargetSelection: ...
    async def load_context(
        self,
        trigger_event_id: UUID,
        config: BoundedNotificationSendDryRunConfig,
    ) -> NotificationSendContext: ...
    async def execute_dry_run(
        self,
        trigger_event_id: UUID,
        config: BoundedNotificationSendDryRunConfig,
    ) -> NotificationDryRunExecution: ...
    async def publish_maintenance(self, event_row: OutboxEventRow) -> str: ...
    async def mark_delivery_result_published(self, event_id: UUID) -> None: ...
    async def ack(self, message_id: str) -> int: ...
    async def close(self) -> None: ...


class BoundedNotificationSendDryRunRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotificationSendDryRunRuntimeConfig,
        state: BoundedNotificationSendDryRunState,
        logger: logging.Logger,
    ) -> BoundedNotificationSendDryRunRuntime: ...


@dataclass(frozen=True, slots=True)
class BoundedNotificationSendDryRunResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedNotificationSendDryRunConfig
    state: BoundedNotificationSendDryRunState = field(default_factory=BoundedNotificationSendDryRunState)
    redis_selection: RedisTargetSelection | None = None
    context: NotificationSendContext | None = None
    execution: NotificationDryRunExecution | None = None
    q_maintenance_published: bool = False
    q_maintenance_message_id_present: bool = False
    redis_ack_status: str = "not_attempted"
    redis_acked_count: int = 0

    def to_sanitized_dict(self) -> dict[str, Any]:
        selection = self.redis_selection
        context = self.context or (self.execution.context if self.execution else None)
        execution = self.execution
        gates = {
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "redis_read_allowed": self.config.allow_redis_read,
            "database_read_allowed": self.config.allow_database_read,
            "redis_consume_allowed": self.config.allow_redis_consume,
            "database_write_allowed": self.config.allow_database_write,
            "redis_ack_allowed": self.config.allow_redis_ack,
            "render_write_allowed": self.config.allow_render_write,
            "delivery_record_write_allowed": self.config.allow_delivery_record_write,
            "delivery_result_outbox_write_allowed": self.config.allow_delivery_result_outbox_write,
            "maintenance_outbox_publish_allowed": self.config.allow_maintenance_outbox_publish,
            "maintenance_redis_publish_allowed": self.config.allow_maintenance_redis_publish,
        }
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
            "plan_action": context.plan_action if context else "not_evaluated",
            "render_action": context.render_action if context else "not_evaluated",
            "delivery_action": context.delivery_action if context else "not_evaluated",
            "delivery_status": (
                execution.delivery_result.delivery_status
                if execution
                else (context.delivery_status if context else None)
            ),
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
            "planned_action": context.planned_action if context else "not_evaluated",
            "notifier_owned_write_counts": dict(execution.notifier_owned_write_counts) if execution else {},
            "gates": gates,
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
                "telegram_response_omitted",
                "raw_source_text_omitted",
                "judge_output_payload_omitted",
                "exception_detail_omitted",
            ],
        }


class RedisExactNotificationSendConsumer:
    def __init__(
        self,
        client: Any,
        *,
        queue_name: str,
        consumer_group: str,
        consumer_name: str,
        block_ms: int,
        state: BoundedNotificationSendDryRunState,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._state = state

    async def inspect_target(self, config: BoundedNotificationSendDryRunConfig) -> RedisTargetSelection:
        self._state.redis_read_attempted = True
        queue_type = _decode_redis_value(await self._client.type(self._queue_name)).strip().lower()
        if queue_type == "none":
            return _selection_error("stream_missing")
        if queue_type != "stream":
            return _selection_error("queue_key_wrong_type")
        group = _find_group(await self._client.xinfo_groups(self._queue_name), self._consumer_group)
        if group is None:
            return _selection_error("consumer_group_missing")
        pending = _safe_int(group.get("pending"))
        lag = _safe_int(group.get("lag"))
        last_delivered_id = _decode_redis_value(
            group.get("last-delivered-id") or group.get(b"last-delivered-id") or "0-0"
        )
        if pending not in (0, None):
            return _selection_error("redis_pending_messages_present", group_pending=pending, group_lag=lag)
        entries = await self._client.xrange(
            self._queue_name,
            min=f"({last_delivered_id}",
            max="+",
            count=config.scan_limit,
        )
        messages = [_stream_message_from_xrange(self._queue_name, entry) for entry in entries or []]
        return _select_exact_message(config, messages, group_pending=pending, group_lag=lag)

    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotificationSendDryRunConfig,
    ) -> RedisTargetSelection:
        self._state.redis_consume_attempted = True
        raw = await self._client.xreadgroup(
            self._consumer_group,
            self._consumer_name,
            {self._queue_name: ">"},
            count=1,
            block=self._block_ms,
        )
        messages = _stream_messages_from_xreadgroup(raw)
        selection = _select_exact_message(config, messages, group_pending=0, group_lag=1)
        if selection.message is None:
            return selection
        if selection.message.message_id != expected.message_id:
            return _selection_error(
                "redis_consume_target_mismatch",
                redis_message_count=selection.redis_message_count,
                group_pending=selection.group_pending,
                group_lag=selection.group_lag,
            )
        return selection

    async def ack(self, message_id: str) -> int:
        self._state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._consumer_group, message_id)
        return int(result or 0)


class DefaultBoundedNotificationSendDryRunRuntime:
    def __init__(
        self,
        *,
        redis_consumer: RedisExactNotificationSendConsumer,
        redis_client: Any,
        session_factory: Any,
        engine: Any,
        state: BoundedNotificationSendDryRunState,
        renderer: NotificationRenderer,
        route_resolver: OutboxRouteResolver,
        xadd_maxlen: int | None,
    ) -> None:
        self._redis_consumer = redis_consumer
        self._redis_client = redis_client
        self._session_factory = session_factory
        self._engine = engine
        self._state = state
        self._renderer = renderer
        self._route_resolver = route_resolver
        self._publisher = RedisStreamsPublisher(redis_client, maxlen=xadd_maxlen)

    async def inspect_target(self, config: BoundedNotificationSendDryRunConfig) -> RedisTargetSelection:
        return await self._redis_consumer.inspect_target(config)

    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotificationSendDryRunConfig,
    ) -> RedisTargetSelection:
        return await self._redis_consumer.consume_target(expected, config)

    async def load_context(
        self,
        trigger_event_id: UUID,
        config: BoundedNotificationSendDryRunConfig,
    ) -> NotificationSendContext:
        async with self._session_factory() as session:
            self._state.database_session_opened = True
            self._state.database_read_attempted = True
            repository = NotifierTelegramRepository(session)
            return await _load_context(repository, self._renderer, trigger_event_id, config)

    async def execute_dry_run(
        self,
        trigger_event_id: UUID,
        config: BoundedNotificationSendDryRunConfig,
    ) -> NotificationDryRunExecution:
        try:
            async with self._session_factory.begin() as session:
                self._state.database_session_opened = True
                self._state.database_read_attempted = True
                repository = NotifierTelegramRepository(session)
                context = await _load_context(repository, self._renderer, trigger_event_id, config)
                if context.error_code is not None:
                    raise BoundedNotificationSendDryRunError(context.error_code)
                execution = await _execute_context(repository, context, self._state)
            self._state.database_committed = True
            return execution
        except Exception:
            self._state.database_rolled_back = True
            raise

    async def publish_maintenance(self, event_row: OutboxEventRow) -> str:
        route = self._route_resolver.resolve(event_row)
        if route.queue_name != MAINTENANCE_QUEUE_NAME or route.stage_name != MAINTENANCE_STAGE_NAME:
            raise BoundedNotificationSendDryRunError("q_maintenance_route_not_allowed")
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


async def build_default_bounded_notification_send_dry_run_runtime(
    runtime_config: BoundedNotificationSendDryRunRuntimeConfig,
    state: BoundedNotificationSendDryRunState,
    logger: logging.Logger,
) -> BoundedNotificationSendDryRunRuntime:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.notifier_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_client_created = True
    redis_consumer = RedisExactNotificationSendConsumer(
        redis_client,
        queue_name=runtime_config.notifier_config.queue_name,
        consumer_group=runtime_config.consumer_group,
        consumer_name=runtime_config.consumer_name,
        block_ms=runtime_config.block_ms,
        state=state,
    )
    return DefaultBoundedNotificationSendDryRunRuntime(
        redis_consumer=redis_consumer,
        redis_client=redis_client,
        session_factory=session_factory,
        engine=engine,
        state=state,
        renderer=NotificationRenderer(max_message_chars=runtime_config.notifier_config.max_message_chars),
        route_resolver=OutboxRouteResolver(),
        xadd_maxlen=runtime_config.xadd_maxlen,
    )


def load_bounded_notification_send_dry_run_runtime_config(
    config: BoundedNotificationSendDryRunConfig,
    env: Mapping[str, str] | None = None,
) -> BoundedNotificationSendDryRunRuntimeConfig:
    del config
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedNotificationSendDryRunError("database_url_missing")
    if not redis_url:
        raise BoundedNotificationSendDryRunError("redis_url_missing")
    try:
        notifier_config = NotifierTelegramConfig(
            app_env=_env_value(source, "APP_ENV", "dev").lower() or "dev",
            database_url=database_url,
            redis_url=redis_url,
            telegram_bot_token="",
            queue_name=_env_value(source, "NOTIFIER_TELEGRAM_QUEUE_NAME", QUEUE_NAME) or QUEUE_NAME,
            consumer_group=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
            consumer_name=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_NAME", DEFAULT_CONSUMER_NAME),
            batch_size=1,
            block_ms=_int_env(source, "NOTIFIER_TELEGRAM_BLOCK_MS", DEFAULT_BLOCK_MS),
            dry_run=True,
            allow_edits=False,
            enable_notification_send=False,
            enable_digest_runtime=False,
            max_message_chars=_int_env(source, "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", 3800),
            edit_window_minutes=_int_env(source, "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES", 180),
            telegram_api_base_url=_env_value(source, "TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
            request_timeout_sec=_float_env(source, "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", 10.0),
            log_level=_env_value(source, "LOG_LEVEL", "INFO").upper() or "INFO",
        )
        notifier_config.validate(require_transport_token=False)
    except (NotifierTelegramConfigurationError, ValueError) as exc:
        raise BoundedNotificationSendDryRunError("runtime_config_error") from exc
    if notifier_config.queue_name != QUEUE_NAME:
        raise BoundedNotificationSendDryRunError("queue_name_not_allowed")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedNotificationSendDryRunError("runtime_config_error")
    return BoundedNotificationSendDryRunRuntimeConfig(
        notifier_config=notifier_config,
        redis_url=redis_url,
        consumer_group=notifier_config.consumer_group,
        consumer_name=notifier_config.consumer_name,
        block_ms=notifier_config.block_ms,
        xadd_maxlen=xadd_maxlen,
    )


async def run_bounded_notification_send_dry_run(
    config: BoundedNotificationSendDryRunConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotificationSendDryRunConfig],
        BoundedNotificationSendDryRunRuntimeConfig,
    ] = load_bounded_notification_send_dry_run_runtime_config,
    runtime_builder: BoundedNotificationSendDryRunRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationSendDryRunResult:
    state = BoundedNotificationSendDryRunState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)
    runtime: BoundedNotificationSendDryRunRuntime | None = None
    try:
        try:
            runtime_config = runtime_config_loader(config)
            state.runtime_config_loaded = True
        except BoundedNotificationSendDryRunError as exc:
            return _result("blocked", exc.error_code, config=config, state=state)
        except Exception as exc:
            return _result(
                "blocked",
                "runtime_config_error",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
            )
        builder = runtime_builder or build_default_bounded_notification_send_dry_run_runtime
        runtime = await builder(runtime_config, state, logger or logging.getLogger(__name__))
        selection = await runtime.inspect_target(config)
        if selection.message is None:
            return _result(
                "blocked",
                selection.error_code or "redis_target_missing",
                config=config,
                state=state,
                redis_selection=selection,
            )
        trigger_event_id = _uuid_or_none(selection.message.fields.get("trigger_event_id"))
        if trigger_event_id is None:
            return _result("blocked", "trigger_event_id_invalid", config=config, state=state, redis_selection=selection)
        context = await runtime.load_context(trigger_event_id, config)
        if context.error_code is not None:
            return _result(
                "blocked",
                context.error_code,
                config=config,
                state=state,
                redis_selection=selection,
                context=context,
            )
        if config.mode == "preview":
            return _result(
                "pass",
                None,
                config=config,
                state=state,
                redis_selection=selection,
                context=context,
            )
        if not selection.target_is_next:
            return _result(
                "blocked",
                "selected_target_not_next_unconsumed",
                config=config,
                state=state,
                redis_selection=selection,
                context=context,
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
            )
        try:
            execution = await runtime.execute_dry_run(trigger_event_id, config)
        except BoundedNotificationSendDryRunError as exc:
            return _result(
                "failed",
                exc.error_code,
                config=config,
                state=state,
                redis_selection=consumed,
                context=context,
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
            )
        maintenance_message_id: str | None = None
        if execution.delivery_result_event_row.status == "published":
            ack_count = await runtime.ack(consumed.message.message_id)
            acked = ack_count == 1
            return _result(
                "pass" if acked else "failed",
                None if acked else "redis_ack_failed",
                config=config,
                state=state,
                redis_selection=consumed,
                execution=execution,
                redis_ack_status="acked" if acked else "failed",
                redis_acked_count=ack_count,
            )
        try:
            maintenance_message_id = await runtime.publish_maintenance(execution.delivery_result_event_row)
            await runtime.mark_delivery_result_published(execution.delivery_result_event_row.event_id)
        except BoundedNotificationSendDryRunError as exc:
            return _result(
                "failed",
                exc.error_code,
                config=config,
                state=state,
                redis_selection=consumed,
                execution=_with_maintenance_message(execution, maintenance_message_id),
                q_maintenance_message_id_present=bool(maintenance_message_id),
            )
        except Exception as exc:
            return _result(
                "failed",
                "q_maintenance_publish_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_selection=consumed,
                execution=_with_maintenance_message(execution, maintenance_message_id),
                q_maintenance_message_id_present=bool(maintenance_message_id),
            )
        ack_count = await runtime.ack(consumed.message.message_id)
        acked = ack_count == 1
        return _result(
            "pass" if acked else "failed",
            None if acked else "redis_ack_failed",
            config=config,
            state=state,
            redis_selection=consumed,
            execution=_with_maintenance_message(execution, maintenance_message_id, marked=True),
            q_maintenance_published=True,
            q_maintenance_message_id_present=bool(maintenance_message_id),
            redis_ack_status="acked" if acked else "failed",
            redis_acked_count=ack_count,
        )
    except Exception as exc:
        return _result(
            "failed",
            "bounded_notification_send_dry_run_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )
    finally:
        if runtime is not None:
            try:
                await runtime.close()
            except Exception:
                pass


def run_bounded_notification_send_dry_run_sync(
    config: BoundedNotificationSendDryRunConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotificationSendDryRunConfig],
        BoundedNotificationSendDryRunRuntimeConfig,
    ] = load_bounded_notification_send_dry_run_runtime_config,
    runtime_builder: BoundedNotificationSendDryRunRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationSendDryRunResult:
    return asyncio.run(
        run_bounded_notification_send_dry_run(
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
        config=BoundedNotificationSendDryRunConfig(),
        state=BoundedNotificationSendDryRunState(),
    ).to_sanitized_dict()


async def _load_context(
    repository: NotifierTelegramRepository,
    renderer: NotificationRenderer,
    trigger_event_id: UUID,
    config: BoundedNotificationSendDryRunConfig,
) -> NotificationSendContext:
    event_raw = await repository.load_event_outbox(trigger_event_id)
    if event_raw is None:
        raise BoundedNotificationSendDryRunError("notification_plan_event_missing")
    event_row = _outbox_event_row(event_raw)
    event_error = _event_row_error(event_row, config)
    if event_error is not None:
        return _error_context(trigger_event_id, event_row, event_error)
    intent = await repository.load_intent_job(trigger_event_id)
    if intent is None:
        return _error_context(trigger_event_id, event_row, "malformed_notification_intent_payload")
    selector_error = _intent_selector_error(intent, event_row, config)
    if selector_error is not None:
        return _error_context(trigger_event_id, event_row, selector_error, intent=intent)
    analysis = await repository.load_analysis(intent.analysis_id)
    if analysis is None or analysis.candidate_group_id != intent.candidate_group_id:
        return _error_context(trigger_event_id, event_row, "notification_intent_context_mismatch", intent=intent)
    if analysis.delivery_decision != intent.delivery_decision:
        return _error_context(trigger_event_id, event_row, "notification_delivery_decision_mismatch", intent=intent)
    candidate = await repository.load_candidate_render_context(intent.candidate_group_id)
    if candidate is None:
        return _error_context(trigger_event_id, event_row, "notification_missing_candidate_render_context", intent=intent)
    judge_output = await repository.load_judge_output_render_fields(analysis.judge_output_id)
    existing_plan = await repository.load_notification_plan(intent.notification_plan_id)
    plan_id = intent.notification_plan_id
    plan_action = "concretize"
    existing_status = None
    if existing_plan is not None:
        plan_id = UUID(str(existing_plan["notification_plan_id"]))
        existing_status = str(existing_plan.get("status") or "")
        plan_action = "reuse_existing_plan"
    else:
        material_existing = await repository.load_existing_plan_by_material(
            analysis_id=intent.analysis_id,
            target_chat_id=intent.target_chat_id,
            material_change_hash=intent.material_change_hash,
        )
        if material_existing is not None:
            plan_id = UUID(str(material_existing["notification_plan_id"]))
            existing_status = str(material_existing.get("status") or "")
            plan_action = "reuse_existing_material_plan"
    send_after = _effective_send_after(existing_plan, intent)
    if _is_future(send_after):
        return NotificationSendContext(
            trigger_event_id=trigger_event_id,
            event_row=event_row,
            intent=intent,
            analysis=analysis,
            judge_output=judge_output,
            candidate=candidate,
            plan_id=plan_id,
            existing_plan_status=existing_status,
            plan_action="defer_until_due" if existing_plan is None else "reuse_existing_plan_defer_until_due",
            render_draft=None,
            render_action="not_due",
            delivery_action="defer_until_due",
            delivery_status=None,
            planned_action="wait_until_send_after",
            error_code="notification_send_after_deferred",
        )
    render = renderer.render(
        notification_plan_id=plan_id,
        payload=RenderInput(
            analysis=analysis,
            judge_output=judge_output,
            candidate=candidate,
            urgency_profile=intent.urgency_profile,
        ),
    )
    existing_render = await repository.load_notification_render_by_hash(
        notification_plan_id=plan_id,
        render_hash=render.render_hash,
    )
    render_action = "reuse_existing_render" if existing_render is not None else "append_render"
    return NotificationSendContext(
        trigger_event_id=trigger_event_id,
        event_row=event_row,
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
        candidate=candidate,
        plan_id=plan_id,
        existing_plan_status=existing_status,
        plan_action=plan_action,
        render_draft=render,
        render_action=render_action,
        delivery_action="suppress_dry_run_no_transport",
        delivery_status="suppressed" if config.mode == "execute" else "would_suppress",
        planned_action="execute_dry_run_delivery" if config.mode == "execute" else "preview_only",
    )


async def _execute_context(
    repository: NotifierTelegramRepository,
    context: NotificationSendContext,
    state: BoundedNotificationSendDryRunState,
) -> NotificationDryRunExecution:
    if context.error_code is not None:
        raise BoundedNotificationSendDryRunError(context.error_code)
    if context.render_draft is None:
        raise BoundedNotificationSendDryRunError("render_not_available")
    write_counts = {
        "notification_plans_insert_calls": 0,
        "notification_renders_insert_calls": 0,
        "notification_delivery_records_insert_calls": 0,
        "notification_plans_status_update_calls": 0,
        "state_transitions_insert_calls": 0,
        "event_outbox_delivery_result_insert_calls": 0,
    }
    plan_id = context.plan_id
    from_state = context.existing_plan_status or "planned"
    if context.plan_action == "concretize":
        state.database_write_attempted = True
        write_counts["notification_plans_insert_calls"] += 1
        plan_id = await repository.insert_notification_plan(
            NotificationPlanDraft(
                notification_plan_id=context.intent.notification_plan_id,
                analysis_id=context.intent.analysis_id,
                candidate_group_id=context.intent.candidate_group_id,
                delivery_decision=context.intent.delivery_decision,
                urgency_profile=context.intent.urgency_profile,
                target_chat_id=context.intent.target_chat_id,
                target_thread_id=context.intent.target_thread_id,
                render_profile=context.intent.render_profile,
                dedupe_subject_key=context.intent.dedupe_subject_key,
                material_change_hash=context.intent.material_change_hash,
                send_after=context.intent.send_after,
                suppress_reason_code=context.intent.suppress_reason_code,
                status="planned",
            )
        )
        from_state = "planned"
    render = context.render_draft
    if render.notification_plan_id != plan_id:
        render = NotificationRenderDraft(
            notification_plan_id=plan_id,
            message_text=render.message_text,
            entities_json=render.entities_json,
            link_preview_options_json=render.link_preview_options_json,
            reply_markup_json=render.reply_markup_json,
            disable_notification=render.disable_notification,
            protect_content=render.protect_content,
            parse_strategy=render.parse_strategy,
            render_hash=render.render_hash,
        )
    existing_delivery_record = await repository.load_suppressed_delivery_record_by_reason(
        notification_plan_id=plan_id,
        transport_error_code=DRY_RUN_REASON_CODE,
    )
    if existing_delivery_record is not None:
        record_id = UUID(str(existing_delivery_record["notification_delivery_record_id"]))
        event_raw = await repository.load_delivery_result_outbox_by_record(
            notification_plan_id=plan_id,
            notification_delivery_record_id=record_id,
        )
        if event_raw is None:
            state.delivery_result_outbox_write_attempted = True
            state.database_write_attempted = True
            write_counts["event_outbox_delivery_result_insert_calls"] += 1
            event_id = await repository.insert_delivery_result_outbox_returning(
                notification_plan_id=plan_id,
                delivery_status=str(existing_delivery_record["delivery_status"]),
                telegram_chat_id=_safe_int(existing_delivery_record.get("telegram_chat_id")),
                telegram_message_id=_safe_int(existing_delivery_record.get("telegram_message_id")),
                notification_delivery_record_id=record_id,
                attempt_count=int(existing_delivery_record.get("attempt_count") or 0),
                transport_error_code=_string_or_none(existing_delivery_record.get("transport_error_code")),
                transport_error_class=_string_or_none(existing_delivery_record.get("transport_error_class")),
                edited=False,
            )
            if event_id is None:
                raise BoundedNotificationSendDryRunError("delivery_result_outbox_insert_failed")
            event_raw = await repository.load_event_outbox(event_id)
        if event_raw is None:
            raise BoundedNotificationSendDryRunError("delivery_result_outbox_readback_failed")
        delivery_result = _delivery_result_from_existing_record(existing_delivery_record)
        updated_context = NotificationSendContext(
            trigger_event_id=context.trigger_event_id,
            event_row=context.event_row,
            intent=context.intent,
            analysis=context.analysis,
            judge_output=context.judge_output,
            candidate=context.candidate,
            plan_id=plan_id,
            existing_plan_status=context.existing_plan_status,
            plan_action=context.plan_action,
            render_draft=render,
            render_action=context.render_action,
            delivery_action="reuse_suppressed_dry_run_no_transport",
            delivery_status=delivery_result.delivery_status,
            planned_action="reuse_existing_dry_run_delivery",
        )
        return NotificationDryRunExecution(
            context=updated_context,
            delivery_result=delivery_result,
            notification_delivery_record_id=record_id,
            delivery_result_event_row=_outbox_event_row(event_raw),
            notifier_owned_write_counts=write_counts,
        )
    state.render_write_attempted = True
    state.database_write_attempted = True
    write_counts["notification_renders_insert_calls"] += 1
    inserted_render_id = await repository.insert_notification_render(render)
    render_action = "append_render" if inserted_render_id is not None else "reuse_existing_render"
    write_counts["notification_plans_status_update_calls"] += 1
    await repository.update_plan_status(notification_plan_id=plan_id, status="rendered")
    write_counts["state_transitions_insert_calls"] += 1
    await repository.insert_state_transition(
        object_type="notification_plan",
        object_id=plan_id,
        from_state=from_state,
        to_state="rendered",
        reason_code="notification_rendered",
    )
    delivery_result = DeliveryResult(
        delivery_status="suppressed",
        telegram_chat_id=None,
        telegram_message_id=None,
        attempt_count=0,
        transport_error_code=DRY_RUN_REASON_CODE,
        transport_error_class=None,
        telegram_response_json={
            "dry_run": True,
            "transport_skipped": True,
            "reason_code": DRY_RUN_REASON_CODE,
            "delivery_action": "noop",
        },
    )
    write_counts["notification_plans_status_update_calls"] += 1
    await repository.update_plan_status(notification_plan_id=plan_id, status=delivery_result.delivery_status)
    state.delivery_record_write_attempted = True
    write_counts["notification_delivery_records_insert_calls"] += 1
    record_id = await repository.insert_delivery_record(
        notification_plan_id=plan_id,
        result_status=delivery_result.delivery_status,
        telegram_chat_id=delivery_result.telegram_chat_id,
        telegram_message_id=delivery_result.telegram_message_id,
        attempt_count=delivery_result.attempt_count,
        transport_error_code=delivery_result.transport_error_code,
        transport_error_class=delivery_result.transport_error_class,
        telegram_response_json=delivery_result.telegram_response_json,
    )
    write_counts["state_transitions_insert_calls"] += 1
    await repository.insert_state_transition(
        object_type="notification_plan",
        object_id=plan_id,
        from_state="rendered",
        to_state=delivery_result.delivery_status,
        reason_code=DRY_RUN_REASON_CODE,
    )
    state.delivery_result_outbox_write_attempted = True
    write_counts["event_outbox_delivery_result_insert_calls"] += 1
    event_id = await repository.insert_delivery_result_outbox_returning(
        notification_plan_id=plan_id,
        delivery_status=delivery_result.delivery_status,
        telegram_chat_id=delivery_result.telegram_chat_id,
        telegram_message_id=delivery_result.telegram_message_id,
        notification_delivery_record_id=record_id,
        attempt_count=delivery_result.attempt_count,
        transport_error_code=delivery_result.transport_error_code,
        transport_error_class=delivery_result.transport_error_class,
        edited=delivery_result.edited,
    )
    if event_id is None:
        raise BoundedNotificationSendDryRunError("delivery_result_outbox_insert_failed")
    event_raw = await repository.load_event_outbox(event_id)
    if event_raw is None:
        raise BoundedNotificationSendDryRunError("delivery_result_outbox_readback_failed")
    updated_context = NotificationSendContext(
        trigger_event_id=context.trigger_event_id,
        event_row=context.event_row,
        intent=context.intent,
        analysis=context.analysis,
        judge_output=context.judge_output,
        candidate=context.candidate,
        plan_id=plan_id,
        existing_plan_status=context.existing_plan_status,
        plan_action=context.plan_action,
        render_draft=render,
        render_action=render_action,
        delivery_action="suppress_dry_run_no_transport",
        delivery_status=delivery_result.delivery_status,
        planned_action="execute_dry_run_delivery",
    )
    return NotificationDryRunExecution(
        context=updated_context,
        delivery_result=delivery_result,
        notification_delivery_record_id=record_id,
        delivery_result_event_row=_outbox_event_row(event_raw),
        notifier_owned_write_counts=write_counts,
    )


def _delivery_result_from_existing_record(record: Mapping[str, Any]) -> DeliveryResult:
    return DeliveryResult(
        delivery_status=str(record["delivery_status"]),  # type: ignore[arg-type]
        telegram_chat_id=_safe_int(record.get("telegram_chat_id")),
        telegram_message_id=_safe_int(record.get("telegram_message_id")),
        attempt_count=int(record.get("attempt_count") or 0),
        transport_error_code=_string_or_none(record.get("transport_error_code")),
        transport_error_class=_string_or_none(record.get("transport_error_class")),
        telegram_response_json=_json_loads(record.get("telegram_response_json")),
    )


def _authority_gate_error(config: BoundedNotificationSendDryRunConfig) -> str | None:
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
    suffix_error = _target_suffix_error(config)
    if suffix_error is not None:
        return suffix_error
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
        if not config.allow_maintenance_outbox_publish:
            return "maintenance_outbox_publish_not_allowed"
        if not config.allow_maintenance_redis_publish:
            return "maintenance_redis_publish_not_allowed"
        if not config.allow_redis_ack:
            return "redis_ack_not_allowed"
    return None


def _target_suffix_error(config: BoundedNotificationSendDryRunConfig) -> str | None:
    if not _valid_uuid_suffix(config.trigger_event_suffix):
        return "trigger_event_suffix_missing_or_invalid"
    if config.notification_plan_id_suffix and not _valid_uuid_suffix(config.notification_plan_id_suffix):
        return "notification_plan_id_suffix_invalid"
    if config.analysis_id_suffix and not _valid_uuid_suffix(config.analysis_id_suffix):
        return "analysis_id_suffix_invalid"
    if config.redis_message_suffix and not _valid_message_suffix(config.redis_message_suffix):
        return "redis_message_suffix_invalid"
    return None


def _valid_uuid_suffix(value: str | None) -> bool:
    return bool(value and UUID_SUFFIX_RE.fullmatch(value.strip().lower()))


def _valid_message_suffix(value: str | None) -> bool:
    text = str(value or "").strip()
    return bool(text and MESSAGE_ID_SUFFIX_RE.fullmatch(text) and not FULL_REDIS_STREAM_ID_RE.fullmatch(text))


def _select_exact_message(
    config: BoundedNotificationSendDryRunConfig,
    messages: list[StreamMessage],
    *,
    group_pending: int | None,
    group_lag: int | None,
) -> RedisTargetSelection:
    matching: list[tuple[StreamMessage, tuple[str, ...]]] = []
    malformed_failures: list[str] = []
    for message in messages:
        failures = _thin_message_failures(message, config)
        if not failures:
            matching.append((message, failures))
        elif any(code in failures for code in ("forbidden_redis_payload_field", "missing_required_redis_payload_field")):
            malformed_failures.extend(failures)
    if malformed_failures and not matching:
        return _selection_error(
            malformed_failures[0],
            redis_message_count=len(messages),
            group_pending=group_pending,
            group_lag=group_lag,
            checks_failed=tuple(sorted(set(malformed_failures))),
        )
    if len(matching) != 1:
        return _selection_error(
            "redis_target_ambiguous_or_missing",
            redis_message_count=len(messages),
            group_pending=group_pending,
            group_lag=group_lag,
        )
    message = matching[0][0]
    fields = message.fields
    return RedisTargetSelection(
        status="matched",
        error_code=None,
        message=message,
        redis_message_count=len(messages),
        group_lag=group_lag,
        group_pending=group_pending,
        message_stage_name=fields.get("stage_name"),
        message_root_object_type=fields.get("root_object_type"),
        trigger_event_id_present=bool(str(fields.get("trigger_event_id") or "").strip()),
        analysis_id_present=bool(str(fields.get("root_object_id") or "").strip()),
        target_is_next=bool(messages and messages[0].message_id == message.message_id),
    )


def _thin_message_failures(message: StreamMessage, config: BoundedNotificationSendDryRunConfig) -> tuple[str, ...]:
    failures: list[str] = []
    fields = message.fields
    if message.stream != QUEUE_NAME:
        failures.append("queue_name_not_allowed")
    missing = [name for name in REQUIRED_THIN_QUEUE_FIELDS if name not in fields]
    if missing:
        failures.append("missing_required_redis_payload_field")
    forbidden = [name for name in fields if name in FORBIDDEN_REDIS_FIELDS]
    if forbidden:
        failures.append("forbidden_redis_payload_field")
    if fields.get("stage_name") != STAGE_NAME:
        failures.append("message_stage_mismatch")
    if fields.get("root_object_type") != ROOT_OBJECT_TYPE:
        failures.append("root_object_type_mismatch")
    if config.redis_message_suffix and not message.message_id.endswith(config.redis_message_suffix):
        failures.append("redis_message_id_mismatch")
    trigger_event_id = str(fields.get("trigger_event_id") or "")
    if not trigger_event_id.endswith(str(config.trigger_event_suffix)):
        failures.append("trigger_event_id_mismatch")
    if config.analysis_id_suffix:
        analysis_id = str(fields.get("root_object_id") or "")
        if not analysis_id.endswith(str(config.analysis_id_suffix)):
            failures.append("analysis_mismatch")
    return tuple(failures)


def _event_row_error(row: OutboxEventRow, config: BoundedNotificationSendDryRunConfig) -> str | None:
    if row.event_type != EVENT_TYPE:
        return "notification_plan_event_type_invalid"
    if row.aggregate_type != ROOT_OBJECT_TYPE:
        return "notification_plan_event_aggregate_type_invalid"
    if _payload_has_forbidden_key(row.payload_json):
        return "malformed_notification_intent_payload"
    if config.trigger_event_suffix and not str(row.event_id).endswith(config.trigger_event_suffix):
        return "trigger_event_id_mismatch"
    return None


def _intent_selector_error(
    intent: NotificationIntentJob,
    row: OutboxEventRow,
    config: BoundedNotificationSendDryRunConfig,
) -> str | None:
    if row.aggregate_id != intent.analysis_id:
        return "notification_plan_event_aggregate_mismatch"
    if config.notification_plan_id_suffix and not str(intent.notification_plan_id).endswith(
        config.notification_plan_id_suffix
    ):
        return "notification_plan_id_mismatch"
    if config.analysis_id_suffix and not str(intent.analysis_id).endswith(config.analysis_id_suffix):
        return "analysis_mismatch"
    return None


def _payload_has_forbidden_key(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key) in FORBIDDEN_INTENT_PAYLOAD_KEYS:
                return True
            if _payload_has_forbidden_key(nested):
                return True
    if isinstance(value, list):
        return any(_payload_has_forbidden_key(item) for item in value)
    return False


def _error_context(
    trigger_event_id: UUID,
    event_row: OutboxEventRow,
    error_code: str,
    *,
    intent: NotificationIntentJob | None = None,
) -> NotificationSendContext:
    safe_intent = intent or NotificationIntentJob(
        trigger_event_id=trigger_event_id,
        event_type=EVENT_TYPE,
        notification_plan_id=UUID(int=0),
        analysis_id=UUID(int=0),
        candidate_group_id=UUID(int=0),
        delivery_decision="suppress",
        urgency_profile="suppressed",
        target_chat_id=0,
        target_thread_id=None,
        render_profile=None,
        dedupe_subject_key="",
        material_change_hash="",
        send_after=None,
        suppress_reason_code=None,
    )
    zero_analysis = AnalysisRenderContext(
        analysis_id=safe_intent.analysis_id,
        candidate_group_id=safe_intent.candidate_group_id,
        judge_output_id=UUID(int=0),
        verdict="unknown",
        delivery_decision=safe_intent.delivery_decision,
        reason_codes_json=[],
        evidence_limitations_ko=None,
        recommended_action_ko=None,
        freshness_note_ko=None,
    )
    zero_candidate = CandidateRenderContext(
        candidate_group_id=safe_intent.candidate_group_id,
        source_message_id=None,
        current_primary_artifact_id=None,
        primary_artifact_type=None,
        primary_canonical_url=None,
        primary_canonical_id=None,
        source_message_link=None,
        source_text_surface=None,
    )
    return NotificationSendContext(
        trigger_event_id=trigger_event_id,
        event_row=event_row,
        intent=safe_intent,
        analysis=zero_analysis,
        judge_output=None,
        candidate=zero_candidate,
        plan_id=safe_intent.notification_plan_id,
        existing_plan_status=None,
        plan_action="not_evaluated",
        render_draft=None,
        render_action="not_evaluated",
        delivery_action="not_evaluated",
        delivery_status=None,
        planned_action="blocked",
        error_code=error_code,
    )


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotificationSendDryRunConfig,
    state: BoundedNotificationSendDryRunState,
    error_class: str | None = None,
    redis_selection: RedisTargetSelection | None = None,
    context: NotificationSendContext | None = None,
    execution: NotificationDryRunExecution | None = None,
    q_maintenance_published: bool = False,
    q_maintenance_message_id_present: bool = False,
    redis_ack_status: str = "not_attempted",
    redis_acked_count: int = 0,
) -> BoundedNotificationSendDryRunResult:
    return BoundedNotificationSendDryRunResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        redis_selection=redis_selection,
        context=context,
        execution=execution,
        q_maintenance_published=q_maintenance_published,
        q_maintenance_message_id_present=q_maintenance_message_id_present,
        redis_ack_status=redis_ack_status,
        redis_acked_count=redis_acked_count,
    )


def _with_maintenance_message(
    execution: NotificationDryRunExecution,
    message_id: str | None,
    *,
    marked: bool = False,
) -> NotificationDryRunExecution:
    return NotificationDryRunExecution(
        context=execution.context,
        delivery_result=execution.delivery_result,
        notification_delivery_record_id=execution.notification_delivery_record_id,
        delivery_result_event_row=execution.delivery_result_event_row,
        q_maintenance_message_id=message_id,
        q_maintenance_marked_published=marked,
        notifier_owned_write_counts=execution.notifier_owned_write_counts,
    )


def _outbox_event_row(row: Mapping[str, Any]) -> OutboxEventRow:
    payload = row.get("payload_json") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=dict(payload or {}),
        status=str(row["status"]),
        fail_count=int(row.get("fail_count") or 0),
        created_at=row.get("created_at") if isinstance(row.get("created_at"), datetime) else _utc_now(),
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


def _selection_error(
    error_code: str,
    *,
    redis_message_count: int = 0,
    group_pending: int | None = None,
    group_lag: int | None = None,
    message_stage_name: str | None = None,
    message_root_object_type: str | None = None,
    trigger_event_id_present: bool = False,
    analysis_id_present: bool = False,
    checks_failed: tuple[str, ...] = (),
) -> RedisTargetSelection:
    return RedisTargetSelection(
        status="blocked",
        error_code=error_code,
        redis_message_count=redis_message_count,
        group_lag=group_lag,
        group_pending=group_pending,
        message_stage_name=message_stage_name,
        message_root_object_type=message_root_object_type,
        trigger_event_id_present=trigger_event_id_present,
        analysis_id_present=analysis_id_present,
        checks_failed=checks_failed,
    )


def _stream_message_from_xrange(queue_name: str, entry: object) -> StreamMessage:
    message_id, fields = entry  # type: ignore[misc]
    return StreamMessage(
        stream=queue_name,
        message_id=_decode_redis_value(message_id),
        fields={_decode_redis_value(key): _decode_redis_value(value) for key, value in dict(fields).items()},
    )


def _stream_messages_from_xreadgroup(raw: object) -> list[StreamMessage]:
    messages: list[StreamMessage] = []
    for stream_name, entries in raw or []:  # type: ignore[union-attr]
        stream = _decode_redis_value(stream_name)
        for message_id, fields in entries:
            messages.append(
                StreamMessage(
                    stream=stream,
                    message_id=_decode_redis_value(message_id),
                    fields={_decode_redis_value(key): _decode_redis_value(value) for key, value in fields.items()},
                )
            )
    return messages


def _find_group(groups: object, consumer_group: str) -> dict[Any, Any] | None:
    for group in groups or []:  # type: ignore[union-attr]
        if not isinstance(group, dict):
            continue
        name = group.get("name", group.get(b"name"))
        if _decode_redis_value(name) == consumer_group:
            return group
    return None


def _effective_send_after(plan_row: Mapping[str, Any] | None, intent: NotificationIntentJob) -> datetime | None:
    if plan_row is not None and plan_row.get("send_after") is not None:
        value = plan_row["send_after"]
        if isinstance(value, datetime):
            return value
    return intent.send_after


def _is_future(value: datetime | None) -> bool:
    if value is None:
        return False
    return _as_utc(value) > datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decode_redis_value(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _uuid_or_none(value: object) -> UUID | None:
    try:
        return UUID(str(value or ""))
    except (TypeError, ValueError, AttributeError):
        return None


def _uuid_suffix(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if _valid_uuid_suffix(text):
        return text
    try:
        return str(UUID(text))[-8:]
    except (TypeError, ValueError, AttributeError):
        return None


def _message_suffix(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if _valid_message_suffix(text):
        return text
    if not FULL_REDIS_STREAM_ID_RE.fullmatch(text):
        return None
    return text[-12:]


def _safe_exception_class(exc: BaseException) -> str:
    text = type(exc).__name__
    return text if SAFE_TOKEN_RE.fullmatch(text) else "Exception"


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _json_loads(value: object) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, Mapping):
        return dict(value)
    return value


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    value = _env_value(env, name)
    if not value:
        return default
    return int(value)


def _float_env(env: Mapping[str, str], name: str, default: float) -> float:
    value = _env_value(env, name)
    if not value:
        return default
    return float(value)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "BoundedNotificationSendDryRunConfig",
    "BoundedNotificationSendDryRunError",
    "BoundedNotificationSendDryRunResult",
    "BoundedNotificationSendDryRunRuntime",
    "BoundedNotificationSendDryRunRuntimeBuilder",
    "BoundedNotificationSendDryRunRuntimeConfig",
    "DELIVERY_RESULT_EVENT_TYPE",
    "DRY_RUN_REASON_CODE",
    "EVENT_TYPE",
    "QUEUE_NAME",
    "RUNNER_NAME",
    "RedisTargetSelection",
    "argument_error_report",
    "build_default_bounded_notification_send_dry_run_runtime",
    "load_bounded_notification_send_dry_run_runtime_config",
    "render_sanitized_json",
    "run_bounded_notification_send_dry_run",
    "run_bounded_notification_send_dry_run_sync",
]
