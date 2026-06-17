from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .idempotency import classify_notifier_idempotency_state
from .models import (
    DeliveryResult,
    NotificationIntentJob,
    NotifierIdempotencyReadback,
    StreamMessage,
)
from .repositories import NotifierTelegramRepository
from .service import NotifierIdempotencyGuardError, NotifierTelegramService


SCHEMA_VERSION = "bounded_notifier_idempotent_noop_worker_once_v1"
RUNNER_NAME = "bounded_notifier_idempotent_noop_worker_once"
QUEUE_NAME = "q.notification.send"
EXPECTED_STAGE_NAME = "notify"
EXPECTED_ROOT_OBJECT_TYPE = "analysis"
EVENT_TYPE = "notification.plan.created.v1"
DEFAULT_CONSUMER_GROUP = "notifier-telegram"
DEFAULT_CONSUMER_NAME = RUNNER_NAME
DEFAULT_BLOCK_MS = 1
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")
MESSAGE_ID_SUFFIX_RE = re.compile(r"^[0-9][0-9-]{0,63}$")
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_]{1,120}$")
ACK_SAFE_CLASSIFICATIONS = {"existing_plan_sent", "existing_terminal_delivery"}
ACK_SAFE_NOOP_REASONS = {
    "notification_duplicate_noop",
    "duplicate_existing_state",
    "notification_already_delivered_noop",
    "notification_duplicate_terminal_noop",
}


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotentNoopWorkerOnceConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write_for_notifier_noop_only: bool = False
    allow_redis_read: bool = False
    allow_redis_consume: bool = False
    allow_redis_ack: bool = False
    require_telegram_disabled: bool = False
    mode: str = "preview"
    queue_name: str = QUEUE_NAME
    redis_message_id_suffix: str | None = None
    trigger_event_suffix: str | None = None
    analysis_suffix: str | None = None


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotentNoopRuntimeConfig:
    notifier_config: NotifierTelegramConfig
    redis_url: str
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    consumer_name: str = DEFAULT_CONSUMER_NAME
    block_ms: int = DEFAULT_BLOCK_MS


@dataclass(slots=True)
class BoundedNotifierIdempotentNoopWorkerOnceState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    redis_client_created: bool = False
    redis_read_attempted: bool = False
    redis_consume_called: bool = False
    redis_ack_attempted: bool = False
    notifier_called: bool = False
    telegram_send_called: bool = False
    telegram_edit_called: bool = False
    database_committed: bool = False
    database_rolled_back: bool = False


class BoundedNotifierIdempotentNoopWorkerOnceError(RuntimeError):
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


class BoundedNotifierIdempotentNoopRuntime(Protocol):
    async def inspect_target(
        self,
        config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    ) -> RedisTargetSelection: ...
    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    ) -> RedisTargetSelection: ...
    async def load_intent(self, trigger_event_id: UUID) -> NotificationIntentJob | None: ...
    async def load_readback(self, intent: NotificationIntentJob) -> NotifierIdempotencyReadback: ...
    async def invoke_notifier(self, trigger_event_id: UUID) -> tuple[DeliveryResult | None, Mapping[str, int]]: ...
    async def commit_database(self) -> None: ...
    async def rollback_database(self) -> None: ...
    async def ack(self, message_id: str) -> int: ...
    async def close(self) -> None: ...


class BoundedNotifierIdempotentNoopRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotifierIdempotentNoopRuntimeConfig,
        state: BoundedNotifierIdempotentNoopWorkerOnceState,
        logger: logging.Logger,
    ) -> BoundedNotifierIdempotentNoopRuntime: ...


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotentNoopWorkerOnceResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedNotifierIdempotentNoopWorkerOnceConfig
    state: BoundedNotifierIdempotentNoopWorkerOnceState = field(
        default_factory=BoundedNotifierIdempotentNoopWorkerOnceState
    )
    redis_selection: RedisTargetSelection | None = None
    idempotency_before: NotifierIdempotencyReadback | None = None
    idempotency_after: NotifierIdempotencyReadback | None = None
    handled_result_status: str | None = None
    handled_transport_error_code: str | None = None
    handled_result_edited: bool = False
    notifier_owned_write_counts: Mapping[str, int] = field(default_factory=dict)
    ack_safe_candidate: bool = False
    ack_safe: bool = False
    ack_attempted: bool = False
    acked: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        before = self.idempotency_before
        after = self.idempotency_after
        no_new_plan = _count(after, "plan_count") <= _count(before, "plan_count") if after else None
        no_new_render = _count(after, "render_count") <= _count(before, "render_count") if after else None
        no_new_delivery = (
            _count(after, "delivery_record_count") <= _count(before, "delivery_record_count") if after else None
        )
        selection = self.redis_selection
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "queue_name": self.config.queue_name,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_event_suffix": self.config.trigger_event_suffix,
            "target_analysis_suffix": self.config.analysis_suffix,
            "redis_message_id_suffix": self.config.redis_message_id_suffix,
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "database_read_allowed": self.config.allow_database_read,
            "database_write_for_notifier_noop_only_allowed": (
                self.config.allow_database_write_for_notifier_noop_only
            ),
            "redis_read_allowed": self.config.allow_redis_read,
            "redis_consume_allowed": self.config.allow_redis_consume,
            "redis_ack_allowed": self.config.allow_redis_ack,
            "require_telegram_disabled": self.config.require_telegram_disabled,
            "runtime_config_loaded": self.state.runtime_config_loaded,
            "database_session_opened": self.state.database_session_opened,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "database_committed": self.state.database_committed,
            "database_rolled_back": self.state.database_rolled_back,
            "redis_client_created": self.state.redis_client_created,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_consume_called": self.state.redis_consume_called,
            "redis_ack_attempted": self.state.redis_ack_attempted,
            "redis_message_count": selection.redis_message_count if selection else 0,
            "redis_group_lag": selection.group_lag if selection else None,
            "redis_group_pending": selection.group_pending if selection else None,
            "message_stage_name": selection.message_stage_name if selection else None,
            "message_root_object_type": selection.message_root_object_type if selection else None,
            "trigger_event_id_present": selection.trigger_event_id_present if selection else False,
            "analysis_id_present": selection.analysis_id_present if selection else False,
            "notifier_called": self.state.notifier_called,
            "handled_result_status": self.handled_result_status,
            "handled_transport_error_code": self.handled_transport_error_code,
            "handled_result_edited": self.handled_result_edited,
            "idempotency_classification_before": (
                before.primary_classification if before is not None else None
            ),
            "idempotency_classifications_before": list(before.classifications) if before is not None else [],
            "idempotency_classification_after": after.primary_classification if after is not None else None,
            "idempotency_classifications_after": list(after.classifications) if after is not None else [],
            "pre_notification_plan_count": _count(before, "plan_count"),
            "post_notification_plan_count": _count(after, "plan_count"),
            "pre_notification_render_count": _count(before, "render_count"),
            "post_notification_render_count": _count(after, "render_count"),
            "pre_notification_delivery_record_count": _count(before, "delivery_record_count"),
            "post_notification_delivery_record_count": _count(after, "delivery_record_count"),
            "pre_sent_delivery_count": _count(before, "sent_delivery_count"),
            "post_sent_delivery_count": _count(after, "sent_delivery_count"),
            "pre_suppressed_delivery_count": _count(before, "suppressed_delivery_count"),
            "post_suppressed_delivery_count": _count(after, "suppressed_delivery_count"),
            "pre_sent_delivery_chat_id_present_count": _count(
                before, "sent_delivery_chat_id_present_count"
            ),
            "pre_sent_delivery_message_id_present_count": _count(
                before, "sent_delivery_message_id_present_count"
            ),
            "no_new_plan_created": no_new_plan,
            "no_new_render_created": no_new_render,
            "no_new_delivery_record_created": no_new_delivery,
            "notifier_owned_write_counts": dict(self.notifier_owned_write_counts),
            "ack_safe_candidate": self.ack_safe_candidate,
            "ack_safe": self.ack_safe,
            "ack_attempted": self.ack_attempted,
            "acked": self.acked,
            "telegram_send_called": self.state.telegram_send_called,
            "telegram_edit_called": self.state.telegram_edit_called,
            "openai_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "docker_or_systemd_called": False,
            "subprocess_started": False,
            "run_forever_started": False,
            "worker_loop_started": False,
            "alembic_or_ddl_ran": False,
            "redactions_applied": {
                "full_event_id_omitted": True,
                "full_analysis_id_omitted": True,
                "full_redis_message_id_omitted": True,
                "full_notification_plan_ids_omitted": True,
                "target_chat_id_omitted": True,
                "telegram_chat_id_omitted": True,
                "telegram_message_id_omitted": True,
                "redis_payload_omitted": True,
                "message_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "telegram_token_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class ForbiddenTelegramNoopProbe:
    def __init__(self, state: BoundedNotifierIdempotentNoopWorkerOnceState) -> None:
        self._state = state

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self._state.telegram_send_called = True
        raise AssertionError("telegram send is forbidden for idempotent no-op worker-once")

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self._state.telegram_edit_called = True
        raise AssertionError("telegram edit is forbidden for idempotent no-op worker-once")


class CountingNotifierNoopRepository:
    def __init__(self, wrapped: NotifierTelegramRepository) -> None:
        self._wrapped = wrapped
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
        self.write_counts["notification_plans_insert_calls"] += 1
        return await self._wrapped.insert_notification_plan(*args, **kwargs)

    async def insert_notification_render(self, *args: Any, **kwargs: Any) -> UUID | None:
        self.write_counts["notification_renders_insert_calls"] += 1
        return await self._wrapped.insert_notification_render(*args, **kwargs)

    async def insert_delivery_record(self, *args: Any, **kwargs: Any) -> UUID:
        self.write_counts["notification_delivery_records_insert_calls"] += 1
        return await self._wrapped.insert_delivery_record(*args, **kwargs)

    async def update_plan_status(self, *args: Any, **kwargs: Any) -> None:
        self.write_counts["notification_plans_status_update_calls"] += 1
        await self._wrapped.update_plan_status(*args, **kwargs)

    async def insert_state_transition(self, *args: Any, **kwargs: Any) -> None:
        self.write_counts["state_transitions_insert_calls"] += 1
        await self._wrapped.insert_state_transition(*args, **kwargs)

    async def insert_delivery_result_outbox(self, *args: Any, **kwargs: Any) -> None:
        self.write_counts["event_outbox_delivery_result_insert_calls"] += 1
        await self._wrapped.insert_delivery_result_outbox(*args, **kwargs)


class RedisExactNextNotificationConsumer:
    def __init__(
        self,
        client: Any,
        *,
        queue_name: str,
        consumer_group: str,
        consumer_name: str,
        block_ms: int,
        state: BoundedNotifierIdempotentNoopWorkerOnceState,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._state = state

    async def inspect_target(
        self,
        config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    ) -> RedisTargetSelection:
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
        if lag != 1:
            return _selection_error("redis_unconsumed_target_not_exact", group_pending=pending, group_lag=lag)
        entries = await self._client.xrange(self._queue_name, min=f"({last_delivered_id}", max="+", count=2)
        messages = [_stream_message_from_xrange(self._queue_name, entry) for entry in entries or []]
        return _select_exact_message(config, messages, group_pending=pending, group_lag=lag)

    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    ) -> RedisTargetSelection:
        self._state.redis_consume_called = True
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
                group_pending=selection.group_pending,
                group_lag=selection.group_lag,
                redis_message_count=selection.redis_message_count,
            )
        return selection

    async def ack(self, message_id: str) -> int:
        self._state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._consumer_group, message_id)
        return int(result or 0)


class DefaultBoundedNotifierIdempotentNoopRuntime:
    def __init__(
        self,
        *,
        redis_consumer: RedisExactNextNotificationConsumer,
        redis_client: Any,
        repository: CountingNotifierNoopRepository,
        service: NotifierTelegramService,
        session: Any,
        session_context: Any,
        engine: Any,
        state: BoundedNotifierIdempotentNoopWorkerOnceState,
    ) -> None:
        self._redis_consumer = redis_consumer
        self._redis_client = redis_client
        self._repository = repository
        self._service = service
        self._session = session
        self._session_context = session_context
        self._engine = engine
        self._state = state
        self._database_closed = False

    async def inspect_target(
        self,
        config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    ) -> RedisTargetSelection:
        return await self._redis_consumer.inspect_target(config)

    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    ) -> RedisTargetSelection:
        return await self._redis_consumer.consume_target(expected, config)

    async def load_intent(self, trigger_event_id: UUID) -> NotificationIntentJob | None:
        self._state.database_read_attempted = True
        return await self._repository.load_intent_job(trigger_event_id)

    async def load_readback(self, intent: NotificationIntentJob) -> NotifierIdempotencyReadback:
        self._state.database_read_attempted = True
        return classify_notifier_idempotency_state(await self._repository.load_idempotency_plan_snapshots(intent))

    async def invoke_notifier(self, trigger_event_id: UUID) -> tuple[DeliveryResult | None, Mapping[str, int]]:
        self._state.notifier_called = True
        result = await self._service.handle_trigger_event(trigger_event_id)
        write_counts = dict(self._repository.write_counts)
        self._state.database_write_attempted = any(value > 0 for value in write_counts.values())
        return result, write_counts

    async def commit_database(self) -> None:
        if self._database_closed:
            return
        await self._session_context.__aexit__(None, None, None)
        self._database_closed = True
        self._state.database_committed = True

    async def rollback_database(self) -> None:
        if self._database_closed:
            return
        await self._session.rollback()
        await self._session_context.__aexit__(None, None, None)
        self._database_closed = True
        self._state.database_rolled_back = True

    async def ack(self, message_id: str) -> int:
        return await self._redis_consumer.ack(message_id)

    async def close(self) -> None:
        if not self._database_closed:
            try:
                await self.rollback_database()
            except Exception:
                pass
        close = getattr(self._redis_client, "aclose", None) or getattr(self._redis_client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result
        await self._engine.dispose()


async def build_default_bounded_notifier_idempotent_noop_runtime(
    runtime_config: BoundedNotifierIdempotentNoopRuntimeConfig,
    state: BoundedNotifierIdempotentNoopWorkerOnceState,
    logger: logging.Logger,
) -> BoundedNotifierIdempotentNoopRuntime:
    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.notifier_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_client_created = True
    redis_consumer = RedisExactNextNotificationConsumer(
        redis_client,
        queue_name=runtime_config.notifier_config.queue_name,
        consumer_group=runtime_config.consumer_group,
        consumer_name=runtime_config.consumer_name,
        block_ms=runtime_config.block_ms,
        state=state,
    )
    repository = CountingNotifierNoopRepository(NotifierTelegramRepository(session))
    service = NotifierTelegramService(
        runtime_config.notifier_config,
        repository=repository,  # type: ignore[arg-type]
        telegram_client=ForbiddenTelegramNoopProbe(state),  # type: ignore[arg-type]
        logger=logger,
    )
    return DefaultBoundedNotifierIdempotentNoopRuntime(
        redis_consumer=redis_consumer,
        redis_client=redis_client,
        repository=repository,
        service=service,
        session=session,
        session_context=session_context,
        engine=engine,
        state=state,
    )


def load_bounded_notifier_idempotent_noop_runtime_config(
    config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    env: Mapping[str, str] | None = None,
) -> BoundedNotifierIdempotentNoopRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedNotifierIdempotentNoopWorkerOnceError("database_url_missing")
    if not redis_url:
        raise BoundedNotifierIdempotentNoopWorkerOnceError("redis_url_missing")
    try:
        notifier_config = NotifierTelegramConfig(
            app_env=_env_value(source, "APP_ENV", "dev").lower() or "dev",
            database_url=database_url,
            redis_url=redis_url,
            telegram_bot_token=_env_value(source, "TELEGRAM_BOT_TOKEN"),
            queue_name=config.queue_name,
            consumer_group=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
            consumer_name=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_NAME", DEFAULT_CONSUMER_NAME),
            batch_size=1,
            block_ms=DEFAULT_BLOCK_MS,
            dry_run=_bool_env(_env_value(source, "NOTIFIER_TELEGRAM_DRY_RUN", "true")),
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
        raise BoundedNotifierIdempotentNoopWorkerOnceError("runtime_config_error") from exc
    return BoundedNotifierIdempotentNoopRuntimeConfig(
        notifier_config=notifier_config,
        redis_url=redis_url,
        consumer_group=notifier_config.consumer_group,
        consumer_name=notifier_config.consumer_name,
        block_ms=notifier_config.block_ms,
    )


async def run_bounded_notifier_idempotent_noop_worker_once(
    config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotifierIdempotentNoopWorkerOnceConfig],
        BoundedNotifierIdempotentNoopRuntimeConfig,
    ] = load_bounded_notifier_idempotent_noop_runtime_config,
    runtime_builder: BoundedNotifierIdempotentNoopRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierIdempotentNoopWorkerOnceResult:
    state = BoundedNotifierIdempotentNoopWorkerOnceState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)
    runtime: BoundedNotifierIdempotentNoopRuntime | None = None
    try:
        try:
            runtime_config = runtime_config_loader(config)
            state.runtime_config_loaded = True
        except BoundedNotifierIdempotentNoopWorkerOnceError as exc:
            return _result("blocked", exc.error_code, config=config, state=state)
        except Exception as exc:
            return _result(
                "blocked",
                "runtime_config_error",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
            )
        if config.require_telegram_disabled and runtime_config.notifier_config.transport_enabled:
            return _result("blocked", "telegram_transport_enabled", config=config, state=state)

        builder = runtime_builder or build_default_bounded_notifier_idempotent_noop_runtime
        runtime = await builder(runtime_config, state, logger or logging.getLogger(__name__))
        selection = await runtime.inspect_target(config)
        if selection.message is None:
            return _result("blocked", selection.error_code or "redis_target_missing", config=config, state=state, redis_selection=selection)

        trigger_event_id = _uuid_or_none(selection.message.fields.get("trigger_event_id"))
        if trigger_event_id is None:
            return _result("blocked", "trigger_event_id_invalid", config=config, state=state, redis_selection=selection)
        intent = await runtime.load_intent(trigger_event_id)
        if intent is None or intent.event_type != EVENT_TYPE:
            return _result("blocked", "notification_intent_missing", config=config, state=state, redis_selection=selection)
        if not str(intent.trigger_event_id).endswith(str(config.trigger_event_suffix)):
            return _result("blocked", "trigger_event_id_mismatch", config=config, state=state, redis_selection=selection)
        if not str(intent.analysis_id).endswith(str(config.analysis_suffix)):
            return _result("blocked", "analysis_mismatch", config=config, state=state, redis_selection=selection)

        before = await runtime.load_readback(intent)
        ack_candidate = _pre_readback_ack_safe(before)
        if config.mode == "preview":
            return _result(
                "pass" if ack_candidate else "blocked",
                None if ack_candidate else "pre_readback_not_ack_safe",
                config=config,
                state=state,
                redis_selection=selection,
                idempotency_before=before,
                idempotency_after=before,
                ack_safe_candidate=ack_candidate,
            )
        if not ack_candidate:
            return _result(
                "blocked",
                "pre_readback_not_ack_safe",
                config=config,
                state=state,
                redis_selection=selection,
                idempotency_before=before,
            )

        consumed = await runtime.consume_target(selection.message, config)
        if consumed.message is None:
            return _result(
                "blocked",
                consumed.error_code or "redis_consume_target_missing",
                config=config,
                state=state,
                redis_selection=consumed,
                idempotency_before=before,
            )

        try:
            delivery_result, write_counts = await runtime.invoke_notifier(trigger_event_id)
        except NotifierIdempotencyGuardError as exc:
            return _result(
                "failed",
                getattr(exc, "reason_code", "duplicate_existing_state"),
                error_class=type(exc).__name__,
                config=config,
                state=state,
                redis_selection=consumed,
                idempotency_before=before,
            )
        except Exception as exc:
            return _result(
                "failed",
                "notifier_invocation_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_selection=consumed,
                idempotency_before=before,
            )
        if delivery_result is None:
            return _result(
                "failed",
                "notifier_invocation_no_result",
                config=config,
                state=state,
                redis_selection=consumed,
                idempotency_before=before,
                notifier_owned_write_counts=write_counts,
            )
        after = await runtime.load_readback(intent)
        safety_error = _ack_safety_error(before, after, delivery_result, state)
        if safety_error is not None:
            return _result(
                "failed",
                safety_error,
                config=config,
                state=state,
                redis_selection=consumed,
                idempotency_before=before,
                idempotency_after=after,
                handled_result_status=str(delivery_result.delivery_status),
                handled_transport_error_code=_safe_token(delivery_result.transport_error_code),
                handled_result_edited=bool(delivery_result.edited),
                notifier_owned_write_counts=write_counts,
                ack_safe_candidate=True,
            )

        await runtime.commit_database()
        ack_count = await runtime.ack(consumed.message.message_id)
        acked = ack_count == 1
        return _result(
            "pass" if acked else "failed",
            None if acked else "redis_ack_failed",
            config=config,
            state=state,
            redis_selection=consumed,
            idempotency_before=before,
            idempotency_after=after,
            handled_result_status=str(delivery_result.delivery_status),
            handled_transport_error_code=_safe_token(delivery_result.transport_error_code),
            handled_result_edited=bool(delivery_result.edited),
            notifier_owned_write_counts=write_counts,
            ack_safe_candidate=True,
            ack_safe=True,
            ack_attempted=True,
            acked=acked,
        )
    except Exception as exc:
        return _result(
            "failed",
            "idempotent_noop_worker_once_failed",
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


def run_bounded_notifier_idempotent_noop_worker_once_sync(
    config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotifierIdempotentNoopWorkerOnceConfig],
        BoundedNotifierIdempotentNoopRuntimeConfig,
    ] = load_bounded_notifier_idempotent_noop_runtime_config,
    runtime_builder: BoundedNotifierIdempotentNoopRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierIdempotentNoopWorkerOnceResult:
    return asyncio.run(
        run_bounded_notifier_idempotent_noop_worker_once(
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
        config=BoundedNotifierIdempotentNoopWorkerOnceConfig(),
        state=BoundedNotifierIdempotentNoopWorkerOnceState(),
    ).to_sanitized_dict()


def _authority_gate_error(config: BoundedNotifierIdempotentNoopWorkerOnceConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if config.mode not in {"preview", "execute"}:
        return "mode_not_allowed"
    if config.queue_name != QUEUE_NAME:
        return "queue_name_not_allowed"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    if not config.require_telegram_disabled:
        return "telegram_disabled_requirement_missing"
    suffix_error = _target_suffix_error(config)
    if suffix_error is not None:
        return suffix_error
    if config.mode == "execute":
        if not config.allow_database_write_for_notifier_noop_only:
            return "database_write_for_notifier_noop_only_not_allowed"
        if not config.allow_redis_consume:
            return "redis_consume_not_allowed"
        if not config.allow_redis_ack:
            return "redis_ack_not_allowed"
    return None


def _target_suffix_error(config: BoundedNotifierIdempotentNoopWorkerOnceConfig) -> str | None:
    if not _valid_uuid_suffix(config.trigger_event_suffix) or not _valid_uuid_suffix(config.analysis_suffix):
        return "suffix_ambiguous_or_missing"
    if config.redis_message_id_suffix and not MESSAGE_ID_SUFFIX_RE.fullmatch(config.redis_message_id_suffix):
        return "redis_message_id_suffix_invalid"
    return None


def _valid_uuid_suffix(value: str | None) -> bool:
    return bool(value and UUID_SUFFIX_RE.fullmatch(value.strip().lower()))


def _select_exact_message(
    config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    messages: list[StreamMessage],
    *,
    group_pending: int | None,
    group_lag: int | None,
) -> RedisTargetSelection:
    if len(messages) != 1:
        return _selection_error(
            "redis_target_ambiguous_or_missing",
            redis_message_count=len(messages),
            group_pending=group_pending,
            group_lag=group_lag,
        )
    message = messages[0]
    fields = message.fields
    base = {
        "redis_message_count": 1,
        "group_pending": group_pending,
        "group_lag": group_lag,
        "message_stage_name": fields.get("stage_name"),
        "message_root_object_type": fields.get("root_object_type"),
        "trigger_event_id_present": bool(str(fields.get("trigger_event_id") or "").strip()),
        "analysis_id_present": bool(str(fields.get("root_object_id") or "").strip()),
    }
    if message.stream != config.queue_name:
        return _selection_error("queue_name_not_allowed", **base)
    if config.redis_message_id_suffix and not message.message_id.endswith(config.redis_message_id_suffix):
        return _selection_error("redis_message_id_mismatch", **base)
    if fields.get("stage_name") != EXPECTED_STAGE_NAME:
        return _selection_error("message_stage_mismatch", **base)
    if fields.get("root_object_type") != EXPECTED_ROOT_OBJECT_TYPE:
        return _selection_error("root_object_type_mismatch", **base)
    trigger_event_id = str(fields.get("trigger_event_id") or "")
    if not trigger_event_id.endswith(str(config.trigger_event_suffix)):
        return _selection_error("trigger_event_id_mismatch", **base)
    analysis_id = str(fields.get("root_object_id") or "")
    if not analysis_id.endswith(str(config.analysis_suffix)):
        return _selection_error("analysis_mismatch", **base)
    return RedisTargetSelection(status="matched", error_code=None, message=message, **base)


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
    )


def _pre_readback_ack_safe(readback: NotifierIdempotencyReadback) -> bool:
    return readback.primary_classification in ACK_SAFE_CLASSIFICATIONS


def _ack_safety_error(
    before: NotifierIdempotencyReadback,
    after: NotifierIdempotencyReadback,
    result: DeliveryResult,
    state: BoundedNotifierIdempotentNoopWorkerOnceState,
) -> str | None:
    if result.delivery_status in {"sent", "edited"} or result.edited:
        return "result_not_idempotent_noop"
    if result.delivery_status != "suppressed" or result.transport_error_code not in ACK_SAFE_NOOP_REASONS:
        return "result_not_idempotent_noop"
    if state.telegram_send_called or state.telegram_edit_called:
        return "telegram_transport_called"
    if after.primary_classification not in ACK_SAFE_CLASSIFICATIONS:
        return "post_readback_not_ack_safe"
    if (
        after.plan_count > before.plan_count
        or after.render_count > before.render_count
        or after.delivery_record_count > before.delivery_record_count
    ):
        return "post_readback_count_increased"
    return None


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotifierIdempotentNoopWorkerOnceConfig,
    state: BoundedNotifierIdempotentNoopWorkerOnceState,
    error_class: str | None = None,
    redis_selection: RedisTargetSelection | None = None,
    idempotency_before: NotifierIdempotencyReadback | None = None,
    idempotency_after: NotifierIdempotencyReadback | None = None,
    handled_result_status: str | None = None,
    handled_transport_error_code: str | None = None,
    handled_result_edited: bool = False,
    notifier_owned_write_counts: Mapping[str, int] | None = None,
    ack_safe_candidate: bool = False,
    ack_safe: bool = False,
    ack_attempted: bool = False,
    acked: bool = False,
) -> BoundedNotifierIdempotentNoopWorkerOnceResult:
    return BoundedNotifierIdempotentNoopWorkerOnceResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        redis_selection=redis_selection,
        idempotency_before=idempotency_before,
        idempotency_after=idempotency_after,
        handled_result_status=handled_result_status,
        handled_transport_error_code=handled_transport_error_code,
        handled_result_edited=handled_result_edited,
        notifier_owned_write_counts=notifier_owned_write_counts or {},
        ack_safe_candidate=ack_safe_candidate,
        ack_safe=ack_safe,
        ack_attempted=ack_attempted,
        acked=acked,
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


def _decode_redis_value(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _count(readback: NotifierIdempotencyReadback | None, attr_name: str) -> int | None:
    if readback is None:
        return None
    return int(getattr(readback, attr_name))


def _uuid_or_none(value: object) -> UUID | None:
    try:
        return UUID(str(value or ""))
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_exception_class(exc: BaseException) -> str:
    text = type(exc).__name__
    return text if re.fullmatch(r"[A-Za-z0-9_]{1,120}", text) else "Exception"


def _safe_token(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    lowered = text.lower()
    if not SAFE_TOKEN_RE.fullmatch(text):
        return "redacted"
    if any(marker in lowered for marker in ("password", "secret", "token", "credential", "api_key", "url")):
        return "redacted"
    return text


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


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


__all__ = [
    "BoundedNotifierIdempotentNoopRuntime",
    "BoundedNotifierIdempotentNoopRuntimeBuilder",
    "BoundedNotifierIdempotentNoopRuntimeConfig",
    "BoundedNotifierIdempotentNoopWorkerOnceConfig",
    "BoundedNotifierIdempotentNoopWorkerOnceError",
    "BoundedNotifierIdempotentNoopWorkerOnceResult",
    "QUEUE_NAME",
    "RUNNER_NAME",
    "RedisTargetSelection",
    "argument_error_report",
    "build_default_bounded_notifier_idempotent_noop_runtime",
    "load_bounded_notifier_idempotent_noop_runtime_config",
    "render_sanitized_json",
    "run_bounded_notifier_idempotent_noop_worker_once",
    "run_bounded_notifier_idempotent_noop_worker_once_sync",
]
