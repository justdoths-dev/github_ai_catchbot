from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

from .config import MaintenanceConfig, MaintenanceConfigurationError
from .delivery_replay import REPLAY_REQUESTED_EVENT_TYPE
from .delivery_retry import DELIVERY_RESULT_EVENT_TYPE
from .models import DeliveryResultWorkerResult, OutboxEvent, RetryPromotionCandidate, StreamMessage
from .repositories import MaintenanceRepository
from .service import MaintenanceService


SCHEMA_VERSION = "bounded_maintenance_recovery_runner_v1"
RUNNER_NAME = "bounded_maintenance_recovery_runner"
MAINTENANCE_RESULT_COMMAND = "maintenance-result"
REPLAY_REQUEST_COMMAND = "replay-request"
DUE_RETRY_COMMAND = "due-retry"
MAINTENANCE_QUEUE_NAME = "q.maintenance"
REPLAY_QUEUE_NAME = "q.replay"
MAINTENANCE_STAGE_NAME = "maintenance"
REPLAY_STAGE_NAME = "replay"
NOTIFICATION_PLAN_ROOT = "notification_plan"
REPLAY_REQUEST_ROOT = "replay_request"
DEFAULT_CONSUMER_NAME = RUNNER_NAME
DEFAULT_BLOCK_MS = 1
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f]{4,12}$")
MESSAGE_ID_SUFFIX_RE = re.compile(r"^[0-9][0-9-]{3,11}$")
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_:-]{1,120}$")


@dataclass(frozen=True, slots=True)
class BoundedMaintenanceQueueOnceConfig:
    command: str
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    allow_redis_read: bool = False
    allow_redis_consume: bool = False
    allow_redis_ack: bool = False
    mode: str = "preview"
    trigger_event_suffix: str | None = None
    root_object_id_suffix: str | None = None
    redis_message_id_suffix: str | None = None


@dataclass(frozen=True, slots=True)
class BoundedMaintenanceDueRetryConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    mode: str = "preview"
    limit: int = 1
    now_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class BoundedMaintenanceRuntimeConfig:
    maintenance_config: MaintenanceConfig
    queue_command: str | None = None


@dataclass(slots=True)
class BoundedMaintenanceRuntimeState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    database_committed: bool = False
    database_rolled_back: bool = False
    redis_client_created: bool = False
    redis_read_attempted: bool = False
    redis_consume_called: bool = False
    redis_ack_attempted: bool = False
    service_called: bool = False


class BoundedMaintenanceRuntimeError(RuntimeError):
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
    root_object_id_present: bool = False
    redis_message_id_suffix: str | None = None
    trigger_event_id_suffix: str | None = None
    root_object_id_suffix: str | None = None


class BoundedMaintenanceQueueRuntime(Protocol):
    async def inspect_target(self, config: BoundedMaintenanceQueueOnceConfig) -> RedisTargetSelection: ...
    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedMaintenanceQueueOnceConfig,
    ) -> RedisTargetSelection: ...
    async def load_outbox_event(self, trigger_event_id: UUID) -> OutboxEvent | None: ...
    async def invoke_maintenance(self, trigger_event_id: UUID) -> DeliveryResultWorkerResult | None: ...
    async def invoke_replay(self, trigger_event_id: UUID) -> None: ...
    async def commit_database(self) -> None: ...
    async def rollback_database(self) -> None: ...
    async def ack(self, message_id: str) -> int: ...
    async def close(self) -> None: ...


class BoundedMaintenanceQueueRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedMaintenanceRuntimeConfig,
        state: BoundedMaintenanceRuntimeState,
        logger: logging.Logger,
    ) -> BoundedMaintenanceQueueRuntime: ...


class BoundedMaintenanceDueRetryRuntime(Protocol):
    async def preview_candidates(self, limit: int, now: datetime) -> list[RetryPromotionCandidate]: ...
    async def promote_due_retries_once(self, limit: int, now: datetime) -> int: ...
    async def commit_database(self) -> None: ...
    async def rollback_database(self) -> None: ...
    async def close(self) -> None: ...


class BoundedMaintenanceDueRetryRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedMaintenanceRuntimeConfig,
        state: BoundedMaintenanceRuntimeState,
        logger: logging.Logger,
    ) -> BoundedMaintenanceDueRetryRuntime: ...


@dataclass(frozen=True, slots=True)
class BoundedMaintenanceResult:
    command: str
    mode: str
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None = None
    queue_config: BoundedMaintenanceQueueOnceConfig | None = None
    due_config: BoundedMaintenanceDueRetryConfig | None = None
    state: BoundedMaintenanceRuntimeState = field(default_factory=BoundedMaintenanceRuntimeState)
    queue_name: str | None = None
    consumer_group: str | None = None
    redis_selection: RedisTargetSelection | None = None
    service_result: DeliveryResultWorkerResult | None = None
    replay_service_completed: bool = False
    ack_attempted: bool = False
    acked: bool = False
    due_candidate_count: int | None = None
    due_candidate_plan_suffixes: tuple[str, ...] = ()
    due_candidate_latest_delivery_suffixes: tuple[str, ...] = ()
    due_action_count: int | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        selection = self.redis_selection
        queue_config = self.queue_config
        due_config = self.due_config
        service = self.service_result
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "command": self.command,
            "mode": self.mode,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "operator_approved": _operator_approved(queue_config, due_config),
            "runtime_config_allowed": _runtime_config_allowed(queue_config, due_config),
            "database_read_allowed": _database_read_allowed(queue_config, due_config),
            "database_write_allowed": _database_write_allowed(queue_config, due_config),
            "redis_read_allowed": queue_config.allow_redis_read if queue_config else False,
            "redis_consume_allowed": queue_config.allow_redis_consume if queue_config else False,
            "redis_ack_allowed": queue_config.allow_redis_ack if queue_config else False,
            "target_event_suffix": _uuid_suffix_projection(queue_config.trigger_event_suffix) if queue_config else None,
            "target_root_object_id_suffix": _uuid_suffix_projection(queue_config.root_object_id_suffix)
            if queue_config
            else None,
            "redis_message_id_suffix": _message_id_suffix_projection(queue_config.redis_message_id_suffix)
            if queue_config
            else None,
            "queue_name": self.queue_name,
            "consumer_group": self.consumer_group,
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
            "message_trigger_event_id_present": selection.trigger_event_id_present if selection else False,
            "message_root_object_id_present": selection.root_object_id_present if selection else False,
            "selected_redis_message_id_suffix": selection.redis_message_id_suffix if selection else None,
            "selected_trigger_event_id_suffix": selection.trigger_event_id_suffix if selection else None,
            "selected_root_object_id_suffix": selection.root_object_id_suffix if selection else None,
            "service_called": self.state.service_called,
            "service_result_processed": service.processed if service else None,
            "service_result_classification": service.classification if service else None,
            "service_result_action": service.action if service else None,
            "service_result_reason_code": service.reason_code if service else None,
            "replay_service_completed": self.replay_service_completed,
            "ack_attempted": self.ack_attempted,
            "acked": self.acked,
            "due_limit": due_config.limit if due_config else None,
            "due_candidate_count": self.due_candidate_count,
            "due_candidate_plan_suffixes": list(self.due_candidate_plan_suffixes),
            "due_candidate_latest_delivery_suffixes": list(self.due_candidate_latest_delivery_suffixes),
            "due_action_count": self.due_action_count,
            "telegram_send_called": False,
            "telegram_edit_called": False,
            "openai_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "container_manager_called": False,
            "host_service_manager_called": False,
            "subprocess_started": False,
            "run_forever_started": False,
            "worker_loop_started": False,
            "alembic_or_ddl_ran": False,
            "redactions_applied": {
                "full_event_id_omitted": True,
                "full_notification_plan_id_omitted": True,
                "full_replay_request_id_omitted": True,
                "full_redis_message_id_omitted": True,
                "payload_json_omitted": True,
                "rendered_text_omitted": True,
                "target_chat_id_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "runtime_env_values_omitted": True,
                "exception_detail_omitted": True,
            },
            "side_effects": {
                "db_write": self.state.database_write_attempted and self.state.database_committed,
                "redis_consume": self.state.redis_consume_called,
                "redis_ack": self.acked,
                "telegram_send_called": False,
                "telegram_edit_called": False,
                "run_forever_called": False,
            },
        }


class RedisExactNextMaintenanceConsumer:
    def __init__(
        self,
        client: Any,
        *,
        queue_name: str,
        consumer_group: str,
        consumer_name: str,
        block_ms: int,
        state: BoundedMaintenanceRuntimeState,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._state = state

    async def inspect_target(self, config: BoundedMaintenanceQueueOnceConfig) -> RedisTargetSelection:
        self._state.redis_read_attempted = True
        group = await self._load_group()
        if group is None:
            return _selection_error("consumer_group_missing")
        pending = _safe_int(_dict_get(group, "pending"))
        lag = _safe_int(_dict_get(group, "lag"))
        if pending and pending > 0:
            return await self._pending_selection_error(config, group_pending=pending, group_lag=lag)

        last_delivered_id = _decode_redis_value(_dict_get(group, "last-delivered-id") or "0-0")
        try:
            entries = await self._client.xrange(self._queue_name, min=f"({last_delivered_id}", max="+", count=2)
        except Exception:
            return _selection_error("redis_xrange_failed", group_pending=pending, group_lag=lag)
        messages = [_stream_message_from_xrange(self._queue_name, entry) for entry in entries or []]
        return _select_next_exact_message(config, messages, group_pending=pending, group_lag=lag)

    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedMaintenanceQueueOnceConfig,
    ) -> RedisTargetSelection:
        self._state.redis_consume_called = True
        try:
            raw = await self._client.xreadgroup(
                self._consumer_group,
                self._consumer_name,
                {self._queue_name: ">"},
                count=1,
                block=self._block_ms,
            )
        except Exception:
            return _selection_error("redis_xreadgroup_failed")
        messages = _stream_messages_from_xreadgroup(raw)
        selection = _select_next_exact_message(config, messages, group_pending=0, group_lag=1)
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

    async def _load_group(self) -> dict[Any, Any] | None:
        try:
            groups = await self._client.xinfo_groups(self._queue_name)
        except Exception:
            return None
        for group in groups or []:
            if isinstance(group, dict) and _decode_redis_value(_dict_get(group, "name")) == self._consumer_group:
                return group
        return None

    async def _pending_selection_error(
        self,
        config: BoundedMaintenanceQueueOnceConfig,
        *,
        group_pending: int,
        group_lag: int | None,
    ) -> RedisTargetSelection:
        entries: list[Any] = []
        pending_range = getattr(self._client, "xpending_range", None)
        if pending_range is not None:
            try:
                entries = await pending_range(self._queue_name, self._consumer_group, "-", "+", 10)
            except Exception:
                entries = []
        for entry in entries:
            message_id = _pending_message_id(entry)
            if message_id and config.redis_message_id_suffix and message_id.endswith(config.redis_message_id_suffix):
                consumer = _pending_consumer_name(entry)
                if consumer and consumer != self._consumer_name:
                    return _selection_error(
                        "target_pending_under_another_consumer",
                        group_pending=group_pending,
                        group_lag=group_lag,
                    )
                return _selection_error(
                    "target_pending_not_unconsumed",
                    group_pending=group_pending,
                    group_lag=group_lag,
                )
        return _selection_error("redis_pending_messages_present", group_pending=group_pending, group_lag=group_lag)


class DefaultBoundedMaintenanceQueueRuntime:
    def __init__(
        self,
        *,
        runtime_config: BoundedMaintenanceRuntimeConfig,
        redis_consumer: RedisExactNextMaintenanceConsumer,
        redis_client: Any,
        state: BoundedMaintenanceRuntimeState,
        logger: logging.Logger,
    ) -> None:
        self._runtime_config = runtime_config
        self._redis_consumer = redis_consumer
        self._redis_client = redis_client
        self._state = state
        self._logger = logger
        self._engine = None
        self._session_context = None
        self._session = None
        self._repository = None
        self._service = None
        self._database_closed = False

    async def inspect_target(self, config: BoundedMaintenanceQueueOnceConfig) -> RedisTargetSelection:
        return await self._redis_consumer.inspect_target(config)

    async def consume_target(
        self,
        expected: StreamMessage,
        config: BoundedMaintenanceQueueOnceConfig,
    ) -> RedisTargetSelection:
        return await self._redis_consumer.consume_target(expected, config)

    async def load_outbox_event(self, trigger_event_id: UUID) -> OutboxEvent | None:
        await self._ensure_database()
        self._state.database_read_attempted = True
        return await self._repository.load_outbox_event(trigger_event_id)  # type: ignore[union-attr]

    async def invoke_maintenance(self, trigger_event_id: UUID) -> DeliveryResultWorkerResult | None:
        await self._ensure_database()
        self._state.service_called = True
        self._state.database_write_attempted = True
        return await self._service.handle_maintenance_trigger_event(trigger_event_id)  # type: ignore[union-attr]

    async def invoke_replay(self, trigger_event_id: UUID) -> None:
        await self._ensure_database()
        self._state.service_called = True
        self._state.database_write_attempted = True
        await self._service.handle_replay_trigger_event(trigger_event_id)  # type: ignore[union-attr]

    async def commit_database(self) -> None:
        if self._database_closed or self._session_context is None:
            return
        await self._session_context.__aexit__(None, None, None)
        self._database_closed = True
        self._state.database_committed = True

    async def rollback_database(self) -> None:
        if self._database_closed or self._session_context is None:
            return
        await self._session.rollback()
        await self._session_context.__aexit__(None, None, None)
        self._database_closed = True
        self._state.database_rolled_back = True

    async def ack(self, message_id: str) -> int:
        return await self._redis_consumer.ack(message_id)

    async def close(self) -> None:
        if not self._database_closed and self._session_context is not None:
            try:
                await self.rollback_database()
            except Exception:
                pass
        close = getattr(self._redis_client, "aclose", None) or getattr(self._redis_client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result
        if self._engine is not None:
            await self._engine.dispose()

    async def _ensure_database(self) -> None:
        if self._session is not None:
            return
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

        config = self._runtime_config.maintenance_config
        self._engine = create_async_engine(config.database_url, future=True)
        session_factory = async_sessionmaker(self._engine, class_=AsyncSession, expire_on_commit=False)
        self._session_context = session_factory.begin()
        self._session = await self._session_context.__aenter__()
        self._state.database_session_opened = True
        self._repository = MaintenanceRepository(self._session)
        self._service = MaintenanceService(config, repository=self._repository, logger=self._logger)


class DefaultBoundedMaintenanceDueRetryRuntime:
    def __init__(
        self,
        *,
        runtime_config: BoundedMaintenanceRuntimeConfig,
        state: BoundedMaintenanceRuntimeState,
        logger: logging.Logger,
    ) -> None:
        self._runtime_config = runtime_config
        self._state = state
        self._logger = logger
        self._engine = None
        self._session_context = None
        self._session = None
        self._repository = None
        self._service = None
        self._database_closed = False

    async def preview_candidates(self, limit: int, now: datetime) -> list[RetryPromotionCandidate]:
        await self._ensure_database(now)
        self._state.database_read_attempted = True
        return await self._repository.load_due_retry_candidates(limit=limit, now=now)  # type: ignore[union-attr]

    async def promote_due_retries_once(self, limit: int, now: datetime) -> int:
        await self._ensure_database(now)
        self._state.database_read_attempted = True
        self._state.database_write_attempted = True
        self._state.service_called = True
        return await self._service.promote_due_retries_once(limit=limit)  # type: ignore[union-attr]

    async def commit_database(self) -> None:
        if self._database_closed or self._session_context is None:
            return
        await self._session_context.__aexit__(None, None, None)
        self._database_closed = True
        self._state.database_committed = True

    async def rollback_database(self) -> None:
        if self._database_closed or self._session_context is None:
            return
        await self._session.rollback()
        await self._session_context.__aexit__(None, None, None)
        self._database_closed = True
        self._state.database_rolled_back = True

    async def close(self) -> None:
        if not self._database_closed and self._session_context is not None:
            try:
                await self.rollback_database()
            except Exception:
                pass
        if self._engine is not None:
            await self._engine.dispose()

    async def _ensure_database(self, now: datetime) -> None:
        if self._session is not None:
            return
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

        config = self._runtime_config.maintenance_config
        self._engine = create_async_engine(config.database_url, future=True)
        session_factory = async_sessionmaker(self._engine, class_=AsyncSession, expire_on_commit=False)
        self._session_context = session_factory.begin()
        self._session = await self._session_context.__aenter__()
        self._state.database_session_opened = True
        self._repository = MaintenanceRepository(self._session)
        self._service = MaintenanceService(
            config,
            repository=self._repository,
            logger=self._logger,
            now_fn=lambda: now,
        )


async def build_default_bounded_maintenance_queue_runtime(
    runtime_config: BoundedMaintenanceRuntimeConfig,
    state: BoundedMaintenanceRuntimeState,
    logger: logging.Logger,
) -> BoundedMaintenanceQueueRuntime:
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.maintenance_config.redis_url, decode_responses=True)
    state.redis_client_created = True
    queue_name, group_name, consumer_name = _queue_runtime_names(
        runtime_config.maintenance_config,
        runtime_config.queue_command or MAINTENANCE_RESULT_COMMAND,
    )
    redis_consumer = RedisExactNextMaintenanceConsumer(
        redis_client,
        queue_name=queue_name,
        consumer_group=group_name,
        consumer_name=consumer_name,
        block_ms=DEFAULT_BLOCK_MS,
        state=state,
    )
    return DefaultBoundedMaintenanceQueueRuntime(
        runtime_config=runtime_config,
        redis_consumer=redis_consumer,
        redis_client=redis_client,
        state=state,
        logger=logger,
    )


async def build_default_bounded_maintenance_due_retry_runtime(
    runtime_config: BoundedMaintenanceRuntimeConfig,
    state: BoundedMaintenanceRuntimeState,
    logger: logging.Logger,
) -> BoundedMaintenanceDueRetryRuntime:
    return DefaultBoundedMaintenanceDueRetryRuntime(
        runtime_config=runtime_config,
        state=state,
        logger=logger,
    )


def load_bounded_maintenance_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedMaintenanceRuntimeConfig:
    try:
        if env is None:
            config = MaintenanceConfig.from_env()
        else:
            with _temporary_environment_overlay(env):
                config = MaintenanceConfig.from_env()
    except (MaintenanceConfigurationError, ValueError, TypeError) as exc:
        raise BoundedMaintenanceRuntimeError("runtime_config_error") from exc
    return BoundedMaintenanceRuntimeConfig(maintenance_config=config)


async def run_bounded_maintenance_queue_once(
    config: BoundedMaintenanceQueueOnceConfig,
    *,
    runtime_config_loader: Callable[[], BoundedMaintenanceRuntimeConfig] = load_bounded_maintenance_runtime_config,
    runtime_builder: BoundedMaintenanceQueueRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedMaintenanceResult:
    state = BoundedMaintenanceRuntimeState()
    gate_error = _queue_authority_gate_error(config)
    if gate_error is not None:
        return _queue_result("blocked", gate_error, config=config, state=state)

    runtime: BoundedMaintenanceQueueRuntime | None = None
    runtime_config: BoundedMaintenanceRuntimeConfig | None = None
    try:
        try:
            runtime_config = runtime_config_loader()
            state.runtime_config_loaded = True
        except BoundedMaintenanceRuntimeError as exc:
            return _queue_result("blocked", exc.error_code, config=config, state=state)
        except Exception as exc:
            return _queue_result(
                "blocked",
                "runtime_config_error",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
            )
        queue_error = _runtime_queue_error(config, runtime_config.maintenance_config)
        if queue_error is not None:
            return _queue_result("blocked", queue_error, config=config, state=state)
        runtime_config = BoundedMaintenanceRuntimeConfig(
            maintenance_config=runtime_config.maintenance_config,
            queue_command=config.command,
        )

        builder = runtime_builder or build_default_bounded_maintenance_queue_runtime
        runtime = await builder(runtime_config, state, logger or logging.getLogger(__name__))
        selection = await runtime.inspect_target(config)
        if selection.message is None:
            return _queue_result(
                "blocked",
                selection.error_code or "redis_target_missing",
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=selection,
            )
        if config.mode == "preview":
            return _queue_result(
                "pass",
                None,
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=selection,
            )

        consumed = await runtime.consume_target(selection.message, config)
        if consumed.message is None:
            return _queue_result(
                "blocked",
                consumed.error_code or "redis_consume_target_missing",
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=consumed,
            )
        trigger_event_id = _uuid_or_none(consumed.message.fields.get("trigger_event_id"))
        if trigger_event_id is None:
            return _queue_result(
                "blocked",
                "trigger_event_id_invalid",
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=consumed,
            )
        event_error = await _event_validation_error(runtime, config=config, message=consumed.message)
        if event_error is not None:
            return _queue_result(
                "blocked",
                event_error,
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=consumed,
            )

        service_result: DeliveryResultWorkerResult | None = None
        replay_completed = False
        try:
            if config.command == MAINTENANCE_RESULT_COMMAND:
                service_result = await runtime.invoke_maintenance(trigger_event_id)
            else:
                await runtime.invoke_replay(trigger_event_id)
                replay_completed = True
        except Exception as exc:
            await _rollback_quietly(runtime)
            return _queue_result(
                "failed",
                "service_execution_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=consumed,
                service_result=service_result,
                replay_service_completed=replay_completed,
            )
        try:
            await runtime.commit_database()
        except Exception as exc:
            return _queue_result(
                "failed",
                "database_commit_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=consumed,
                service_result=service_result,
                replay_service_completed=replay_completed,
            )
        try:
            ack_count = await runtime.ack(consumed.message.message_id)
        except Exception as exc:
            return _queue_result(
                "failed",
                "ack_failed_after_durable_completion",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                runtime_config=runtime_config,
                redis_selection=consumed,
                service_result=service_result,
                replay_service_completed=replay_completed,
                ack_attempted=True,
                acked=False,
            )
        acked = ack_count == 1
        return _queue_result(
            "pass" if acked else "failed",
            None if acked else "ack_failed_after_durable_completion",
            config=config,
            state=state,
            runtime_config=runtime_config,
            redis_selection=consumed,
            service_result=service_result,
            replay_service_completed=replay_completed,
            ack_attempted=True,
            acked=acked,
        )
    except Exception as exc:
        if runtime is not None:
            await _rollback_quietly(runtime)
        return _queue_result(
            "failed",
            "bounded_queue_once_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            runtime_config=runtime_config,
        )
    finally:
        if runtime is not None:
            try:
                await runtime.close()
            except Exception:
                pass


async def run_bounded_maintenance_due_retry(
    config: BoundedMaintenanceDueRetryConfig,
    *,
    runtime_config_loader: Callable[[], BoundedMaintenanceRuntimeConfig] = load_bounded_maintenance_runtime_config,
    runtime_builder: BoundedMaintenanceDueRetryRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedMaintenanceResult:
    state = BoundedMaintenanceRuntimeState()
    gate_error = _due_authority_gate_error(config)
    if gate_error is not None:
        return _due_result("blocked", gate_error, config=config, state=state)

    runtime: BoundedMaintenanceDueRetryRuntime | None = None
    runtime_config: BoundedMaintenanceRuntimeConfig | None = None
    now = _as_utc(config.now_utc or datetime.now(timezone.utc))
    try:
        try:
            runtime_config = runtime_config_loader()
            state.runtime_config_loaded = True
        except BoundedMaintenanceRuntimeError as exc:
            return _due_result("blocked", exc.error_code, config=config, state=state)
        except Exception as exc:
            return _due_result(
                "blocked",
                "runtime_config_error",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
            )
        if runtime_config.maintenance_config.maintenance_queue_name != MAINTENANCE_QUEUE_NAME:
            return _due_result("blocked", "queue_name_not_allowed", config=config, state=state)

        builder = runtime_builder or build_default_bounded_maintenance_due_retry_runtime
        runtime = await builder(runtime_config, state, logger or logging.getLogger(__name__))
        candidates = await runtime.preview_candidates(config.limit, now)
        candidate_summary = _due_candidate_summary(candidates)
        if config.mode == "preview":
            return _due_result(
                "pass",
                None,
                config=config,
                state=state,
                due_candidate_count=len(candidates),
                due_candidate_plan_suffixes=candidate_summary["plan_suffixes"],
                due_candidate_latest_delivery_suffixes=candidate_summary["latest_delivery_suffixes"],
            )
        try:
            action_count = await runtime.promote_due_retries_once(config.limit, now)
            await runtime.commit_database()
        except Exception as exc:
            await _rollback_quietly(runtime)
            return _due_result(
                "failed",
                "due_retry_execution_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                due_candidate_count=len(candidates),
                due_candidate_plan_suffixes=candidate_summary["plan_suffixes"],
                due_candidate_latest_delivery_suffixes=candidate_summary["latest_delivery_suffixes"],
            )
        return _due_result(
            "pass",
            None,
            config=config,
            state=state,
            due_candidate_count=len(candidates),
            due_candidate_plan_suffixes=candidate_summary["plan_suffixes"],
            due_candidate_latest_delivery_suffixes=candidate_summary["latest_delivery_suffixes"],
            due_action_count=action_count,
        )
    except Exception as exc:
        if runtime is not None:
            await _rollback_quietly(runtime)
        return _due_result(
            "failed",
            "bounded_due_retry_failed",
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


def run_bounded_maintenance_queue_once_sync(
    config: BoundedMaintenanceQueueOnceConfig,
    *,
    runtime_config_loader: Callable[[], BoundedMaintenanceRuntimeConfig] = load_bounded_maintenance_runtime_config,
    runtime_builder: BoundedMaintenanceQueueRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedMaintenanceResult:
    return asyncio.run(
        run_bounded_maintenance_queue_once(
            config,
            runtime_config_loader=runtime_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def run_bounded_maintenance_due_retry_sync(
    config: BoundedMaintenanceDueRetryConfig,
    *,
    runtime_config_loader: Callable[[], BoundedMaintenanceRuntimeConfig] = load_bounded_maintenance_runtime_config,
    runtime_builder: BoundedMaintenanceDueRetryRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedMaintenanceResult:
    return asyncio.run(
        run_bounded_maintenance_due_retry(
            config,
            runtime_config_loader=runtime_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str, *, command: str = MAINTENANCE_RESULT_COMMAND) -> dict[str, Any]:
    return _queue_result(
        "blocked",
        error_code,
        config=BoundedMaintenanceQueueOnceConfig(command=command),
        state=BoundedMaintenanceRuntimeState(),
    ).to_sanitized_dict()


def parse_now_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _as_utc(datetime.fromisoformat(text))
    except ValueError as exc:
        raise BoundedMaintenanceRuntimeError("now_utc_invalid") from exc


def _queue_authority_gate_error(config: BoundedMaintenanceQueueOnceConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if config.command not in {MAINTENANCE_RESULT_COMMAND, REPLAY_REQUEST_COMMAND}:
        return "command_not_allowed"
    if config.mode not in {"preview", "execute"}:
        return "mode_not_allowed"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    suffix_error = _target_suffix_error(config)
    if suffix_error is not None:
        return suffix_error
    if config.mode == "execute":
        if not config.allow_database_read:
            return "database_read_not_allowed"
        if not config.allow_database_write:
            return "database_write_not_allowed"
        if not config.allow_redis_consume:
            return "redis_consume_not_allowed"
        if not config.allow_redis_ack:
            return "redis_ack_not_allowed"
    return None


def _due_authority_gate_error(config: BoundedMaintenanceDueRetryConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if config.mode not in {"preview", "execute"}:
        return "mode_not_allowed"
    if config.limit < 1 or config.limit > 500:
        return "limit_not_allowed"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if config.mode == "execute" and not config.allow_database_write:
        return "database_write_not_allowed"
    return None


def _target_suffix_error(config: BoundedMaintenanceQueueOnceConfig) -> str | None:
    if not _valid_uuid_suffix(config.trigger_event_suffix) or not _valid_uuid_suffix(config.root_object_id_suffix):
        return "suffix_ambiguous_or_missing"
    if not config.redis_message_id_suffix or not MESSAGE_ID_SUFFIX_RE.fullmatch(config.redis_message_id_suffix):
        return "redis_message_id_suffix_invalid"
    return None


def _runtime_queue_error(config: BoundedMaintenanceQueueOnceConfig, runtime: MaintenanceConfig) -> str | None:
    if config.command == MAINTENANCE_RESULT_COMMAND:
        if runtime.maintenance_queue_name != MAINTENANCE_QUEUE_NAME:
            return "queue_name_not_allowed"
        if not runtime.maintenance_consumer_group:
            return "consumer_group_missing"
        return None
    if runtime.replay_queue_name != REPLAY_QUEUE_NAME:
        return "queue_name_not_allowed"
    if not runtime.replay_consumer_group:
        return "consumer_group_missing"
    return None


async def _event_validation_error(
    runtime: BoundedMaintenanceQueueRuntime,
    *,
    config: BoundedMaintenanceQueueOnceConfig,
    message: StreamMessage,
) -> str | None:
    trigger_event_id = _uuid_or_none(message.fields.get("trigger_event_id"))
    if trigger_event_id is None:
        return "trigger_event_id_invalid"
    event = await runtime.load_outbox_event(trigger_event_id)
    if event is None:
        return "event_outbox_missing"
    redis_root_id = _uuid_or_none(message.fields.get("root_object_id"))
    if redis_root_id is None:
        return "redis_root_object_id_invalid"
    if config.command == MAINTENANCE_RESULT_COMMAND:
        payload_plan_id = _uuid_or_none(event.payload_json.get("notification_plan_id"))
        if event.event_type != DELIVERY_RESULT_EVENT_TYPE:
            return "event_type_mismatch"
        if event.aggregate_type != NOTIFICATION_PLAN_ROOT:
            return "event_aggregate_type_mismatch"
        if payload_plan_id is None:
            return "event_payload_plan_missing"
        if payload_plan_id != event.aggregate_id:
            return "event_payload_plan_mismatch"
        if redis_root_id != event.aggregate_id:
            return "redis_root_object_id_mismatch"
        return None

    payload_replay_request_id = _uuid_or_none(event.payload_json.get("replay_request_id"))
    if event.event_type != REPLAY_REQUESTED_EVENT_TYPE:
        return "event_type_mismatch"
    if event.aggregate_type != REPLAY_REQUEST_ROOT:
        return "event_aggregate_type_mismatch"
    if payload_replay_request_id is None:
        return "event_payload_replay_request_missing"
    if payload_replay_request_id != event.aggregate_id:
        return "event_payload_replay_request_mismatch"
    if redis_root_id != event.aggregate_id:
        return "redis_root_object_id_mismatch"
    return None


def _select_next_exact_message(
    config: BoundedMaintenanceQueueOnceConfig,
    messages: list[StreamMessage],
    *,
    group_pending: int | None,
    group_lag: int | None,
) -> RedisTargetSelection:
    if not messages:
        return _selection_error("redis_unconsumed_target_missing", group_pending=group_pending, group_lag=group_lag)
    first = messages[0]
    selected = _select_exact_message(config, first, group_pending=group_pending, group_lag=group_lag)
    if selected.message is not None:
        return selected
    if any(_message_matches_selectors(config, message) for message in messages[1:]):
        return _selection_error(
            "target_not_next_unconsumed",
            redis_message_count=len(messages),
            group_pending=group_pending,
            group_lag=group_lag,
            message_stage_name=first.fields.get("stage_name"),
            message_root_object_type=first.fields.get("root_object_type"),
            trigger_event_id_present=bool(str(first.fields.get("trigger_event_id") or "").strip()),
            root_object_id_present=bool(str(first.fields.get("root_object_id") or "").strip()),
            redis_message_id_suffix=_id_suffix(first.message_id),
            trigger_event_id_suffix=_id_suffix(first.fields.get("trigger_event_id")),
            root_object_id_suffix=_id_suffix(first.fields.get("root_object_id")),
        )
    return selected


def _select_exact_message(
    config: BoundedMaintenanceQueueOnceConfig,
    message: StreamMessage,
    *,
    group_pending: int | None,
    group_lag: int | None,
) -> RedisTargetSelection:
    fields = message.fields
    base = {
        "redis_message_count": 1,
        "group_pending": group_pending,
        "group_lag": group_lag,
        "message_stage_name": fields.get("stage_name"),
        "message_root_object_type": fields.get("root_object_type"),
        "trigger_event_id_present": bool(str(fields.get("trigger_event_id") or "").strip()),
        "root_object_id_present": bool(str(fields.get("root_object_id") or "").strip()),
        "redis_message_id_suffix": _id_suffix(message.message_id),
        "trigger_event_id_suffix": _id_suffix(fields.get("trigger_event_id")),
        "root_object_id_suffix": _id_suffix(fields.get("root_object_id")),
    }
    if message.stream != _expected_queue_name(config.command):
        return _selection_error("queue_name_not_allowed", **base)
    if not message.message_id.endswith(str(config.redis_message_id_suffix)):
        return _selection_error("redis_message_id_mismatch", **base)
    if fields.get("stage_name") != _expected_stage_name(config.command):
        return _selection_error("message_stage_mismatch", **base)
    if fields.get("root_object_type") != _expected_root_object_type(config.command):
        return _selection_error("root_object_type_mismatch", **base)
    if not str(fields.get("trigger_event_id") or "").endswith(str(config.trigger_event_suffix)):
        return _selection_error("trigger_event_id_mismatch", **base)
    if not str(fields.get("root_object_id") or "").endswith(str(config.root_object_id_suffix)):
        return _selection_error("root_object_id_mismatch", **base)
    return RedisTargetSelection(status="matched", error_code=None, message=message, **base)


def _message_matches_selectors(config: BoundedMaintenanceQueueOnceConfig, message: StreamMessage) -> bool:
    return (
        message.stream == _expected_queue_name(config.command)
        and message.message_id.endswith(str(config.redis_message_id_suffix))
        and message.fields.get("stage_name") == _expected_stage_name(config.command)
        and message.fields.get("root_object_type") == _expected_root_object_type(config.command)
        and str(message.fields.get("trigger_event_id") or "").endswith(str(config.trigger_event_suffix))
        and str(message.fields.get("root_object_id") or "").endswith(str(config.root_object_id_suffix))
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
    root_object_id_present: bool = False,
    redis_message_id_suffix: str | None = None,
    trigger_event_id_suffix: str | None = None,
    root_object_id_suffix: str | None = None,
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
        root_object_id_present=root_object_id_present,
        redis_message_id_suffix=redis_message_id_suffix,
        trigger_event_id_suffix=trigger_event_id_suffix,
        root_object_id_suffix=root_object_id_suffix,
    )


def _queue_result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedMaintenanceQueueOnceConfig,
    state: BoundedMaintenanceRuntimeState,
    runtime_config: BoundedMaintenanceRuntimeConfig | None = None,
    error_class: str | None = None,
    redis_selection: RedisTargetSelection | None = None,
    service_result: DeliveryResultWorkerResult | None = None,
    replay_service_completed: bool = False,
    ack_attempted: bool = False,
    acked: bool = False,
) -> BoundedMaintenanceResult:
    queue_name = None
    consumer_group = None
    if runtime_config is not None:
        queue_name = _expected_queue_name(config.command)
        consumer_group = _expected_consumer_group(config.command, runtime_config.maintenance_config)
    return BoundedMaintenanceResult(
        command=config.command,
        mode=config.mode,
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        queue_config=config,
        state=state,
        queue_name=queue_name,
        consumer_group=consumer_group,
        redis_selection=redis_selection,
        service_result=service_result,
        replay_service_completed=replay_service_completed,
        ack_attempted=ack_attempted,
        acked=acked,
    )


def _due_result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedMaintenanceDueRetryConfig,
    state: BoundedMaintenanceRuntimeState,
    error_class: str | None = None,
    due_candidate_count: int | None = None,
    due_candidate_plan_suffixes: tuple[str, ...] = (),
    due_candidate_latest_delivery_suffixes: tuple[str, ...] = (),
    due_action_count: int | None = None,
) -> BoundedMaintenanceResult:
    return BoundedMaintenanceResult(
        command=DUE_RETRY_COMMAND,
        mode=config.mode,
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        due_config=config,
        state=state,
        queue_name=None,
        due_candidate_count=due_candidate_count,
        due_candidate_plan_suffixes=due_candidate_plan_suffixes,
        due_candidate_latest_delivery_suffixes=due_candidate_latest_delivery_suffixes,
        due_action_count=due_action_count,
    )


def _due_candidate_summary(candidates: list[RetryPromotionCandidate]) -> dict[str, tuple[str, ...]]:
    return {
        "plan_suffixes": tuple(_id_suffix(candidate.plan.notification_plan_id) for candidate in candidates),
        "latest_delivery_suffixes": tuple(
            _id_suffix(candidate.latest_delivery.notification_delivery_record_id)
            for candidate in candidates
            if candidate.latest_delivery is not None
        ),
    }


def _queue_runtime_names(config: MaintenanceConfig, command: str) -> tuple[str, str, str]:
    if command == REPLAY_REQUEST_COMMAND:
        return (
            REPLAY_QUEUE_NAME,
            config.replay_consumer_group,
            config.replay_consumer_name or DEFAULT_CONSUMER_NAME,
        )
    return (
        MAINTENANCE_QUEUE_NAME,
        config.maintenance_consumer_group,
        config.maintenance_consumer_name or DEFAULT_CONSUMER_NAME,
    )


def _expected_queue_name(command: str) -> str:
    return MAINTENANCE_QUEUE_NAME if command == MAINTENANCE_RESULT_COMMAND else REPLAY_QUEUE_NAME


def _expected_stage_name(command: str) -> str:
    return MAINTENANCE_STAGE_NAME if command == MAINTENANCE_RESULT_COMMAND else REPLAY_STAGE_NAME


def _expected_root_object_type(command: str) -> str:
    return NOTIFICATION_PLAN_ROOT if command == MAINTENANCE_RESULT_COMMAND else REPLAY_REQUEST_ROOT


def _expected_consumer_group(command: str, config: MaintenanceConfig) -> str:
    return config.maintenance_consumer_group if command == MAINTENANCE_RESULT_COMMAND else config.replay_consumer_group


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


def _pending_message_id(entry: Any) -> str | None:
    if isinstance(entry, dict):
        return _decode_redis_value(entry.get("message_id") or entry.get(b"message_id") or entry.get("id") or entry.get(b"id"))
    if isinstance(entry, (tuple, list)) and entry:
        return _decode_redis_value(entry[0])
    return None


def _pending_consumer_name(entry: Any) -> str | None:
    if isinstance(entry, dict):
        value = entry.get("consumer") or entry.get(b"consumer") or entry.get("consumername") or entry.get(b"consumername")
        return _decode_redis_value(value) if value is not None else None
    if isinstance(entry, (tuple, list)) and len(entry) > 1:
        return _decode_redis_value(entry[1])
    return None


def _decode_redis_value(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _dict_get(values: Mapping[Any, Any], name: str) -> Any:
    return values.get(name, values.get(name.encode("utf-8")))


def _valid_uuid_suffix(value: str | None) -> bool:
    return bool(value and UUID_SUFFIX_RE.fullmatch(value.strip().lower()))


def _uuid_suffix_projection(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().lower()
    return text if UUID_SUFFIX_RE.fullmatch(text) else None


def _message_id_suffix_projection(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text if MESSAGE_ID_SUFFIX_RE.fullmatch(text) else None


def _uuid_or_none(value: object) -> UUID | None:
    try:
        return UUID(str(value or ""))
    except (TypeError, ValueError, AttributeError):
        return None


def _id_suffix(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[-8:] if text else None


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _safe_exception_class(exc: BaseException) -> str:
    text = type(exc).__name__
    return text if SAFE_TOKEN_RE.fullmatch(text) else "Exception"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _rollback_quietly(runtime: Any) -> None:
    try:
        await runtime.rollback_database()
    except Exception:
        pass


def _operator_approved(
    queue_config: BoundedMaintenanceQueueOnceConfig | None,
    due_config: BoundedMaintenanceDueRetryConfig | None,
) -> bool:
    return queue_config.operator_approved if queue_config else bool(due_config and due_config.operator_approved)


def _runtime_config_allowed(
    queue_config: BoundedMaintenanceQueueOnceConfig | None,
    due_config: BoundedMaintenanceDueRetryConfig | None,
) -> bool:
    return queue_config.allow_runtime_config if queue_config else bool(due_config and due_config.allow_runtime_config)


def _database_read_allowed(
    queue_config: BoundedMaintenanceQueueOnceConfig | None,
    due_config: BoundedMaintenanceDueRetryConfig | None,
) -> bool:
    return queue_config.allow_database_read if queue_config else bool(due_config and due_config.allow_database_read)


def _database_write_allowed(
    queue_config: BoundedMaintenanceQueueOnceConfig | None,
    due_config: BoundedMaintenanceDueRetryConfig | None,
) -> bool:
    return queue_config.allow_database_write if queue_config else bool(due_config and due_config.allow_database_write)


@contextmanager
def _temporary_environment_overlay(values: Mapping[str, str]):
    previous: dict[str, str | None] = {}
    try:
        for key, value in values.items():
            previous[key] = os.environ.get(key)
            os.environ[key] = value
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


__all__ = [
    "BoundedMaintenanceDueRetryConfig",
    "BoundedMaintenanceDueRetryRuntime",
    "BoundedMaintenanceDueRetryRuntimeBuilder",
    "BoundedMaintenanceQueueOnceConfig",
    "BoundedMaintenanceQueueRuntime",
    "BoundedMaintenanceQueueRuntimeBuilder",
    "BoundedMaintenanceResult",
    "BoundedMaintenanceRuntimeConfig",
    "BoundedMaintenanceRuntimeError",
    "BoundedMaintenanceRuntimeState",
    "DUE_RETRY_COMMAND",
    "MAINTENANCE_RESULT_COMMAND",
    "REPLAY_REQUEST_COMMAND",
    "RUNNER_NAME",
    "RedisExactNextMaintenanceConsumer",
    "RedisTargetSelection",
    "SCHEMA_VERSION",
    "argument_error_report",
    "build_default_bounded_maintenance_due_retry_runtime",
    "build_default_bounded_maintenance_queue_runtime",
    "load_bounded_maintenance_runtime_config",
    "parse_now_utc",
    "render_sanitized_json",
    "run_bounded_maintenance_due_retry",
    "run_bounded_maintenance_due_retry_sync",
    "run_bounded_maintenance_queue_once",
    "run_bounded_maintenance_queue_once_sync",
]
