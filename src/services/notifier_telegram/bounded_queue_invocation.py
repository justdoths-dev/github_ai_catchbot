from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from .bounded_invocation import (
    BoundedNotifierDryRunInvocationConfig,
    BoundedNotifierDryRunInvocationResult,
    BoundedNotifierRuntimeBuilder,
    load_forced_dry_run_notifier_config,
    run_bounded_notifier_dry_run_invocation,
)
from .config import NotifierTelegramConfig
from .models import StreamMessage
from .redis_streams import RedisStreamConsumer

SCHEMA_VERSION = "bounded_notifier_queue_dry_run_invocation_v1"
RUNNER_NAME = "bounded_notifier_queue_dry_run_runner"
MODE = "notifier_queue_dry_run_send_disabled_one_shot"
QUEUE_NAME = "q.notification.send"
DEFAULT_CONSUMER_GROUP = "notifier-telegram"
DEFAULT_BLOCK_MS = 1
DEFAULT_BATCH_SIZE = 1


@dataclass(frozen=True, slots=True)
class BoundedNotificationQueueRuntimeConfig:
    redis_url: str
    queue_name: str = QUEUE_NAME
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    consumer_name: str = RUNNER_NAME
    block_ms: int = DEFAULT_BLOCK_MS
    batch_size: int = DEFAULT_BATCH_SIZE


@dataclass(frozen=True, slots=True)
class BoundedNotifierQueueDryRunConfig:
    operator_approved: bool = False
    allow_redis_read: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False


@dataclass(slots=True)
class BoundedQueueInvocationState:
    queue_consumer_created: bool = False
    redis_read_attempted: bool = False
    redis_ack_attempted: bool = False
    bounded_invocation_attempted: bool = False


class BoundedNotificationQueueConsumer(Protocol):
    async def read_one(self) -> StreamMessage | None: ...
    async def ack(self, message_id: str) -> int: ...
    async def close(self) -> None: ...


class BoundedNotificationQueueConsumerBuilder(Protocol):
    async def __call__(
        self,
        queue_config: BoundedNotificationQueueRuntimeConfig,
        state: BoundedQueueInvocationState,
        logger: logging.Logger,
    ) -> BoundedNotificationQueueConsumer: ...


class BoundedInvocationRunner(Protocol):
    async def __call__(
        self,
        config: BoundedNotifierDryRunInvocationConfig,
        *,
        notifier_config_loader: Callable[[], NotifierTelegramConfig],
        runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
        logger: logging.Logger | None = None,
    ) -> BoundedNotifierDryRunInvocationResult: ...


class BoundedQueueInvocationError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class RedisOneShotNotificationQueueConsumer:
    def __init__(self, redis_consumer: RedisStreamConsumer, redis_client: Any) -> None:
        self._redis_consumer = redis_consumer
        self._redis_client = redis_client

    async def read_one(self) -> StreamMessage | None:
        messages = await self._redis_consumer.read_batch()
        return messages[0] if messages else None

    async def ack(self, message_id: str) -> int:
        await self._redis_consumer.ack(message_id)
        return 1

    async def close(self) -> None:
        close = getattr(self._redis_client, "aclose", None) or getattr(self._redis_client, "close", None)
        if close is None:
            return
        result = close()
        if hasattr(result, "__await__"):
            await result


@dataclass(frozen=True, slots=True)
class BoundedNotifierQueueDryRunResult:
    status: str
    ok: bool
    error_code: str | None
    queue_name: str
    operator_approved: bool
    redis_read_allowed: bool
    database_write_allowed: bool
    redis_ack_allowed: bool
    redis_message_count: int
    redis_ack_count: int
    trigger_event_id_present: bool
    processed_event_count: int
    bounded_invocation_summary: Mapping[str, Any] | None = None
    state: BoundedQueueInvocationState = field(default_factory=BoundedQueueInvocationState)

    def to_sanitized_dict(self) -> dict[str, Any]:
        nested = dict(self.bounded_invocation_summary or {})
        nested_side_effects = dict(nested.get("side_effects") or {})
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "queue_name": self.queue_name,
            "operator_approved": self.operator_approved,
            "redis_read_allowed": self.redis_read_allowed,
            "database_write_allowed": self.database_write_allowed,
            "redis_ack_allowed": self.redis_ack_allowed,
            "send_enabled": False,
            "dry_run": True,
            "edits_allowed": False,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_message_count": self.redis_message_count,
            "redis_ack_attempted": self.state.redis_ack_attempted,
            "redis_ack_count": self.redis_ack_count,
            "trigger_event_id_present": self.trigger_event_id_present,
            "bounded_invocation_attempted": self.state.bounded_invocation_attempted,
            "processed_event_count": self.processed_event_count,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "bounded_invocation_summary": nested,
            "redactions_applied": [
                "trigger_event_id_omitted",
                "redis_url_omitted",
                "database_url_omitted",
                "telegram_token_omitted",
                "redis_payload_omitted",
                "redis_message_id_omitted",
                "rendered_message_text_omitted",
                "telegram_request_omitted",
                "telegram_response_omitted",
                "exception_detail_omitted",
                "source_raw_text_omitted",
            ],
            "side_effects": {
                "queue_consumer_created": self.state.queue_consumer_created,
                "redis_stream_read": self.state.redis_read_attempted,
                "redis_acknowledged": self.redis_ack_count > 0,
                "redis_mutation": self.redis_ack_count > 0,
                "database_write_allowed": self.database_write_allowed,
                "bounded_invocation_attempted": self.state.bounded_invocation_attempted,
                "database_session_opened": bool(nested_side_effects.get("database_session_opened", False)),
                "event_outbox_read_attempted": bool(nested_side_effects.get("event_outbox_read_attempted", False)),
                "notifier_invocation_attempted": bool(
                    nested_side_effects.get("notifier_invocation_attempted", False)
                ),
                "telegram_send_called": bool(nested_side_effects.get("telegram_send_called", False)),
                "telegram_edit_called": bool(nested_side_effects.get("telegram_edit_called", False)),
                "worker_loop_started": False,
                "run_forever_called": False,
                "docker_or_systemd_called": False,
                "openai_called": False,
                "github_called": False,
                "x_called": False,
                "web_called": False,
                "analysis_mutated": False,
                "judge_output_mutated": False,
                "candidate_group_mutated": False,
                "artifact_mutated": False,
                "source_message_mutated": False,
                "db_schema_mutated": False,
                "feature_flags_mutated": False,
            },
        }


def load_bounded_notification_queue_config(
    env: Mapping[str, str] | None = None,
) -> BoundedNotificationQueueRuntimeConfig:
    source = os.environ if env is None else env
    redis_url = _env_value(source, "REDIS_URL")
    if not redis_url:
        raise BoundedQueueInvocationError("redis_url_missing")
    queue_name = _env_value(source, "NOTIFIER_TELEGRAM_QUEUE_NAME", QUEUE_NAME) or QUEUE_NAME
    if queue_name != QUEUE_NAME:
        raise BoundedQueueInvocationError("queue_name_not_allowed")
    consumer_group = _env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP)
    consumer_name = _env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_NAME", RUNNER_NAME)
    if not consumer_group:
        raise BoundedQueueInvocationError("consumer_group_missing")
    if not consumer_name:
        raise BoundedQueueInvocationError("consumer_name_missing")
    return BoundedNotificationQueueRuntimeConfig(
        redis_url=redis_url,
        queue_name=queue_name,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
    )


async def build_default_bounded_notification_queue_consumer(
    queue_config: BoundedNotificationQueueRuntimeConfig,
    state: BoundedQueueInvocationState,
    logger: logging.Logger,
) -> BoundedNotificationQueueConsumer:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(queue_config.redis_url, decode_responses=True)
    state.queue_consumer_created = True
    redis_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=queue_config.queue_name,
        consumer_group=queue_config.consumer_group,
        consumer_name=queue_config.consumer_name,
        block_ms=queue_config.block_ms,
        batch_size=queue_config.batch_size,
    )
    return RedisOneShotNotificationQueueConsumer(redis_consumer, redis_client)


async def run_bounded_notifier_queue_dry_run_invocation(
    config: BoundedNotifierQueueDryRunConfig,
    *,
    queue_config_loader: Callable[[], BoundedNotificationQueueRuntimeConfig] = load_bounded_notification_queue_config,
    consumer_builder: BoundedNotificationQueueConsumerBuilder | None = None,
    bounded_invocation_runner: BoundedInvocationRunner = run_bounded_notifier_dry_run_invocation,
    notifier_config_loader: Callable[[], NotifierTelegramConfig] = load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierQueueDryRunResult:
    state = BoundedQueueInvocationState()
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if not config.allow_redis_read:
        return _result("blocked", "redis_read_not_allowed", config=config, state=state)

    queue_name = QUEUE_NAME
    consumer: BoundedNotificationQueueConsumer | None = None
    try:
        queue_config = queue_config_loader()
        queue_name = queue_config.queue_name
        queue_config_error = _validate_queue_config(queue_config)
        if queue_config_error is not None:
            return _result("blocked", queue_config_error, config=config, state=state, queue_name=queue_name)
    except BoundedQueueInvocationError as exc:
        return _result("blocked", exc.error_code, config=config, state=state, queue_name=queue_name)
    except Exception:
        return _result("blocked", "queue_runtime_config_error", config=config, state=state, queue_name=queue_name)

    try:
        builder = consumer_builder or build_default_bounded_notification_queue_consumer
        consumer = await builder(queue_config, state, logger or logging.getLogger(__name__))
        state.queue_consumer_created = True
        state.redis_read_attempted = True
        message = await consumer.read_one()
    except Exception:
        await _close_consumer(consumer)
        return _result("failed", "redis_read_failed", config=config, state=state, queue_name=queue_name)

    if message is None:
        await _close_consumer(consumer)
        return _result("blocked", "redis_message_missing", config=config, state=state, queue_name=queue_name)

    trigger_event_id_raw = str(message.fields.get("trigger_event_id", "")).strip()
    trigger_event_id_present = bool(trigger_event_id_raw)
    trigger_event_id = _uuid_or_none(trigger_event_id_raw)
    if not trigger_event_id_present:
        await _close_consumer(consumer)
        return _result(
            "blocked",
            "trigger_event_id_missing",
            config=config,
            state=state,
            queue_name=queue_name,
            redis_message_count=1,
            trigger_event_id_present=False,
        )
    if trigger_event_id is None:
        await _close_consumer(consumer)
        return _result(
            "blocked",
            "trigger_event_id_invalid",
            config=config,
            state=state,
            queue_name=queue_name,
            redis_message_count=1,
            trigger_event_id_present=True,
        )
    if not config.allow_database_write:
        await _close_consumer(consumer)
        return _result(
            "blocked",
            "database_write_not_allowed",
            config=config,
            state=state,
            queue_name=queue_name,
            redis_message_count=1,
            trigger_event_id_present=True,
        )

    state.bounded_invocation_attempted = True
    try:
        invocation_result = await bounded_invocation_runner(
            BoundedNotifierDryRunInvocationConfig(
                trigger_event_id=str(trigger_event_id),
                operator_approved=True,
                allow_database_write=True,
            ),
            notifier_config_loader=notifier_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    except Exception:
        await _close_consumer(consumer)
        return _result(
            "failed",
            "bounded_invocation_failed",
            config=config,
            state=state,
            queue_name=queue_name,
            redis_message_count=1,
            trigger_event_id_present=True,
        )

    bounded_summary = invocation_result.to_sanitized_dict()
    if not invocation_result.ok:
        await _close_consumer(consumer)
        return _result(
            invocation_result.status,
            invocation_result.error_code or "bounded_invocation_failed",
            config=config,
            state=state,
            queue_name=queue_name,
            redis_message_count=1,
            trigger_event_id_present=True,
            processed_event_count=invocation_result.processed_event_count,
            bounded_invocation_summary=bounded_summary,
        )

    redis_ack_count = 0
    if config.allow_redis_ack:
        try:
            state.redis_ack_attempted = True
            redis_ack_count = await consumer.ack(message.message_id)
        except Exception:
            await _close_consumer(consumer)
            return _result(
                "failed",
                "redis_ack_failed",
                config=config,
                state=state,
                queue_name=queue_name,
                redis_message_count=1,
                trigger_event_id_present=True,
                processed_event_count=invocation_result.processed_event_count,
                bounded_invocation_summary=bounded_summary,
            )

    await _close_consumer(consumer)
    return _result(
        "pass",
        None,
        config=config,
        state=state,
        queue_name=queue_name,
        redis_message_count=1,
        redis_ack_count=redis_ack_count,
        trigger_event_id_present=True,
        processed_event_count=invocation_result.processed_event_count,
        bounded_invocation_summary=bounded_summary,
    )


def run_bounded_notifier_queue_dry_run_invocation_sync(
    config: BoundedNotifierQueueDryRunConfig,
    *,
    queue_config_loader: Callable[[], BoundedNotificationQueueRuntimeConfig] = load_bounded_notification_queue_config,
    consumer_builder: BoundedNotificationQueueConsumerBuilder | None = None,
    bounded_invocation_runner: BoundedInvocationRunner = run_bounded_notifier_dry_run_invocation,
    notifier_config_loader: Callable[[], NotifierTelegramConfig] = load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierQueueDryRunResult:
    return asyncio.run(
        run_bounded_notifier_queue_dry_run_invocation(
            config,
            queue_config_loader=queue_config_loader,
            consumer_builder=consumer_builder,
            bounded_invocation_runner=bounded_invocation_runner,
            notifier_config_loader=notifier_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotifierQueueDryRunConfig,
    state: BoundedQueueInvocationState,
    queue_name: str = QUEUE_NAME,
    redis_message_count: int = 0,
    redis_ack_count: int = 0,
    trigger_event_id_present: bool = False,
    processed_event_count: int = 0,
    bounded_invocation_summary: Mapping[str, Any] | None = None,
) -> BoundedNotifierQueueDryRunResult:
    return BoundedNotifierQueueDryRunResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        queue_name=queue_name,
        operator_approved=config.operator_approved,
        redis_read_allowed=config.allow_redis_read,
        database_write_allowed=config.allow_database_write,
        redis_ack_allowed=config.allow_redis_ack,
        redis_message_count=redis_message_count,
        redis_ack_count=redis_ack_count,
        trigger_event_id_present=trigger_event_id_present,
        processed_event_count=processed_event_count,
        bounded_invocation_summary=bounded_invocation_summary,
        state=state,
    )


async def _close_consumer(consumer: BoundedNotificationQueueConsumer | None) -> None:
    if consumer is None:
        return
    try:
        await consumer.close()
    except Exception:
        pass


def _uuid_or_none(value: object) -> UUID | None:
    try:
        return UUID(str(value or ""))
    except (TypeError, ValueError, AttributeError):
        return None


def _validate_queue_config(queue_config: BoundedNotificationQueueRuntimeConfig) -> str | None:
    if not str(queue_config.redis_url or "").strip():
        return "redis_url_missing"
    if queue_config.queue_name != QUEUE_NAME:
        return "queue_name_not_allowed"
    if not str(queue_config.consumer_group or "").strip():
        return "consumer_group_missing"
    if not str(queue_config.consumer_name or "").strip():
        return "consumer_name_missing"
    if queue_config.batch_size != DEFAULT_BATCH_SIZE:
        return "batch_size_not_allowed"
    if queue_config.block_ms < 1:
        return "block_ms_invalid"
    return None


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


__all__ = [
    "BoundedInvocationRunner",
    "BoundedNotificationQueueConsumer",
    "BoundedNotificationQueueConsumerBuilder",
    "BoundedNotificationQueueRuntimeConfig",
    "BoundedNotifierQueueDryRunConfig",
    "BoundedNotifierQueueDryRunResult",
    "QUEUE_NAME",
    "RUNNER_NAME",
    "build_default_bounded_notification_queue_consumer",
    "load_bounded_notification_queue_config",
    "load_forced_dry_run_notifier_config",
    "render_sanitized_json",
    "run_bounded_notifier_queue_dry_run_invocation",
    "run_bounded_notifier_queue_dry_run_invocation_sync",
]
