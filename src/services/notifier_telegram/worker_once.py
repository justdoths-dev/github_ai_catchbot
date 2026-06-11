from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any, Protocol
from uuid import UUID

from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .models import StreamMessage
from .redis_streams import RedisStreamConsumer
from .repositories import NotifierTelegramRepository
from .service import NotifierTelegramService
from .telegram_client import TelegramBotClient
from .worker import NotifierTelegramWorker, RedisStreamConsumerProtocol

SCHEMA_VERSION = "notifier_worker_once_invocation_v1"
EXPECTED_QUEUE_NAME = "q.notification.send"
EXPECTED_STAGE_NAME = "notify"
EXPECTED_ROOT_OBJECT_TYPE = "analysis"
ALLOWED_ROOT_OBJECT_TYPES = (EXPECTED_ROOT_OBJECT_TYPE, "notification_plan")
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
_FORBIDDEN_FIELD_NAMES = {
    "payload_json",
    "database_url",
    "redis_url",
    "telegram_bot_token",
    "openai_api_key",
    "github_token",
}
_FORBIDDEN_FIELD_MARKERS = ("password", "secret", "token", "credential", "api_key", "url")


class TriggerServiceProtocol(Protocol):
    async def handle_trigger_event(self, trigger_event_id: str): ...


class WorkerOnceRuntimeBuilder(Protocol):
    async def __call__(
        self,
        config: NotifierTelegramConfig,
        state: "WorkerOnceRuntimeState",
        logger: logging.Logger,
    ) -> "WorkerOnceRuntime": ...


class WorkerBuilder(Protocol):
    def __call__(
        self,
        config: NotifierTelegramConfig,
        *,
        consumer: RedisStreamConsumerProtocol,
        service: TriggerServiceProtocol,
        logger: logging.Logger | None = None,
    ) -> Any: ...


@dataclass(slots=True)
class WorkerOnceRuntimeState:
    redis_read: bool = False
    redis_ack: bool = False
    database_session_opened: bool = False


@dataclass(slots=True)
class WorkerOnceRuntime:
    consumer: RedisStreamConsumerProtocol
    service: TriggerServiceProtocol
    dispose: Callable[[], Awaitable[None]]
    classify_read_failure: Callable[[BaseException], Awaitable[str | None]] | None = None


async def run_worker_once_invocation(
    *,
    queue: str | None,
    confirm_worker_once: bool,
    output_format: str | None,
    emit_json=print,
    config_loader: Callable[[], NotifierTelegramConfig] = NotifierTelegramConfig.from_env,
    runtime_builder: WorkerOnceRuntimeBuilder | None = None,
    worker_builder: WorkerBuilder = NotifierTelegramWorker,
    logger: logging.Logger | None = None,
) -> int:
    state = WorkerOnceRuntimeState()
    config: NotifierTelegramConfig | None = None
    output = str(output_format or "")

    if not confirm_worker_once:
        emit_json(_to_json(_payload(status="rejected", reason_code="confirm_worker_once_required", state=state)))
        return 2
    if queue != EXPECTED_QUEUE_NAME:
        emit_json(_to_json(_payload(status="rejected", reason_code="unsupported_queue", state=state, queue=queue)))
        return 2
    if output != "json":
        emit_json(_to_json(_payload(status="rejected", reason_code="unsupported_format", state=state)))
        return 2

    try:
        config = replace(config_loader(), queue_name=EXPECTED_QUEUE_NAME, batch_size=1)
    except (NotifierTelegramConfigurationError, ValueError, TypeError):
        emit_json(_to_json(_payload(status="failed", reason_code="runtime_config_error", state=state)))
        return 1

    builder = runtime_builder or build_default_worker_once_runtime
    runtime: WorkerOnceRuntime | None = None
    try:
        runtime = await builder(config, state, logger or logging.getLogger(__name__))
    except Exception:
        emit_json(_to_json(_payload(status="failed", reason_code="runtime_builder_error", state=state, config=config)))
        return 1

    try:
        try:
            messages = await runtime.consumer.read_batch()
        except Exception as exc:
            reason_code = await _classify_read_batch_failure(exc, runtime)
            emit_json(
                _to_json(
                    _payload(
                        status="failed",
                        reason_code=reason_code,
                        state=state,
                        config=config,
                    )
                )
            )
            return 1

        state.redis_read = True
        if not messages:
            emit_json(
                _to_json(
                    _payload(
                        status="empty",
                        reason_code="no_message_available",
                        state=state,
                        config=config,
                    )
                )
            )
            return 0

        if len(messages) != 1 or _malformed_message(messages[0]):
            emit_json(
                _to_json(
                    _payload(
                        status="rejected",
                        reason_code="malformed_message",
                        state=state,
                        config=config,
                        message=messages[0],
                    )
                )
            )
            return 2

        tracked_service = _TrackedTriggerService(runtime.service)
        single_message_consumer = _SingleMessageConsumer(messages[0], ack_delegate=runtime.consumer, state=state)
        worker = worker_builder(
            config,
            consumer=single_message_consumer,
            service=tracked_service,
            logger=logger,
        )
        try:
            await worker.run_once()
        except Exception:
            emit_json(
                _to_json(
                    _payload(
                        status="failed",
                        reason_code="handler_failed",
                        state=state,
                        config=config,
                        message=messages[0],
                        handler_called=tracked_service.handler_called,
                    )
                )
            )
            return 1

        emit_json(
            _to_json(
                _payload(
                    status="processed",
                    reason_code="processed",
                    state=state,
                    config=config,
                    message=messages[0],
                    handler_called=tracked_service.handler_called,
                    delivery_result_summary=tracked_service.delivery_result_summary,
                )
            )
        )
        return 0
    except Exception:
        emit_json(_to_json(_payload(status="failed", reason_code="handler_failed", state=state, config=config)))
        return 1
    finally:
        if runtime is not None:
            try:
                await runtime.dispose()
            except Exception:
                pass


async def build_default_worker_once_runtime(
    config: NotifierTelegramConfig,
    state: WorkerOnceRuntimeState,
    logger: logging.Logger,
) -> WorkerOnceRuntime:
    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    consumer = RedisStreamConsumer(
        redis_client,
        queue_name=EXPECTED_QUEUE_NAME,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        batch_size=1,
    )
    telegram_client = TelegramBotClient(
        bot_token=config.telegram_bot_token,
        base_url=config.telegram_api_base_url,
        timeout_sec=config.request_timeout_sec,
    )

    class SessionBackedService:
        async def handle_trigger_event(self, trigger_event_id: str):
            state.database_session_opened = True
            async with session_factory.begin() as session:
                repository = NotifierTelegramRepository(session)
                service = NotifierTelegramService(
                    config,
                    repository=repository,
                    telegram_client=telegram_client,
                    logger=logger,
                )
                return await service.handle_trigger_event(trigger_event_id)

    async def dispose() -> None:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()

    async def classify_read_failure(exc: BaseException) -> str | None:
        del exc
        return await _classify_redis_readiness(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=config.consumer_group,
        )

    return WorkerOnceRuntime(
        consumer=consumer,
        service=SessionBackedService(),
        dispose=dispose,
        classify_read_failure=classify_read_failure,
    )


class _TrackedTriggerService:
    def __init__(self, wrapped: TriggerServiceProtocol) -> None:
        self._wrapped = wrapped
        self.handler_called = False
        self.delivery_result_summary: dict[str, Any] | None = None

    async def handle_trigger_event(self, trigger_event_id: str):
        self.handler_called = True
        result = await self._wrapped.handle_trigger_event(trigger_event_id)
        if result is None:
            raise _HandlerReturnedNoResult()
        self.delivery_result_summary = _delivery_result_summary(result)
        return result


class _HandlerReturnedNoResult(Exception):
    pass


class _SingleMessageConsumer:
    def __init__(
        self,
        message: StreamMessage,
        *,
        ack_delegate: RedisStreamConsumerProtocol,
        state: WorkerOnceRuntimeState,
    ) -> None:
        self._message = message
        self._ack_delegate = ack_delegate
        self._state = state
        self._read = False

    async def ensure_group(self) -> None:
        return None

    async def read_batch(self) -> list[StreamMessage]:
        if self._read:
            return []
        self._read = True
        return [self._message]

    async def ack(self, message_id: str) -> None:
        await self._ack_delegate.ack(message_id)
        self._state.redis_ack = True


def _malformed_message(message: StreamMessage) -> bool:
    fields = message.fields
    if message.stream != EXPECTED_QUEUE_NAME:
        return True
    if set(fields) != set(REQUIRED_THIN_QUEUE_FIELDS):
        return True
    if fields.get("stage_name") != EXPECTED_STAGE_NAME:
        return True
    if fields.get("root_object_type") not in ALLOWED_ROOT_OBJECT_TYPES:
        return True
    if _uuid_or_none(fields.get("trigger_event_id")) is None:
        return True
    for field_name in fields:
        normalized = field_name.strip().lower()
        if normalized in _FORBIDDEN_FIELD_NAMES:
            return True
        if any(marker in normalized for marker in _FORBIDDEN_FIELD_MARKERS):
            return True
    for required_nonempty in ("job_id", "root_object_id", "idempotency_key"):
        if not str(fields.get(required_nonempty) or "").strip():
            return True
    return False


def _classify_read_batch_error(exc: BaseException) -> str:
    text = _exception_search_text(exc)
    if "wrongtype" in text or "wrong kind of value" in text:
        return "queue_key_wrong_type"
    if "nogroup" in text:
        stream_missing = (
            "no such key" in text
            or "key does not exist" in text
            or "missing stream" in text
            or "no such stream" in text
            or "stream does not exist" in text
            or "stream_missing" in text
        )
        group_missing = "consumer group" in text or "no such group" in text or "group does not exist" in text
        if group_missing and not stream_missing:
            return "consumer_group_missing"
        if stream_missing and group_missing:
            return "redis_read_failed"
        if stream_missing:
            return "stream_missing"
    return "redis_read_failed"


async def _classify_read_batch_failure(exc: BaseException, runtime: WorkerOnceRuntime) -> str:
    reason_code = _classify_read_batch_error(exc)
    if reason_code != "redis_read_failed" or "nogroup" not in _exception_search_text(exc):
        return reason_code
    if runtime.classify_read_failure is None:
        return reason_code
    try:
        readiness_reason_code = await runtime.classify_read_failure(exc)
    except Exception:
        return "redis_read_failed"
    if readiness_reason_code in {"stream_missing", "queue_key_wrong_type", "consumer_group_missing"}:
        return readiness_reason_code
    return "redis_read_failed"


async def _classify_redis_readiness(client: Any, *, queue_name: str, consumer_group: str) -> str:
    try:
        queue_type = _decode_redis_value(await client.type(queue_name)).strip().lower()
        if queue_type == "none":
            return "stream_missing"
        if queue_type != "stream":
            return "queue_key_wrong_type"
        groups = await client.xinfo_groups(queue_name)
    except Exception:
        return "redis_read_failed"
    saw_unknown_group_shape = False
    for group in groups or []:
        group_name = _redis_group_name(group)
        if group_name is None:
            saw_unknown_group_shape = True
            continue
        if group_name == consumer_group:
            return "redis_read_failed"
    if saw_unknown_group_shape:
        return "redis_read_failed"
    return "consumer_group_missing"


def _redis_group_name(group: object) -> str | None:
    if isinstance(group, dict):
        for key in ("name", b"name"):
            if key in group:
                return _decode_redis_value(group[key])
    return None


def _decode_redis_value(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _exception_search_text(exc: BaseException) -> str:
    parts: list[str] = [type(exc).__name__]
    current: BaseException | None = exc
    while current is not None:
        parts.append(str(current))
        for arg in getattr(current, "args", ()):
            parts.append(str(arg))
        for attr_name in ("code", "error_code", "message", "detail", "response"):
            attr_value = getattr(current, attr_name, None)
            if attr_value is not None:
                parts.append(str(attr_value))
        current = current.__cause__ or current.__context__
    return " ".join(parts).lower()


def _payload(
    *,
    status: str,
    reason_code: str,
    state: WorkerOnceRuntimeState,
    queue: str | None = EXPECTED_QUEUE_NAME,
    config: NotifierTelegramConfig | None = None,
    message: StreamMessage | None = None,
    handler_called: bool = False,
    delivery_result_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    trigger_event_id = str((message.fields if message else {}).get("trigger_event_id") or "").strip()
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "queue": queue or "",
        "message_seen": message is not None,
        "handler_called": handler_called,
        "acked": state.redis_ack,
        "trigger_event_id_present": bool(trigger_event_id),
        "authority": {
            "telegram_transport_possible": bool(config and config.transport_enabled),
            "redis_read": state.redis_read,
            "redis_ack": state.redis_ack,
            "database_session_opened": state.database_session_opened,
            "workers_started": False,
            "run_forever_started": False,
            "openai_called": False,
            "github_called": False,
            "docker_or_systemd_called": False,
            "subprocess_started": False,
            "shell_invoked": False,
            "env_file_mutated": False,
            "feature_flags_applied": False,
            "alembic_or_ddl_ran": False,
        },
    }
    if delivery_result_summary is not None:
        payload["delivery_result_summary"] = delivery_result_summary
    return payload


_SAFE_SUMMARY_TOKEN = re.compile(r"^[A-Za-z0-9_]{1,120}$")
_SENSITIVE_SUMMARY_MARKERS = ("password", "secret", "token", "credential", "api_key", "database_url", "redis_url")


def _delivery_result_summary(result: object) -> dict[str, Any]:
    return {
        "delivery_status": _safe_summary_token(getattr(result, "delivery_status", None)),
        "attempt_count": _safe_int(getattr(result, "attempt_count", None)),
        "transport_error_code": _safe_summary_token(getattr(result, "transport_error_code", None)),
        "transport_error_class": _safe_summary_token(getattr(result, "transport_error_class", None)),
        "telegram_chat_id_present": getattr(result, "telegram_chat_id", None) is not None,
        "telegram_message_id_present": getattr(result, "telegram_message_id", None) is not None,
        "retry_after_seconds_present": getattr(result, "retry_after_seconds", None) is not None,
        "edited": bool(getattr(result, "edited", False)),
    }


def _safe_summary_token(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    lowered = text.lower()
    if not _SAFE_SUMMARY_TOKEN.fullmatch(text):
        return "redacted"
    if any(marker in lowered for marker in _SENSITIVE_SUMMARY_MARKERS):
        return "redacted"
    return text


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _uuid_or_none(value: object) -> UUID | None:
    try:
        return UUID(str(value or ""))
    except (TypeError, ValueError, AttributeError):
        return None


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
