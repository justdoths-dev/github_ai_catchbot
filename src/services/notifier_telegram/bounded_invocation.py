from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .repositories import NotifierTelegramRepository
from .service import NotifierTelegramService

RUNNER_NAME = "bounded_notifier_dry_run_invocation_runner"
MODE = "notifier_dry_run_send_disabled_one_shot"
SCHEMA_VERSION = "bounded_notifier_dry_run_invocation_v1"
EXPECTED_EVENT_TYPE = "notification.plan.created.v1"
_UNUSED_REDIS_URL = "redis://not-used-by-bounded-notifier-dry-run"
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_]{1,120}$")


@dataclass(frozen=True, slots=True)
class BoundedNotifierDryRunInvocationConfig:
    trigger_event_id: str | None
    operator_approved: bool = False
    allow_database_write: bool = False


@dataclass(slots=True)
class BoundedInvocationState:
    database_session_opened: bool = False
    event_outbox_read_attempted: bool = False
    notifier_invocation_attempted: bool = False
    network_attempted: bool = False
    transport_attempted: bool = False
    telegram_send_called: bool = False
    telegram_edit_called: bool = False


@dataclass(frozen=True, slots=True)
class EventOutboxRecord:
    event_id: UUID
    event_type: str


@dataclass(frozen=True, slots=True)
class NotifierInvocationOutcome:
    delivery_result: object | None
    notifier_owned_write_counts: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BoundedNotifierRuntime:
    notifier_config: NotifierTelegramConfig
    load_event_outbox: Callable[[UUID], Awaitable[EventOutboxRecord | None]]
    invoke_notifier: Callable[[UUID], Awaitable[NotifierInvocationOutcome]]
    close: Callable[[bool], Awaitable[None]]


class BoundedNotifierRuntimeBuilder(Protocol):
    async def __call__(
        self,
        notifier_config: NotifierTelegramConfig,
        state: BoundedInvocationState,
        logger: logging.Logger,
    ) -> BoundedNotifierRuntime: ...


class BoundedNotifierInvocationError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class ForbiddenTelegramTransportProbe:
    def __init__(self, state: BoundedInvocationState) -> None:
        self._state = state

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self._state.transport_attempted = True
        self._state.telegram_send_called = True
        raise AssertionError("telegram send is forbidden for bounded dry-run invocation")

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self._state.transport_attempted = True
        self._state.telegram_edit_called = True
        raise AssertionError("telegram edit is forbidden for bounded dry-run invocation")


class CountingNotifierRepository:
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


@dataclass(frozen=True, slots=True)
class BoundedNotifierDryRunInvocationResult:
    status: str
    ok: bool
    error_code: str | None
    trigger_event_id_present: bool
    operator_approved: bool
    database_write_allowed: bool
    processed_event_count: int
    event_type_supported: bool = False
    delivery_result_summary: Mapping[str, Any] | None = None
    notifier_owned_write_counts: Mapping[str, int] = field(default_factory=dict)
    state: BoundedInvocationState = field(default_factory=BoundedInvocationState)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "trigger_event_id_present": self.trigger_event_id_present,
            "operator_approved": self.operator_approved,
            "database_write_allowed": self.database_write_allowed,
            "send_enabled": False,
            "dry_run": True,
            "edits_allowed": False,
            "network_attempted": self.state.network_attempted,
            "transport_attempted": self.state.transport_attempted,
            "processed_event_count": self.processed_event_count,
            "event_type_supported": self.event_type_supported,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "delivery_result_summary": dict(self.delivery_result_summary or {}),
            "notifier_owned_write_counts": dict(self.notifier_owned_write_counts),
            "redactions_applied": [
                "trigger_event_id_omitted",
                "database_url_omitted",
                "redis_url_omitted",
                "telegram_token_omitted",
                "telegram_request_omitted",
                "telegram_response_omitted",
                "rendered_message_text_omitted",
                "exception_detail_omitted",
                "source_raw_text_omitted",
            ],
            "side_effects": {
                "database_session_opened": self.state.database_session_opened,
                "event_outbox_read_attempted": self.state.event_outbox_read_attempted,
                "database_write_allowed": self.database_write_allowed,
                "notifier_invocation_attempted": self.state.notifier_invocation_attempted,
                "telegram_send_called": self.state.telegram_send_called,
                "telegram_edit_called": self.state.telegram_edit_called,
                "redis_stream_read": False,
                "redis_mutation": False,
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


def load_forced_dry_run_notifier_config(
    env: Mapping[str, str] | None = None,
) -> NotifierTelegramConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    if not database_url:
        raise BoundedNotifierInvocationError("database_url_missing")
    try:
        config = NotifierTelegramConfig(
            app_env=_env_value(source, "APP_ENV", "dev").lower() or "dev",
            database_url=database_url,
            redis_url=_env_value(source, "REDIS_URL", _UNUSED_REDIS_URL) or _UNUSED_REDIS_URL,
            telegram_bot_token="",
            queue_name=_env_value(source, "NOTIFIER_TELEGRAM_QUEUE_NAME", "q.notification.send"),
            consumer_group=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_GROUP", "notifier-telegram"),
            consumer_name=_env_value(source, "NOTIFIER_TELEGRAM_CONSUMER_NAME", RUNNER_NAME),
            batch_size=1,
            block_ms=1,
            dry_run=True,
            allow_edits=False,
            enable_notification_send=False,
            enable_digest_runtime=_bool_env(_env_value(source, "ENABLE_DIGEST_RUNTIME", "false")),
            max_message_chars=_int_env(source, "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", 3800),
            edit_window_minutes=_int_env(source, "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES", 180),
            telegram_api_base_url=_env_value(source, "TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
            request_timeout_sec=_float_env(source, "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", 10.0),
            log_level=_env_value(source, "LOG_LEVEL", "INFO").upper() or "INFO",
        )
        config.validate(require_transport_token=False)
    except (NotifierTelegramConfigurationError, ValueError) as exc:
        raise BoundedNotifierInvocationError("runtime_config_error") from exc
    return config


async def run_bounded_notifier_dry_run_invocation(
    config: BoundedNotifierDryRunInvocationConfig,
    *,
    notifier_config_loader: Callable[[], NotifierTelegramConfig] = load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierDryRunInvocationResult:
    state = BoundedInvocationState()
    trigger_event_id_present = bool(str(config.trigger_event_id or "").strip())
    if not config.operator_approved:
        return _result(
            "blocked",
            "operator_approval_missing",
            config=config,
            state=state,
            trigger_event_id_present=trigger_event_id_present,
        )
    trigger_event_id = _uuid_or_none(config.trigger_event_id)
    if trigger_event_id is None:
        return _result(
            "blocked",
            "trigger_event_id_missing",
            config=config,
            state=state,
            trigger_event_id_present=trigger_event_id_present,
        )
    if not config.allow_database_write:
        return _result(
            "blocked",
            "database_write_not_allowed",
            config=config,
            state=state,
            trigger_event_id_present=trigger_event_id_present,
        )

    try:
        notifier_config = _force_dry_run_config(notifier_config_loader())
    except BoundedNotifierInvocationError as exc:
        return _result(
            "blocked",
            exc.error_code,
            config=config,
            state=state,
            trigger_event_id_present=trigger_event_id_present,
        )
    except Exception:
        return _result(
            "blocked",
            "runtime_config_error",
            config=config,
            state=state,
            trigger_event_id_present=trigger_event_id_present,
        )

    runtime: BoundedNotifierRuntime | None = None
    commit_runtime = False
    try:
        builder = runtime_builder or build_default_bounded_notifier_runtime
        runtime = await builder(notifier_config, state, logger or logging.getLogger(__name__))
        state.event_outbox_read_attempted = True
        event = await runtime.load_event_outbox(trigger_event_id)
        if event is None:
            return _result(
                "blocked",
                "event_outbox_event_missing",
                config=config,
                state=state,
                trigger_event_id_present=True,
            )
        if event.event_type != EXPECTED_EVENT_TYPE:
            return _result(
                "blocked",
                "unsupported_event_type",
                config=config,
                state=state,
                trigger_event_id_present=True,
            )
        state.notifier_invocation_attempted = True
        outcome = await runtime.invoke_notifier(trigger_event_id)
        if outcome.delivery_result is None:
            return _result(
                "failed",
                "notifier_invocation_no_result",
                config=config,
                state=state,
                trigger_event_id_present=True,
                event_type_supported=True,
                notifier_owned_write_counts=outcome.notifier_owned_write_counts,
            )
        commit_runtime = True
        return _result(
            "pass",
            None,
            config=config,
            state=state,
            trigger_event_id_present=True,
            event_type_supported=True,
            processed_event_count=1,
            delivery_result_summary=_delivery_result_summary(outcome.delivery_result),
            notifier_owned_write_counts=outcome.notifier_owned_write_counts,
        )
    except Exception:
        return _result(
            "failed",
            "notifier_invocation_failed",
            config=config,
            state=state,
            trigger_event_id_present=True,
        )
    finally:
        if runtime is not None:
            try:
                await runtime.close(commit_runtime)
            except Exception:
                pass


async def build_default_bounded_notifier_runtime(
    notifier_config: NotifierTelegramConfig,
    state: BoundedInvocationState,
    logger: logging.Logger,
) -> BoundedNotifierRuntime:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(notifier_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    base_repository = NotifierTelegramRepository(session)
    repository = CountingNotifierRepository(base_repository)
    transport_probe = ForbiddenTelegramTransportProbe(state)
    service = NotifierTelegramService(
        notifier_config,
        repository=repository,  # type: ignore[arg-type]
        telegram_client=transport_probe,  # type: ignore[arg-type]
        logger=logger,
    )

    async def load_event(event_id: UUID) -> EventOutboxRecord | None:
        row = await repository.load_event_outbox(event_id)
        if row is None:
            return None
        return EventOutboxRecord(event_id=UUID(str(row["event_id"])), event_type=str(row["event_type"]))

    async def invoke(event_id: UUID) -> NotifierInvocationOutcome:
        result = await service.handle_trigger_event(event_id)
        return NotifierInvocationOutcome(
            delivery_result=result,
            notifier_owned_write_counts=dict(repository.write_counts),
        )

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedNotifierRuntime(
        notifier_config=notifier_config,
        load_event_outbox=load_event,
        invoke_notifier=invoke,
        close=close,
    )


def run_bounded_notifier_dry_run_invocation_sync(
    config: BoundedNotifierDryRunInvocationConfig,
    *,
    notifier_config_loader: Callable[[], NotifierTelegramConfig] = load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierDryRunInvocationResult:
    return asyncio.run(
        run_bounded_notifier_dry_run_invocation(
            config,
            notifier_config_loader=notifier_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def _force_dry_run_config(config: NotifierTelegramConfig) -> NotifierTelegramConfig:
    forced = NotifierTelegramConfig(
        app_env=config.app_env,
        database_url=config.database_url,
        redis_url=config.redis_url or _UNUSED_REDIS_URL,
        telegram_bot_token="",
        queue_name=config.queue_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        batch_size=1,
        block_ms=1,
        dry_run=True,
        allow_edits=False,
        enable_notification_send=False,
        enable_digest_runtime=config.enable_digest_runtime,
        max_message_chars=config.max_message_chars,
        edit_window_minutes=config.edit_window_minutes,
        telegram_api_base_url=config.telegram_api_base_url,
        request_timeout_sec=config.request_timeout_sec,
        log_level=config.log_level,
    )
    forced.validate(require_transport_token=False)
    return forced


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotifierDryRunInvocationConfig,
    state: BoundedInvocationState,
    trigger_event_id_present: bool,
    event_type_supported: bool = False,
    processed_event_count: int = 0,
    delivery_result_summary: Mapping[str, Any] | None = None,
    notifier_owned_write_counts: Mapping[str, int] | None = None,
) -> BoundedNotifierDryRunInvocationResult:
    return BoundedNotifierDryRunInvocationResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        trigger_event_id_present=trigger_event_id_present,
        operator_approved=config.operator_approved,
        database_write_allowed=config.allow_database_write,
        processed_event_count=processed_event_count,
        event_type_supported=event_type_supported,
        delivery_result_summary=delivery_result_summary,
        notifier_owned_write_counts=notifier_owned_write_counts or {},
        state=state,
    )


def _delivery_result_summary(result: object) -> dict[str, Any]:
    return {
        "delivery_status": _safe_token(getattr(result, "delivery_status", None)),
        "attempt_count": _safe_int(getattr(result, "attempt_count", None)),
        "transport_error_code": _safe_token(getattr(result, "transport_error_code", None)),
        "transport_error_class": _safe_token(getattr(result, "transport_error_class", None)),
        "telegram_chat_id_present": getattr(result, "telegram_chat_id", None) is not None,
        "telegram_message_id_present": getattr(result, "telegram_message_id", None) is not None,
        "retry_after_seconds_present": getattr(result, "retry_after_seconds", None) is not None,
        "edited": bool(getattr(result, "edited", False)),
    }


def _safe_token(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    lowered = text.lower()
    if not _SAFE_TOKEN.fullmatch(text):
        return "redacted"
    if any(marker in lowered for marker in ("token", "secret", "password", "credential", "database_url", "redis_url")):
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
