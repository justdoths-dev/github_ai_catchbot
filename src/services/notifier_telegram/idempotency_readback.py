from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

import sqlalchemy as sa

from .idempotency import classify_notifier_idempotency_state
from .models import NotifierIdempotencyReadback, NotifierPlanIdempotencySnapshot, NotificationIntentJob
from .repositories import NotifierTelegramRepository


SCHEMA_VERSION = "bounded_notifier_idempotency_readback_v1"
RUNNER_NAME = "bounded_notifier_idempotency_readback"
MODE = "read_only"
EVENT_TYPE = "notification.plan.created.v1"
DEFAULT_DB_SCAN_LIMIT = 2
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotencyReadbackConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    event_suffix: str | None = None
    analysis_suffix: str | None = None


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotencyRuntimeConfig:
    database_url: str


@dataclass(slots=True)
class BoundedNotifierIdempotencyReadbackState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False


class BoundedNotifierIdempotencyReadbackError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class BoundedNotifierIdempotencyRepository(Protocol):
    async def load_intents_by_event_suffix(
        self,
        *,
        event_suffix: str,
        limit: int,
    ) -> list[NotificationIntentJob]: ...
    async def load_idempotency_plan_snapshots(
        self,
        intent: NotificationIntentJob,
    ) -> list[NotifierPlanIdempotencySnapshot]: ...


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotencyRepositoryHandle:
    repository: BoundedNotifierIdempotencyRepository
    close: Callable[[], Awaitable[None]]


class BoundedNotifierIdempotencyRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotifierIdempotencyRuntimeConfig,
        state: BoundedNotifierIdempotencyReadbackState,
    ) -> BoundedNotifierIdempotencyRepositoryHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotencyReadbackResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedNotifierIdempotencyReadbackConfig
    state: BoundedNotifierIdempotencyReadbackState = field(default_factory=BoundedNotifierIdempotencyReadbackState)
    readback: NotifierIdempotencyReadback | None = None
    target_event_suffix: str | None = None
    target_analysis_suffix: str | None = None

    def to_sanitized_dict(self) -> dict[str, object]:
        readback = self.readback
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_event_suffix": self.target_event_suffix or self.config.event_suffix,
            "target_analysis_suffix": self.target_analysis_suffix or self.config.analysis_suffix,
            "primary_classification": readback.primary_classification if readback else None,
            "classifications": list(readback.classifications) if readback else [],
            "notification_plan_count": readback.plan_count if readback else 0,
            "notification_render_count": readback.render_count if readback else 0,
            "notification_delivery_record_count": readback.delivery_record_count if readback else 0,
            "sent_delivery_count": readback.sent_delivery_count if readback else 0,
            "suppressed_delivery_count": readback.suppressed_delivery_count if readback else 0,
            "terminal_delivery_count": readback.terminal_delivery_count if readback else 0,
            "retryable_failure_count": readback.retryable_failure_count if readback else 0,
            "sent_delivery_chat_id_present_count": (
                readback.sent_delivery_chat_id_present_count if readback else 0
            ),
            "sent_delivery_message_id_present_count": (
                readback.sent_delivery_message_id_present_count if readback else 0
            ),
            "notification_plan_suffixes": list(readback.plan_id_suffixes) if readback else [],
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "database_read_allowed": self.config.allow_database_read,
            "runtime_config_loaded": self.state.runtime_config_loaded,
            "database_session_opened": self.state.database_session_opened,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": False,
            "redis_read_attempted": False,
            "redis_ack_called": False,
            "redis_consume_called": False,
            "redis_publish_attempted": False,
            "notifier_called": False,
            "telegram_send_called": False,
            "telegram_edit_called": False,
            "openai_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "docker_or_systemd_called": False,
            "alembic_or_ddl_ran": False,
            "redactions_applied": {
                "full_event_id_omitted": True,
                "full_analysis_id_omitted": True,
                "full_notification_plan_ids_omitted": True,
                "target_chat_id_omitted": True,
                "telegram_chat_id_omitted": True,
                "telegram_message_id_omitted": True,
                "payload_json_omitted": True,
                "message_text_omitted": True,
                "database_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class SqlAlchemyBoundedNotifierIdempotencyRepository:
    def __init__(self, session: object) -> None:
        self._session = session
        self._repository = NotifierTelegramRepository(session)  # type: ignore[arg-type]

    async def load_intents_by_event_suffix(
        self,
        *,
        event_suffix: str,
        limit: int,
    ) -> list[NotificationIntentJob]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id
                FROM event_outbox
                WHERE event_type = :event_type
                  AND event_id::text LIKE :event_suffix_pattern
                ORDER BY created_at DESC, event_id DESC
                LIMIT :limit
                """
            ),
            {
                "event_type": EVENT_TYPE,
                "event_suffix_pattern": f"%{event_suffix}",
                "limit": limit,
            },
        )
        intents: list[NotificationIntentJob] = []
        for row in result.mappings().all():
            intent = await self._repository.load_intent_job(UUID(str(row["event_id"])))
            if intent is not None:
                intents.append(intent)
        return intents

    async def load_idempotency_plan_snapshots(
        self,
        intent: NotificationIntentJob,
    ) -> list[NotifierPlanIdempotencySnapshot]:
        return await self._repository.load_idempotency_plan_snapshots(intent)


async def build_default_bounded_notifier_idempotency_repository(
    runtime_config: BoundedNotifierIdempotencyRuntimeConfig,
    state: BoundedNotifierIdempotencyReadbackState,
) -> BoundedNotifierIdempotencyRepositoryHandle:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True

    async def close() -> None:
        await session.close()
        await engine.dispose()

    return BoundedNotifierIdempotencyRepositoryHandle(
        repository=SqlAlchemyBoundedNotifierIdempotencyRepository(session),
        close=close,
    )


def load_bounded_notifier_idempotency_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedNotifierIdempotencyRuntimeConfig:
    source = os.environ if env is None else env
    database_url = str(source.get("DATABASE_URL", "") or "").strip()
    if not database_url:
        raise BoundedNotifierIdempotencyReadbackError("database_url_missing")
    return BoundedNotifierIdempotencyRuntimeConfig(database_url=database_url)


async def run_bounded_notifier_idempotency_readback(
    config: BoundedNotifierIdempotencyReadbackConfig,
    *,
    runtime_config_loader: Callable[[], BoundedNotifierIdempotencyRuntimeConfig] = (
        load_bounded_notifier_idempotency_runtime_config
    ),
    repository_builder: BoundedNotifierIdempotencyRepositoryBuilder | None = None,
) -> BoundedNotifierIdempotencyReadbackResult:
    state = BoundedNotifierIdempotencyReadbackState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)
    suffix_error = _target_suffix_error(config)
    if suffix_error is not None:
        return _result("blocked", suffix_error, config=config, state=state)

    repository_handle: BoundedNotifierIdempotencyRepositoryHandle | None = None
    try:
        try:
            runtime_config = runtime_config_loader()
            state.runtime_config_loaded = True
        except BoundedNotifierIdempotencyReadbackError as exc:
            return _result("blocked", exc.error_code, config=config, state=state)
        except Exception as exc:
            return _result("blocked", "runtime_config_error", error_class=_safe_exception_class(exc), config=config, state=state)

        repository_handle = await (
            repository_builder or build_default_bounded_notifier_idempotency_repository
        )(runtime_config, state)
        state.database_read_attempted = True
        intents = await repository_handle.repository.load_intents_by_event_suffix(
            event_suffix=str(config.event_suffix),
            limit=DEFAULT_DB_SCAN_LIMIT,
        )
        if len(intents) != 1:
            return _result("blocked", "event_suffix_ambiguous_or_missing", config=config, state=state)
        intent = intents[0]
        if not str(intent.analysis_id).endswith(str(config.analysis_suffix)):
            return _result("blocked", "context_mismatch", config=config, state=state)

        snapshots = await repository_handle.repository.load_idempotency_plan_snapshots(intent)
        readback = classify_notifier_idempotency_state(snapshots)
        return _result(
            "pass",
            None,
            config=config,
            state=state,
            readback=readback,
            target_event_suffix=str(intent.trigger_event_id)[-8:],
            target_analysis_suffix=str(intent.analysis_id)[-8:],
        )
    except Exception as exc:
        return _result(
            "failed",
            "notifier_idempotency_readback_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )
    finally:
        if repository_handle is not None:
            try:
                await repository_handle.close()
            except Exception:
                pass


def run_bounded_notifier_idempotency_readback_sync(
    config: BoundedNotifierIdempotencyReadbackConfig,
    *,
    runtime_config_loader: Callable[[], BoundedNotifierIdempotencyRuntimeConfig] = (
        load_bounded_notifier_idempotency_runtime_config
    ),
    repository_builder: BoundedNotifierIdempotencyRepositoryBuilder | None = None,
) -> BoundedNotifierIdempotencyReadbackResult:
    return asyncio.run(
        run_bounded_notifier_idempotency_readback(
            config,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
        )
    )


def render_sanitized_json(report: Mapping[str, object]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, object]:
    return _result(
        "blocked",
        error_code,
        config=BoundedNotifierIdempotencyReadbackConfig(),
        state=BoundedNotifierIdempotencyReadbackState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotifierIdempotencyReadbackConfig,
    state: BoundedNotifierIdempotencyReadbackState,
    error_class: str | None = None,
    readback: NotifierIdempotencyReadback | None = None,
    target_event_suffix: str | None = None,
    target_analysis_suffix: str | None = None,
) -> BoundedNotifierIdempotencyReadbackResult:
    return BoundedNotifierIdempotencyReadbackResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        readback=readback,
        target_event_suffix=target_event_suffix,
        target_analysis_suffix=target_analysis_suffix,
    )


def _authority_gate_error(config: BoundedNotifierIdempotencyReadbackConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    return None


def _target_suffix_error(config: BoundedNotifierIdempotencyReadbackConfig) -> str | None:
    if not _valid_suffix(config.event_suffix) or not _valid_suffix(config.analysis_suffix):
        return "suffix_ambiguous_or_missing"
    return None


def _valid_suffix(value: str | None) -> bool:
    if value is None:
        return False
    return UUID_SUFFIX_RE.fullmatch(value.strip().lower()) is not None


def _safe_exception_class(exc: BaseException) -> str:
    text = type(exc).__name__
    return text if re.fullmatch(r"[A-Za-z0-9_]{1,120}", text) else "Exception"


__all__ = [
    "BoundedNotifierIdempotencyReadbackConfig",
    "BoundedNotifierIdempotencyReadbackError",
    "BoundedNotifierIdempotencyReadbackResult",
    "BoundedNotifierIdempotencyRepository",
    "BoundedNotifierIdempotencyRepositoryBuilder",
    "BoundedNotifierIdempotencyRepositoryHandle",
    "BoundedNotifierIdempotencyRuntimeConfig",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "SqlAlchemyBoundedNotifierIdempotencyRepository",
    "argument_error_report",
    "build_default_bounded_notifier_idempotency_repository",
    "load_bounded_notifier_idempotency_runtime_config",
    "render_sanitized_json",
    "run_bounded_notifier_idempotency_readback",
    "run_bounded_notifier_idempotency_readback_sync",
]

