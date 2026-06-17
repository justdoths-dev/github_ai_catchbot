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

import sqlalchemy as sa

from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .idempotency import classify_notifier_idempotency_state
from .models import NotificationIntentJob, NotifierIdempotencyReadback
from .repositories import NotifierTelegramRepository


SCHEMA_VERSION = "bounded_notifier_idempotent_noop_proof_message_v1"
RUNNER_NAME = "bounded_notifier_idempotent_noop_proof_message"
QUEUE_NAME = "q.notification.send"
EXPECTED_STAGE_NAME = "notify"
EXPECTED_ROOT_OBJECT_TYPE = "analysis"
EVENT_TYPE = "notification.plan.created.v1"
PROOF_KIND = "idempotent_noop_reprocess_v1"
DEFAULT_CONSUMER_GROUP = "notifier-telegram"
DEFAULT_CONSUMER_NAME = RUNNER_NAME
DEFAULT_XADD_MAXLEN = 10000
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f]{8}$")
PROOF_KEY_SUFFIX_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
ACK_SAFE_CLASSIFICATIONS = {"existing_plan_sent", "existing_terminal_delivery"}


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotentNoopProofMessageConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_redis_read: bool = False
    allow_redis_publish: bool = False
    allow_proof_message_publish: bool = False
    require_telegram_disabled: bool = False
    mode: str = "preview"
    queue_name: str = QUEUE_NAME
    trigger_event_suffix: str | None = None
    analysis_suffix: str | None = None
    proof_idempotency_key_suffix: str | None = None


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotentNoopProofMessageRuntimeConfig:
    notifier_config: NotifierTelegramConfig
    redis_url: str
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    consumer_name: str = DEFAULT_CONSUMER_NAME
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedNotifierIdempotentNoopProofMessageState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    redis_client_created: bool = False
    redis_read_attempted: bool = False
    redis_publish_attempted: bool = False
    redis_consume_called: bool = False
    redis_ack_called: bool = False
    notifier_called: bool = False
    telegram_send_called: bool = False
    telegram_edit_called: bool = False


class BoundedNotifierIdempotentNoopProofMessageError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class RedisProofQueueInspection:
    status: str
    error_code: str | None
    queue_type: str | None = None
    consumer_group_present: bool = False
    group_lag: int | None = None
    group_pending: int | None = None
    stream_tail_checked: bool = False
    stream_tail_count: int = 0


class BoundedNotifierIdempotentNoopProofMessageRuntime(Protocol):
    async def load_intents_by_event_suffix(
        self,
        *,
        event_suffix: str,
        limit: int,
    ) -> list[NotificationIntentJob]: ...
    async def load_readback(self, intent: NotificationIntentJob) -> NotifierIdempotencyReadback: ...
    async def inspect_redis_state(
        self,
        config: BoundedNotifierIdempotentNoopProofMessageConfig,
    ) -> RedisProofQueueInspection: ...
    async def publish_proof_message(self, fields: Mapping[str, str]) -> str: ...
    async def close(self) -> None: ...


class BoundedNotifierIdempotentNoopProofMessageRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotifierIdempotentNoopProofMessageRuntimeConfig,
        state: BoundedNotifierIdempotentNoopProofMessageState,
        logger: logging.Logger,
    ) -> BoundedNotifierIdempotentNoopProofMessageRuntime: ...


@dataclass(frozen=True, slots=True)
class BoundedNotifierIdempotentNoopProofMessageResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedNotifierIdempotentNoopProofMessageConfig
    state: BoundedNotifierIdempotentNoopProofMessageState = field(
        default_factory=BoundedNotifierIdempotentNoopProofMessageState
    )
    readback: NotifierIdempotencyReadback | None = None
    redis_inspection: RedisProofQueueInspection | None = None
    proof_publish_safe: bool = False
    proof_message_fields: Mapping[str, str] | None = None
    proof_message_id_suffix: str | None = None
    proof_message_published: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        readback = self.readback
        fields = dict(self.proof_message_fields or {})
        inspection = self.redis_inspection
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_event_suffix": self.config.trigger_event_suffix,
            "target_analysis_suffix": self.config.analysis_suffix,
            "queue_name": self.config.queue_name,
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "database_read_allowed": self.config.allow_database_read,
            "redis_read_allowed": self.config.allow_redis_read,
            "redis_publish_allowed": self.config.allow_redis_publish,
            "proof_message_publish_allowed": self.config.allow_proof_message_publish,
            "require_telegram_disabled": self.config.require_telegram_disabled,
            "runtime_config_loaded": self.state.runtime_config_loaded,
            "database_session_opened": self.state.database_session_opened,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "redis_client_created": self.state.redis_client_created,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "redis_consume_called": self.state.redis_consume_called,
            "redis_ack_called": self.state.redis_ack_called,
            "notifier_called": self.state.notifier_called,
            "telegram_send_called": self.state.telegram_send_called,
            "telegram_edit_called": self.state.telegram_edit_called,
            "pre_idempotency_classification": (
                readback.primary_classification if readback is not None else None
            ),
            "pre_idempotency_classifications": list(readback.classifications) if readback else [],
            "pre_notification_plan_count": _count(readback, "plan_count"),
            "pre_notification_render_count": _count(readback, "render_count"),
            "pre_notification_delivery_record_count": _count(readback, "delivery_record_count"),
            "pre_sent_delivery_count": _count(readback, "sent_delivery_count"),
            "pre_suppressed_delivery_count": _count(readback, "suppressed_delivery_count"),
            "proof_publish_safe": self.proof_publish_safe,
            "proof_message_published": self.proof_message_published,
            "proof_message_id_suffix": self.proof_message_id_suffix,
            "proof_message_stage_name": fields.get("stage_name"),
            "proof_message_root_object_type": fields.get("root_object_type"),
            "proof_message_trigger_event_id_present": bool(fields.get("trigger_event_id")),
            "proof_message_root_object_id_present": bool(fields.get("root_object_id")),
            "proof_message_has_payload_json": "payload_json" in fields,
            "proof_message_has_message_text": "message_text" in fields,
            "proof_message_has_chat_id": _has_chat_id_field(fields),
            "proof_kind": fields.get("proof_kind"),
            "redis_queue_type": inspection.queue_type if inspection else None,
            "redis_consumer_group_present": inspection.consumer_group_present if inspection else False,
            "redis_group_lag": inspection.group_lag if inspection else None,
            "redis_group_pending": inspection.group_pending if inspection else None,
            "redis_stream_tail_checked": inspection.stream_tail_checked if inspection else False,
            "redis_stream_tail_count": inspection.stream_tail_count if inspection else 0,
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
                "payload_json_omitted": True,
                "message_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "telegram_token_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class SqlAlchemyBoundedNotifierIdempotentNoopProofRepository:
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

    async def load_readback(self, intent: NotificationIntentJob) -> NotifierIdempotencyReadback:
        snapshots = await self._repository.load_idempotency_plan_snapshots(intent)
        return classify_notifier_idempotency_state(snapshots)


class DefaultBoundedNotifierIdempotentNoopProofMessageRuntime:
    def __init__(
        self,
        *,
        redis_client: Any,
        queue_name: str,
        consumer_group: str,
        xadd_maxlen: int | None,
        repository: SqlAlchemyBoundedNotifierIdempotentNoopProofRepository,
        session: Any,
        engine: Any,
        state: BoundedNotifierIdempotentNoopProofMessageState,
    ) -> None:
        self._redis_client = redis_client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._xadd_maxlen = xadd_maxlen
        self._repository = repository
        self._session = session
        self._engine = engine
        self._state = state

    async def load_intents_by_event_suffix(
        self,
        *,
        event_suffix: str,
        limit: int,
    ) -> list[NotificationIntentJob]:
        self._state.database_read_attempted = True
        return await self._repository.load_intents_by_event_suffix(
            event_suffix=event_suffix,
            limit=limit,
        )

    async def load_readback(self, intent: NotificationIntentJob) -> NotifierIdempotencyReadback:
        self._state.database_read_attempted = True
        return await self._repository.load_readback(intent)

    async def inspect_redis_state(
        self,
        config: BoundedNotifierIdempotentNoopProofMessageConfig,
    ) -> RedisProofQueueInspection:
        del config
        self._state.redis_read_attempted = True
        queue_type = _decode_redis_value(await self._redis_client.type(self._queue_name)).strip().lower()
        if queue_type == "none":
            return RedisProofQueueInspection(status="blocked", error_code="stream_missing", queue_type=queue_type)
        if queue_type != "stream":
            return RedisProofQueueInspection(status="blocked", error_code="queue_key_wrong_type", queue_type=queue_type)

        group = _find_group(await self._redis_client.xinfo_groups(self._queue_name), self._consumer_group)
        if group is None:
            return RedisProofQueueInspection(
                status="blocked",
                error_code="consumer_group_missing",
                queue_type=queue_type,
            )
        pending = _safe_int(group.get("pending"))
        lag = _safe_int(group.get("lag"))
        entries = await self._redis_client.xrevrange(self._queue_name, max="+", min="-", count=1)
        tail_count = len(entries or [])
        if pending not in (0, None):
            return RedisProofQueueInspection(
                status="blocked",
                error_code="redis_pending_messages_present",
                queue_type=queue_type,
                consumer_group_present=True,
                group_lag=lag,
                group_pending=pending,
                stream_tail_checked=True,
                stream_tail_count=tail_count,
            )
        if lag not in (0, None):
            return RedisProofQueueInspection(
                status="blocked",
                error_code="redis_existing_unconsumed_messages_present",
                queue_type=queue_type,
                consumer_group_present=True,
                group_lag=lag,
                group_pending=pending,
                stream_tail_checked=True,
                stream_tail_count=tail_count,
            )
        return RedisProofQueueInspection(
            status="matched",
            error_code=None,
            queue_type=queue_type,
            consumer_group_present=True,
            group_lag=lag,
            group_pending=pending,
            stream_tail_checked=True,
            stream_tail_count=tail_count,
        )

    async def publish_proof_message(self, fields: Mapping[str, str]) -> str:
        self._state.redis_publish_attempted = True
        if self._xadd_maxlen is None:
            message_id = await self._redis_client.xadd(self._queue_name, dict(fields))
        else:
            message_id = await self._redis_client.xadd(
                self._queue_name,
                dict(fields),
                maxlen=self._xadd_maxlen,
                approximate=True,
            )
        return _decode_redis_value(message_id)

    async def close(self) -> None:
        await self._session.close()
        close = getattr(self._redis_client, "aclose", None) or getattr(self._redis_client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result
        await self._engine.dispose()


async def build_default_bounded_notifier_idempotent_noop_proof_message_runtime(
    runtime_config: BoundedNotifierIdempotentNoopProofMessageRuntimeConfig,
    state: BoundedNotifierIdempotentNoopProofMessageState,
    logger: logging.Logger,
) -> BoundedNotifierIdempotentNoopProofMessageRuntime:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.notifier_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_client_created = True
    repository = SqlAlchemyBoundedNotifierIdempotentNoopProofRepository(session)
    return DefaultBoundedNotifierIdempotentNoopProofMessageRuntime(
        redis_client=redis_client,
        queue_name=runtime_config.notifier_config.queue_name,
        consumer_group=runtime_config.consumer_group,
        xadd_maxlen=runtime_config.xadd_maxlen,
        repository=repository,
        session=session,
        engine=engine,
        state=state,
    )


def load_bounded_notifier_idempotent_noop_proof_message_runtime_config(
    config: BoundedNotifierIdempotentNoopProofMessageConfig,
    env: Mapping[str, str] | None = None,
) -> BoundedNotifierIdempotentNoopProofMessageRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedNotifierIdempotentNoopProofMessageError("database_url_missing")
    if not redis_url:
        raise BoundedNotifierIdempotentNoopProofMessageError("redis_url_missing")
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
            block_ms=1,
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
        xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
        if xadd_maxlen is not None and xadd_maxlen <= 0:
            raise ValueError("xadd maxlen must be positive")
    except (NotifierTelegramConfigurationError, ValueError) as exc:
        raise BoundedNotifierIdempotentNoopProofMessageError("runtime_config_error") from exc
    return BoundedNotifierIdempotentNoopProofMessageRuntimeConfig(
        notifier_config=notifier_config,
        redis_url=redis_url,
        consumer_group=notifier_config.consumer_group,
        consumer_name=notifier_config.consumer_name,
        xadd_maxlen=xadd_maxlen,
    )


async def run_bounded_notifier_idempotent_noop_proof_message(
    config: BoundedNotifierIdempotentNoopProofMessageConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotifierIdempotentNoopProofMessageConfig],
        BoundedNotifierIdempotentNoopProofMessageRuntimeConfig,
    ] = load_bounded_notifier_idempotent_noop_proof_message_runtime_config,
    runtime_builder: BoundedNotifierIdempotentNoopProofMessageRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierIdempotentNoopProofMessageResult:
    state = BoundedNotifierIdempotentNoopProofMessageState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)
    runtime: BoundedNotifierIdempotentNoopProofMessageRuntime | None = None
    try:
        try:
            runtime_config = runtime_config_loader(config)
            state.runtime_config_loaded = True
        except BoundedNotifierIdempotentNoopProofMessageError as exc:
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

        builder = runtime_builder or build_default_bounded_notifier_idempotent_noop_proof_message_runtime
        runtime = await builder(runtime_config, state, logger or logging.getLogger(__name__))
        intents = await runtime.load_intents_by_event_suffix(
            event_suffix=str(config.trigger_event_suffix),
            limit=2,
        )
        if len(intents) != 1:
            return _result("blocked", "event_suffix_ambiguous_or_missing", config=config, state=state)
        intent = intents[0]
        if intent.event_type != EVENT_TYPE:
            return _result("blocked", "event_type_mismatch", config=config, state=state)
        if not str(intent.trigger_event_id).endswith(str(config.trigger_event_suffix)):
            return _result("blocked", "trigger_event_id_mismatch", config=config, state=state)
        if not str(intent.analysis_id).endswith(str(config.analysis_suffix)):
            return _result("blocked", "analysis_mismatch", config=config, state=state)

        readback = await runtime.load_readback(intent)
        try:
            redis_inspection = await runtime.inspect_redis_state(config)
        except Exception as exc:
            return _result(
                "failed",
                "redis_read_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                readback=readback,
            )

        proof_fields = _build_proof_message_fields(intent, config)
        safety_error = _proof_publish_safety_error(readback, redis_inspection)
        proof_safe = safety_error is None
        if config.mode == "preview":
            return _result(
                "pass" if proof_safe else "blocked",
                None if proof_safe else safety_error,
                config=config,
                state=state,
                readback=readback,
                redis_inspection=redis_inspection,
                proof_publish_safe=proof_safe,
                proof_message_fields=proof_fields,
            )
        if not proof_safe:
            return _result(
                "blocked",
                safety_error,
                config=config,
                state=state,
                readback=readback,
                redis_inspection=redis_inspection,
                proof_message_fields=proof_fields,
            )

        try:
            message_id = await runtime.publish_proof_message(proof_fields)
        except Exception as exc:
            return _result(
                "failed",
                "redis_xadd_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                readback=readback,
                redis_inspection=redis_inspection,
                proof_publish_safe=True,
                proof_message_fields=proof_fields,
            )
        return _result(
            "pass",
            None,
            config=config,
            state=state,
            readback=readback,
            redis_inspection=redis_inspection,
            proof_publish_safe=True,
            proof_message_fields=proof_fields,
            proof_message_id_suffix=_message_id_suffix(message_id),
            proof_message_published=True,
        )
    except Exception as exc:
        return _result(
            "failed",
            "idempotent_noop_proof_message_failed",
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


def run_bounded_notifier_idempotent_noop_proof_message_sync(
    config: BoundedNotifierIdempotentNoopProofMessageConfig,
    *,
    runtime_config_loader: Callable[
        [BoundedNotifierIdempotentNoopProofMessageConfig],
        BoundedNotifierIdempotentNoopProofMessageRuntimeConfig,
    ] = load_bounded_notifier_idempotent_noop_proof_message_runtime_config,
    runtime_builder: BoundedNotifierIdempotentNoopProofMessageRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotifierIdempotentNoopProofMessageResult:
    return asyncio.run(
        run_bounded_notifier_idempotent_noop_proof_message(
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
        config=BoundedNotifierIdempotentNoopProofMessageConfig(),
        state=BoundedNotifierIdempotentNoopProofMessageState(),
    ).to_sanitized_dict()


def _authority_gate_error(config: BoundedNotifierIdempotentNoopProofMessageConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if config.mode not in {"preview", "publish"}:
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
    if config.mode == "publish":
        if not config.allow_redis_publish:
            return "redis_publish_not_allowed"
        if not config.allow_proof_message_publish:
            return "proof_message_publish_not_allowed"
    return None


def _target_suffix_error(config: BoundedNotifierIdempotentNoopProofMessageConfig) -> str | None:
    if not _valid_uuid_suffix(config.trigger_event_suffix) or not _valid_uuid_suffix(config.analysis_suffix):
        return "suffix_ambiguous_or_missing"
    suffix = config.proof_idempotency_key_suffix
    if suffix is not None and not PROOF_KEY_SUFFIX_RE.fullmatch(suffix):
        return "proof_idempotency_key_suffix_invalid"
    return None


def _valid_uuid_suffix(value: str | None) -> bool:
    return bool(value and UUID_SUFFIX_RE.fullmatch(value.strip().lower()))


def _proof_publish_safety_error(
    readback: NotifierIdempotencyReadback,
    redis_inspection: RedisProofQueueInspection,
) -> str | None:
    if readback.primary_classification not in ACK_SAFE_CLASSIFICATIONS:
        return "pre_readback_not_proof_publish_safe"
    if redis_inspection.error_code is not None:
        return redis_inspection.error_code
    if redis_inspection.status != "matched":
        return "redis_state_not_publish_safe"
    return None


def _build_proof_message_fields(
    intent: NotificationIntentJob,
    config: BoundedNotifierIdempotentNoopProofMessageConfig,
) -> dict[str, str]:
    suffix = config.proof_idempotency_key_suffix or (
        f"{str(intent.trigger_event_id)[-8:]}:{str(intent.analysis_id)[-8:]}"
    )
    return {
        "job_id": str(intent.trigger_event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": EXPECTED_ROOT_OBJECT_TYPE,
        "root_object_id": str(intent.analysis_id),
        "idempotency_key": f"{PROOF_KIND}:{suffix}",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(intent.trigger_event_id),
        "proof_kind": PROOF_KIND,
    }


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotifierIdempotentNoopProofMessageConfig,
    state: BoundedNotifierIdempotentNoopProofMessageState,
    error_class: str | None = None,
    readback: NotifierIdempotencyReadback | None = None,
    redis_inspection: RedisProofQueueInspection | None = None,
    proof_publish_safe: bool = False,
    proof_message_fields: Mapping[str, str] | None = None,
    proof_message_id_suffix: str | None = None,
    proof_message_published: bool = False,
) -> BoundedNotifierIdempotentNoopProofMessageResult:
    return BoundedNotifierIdempotentNoopProofMessageResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        readback=readback,
        redis_inspection=redis_inspection,
        proof_publish_safe=proof_publish_safe,
        proof_message_fields=proof_message_fields,
        proof_message_id_suffix=proof_message_id_suffix,
        proof_message_published=proof_message_published,
    )


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


def _has_chat_id_field(fields: Mapping[str, str]) -> bool:
    return any("chat_id" in key.lower() for key in fields)


def _message_id_suffix(value: str) -> str:
    return str(value)[-8:] if value else None


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _safe_exception_class(exc: BaseException) -> str:
    text = type(exc).__name__
    return text if re.fullmatch(r"[A-Za-z0-9_]{1,120}", text) else "Exception"


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
    "BoundedNotifierIdempotentNoopProofMessageConfig",
    "BoundedNotifierIdempotentNoopProofMessageError",
    "BoundedNotifierIdempotentNoopProofMessageResult",
    "BoundedNotifierIdempotentNoopProofMessageRuntime",
    "BoundedNotifierIdempotentNoopProofMessageRuntimeBuilder",
    "BoundedNotifierIdempotentNoopProofMessageRuntimeConfig",
    "QUEUE_NAME",
    "PROOF_KIND",
    "RUNNER_NAME",
    "RedisProofQueueInspection",
    "SCHEMA_VERSION",
    "argument_error_report",
    "build_default_bounded_notifier_idempotent_noop_proof_message_runtime",
    "load_bounded_notifier_idempotent_noop_proof_message_runtime_config",
    "render_sanitized_json",
    "run_bounded_notifier_idempotent_noop_proof_message",
    "run_bounded_notifier_idempotent_noop_proof_message_sync",
]
