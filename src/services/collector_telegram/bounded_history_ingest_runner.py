from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher
from src.services.outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError

from .config import CollectorTelegramConfig
from .exceptions import TDLibTransportError
from .idempotency import IdempotencyPolicy
from .message_projection import MessageProjectionBuilder
from .models import SourceMessageProjection
from .outbox import CollectorOutboxBuilder
from .repositories import CollectorRepository
from .tdlib_client import TDJsonTransport, TDLibClient

SCHEMA_VERSION = "live_collector_one_channel_source_last_rollout_v1"
RUNNER_NAME = "bounded_collector_history_ingest_runner"
MODE_PLAN = "plan"
MODE_EXECUTE = "execute"
EXECUTE_CONFIRM_TOKEN = "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE"
DEFAULT_HISTORY_LIMIT = 10
MAX_HISTORY_LIMIT = 30
DEFAULT_MAX_MESSAGES = DEFAULT_HISTORY_LIMIT
MAX_MESSAGES_HARD_LIMIT = MAX_HISTORY_LIMIT
HISTORY_READ_TIMEOUT_SEC = 30.0
HISTORY_RECEIVE_TIMEOUT_SEC = 1.0
AUTH_READY_TIMEOUT_SEC = 30.0
AUTH_RECEIVE_TIMEOUT_SEC = 1.0
SOURCE_KIND_PUBLIC_USERNAME = "public_username"
QUEUE_NAME = "q.source.normalize"
STAGE_NAME = "normalize"
ROOT_OBJECT_TYPE = "source_message"
DEFAULT_XADD_MAXLEN = 10000

_INTERACTIVE_AUTHORIZATION_STATES = frozenset(
    {
        "authorizationStateWaitPhoneNumber",
        "authorizationStateWaitCode",
        "authorizationStateWaitOtherDeviceConfirmation",
        "authorizationStateWaitPassword",
    }
)
_TERMINAL_NOT_READY_AUTHORIZATION_STATES = frozenset(
    {
        "authorizationStateLoggingOut",
        "authorizationStateClosing",
        "authorizationStateClosed",
    }
)
_SOURCE_EVENT_TYPES = frozenset(
    {
        "source_message.created.v1",
        "source_message.edited.v1",
        "source_message.deleted.v1",
        "source_message.reconciled.v1",
    }
)

JsonDict = dict[str, Any]
RuntimeConfigLoader = Callable[[], CollectorTelegramConfig]


class BoundedHistoryIngestError(RuntimeError):
    def __init__(self, error_code: str, *, partial_publish: "PublishEventsResult | None" = None) -> None:
        super().__init__(error_code)
        self.error_code = error_code
        self.partial_publish = partial_publish


class _RunResultReady(Exception):
    pass


class BoundedHistoryRepository(Protocol):
    def transaction(self) -> Any: ...

    async def find_public_username_registry_targets(self, normalized_source_value: str) -> Sequence[Any]: ...

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None: ...

    async def get_latest_version(self, source_message_id: str) -> Mapping[str, Any] | None: ...

    async def upsert_source_message(
        self,
        projection: SourceMessageProjection,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]: ...

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]: ...

    async def insert_outbox_event(self, event: Any) -> bool | None: ...

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str) -> Any | None: ...

    async def mark_outbox_published(
        self,
        *,
        event_id: UUID,
        published_at: datetime | None = None,
    ) -> bool | None: ...

    async def update_channel_sync_cursor(
        self,
        *,
        registry_id: str,
        last_seen_message_id: int | None = None,
        last_seen_message_date: datetime | None = None,
        last_history_sync_at: datetime | None = None,
    ) -> None: ...

    async def count_source_message_versions(self, source_message_id: str) -> int: ...

    async def count_source_created_events(self, source_message_id: str) -> int: ...

    async def count_source_outbox_events(self, source_message_id: str) -> int: ...


class BoundedHistoryClient(Protocol):
    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int) -> Sequence[Mapping[str, Any]]: ...
    async def close(self) -> None: ...


class BoundedHistoryRedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestConfig:
    mode: str = MODE_PLAN
    source_kind: str = SOURCE_KIND_PUBLIC_USERNAME
    source_value: str | None = None
    registry_id_suffix: str | None = None
    history_limit: int = DEFAULT_HISTORY_LIMIT
    operator_approved: bool = False
    confirm_token: str | None = None
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_telegram_read: bool = False
    allow_database_write: bool = False
    allow_source_message_write: bool = False
    allow_source_version_write: bool = False
    allow_source_outbox_write: bool = False
    allow_source_outbox_publish: bool = False
    allow_redis_publish: bool = False
    # Legacy construction aliases kept for old tests/importers; the CLI no longer exposes them.
    allow_outbox_write: bool = False
    max_messages: int | None = None
    chat_id: int | None = None
    registry_id: str | None = None


@dataclass(slots=True)
class BoundedTelegramCollectorHistoryIngestState:
    runtime_config_attempted: bool = False
    runtime_builder_attempted: bool = False
    database_read_attempted: bool = False
    registry_lookup_attempted: bool = False
    registry_targets_seen_count: int = 0
    tdlib_auth_ready_checked: bool = False
    tdlib_auth_ready: bool = False
    tdlib_parameters_submitted: bool = False
    tdlib_log_suppression_attempted: bool = False
    tdlib_log_suppression_confirmed: bool = False
    telegram_read_attempted: bool = False
    telegram_read_called: bool = False
    source_message_write_attempted: bool = False
    source_version_write_attempted: bool = False
    source_outbox_write_attempted: bool = False
    channel_cursor_write_attempted: bool = False
    source_outbox_publish_attempted: bool = False
    redis_publish_attempted: bool = False
    event_outbox_status_write_attempted: bool = False

    @property
    def database_write_attempted(self) -> bool:
        return (
            self.source_message_write_attempted
            or self.source_version_write_attempted
            or self.source_outbox_write_attempted
            or self.channel_cursor_write_attempted
            or self.event_outbox_status_write_attempted
        )


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestRuntimeHandle:
    repository: BoundedHistoryRepository
    history_client: BoundedHistoryClient
    close: Callable[[bool], Awaitable[None]]
    redis_publisher: BoundedHistoryRedisPublisher | None = None
    commit: Callable[[], Awaitable[None]] | None = None
    rollback: Callable[[], Awaitable[None]] | None = None


class BoundedTelegramCollectorHistoryIngestRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: CollectorTelegramConfig,
        state: BoundedTelegramCollectorHistoryIngestState,
        logger: logging.Logger,
    ) -> BoundedTelegramCollectorHistoryIngestRuntimeHandle: ...


@dataclass(frozen=True, slots=True)
class HistoryMessageApplyResult:
    source_message_created: bool = False
    source_version_appended: bool = False
    outbox_event_inserted: bool = False
    idempotent_noop: bool = False
    source_message_id_suffix: str | None = None
    outbox_event: OutboxEventRow | None = None


@dataclass(frozen=True, slots=True)
class HistoryMessagePreviewResult:
    would_insert_source_message: bool = False
    would_append_source_version: bool = False
    would_insert_outbox_event: bool = False
    would_skip_same_hash: bool = False


@dataclass(frozen=True, slots=True)
class PublishEventsResult:
    published_count: int = 0
    marked_published_count: int = 0
    event_fingerprints: tuple[str, ...] = ()
    redis_message_fingerprints: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SourceLastReadbackResult:
    source_current_found_count: int = 0
    source_version_rows_count: int = 0
    source_created_events_count: int = 0
    source_outbox_events_count: int = 0
    source_message_fingerprints: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestResult:
    status: str
    ok: bool
    error_code: str | None
    config: BoundedTelegramCollectorHistoryIngestConfig
    state: BoundedTelegramCollectorHistoryIngestState = field(default_factory=BoundedTelegramCollectorHistoryIngestState)
    mode: str = MODE_PLAN
    source_kind: str = SOURCE_KIND_PUBLIC_USERNAME
    source_value_surface: str | None = None
    target_registry_id_suffix: str | None = None
    target_joined: bool = False
    target_chat_id_present: bool = False
    messages_requested: int = DEFAULT_HISTORY_LIMIT
    messages_seen: int = 0
    would_insert_source_messages_count: int = 0
    would_append_source_versions_count: int = 0
    would_insert_outbox_events_count: int = 0
    would_skip_same_hash_count: int = 0
    source_messages_created_count: int = 0
    source_versions_appended_count: int = 0
    outbox_events_inserted_count: int = 0
    source_created_events_count: int = 0
    idempotent_noop_count: int = 0
    duplicate_noop_proof_count: int = 0
    redis_events_published_count: int = 0
    event_outbox_marked_published_count: int = 0
    source_message_id_suffixes: tuple[str, ...] = ()
    event_id_suffixes: tuple[str, ...] = ()
    redis_message_id_suffixes: tuple[str, ...] = ()
    exact_channel_target_fingerprint: str | None = None
    registry_target_fingerprint: str | None = None
    source_message_fingerprints: tuple[str, ...] = ()
    source_outbox_event_fingerprints: tuple[str, ...] = ()
    redis_message_fingerprints: tuple[str, ...] = ()
    readback: SourceLastReadbackResult = field(default_factory=SourceLastReadbackResult)
    error_class: str | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        gates = {
            "operator_approved": self.config.operator_approved,
            "confirm_token_present": bool((self.config.confirm_token or "").strip()),
            "confirm_token_valid": _confirm_token_valid(self.config),
            "runtime_config_allowed": self.config.allow_runtime_config,
            "database_read_allowed": self.config.allow_database_read,
            "telegram_read_allowed": self.config.allow_telegram_read,
            "database_write_allowed": self.config.allow_database_write,
            "source_message_write_allowed": self.config.allow_source_message_write,
            "source_version_write_allowed": self.config.allow_source_version_write,
            "source_outbox_write_allowed": _source_outbox_write_allowed(self.config),
            "source_outbox_publish_allowed": self.config.allow_source_outbox_publish,
            "redis_publish_allowed": self.config.allow_redis_publish,
        }
        side_effects = {
            "db_write": self.state.database_write_attempted,
            "redis_mutation": self.redis_events_published_count > 0,
            "telegram_read_called": self.state.telegram_read_called,
            "telegram_send_called": False,
            "telegram_edit_called": False,
            "openai_called": False,
            "github_called": False,
            "x_called": False,
            "web_called": False,
            "notification_table_write": False,
            "worker_loop_started": False,
            "systemd_called": False,
            "docker_called": False,
            "alembic_called": False,
        }
        authority = {
            "live_telegram_read_attempted": self.state.telegram_read_attempted,
            "telegram_send_attempted": False,
            "openai_attempted": False,
            "github_attempted": False,
            "x_attempted": False,
            "web_attempted": False,
            "redis_consume_or_ack": False,
            "broad_registry_ingest": False,
            "docker_or_systemd_called": False,
            "alembic_or_ddl_ran": False,
        }
        bounded_counts = {
            "registry_targets": self.state.registry_targets_seen_count,
            "source_messages_created": self.source_messages_created_count,
            "source_versions_created": self.source_versions_appended_count,
            "source_created_events": self.source_created_events_count,
            "source_normalize_handoffs": self.redis_events_published_count,
            "duplicate_noops": self.duplicate_noop_proof_count,
        }
        readback = {
            "source_current_found_count": self.readback.source_current_found_count,
            "source_version_rows_count": self.readback.source_version_rows_count,
            "source_created_events_count": self.readback.source_created_events_count,
            "source_outbox_events_count": self.readback.source_outbox_events_count,
        }
        redactions_applied = {
            "full_chat_id_omitted": True,
            "full_registry_id_omitted": True,
            "source_ref_omitted": True,
            "full_source_message_id_omitted": True,
            "full_event_id_omitted": True,
            "full_redis_message_id_omitted": True,
            "raw_message_json_omitted": True,
            "message_text_omitted": True,
            "entities_json_omitted": True,
            "url_surface_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "telegram_credentials_omitted": True,
            "tdlib_session_paths_omitted": True,
            "exception_detail_omitted": True,
            "traceback_omitted": True,
            "stderr_omitted": True,
        }
        raw_values_printed = {
            "source_text": False,
            "source_ref": False,
            "url": False,
            "raw_id": False,
            "tdlib_payload": False,
            "database_url": False,
            "redis_url": False,
            "secret": False,
            "runtime_value": False,
            "stderr": False,
            "traceback": False,
            "exception_body": False,
        }
        rollback_stop_readback = {
            "always_on_collector_started": False,
            "broad_worker_started": False,
            "exact_runner_completed": self.ok,
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.mode,
            "status": _report_status(self.status, self.ok),
            "reason_code": self.error_code or "ok",
            "exact_channel_target_fingerprint": self.exact_channel_target_fingerprint,
            "registry_target_fingerprint": self.registry_target_fingerprint,
            "source_message_fingerprints": list(self.source_message_fingerprints),
            "source_outbox_event_fingerprints": list(self.source_outbox_event_fingerprints),
            "bounded_counts": bounded_counts,
            "authority": authority,
            "redactions_applied": redactions_applied,
            "raw_values_printed": raw_values_printed,
            "rollback_stop_readback": rollback_stop_readback,
            "readback": readback,
            "source_kind": self.source_kind,
            "source_value_surface": None,
            "source_value_fingerprint": self.exact_channel_target_fingerprint,
            "target_registry_id_suffix": None,
            "target_registry_fingerprint": self.registry_target_fingerprint,
            "target_joined": self.target_joined,
            "target_chat_id_present": self.target_chat_id_present,
            "gates": gates,
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "database_read_allowed": self.config.allow_database_read,
            "telegram_read_allowed": self.config.allow_telegram_read,
            "database_write_allowed": self.config.allow_database_write,
            "source_message_write_allowed": self.config.allow_source_message_write,
            "source_version_write_allowed": self.config.allow_source_version_write,
            "source_outbox_write_allowed": _source_outbox_write_allowed(self.config),
            "source_outbox_publish_allowed": self.config.allow_source_outbox_publish,
            "redis_publish_allowed": self.config.allow_redis_publish,
            "runtime_config_attempted": self.state.runtime_config_attempted,
            "runtime_builder_attempted": self.state.runtime_builder_attempted,
            "database_read_attempted": self.state.database_read_attempted,
            "registry_lookup_attempted": self.state.registry_lookup_attempted,
            "registry_targets_seen_count": self.state.registry_targets_seen_count,
            "tdlib_auth_ready_checked": self.state.tdlib_auth_ready_checked,
            "tdlib_auth_ready": self.state.tdlib_auth_ready,
            "tdlib_parameters_submitted": self.state.tdlib_parameters_submitted,
            "tdlib_log_suppression_attempted": self.state.tdlib_log_suppression_attempted,
            "tdlib_log_suppression_confirmed": self.state.tdlib_log_suppression_confirmed,
            "telegram_read_attempted": self.state.telegram_read_attempted,
            "telegram_read_called": self.state.telegram_read_called,
            "database_write_attempted": self.state.database_write_attempted,
            "source_message_write_attempted": self.state.source_message_write_attempted,
            "source_version_write_attempted": self.state.source_version_write_attempted,
            "source_outbox_write_attempted": self.state.source_outbox_write_attempted,
            "outbox_write_attempted": self.state.source_outbox_write_attempted,
            "channel_cursor_write_attempted": self.state.channel_cursor_write_attempted,
            "source_outbox_publish_attempted": self.state.source_outbox_publish_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "event_outbox_status_write_attempted": self.state.event_outbox_status_write_attempted,
            "messages_requested": self.messages_requested,
            "messages_seen": self.messages_seen,
            "would_insert_source_messages_count": self.would_insert_source_messages_count,
            "would_append_source_versions_count": self.would_append_source_versions_count,
            "would_insert_outbox_events_count": self.would_insert_outbox_events_count,
            "would_skip_same_hash_count": self.would_skip_same_hash_count,
            "source_messages_created_count": self.source_messages_created_count,
            "source_versions_appended_count": self.source_versions_appended_count,
            "outbox_events_inserted_count": self.outbox_events_inserted_count,
            "source_created_events_count": self.source_created_events_count,
            "idempotent_noop_count": self.idempotent_noop_count,
            "duplicate_noop_proof_count": self.duplicate_noop_proof_count,
            "redis_events_published_count": self.redis_events_published_count,
            "event_outbox_marked_published_count": self.event_outbox_marked_published_count,
            "source_message_id_suffixes": [],
            "event_id_suffixes": [],
            "redis_message_id_suffixes": [],
            "redis_message_fingerprints": list(self.redis_message_fingerprints),
            "ok": self.ok,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "side_effects": side_effects,
        }


@dataclass(frozen=True, slots=True)
class _ResolvedTarget:
    chat_id: int
    registry_id: str
    source_value_surface: str
    joined: bool
    chat_id_present: bool
    last_seen_message_id: int | None = None
    last_seen_message_date: datetime | None = None


class _TDLibBoundedHistoryClient:
    def __init__(
        self,
        tdlib: TDLibClient,
        *,
        state: BoundedTelegramCollectorHistoryIngestState,
        timeout_sec: float = HISTORY_READ_TIMEOUT_SEC,
        auth_ready_timeout_sec: float = AUTH_READY_TIMEOUT_SEC,
        require_native_log_suppression: bool = True,
    ) -> None:
        self._tdlib = tdlib
        self._state = state
        self._timeout_sec = timeout_sec
        self._auth_ready_timeout_sec = auth_ready_timeout_sec
        self._require_native_log_suppression = require_native_log_suppression

    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int) -> Sequence[Mapping[str, Any]]:
        await self._ensure_ready_for_history_read()
        request = self._tdlib.build_get_chat_history_request(
            chat_id=chat_id,
            from_message_id=0,
            offset=0,
            limit=limit,
            only_local=False,
        )
        payload = dict(request.payload)
        extra = f"{RUNNER_NAME}:{uuid4()}"
        payload["@extra"] = extra
        self._state.telegram_read_called = True
        await self._tdlib.send(payload)

        deadline = time.monotonic() + self._timeout_sec
        while time.monotonic() < deadline:
            receive_timeout = min(HISTORY_RECEIVE_TIMEOUT_SEC, max(0.0, deadline - time.monotonic()))
            response = await self._tdlib.receive(receive_timeout)
            if response is None:
                continue
            if response.get("@extra") != extra:
                continue
            if response.get("@type") == "error":
                raise BoundedHistoryIngestError("telegram_history_read_failed")
            messages = response.get("messages")
            if not isinstance(messages, list):
                raise BoundedHistoryIngestError("telegram_history_response_invalid")
            return tuple(message for message in messages if isinstance(message, Mapping))
        raise BoundedHistoryIngestError("telegram_history_read_timeout")

    async def close(self) -> None:
        await self._tdlib.close()

    async def _ensure_ready_for_history_read(self) -> None:
        try:
            await self._tdlib.initialize()
        except TDLibTransportError as exc:
            self._copy_log_suppression_state()
            if self._state.tdlib_log_suppression_attempted and not self._state.tdlib_log_suppression_confirmed:
                raise BoundedHistoryIngestError("tdlib_log_suppression_unconfirmed") from exc
            raise BoundedHistoryIngestError("tdlib_initialize_failed") from exc

        self._copy_log_suppression_state()
        if self._require_native_log_suppression and not self._state.tdlib_log_suppression_confirmed:
            raise BoundedHistoryIngestError("tdlib_log_suppression_unconfirmed")

        self._state.tdlib_auth_ready_checked = True
        await self._request_authorization_state()

        deadline = time.monotonic() + self._auth_ready_timeout_sec
        while time.monotonic() < deadline:
            receive_timeout = min(AUTH_RECEIVE_TIMEOUT_SEC, max(0.0, deadline - time.monotonic()))
            response = await self._tdlib.receive(receive_timeout)
            if response is None:
                continue
            authorization_state = _authorization_state_from_tdlib_payload(response)
            if authorization_state is None:
                if response.get("@type") == "error" and _is_bounded_auth_extra(response.get("@extra")):
                    raise BoundedHistoryIngestError("tdlib_parameters_required")
                continue

            state_type = authorization_state.get("@type")
            if state_type == "authorizationStateReady":
                self._state.tdlib_auth_ready = True
                return
            if state_type == "authorizationStateWaitTdlibParameters":
                await self._submit_tdlib_parameters()
                continue
            if state_type == "authorizationStateWaitEncryptionKey":
                await self._submit_database_encryption_key()
                continue
            if state_type in _INTERACTIVE_AUTHORIZATION_STATES:
                raise BoundedHistoryIngestError("tdlib_not_authorized")
            if state_type in _TERMINAL_NOT_READY_AUTHORIZATION_STATES:
                raise BoundedHistoryIngestError("tdlib_not_authorized")
            raise BoundedHistoryIngestError("tdlib_auth_state_invalid")

        raise BoundedHistoryIngestError("tdlib_auth_ready_timeout")

    async def _request_authorization_state(self) -> None:
        payload = dict(self._tdlib.build_get_authorization_state_request().payload)
        payload["@extra"] = f"{RUNNER_NAME}:auth_state:{uuid4()}"
        await self._tdlib.send(payload)

    async def _submit_tdlib_parameters(self) -> None:
        payload = dict(self._tdlib.build_set_tdlib_parameters_request().payload)
        payload["@extra"] = f"{RUNNER_NAME}:tdlib_parameters:{uuid4()}"
        self._state.tdlib_parameters_submitted = True
        await self._tdlib.send(payload)

    async def _submit_database_encryption_key(self) -> None:
        payload = dict(self._tdlib.build_check_database_encryption_key_request().payload)
        payload["@extra"] = f"{RUNNER_NAME}:database_encryption_key:{uuid4()}"
        await self._tdlib.send(payload)

    def _copy_log_suppression_state(self) -> None:
        self._state.tdlib_log_suppression_attempted = self._tdlib.native_log_suppression_attempted()
        self._state.tdlib_log_suppression_confirmed = self._tdlib.native_log_suppression_confirmed()


class HistoryMessageIngestProcessor:
    def __init__(
        self,
        *,
        repository: BoundedHistoryRepository,
        projection_builder: MessageProjectionBuilder,
        outbox_builder: CollectorOutboxBuilder,
        state: BoundedTelegramCollectorHistoryIngestState,
    ) -> None:
        self._repository = repository
        self._projection_builder = projection_builder
        self._outbox_builder = outbox_builder
        self._state = state

    async def preview_history_message(self, message: Mapping[str, Any]) -> HistoryMessagePreviewResult:
        projection = self._projection_builder.build_source_projection(dict(message))
        self._state.database_read_attempted = True
        existing = await self._repository.get_source_message(
            platform="telegram",
            chat_id=projection.chat_id,
            message_id=projection.message_id,
        )
        source_message_id = _coerce_source_message_id(existing)
        if source_message_id is None:
            return HistoryMessagePreviewResult(
                would_insert_source_message=True,
                would_append_source_version=True,
                would_insert_outbox_event=True,
            )
        latest = await self._repository.get_latest_version(source_message_id)
        if latest is not None and str(latest.get("content_hash")) == projection.content_hash:
            return HistoryMessagePreviewResult(would_skip_same_hash=True)
        return HistoryMessagePreviewResult(
            would_append_source_version=True,
            would_insert_outbox_event=True,
        )

    async def apply_history_message(self, message: Mapping[str, Any]) -> HistoryMessageApplyResult:
        projection = self._projection_builder.build_source_projection(dict(message))
        observed_at = datetime.now(timezone.utc)

        async with self._repository.transaction():
            existing = await self._repository.get_source_message(
                platform="telegram",
                chat_id=projection.chat_id,
                message_id=projection.message_id,
            )
            source_message_id = _coerce_source_message_id(existing)
            if source_message_id is not None:
                latest = await self._repository.get_latest_version(source_message_id)
                if latest is not None and str(latest.get("content_hash")) == projection.content_hash:
                    return HistoryMessageApplyResult(
                        idempotent_noop=True,
                        source_message_id_suffix=_safe_suffix(source_message_id),
                    )

            self._state.source_message_write_attempted = True
            current_row = await self._repository.upsert_source_message(projection, platform="telegram")
            resolved_source_message_id = _require_source_message_id(current_row)

            self._state.source_version_write_attempted = True
            changed, version_row = await self._repository.append_source_message_version_if_changed(
                source_message_id=resolved_source_message_id,
                projection=projection,
                version_reason="new" if existing is None else "reconcile",
                observed_at=observed_at,
                telegram_edit_date=projection.edited_at,
            )
            if not changed or version_row is None:
                return HistoryMessageApplyResult(
                    idempotent_noop=True,
                    source_message_id_suffix=_safe_suffix(resolved_source_message_id),
                )

            version_no = _require_version_no(version_row)
            if existing is None:
                outbox = self._outbox_builder.build_created(
                    source_message_id=resolved_source_message_id,
                    current_version_no=version_no,
                    logical_post_key=projection.logical_post_key,
                    occurred_at=projection.posted_at,
                )
            else:
                outbox = self._outbox_builder.build_reconciled(
                    source_message_id=resolved_source_message_id,
                    current_version_no=version_no,
                    logical_post_key=projection.logical_post_key,
                    occurred_at=observed_at,
                    reconcile_reason="bounded_history_ingest",
                )
            self._state.source_outbox_write_attempted = True
            inserted = await self._repository.insert_outbox_event(outbox)
            outbox_event = None
            if inserted is None or bool(inserted):
                outbox_event = _coerce_outbox_event_row(
                    await self._repository.get_outbox_event_by_dedupe_key(outbox.dedupe_key)
                )
                if outbox_event is None:
                    raise BoundedHistoryIngestError("outbox_event_readback_missing")
            return HistoryMessageApplyResult(
                source_message_created=existing is None,
                source_version_appended=True,
                outbox_event_inserted=True if inserted is None else bool(inserted),
                idempotent_noop=False,
                source_message_id_suffix=_safe_suffix(resolved_source_message_id),
                outbox_event=outbox_event,
            )


async def build_default_bounded_history_ingest_runtime(
    runtime_config: CollectorTelegramConfig,
    state: BoundedTelegramCollectorHistoryIngestState,
    logger: logging.Logger,
) -> BoundedTelegramCollectorHistoryIngestRuntimeHandle:
    runtime_config.ensure_runtime_dirs()
    engine = create_async_engine(runtime_config.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    session = session_factory()
    repository = CollectorRepository(session, logger=logger)
    tdlib = TDLibClient(runtime_config, transport=TDJsonTransport(), logger=logger)
    history_client = _TDLibBoundedHistoryClient(tdlib, state=state)
    redis_client: Any | None = None
    redis_publisher: RedisStreamsPublisher | None = None
    if runtime_config.redis_url:
        from redis.asyncio import Redis  # type: ignore[import-not-found]

        redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
        redis_publisher = RedisStreamsPublisher(redis_client, maxlen=DEFAULT_XADD_MAXLEN)

    transaction_finalized = False

    async def commit() -> None:
        nonlocal transaction_finalized
        await session.commit()
        transaction_finalized = True

    async def rollback() -> None:
        nonlocal transaction_finalized
        await session.rollback()
        transaction_finalized = True

    async def close(commit: bool) -> None:
        try:
            if commit:
                if not transaction_finalized:
                    await session.commit()
            else:
                await session.rollback()
        finally:
            with contextlib.suppress(Exception):
                await history_client.close()
            if redis_client is not None:
                close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
                if close_client is not None:
                    with contextlib.suppress(Exception):
                        result = close_client()
                        if hasattr(result, "__await__"):
                            await result
            with contextlib.suppress(Exception):
                await session.close()
            with contextlib.suppress(Exception):
                await engine.dispose()

    return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
        repository=repository,
        history_client=history_client,
        redis_publisher=redis_publisher,
        close=close,
        commit=commit,
        rollback=rollback,
    )


async def run_bounded_telegram_collector_history_ingest(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    runtime_config_loader: RuntimeConfigLoader | None = None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedTelegramCollectorHistoryIngestResult:
    state = BoundedTelegramCollectorHistoryIngestState()
    effective_logger = logger or logging.getLogger(__name__)
    mode = _normalize_mode(config.mode)
    history_limit = _history_limit(config)
    normalized_source_value = _normalize_source_value(config.source_value)

    def make_result(
        status: str,
        error_code: str | None,
        *,
        ok: bool = False,
        error_class: str | None = None,
        target: _ResolvedTarget | None = None,
        messages_seen: int = 0,
        would_insert: int = 0,
        would_append: int = 0,
        would_outbox: int = 0,
        would_skip: int = 0,
        created_count: int = 0,
        version_count: int = 0,
        outbox_count: int = 0,
        created_event_count: int = 0,
        noop_count: int = 0,
        duplicate_noop_count: int = 0,
        publish: PublishEventsResult | None = None,
        source_message_suffixes: Sequence[str] = (),
        source_outbox_event_fingerprints: Sequence[str] = (),
        readback: SourceLastReadbackResult | None = None,
    ) -> BoundedTelegramCollectorHistoryIngestResult:
        source_readback = readback or SourceLastReadbackResult()
        publish_event_fingerprints = () if publish is None else publish.event_fingerprints
        publish_redis_fingerprints = () if publish is None else publish.redis_message_fingerprints
        return BoundedTelegramCollectorHistoryIngestResult(
            status=status,
            ok=ok,
            error_code=error_code,
            config=config,
            state=state,
            mode=mode,
            source_kind=SOURCE_KIND_PUBLIC_USERNAME,
            source_value_surface=normalized_source_value,
            target_registry_id_suffix=_safe_suffix(target.registry_id) if target is not None else None,
            target_joined=target.joined if target is not None else False,
            target_chat_id_present=target.chat_id_present if target is not None else False,
            messages_requested=history_limit,
            messages_seen=messages_seen,
            would_insert_source_messages_count=would_insert,
            would_append_source_versions_count=would_append,
            would_insert_outbox_events_count=would_outbox,
            would_skip_same_hash_count=would_skip,
            source_messages_created_count=created_count,
            source_versions_appended_count=version_count,
            outbox_events_inserted_count=outbox_count,
            source_created_events_count=created_event_count,
            idempotent_noop_count=noop_count,
            duplicate_noop_proof_count=duplicate_noop_count,
            redis_events_published_count=0 if publish is None else publish.published_count,
            event_outbox_marked_published_count=0 if publish is None else publish.marked_published_count,
            source_message_id_suffixes=tuple(source_message_suffixes),
            event_id_suffixes=(),
            redis_message_id_suffixes=(),
            exact_channel_target_fingerprint=_fingerprint(
                "channel_target",
                f"{SOURCE_KIND_PUBLIC_USERNAME}:{normalized_source_value}",
            )
            if normalized_source_value is not None
            else None,
            registry_target_fingerprint=_target_fingerprint(target),
            source_message_fingerprints=source_readback.source_message_fingerprints,
            source_outbox_event_fingerprints=tuple(source_outbox_event_fingerprints or publish_event_fingerprints),
            redis_message_fingerprints=publish_redis_fingerprints,
            readback=source_readback,
            error_class=error_class,
        )

    if not config.operator_approved:
        return make_result("blocked", "operator_approval_missing")
    if mode not in {MODE_PLAN, MODE_EXECUTE}:
        return make_result("blocked", "mode_invalid")
    if config.source_kind != SOURCE_KIND_PUBLIC_USERNAME:
        return make_result("blocked", "source_kind_unsupported")
    if normalized_source_value is None:
        return make_result("blocked", "source_value_missing")
    if config.chat_id is not None or config.registry_id is not None:
        return make_result("blocked", "direct_chat_or_registry_id_target_not_allowed")
    if not _valid_history_limit(history_limit):
        return make_result("blocked", "history_limit_out_of_bounds")
    if mode == MODE_EXECUTE and not _confirm_token_valid(config):
        return make_result(
            "blocked",
            "confirm_token_missing" if not (config.confirm_token or "").strip() else "confirm_token_invalid",
        )
    if mode == MODE_PLAN:
        plan_error = _plan_authority_gate_error(config)
        if plan_error is not None:
            return make_result("blocked", plan_error)
    if not config.allow_runtime_config:
        return make_result("blocked", "runtime_config_not_allowed")

    loader = runtime_config_loader or CollectorTelegramConfig.from_env
    state.runtime_config_attempted = True
    try:
        runtime_config = loader()
    except Exception as exc:
        return make_result("blocked", "runtime_config_failed", error_class=_safe_exception_class(exc))

    if not config.allow_database_read:
        return make_result("blocked", "database_read_not_allowed")
    if mode == MODE_EXECUTE:
        write_error = _execute_write_gate_error(config)
        if write_error is not None:
            return make_result("blocked", write_error)
        if config.allow_source_outbox_publish and not config.allow_redis_publish:
            return make_result("blocked", "redis_publish_not_allowed")

    builder = runtime_builder or build_default_bounded_history_ingest_runtime
    state.runtime_builder_attempted = True
    runtime: BoundedTelegramCollectorHistoryIngestRuntimeHandle | None = None
    close_commit = False
    result: BoundedTelegramCollectorHistoryIngestResult | None = None
    try:
        runtime = await builder(runtime_config, state, effective_logger)
        target = await _resolve_exact_public_username_target(config, runtime.repository, state)

        if mode == MODE_PLAN:
            result = make_result("plan_completed", None, ok=True, target=target)
            raise _RunResultReady

        if not config.allow_telegram_read:
            result = make_result("blocked", "telegram_read_not_allowed", target=target)
            raise _RunResultReady

        state.telegram_read_attempted = True
        messages = await runtime.history_client.fetch_newest_history_messages(
            chat_id=target.chat_id,
            limit=history_limit,
        )
        state.telegram_read_called = True
        if len(messages) > history_limit:
            result = make_result("blocked", "history_result_exceeds_requested_limit", target=target)
            raise _RunResultReady
        selected_messages = [dict(message) for message in messages if isinstance(message, Mapping)][:history_limit]

        processor = HistoryMessageIngestProcessor(
            repository=runtime.repository,
            projection_builder=MessageProjectionBuilder(logger=effective_logger),
            outbox_builder=CollectorOutboxBuilder(IdempotencyPolicy()),
            state=state,
        )

        created_count = 0
        version_count = 0
        outbox_count = 0
        created_event_count = 0
        noop_count = 0
        outbox_events: list[OutboxEventRow] = []
        outbox_event_fingerprints: list[str] = []
        source_message_suffixes: list[str] = []
        last_seen_message_id: int | None = None
        last_seen_message_date: datetime | None = None
        for message in reversed(selected_messages):
            applied = await processor.apply_history_message(message)
            created_count += int(applied.source_message_created)
            version_count += int(applied.source_version_appended)
            outbox_count += int(applied.outbox_event_inserted)
            noop_count += int(applied.idempotent_noop)
            if applied.source_message_id_suffix is not None:
                source_message_suffixes.append(applied.source_message_id_suffix)
            if applied.outbox_event is not None:
                outbox_events.append(applied.outbox_event)
                outbox_event_fingerprints.append(_fingerprint("source_outbox_event", applied.outbox_event.event_id))
                created_event_count += int(applied.outbox_event.event_type == "source_message.created.v1")
            message_id = _message_id(message)
            message_date = _message_date(message)
            if message_id is not None and (last_seen_message_id is None or message_id > last_seen_message_id):
                last_seen_message_id = message_id
                last_seen_message_date = message_date

        if selected_messages and (created_count > 0 or version_count > 0 or outbox_count > 0):
            async with runtime.repository.transaction():
                state.channel_cursor_write_attempted = True
                await runtime.repository.update_channel_sync_cursor(
                    registry_id=target.registry_id,
                    last_seen_message_id=last_seen_message_id,
                    last_seen_message_date=last_seen_message_date,
                    last_history_sync_at=datetime.now(timezone.utc),
                )

        if runtime.commit is not None or config.allow_source_outbox_publish:
            try:
                await _commit_source_ingest(runtime)
            except BoundedHistoryIngestError as exc:
                result = make_result(
                    "failed",
                    exc.error_code,
                    target=target,
                    messages_seen=len(selected_messages),
                    created_count=created_count,
                    version_count=version_count,
                    outbox_count=outbox_count,
                    created_event_count=created_event_count,
                    noop_count=noop_count,
                    source_message_suffixes=source_message_suffixes,
                    source_outbox_event_fingerprints=outbox_event_fingerprints,
                )
                raise _RunResultReady
            except Exception as exc:
                result = make_result(
                    "failed",
                    "runtime_commit_failed",
                    error_class=_safe_exception_class(exc),
                    target=target,
                    messages_seen=len(selected_messages),
                    created_count=created_count,
                    version_count=version_count,
                    outbox_count=outbox_count,
                    created_event_count=created_event_count,
                    noop_count=noop_count,
                    source_message_suffixes=source_message_suffixes,
                    source_outbox_event_fingerprints=outbox_event_fingerprints,
                )
                raise _RunResultReady
        close_commit = True

        readback = SourceLastReadbackResult()
        duplicate_noop_count = 0
        try:
            duplicate_noop_count = await _prove_duplicate_noop(
                processor=processor,
                messages=selected_messages,
            )
            readback = await _readback_source_last_proof(
                repository=runtime.repository,
                projection_builder=MessageProjectionBuilder(logger=effective_logger),
                messages=selected_messages,
                target=target,
                state=state,
            )
            if runtime.commit is not None:
                await _commit_source_ingest(runtime)
        except BoundedHistoryIngestError as exc:
            close_commit = False
            result = make_result(
                "failed",
                exc.error_code,
                target=target,
                messages_seen=len(selected_messages),
                created_count=created_count,
                version_count=version_count,
                outbox_count=outbox_count,
                created_event_count=created_event_count,
                noop_count=noop_count,
                duplicate_noop_count=duplicate_noop_count,
                source_message_suffixes=source_message_suffixes,
                source_outbox_event_fingerprints=outbox_event_fingerprints,
                readback=readback,
            )
            raise _RunResultReady
        except Exception as exc:
            close_commit = False
            result = make_result(
                "failed",
                "source_readback_commit_failed",
                error_class=_safe_exception_class(exc),
                target=target,
                messages_seen=len(selected_messages),
                created_count=created_count,
                version_count=version_count,
                outbox_count=outbox_count,
                created_event_count=created_event_count,
                noop_count=noop_count,
                duplicate_noop_count=duplicate_noop_count,
                source_message_suffixes=source_message_suffixes,
                source_outbox_event_fingerprints=outbox_event_fingerprints,
                readback=readback,
            )
            raise _RunResultReady

        publish_result = PublishEventsResult()
        if config.allow_source_outbox_publish:
            if runtime.redis_publisher is None:
                close_commit = False
                result = make_result(
                    "blocked",
                    "redis_runtime_unavailable",
                    target=target,
                    messages_seen=len(selected_messages),
                    created_count=created_count,
                    version_count=version_count,
                    outbox_count=outbox_count,
                    created_event_count=created_event_count,
                    noop_count=noop_count,
                    duplicate_noop_count=duplicate_noop_count,
                    source_message_suffixes=source_message_suffixes,
                    source_outbox_event_fingerprints=outbox_event_fingerprints,
                    readback=readback,
                )
                raise _RunResultReady
            try:
                publish_result = await _publish_source_outbox_events(
                    repository=runtime.repository,
                    publisher=runtime.redis_publisher,
                    events=outbox_events,
                    state=state,
                    commit_mark_published=runtime.commit,
                )
            except BoundedHistoryIngestError as exc:
                close_commit = False
                result = make_result(
                    "failed",
                    exc.error_code,
                    error_class=_safe_exception_class(exc.__cause__) if exc.__cause__ is not None else None,
                    target=target,
                    messages_seen=len(selected_messages),
                    created_count=created_count,
                    version_count=version_count,
                    outbox_count=outbox_count,
                    created_event_count=created_event_count,
                    noop_count=noop_count,
                    duplicate_noop_count=duplicate_noop_count,
                    publish=exc.partial_publish,
                    source_message_suffixes=source_message_suffixes,
                    source_outbox_event_fingerprints=outbox_event_fingerprints,
                    readback=readback,
                )
                raise _RunResultReady

        result = make_result(
            "completed",
            None,
            ok=True,
            target=target,
            messages_seen=len(selected_messages),
            created_count=created_count,
            version_count=version_count,
            outbox_count=outbox_count,
            created_event_count=created_event_count,
            noop_count=noop_count,
            duplicate_noop_count=duplicate_noop_count,
            publish=publish_result,
            source_message_suffixes=source_message_suffixes,
            source_outbox_event_fingerprints=outbox_event_fingerprints,
            readback=readback,
        )
        raise _RunResultReady
    except _RunResultReady:
        pass
    except BoundedHistoryIngestError as exc:
        result = make_result("blocked", exc.error_code)
    except Exception as exc:
        result = make_result("failed", "unexpected_failure", error_class=_safe_exception_class(exc))
    finally:
        if runtime is not None:
            try:
                await runtime.close(close_commit)
            except Exception as exc:
                result = make_result(
                    "failed",
                    _runtime_close_error_code(close_commit),
                    error_class=_safe_exception_class(exc),
                )

    assert result is not None
    return result


def run_bounded_telegram_collector_history_ingest_sync(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    runtime_config_loader: RuntimeConfigLoader | None = None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedTelegramCollectorHistoryIngestResult:
    return asyncio.run(
        run_bounded_telegram_collector_history_ingest(
            config,
            runtime_config_loader=runtime_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return BoundedTelegramCollectorHistoryIngestResult(
        status="blocked",
        ok=False,
        error_code=error_code,
        config=BoundedTelegramCollectorHistoryIngestConfig(),
    ).to_sanitized_dict()


async def _resolve_exact_public_username_target(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    repository: BoundedHistoryRepository,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> _ResolvedTarget:
    normalized_source_value = _normalize_source_value(config.source_value)
    if normalized_source_value is None:
        raise BoundedHistoryIngestError("source_value_missing")
    state.registry_lookup_attempted = True
    state.database_read_attempted = True
    rows = list(await repository.find_public_username_registry_targets(normalized_source_value))
    state.registry_targets_seen_count = len(rows)
    if not rows:
        raise BoundedHistoryIngestError("registry_target_missing")
    if len(rows) > 1:
        raise BoundedHistoryIngestError("registry_target_multiple")

    row = rows[0]
    registry_id = _row_value(row, "registry_id")
    if not isinstance(registry_id, str) or not registry_id:
        registry_id = str(registry_id) if isinstance(registry_id, UUID) else ""
    if not registry_id:
        raise BoundedHistoryIngestError("registry_id_invalid")
    if config.registry_id_suffix and not registry_id.endswith(config.registry_id_suffix):
        raise BoundedHistoryIngestError("registry_id_suffix_mismatch")
    if _row_value(row, "source_kind") != SOURCE_KIND_PUBLIC_USERNAME:
        raise BoundedHistoryIngestError("source_kind_unsupported")
    if _row_value(row, "desired_state") != "active":
        raise BoundedHistoryIngestError("registry_target_not_active")
    access_state = _row_value(row, "access_state")
    if access_state != "joined":
        raise BoundedHistoryIngestError("registry_target_not_joined")
    chat_id = _row_value(row, "chat_id")
    if chat_id is None:
        raise BoundedHistoryIngestError("registry_target_chat_id_missing")
    try:
        chat_id_int = int(chat_id)
    except (TypeError, ValueError) as exc:
        raise BoundedHistoryIngestError("registry_target_chat_id_invalid") from exc
    return _ResolvedTarget(
        chat_id=chat_id_int,
        registry_id=registry_id,
        source_value_surface=normalized_source_value,
        joined=True,
        chat_id_present=True,
        last_seen_message_id=_optional_int(_row_value(row, "last_seen_message_id")),
        last_seen_message_date=_optional_datetime(_row_value(row, "last_seen_message_date")),
    )


async def _commit_source_ingest(runtime: BoundedTelegramCollectorHistoryIngestRuntimeHandle) -> None:
    if runtime.commit is None:
        raise BoundedHistoryIngestError("runtime_commit_unavailable")
    await runtime.commit()


async def _prove_duplicate_noop(
    *,
    processor: HistoryMessageIngestProcessor,
    messages: Sequence[Mapping[str, Any]],
) -> int:
    duplicate_noops = 0
    for message in reversed([dict(item) for item in messages if isinstance(item, Mapping)]):
        applied = await processor.apply_history_message(message)
        if not applied.idempotent_noop:
            raise BoundedHistoryIngestError("duplicate_noop_proof_failed")
        duplicate_noops += 1
    return duplicate_noops


async def _readback_source_last_proof(
    *,
    repository: BoundedHistoryRepository,
    projection_builder: MessageProjectionBuilder,
    messages: Sequence[Mapping[str, Any]],
    target: _ResolvedTarget,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> SourceLastReadbackResult:
    source_current_found = 0
    source_version_rows = 0
    source_created_events = 0
    source_outbox_events = 0
    fingerprints: list[str] = []
    seen_source_ids: set[str] = set()

    for message in reversed([dict(item) for item in messages if isinstance(item, Mapping)]):
        projection = projection_builder.build_source_projection(message)
        if projection.chat_id != target.chat_id:
            raise BoundedHistoryIngestError("non_target_channel_readback_mismatch")
        state.database_read_attempted = True
        row = await repository.get_source_message(
            platform="telegram",
            chat_id=projection.chat_id,
            message_id=projection.message_id,
        )
        if row is None:
            raise BoundedHistoryIngestError("source_current_readback_missing")
        source_message_id = _require_source_message_id(row)
        if source_message_id in seen_source_ids:
            continue
        seen_source_ids.add(source_message_id)
        latest = await repository.get_latest_version(source_message_id)
        if latest is None:
            raise BoundedHistoryIngestError("source_version_readback_missing")
        if str(latest.get("content_hash")) != projection.content_hash:
            raise BoundedHistoryIngestError("source_version_content_hash_mismatch")
        version_count = await repository.count_source_message_versions(source_message_id)
        created_count = await repository.count_source_created_events(source_message_id)
        outbox_count = await repository.count_source_outbox_events(source_message_id)
        if version_count < 1:
            raise BoundedHistoryIngestError("source_version_readback_missing")
        if outbox_count < 1:
            raise BoundedHistoryIngestError("source_outbox_readback_missing")
        source_current_found += 1
        source_version_rows += version_count
        source_created_events += created_count
        source_outbox_events += outbox_count
        fingerprints.append(_fingerprint("source_message", source_message_id))

    return SourceLastReadbackResult(
        source_current_found_count=source_current_found,
        source_version_rows_count=source_version_rows,
        source_created_events_count=source_created_events,
        source_outbox_events_count=source_outbox_events,
        source_message_fingerprints=tuple(fingerprints),
    )


async def _publish_source_outbox_events(
    *,
    repository: BoundedHistoryRepository,
    publisher: BoundedHistoryRedisPublisher,
    events: Sequence[OutboxEventRow],
    state: BoundedTelegramCollectorHistoryIngestState,
    commit_mark_published: Callable[[], Awaitable[None]] | None,
) -> PublishEventsResult:
    resolver = OutboxRouteResolver()
    published = 0
    marked = 0
    event_fingerprints: list[str] = []
    redis_fingerprints: list[str] = []
    for row in events:
        if row.status != "pending":
            continue
        if row.aggregate_type != ROOT_OBJECT_TYPE or row.event_type not in _SOURCE_EVENT_TYPES:
            raise BoundedHistoryIngestError("source_outbox_event_contract_mismatch")
        try:
            route = resolver.resolve(row)
        except UnsupportedOutboxEventTypeError as exc:
            raise BoundedHistoryIngestError("source_outbox_route_invalid") from exc
        if route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            raise BoundedHistoryIngestError("source_outbox_route_invalid")
        message = _build_stream_message(row, route)
        state.source_outbox_publish_attempted = True
        state.redis_publish_attempted = True
        try:
            redis_message_id = await publisher.publish(route, message)
        except Exception as exc:
            raise BoundedHistoryIngestError("redis_publish_failed") from exc
        partial_after_xadd = PublishEventsResult(
            published_count=published + 1,
            marked_published_count=marked,
            event_fingerprints=tuple(
                value for value in [*event_fingerprints, _fingerprint("source_outbox_event", row.event_id)] if value
            ),
            redis_message_fingerprints=tuple(
                value for value in [*redis_fingerprints, _fingerprint("redis_message", redis_message_id)] if value
            ),
        )
        try:
            async with repository.transaction():
                state.event_outbox_status_write_attempted = True
                try:
                    marked_result = await repository.mark_outbox_published(
                        event_id=row.event_id,
                        published_at=datetime.now(timezone.utc),
                    )
                except Exception as exc:
                    raise BoundedHistoryIngestError(
                        "event_outbox_mark_published_failed",
                        partial_publish=partial_after_xadd,
                    ) from exc
                if marked_result is False:
                    raise BoundedHistoryIngestError(
                        "event_outbox_mark_published_failed",
                        partial_publish=partial_after_xadd,
                    )
        except BoundedHistoryIngestError:
            raise
        except Exception as exc:
            raise BoundedHistoryIngestError(
                "event_outbox_mark_published_commit_failed",
                partial_publish=partial_after_xadd,
            ) from exc
        if commit_mark_published is None:
            raise BoundedHistoryIngestError(
                "event_outbox_mark_published_commit_unavailable",
                partial_publish=partial_after_xadd,
            )
        try:
            await commit_mark_published()
        except Exception as exc:
            raise BoundedHistoryIngestError(
                "event_outbox_mark_published_commit_failed",
                partial_publish=partial_after_xadd,
            ) from exc
        published += 1
        marked += 1
        event_fingerprints.append(_fingerprint("source_outbox_event", row.event_id))
        redis_fingerprints.append(_fingerprint("redis_message", redis_message_id))
    return PublishEventsResult(
        published_count=published,
        marked_published_count=marked,
        event_fingerprints=tuple(value for value in event_fingerprints if value),
        redis_message_fingerprints=tuple(value for value in redis_fingerprints if value),
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


def _coerce_outbox_event_row(row: Any | None) -> OutboxEventRow | None:
    if row is None:
        return None
    if isinstance(row, OutboxEventRow):
        return row
    if not isinstance(row, Mapping):
        raise BoundedHistoryIngestError("outbox_event_readback_invalid")
    payload = row.get("payload_json") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        fail_count=int(row.get("fail_count") or 0),
        created_at=row.get("created_at") or datetime.now(timezone.utc),
    )


def _authorization_state_from_tdlib_payload(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    if payload.get("@type") == "updateAuthorizationState":
        authorization_state = payload.get("authorization_state")
        return authorization_state if isinstance(authorization_state, Mapping) else None
    payload_type = payload.get("@type")
    if isinstance(payload_type, str) and payload_type.startswith("authorizationState"):
        return payload
    return None


def _is_bounded_auth_extra(value: object) -> bool:
    return isinstance(value, str) and value.startswith(f"{RUNNER_NAME}:")


def _normalize_mode(value: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized


def _history_limit(config: BoundedTelegramCollectorHistoryIngestConfig) -> int:
    value = config.history_limit if config.max_messages is None else config.max_messages
    return int(value)


def _valid_history_limit(value: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_HISTORY_LIMIT


def _normalize_source_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lstrip("@").strip().lower()
    return normalized or None


def _source_outbox_write_allowed(config: BoundedTelegramCollectorHistoryIngestConfig) -> bool:
    return bool(config.allow_source_outbox_write or config.allow_outbox_write)


def _confirm_token_valid(config: BoundedTelegramCollectorHistoryIngestConfig) -> bool:
    return (config.confirm_token or "").strip() == EXECUTE_CONFIRM_TOKEN


def _plan_authority_gate_error(config: BoundedTelegramCollectorHistoryIngestConfig) -> str | None:
    if config.allow_telegram_read:
        return "plan_telegram_read_not_allowed"
    if (
        config.allow_database_write
        or config.allow_source_message_write
        or config.allow_source_version_write
        or _source_outbox_write_allowed(config)
    ):
        return "plan_database_write_not_allowed"
    if config.allow_source_outbox_publish or config.allow_redis_publish:
        return "plan_redis_write_not_allowed"
    return None


def _execute_write_gate_error(config: BoundedTelegramCollectorHistoryIngestConfig) -> str | None:
    if not config.allow_telegram_read:
        return "telegram_read_not_allowed"
    if not config.allow_database_write:
        return "database_write_not_allowed"
    if not config.allow_source_message_write:
        return "source_message_write_not_allowed"
    if not config.allow_source_version_write:
        return "source_version_write_not_allowed"
    if not _source_outbox_write_allowed(config):
        return "source_outbox_write_not_allowed"
    return None


def _coerce_source_message_id(row: Mapping[str, Any] | None) -> str | None:
    if row is None:
        return None
    source_message_id = _coerce_uuid_string(row.get("source_message_id"))
    if source_message_id is not None:
        return source_message_id
    raise BoundedHistoryIngestError("source_message_id_invalid")


def _require_source_message_id(row: Mapping[str, Any]) -> str:
    source_message_id = _coerce_uuid_string(row.get("source_message_id"))
    if source_message_id is not None:
        return source_message_id
    raise BoundedHistoryIngestError("source_message_id_invalid")


def _coerce_uuid_string(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, UUID):
        return str(value)
    return None


def _require_version_no(row: Mapping[str, Any]) -> int:
    value = row.get("version_no")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise BoundedHistoryIngestError("source_version_no_invalid")


def _message_id(message: Mapping[str, Any]) -> int | None:
    try:
        return int(message["id"])
    except (KeyError, TypeError, ValueError):
        return None


def _message_date(message: Mapping[str, Any]) -> datetime | None:
    raw = message.get("date")
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _safe_suffix(value: object | None, *, width: int = 8) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text[-width:]


def _safe_hash12(value: object | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]


def _fingerprint(kind: str, value: object | None) -> str:
    digest = hashlib.sha256(f"{kind}:{value}".encode("utf-8")).hexdigest()[:16]
    return f"sha256:{digest}"


def _target_fingerprint(target: _ResolvedTarget | None) -> str | None:
    if target is None:
        return None
    return _fingerprint("registry_target", f"{target.registry_id}:{target.chat_id}")


def _report_status(status: str, ok: bool) -> str:
    if ok:
        return "pass"
    if status == "failed":
        return "failed"
    return "blocked"


def _runtime_close_error_code(commit: bool) -> str:
    return "runtime_commit_failed" if commit else "runtime_rollback_failed"


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


__all__ = [
    "BoundedHistoryIngestError",
    "BoundedTelegramCollectorHistoryIngestConfig",
    "BoundedTelegramCollectorHistoryIngestResult",
    "BoundedTelegramCollectorHistoryIngestRuntimeBuilder",
    "BoundedTelegramCollectorHistoryIngestRuntimeHandle",
    "BoundedTelegramCollectorHistoryIngestState",
    "DEFAULT_HISTORY_LIMIT",
    "DEFAULT_MAX_MESSAGES",
    "EXECUTE_CONFIRM_TOKEN",
    "MAX_HISTORY_LIMIT",
    "MAX_MESSAGES_HARD_LIMIT",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "argument_error_report",
    "build_default_bounded_history_ingest_runtime",
    "render_sanitized_json",
    "run_bounded_telegram_collector_history_ingest",
    "run_bounded_telegram_collector_history_ingest_sync",
]
