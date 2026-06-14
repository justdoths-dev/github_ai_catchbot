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

from .config import CollectorTelegramConfig
from .exceptions import TDLibTransportError
from .idempotency import IdempotencyPolicy
from .message_projection import MessageProjectionBuilder
from .models import SourceMessageProjection, TrackedChat
from .outbox import CollectorOutboxBuilder
from .repositories import CollectorRepository
from .tdlib_client import TDJsonTransport, TDLibClient

SCHEMA_VERSION = "bounded_telegram_collector_history_ingest_v1"
RUNNER_NAME = "bounded_telegram_collector_history_ingest_runner"
MODE = "telegram_collector_one_shot_history_ingest"
DEFAULT_MAX_MESSAGES = 1
MAX_MESSAGES_HARD_LIMIT = 3
HISTORY_READ_TIMEOUT_SEC = 30.0
HISTORY_RECEIVE_TIMEOUT_SEC = 1.0
AUTH_READY_TIMEOUT_SEC = 30.0
AUTH_RECEIVE_TIMEOUT_SEC = 1.0

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

JsonDict = dict[str, Any]
RuntimeConfigLoader = Callable[[], CollectorTelegramConfig]


class BoundedHistoryIngestError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class BoundedHistoryRepository(Protocol):
    def transaction(self) -> Any: ...

    async def get_active_joined_tracked_chat_by_registry_id(self, registry_id: str) -> TrackedChat | None: ...

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


class BoundedHistoryClient(Protocol):
    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int) -> Sequence[Mapping[str, Any]]: ...
    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_telegram_read: bool = False
    allow_database_write: bool = False
    allow_outbox_write: bool = False
    max_messages: int = DEFAULT_MAX_MESSAGES
    chat_id: int | None = None
    registry_id: str | None = None


@dataclass(slots=True)
class BoundedTelegramCollectorHistoryIngestState:
    runtime_config_attempted: bool = False
    runtime_builder_attempted: bool = False
    registry_lookup_attempted: bool = False
    tdlib_auth_ready_checked: bool = False
    tdlib_auth_ready: bool = False
    tdlib_parameters_submitted: bool = False
    tdlib_log_suppression_attempted: bool = False
    tdlib_log_suppression_confirmed: bool = False
    telegram_read_attempted: bool = False
    telegram_read_called: bool = False
    database_write_attempted: bool = False
    outbox_write_attempted: bool = False


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestRuntimeHandle:
    repository: BoundedHistoryRepository
    history_client: BoundedHistoryClient
    close: Callable[[bool], Awaitable[None]]


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


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestResult:
    status: str
    ok: bool
    error_code: str | None
    config: BoundedTelegramCollectorHistoryIngestConfig
    state: BoundedTelegramCollectorHistoryIngestState = field(default_factory=BoundedTelegramCollectorHistoryIngestState)
    target_selection_mode: str = "none"
    target_chat_id_suffix: str | None = None
    target_chat_id_sha256_12: str | None = None
    target_registry_id_suffix: str | None = None
    messages_requested: int = DEFAULT_MAX_MESSAGES
    messages_seen: int = 0
    source_messages_created_count: int = 0
    source_versions_appended_count: int = 0
    outbox_events_inserted_count: int = 0
    idempotent_noop_count: int = 0
    error_class: str | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        gates = {
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "telegram_read_allowed": self.config.allow_telegram_read,
            "database_write_allowed": self.config.allow_database_write,
            "outbox_write_allowed": self.config.allow_outbox_write,
        }
        side_effects = {
            "db_write": self.state.database_write_attempted,
            "redis_mutation": False,
            "telegram_read_called": self.state.telegram_read_called,
            "telegram_send_called": False,
            "telegram_edit_called": False,
            "openai_called": False,
            "github_called": False,
            "x_called": False,
            "web_called": False,
            "notification_table_write": False,
            "worker_started": False,
            "run_forever_called": False,
            "systemd_called": False,
            "docker_called": False,
            "alembic_called": False,
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "gates": gates,
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "telegram_read_allowed": self.config.allow_telegram_read,
            "database_write_allowed": self.config.allow_database_write,
            "outbox_write_allowed": self.config.allow_outbox_write,
            "runtime_config_attempted": self.state.runtime_config_attempted,
            "runtime_builder_attempted": self.state.runtime_builder_attempted,
            "registry_lookup_attempted": self.state.registry_lookup_attempted,
            "target_selection_mode": self.target_selection_mode,
            "target_chat_id_suffix": self.target_chat_id_suffix,
            "target_chat_id_sha256_12": self.target_chat_id_sha256_12,
            "target_registry_id_suffix": self.target_registry_id_suffix,
            "tdlib_auth_ready_checked": self.state.tdlib_auth_ready_checked,
            "tdlib_auth_ready": self.state.tdlib_auth_ready,
            "tdlib_parameters_submitted": self.state.tdlib_parameters_submitted,
            "tdlib_log_suppression_attempted": self.state.tdlib_log_suppression_attempted,
            "tdlib_log_suppression_confirmed": self.state.tdlib_log_suppression_confirmed,
            "telegram_read_attempted": self.state.telegram_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "outbox_write_attempted": self.state.outbox_write_attempted,
            "messages_requested": self.messages_requested,
            "messages_seen": self.messages_seen,
            "source_messages_created_count": self.source_messages_created_count,
            "source_versions_appended_count": self.source_versions_appended_count,
            "outbox_events_inserted_count": self.outbox_events_inserted_count,
            "idempotent_noop_count": self.idempotent_noop_count,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "redactions_applied": [
                "full_chat_id_omitted",
                "full_registry_id_omitted",
                "raw_message_json_omitted",
                "message_text_omitted",
                "database_url_omitted",
                "telegram_credentials_omitted",
                "tdlib_session_paths_omitted",
                "exception_detail_omitted",
            ],
            "side_effects": side_effects,
        }


@dataclass(frozen=True, slots=True)
class _ResolvedTarget:
    mode: str
    chat_id: int
    registry_id: str | None = None


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
                    return HistoryMessageApplyResult(idempotent_noop=True)

            self._state.database_write_attempted = True
            current_row = await self._repository.upsert_source_message(projection, platform="telegram")
            resolved_source_message_id = _require_source_message_id(current_row)

            changed, version_row = await self._repository.append_source_message_version_if_changed(
                source_message_id=resolved_source_message_id,
                projection=projection,
                version_reason="new" if existing is None else "reconcile",
                observed_at=observed_at,
                telegram_edit_date=projection.edited_at,
            )
            if not changed or version_row is None:
                return HistoryMessageApplyResult(idempotent_noop=True)

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
                    reconcile_reason="bounded_one_shot_history_ingest",
                )
            self._state.outbox_write_attempted = True
            inserted = await self._repository.insert_outbox_event(outbox)
            return HistoryMessageApplyResult(
                source_message_created=existing is None,
                source_version_appended=True,
                outbox_event_inserted=True if inserted is None else bool(inserted),
                idempotent_noop=False,
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

    async def close(commit: bool) -> None:
        try:
            if commit:
                await session.commit()
            else:
                await session.rollback()
        finally:
            with contextlib.suppress(Exception):
                await history_client.close()
            with contextlib.suppress(Exception):
                await session.close()
            with contextlib.suppress(Exception):
                await engine.dispose()

    return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
        repository=repository,
        history_client=history_client,
        close=close,
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
    target_mode = _target_selection_mode(config)

    def blocked(error_code: str, *, error_class: str | None = None) -> BoundedTelegramCollectorHistoryIngestResult:
        return BoundedTelegramCollectorHistoryIngestResult(
            status="blocked",
            ok=False,
            error_code=error_code,
            config=config,
            state=state,
            target_selection_mode=target_mode,
            target_registry_id_suffix=_safe_suffix(config.registry_id),
            messages_requested=config.max_messages,
            error_class=error_class,
        )

    if not config.operator_approved:
        return blocked("operator_approval_missing")
    if not _valid_max_messages(config.max_messages):
        return blocked("max_messages_out_of_bounds")
    if not config.allow_runtime_config:
        return blocked("runtime_config_not_allowed")
    if target_mode == "conflict":
        return blocked("target_selection_conflict")
    if target_mode == "none":
        return blocked("target_selection_missing")

    loader = runtime_config_loader or CollectorTelegramConfig.from_env
    state.runtime_config_attempted = True
    try:
        runtime_config = loader()
    except Exception as exc:
        return blocked("runtime_config_failed", error_class=_safe_exception_class(exc))

    if not config.allow_telegram_read:
        return blocked("telegram_read_not_allowed")
    if not config.allow_database_write:
        return blocked("database_write_not_allowed")
    if not config.allow_outbox_write:
        return blocked("outbox_write_not_allowed")

    builder = runtime_builder or build_default_bounded_history_ingest_runtime
    state.runtime_builder_attempted = True
    runtime: BoundedTelegramCollectorHistoryIngestRuntimeHandle | None = None
    commit = False
    result: BoundedTelegramCollectorHistoryIngestResult | None = None
    try:
        runtime = await builder(runtime_config, state, effective_logger)
        target = await _resolve_target(config, runtime.repository, state)

        state.telegram_read_attempted = True
        messages = await runtime.history_client.fetch_newest_history_messages(
            chat_id=target.chat_id,
            limit=config.max_messages,
        )
        state.telegram_read_called = True
        selected_messages = [dict(message) for message in messages if isinstance(message, Mapping)][: config.max_messages]

        processor = HistoryMessageIngestProcessor(
            repository=runtime.repository,
            projection_builder=MessageProjectionBuilder(logger=effective_logger),
            outbox_builder=CollectorOutboxBuilder(IdempotencyPolicy()),
            state=state,
        )

        created_count = 0
        version_count = 0
        outbox_count = 0
        noop_count = 0
        for message in reversed(selected_messages):
            applied = await processor.apply_history_message(message)
            created_count += int(applied.source_message_created)
            version_count += int(applied.source_version_appended)
            outbox_count += int(applied.outbox_event_inserted)
            noop_count += int(applied.idempotent_noop)

        commit = True
        result = BoundedTelegramCollectorHistoryIngestResult(
            status="completed",
            ok=True,
            error_code=None,
            config=config,
            state=state,
            target_selection_mode=target.mode,
            target_chat_id_suffix=_safe_suffix(target.chat_id),
            target_chat_id_sha256_12=_safe_hash12(target.chat_id),
            target_registry_id_suffix=_safe_suffix(target.registry_id),
            messages_requested=config.max_messages,
            messages_seen=len(selected_messages),
            source_messages_created_count=created_count,
            source_versions_appended_count=version_count,
            outbox_events_inserted_count=outbox_count,
            idempotent_noop_count=noop_count,
        )
    except BoundedHistoryIngestError as exc:
        result = blocked(exc.error_code)
    except Exception as exc:
        result = blocked("unexpected_failure", error_class=_safe_exception_class(exc))
    finally:
        if runtime is not None:
            try:
                await runtime.close(commit)
            except Exception as exc:
                result = blocked(_runtime_close_error_code(commit), error_class=_safe_exception_class(exc))

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
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def argument_error_report(error_code: str) -> dict[str, Any]:
    return BoundedTelegramCollectorHistoryIngestResult(
        status="blocked",
        ok=False,
        error_code=error_code,
        config=BoundedTelegramCollectorHistoryIngestConfig(),
    ).to_sanitized_dict()


async def _resolve_target(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    repository: BoundedHistoryRepository,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> _ResolvedTarget:
    if config.chat_id is not None:
        return _ResolvedTarget(mode="chat_id", chat_id=int(config.chat_id))
    if config.registry_id is None:
        raise BoundedHistoryIngestError("target_selection_missing")
    state.registry_lookup_attempted = True
    tracked = await repository.get_active_joined_tracked_chat_by_registry_id(config.registry_id)
    if tracked is None or tracked.chat_id is None:
        raise BoundedHistoryIngestError("registry_target_not_active_joined")
    return _ResolvedTarget(mode="registry_id", chat_id=int(tracked.chat_id), registry_id=config.registry_id)


def _target_selection_mode(config: BoundedTelegramCollectorHistoryIngestConfig) -> str:
    if config.chat_id is not None and config.registry_id is not None:
        return "conflict"
    if config.chat_id is not None:
        return "chat_id"
    if config.registry_id is not None:
        return "registry_id"
    return "none"


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


def _valid_max_messages(value: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_MESSAGES_HARD_LIMIT


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


def _safe_suffix(value: object | None, *, width: int = 4) -> str | None:
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
    "DEFAULT_MAX_MESSAGES",
    "MAX_MESSAGES_HARD_LIMIT",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "argument_error_report",
    "build_default_bounded_history_ingest_runtime",
    "render_sanitized_json",
    "run_bounded_telegram_collector_history_ingest",
    "run_bounded_telegram_collector_history_ingest_sync",
]
