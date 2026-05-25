from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import CollectorTelegramConfig
from .idempotency import IdempotencyPolicy
from .message_projection import MessageProjectionBuilder
from .outbox import CollectorOutboxBuilder
from .repositories import CollectorRepository
from .singleton_guard import CollectorSingletonGuard
from .tdlib_client import TDJsonTransport, TDLibClient
from .update_dispatcher import UpdateDispatcher
from .update_handlers import CollectorUpdateHandlers

JsonDict = dict[str, Any]

MAX_SMOKE_DURATION_SEC = 120
MAX_SMOKE_UPDATES = 100
MAX_SMOKE_DB_WRITES = 100
DEFAULT_RECEIVE_TIMEOUT_SEC = 1.0

COLLECTOR_OWNED_WRITE_TABLES = (
    "telegram_raw_updates",
    "source_messages",
    "source_message_versions",
    "event_outbox",
)

_FORBIDDEN_TDLIB_REQUEST_FLAGS = {
    "getChatHistory": "tdlib_history_fetch_called",
    "joinChat": "tdlib_join_called",
    "joinChatByInviteLink": "tdlib_join_called",
    "searchPublicChat": "tdlib_search_public_chat_called",
    "sendMessage": "tdlib_send_message_called",
}

_MANUAL_AUTHORIZATION_STATES = {
    "authorizationStateWaitPhoneNumber",
    "authorizationStateWaitCode",
    "authorizationStateWaitPassword",
}


class BoundedSmokeRunnerConfigError(RuntimeError):
    pass


class BoundedSmokeForbiddenTDLibAction(RuntimeError):
    pass


class BoundedSmokeManualAuthorizationRequired(RuntimeError):
    pass


class BoundedCollectorSmokePartialFailure(RuntimeError):
    def __init__(self, result: "BoundedCollectorSmokeResult") -> None:
        super().__init__(result.failure_class or "bounded collector smoke failed")
        self.result = result


class SmokeBoundsProtocol(Protocol):
    max_duration_sec: int
    max_updates: int
    max_db_writes: int


class SingletonGuardProtocol(Protocol):
    def acquire(self) -> None: ...

    def release(self) -> None: ...


class TDLibClientProtocol(Protocol):
    async def initialize(self) -> None: ...

    async def send(self, request: JsonDict) -> None: ...

    async def receive(self, timeout: float) -> JsonDict | None: ...

    async def close(self) -> None: ...

    def build_set_tdlib_parameters_request(self) -> Any: ...

    def build_check_database_encryption_key_request(self) -> Any: ...


class DispatcherProtocol(Protocol):
    async def dispatch(self, update: JsonDict) -> Any: ...


class DispatcherContextProtocol(Protocol):
    async def __aenter__(self) -> DispatcherProtocol: ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None: ...


DispatcherFactory = Callable[["CollectorSmokeWriteCounter"], DispatcherContextProtocol]
MonotonicClock = Callable[[], float]


@dataclass(slots=True)
class CollectorSmokeWriteCounter:
    telegram_raw_updates_written: int = 0
    source_messages_written: int = 0
    source_message_versions_written: int = 0
    event_outbox_written: int = 0
    written_tables: set[str] = field(default_factory=set)

    @property
    def total(self) -> int:
        return (
            self.telegram_raw_updates_written
            + self.source_messages_written
            + self.source_message_versions_written
            + self.event_outbox_written
        )

    def count_table(self, table_name: str, amount: int = 1) -> None:
        if amount <= 0:
            return
        if table_name not in COLLECTOR_OWNED_WRITE_TABLES:
            self.written_tables.add(table_name)
            return
        current = getattr(self, f"{table_name}_written")
        setattr(self, f"{table_name}_written", current + amount)
        self.written_tables.add(table_name)


@dataclass(frozen=True, slots=True)
class BoundedCollectorSmokeResult:
    status: str = "completed"
    failure_class: str | None = None
    updates_observed: int = 0
    telegram_raw_updates_written: int = 0
    source_messages_written: int = 0
    source_message_versions_written: int = 0
    event_outbox_written: int = 0
    duration_exhausted: bool = False
    update_cap_exhausted: bool = False
    db_write_cap_exhausted: bool = False
    written_tables: tuple[str, ...] = ()
    side_effects: Mapping[str, bool] = field(default_factory=dict)


class CountingCollectorRepository:
    def __init__(
        self,
        repository: Any,
        counter: CollectorSmokeWriteCounter,
    ) -> None:
        self._repository = repository
        self._counter = counter

    def transaction(self) -> Any:
        return self._repository.transaction()

    async def insert_raw_update(
        self,
        *,
        update_type: str,
        payload_json: JsonDict,
        chat_id: int | None = None,
        message_id: int | None = None,
    ) -> int:
        value = await self._repository.insert_raw_update(
            update_type=update_type,
            payload_json=payload_json,
            chat_id=chat_id,
            message_id=message_id,
        )
        self._counter.count_table("telegram_raw_updates")
        return value

    async def mark_raw_update_applied(self, update_seq: int) -> None:
        await self._repository.mark_raw_update_applied(update_seq)

    async def mark_raw_update_failed(self, update_seq: int, error_text: str) -> None:
        await self._repository.mark_raw_update_failed(update_seq, error_text)

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None:
        return await self._repository.get_source_message(
            platform=platform,
            chat_id=chat_id,
            message_id=message_id,
        )

    async def upsert_source_message(
        self,
        projection: Any,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]:
        value = await self._repository.upsert_source_message(
            projection,
            platform=platform,
        )
        self._counter.count_table("source_messages")
        return value

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: Any = None,
        telegram_edit_date: Any = None,
    ) -> tuple[bool, Mapping[str, Any] | None]:
        changed, row = await self._repository.append_source_message_version_if_changed(
            source_message_id=source_message_id,
            projection=projection,
            version_reason=version_reason,
            observed_at=observed_at,
            telegram_edit_date=telegram_edit_date,
        )
        if changed:
            self._counter.count_table("source_message_versions")
        return changed, row

    async def mark_message_deleted(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
        delete_kind: str,
        deleted_at: Any = None,
    ) -> Mapping[str, Any] | None:
        row = await self._repository.mark_message_deleted(
            platform=platform,
            chat_id=chat_id,
            message_id=message_id,
            delete_kind=delete_kind,
            deleted_at=deleted_at,
        )
        if row is not None:
            self._counter.count_table("source_messages")
        return row

    async def insert_outbox_event(self, event: Any) -> None:
        await self._repository.insert_outbox_event(event)
        self._counter.count_table("event_outbox")


class _DefaultDispatcherContext:
    def __init__(
        self,
        *,
        config: CollectorTelegramConfig,
        counter: CollectorSmokeWriteCounter,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._counter = counter
        self._logger = logger
        self._engine: Any | None = None
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> UpdateDispatcher:
        self._engine = create_async_engine(
            _async_database_url(self._config.database_url),
            future=True,
        )
        session_factory = async_sessionmaker(self._engine, expire_on_commit=False)
        self._session = session_factory()
        repository = CountingCollectorRepository(
            CollectorRepository(
                self._session,
                logger=self._logger.getChild("repository"),
            ),
            self._counter,
        )
        handlers = CollectorUpdateHandlers(
            repository,
            MessageProjectionBuilder(logger=self._logger.getChild("projection")),
            CollectorOutboxBuilder(IdempotencyPolicy()),
            logger=self._logger.getChild("handlers"),
        )
        return UpdateDispatcher(
            repository,
            handlers,
            logger=self._logger.getChild("dispatcher"),
        )

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None


class BoundedCollectorSmokeRunner:
    def __init__(
        self,
        *,
        config: CollectorTelegramConfig,
        tdlib_client: TDLibClientProtocol,
        singleton_guard: SingletonGuardProtocol,
        dispatcher_factory: DispatcherFactory,
        receive_timeout_sec: float = DEFAULT_RECEIVE_TIMEOUT_SEC,
        monotonic: MonotonicClock = time.monotonic,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._tdlib_client = tdlib_client
        self._singleton_guard = singleton_guard
        self._dispatcher_factory = dispatcher_factory
        self._receive_timeout_sec = receive_timeout_sec
        self._monotonic = monotonic
        self._logger = logger or logging.getLogger(__name__)

    async def run(
        self,
        *,
        runtime_env: Mapping[str, str],
        bounds: SmokeBoundsProtocol,
    ) -> BoundedCollectorSmokeResult:
        del runtime_env
        self._validate_bounds(bounds)
        self._config.validate()
        self._config.ensure_runtime_dirs()

        counter = CollectorSmokeWriteCounter()
        side_effects = _runner_side_effects()
        updates_observed = 0
        duration_exhausted = False
        update_cap_exhausted = False
        db_write_cap_exhausted = False
        guard_acquired = False
        client_initialized = False
        deadline = self._monotonic() + bounds.max_duration_sec

        try:
            self._singleton_guard.acquire()
            guard_acquired = True
            side_effects["live_collector_started"] = True
            side_effects["collector_runtime_started"] = True

            await self._tdlib_client.initialize()
            client_initialized = True
            side_effects["tdlib_initialized"] = True

            async with self._dispatcher_factory(counter) as dispatcher:
                while True:
                    now = self._monotonic()
                    if now >= deadline:
                        duration_exhausted = True
                        break
                    if updates_observed >= bounds.max_updates:
                        update_cap_exhausted = True
                        break
                    if counter.total >= bounds.max_db_writes:
                        db_write_cap_exhausted = True
                        break

                    timeout = min(self._receive_timeout_sec, max(deadline - now, 0.0))
                    side_effects["tdlib_receive_called"] = True
                    side_effects["telegram_api_called"] = True
                    payload = await self._tdlib_client.receive(timeout)
                    if payload is None:
                        duration_exhausted = self._monotonic() >= deadline
                        break
                    if not isinstance(payload, dict):
                        continue

                    if await self._handle_authorization_update(payload, side_effects):
                        continue

                    update_type = _payload_type(payload)
                    if update_type is None or not update_type.startswith("update"):
                        continue

                    updates_observed += 1
                    estimated_writes = _estimate_reported_writes(payload)
                    if counter.total + estimated_writes > bounds.max_db_writes:
                        db_write_cap_exhausted = True
                        break

                    await dispatcher.dispatch(payload)

                    if updates_observed >= bounds.max_updates:
                        update_cap_exhausted = True
                        break
                    if counter.total >= bounds.max_db_writes:
                        db_write_cap_exhausted = True
                        break
        except BoundedSmokeManualAuthorizationRequired as exc:
            raise BoundedCollectorSmokePartialFailure(
                _build_result(
                    counter=counter,
                    side_effects=side_effects,
                    updates_observed=updates_observed,
                    duration_exhausted=duration_exhausted,
                    update_cap_exhausted=update_cap_exhausted,
                    db_write_cap_exhausted=db_write_cap_exhausted,
                    status="blocked",
                    failure_class="manual_authorization_required",
                )
            ) from exc
        except Exception as exc:
            raise BoundedCollectorSmokePartialFailure(
                _build_result(
                    counter=counter,
                    side_effects=side_effects,
                    updates_observed=updates_observed,
                    duration_exhausted=duration_exhausted,
                    update_cap_exhausted=update_cap_exhausted,
                    db_write_cap_exhausted=db_write_cap_exhausted,
                    status="failed",
                    failure_class=type(exc).__name__,
                )
            ) from exc
        finally:
            if client_initialized:
                with contextlib.suppress(Exception):
                    await self._tdlib_client.close()
            if guard_acquired:
                self._singleton_guard.release()

        return _build_result(
            counter=counter,
            side_effects=side_effects,
            updates_observed=updates_observed,
            duration_exhausted=duration_exhausted,
            update_cap_exhausted=update_cap_exhausted,
            db_write_cap_exhausted=db_write_cap_exhausted,
        )

    async def _handle_authorization_update(
        self,
        payload: JsonDict,
        side_effects: dict[str, bool],
    ) -> bool:
        if payload.get("@type") != "updateAuthorizationState":
            return False
        authorization_state = payload.get("authorization_state")
        if not isinstance(authorization_state, dict):
            return True
        state_type = _payload_type(authorization_state)
        if state_type == "authorizationStateWaitTdlibParameters":
            await self._send_tdlib_request(
                self._tdlib_client.build_set_tdlib_parameters_request(),
                side_effects,
            )
            return True
        if state_type == "authorizationStateWaitEncryptionKey":
            await self._send_tdlib_request(
                self._tdlib_client.build_check_database_encryption_key_request(),
                side_effects,
            )
            return True
        if state_type in _MANUAL_AUTHORIZATION_STATES:
            self._logger.warning(
                "bounded_collector_smoke_manual_authorization_required",
                extra={
                    "service": "collector-telegram",
                    "event": "bounded_collector_smoke_manual_authorization_required",
                    "authorization_state": state_type,
                },
            )
            raise BoundedSmokeManualAuthorizationRequired(state_type)
        return True

    async def _send_tdlib_request(
        self,
        request: Any,
        side_effects: dict[str, bool],
    ) -> None:
        payload = _unwrap_request_payload(request)
        request_type = _payload_type(payload)
        forbidden_flag = _FORBIDDEN_TDLIB_REQUEST_FLAGS.get(request_type or "")
        if forbidden_flag is not None:
            side_effects[forbidden_flag] = True
            raise BoundedSmokeForbiddenTDLibAction(
                f"forbidden TDLib request during bounded smoke: {request_type}"
            )
        await self._tdlib_client.send(payload)
        side_effects["tdlib_send_called"] = True
        side_effects["telegram_api_called"] = True

    def _validate_bounds(self, bounds: SmokeBoundsProtocol) -> None:
        if not _valid_bound(bounds.max_duration_sec, MAX_SMOKE_DURATION_SEC):
            raise BoundedSmokeRunnerConfigError("max_duration_sec is outside smoke bounds")
        if not _valid_bound(bounds.max_updates, MAX_SMOKE_UPDATES):
            raise BoundedSmokeRunnerConfigError("max_updates is outside smoke bounds")
        if not _valid_bound(bounds.max_db_writes, MAX_SMOKE_DB_WRITES):
            raise BoundedSmokeRunnerConfigError("max_db_writes is outside smoke bounds")


def build_default_bounded_collector_smoke_runner(
    runtime_env: Mapping[str, str],
) -> BoundedCollectorSmokeRunner:
    config = CollectorTelegramConfig.from_env(runtime_env)
    logger = logging.getLogger(__name__)
    transport = TDJsonTransport(library_path=_runtime_env_tdjson_library_path(runtime_env))
    transport.assert_available()
    tdlib_client = TDLibClient(
        config,
        transport=transport,
        logger=logger.getChild("tdlib"),
    )

    def dispatcher_factory(
        counter: CollectorSmokeWriteCounter,
    ) -> _DefaultDispatcherContext:
        return _DefaultDispatcherContext(
            config=config,
            counter=counter,
            logger=logger,
        )

    return BoundedCollectorSmokeRunner(
        config=config,
        tdlib_client=tdlib_client,
        singleton_guard=CollectorSingletonGuard(lock_path=config.singleton_lock_path),
        dispatcher_factory=dispatcher_factory,
        logger=logger,
    )


def _async_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return "postgresql+psycopg://" + database_url.removeprefix("postgresql://")
    return database_url


def _runner_side_effects() -> dict[str, bool]:
    return {
        "database_mutation_performed": False,
        "telegram_raw_updates_written": False,
        "source_messages_written": False,
        "source_message_versions_written": False,
        "event_outbox_written": False,
        "redis_mutation_performed": False,
        "telegram_api_called": False,
        "tdlib_initialized": False,
        "tdlib_send_called": False,
        "tdlib_receive_called": False,
        "tdlib_auth_attempted": False,
        "tdlib_phone_number_submitted": False,
        "tdlib_code_submitted": False,
        "tdlib_password_submitted": False,
        "tdlib_join_called": False,
        "tdlib_history_fetch_called": False,
        "tdlib_public_username_resolve_called": False,
        "tdlib_search_public_chat_called": False,
        "tdlib_send_message_called": False,
        "live_collector_started": False,
        "collector_runtime_started": False,
        "notifier_transport_enabled": False,
        "outbox_relay_started": False,
        "router_normalizer_started": False,
        "alembic_upgrade_run": False,
        "alembic_downgrade_run": False,
        "alembic_stamp_run": False,
        "docker_or_systemd_changed": False,
        "files_mutated_outside_repo": False,
        "telegram_channel_registry_updated": False,
        "telegram_channel_registry_inserted": False,
        "telegram_channel_registry_deleted": False,
    }


def _build_result(
    *,
    counter: CollectorSmokeWriteCounter,
    side_effects: dict[str, bool],
    updates_observed: int,
    duration_exhausted: bool,
    update_cap_exhausted: bool,
    db_write_cap_exhausted: bool,
    status: str = "completed",
    failure_class: str | None = None,
) -> BoundedCollectorSmokeResult:
    if counter.total > 0:
        side_effects["database_mutation_performed"] = True
    for table_name in counter.written_tables:
        side_effect_key = f"{table_name}_written"
        if side_effect_key in side_effects:
            side_effects[side_effect_key] = True

    return BoundedCollectorSmokeResult(
        status=status,
        failure_class=failure_class,
        updates_observed=updates_observed,
        telegram_raw_updates_written=counter.telegram_raw_updates_written,
        source_messages_written=counter.source_messages_written,
        source_message_versions_written=counter.source_message_versions_written,
        event_outbox_written=counter.event_outbox_written,
        duration_exhausted=duration_exhausted,
        update_cap_exhausted=update_cap_exhausted,
        db_write_cap_exhausted=db_write_cap_exhausted,
        written_tables=tuple(sorted(counter.written_tables)),
        side_effects=dict(side_effects),
    )


def _runtime_env_tdjson_library_path(runtime_env: Mapping[str, str]) -> str | None:
    candidate = runtime_env.get("TDJSON_LIBRARY_PATH")
    if not isinstance(candidate, str):
        return None
    stripped = candidate.strip()
    return stripped or None


def _valid_bound(value: Any, upper_bound: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value <= upper_bound


def _payload_type(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("@type")
    return value if isinstance(value, str) and value else None


def _unwrap_request_payload(request: Any) -> JsonDict:
    if isinstance(request, dict):
        return request
    payload = getattr(request, "payload", None)
    if isinstance(payload, dict):
        return payload
    raise BoundedSmokeRunnerConfigError("TDLib request payload is not an object")


def _estimate_reported_writes(update: Mapping[str, Any]) -> int:
    update_type = _payload_type(update)
    if update_type == "updateNewMessage":
        return 4
    if update_type == "updateMessageContent":
        return 4
    if update_type == "updateMessageEdited":
        return 2
    if update_type == "updateDeleteMessages":
        message_ids = update.get("message_ids")
        message_count = len(message_ids) if isinstance(message_ids, list) else 0
        return 1 + (2 * max(message_count, 0))
    if update_type == "updateChatLastMessage":
        return 1
    return 1
