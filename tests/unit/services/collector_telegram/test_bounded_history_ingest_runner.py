from __future__ import annotations

import ast
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from src.services.collector_telegram import bounded_history_ingest_runner as runner_module
from src.services.collector_telegram.bounded_history_ingest_runner import (
    BoundedHistoryIngestError,
    BoundedTelegramCollectorHistoryIngestConfig,
    BoundedTelegramCollectorHistoryIngestRuntimeHandle,
    BoundedTelegramCollectorHistoryIngestState,
    _TDLibBoundedHistoryClient,
    run_bounded_telegram_collector_history_ingest,
)
from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.models import CollectorEnvironment, CollectorMode, TrackedChat
from src.services.collector_telegram.tdlib_client import TDLibClient
from src.services.outbox_relay.models import OutboxEventRow


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/collector_telegram/bounded_history_ingest_runner.py"
TOOL_PATH = ROOT / "tools/bounded_telegram_collector_history_ingest_runner.py"
NEW_TOOL_PATH = ROOT / "tools/bounded_collector_history_ingest_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_db_password@127.0.0.1/db"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_ID = 444555666
RAW_MESSAGE_TEXT = "sentinel bounded history ingest raw message text"
RAW_SECRET = "sentinel_telegram_api_hash_history_ingest"
EXCEPTION_DETAIL = "private exception detail with sentinel bounded history ingest raw message text"
CLOSE_EXCEPTION_DETAIL = "private close failure detail with sentinel bounded history ingest raw message text"
_DEFAULT_SOURCE_MESSAGE_ID = object()
_DEFAULT_TRACKED_CHAT = object()


class FakeTransaction:
    def __init__(self, repository: "FakeRepository") -> None:
        self.repository = repository
        self.snapshot: Any = None
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> "FakeRepository":
        self.snapshot = self.repository.snapshot()
        self.repository.transactions.append(self)
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            self.repository.restore(self.snapshot)
            self.rolled_back = True
            return None
        self.committed = True
        return None


class FakeRepository:
    def __init__(
        self,
        *,
        tracked: TrackedChat | None | object = _DEFAULT_TRACKED_CHAT,
        fail_upsert: Exception | None = None,
        return_uuid_source_message_id: bool = False,
        upsert_source_message_id: object = _DEFAULT_SOURCE_MESSAGE_ID,
        omit_upsert_source_message_id: bool = False,
        extra_targets: list[TrackedChat] | None = None,
    ) -> None:
        if tracked is _DEFAULT_TRACKED_CHAT:
            self.tracked = TrackedChat(
                registry_id="11111111-1111-1111-1111-111111111111",
                chat_id=RAW_CHAT_ID,
                desired_state="active",
                access_state="joined",
                source_kind="public_username",
                source_value="trendingrepo",
                priority_weight=100,
            )
        else:
            self.tracked = tracked
        self.fail_upsert = fail_upsert
        self.extra_targets = list(extra_targets or [])
        self.return_uuid_source_message_id = return_uuid_source_message_id
        self.upsert_source_message_id = upsert_source_message_id
        self.omit_upsert_source_message_id = omit_upsert_source_message_id
        self.messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[Any] = []
        self.dedupe_keys: set[str] = set()
        self.transactions: list[FakeTransaction] = []
        self.registry_lookups: list[str] = []
        self.mark_published_calls: list[UUID] = []
        self.cursor_updates: list[dict[str, Any]] = []
        self.upsert_calls = 0

    def snapshot(self) -> Any:
        return (
            deepcopy(self.messages),
            deepcopy(self.versions),
            list(self.outbox),
            set(self.dedupe_keys),
            self.upsert_calls,
        )

    def restore(self, snapshot: Any) -> None:
        self.messages, self.versions, self.outbox, self.dedupe_keys, self.upsert_calls = snapshot

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    async def find_public_username_registry_targets(self, normalized_source_value: str) -> list[dict[str, Any]]:
        self.registry_lookups.append(normalized_source_value)
        rows = []
        for target in [self.tracked, *self.extra_targets]:
            if target is None:
                continue
            source_value = target.source_value.strip().lstrip("@").lower()
            if target.source_kind != "public_username" or source_value != normalized_source_value:
                continue
            rows.append(
                {
                    "registry_id": target.registry_id,
                    "chat_id": target.chat_id,
                    "desired_state": target.desired_state,
                    "access_state": target.access_state,
                    "source_kind": target.source_kind,
                    "source_value": target.source_value,
                    "username_snapshot": f"@{target.source_value}",
                    "priority_weight": target.priority_weight,
                    "last_seen_message_id": target.last_seen_message_id,
                    "last_seen_message_date": target.last_seen_message_date,
                }
            )
        return rows

    async def get_active_joined_tracked_chat_by_registry_id(self, registry_id: str) -> TrackedChat | None:
        self.registry_lookups.append(registry_id)
        if self.tracked is None:
            return None
        if self.tracked.registry_id != registry_id:
            return None
        if self.tracked.desired_state != "active" or self.tracked.access_state != "joined":
            return None
        return self.tracked

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int) -> dict[str, Any] | None:
        assert platform == "telegram"
        return self.messages.get((chat_id, message_id))

    async def get_latest_version(self, source_message_id: str) -> dict[str, Any] | None:
        versions = self.versions.get(source_message_id, [])
        return versions[-1] if versions else None

    async def upsert_source_message(self, projection: Any, *, platform: str = "telegram") -> dict[str, Any]:
        if self.fail_upsert is not None:
            raise self.fail_upsert
        assert platform == "telegram"
        self.upsert_calls += 1
        key = (projection.chat_id, projection.message_id)
        row = self.messages.get(key)
        if row is None:
            source_message_uuid = uuid5(NAMESPACE_URL, f"telegram:{projection.chat_id}:{projection.message_id}")
            source_message_id = str(source_message_uuid)
            row_source_message_id = self.upsert_source_message_id
            if row_source_message_id is _DEFAULT_SOURCE_MESSAGE_ID:
                row_source_message_id = source_message_uuid if self.return_uuid_source_message_id else source_message_id
            row = {
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "logical_post_key": projection.logical_post_key,
                "current_version_no": 0,
            }
            if not self.omit_upsert_source_message_id:
                row["source_message_id"] = row_source_message_id
            self.messages[key] = row
            self.versions[source_message_id] = []
        row["logical_post_key"] = projection.logical_post_key
        return row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: Any = None,
        telegram_edit_date: Any = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        del observed_at, telegram_edit_date
        versions = self.versions.setdefault(source_message_id, [])
        if versions and versions[-1]["content_hash"] == projection.content_hash:
            return False, None
        row = {
            "source_message_id": source_message_id,
            "version_no": len(versions) + 1,
            "version_reason": version_reason,
            "content_hash": projection.content_hash,
        }
        versions.append(row)
        return True, row

    async def insert_outbox_event(self, event: Any) -> bool:
        if event.dedupe_key in self.dedupe_keys:
            return False
        self.dedupe_keys.add(event.dedupe_key)
        self.outbox.append(event)
        return True

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str) -> OutboxEventRow | None:
        for event in self.outbox:
            if event.dedupe_key == dedupe_key:
                return OutboxEventRow(
                    event_id=uuid5(NAMESPACE_URL, event.dedupe_key),
                    event_type=event.event_type,
                    aggregate_type=event.aggregate_type,
                    aggregate_id=UUID(str(event.aggregate_id)),
                    dedupe_key=event.dedupe_key,
                    payload_json=dict(event.payload_json),
                    status="pending",
                    fail_count=0,
                    created_at=datetime.now(timezone.utc),
                )
        return None

    async def mark_outbox_published(self, *, event_id: UUID, published_at: Any = None) -> bool:
        del published_at
        self.mark_published_calls.append(event_id)
        return True

    async def update_channel_sync_cursor(self, **kwargs: Any) -> None:
        self.cursor_updates.append(kwargs)


class FakeHistoryClient:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = messages
        self.calls: list[dict[str, int]] = []
        self.closed = False

    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int):
        self.calls.append({"chat_id": chat_id, "limit": limit})
        return tuple(deepcopy(self.messages))

    async def close(self) -> None:
        self.closed = True


class FakeRedisPublisher:
    def __init__(self, *, failure: Exception | None = None, message_id: str = "1234567890-0") -> None:
        self.failure = failure
        self.message_id = message_id
        self.publish_calls: list[tuple[Any, Any]] = []

    async def publish(self, route: Any, message: Any) -> str:
        self.publish_calls.append((route, message))
        if self.failure is not None:
            raise self.failure
        return self.message_id


class ObservingRedisPublisher(FakeRedisPublisher):
    def __init__(self, repository: ImplicitTransactionRepository, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.repository = repository
        self.committed_counts_at_publish: list[tuple[int, int, int]] = []
        self.pending_counts_at_publish: list[tuple[int, int, int]] = []

    async def publish(self, route: Any, message: Any) -> str:
        self.committed_counts_at_publish.append(self.repository.committed_counts())
        self.pending_counts_at_publish.append(self.repository.pending_counts())
        return await super().publish(route, message)


class FakeTDLibTransport:
    def __init__(
        self,
        *,
        auth_payloads: list[dict[str, Any]] | None = None,
        history_messages: list[dict[str, Any]] | None = None,
        log_suppression_attempted: bool = True,
        log_suppression_confirmed: bool = True,
    ) -> None:
        self.auth_payloads = list(auth_payloads or [])
        self.history_messages = list(history_messages or [_message()])
        self.log_suppression_attempted = log_suppression_attempted
        self.log_suppression_confirmed = log_suppression_confirmed
        self.initialized = False
        self.closed = False
        self.sent_requests: list[dict[str, Any]] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def send(self, request: dict[str, Any]) -> None:
        self.sent_requests.append(dict(request))
        if request.get("@type") == "getChatHistory":
            extra = request.get("@extra")
            self.auth_payloads.append(
                {
                    "@type": "messages",
                    "@extra": extra,
                    "messages": deepcopy(self.history_messages),
                }
            )

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        del timeout
        if self.auth_payloads:
            return self.auth_payloads.pop(0)
        return None

    async def close(self) -> None:
        self.closed = True

    def native_log_suppression_attempted(self) -> bool:
        return self.log_suppression_attempted

    def native_log_suppression_confirmed(self) -> bool:
        return self.log_suppression_confirmed


class FakeRuntimeBuilder:
    def __init__(
        self,
        repository: FakeRepository,
        history_client: FakeHistoryClient,
        *,
        close_error: Exception | None = None,
        commit_error: Exception | None = None,
        redis_publisher: FakeRedisPublisher | None = None,
    ) -> None:
        self.repository = repository
        self.history_client = history_client
        self.close_error = close_error
        self.commit_error = commit_error
        self.redis_publisher = redis_publisher
        self.calls = 0
        self.commit_calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger
        self.calls += 1

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            await self.history_client.close()
            if self.close_error is not None:
                raise self.close_error

        async def commit() -> None:
            self.commit_calls += 1
            if self.commit_error is not None:
                raise self.commit_error

        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=self.repository,
            history_client=self.history_client,
            redis_publisher=self.redis_publisher,
            close=close,
            commit=commit,
        )


class FakeDefaultSession:
    def __init__(self, *, commit_error: Exception | None = None) -> None:
        self.commit_error = commit_error
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_error is not None:
            raise self.commit_error

    async def rollback(self) -> None:
        self.rollback_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


class FakeDefaultEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1


class FakeDefaultTDLib:
    def __init__(self, runtime_config: CollectorTelegramConfig, *, transport: object, logger: Any) -> None:
        self.runtime_config = runtime_config
        self.transport = transport
        self.logger = logger


class FakeDefaultHistoryClient:
    def __init__(self, tdlib: FakeDefaultTDLib, *, state: BoundedTelegramCollectorHistoryIngestState) -> None:
        self.tdlib = tdlib
        self.state = state
        self.close_calls = 0

    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int):
        del chat_id, limit
        raise AssertionError("default runtime close tests do not fetch history")

    async def close(self) -> None:
        self.close_calls += 1


class ImplicitTransaction:
    def __init__(self, repository: "ImplicitTransactionRepository") -> None:
        self.repository = repository

    async def __aenter__(self) -> "ImplicitTransactionRepository":
        self.repository.ensure_pending()
        self.repository.implicit_transaction_open = True
        self.repository.transaction_enters += 1
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.repository.transaction_exits.append(exc_type)
        if self.repository.implicit_transaction_open:
            return None
        if exc_type is None:
            self.repository.finalize(commit=True)
        else:
            self.repository.finalize(commit=False)
        return None


class ImplicitTransactionRepository:
    def __init__(self, *, tracked: TrackedChat, fail_after_upsert: Exception | None = None) -> None:
        self.tracked = tracked
        self.fail_after_upsert = fail_after_upsert
        self.implicit_transaction_open = False
        self.committed_messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.committed_versions: dict[str, list[dict[str, Any]]] = {}
        self.committed_outbox: list[Any] = []
        self.pending_messages: dict[tuple[int, int], dict[str, Any]] | None = None
        self.pending_versions: dict[str, list[dict[str, Any]]] | None = None
        self.pending_outbox: list[Any] | None = None
        self.registry_lookups: list[str] = []
        self.mark_published_calls: list[UUID] = []
        self.cursor_updates: list[dict[str, Any]] = []
        self.transaction_enters = 0
        self.transaction_exits: list[Any] = []
        self.upsert_calls = 0

    def ensure_pending(self) -> None:
        if self.pending_messages is None:
            self.pending_messages = deepcopy(self.committed_messages)
            self.pending_versions = deepcopy(self.committed_versions)
            self.pending_outbox = list(self.committed_outbox)

    def finalize(self, *, commit: bool) -> None:
        if commit and self.pending_messages is not None:
            self.committed_messages = deepcopy(self.pending_messages)
            self.committed_versions = deepcopy(self.pending_versions or {})
            self.committed_outbox = list(self.pending_outbox or [])
        self.pending_messages = None
        self.pending_versions = None
        self.pending_outbox = None
        self.implicit_transaction_open = False

    def pending_counts(self) -> tuple[int, int, int]:
        return (
            len(self.pending_messages or {}),
            sum(len(versions) for versions in (self.pending_versions or {}).values()),
            len(self.pending_outbox or []),
        )

    def committed_counts(self) -> tuple[int, int, int]:
        return (
            len(self.committed_messages),
            sum(len(versions) for versions in self.committed_versions.values()),
            len(self.committed_outbox),
        )

    def transaction(self) -> ImplicitTransaction:
        return ImplicitTransaction(self)

    async def find_public_username_registry_targets(self, normalized_source_value: str) -> list[dict[str, Any]]:
        self.registry_lookups.append(normalized_source_value)
        self.implicit_transaction_open = True
        self.ensure_pending()
        source_value = self.tracked.source_value.strip().lstrip("@").lower()
        if self.tracked.source_kind != "public_username" or source_value != normalized_source_value:
            return []
        return [
            {
                "registry_id": self.tracked.registry_id,
                "chat_id": self.tracked.chat_id,
                "desired_state": self.tracked.desired_state,
                "access_state": self.tracked.access_state,
                "source_kind": self.tracked.source_kind,
                "source_value": self.tracked.source_value,
                "username_snapshot": f"@{self.tracked.source_value}",
                "priority_weight": self.tracked.priority_weight,
                "last_seen_message_id": self.tracked.last_seen_message_id,
                "last_seen_message_date": self.tracked.last_seen_message_date,
            }
        ]

    async def get_active_joined_tracked_chat_by_registry_id(self, registry_id: str) -> TrackedChat | None:
        self.registry_lookups.append(registry_id)
        self.implicit_transaction_open = True
        self.ensure_pending()
        if self.tracked.registry_id != registry_id:
            return None
        if self.tracked.desired_state != "active" or self.tracked.access_state != "joined":
            return None
        return self.tracked

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int) -> dict[str, Any] | None:
        assert platform == "telegram"
        self.ensure_pending()
        return (self.pending_messages or {}).get((chat_id, message_id))

    async def get_latest_version(self, source_message_id: str) -> dict[str, Any] | None:
        self.ensure_pending()
        versions = (self.pending_versions or {}).get(source_message_id, [])
        return versions[-1] if versions else None

    async def upsert_source_message(self, projection: Any, *, platform: str = "telegram") -> dict[str, Any]:
        assert platform == "telegram"
        self.ensure_pending()
        self.upsert_calls += 1
        source_message_uuid = uuid5(NAMESPACE_URL, f"telegram:{projection.chat_id}:{projection.message_id}")
        source_message_id = str(source_message_uuid)
        row = {
            "source_message_id": source_message_id,
            "chat_id": projection.chat_id,
            "message_id": projection.message_id,
            "logical_post_key": projection.logical_post_key,
            "current_version_no": 0,
        }
        assert self.pending_messages is not None
        assert self.pending_versions is not None
        self.pending_messages[(projection.chat_id, projection.message_id)] = row
        self.pending_versions.setdefault(source_message_id, [])
        return row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: Any = None,
        telegram_edit_date: Any = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        del observed_at, telegram_edit_date
        if self.fail_after_upsert is not None:
            raise self.fail_after_upsert
        self.ensure_pending()
        assert self.pending_versions is not None
        versions = self.pending_versions.setdefault(source_message_id, [])
        row = {
            "source_message_id": source_message_id,
            "version_no": len(versions) + 1,
            "version_reason": version_reason,
            "content_hash": projection.content_hash,
        }
        versions.append(row)
        return True, row

    async def insert_outbox_event(self, event: Any) -> bool:
        self.ensure_pending()
        assert self.pending_outbox is not None
        self.pending_outbox.append(event)
        return True

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str) -> OutboxEventRow | None:
        self.ensure_pending()
        for event in self.pending_outbox or []:
            if event.dedupe_key == dedupe_key:
                return OutboxEventRow(
                    event_id=uuid5(NAMESPACE_URL, event.dedupe_key),
                    event_type=event.event_type,
                    aggregate_type=event.aggregate_type,
                    aggregate_id=UUID(str(event.aggregate_id)),
                    dedupe_key=event.dedupe_key,
                    payload_json=dict(event.payload_json),
                    status="pending",
                    fail_count=0,
                    created_at=datetime.now(timezone.utc),
                )
        return None

    async def mark_outbox_published(self, *, event_id: UUID, published_at: Any = None) -> bool:
        del published_at
        self.mark_published_calls.append(event_id)
        return True

    async def update_channel_sync_cursor(self, **kwargs: Any) -> None:
        self.cursor_updates.append(kwargs)


class ImplicitTransactionRuntimeBuilder:
    def __init__(
        self,
        repository: ImplicitTransactionRepository,
        history_client: FakeHistoryClient,
        *,
        commit_errors: list[Exception | None] | None = None,
    ) -> None:
        self.repository = repository
        self.history_client = history_client
        self.commit_errors = list(commit_errors or [])
        self.commit_calls = 0
        self.pre_commit_committed_counts: list[tuple[int, int, int]] = []
        self.pre_commit_pending_counts: list[tuple[int, int, int]] = []
        self.close_commits: list[bool] = []
        self.pre_close_committed_counts: list[tuple[int, int, int]] = []
        self.pre_close_pending_counts: list[tuple[int, int, int]] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            self.pre_close_committed_counts.append(self.repository.committed_counts())
            self.pre_close_pending_counts.append(self.repository.pending_counts())
            if self.repository.pending_messages is not None:
                self.repository.finalize(commit=commit)
            await self.history_client.close()

        async def commit() -> None:
            self.commit_calls += 1
            self.pre_commit_committed_counts.append(self.repository.committed_counts())
            self.pre_commit_pending_counts.append(self.repository.pending_counts())
            if self.commit_errors:
                error = self.commit_errors.pop(0)
                if error is not None:
                    raise error
            self.repository.finalize(commit=True)

        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=self.repository,
            history_client=self.history_client,
            close=close,
            commit=commit,
        )


class Loader:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> CollectorTelegramConfig:
        self.calls += 1
        return _runtime_config()


def _runtime_config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env=CollectorEnvironment.TEST,
        database_url=DB_URL,
        redis_url=None,
        collector_mode=CollectorMode.REPLAY,
        telegram_api_id=12345,
        telegram_api_hash=RAW_SECRET,
        telegram_phone_number="+15555550123",
        telegram_2fa_password=None,
        tdlib_state_dir="/tmp/sentinel-history-ingest-tdlib-state",
        tdlib_files_dir="/tmp/sentinel-history-ingest-tdlib-files",
        tdlib_db_encryption_key="sentinel-tdlib-encryption-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=3,
        warm_backfill_limit=1,
        history_page_limit=3,
    )


def _message(*, text: str = RAW_MESSAGE_TEXT, message_id: int = RAW_MESSAGE_ID) -> dict[str, Any]:
    return {
        "@type": "message",
        "chat_id": RAW_CHAT_ID,
        "id": message_id,
        "date": 1713550000,
        "is_channel_post": True,
        "content": {
            "@type": "messageText",
            "text": {"text": text, "entities": []},
        },
        "raw_nested_secret": "sentinel raw message json payload value",
    }


def _approved_config(**overrides: Any) -> BoundedTelegramCollectorHistoryIngestConfig:
    values = {
        "mode": "execute",
        "source_kind": "public_username",
        "source_value": "trendingrepo",
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "allow_telegram_read": True,
        "allow_database_write": True,
        "allow_source_message_write": True,
        "allow_source_version_write": True,
        "allow_source_outbox_write": True,
        "history_limit": 1,
    }
    values.update(overrides)
    return BoundedTelegramCollectorHistoryIngestConfig(**values)


def _tracked_chat(
    registry_id: str = "11111111-1111-1111-1111-111111111111",
    *,
    desired_state: str = "active",
    access_state: str = "joined",
    chat_id: int | None = RAW_CHAT_ID,
    source_value: str = "trendingrepo",
) -> TrackedChat:
    return TrackedChat(
        registry_id=registry_id,
        chat_id=chat_id,
        desired_state=desired_state,
        access_state=access_state,
        source_kind="public_username",
        source_value=source_value,
        priority_weight=100,
    )


def _install_default_runtime_fakes(monkeypatch: pytest.MonkeyPatch, session: FakeDefaultSession):
    engine = FakeDefaultEngine()
    history_clients: list[FakeDefaultHistoryClient] = []

    def fake_async_sessionmaker(received_engine: FakeDefaultEngine, *, expire_on_commit: bool):
        assert received_engine is engine
        assert expire_on_commit is False
        return lambda: session

    def fake_history_client(tdlib: FakeDefaultTDLib, *, state: BoundedTelegramCollectorHistoryIngestState):
        client = FakeDefaultHistoryClient(tdlib, state=state)
        history_clients.append(client)
        return client

    monkeypatch.setattr(runner_module, "create_async_engine", lambda database_url: engine)
    monkeypatch.setattr(runner_module, "async_sessionmaker", fake_async_sessionmaker)
    monkeypatch.setattr(runner_module, "TDJsonTransport", lambda: object())
    monkeypatch.setattr(runner_module, "TDLibClient", FakeDefaultTDLib)
    monkeypatch.setattr(runner_module, "_TDLibBoundedHistoryClient", fake_history_client)
    return engine, history_clients


async def _run(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    repository: FakeRepository | None = None,
    history: FakeHistoryClient | None = None,
    loader: Loader | None = None,
    close_error: Exception | None = None,
    commit_error: Exception | None = None,
    redis_publisher: FakeRedisPublisher | None = None,
):
    fake_repository = repository or FakeRepository()
    fake_history = history or FakeHistoryClient([_message()])
    fake_loader = loader or Loader()
    builder = FakeRuntimeBuilder(
        fake_repository,
        fake_history,
        close_error=close_error,
        commit_error=commit_error,
        redis_publisher=redis_publisher,
    )
    result = await run_bounded_telegram_collector_history_ingest(
        config,
        runtime_config_loader=fake_loader,
        runtime_builder=builder,
    )
    return result, fake_loader, builder, fake_repository, fake_history


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("commit", "expected_commit_calls", "expected_rollback_calls"),
    [
        (True, 1, 0),
        (False, 0, 1),
    ],
)
async def test_default_runtime_close_finalizes_session_from_commit_flag(
    monkeypatch: pytest.MonkeyPatch,
    commit: bool,
    expected_commit_calls: int,
    expected_rollback_calls: int,
) -> None:
    session = FakeDefaultSession()
    engine, history_clients = _install_default_runtime_fakes(monkeypatch, session)
    handle = await runner_module.build_default_bounded_history_ingest_runtime(
        _runtime_config(),
        BoundedTelegramCollectorHistoryIngestState(),
        runner_module.logging.getLogger(__name__),
    )

    await handle.close(commit)

    assert session.commit_calls == expected_commit_calls
    assert session.rollback_calls == expected_rollback_calls
    assert session.close_calls == 1
    assert engine.dispose_calls == 1
    assert len(history_clients) == 1
    assert history_clients[0].close_calls == 1


@pytest.mark.asyncio
async def test_default_runtime_close_commit_failure_is_visible_and_still_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeDefaultSession(commit_error=RuntimeError("sentinel_commit_failed"))
    engine, history_clients = _install_default_runtime_fakes(monkeypatch, session)
    handle = await runner_module.build_default_bounded_history_ingest_runtime(
        _runtime_config(),
        BoundedTelegramCollectorHistoryIngestState(),
        runner_module.logging.getLogger(__name__),
    )

    with pytest.raises(RuntimeError, match="sentinel_commit_failed"):
        await handle.close(True)

    assert session.commit_calls == 1
    assert session.rollback_calls == 0
    assert session.close_calls == 1
    assert engine.dispose_calls == 1
    assert len(history_clients) == 1
    assert history_clients[0].close_calls == 1


@pytest.mark.asyncio
async def test_successful_run_with_implicit_transaction_persists_on_close_commit_true() -> None:
    tracked = _tracked_chat()
    repository = ImplicitTransactionRepository(tracked=tracked)
    history = FakeHistoryClient([_message()])
    builder = ImplicitTransactionRuntimeBuilder(repository, history)

    result = await run_bounded_telegram_collector_history_ingest(
        _approved_config(),
        runtime_config_loader=Loader(),
        runtime_builder=builder,
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["source_messages_created_count"] == 1
    assert report["source_versions_appended_count"] == 1
    assert report["outbox_events_inserted_count"] == 1
    assert repository.registry_lookups == ["trendingrepo"]
    assert repository.transaction_enters == 2
    assert builder.commit_calls == 1
    assert builder.pre_commit_committed_counts == [(0, 0, 0)]
    assert builder.pre_commit_pending_counts == [(1, 1, 1)]
    assert builder.close_commits == [True]
    assert builder.pre_close_committed_counts == [(1, 1, 1)]
    assert builder.pre_close_pending_counts == [(0, 0, 0)]
    assert repository.committed_counts() == (1, 1, 1)
    assert repository.pending_counts() == (0, 0, 0)
    assert len(repository.committed_outbox) == 1
    assert repository.committed_outbox[0].event_type == "source_message.created.v1"


@pytest.mark.asyncio
async def test_failure_after_write_with_implicit_transaction_rolls_back_pending_state() -> None:
    tracked = _tracked_chat()
    repository = ImplicitTransactionRepository(
        tracked=tracked,
        fail_after_upsert=RuntimeError(EXCEPTION_DETAIL),
    )
    history = FakeHistoryClient([_message()])
    builder = ImplicitTransactionRuntimeBuilder(repository, history)

    result = await run_bounded_telegram_collector_history_ingest(
        _approved_config(),
        runtime_config_loader=Loader(),
        runtime_builder=builder,
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "unexpected_failure"
    assert report["database_write_attempted"] is True
    assert report["outbox_write_attempted"] is False
    assert repository.transaction_enters == 1
    assert builder.commit_calls == 0
    assert builder.close_commits == [False]
    assert builder.pre_close_committed_counts == [(0, 0, 0)]
    assert builder.pre_close_pending_counts == [(1, 0, 0)]
    assert repository.committed_counts() == (0, 0, 0)
    assert repository.pending_counts() == (0, 0, 0)
    assert repository.committed_outbox == []


@pytest.mark.asyncio
async def test_successful_processing_close_commit_failure_returns_sanitized_blocked_result() -> None:
    close_error = RuntimeError(CLOSE_EXCEPTION_DETAIL)
    result, _loader, builder, _repository, history = await _run(
        _approved_config(),
        close_error=close_error,
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is False
    assert report["status"] == "failed"
    assert report["error_code"] == "runtime_commit_failed"
    assert report["error_class"] == "RuntimeError"
    assert builder.close_commits == [True]
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    for raw_value in (
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        RAW_MESSAGE_TEXT,
        DB_URL,
        RAW_SECRET,
        CLOSE_EXCEPTION_DETAIL,
    ):
        assert raw_value not in rendered


@pytest.mark.asyncio
async def test_failed_processing_close_rollback_failure_returns_sanitized_blocked_result() -> None:
    repository = FakeRepository(fail_upsert=RuntimeError(EXCEPTION_DETAIL))
    result, _loader, builder, _repository, history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
        close_error=ValueError(CLOSE_EXCEPTION_DETAIL),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is False
    assert report["status"] == "failed"
    assert report["error_code"] == "runtime_rollback_failed"
    assert report["error_class"] == "ValueError"
    assert report["database_write_attempted"] is True
    assert report["outbox_write_attempted"] is False
    assert builder.close_commits == [False]
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    for raw_value in (
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        RAW_MESSAGE_TEXT,
        DB_URL,
        RAW_SECRET,
        EXCEPTION_DETAIL,
        CLOSE_EXCEPTION_DETAIL,
    ):
        assert raw_value not in rendered


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_or_authority() -> None:
    def forbidden_loader() -> CollectorTelegramConfig:
        raise AssertionError("runtime config must not be loaded")

    result = await run_bounded_telegram_collector_history_ingest(
        BoundedTelegramCollectorHistoryIngestConfig(),
        runtime_config_loader=forbidden_loader,
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["runtime_config_attempted"] is False
    assert report["runtime_builder_attempted"] is False
    assert report["telegram_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["outbox_write_attempted"] is False
    assert report["side_effects"]["db_write"] is False
    assert report["side_effects"]["telegram_read_called"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "error_code", "loader_calls", "builder_calls"),
    [
        ({"allow_runtime_config": False}, "runtime_config_not_allowed", 0, 0),
        ({"allow_telegram_read": False}, "telegram_read_not_allowed", 1, 0),
        ({"allow_database_write": False}, "database_write_not_allowed", 1, 0),
        ({"allow_source_outbox_write": False}, "source_outbox_write_not_allowed", 1, 0),
    ],
)
async def test_missing_each_gate_blocks_before_next_authority(
    overrides: dict[str, Any],
    error_code: str,
    loader_calls: int,
    builder_calls: int,
) -> None:
    result, loader, builder, _repository, history = await _run(_approved_config(**overrides))
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == error_code
    assert loader.calls == loader_calls
    assert builder.calls == builder_calls
    assert history.calls == []
    assert report["telegram_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["outbox_write_attempted"] is False


@pytest.mark.asyncio
async def test_max_messages_hard_cap_blocks_before_runtime_config() -> None:
    result, loader, builder, _repository, history = await _run(_approved_config(history_limit=31))
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "history_limit_out_of_bounds"
    assert loader.calls == 0
    assert builder.calls == 0
    assert history.calls == []


@pytest.mark.asyncio
async def test_registry_target_requires_active_joined_single_row_before_read() -> None:
    repository = FakeRepository(tracked=None)
    history = FakeHistoryClient([_message()])
    result, _loader, builder, repository, history = await _run(
        _approved_config(),
        repository=repository,
        history=history,
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "registry_target_missing"
    assert report["registry_lookup_attempted"] is True
    assert builder.calls == 1
    assert repository.registry_lookups == ["trendingrepo"]
    assert history.calls == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tracked", "error_code"),
    [
        (_tracked_chat(access_state="unresolved"), "registry_target_not_joined"),
        (_tracked_chat(chat_id=None), "registry_target_chat_id_missing"),
        (_tracked_chat(desired_state="paused"), "registry_target_not_active"),
    ],
)
async def test_exact_public_username_target_rejects_unready_registry_rows(
    tracked: TrackedChat,
    error_code: str,
) -> None:
    result, _loader, builder, repository, history = await _run(
        _approved_config(),
        repository=FakeRepository(tracked=tracked),
        history=FakeHistoryClient([_message()]),
    )

    assert result.ok is False
    assert result.error_code == error_code
    assert repository.registry_lookups == ["trendingrepo"]
    assert history.calls == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
async def test_exact_public_username_target_rejects_multiple_rows_before_read() -> None:
    result, _loader, builder, repository, history = await _run(
        _approved_config(),
        repository=FakeRepository(
            tracked=_tracked_chat("11111111-1111-1111-1111-111111111111"),
            extra_targets=[_tracked_chat("22222222-2222-2222-2222-222222222222")],
        ),
        history=FakeHistoryClient([_message()]),
    )

    assert result.ok is False
    assert result.error_code == "registry_target_multiple"
    assert repository.registry_lookups == ["trendingrepo"]
    assert history.calls == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
async def test_exact_public_username_target_accepts_at_prefixed_source_value_and_suffix_gate() -> None:
    result, _loader, builder, repository, history = await _run(
        _approved_config(source_value="@trendingrepo", registry_id_suffix="11111111"),
        history=FakeHistoryClient([_message()]),
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["source_value_surface"] == "trendingrepo"
    assert report["target_registry_id_suffix"] == "11111111"
    assert report["target_joined"] is True
    assert report["target_chat_id_present"] is True
    assert repository.registry_lookups == ["trendingrepo"]
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    assert builder.close_commits == [True]


@pytest.mark.asyncio
async def test_exact_public_username_target_rejects_registry_suffix_mismatch() -> None:
    result, _loader, builder, _repository, history = await _run(
        _approved_config(registry_id_suffix="ffffffff"),
        history=FakeHistoryClient([_message()]),
    )

    assert result.ok is False
    assert result.error_code == "registry_id_suffix_mismatch"
    assert history.calls == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
async def test_preview_without_telegram_read_gate_resolves_target_without_fetch_or_writes() -> None:
    result, _loader, builder, repository, history = await _run(
        _approved_config(
            mode="preview",
            allow_telegram_read=False,
            allow_database_write=False,
            allow_source_message_write=False,
            allow_source_version_write=False,
            allow_source_outbox_write=False,
        ),
        history=FakeHistoryClient([_message()]),
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["status"] == "preview_completed"
    assert report["target_joined"] is True
    assert report["target_chat_id_present"] is True
    assert report["telegram_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert repository.messages == {}
    assert repository.outbox == []
    assert history.calls == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
async def test_preview_with_telegram_read_reports_would_counts_and_redacts_body() -> None:
    result, _loader, builder, repository, history = await _run(
        _approved_config(
            mode="preview",
            allow_database_write=False,
            allow_source_message_write=False,
            allow_source_version_write=False,
            allow_source_outbox_write=False,
        ),
        history=FakeHistoryClient([_message()]),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is True
    assert report["messages_seen"] == 1
    assert report["would_insert_source_messages_count"] == 1
    assert report["would_append_source_versions_count"] == 1
    assert report["would_insert_outbox_events_count"] == 1
    assert report["would_skip_same_hash_count"] == 0
    assert report["database_write_attempted"] is False
    assert repository.messages == {}
    assert repository.outbox == []
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    assert builder.close_commits == [False]
    assert str(RAW_CHAT_ID) not in rendered
    assert RAW_MESSAGE_TEXT not in rendered


@pytest.mark.asyncio
async def test_one_fake_message_flows_through_projection_repository_and_outbox() -> None:
    result, _loader, builder, repository, history = await _run(_approved_config())
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is True
    assert report["messages_requested"] == 1
    assert report["messages_seen"] == 1
    assert report["source_messages_created_count"] == 1
    assert report["source_versions_appended_count"] == 1
    assert report["outbox_events_inserted_count"] == 1
    assert report["idempotent_noop_count"] == 0
    assert report["database_write_attempted"] is True
    assert report["outbox_write_attempted"] is True
    assert report["side_effects"]["db_write"] is True
    assert report["side_effects"]["redis_mutation"] is False
    assert report["side_effects"]["telegram_send_called"] is False
    assert report["side_effects"]["telegram_edit_called"] is False
    assert report["side_effects"]["openai_called"] is False
    assert report["side_effects"]["github_called"] is False
    assert report["side_effects"]["x_called"] is False
    assert report["side_effects"]["web_called"] is False
    assert report["side_effects"]["notification_table_write"] is False
    assert report["side_effects"]["worker_loop_started"] is False
    assert report["side_effects"]["systemd_called"] is False
    assert report["side_effects"]["docker_called"] is False
    assert report["side_effects"]["alembic_called"] is False
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    assert len(repository.messages) == 1
    assert len(repository.outbox) == 1
    assert repository.outbox[0].event_type == "source_message.created.v1"
    assert builder.close_commits == [True]
    assert str(RAW_CHAT_ID) not in rendered
    assert RAW_MESSAGE_TEXT not in rendered
    assert DB_URL not in rendered
    assert RAW_SECRET not in rendered


@pytest.mark.asyncio
async def test_new_source_message_accepts_uuid_source_message_id_from_upsert_row() -> None:
    repository = FakeRepository(return_uuid_source_message_id=True)
    result, _loader, builder, repository, history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    report = result.to_sanitized_dict()

    row = repository.messages[(RAW_CHAT_ID, RAW_MESSAGE_ID)]
    source_message_id = row["source_message_id"]

    assert result.ok is True
    assert report["source_messages_created_count"] == 1
    assert report["source_versions_appended_count"] == 1
    assert report["outbox_events_inserted_count"] == 1
    assert isinstance(source_message_id, UUID)
    assert repository.outbox[0].aggregate_id == str(source_message_id)
    assert repository.outbox[0].payload_json["source_message_id"] == str(source_message_id)
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    assert builder.close_commits == [True]


@pytest.mark.asyncio
async def test_existing_source_message_accepts_uuid_source_message_id_from_existing_row() -> None:
    repository = FakeRepository(return_uuid_source_message_id=True)
    first_result, _loader, _builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    row = repository.messages[(RAW_CHAT_ID, RAW_MESSAGE_ID)]
    assert isinstance(row["source_message_id"], UUID)

    second_result, _loader, second_builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    second = second_result.to_sanitized_dict()

    assert first_result.ok is True
    assert second_result.ok is True
    assert second["source_messages_created_count"] == 0
    assert second["source_versions_appended_count"] == 0
    assert second["outbox_events_inserted_count"] == 0
    assert second["idempotent_noop_count"] == 1
    assert second["database_write_attempted"] is False
    assert second["outbox_write_attempted"] is False
    assert second_builder.close_commits == [True]
    assert repository.upsert_calls == 1
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repository",
    [
        FakeRepository(upsert_source_message_id=123),
        FakeRepository(omit_upsert_source_message_id=True),
    ],
)
async def test_invalid_or_missing_upsert_source_message_id_fails_closed_without_outbox(
    repository: FakeRepository,
) -> None:
    result, _loader, builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "source_message_id_invalid"
    assert report["database_write_attempted"] is True
    assert report["outbox_write_attempted"] is False
    assert report["outbox_events_inserted_count"] == 0
    assert repository.messages == {}
    assert repository.outbox == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
async def test_invalid_existing_source_message_id_fails_closed_before_database_write() -> None:
    repository = FakeRepository()
    repository.messages[(RAW_CHAT_ID, RAW_MESSAGE_ID)] = {
        "source_message_id": {"unexpected": "shape"},
        "chat_id": RAW_CHAT_ID,
        "message_id": RAW_MESSAGE_ID,
        "logical_post_key": f"telegram:{RAW_CHAT_ID}:{RAW_MESSAGE_ID}",
        "current_version_no": 0,
    }
    result, _loader, builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "source_message_id_invalid"
    assert report["database_write_attempted"] is False
    assert report["outbox_write_attempted"] is False
    assert repository.upsert_calls == 0
    assert repository.outbox == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
async def test_tdlib_history_client_submits_parameters_before_history_request() -> None:
    state = BoundedTelegramCollectorHistoryIngestState()
    transport = FakeTDLibTransport(
        auth_payloads=[
            {"@type": "authorizationStateWaitTdlibParameters"},
            {"@type": "authorizationStateWaitEncryptionKey"},
            {
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": "authorizationStateReady"},
            },
        ]
    )
    tdlib = TDLibClient(_runtime_config(), transport=transport)
    client = _TDLibBoundedHistoryClient(tdlib, state=state, auth_ready_timeout_sec=0.1)

    messages = await client.fetch_newest_history_messages(chat_id=RAW_CHAT_ID, limit=1)

    request_types = [request["@type"] for request in transport.sent_requests]
    assert request_types == [
        "getAuthorizationState",
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "getChatHistory",
    ]
    assert messages[0]["id"] == RAW_MESSAGE_ID
    assert state.tdlib_log_suppression_attempted is True
    assert state.tdlib_log_suppression_confirmed is True
    assert state.tdlib_auth_ready_checked is True
    assert state.tdlib_auth_ready is True
    assert state.tdlib_parameters_submitted is True
    assert state.telegram_read_called is True
    assert transport.sent_requests[1]["api_hash"] == RAW_SECRET
    assert transport.sent_requests[2]["encryption_key"]
    assert transport.sent_requests[3]["chat_id"] == RAW_CHAT_ID


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state_type",
    [
        "authorizationStateWaitPhoneNumber",
        "authorizationStateWaitCode",
        "authorizationStateWaitOtherDeviceConfirmation",
        "authorizationStateWaitPassword",
    ],
)
async def test_tdlib_history_client_blocks_interactive_auth_before_history_request(state_type: str) -> None:
    state = BoundedTelegramCollectorHistoryIngestState()
    transport = FakeTDLibTransport(auth_payloads=[{"@type": state_type}])
    tdlib = TDLibClient(_runtime_config(), transport=transport)
    client = _TDLibBoundedHistoryClient(tdlib, state=state, auth_ready_timeout_sec=0.1)

    with pytest.raises(BoundedHistoryIngestError) as exc_info:
        await client.fetch_newest_history_messages(chat_id=RAW_CHAT_ID, limit=1)

    assert exc_info.value.error_code == "tdlib_not_authorized"
    assert [request["@type"] for request in transport.sent_requests] == ["getAuthorizationState"]
    assert state.tdlib_auth_ready_checked is True
    assert state.tdlib_auth_ready is False
    assert state.telegram_read_called is False


@pytest.mark.asyncio
async def test_tdlib_history_client_times_out_before_history_request_when_ready_never_seen() -> None:
    state = BoundedTelegramCollectorHistoryIngestState()
    transport = FakeTDLibTransport(auth_payloads=[])
    tdlib = TDLibClient(_runtime_config(), transport=transport)
    client = _TDLibBoundedHistoryClient(tdlib, state=state, auth_ready_timeout_sec=0.01)

    with pytest.raises(BoundedHistoryIngestError) as exc_info:
        await client.fetch_newest_history_messages(chat_id=RAW_CHAT_ID, limit=1)

    assert exc_info.value.error_code == "tdlib_auth_ready_timeout"
    assert [request["@type"] for request in transport.sent_requests] == ["getAuthorizationState"]
    assert state.tdlib_auth_ready_checked is True
    assert state.tdlib_auth_ready is False
    assert state.telegram_read_called is False


@pytest.mark.asyncio
async def test_tdlib_history_client_requires_log_suppression_before_auth_or_history_request() -> None:
    state = BoundedTelegramCollectorHistoryIngestState()
    transport = FakeTDLibTransport(
        auth_payloads=[{"@type": "authorizationStateReady"}],
        log_suppression_attempted=True,
        log_suppression_confirmed=False,
    )
    tdlib = TDLibClient(_runtime_config(), transport=transport)
    client = _TDLibBoundedHistoryClient(tdlib, state=state, auth_ready_timeout_sec=0.1)

    with pytest.raises(BoundedHistoryIngestError) as exc_info:
        await client.fetch_newest_history_messages(chat_id=RAW_CHAT_ID, limit=1)

    assert exc_info.value.error_code == "tdlib_log_suppression_unconfirmed"
    assert transport.sent_requests == []
    assert state.tdlib_log_suppression_attempted is True
    assert state.tdlib_log_suppression_confirmed is False
    assert state.tdlib_auth_ready_checked is False
    assert state.telegram_read_called is False


@pytest.mark.asyncio
async def test_duplicate_same_message_is_idempotent_noop_without_new_outbox() -> None:
    repository = FakeRepository()
    first_history = FakeHistoryClient([_message()])
    first_result, _loader, _builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=first_history,
    )
    second_history = FakeHistoryClient([_message()])
    second_result, _loader, _builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=second_history,
    )
    second = second_result.to_sanitized_dict()

    assert first_result.ok is True
    assert second_result.ok is True
    assert second["source_messages_created_count"] == 0
    assert second["source_versions_appended_count"] == 0
    assert second["outbox_events_inserted_count"] == 0
    assert second["idempotent_noop_count"] == 1
    assert second["database_write_attempted"] is False
    assert second["outbox_write_attempted"] is False
    assert len(repository.outbox) == 1
    assert repository.upsert_calls == 1


@pytest.mark.asyncio
async def test_changed_history_message_appends_reconcile_version_and_outbox() -> None:
    repository = FakeRepository()
    first_result, _loader, _builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    second_result, _loader, _builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message(text="changed bounded history ingest text")]),
    )
    second = second_result.to_sanitized_dict()
    source_message_id = next(iter(repository.versions))

    assert first_result.ok is True
    assert second_result.ok is True
    assert second["source_messages_created_count"] == 0
    assert second["source_versions_appended_count"] == 1
    assert second["outbox_events_inserted_count"] == 1
    assert repository.versions[source_message_id][-1]["version_reason"] == "reconcile"
    assert len(repository.outbox) == 2
    assert repository.outbox[-1].event_type == "source_message.reconciled.v1"
    assert repository.cursor_updates[-1]["last_seen_message_id"] == RAW_MESSAGE_ID


@pytest.mark.asyncio
async def test_publish_after_source_commit() -> None:
    tracked = _tracked_chat()
    repository = ImplicitTransactionRepository(tracked=tracked)
    history = FakeHistoryClient([_message()])
    publisher = ObservingRedisPublisher(repository)
    builder = ImplicitTransactionRuntimeBuilder(repository, history)

    async def runtime_builder(runtime_config, state, logger):
        handle = await builder(runtime_config, state, logger)
        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=handle.repository,
            history_client=handle.history_client,
            close=handle.close,
            commit=handle.commit,
            redis_publisher=publisher,
        )

    result = await run_bounded_telegram_collector_history_ingest(
        _approved_config(allow_source_outbox_publish=True, allow_redis_publish=True),
        runtime_config_loader=Loader(),
        runtime_builder=runtime_builder,
    )

    assert result.ok is True
    assert builder.commit_calls == 2
    assert publisher.committed_counts_at_publish == [(1, 1, 1)]
    assert publisher.pending_counts_at_publish == [(0, 0, 0)]
    assert builder.pre_commit_committed_counts[0] == (0, 0, 0)
    assert builder.pre_commit_pending_counts[0] == (1, 1, 1)


@pytest.mark.asyncio
async def test_source_commit_failure_blocks_redis_publish() -> None:
    tracked = _tracked_chat()
    repository = ImplicitTransactionRepository(tracked=tracked)
    history = FakeHistoryClient([_message()])
    publisher = ObservingRedisPublisher(repository)
    builder = ImplicitTransactionRuntimeBuilder(
        repository,
        history,
        commit_errors=[RuntimeError(CLOSE_EXCEPTION_DETAIL)],
    )

    async def runtime_builder(runtime_config, state, logger):
        handle = await builder(runtime_config, state, logger)
        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=handle.repository,
            history_client=handle.history_client,
            close=handle.close,
            commit=handle.commit,
            redis_publisher=publisher,
        )

    result = await run_bounded_telegram_collector_history_ingest(
        _approved_config(allow_source_outbox_publish=True, allow_redis_publish=True),
        runtime_config_loader=Loader(),
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is False
    assert report["error_code"] == "runtime_commit_failed"
    assert report["error_class"] == "RuntimeError"
    assert publisher.publish_calls == []
    assert repository.committed_counts() == (0, 0, 0)
    assert builder.close_commits == [False]
    for raw_value in (str(RAW_CHAT_ID), RAW_MESSAGE_TEXT, DB_URL, RAW_SECRET, CLOSE_EXCEPTION_DETAIL):
        assert raw_value not in rendered


@pytest.mark.asyncio
async def test_redis_publish_failure_preserves_committed_source_and_pending_outbox() -> None:
    tracked = _tracked_chat()
    repository = ImplicitTransactionRepository(tracked=tracked)
    history = FakeHistoryClient([_message()])
    publisher = ObservingRedisPublisher(
        repository,
        failure=RuntimeError("private redis xadd failure detail"),
    )
    builder = ImplicitTransactionRuntimeBuilder(repository, history)

    async def runtime_builder(runtime_config, state, logger):
        handle = await builder(runtime_config, state, logger)
        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=handle.repository,
            history_client=handle.history_client,
            close=handle.close,
            commit=handle.commit,
            redis_publisher=publisher,
        )

    result = await run_bounded_telegram_collector_history_ingest(
        _approved_config(allow_source_outbox_publish=True, allow_redis_publish=True),
        runtime_config_loader=Loader(),
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is False
    assert report["error_code"] == "redis_publish_failed"
    assert report["redis_events_published_count"] == 0
    assert report["event_outbox_marked_published_count"] == 0
    assert repository.committed_counts() == (1, 1, 1)
    assert len(repository.committed_outbox) == 1
    assert repository.mark_published_calls == []
    assert builder.close_commits == [False]
    assert "private redis xadd failure detail" not in rendered


@pytest.mark.asyncio
async def test_mark_published_commit_failure_is_not_success() -> None:
    tracked = _tracked_chat()
    repository = ImplicitTransactionRepository(tracked=tracked)
    history = FakeHistoryClient([_message()])
    publisher = ObservingRedisPublisher(repository, message_id="9999999999-0")
    builder = ImplicitTransactionRuntimeBuilder(
        repository,
        history,
        commit_errors=[None, RuntimeError(CLOSE_EXCEPTION_DETAIL)],
    )

    async def runtime_builder(runtime_config, state, logger):
        handle = await builder(runtime_config, state, logger)
        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=handle.repository,
            history_client=handle.history_client,
            close=handle.close,
            commit=handle.commit,
            redis_publisher=publisher,
        )

    result = await run_bounded_telegram_collector_history_ingest(
        _approved_config(allow_source_outbox_publish=True, allow_redis_publish=True),
        runtime_config_loader=Loader(),
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is False
    assert report["error_code"] == "event_outbox_mark_published_commit_failed"
    assert report["redis_events_published_count"] == 1
    assert report["event_outbox_marked_published_count"] == 0
    assert report["side_effects"]["redis_mutation"] is True
    assert publisher.publish_calls
    assert repository.committed_counts() == (1, 1, 1)
    assert repository.pending_counts() == (0, 0, 0)
    assert builder.close_commits == [False]
    for raw_value in (str(RAW_CHAT_ID), RAW_MESSAGE_TEXT, DB_URL, RAW_SECRET, CLOSE_EXCEPTION_DETAIL):
        assert raw_value not in rendered


@pytest.mark.asyncio
async def test_no_publish_mode_still_commits_source() -> None:
    tracked = _tracked_chat()
    repository = ImplicitTransactionRepository(tracked=tracked)
    history = FakeHistoryClient([_message()])
    builder = ImplicitTransactionRuntimeBuilder(repository, history)
    publisher = ObservingRedisPublisher(repository)

    result = await run_bounded_telegram_collector_history_ingest(
        _approved_config(),
        runtime_config_loader=Loader(),
        runtime_builder=builder,
    )

    assert result.ok is True
    assert repository.committed_counts() == (1, 1, 1)
    assert repository.pending_counts() == (0, 0, 0)
    assert publisher.publish_calls == []
    assert builder.commit_calls == 1
    assert builder.close_commits == [True]


@pytest.mark.asyncio
async def test_source_outbox_publish_uses_thin_redis_shape_and_marks_published_after_xadd() -> None:
    publisher = FakeRedisPublisher(message_id="9999999999-0")
    result, _loader, builder, repository, history = await _run(
        _approved_config(allow_source_outbox_publish=True, allow_redis_publish=True),
        history=FakeHistoryClient([_message()]),
        redis_publisher=publisher,
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is True
    assert report["redis_events_published_count"] == 1
    assert report["event_outbox_marked_published_count"] == 1
    assert report["source_outbox_publish_attempted"] is True
    assert report["redis_publish_attempted"] is True
    assert report["event_outbox_status_write_attempted"] is True
    assert report["event_id_suffixes"]
    assert report["redis_message_id_suffixes"] == ["999999-0"]
    assert repository.mark_published_calls == [UUID(publisher.publish_calls[0][1].trigger_event_id)]
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    assert builder.close_commits == [True]

    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.source.normalize"
    assert route.stage_name == "normalize"
    assert fields == {
        "job_id": message.job_id,
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": message.root_object_id,
        "idempotency_key": message.idempotency_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": message.trigger_event_id,
    }
    assert fields["job_id"] == fields["trigger_event_id"]
    assert "payload_json" not in fields
    assert "raw_message_json" not in fields
    assert RAW_MESSAGE_TEXT not in json.dumps(fields, sort_keys=True)
    for raw in (str(RAW_CHAT_ID), RAW_MESSAGE_TEXT, DB_URL, RAW_SECRET, message.job_id, message.root_object_id):
        assert raw not in rendered


@pytest.mark.asyncio
async def test_redis_publish_failure_does_not_mark_event_published_or_claim_success() -> None:
    publisher = FakeRedisPublisher(failure=RuntimeError("private redis xadd failure detail"))
    result, _loader, builder, repository, _history = await _run(
        _approved_config(allow_source_outbox_publish=True, allow_redis_publish=True),
        history=FakeHistoryClient([_message()]),
        redis_publisher=publisher,
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is False
    assert report["error_code"] == "redis_publish_failed"
    assert report["redis_events_published_count"] == 0
    assert report["event_outbox_marked_published_count"] == 0
    assert report["redis_publish_attempted"] is True
    assert report["event_outbox_status_write_attempted"] is False
    assert repository.mark_published_calls == []
    assert builder.close_commits == [False]
    assert "private redis xadd failure detail" not in rendered


@pytest.mark.asyncio
async def test_failure_report_omits_raw_message_json_secrets_and_exception_detail() -> None:
    repository = FakeRepository(fail_upsert=RuntimeError(EXCEPTION_DETAIL))
    result, _loader, _builder, _repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    output = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "unexpected_failure"
    assert result.error_class == "RuntimeError"
    assert "RuntimeError" in output
    for raw_value in (
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        RAW_MESSAGE_TEXT,
        "sentinel raw message json payload value",
        DB_URL,
        RAW_SECRET,
        EXCEPTION_DETAIL,
    ):
        assert raw_value not in output


def test_runner_and_cli_do_not_import_forbidden_authority_modules() -> None:
    forbidden_import_roots = {
        "subprocess",
        "scripts.ops",
        "src.services.notifier_telegram",
        "src.services.judge_openai",
        "src.services.gh_enricher",
        "src.services.x_enricher",
        "src.services.web_enricher",
    }
    forbidden_call_names = {"run_forever", "systemctl", "docker", "alembic"}

    forbidden_text = (
        "run_forever",
        "runtime.env",
        "xreadgroup",
        "xack",
        "xgroup_create",
        "xclaim",
        "xautoclaim",
    )

    for path in (SOURCE_PATH, TOOL_PATH, NEW_TOOL_PATH):
        source_text = path.read_text(encoding="utf-8")
        tree = ast.parse(source_text)
        imported: set[str] = set()
        called: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
            elif isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    called.add(func.id)
                elif isinstance(func, ast.Attribute):
                    called.add(func.attr)

        assert not (imported & forbidden_import_roots)
        assert not (called & forbidden_call_names)
        for forbidden in forbidden_text:
            assert forbidden not in source_text
