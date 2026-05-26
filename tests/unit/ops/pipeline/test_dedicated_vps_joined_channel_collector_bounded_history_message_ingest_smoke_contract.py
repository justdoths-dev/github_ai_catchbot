from __future__ import annotations

import copy
from pathlib import Path
from typing import Any
from uuid import uuid5, NAMESPACE_URL


FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-history-ingest@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-secret-history-ingest@127.0.0.1:6379/0"
FAKE_DATABASE_PASSWORD = "unit-db-password-history-ingest"
FAKE_REDIS_PASSWORD = "unit-redis-secret-history-ingest"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-history-ingest"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_ID = 444555666
RAW_SOURCE_VALUE = "SensitiveHistoryIngestChannel"
RAW_USERNAME = "SensitiveHistoryIngestUsername"
RAW_TITLE = "Sensitive History Ingest Channel Title"
RAW_INVITE_LINK = "https://t.me/+sensitiveInviteLinkForHistoryIngest"
RAW_TDLIB_PAYLOAD_VALUE = "unit-raw-tdlib-payload-value-history-ingest"
RAW_TEMP_PATH = "/tmp/sensitive-history-ingest-path"
RAW_PHONE = "+15555550126"
RAW_MESSAGE_TEXT = "sensitive history ingest message text"
RAW_CAPTION_TEXT = "sensitive history ingest caption text"


class FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        rows: list[Any] | None = None,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def fetchall(self) -> list[Any]:
        return self._rows


class FakeTransaction:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class FakeDatabaseConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table_available: dict[str, bool] | None = None,
        fail_select_1: bool = False,
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available or {}
        self.fail_select_1 = fail_select_1
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.closed = False
        self.transaction: FakeTransaction | None = None

    def begin(self) -> FakeTransaction:
        self.transaction = FakeTransaction()
        return self.transaction

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(statement)
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()
        gate = module.gate

        padded = f" {normalized.upper()} "
        for forbidden in (" INSERT ", " UPDATE ", " DELETE "):
            assert forbidden not in padded, statement

        if normalized == _normalize(gate.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()

        if normalized == _normalize(gate.SELECT_ONE_QUERY):
            if self.fail_select_1:
                raise RuntimeError(f"cannot connect to {FAKE_DATABASE_URL}")
            return FakeResult(scalar=1)

        if normalized == _normalize(gate.TABLE_AVAILABLE_QUERY):
            qualified = params.get("qualified_table_name")
            table = str(qualified).rsplit(".", 1)[-1]
            return FakeResult(scalar=self.table_available.get(table, True))

        if normalized == _normalize(gate.COUNT_JOINED_ROWS_QUERY):
            return FakeResult(scalar=len(self._joined_rows()))

        if normalized == _normalize(module.SELECT_HISTORY_TARGET_ROWS_LIMIT_QUERY):
            rows = self._joined_rows()
            limit = params.get("limit")
            if isinstance(limit, int):
                rows = rows[:limit]
            return FakeResult(rows=[{"chat_id": row["chat_id"]} for row in rows])

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.closed = True

    def _joined_rows(self) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self.rows
            if row["desired_state"] == "active"
            and row["access_state"] == "joined"
            and row["chat_id"] is not None
        ]
        return sorted(
            rows,
            key=lambda row: (-int(row.get("priority_weight", 0)), row["registry_id"]),
        )


class FakeHistoryProbe:
    def __init__(
        self,
        *,
        results: list[Any] | None = None,
        status: str = "ready",
        final_authorization_state: str | None = "authorizationStateReady",
        helper_status: str = "ready",
        readiness_request_types_sent: list[str] | None = None,
        fail_initialize: Exception | None = None,
    ) -> None:
        self.results = list(results or [])
        self.status = status
        self.final_authorization_state = final_authorization_state
        self.helper_status = helper_status
        self.readiness_request_types_sent = readiness_request_types_sent or [
            "getAuthorizationState",
            "setTdlibParameters",
        ]
        self.fail_initialize = fail_initialize
        self.initialized = False
        self.closed = False
        self.fetch_calls: list[dict[str, int]] = []
        self.tdlib_send_called = False
        self.tdlib_receive_called = False

    @property
    def tdlib_ready_probe_summary(self) -> dict[str, Any]:
        return {
            "tdlib_ready_probe_attempted": self.initialized,
            "tdlib_ready_probe_status": self.status,
            "tdlib_ready_probe_final_authorization_state": (
                self.final_authorization_state
            ),
            "tdlib_ready_helper_status": self.helper_status,
            "tdlib_ready_probe_request_types_sent": list(
                self.readiness_request_types_sent
            ),
            "tdlib_ready_probe_authorization_states_seen": (
                [self.final_authorization_state]
                if self.final_authorization_state
                else []
            ),
        }

    async def initialize(self) -> None:
        self.initialized = True
        self.tdlib_send_called = True
        self.tdlib_receive_called = True
        if self.fail_initialize is not None:
            raise self.fail_initialize

    async def fetch_chat_history(self, *, chat_id: int, limit: int) -> Any:
        self.fetch_calls.append({"chat_id": chat_id, "limit": limit})
        self.tdlib_send_called = True
        self.tdlib_receive_called = True
        if self.results:
            result = self.results.pop(0)
        else:
            result = _module().HistoryIngestChatResult(status="empty")
        if isinstance(result, Exception):
            raise result
        return result

    async def close(self) -> None:
        self.closed = True


class FakeRepositoryTransaction:
    def __init__(self, repository: "FakeIngestRepository") -> None:
        self.repository = repository
        self.committed = False
        self.rolled_back = False
        self._snapshot: Any = None

    async def __aenter__(self) -> "FakeIngestRepository":
        if self.repository.fail_transaction_enter is not None:
            raise self.repository.fail_transaction_enter
        self._snapshot = self.repository.snapshot()
        self.repository.transactions.append(self)
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            self.repository.restore(self._snapshot)
            self.rolled_back = True
            return None
        self.committed = True
        return None


class FakeIngestRepository:
    def __init__(
        self,
        *,
        fail_transaction_enter: Exception | None = None,
        fail_get: Exception | None = None,
        fail_upsert: Exception | None = None,
        fail_append: Exception | None = None,
        fail_outbox: bool = False,
        fail_outbox_exception: Exception | None = None,
    ) -> None:
        self.fail_transaction_enter = fail_transaction_enter
        self.fail_get = fail_get
        self.fail_upsert = fail_upsert
        self.fail_append = fail_append
        self.fail_outbox = fail_outbox
        self.fail_outbox_exception = fail_outbox_exception
        self.messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[Any] = []
        self.transactions: list[FakeRepositoryTransaction] = []

    def snapshot(self) -> Any:
        return (
            copy.deepcopy(self.messages),
            copy.deepcopy(self.versions),
            list(self.outbox),
        )

    def restore(self, snapshot: Any) -> None:
        self.messages, self.versions, self.outbox = snapshot

    def transaction(self) -> FakeRepositoryTransaction:
        return FakeRepositoryTransaction(self)

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> dict[str, Any] | None:
        if self.fail_get is not None:
            raise self.fail_get
        assert platform == "telegram"
        return self.messages.get((chat_id, message_id))

    async def upsert_source_message(
        self,
        projection: Any,
        *,
        platform: str = "telegram",
    ) -> dict[str, Any]:
        if self.fail_upsert is not None:
            raise self.fail_upsert
        assert platform == "telegram"
        key = (projection.chat_id, projection.message_id)
        row = self.messages.get(key)
        if row is None:
            row = {
                "source_message_id": str(
                    uuid5(
                        NAMESPACE_URL,
                        f"telegram:{projection.chat_id}:{projection.message_id}",
                    )
                ),
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "current_version_no": 0,
                "content_hash": None,
            }
            self.messages[key] = row
            self.versions[row["source_message_id"]] = []
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
        if self.fail_append is not None:
            raise self.fail_append
        versions = self.versions.setdefault(source_message_id, [])
        previous_hash = versions[-1]["content_hash"] if versions else None
        if previous_hash == projection.content_hash:
            return False, None
        row = {
            "source_message_id": source_message_id,
            "version_no": len(versions) + 1,
            "version_reason": version_reason,
            "content_hash": projection.content_hash,
        }
        versions.append(row)
        for current in self.messages.values():
            if current["source_message_id"] == source_message_id:
                current["current_version_no"] = row["version_no"]
                current["content_hash"] = projection.content_hash
                break
        return True, row

    async def insert_outbox_event(self, event: Any) -> None:
        if self.fail_outbox_exception is not None:
            raise self.fail_outbox_exception
        if self.fail_outbox:
            raise RuntimeError("outbox insert failed")
        self.outbox.append(event)


class FakeRepositoryContext:
    def __init__(self, repository: FakeIngestRepository) -> None:
        self.repository = repository
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakeIngestRepository:
        self.entered = True
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.exited = True


def _module():
    from scripts.ops import (
        dedicated_vps_joined_channel_collector_bounded_history_message_ingest_smoke
        as module,
    )

    return module


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(tmp_path: Path) -> dict[str, str]:
    tdlib_state_dir = tmp_path / "tdlib-state"
    tdlib_files_dir = tmp_path / "tdlib-files"
    lock_dir = tmp_path / "locks"
    tdlib_state_dir.mkdir(parents=True, exist_ok=True)
    tdlib_files_dir.mkdir(parents=True, exist_ok=True)
    lock_dir.mkdir(parents=True, exist_ok=True)
    return {
        "APP_ENV": "test",
        "COLLECTOR_MODE": "replay",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "TELEGRAM_API_HASH": FAKE_TELEGRAM_SECRET,
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_PHONE_NUMBER": RAW_PHONE,
        "TELEGRAM_2FA_PASSWORD": "unit-2fa-secret",
        "TDLIB_DB_ENCRYPTION_KEY": "fake-tdlib-encryption-key",
        "TDLIB_STATE_DIR": str(tdlib_state_dir),
        "TDLIB_FILES_DIR": str(tdlib_files_dir),
        "COLLECTOR_SINGLETON_LOCK_PATH": str(lock_dir / "collector.lock"),
        "HISTORY_INGEST_TEMP_PATH": RAW_TEMP_PATH,
        "HISTORY_INGEST_INVITE_LINK": RAW_INVITE_LINK,
    }


def _runtime_env_reader(tmp_path: Path):
    return lambda _path: _runtime_env(tmp_path)


def _registry_row(
    registry_id: str,
    *,
    desired_state: str = "active",
    access_state: str = "joined",
    chat_id: int | None = RAW_CHAT_ID,
    priority_weight: int = 100,
) -> dict[str, Any]:
    return {
        "registry_id": registry_id,
        "source_kind": "public_username",
        "source_value": RAW_SOURCE_VALUE,
        "username_snapshot": RAW_USERNAME,
        "title_snapshot": RAW_TITLE,
        "desired_state": desired_state,
        "access_state": access_state,
        "chat_id": chat_id,
        "priority_weight": priority_weight,
    }


def _text_message(
    *,
    message_id: int = RAW_MESSAGE_ID,
    text: str = RAW_MESSAGE_TEXT,
    chat_id: int = RAW_CHAT_ID,
) -> dict[str, Any]:
    return {
        "@type": "message",
        "chat_id": chat_id,
        "id": message_id,
        "date": 1713550000,
        "is_channel_post": True,
        "author_signature": RAW_USERNAME,
        "content": {
            "@type": "messageText",
            "text": {"text": text, "entities": []},
        },
        "_raw_tdlib_payload": RAW_TDLIB_PAYLOAD_VALUE,
    }


def _photo_message(
    *,
    message_id: int = RAW_MESSAGE_ID + 1,
    caption: str = RAW_CAPTION_TEXT,
    chat_id: int = RAW_CHAT_ID,
) -> dict[str, Any]:
    return {
        "@type": "message",
        "chat_id": chat_id,
        "id": message_id,
        "date": 1713550100,
        "is_channel_post": True,
        "content": {
            "@type": "messagePhoto",
            "caption": {"text": caption, "entities": []},
        },
    }


def _history_result(*messages: dict[str, Any], request_types: tuple[str, ...] = ("getChatHistory",)) -> Any:
    return _module().HistoryIngestChatResult(
        status="history",
        messages=tuple(messages),
        request_types_sent=request_types,
    )


def _run_report(
    tmp_path: Path,
    *,
    db: FakeDatabaseConnection | None = None,
    approved_tdlib: bool = False,
    approved_history: bool = False,
    approved_db_write: bool = False,
    probe: FakeHistoryProbe | None = None,
    repository: FakeIngestRepository | None = None,
    history_ingest_max_chats: int = 1,
    history_ingest_history_limit: int = 3,
    history_ingest_max_messages: int = 3,
    history_ingest_max_db_writes: int = 30,
    runtime_env_reader: Any | None = None,
) -> tuple[
    dict[str, Any],
    FakeDatabaseConnection,
    FakeHistoryProbe | None,
    FakeIngestRepository,
]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    fake_repository = repository or FakeIngestRepository()
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        approved_tdlib_readiness_probe=approved_tdlib,
        approved_history_message_ingest_smoke=approved_history,
        approved_history_message_ingest_db_write=approved_db_write,
        history_ingest_max_chats=history_ingest_max_chats,
        history_ingest_history_limit=history_ingest_history_limit,
        history_ingest_max_messages=history_ingest_max_messages,
        history_ingest_max_db_writes=history_ingest_max_db_writes,
        runtime_env_reader=runtime_env_reader or _runtime_env_reader(tmp_path),
        database_connection_factory=lambda _database_url: fake_db,
        history_probe_factory=(
            (lambda _values, _max, _timeout, _overall: probe)
            if probe is not None
            else None
        ),
        repository_context_factory=lambda _values: FakeRepositoryContext(
            fake_repository
        ),
    )
    return result.report, fake_db, probe, fake_repository


def _raw_exception_message() -> str:
    return " ".join(
        [
            str(RAW_CHAT_ID),
            str(RAW_MESSAGE_ID),
            RAW_MESSAGE_TEXT,
            FAKE_DATABASE_URL,
            RAW_TEMP_PATH,
        ]
    )


def test_default_no_write_mode_does_not_call_get_chat_history_or_mutate_db(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, db, probe, repository = _run_report(tmp_path, probe=probe)

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_ingest_ready"
    )
    assert report["history_ingest_approved"] is False
    assert report["history_ingest_attempted"] is False
    assert report["tdlib_readiness_probe_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert report["database_mutation_performed"] is False
    assert report["redis_mutation_performed"] is False
    assert db.transaction is not None
    assert db.transaction.rolled_back is True
    assert probe is not None
    assert probe.initialized is False
    assert probe.fetch_calls == []
    assert repository.messages == {}


def test_history_ingest_flag_without_tdlib_readiness_approval_blocks(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, probe, repository = _run_report(
        tmp_path,
        approved_tdlib=False,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_not_ready"
    assert "approval.tdlib_readiness_probe_required" in report["checks_failed"]
    assert report["history_ingest_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert probe is not None
    assert probe.initialized is False
    assert repository.messages == {}


def test_history_ingest_db_write_approval_missing_returns_ready_without_mutation(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=False,
        probe=probe,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_ingest_ready"
    )
    assert report["history_ingest_approved"] is True
    assert report["history_ingest_db_write_approved"] is False
    assert report["history_ingest_attempted"] is False
    assert report["database_mutation_performed"] is False
    assert probe is not None
    assert probe.initialized is False
    assert repository.messages == {}


def test_invalid_caps_block_before_tdlib_or_history_call(tmp_path: Path) -> None:
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        history_ingest_max_chats=4,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_not_ready"
    assert "history_ingest.max_chats_out_of_bounds" in report["checks_failed"]
    assert report["runtime_env_read"] is False
    assert report["tdlib_readiness_probe_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert probe is not None
    assert probe.initialized is False
    assert repository.messages == {}


def test_tdlib_not_ready_blocks_before_history_fetch(tmp_path: Path) -> None:
    probe = FakeHistoryProbe(
        results=[_history_result(_text_message())],
        status="not_ready",
        final_authorization_state="authorizationStateWaitCode",
    )

    report, _db, probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_not_ready"
    assert "tdlib.not_ready" in report["checks_failed"]
    assert report["tdlib_ready_probe_final_authorization_state"] == (
        "authorizationStateWaitCode"
    )
    assert report["history_ingest_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert probe is not None
    assert probe.fetch_calls == []
    assert probe.closed is True
    assert repository.messages == {}


def test_no_joined_rows_blocks_before_tdlib_or_history_call(tmp_path: Path) -> None:
    db = FakeDatabaseConnection(
        [_registry_row("registry-unjoined", access_state="resolved_not_joined")]
    )
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, probe, repository = _run_report(
        tmp_path,
        db=db,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_not_ready"
    assert "registry.no_active_joined_rows" in report["checks_failed"]
    assert report["joined_row_count_bucket"] == "zero"
    assert report["tdlib_readiness_probe_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert probe is not None
    assert probe.initialized is False
    assert repository.messages == {}


def test_fake_message_text_new_message_writes_source_version_outbox(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_ingest_writes_observed"
    )
    assert report["history_ingest_attempted"] is True
    assert report["history_ingest_status"] == "writes_observed"
    assert report["history_ingest_selected_chats_bucket"] == "one"
    assert report["history_ingest_get_chat_history_requests_bucket"] == "one"
    assert report["history_ingest_history_messages_observed_bucket"] == "one"
    assert report["history_ingest_message_bearing_messages_observed_bucket"] == "one"
    assert report["history_ingest_messages_considered_bucket"] == "one"
    assert report["history_ingest_messages_written_bucket"] == "one"
    assert report["history_ingest_messages_noop_bucket"] == "zero"
    assert report["history_ingest_source_messages_written_bucket"] == "one"
    assert report["history_ingest_source_message_versions_written_bucket"] == "one"
    assert report["history_ingest_event_outbox_written_bucket"] == "one"
    assert report["history_ingest_created_events_bucket"] == "one"
    assert report["history_ingest_reconciled_events_bucket"] == "zero"
    assert report["history_ingest_content_type_buckets"] == {"messageText": "one"}
    assert report["history_ingest_failure_stage"] == "none"
    assert report["history_ingest_failure_class"] is None
    assert report["history_ingest_projection_attempted"] is True
    assert report["history_ingest_projection_succeeded"] is True
    assert report["history_ingest_repository_transaction_attempted"] is True
    assert report["history_ingest_repository_transaction_entered"] is True
    assert report["history_ingest_get_existing_attempted"] is True
    assert report["history_ingest_upsert_attempted"] is True
    assert report["history_ingest_upsert_succeeded"] is True
    assert report["history_ingest_version_append_attempted"] is True
    assert report["history_ingest_version_append_succeeded"] is True
    assert report["history_ingest_outbox_build_attempted"] is True
    assert report["history_ingest_outbox_build_succeeded"] is True
    assert report["history_ingest_outbox_insert_attempted"] is True
    assert report["history_ingest_outbox_insert_succeeded"] is True
    assert report["history_ingest_transaction_completed"] is True
    assert report["database_mutation_performed"] is True
    assert report["redis_mutation_performed"] is False
    assert report["telegram_raw_updates_written"] is False
    assert report["source_messages_written"] is True
    assert report["source_message_versions_written"] is True
    assert report["event_outbox_written"] is True
    assert report["tdlib_history_fetch_called"] is True
    assert probe is not None
    assert probe.fetch_calls == [{"chat_id": RAW_CHAT_ID, "limit": 3}]
    assert len(repository.messages) == 1
    assert len(repository.outbox) == 1
    assert repository.outbox[0].event_type == "source_message.created.v1"


def test_repeated_identical_fake_message_is_idempotent_noop(
    tmp_path: Path,
) -> None:
    repository = FakeIngestRepository()
    first_probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    first_report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=first_probe,
        repository=repository,
    )
    second_probe = FakeHistoryProbe(results=[_history_result(_text_message())])
    second_report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=second_probe,
        repository=repository,
    )

    source_id = next(iter(repository.messages.values()))["source_message_id"]
    assert first_report["contract_status"].endswith("writes_observed")
    assert second_report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_ingest_noop_observed"
    )
    assert second_report["history_ingest_messages_written_bucket"] == "zero"
    assert second_report["history_ingest_messages_noop_bucket"] == "one"
    assert second_report["history_ingest_failure_stage"] == "none"
    assert second_report["history_ingest_failure_class"] is None
    assert second_report["history_ingest_projection_attempted"] is True
    assert second_report["history_ingest_projection_succeeded"] is True
    assert second_report["history_ingest_repository_transaction_entered"] is True
    assert second_report["history_ingest_get_existing_attempted"] is True
    assert second_report["history_ingest_upsert_attempted"] is False
    assert second_report["history_ingest_version_append_attempted"] is True
    assert second_report["history_ingest_version_append_succeeded"] is True
    assert second_report["history_ingest_outbox_build_attempted"] is False
    assert second_report["history_ingest_outbox_insert_attempted"] is False
    assert second_report["history_ingest_transaction_completed"] is True
    assert second_report["database_mutation_performed"] is False
    assert len(repository.versions[source_id]) == 1
    assert len(repository.outbox) == 1


def test_changed_fake_message_appends_version_and_emits_reconciled_event(
    tmp_path: Path,
) -> None:
    repository = FakeIngestRepository()
    first_probe = FakeHistoryProbe(results=[_history_result(_text_message())])
    _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=first_probe,
        repository=repository,
    )
    changed_probe = FakeHistoryProbe(
        results=[_history_result(_text_message(text="changed sensitive text"))]
    )

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=changed_probe,
        repository=repository,
    )

    source_id = next(iter(repository.messages.values()))["source_message_id"]
    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_ingest_writes_observed"
    )
    assert report["history_ingest_created_events_bucket"] == "zero"
    assert report["history_ingest_reconciled_events_bucket"] == "one"
    assert len(repository.versions[source_id]) == 2
    assert repository.versions[source_id][-1]["version_reason"] == "reconcile"
    assert len(repository.outbox) == 2
    assert repository.outbox[-1].event_type == "source_message.reconciled.v1"


def test_projection_builder_failure_reports_stage_and_sanitized_class(
    tmp_path: Path,
) -> None:
    message = _text_message()
    del message["id"]
    probe = FakeHistoryProbe(results=[_history_result(message)])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )
    rendered = _module().render_json(report)

    assert report["contract_status"] == "blocked_history_message_ingest_failed"
    assert "history_ingest.unexpected_failure" in report["checks_failed"]
    assert report["history_ingest_failure_stage"] == "projection_build"
    assert report["history_ingest_failure_class"] == "KeyError"
    assert report["history_ingest_projection_attempted"] is True
    assert report["history_ingest_projection_succeeded"] is False
    assert report["history_ingest_repository_transaction_attempted"] is False
    assert repository.messages == {}
    assert str(RAW_CHAT_ID) not in rendered
    assert str(RAW_MESSAGE_ID) not in rendered
    assert RAW_MESSAGE_TEXT not in rendered


def test_repository_transaction_enter_failure_reports_stage(tmp_path: Path) -> None:
    repository = FakeIngestRepository(
        fail_transaction_enter=RuntimeError(_raw_exception_message())
    )
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        repository=repository,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_failed"
    assert report["history_ingest_failure_stage"] == "repository_transaction_enter"
    assert report["history_ingest_failure_class"] == "RuntimeError"
    assert report["history_ingest_projection_succeeded"] is True
    assert report["history_ingest_repository_transaction_attempted"] is True
    assert report["history_ingest_repository_transaction_entered"] is False
    assert repository.transactions == []


def test_get_source_message_failure_reports_stage(tmp_path: Path) -> None:
    repository = FakeIngestRepository(fail_get=LookupError(_raw_exception_message()))
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        repository=repository,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_failed"
    assert report["history_ingest_failure_stage"] == "get_existing_source_message"
    assert report["history_ingest_failure_class"] == "LookupError"
    assert report["history_ingest_get_existing_attempted"] is True
    assert repository.transactions[-1].rolled_back is True


def test_upsert_source_message_failure_reports_stage(tmp_path: Path) -> None:
    repository = FakeIngestRepository(fail_upsert=RuntimeError(_raw_exception_message()))
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        repository=repository,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_failed"
    assert report["history_ingest_failure_stage"] == "upsert_source_message"
    assert report["history_ingest_failure_class"] == "RuntimeError"
    assert report["history_ingest_upsert_attempted"] is True
    assert report["history_ingest_upsert_succeeded"] is False
    assert repository.transactions[-1].rolled_back is True


def test_append_source_message_version_failure_reports_stage(tmp_path: Path) -> None:
    repository = FakeIngestRepository(fail_append=RuntimeError(_raw_exception_message()))
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        repository=repository,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_failed"
    assert report["history_ingest_failure_stage"] == "append_source_message_version"
    assert report["history_ingest_failure_class"] == "RuntimeError"
    assert report["history_ingest_upsert_succeeded"] is True
    assert report["history_ingest_version_append_attempted"] is True
    assert report["history_ingest_version_append_succeeded"] is False
    assert repository.transactions[-1].rolled_back is True
    assert repository.messages == {}


def test_outbox_build_failure_reports_created_stage(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from src.services.collector_telegram import outbox as outbox_module

    def fail_build_created(self: Any, **_kwargs: Any) -> Any:
        raise ValueError(_raw_exception_message())

    monkeypatch.setattr(
        outbox_module.CollectorOutboxBuilder,
        "build_created",
        fail_build_created,
    )
    repository = FakeIngestRepository()
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        repository=repository,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_failed"
    assert report["history_ingest_failure_stage"] == "outbox_build_created"
    assert report["history_ingest_failure_class"] == "ValueError"
    assert report["history_ingest_version_append_succeeded"] is True
    assert report["history_ingest_outbox_build_attempted"] is True
    assert report["history_ingest_outbox_build_succeeded"] is False
    assert repository.transactions[-1].rolled_back is True
    assert repository.messages == {}


def test_outbox_insert_failure_rolls_back_source_and_version_writes(
    tmp_path: Path,
) -> None:
    repository = FakeIngestRepository(fail_outbox=True)
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        repository=repository,
    )

    assert report["contract_status"] == "blocked_history_message_ingest_failed"
    assert "history_ingest.unexpected_failure" in report["checks_failed"]
    assert report["history_ingest_failure_stage"] == "insert_outbox_event"
    assert report["history_ingest_failure_class"] == "RuntimeError"
    assert report["history_ingest_outbox_build_succeeded"] is True
    assert report["history_ingest_outbox_insert_attempted"] is True
    assert report["history_ingest_outbox_insert_succeeded"] is False
    assert report["history_ingest_transaction_completed"] is False
    assert repository.transactions
    assert repository.transactions[-1].rolled_back is True
    assert repository.messages == {}
    assert repository.versions == {}
    assert repository.outbox == []


def test_exception_messages_with_raw_values_are_not_rendered(tmp_path: Path) -> None:
    repository = FakeIngestRepository(
        fail_upsert=RuntimeError(_raw_exception_message())
    )
    probe = FakeHistoryProbe(results=[_history_result(_text_message())])

    report, _db, _probe, _repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        repository=repository,
    )
    rendered = _module().render_json(report)

    assert report["history_ingest_failure_stage"] == "upsert_source_message"
    assert report["history_ingest_failure_class"] == "RuntimeError"
    assert "RuntimeError" in rendered
    for raw_value in [
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        RAW_MESSAGE_TEXT,
        FAKE_DATABASE_URL,
        RAW_TEMP_PATH,
    ]:
        assert raw_value not in rendered


def test_db_write_cap_stops_further_writes_and_reports_cap_exhaustion(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(
        results=[
            _history_result(
                _text_message(message_id=RAW_MESSAGE_ID),
                _text_message(message_id=RAW_MESSAGE_ID + 1),
            )
        ]
    )

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
        history_ingest_max_db_writes=3,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_ingest_writes_observed"
    )
    assert report["history_ingest_db_write_cap_exhausted"] is True
    assert report["history_ingest_history_messages_observed_bucket"] == "two_to_five"
    assert report["history_ingest_message_bearing_messages_observed_bucket"] == (
        "two_to_five"
    )
    assert report["history_ingest_messages_considered_bucket"] == "one"
    assert report["history_ingest_messages_written_bucket"] == "one"
    assert len(repository.messages) == 1
    assert len(repository.outbox) == 1


def test_forbidden_request_side_effects_block(tmp_path: Path) -> None:
    probe = FakeHistoryProbe(
        results=[
            _history_result(
                _text_message(),
                request_types=("getChatHistory", "sendMessage"),
            )
        ]
    )

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )

    assert report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert report["tdlib_history_fetch_called"] is True
    assert report["tdlib_send_message_called"] is True
    assert report["database_mutation_performed"] is False
    assert repository.messages == {}


def test_no_message_bearing_history_returns_no_messages_observed(
    tmp_path: Path,
) -> None:
    message = {
        "@type": "message",
        "chat_id": RAW_CHAT_ID,
        "id": RAW_MESSAGE_ID,
        "date": 1713550000,
        "content": {"@type": "messagePhoto", "caption": {"text": "", "entities": []}},
    }
    probe = FakeHistoryProbe(results=[_history_result(message)])

    report, _db, _probe, repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_ingest_no_messages_observed"
    )
    assert report["history_ingest_history_messages_observed_bucket"] == "one"
    assert report["history_ingest_message_bearing_messages_observed_bucket"] == "zero"
    assert report["database_mutation_performed"] is False
    assert repository.messages == {}


def test_output_redaction_excludes_sensitive_runtime_and_tdlib_values(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(
        results=[_history_result(_text_message(), _photo_message())]
    )

    report, _db, _probe, _repository = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        approved_db_write=True,
        probe=probe,
    )
    rendered = _module().render_json(report)

    forbidden_values = [
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        RAW_USERNAME,
        RAW_SOURCE_VALUE,
        RAW_TITLE,
        RAW_INVITE_LINK,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_DATABASE_PASSWORD,
        FAKE_REDIS_PASSWORD,
        RAW_PHONE,
        FAKE_TELEGRAM_SECRET,
        RAW_TDLIB_PAYLOAD_VALUE,
        RAW_TEMP_PATH,
        RAW_MESSAGE_TEXT,
        RAW_CAPTION_TEXT,
    ]
    for raw_value in forbidden_values:
        assert raw_value not in rendered
