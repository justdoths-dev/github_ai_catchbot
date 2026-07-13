from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any

import pytest


FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-for-resolved-not-joined-join@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-secret-resolved-not-joined-join@127.0.0.1:6379/0"
FAKE_DATABASE_PASSWORD = "unit-db-password-for-resolved-not-joined-join"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-resolved-not-joined-join"
RAW_PUBLIC_USERNAME = "SensitiveAlphaChannel"
NORMALIZED_PUBLIC_USERNAME = RAW_PUBLIC_USERNAME.lower()
RAW_TITLE = "Sensitive Alpha Channel Title"
RAW_CHAT_ID = 9876543210123
RAW_REGISTRY_ID = "11111111-1111-4111-8111-111111111111"
RAW_LOCATOR_PATH = "/tmp/private-sensitive-exact-target-locator.json"
RAW_LOCATOR_CONTENT = "sensitive-private-locator-content"
RAW_TDLIB_PAYLOAD_VALUE = "unit-raw-tdlib-payload-value-resolved-not-joined-join"
RAW_INVITE_LINK = "https://t.me/+sensitiveInviteLinkForJoinOperator"
RAW_EXTRA = "raw-extra-join-operator"
RAW_TEMP_PATH = "/tmp/sensitive-join-operator-path"
CLEANUP_FAILURE_SENTINEL = " ".join(
    (
        "postgresql://user:secret@host/database",
        "redis://:secret@host/0",
        "/private/exact-target-locator.json",
        "raw-public-source",
        "raw-registry-id",
        "raw-chat-id",
        "raw-message-id",
        "raw-telegram-secret",
    )
)
CLEANUP_FAILURE_FORBIDDEN_VALUES = tuple(CLEANUP_FAILURE_SENTINEL.split())


class FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        rows: list[Any] | None = None,
        rowcount: int = 0,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []
        self.rowcount = rowcount

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def fetchall(self) -> list[Any]:
        return self._rows


class FakeTransaction:
    def __init__(
        self,
        db: "FakeDatabaseConnection",
        *,
        rollback_failure: Exception | None = None,
    ) -> None:
        self._db = db
        self._snapshot = copy.deepcopy(db.rows)
        self._rollback_failure = rollback_failure
        self.commit_called = False
        self.committed = False
        self.rollback_called = False
        self.rollback_succeeded: bool | None = None
        self.rolled_back = False

    def commit(self) -> None:
        self.commit_called = True
        self.committed = True
        self._db.event_log.append("transaction.commit")

    def rollback(self) -> None:
        self.rollback_called = True
        self._db.event_log.append("transaction.rollback")
        if self._rollback_failure is not None:
            self.rollback_succeeded = False
            raise self._rollback_failure
        self.rollback_succeeded = True
        self.rolled_back = True
        if not self.committed:
            self._db.rows = copy.deepcopy(self._snapshot)


class FakeDatabaseConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table_available: bool = True,
        fail_select_1: bool = False,
        exact_select_override: list[Any] | None = None,
        exact_update_rowcount: int | None = None,
        exact_readback_override: list[Any] | None = None,
        read_rollback_failure: Exception | None = None,
        mutation_rollback_failure: Exception | None = None,
        connection_close_failure: Exception | None = None,
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available
        self.fail_select_1 = fail_select_1
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.closed = False
        self.transaction: FakeTransaction | None = None
        self.transactions: list[FakeTransaction] = []
        self.event_log: list[str] = []
        self.update_attempts = 0
        self.updated_rows = 0
        self.exact_select_override = exact_select_override
        self.exact_update_rowcount = exact_update_rowcount
        self.exact_readback_override = exact_readback_override
        self.read_rollback_failure = read_rollback_failure
        self.mutation_rollback_failure = mutation_rollback_failure
        self.connection_close_failure = connection_close_failure
        self.close_attempted = False
        self.close_succeeded: bool | None = None

    def begin(self) -> FakeTransaction:
        rollback_failure = (
            self.read_rollback_failure
            if not self.transactions
            else self.mutation_rollback_failure
        )
        self.transaction = FakeTransaction(
            self,
            rollback_failure=rollback_failure,
        )
        self.transactions.append(self.transaction)
        self.event_log.append("transaction.begin")
        return self.transaction

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(statement)
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()

        if normalized == _normalize(module.SELECT_ONE_QUERY):
            if self.fail_select_1:
                raise RuntimeError(f"cannot connect to {FAKE_DATABASE_URL}")
            return FakeResult(scalar=1)

        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            return FakeResult(scalar=self.table_available)

        if normalized == _normalize(module.COUNT_TARGET_ROWS_QUERY):
            return FakeResult(scalar=len(self._target_rows()))

        if normalized in {
            _normalize(module.SELECT_TARGET_ROWS_QUERY),
            _normalize(module.SELECT_TARGET_ROWS_LIMIT_QUERY),
        }:
            rows = self._target_rows()
            limit = params.get("limit")
            if isinstance(limit, int):
                rows = rows[:limit]
            return FakeResult(
                rows=[
                    {
                        "registry_id": row["registry_id"],
                        "chat_id": row["chat_id"],
                    }
                    for row in rows
                ]
            )

        if normalized == _normalize(module.SELECT_EXACT_TARGET_ROWS_QUERY):
            self.event_log.append("sql.exact_select")
            if self.exact_select_override is not None:
                return FakeResult(rows=copy.deepcopy(self.exact_select_override))
            return FakeResult(
                rows=[
                            {
                                "registry_id": row["registry_id"],
                                "source_value": row["source_value"],
                                "source_kind": row["source_kind"],
                                "desired_state": row["desired_state"],
                                "access_state": row["access_state"],
                                "chat_id": row["chat_id"],
                    }
                    for row in self._exact_target_rows(
                        params["normalized_source_value"]
                    )[:2]
                ]
            )

        if normalized == _normalize(module.UPDATE_JOIN_STATE_REGISTRY_ROW_QUERY):
            self.update_attempts += 1
            for row in self.rows:
                if (
                    row["registry_id"] == params["registry_id"]
                    and row["source_kind"] == "public_username"
                    and row["desired_state"] == "active"
                    and row["access_state"] == "resolved_not_joined"
                    and row["chat_id"] == params["chat_id"]
                    and row["chat_id"] is not None
                ):
                    row["access_state"] = params["access_state"]
                    row["last_join_attempt_at"] = params["attempted_at"]
                    row["updated_at"] = params["attempted_at"]
                    self.updated_rows += 1
                    return FakeResult(rowcount=1)
            return FakeResult(rowcount=0)

        if normalized == _normalize(module.UPDATE_EXACT_JOIN_STATE_REGISTRY_ROW_QUERY):
            self.event_log.append("sql.exact_update")
            self.update_attempts += 1
            if self.exact_update_rowcount is not None:
                return FakeResult(rowcount=self.exact_update_rowcount)
            for row in self.rows:
                if (
                    row["registry_id"] == params["registry_id"]
                    and row["source_kind"] == "public_username"
                    and row["source_value"] == params["source_value"]
                    and module._normalize_exact_public_username(row["source_value"])
                    == params["normalized_source_value"]
                    and row["desired_state"] == "active"
                    and row["access_state"] == "resolved_not_joined"
                    and row["chat_id"] == params["chat_id"]
                    and row["chat_id"] is not None
                ):
                    row["access_state"] = "joined"
                    row["last_join_attempt_at"] = params["attempted_at"]
                    row["updated_at"] = params["attempted_at"]
                    self.updated_rows += 1
                    return FakeResult(rowcount=1)
            return FakeResult(rowcount=0)

        if normalized == _normalize(module.SELECT_EXACT_JOIN_READBACK_QUERY):
            self.event_log.append("sql.exact_readback")
            if self.exact_readback_override is not None:
                return FakeResult(rows=copy.deepcopy(self.exact_readback_override))
            rows = [
                            {
                                "registry_id": row["registry_id"],
                                "source_value": row["source_value"],
                                "source_kind": row["source_kind"],
                                "desired_state": row["desired_state"],
                                "access_state": row["access_state"],
                                "chat_id": row["chat_id"],
                }
                for row in self.rows
                if row["registry_id"] == params["registry_id"]
                and row["source_kind"] == "public_username"
                and row["source_value"] == params["source_value"]
                and module._normalize_exact_public_username(row["source_value"])
                == params["normalized_source_value"]
                and row["desired_state"] == "active"
                and row["access_state"] == "joined"
                and row["chat_id"] == params["chat_id"]
                and row["chat_id"] is not None
            ]
            return FakeResult(rows=rows)

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.close_attempted = True
        if self.connection_close_failure is not None:
            self.event_log.append("connection.close")
            self.close_succeeded = False
            raise self.connection_close_failure
        self.close_succeeded = True
        self.closed = True

    def _target_rows(self) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self.rows
            if row["source_kind"] == "public_username"
            and row["desired_state"] == "active"
            and row["access_state"] == "resolved_not_joined"
            and row["chat_id"] is not None
        ]
        return sorted(
            rows,
            key=lambda row: (-int(row.get("priority_weight", 0)), row["registry_id"]),
        )

    def _exact_target_rows(
        self,
        normalized_source_value: str,
    ) -> list[dict[str, Any]]:
        module = _module()
        return sorted(
            [
                row
                for row in self.rows
                if row["source_kind"] == "public_username"
                and row["desired_state"] == "active"
                and module._normalize_exact_public_username(row["source_value"])
                == normalized_source_value
            ],
            key=lambda row: row["registry_id"],
        )


class FakeJoiner:
    def __init__(
        self,
        responses: dict[int, Any],
        *,
        fail_initialize: Exception | None = None,
        fail_close: Exception | None = None,
        event_log: list[str] | None = None,
    ) -> None:
        self.responses = responses
        self.fail_initialize = fail_initialize
        self.fail_close = fail_close
        self.initialized = False
        self.closed = False
        self.close_attempted = False
        self.close_succeeded: bool | None = None
        self.calls: list[int] = []
        self.tdlib_send_called = False
        self.tdlib_receive_called = False
        self.history_called = False
        self.event_log = event_log

    async def initialize(self) -> None:
        if self.fail_initialize is not None:
            raise self.fail_initialize
        self.initialized = True
        if self.event_log is not None:
            self.event_log.append("joiner.initialize")

    async def join_chat(self, chat_id: int) -> Any:
        self.calls.append(chat_id)
        if self.event_log is not None:
            self.event_log.append("joiner.join_chat")
        self.tdlib_send_called = True
        response = self.responses.get(chat_id)
        if isinstance(response, Exception):
            raise response
        self.tdlib_receive_called = True
        return response

    async def close(self) -> None:
        self.close_attempted = True
        if self.event_log is not None:
            self.event_log.append("joiner.close")
        if self.fail_close is not None:
            self.close_succeeded = False
            raise self.fail_close
        self.close_succeeded = True
        self.closed = True


def _module():
    from scripts.ops import (
        dedicated_vps_telegram_channel_registry_resolved_not_joined_join_operator
        as module,
    )

    return module


def _resolver_module():
    from scripts.ops import (
        dedicated_vps_telegram_channel_registry_public_username_resolve_operator
        as module,
    )

    return module


def _production_joiner_with_injected_close(
    response: Any,
    *,
    close_failure: Exception | None,
    event_log: list[str] | None = None,
) -> tuple[Any, Any]:
    module = _module()
    resolver_module = _resolver_module()

    class InjectedTDLibClient:
        def __init__(self) -> None:
            self.close_attempted = False
            self.close_succeeded: bool | None = None

        async def close(self) -> None:
            self.close_attempted = True
            if event_log is not None:
                event_log.append("production_client.close")
            if close_failure is not None:
                self.close_succeeded = False
                raise close_failure
            self.close_succeeded = True

    client = InjectedTDLibClient()
    base = object.__new__(resolver_module.TDLibPublicUsernameResolver)
    base._client = client
    base.tdlib_send_called = False
    base.tdlib_receive_called = False

    joiner = object.__new__(module.TDLibResolvedNotJoinedJoiner)
    joiner._base = base
    joiner.initialized = False
    joiner.calls = []

    async def initialize() -> None:
        joiner.initialized = True

    async def join_chat(chat_id: int) -> Any:
        joiner.calls.append(chat_id)
        base.tdlib_send_called = True
        base.tdlib_receive_called = True
        return response

    joiner.initialize = initialize
    joiner.join_chat = join_chat
    return joiner, client


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "TELEGRAM_API_HASH": FAKE_TELEGRAM_SECRET,
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_PHONE_NUMBER": "+10000000000",
        "TDLIB_DB_ENCRYPTION_KEY": "fake-tdlib-key",
        "TDLIB_STATE_DIR": "/safe/unit/tdlib-state",
        "TDLIB_FILES_DIR": "/safe/unit/tdlib-files",
        "JOIN_OPERATOR_TEMP_PATH": RAW_TEMP_PATH,
    }


def _registry_row(
    registry_id: str,
    *,
    source_kind: str = "public_username",
    desired_state: str = "active",
    access_state: str = "resolved_not_joined",
    chat_id: int | None = RAW_CHAT_ID,
    source_value: str = RAW_PUBLIC_USERNAME,
    title_snapshot: str | None = RAW_TITLE,
    priority_weight: int = 100,
) -> dict[str, Any]:
    return {
        "registry_id": registry_id,
        "source_kind": source_kind,
        "source_value": source_value,
        "desired_state": desired_state,
        "access_state": access_state,
        "chat_id": chat_id,
        "username_snapshot": source_value,
        "title_snapshot": title_snapshot,
        "last_join_attempt_at": None,
        "updated_at": None,
        "priority_weight": priority_weight,
    }


def _exact_registry_row(
    *,
    registry_id: str = RAW_REGISTRY_ID,
    source_value: str = RAW_PUBLIC_USERNAME,
    desired_state: str = "active",
    access_state: str = "resolved_not_joined",
    chat_id: int | None = RAW_CHAT_ID,
) -> dict[str, Any]:
    return _registry_row(
        registry_id,
        source_value=source_value,
        desired_state=desired_state,
        access_state=access_state,
        chat_id=chat_id,
    )


def _join_result(
    status: str,
    *,
    failure_class: str | None = None,
    response_extra_matched: bool = True,
    response_without_extra_count: int = 0,
    response_wrong_extra_count: int = 0,
    function_response_types_seen: tuple[str, ...] = ("chat",),
) -> Any:
    return _module().JoinChatResult(
        status=status,
        failure_class=failure_class,
        function_response_types_seen=function_response_types_seen,
        response_extra_matched=response_extra_matched,
        response_without_extra_count=response_without_extra_count,
        response_wrong_extra_count=response_wrong_extra_count,
    )


def _run_report(
    *,
    db: FakeDatabaseConnection | None = None,
    joiner: FakeJoiner | None = None,
    dry_run: bool = True,
    approved_tdlib: bool = False,
    approved_mutation: bool = False,
    limit: int | None = None,
    runtime_env_reader=_runtime_env,
) -> tuple[dict[str, Any], FakeDatabaseConnection, FakeJoiner | None]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    fake_joiner = joiner
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        dry_run=dry_run,
        approved_tdlib_join_resolved_not_joined=approved_tdlib,
        approved_registry_join_mutation=approved_mutation,
        limit=limit,
        runtime_env_reader=runtime_env_reader,
        database_connection_factory=lambda _database_url: fake_db,
        resolved_not_joined_joiner_factory=(
            (lambda _values: fake_joiner) if fake_joiner is not None else None
        ),
    )
    return result.report, fake_db, fake_joiner


def _run_exact_result(
    *,
    db: FakeDatabaseConnection | None = None,
    joiner: FakeJoiner | None = None,
    locator_source_value: str = NORMALIZED_PUBLIC_USERNAME,
    dry_run: bool = False,
    approved_tdlib: bool = True,
    approved_mutation: bool = True,
    limit: int | None = None,
) -> tuple[
    Any,
    FakeDatabaseConnection,
    FakeJoiner | None,
    list[object],
]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_exact_registry_row()])
    fake_joiner = joiner
    if fake_joiner is not None:
        fake_joiner.event_log = fake_db.event_log
    locator_reader_calls: list[object] = []
    bounded_reader = module.bounded_history_ingest_runner._read_target_locator

    def strict_locator_reader(value: object | None) -> dict[str, Any]:
        locator_reader_calls.append(value)
        return {"source_value": locator_source_value}

    module.bounded_history_ingest_runner._read_target_locator = strict_locator_reader
    try:
        result = module.generate_report(
            runtime_env_path="/safe/unit/runtime.env",
            dry_run=dry_run,
            approved_tdlib_join_resolved_not_joined=approved_tdlib,
            approved_registry_join_mutation=approved_mutation,
            target_locator_path=RAW_LOCATOR_PATH,
            limit=limit,
            runtime_env_reader=_runtime_env,
            database_connection_factory=lambda _database_url: fake_db,
            resolved_not_joined_joiner_factory=(
                (lambda _values: fake_joiner) if fake_joiner is not None else None
            ),
        )
    finally:
        module.bounded_history_ingest_runner._read_target_locator = bounded_reader
    return result, fake_db, fake_joiner, locator_reader_calls


def _run_exact_report(
    *,
    db: FakeDatabaseConnection | None = None,
    joiner: FakeJoiner | None = None,
    locator_source_value: str = NORMALIZED_PUBLIC_USERNAME,
    dry_run: bool = False,
    approved_tdlib: bool = True,
    approved_mutation: bool = True,
    limit: int | None = None,
) -> tuple[
    dict[str, Any],
    FakeDatabaseConnection,
    FakeJoiner | None,
    list[object],
]:
    result, fake_db, fake_joiner, locator_reader_calls = _run_exact_result(
        db=db,
        joiner=joiner,
        locator_source_value=locator_source_value,
        dry_run=dry_run,
        approved_tdlib=approved_tdlib,
        approved_mutation=approved_mutation,
        limit=limit,
    )
    return result.report, fake_db, fake_joiner, locator_reader_calls


def test_dry_run_reads_eligible_rows_without_tdlib_or_db_mutation() -> None:
    report, db, joiner = _run_report()

    assert report["contract_status"] == "dry_run_resolved_not_joined_join_plan_ready"
    assert report["runtime_env_read"] is True
    assert report["database_connected"] is True
    assert report["target_rows_checked"] is True
    assert report["target_row_count_bucket"] == "one"
    assert report["tdlib_join_attempted"] is False
    assert report["side_effects"]["tdlib_initialized"] is False
    assert report["side_effects"]["telegram_api_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0
    assert joiner is None


def test_no_eligible_rows_returns_blocked_without_side_effects() -> None:
    db = FakeDatabaseConnection(
        [_registry_row("registry-joined", access_state="joined")]
    )

    report, db, _joiner = _run_report(db=db)

    assert report["contract_status"] == "blocked_no_resolved_not_joined_rows"
    assert report["target_row_count_bucket"] == "zero"
    assert report["tdlib_join_attempted"] is False
    assert report["side_effects"]["tdlib_initialized"] is False
    assert report["side_effects"]["telegram_api_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0


def test_registry_mutation_approval_without_tdlib_join_approval_fails_closed() -> None:
    report, db, joiner = _run_report(approved_mutation=True)

    assert report["contract_status"] == "blocked_approval_required"
    assert report["checks_failed"] == ["approval.tdlib_join_required"]
    assert report["tdlib_join_attempted"] is False
    assert report["side_effects"]["tdlib_initialized"] is False
    assert db.update_attempts == 0
    assert joiner is None


def test_tdlib_approved_no_mutation_mode_sends_join_chat_without_update() -> None:
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, joiner = _run_report(
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=False,
    )

    assert report["contract_status"] == "resolved_not_joined_join_completed_no_mutation"
    assert joiner is not None
    assert joiner.initialized is True
    assert joiner.calls == [RAW_CHAT_ID]
    assert report["tdlib_join_attempted"] is True
    assert report["join_attempt_count_bucket"] == "one"
    assert report["join_success_count_bucket"] == "one"
    assert report["join_response_extra_matched_count_bucket"] == "one"
    assert report["updated_row_count_bucket"] == "zero"
    assert report["side_effects"]["tdlib_join_called"] is True
    assert report["side_effects"]["telegram_api_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert db.update_attempts == 0


def test_fully_approved_success_updates_guarded_row_to_joined() -> None:
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, _joiner = _run_report(
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "resolved_not_joined_join_registry_updated"
    assert report["updated_row_count_bucket"] == "one"
    assert report["registry_join_mutation_performed"] is True
    assert report["side_effects"]["database_mutation_performed"] is True
    assert report["side_effects"]["telegram_channel_registry_updated"] is True
    assert db.rows[0]["access_state"] == "joined"
    assert db.rows[0]["last_join_attempt_at"] is not None
    assert db.transaction is not None
    assert db.transaction.committed is True


def test_guarded_update_does_not_touch_ineligible_rows() -> None:
    rows = [
        _registry_row("registry-eligible", chat_id=111),
        _registry_row("registry-paused", desired_state="paused", chat_id=222),
        _registry_row("registry-joined", access_state="joined", chat_id=333),
        _registry_row("registry-invite", source_kind="invite_link", chat_id=444),
        _registry_row("registry-null-chat", chat_id=None),
    ]
    db = FakeDatabaseConnection(rows)
    joiner = FakeJoiner({111: _join_result("joined")})

    report, db, joiner = _run_report(
        db=db,
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "resolved_not_joined_join_registry_updated"
    assert joiner is not None
    assert joiner.calls == [111]
    assert rows[0]["access_state"] == "joined"
    assert rows[1]["access_state"] == "resolved_not_joined"
    assert rows[2]["access_state"] == "joined"
    assert rows[3]["access_state"] == "resolved_not_joined"
    assert rows[4]["access_state"] == "resolved_not_joined"
    update_sql = _module().UPDATE_JOIN_STATE_REGISTRY_ROW_QUERY
    assert "registry_id = :registry_id" in update_sql
    assert "source_kind = 'public_username'" in update_sql
    assert "desired_state = 'active'" in update_sql
    assert "access_state = 'resolved_not_joined'" in update_sql
    assert "chat_id IS NOT NULL" in update_sql


def test_join_requested_classification_updates_only_when_mutation_is_approved() -> None:
    no_mutation_joiner = FakeJoiner(
        {RAW_CHAT_ID: _join_result("join_requested", failure_class="join_requested")}
    )
    report, db, _joiner = _run_report(
        joiner=no_mutation_joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=False,
    )
    assert report["join_requested_count_bucket"] == "one"
    assert report["updated_row_count_bucket"] == "zero"
    assert db.rows[0]["access_state"] == "resolved_not_joined"

    mutation_joiner = FakeJoiner(
        {RAW_CHAT_ID: _join_result("join_requested", failure_class="join_requested")}
    )
    report, db, _joiner = _run_report(
        joiner=mutation_joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )
    assert report["join_requested_count_bucket"] == "one"
    assert report["updated_row_count_bucket"] == "one"
    assert db.rows[0]["access_state"] == "join_requested"


def test_forbidden_private_access_denied_classification_is_sanitized() -> None:
    joiner = FakeJoiner(
        {
            RAW_CHAT_ID: _join_result(
                "forbidden",
                failure_class="forbidden",
                function_response_types_seen=("error",),
            )
        }
    )

    report, db, _joiner = _run_report(
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["join_forbidden_count_bucket"] == "one"
    assert report["join_failure_classes_seen"] == ["forbidden"]
    assert report["updated_row_count_bucket"] == "one"
    assert db.rows[0]["access_state"] == "forbidden"
    rendered = _module().render_json(report)
    for forbidden in (
        RAW_PUBLIC_USERNAME,
        RAW_TITLE,
        str(RAW_CHAT_ID),
        RAW_TDLIB_PAYLOAD_VALUE,
        RAW_INVITE_LINK,
        RAW_EXTRA,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_DATABASE_PASSWORD,
        FAKE_TELEGRAM_SECRET,
    ):
        assert forbidden not in rendered


@pytest.mark.parametrize(
    ("join_status", "failure_class", "count_field", "expected_access_state"),
    (
        ("joined", None, "join_success_count_bucket", "joined"),
        (
            "join_requested",
            "join_requested",
            "join_requested_count_bucket",
            "join_requested",
        ),
        ("forbidden", "forbidden", "join_forbidden_count_bucket", "forbidden"),
    ),
)
def test_production_joiner_close_failure_does_not_change_broad_primary_result(
    join_status: str,
    failure_class: str | None,
    count_field: str,
    expected_access_state: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    resolver_module = _resolver_module()
    response = _join_result(join_status, failure_class=failure_class)
    successful_db = FakeDatabaseConnection([_registry_row("registry-1")])
    failing_db = FakeDatabaseConnection([_registry_row("registry-1")])
    successful_joiner, successful_client = _production_joiner_with_injected_close(
        response,
        close_failure=None,
    )
    failing_joiner, failing_client = _production_joiner_with_injected_close(
        response,
        close_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )

    def generate(joiner: Any, db: FakeDatabaseConnection) -> Any:
        return module.generate_report(
            runtime_env_path="/safe/unit/runtime.env",
            dry_run=False,
            approved_tdlib_join_resolved_not_joined=True,
            approved_registry_join_mutation=True,
            runtime_env_reader=_runtime_env,
            database_connection_factory=lambda _database_url: db,
            resolved_not_joined_joiner_factory=lambda _values: joiner,
        )

    successful_result = generate(successful_joiner, successful_db)
    failing_result = generate(failing_joiner, failing_db)
    stdout, stderr = capsys.readouterr()

    assert type(successful_joiner) is module.TDLibResolvedNotJoinedJoiner
    assert type(failing_joiner) is module.TDLibResolvedNotJoinedJoiner
    assert successful_joiner.close.__func__ is module.TDLibResolvedNotJoinedJoiner.close
    assert failing_joiner.close.__func__ is module.TDLibResolvedNotJoinedJoiner.close
    assert (
        failing_joiner._base.close.__func__
        is resolver_module.TDLibPublicUsernameResolver.close
    )
    assert successful_result.exit_code == 0
    assert failing_result.exit_code == successful_result.exit_code
    assert failing_result.report == successful_result.report
    assert failing_result.report["contract_status"] == (
        "resolved_not_joined_join_registry_updated"
    )
    assert failing_result.report[count_field] == "one"
    assert failing_db.rows[0]["access_state"] == expected_access_state
    assert successful_db.rows[0]["access_state"] == expected_access_state
    assert failing_result.report["registry_join_mutation_performed"] is True
    assert failing_result.report["side_effects"]["database_mutation_performed"] is True
    assert "exact_target_cleanup_failure_codes" not in failing_result.report
    assert failing_client.close_attempted is True
    assert failing_client.close_succeeded is False
    assert successful_client.close_attempted is True
    assert successful_client.close_succeeded is True
    assert stdout == ""
    assert stderr == ""
    rendered = json.dumps(
        [successful_result.report, failing_result.report],
        sort_keys=True,
    )
    _assert_cleanup_failure_is_sanitized(rendered + stdout + stderr)
    assert "RuntimeError" not in rendered + stdout + stderr
    assert "Traceback" not in rendered + stdout + stderr


def test_not_found_is_classified_and_left_unmutated() -> None:
    joiner = FakeJoiner(
        {
            RAW_CHAT_ID: _join_result(
                "not_found",
                failure_class="not_found",
                function_response_types_seen=("error",),
            )
        }
    )

    report, db, _joiner = _run_report(
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "resolved_not_joined_join_partial"
    assert report["join_not_found_count_bucket"] == "one"
    assert report["updated_row_count_bucket"] == "zero"
    assert report["skipped_row_count_bucket"] == "one"
    assert db.rows[0]["access_state"] == "resolved_not_joined"


def test_response_timeout_is_classified_without_mutation() -> None:
    joiner = FakeJoiner(
        {
            RAW_CHAT_ID: _join_result(
                "response_timeout",
                failure_class="response_timeout",
                response_extra_matched=False,
                function_response_types_seen=(),
            )
        }
    )

    report, db, _joiner = _run_report(
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "resolved_not_joined_join_partial"
    assert report["join_response_timeout_count_bucket"] == "one"
    assert report["updated_row_count_bucket"] == "zero"
    assert report["registry_join_mutation_performed"] is False
    assert db.rows[0]["access_state"] == "resolved_not_joined"


def test_wrong_extra_and_without_extra_responses_do_not_cause_join_success() -> None:
    module = _module()
    payloads: list[dict[str, Any] | None] = [
        {"@type": "chat", "@extra": "wrong-extra", "id": 111},
        {"@type": "chat", "id": 111},
        None,
    ]

    async def receive_payload(_timeout: float) -> dict[str, Any] | None:
        return payloads.pop(0) if payloads else None

    result = asyncio.run(
        module._wait_for_matching_join_response(
            extra="expected-extra",
            receive_payload=receive_payload,
            wait_config=module.TDLibJoinRpcWaitConfig(
                max_updates=3,
                receive_timeout_sec=0.01,
                max_duration_sec=1.0,
            ),
        )
    )

    assert result.status == "response_timeout"
    assert result.response_extra_matched is False
    assert result.response_wrong_extra_count == 1
    assert result.response_without_extra_count == 1
    assert result.function_response_types_seen == ("chat",)


def test_authorization_lost_after_prior_update_rolls_back_all_updates() -> None:
    rows = [
        _registry_row("registry-one", chat_id=101, priority_weight=200),
        _registry_row("registry-two", chat_id=102, priority_weight=100),
    ]
    db = FakeDatabaseConnection(rows)
    joiner = FakeJoiner(
        {
            101: _join_result("joined"),
            102: _join_result(
                "authorization_lost",
                failure_class="authorization_lost",
                response_extra_matched=False,
                function_response_types_seen=(),
            ),
        }
    )

    report, db, joiner = _run_report(
        db=db,
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "resolved_not_joined_join_authorization_lost"
    assert joiner is not None
    assert joiner.calls == [101, 102]
    assert report["updated_row_count_bucket"] == "zero"
    assert report["registry_join_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert db.rows[1]["access_state"] == "resolved_not_joined"
    assert db.transaction is not None
    assert db.transaction.committed is False
    assert db.transaction.rolled_back is True


def test_no_forbidden_pipeline_side_effects_are_reported() -> None:
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, _db, _joiner = _run_report(
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    side_effects = report["side_effects"]
    assert side_effects["tdlib_join_called"] is True
    assert side_effects["tdlib_history_fetch_called"] is False
    assert side_effects["live_collector_started"] is False
    assert side_effects["collector_runtime_started"] is False
    assert side_effects["source_messages_written"] is False
    assert side_effects["source_message_versions_written"] is False
    assert side_effects["event_outbox_written"] is False
    assert side_effects["redis_mutation_performed"] is False
    assert side_effects["notifier_transport_enabled"] is False
    assert side_effects["outbox_relay_started"] is False
    assert side_effects["router_normalizer_started"] is False
    assert side_effects["alembic_upgrade_run"] is False
    assert side_effects["alembic_downgrade_run"] is False
    assert side_effects["alembic_stamp_run"] is False
    assert side_effects["docker_or_systemd_changed"] is False


def test_rendered_report_excludes_raw_sensitive_values() -> None:
    joiner = FakeJoiner(
        {
            RAW_CHAT_ID: _join_result(
                "forbidden",
                failure_class="forbidden",
                function_response_types_seen=("error",),
            )
        }
    )

    report, _db, _joiner = _run_report(
        joiner=joiner,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )
    rendered = json.dumps(report, sort_keys=True)

    for forbidden in (
        RAW_PUBLIC_USERNAME,
        RAW_TITLE,
        str(RAW_CHAT_ID),
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_DATABASE_PASSWORD,
        FAKE_TELEGRAM_SECRET,
        RAW_TDLIB_PAYLOAD_VALUE,
        RAW_EXTRA,
        RAW_INVITE_LINK,
        RAW_TEMP_PATH,
    ):
        assert forbidden not in rendered


@pytest.mark.parametrize(
    ("dry_run", "approved_tdlib", "approved_mutation"),
    (
        (True, True, True),
        (False, False, True),
        (False, True, False),
        (False, False, False),
    ),
)
def test_exact_mode_requires_both_existing_approvals_and_non_dry_run(
    dry_run: bool,
    approved_tdlib: bool,
    approved_mutation: bool,
) -> None:
    report, db, _joiner, locator_calls = _run_exact_report(
        dry_run=dry_run,
        approved_tdlib=approved_tdlib,
        approved_mutation=approved_mutation,
    )

    assert report["contract_status"] == "blocked_exact_target_approval_required"
    assert report["target_locator_present"] is True
    assert report["target_locator_read"] is False
    assert locator_calls == []
    assert db.transactions == []
    assert db.update_attempts == 0


def test_exact_locator_selection_is_mutually_exclusive_with_broad_limit() -> None:
    module = _module()
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--runtime-env-path",
                "/safe/unit/runtime.env",
                "--target-locator-path",
                RAW_LOCATOR_PATH,
                "--limit",
                "1",
            ]
        )

    report, db, _joiner, locator_calls = _run_exact_report(limit=1)
    assert report["contract_status"] == "blocked_exact_target_selection_ambiguous"
    assert locator_calls == []
    assert db.transactions == []
    assert db.update_attempts == 0


def test_exact_mode_reuses_strict_locator_reader_and_commits_guarded_current_chat(
) -> None:
    module = _module()
    prefixed_source = f"https://t.me/{RAW_PUBLIC_USERNAME}"
    db = FakeDatabaseConnection(
        [_exact_registry_row(source_value=prefixed_source, chat_id=RAW_CHAT_ID)]
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, joiner, locator_calls = _run_exact_report(db=db, joiner=joiner)

    assert locator_calls == [RAW_LOCATOR_PATH]
    assert report["contract_status"] == "exact_target_join_registry_updated"
    assert report["target_locator_read"] is True
    assert report["exact_target_match_count_bucket"] == "one"
    assert report["exact_target_durable_readback_matched"] is True
    assert joiner is not None
    assert joiner.calls == [RAW_CHAT_ID]
    assert db.rows[0]["access_state"] == "joined"
    assert db.update_attempts == 1
    assert len(db.transactions) == 2
    assert db.transactions[0].rolled_back is True
    assert db.transactions[0].committed is False
    assert db.transactions[1].committed is True

    read_rollback = db.event_log.index("transaction.rollback")
    join_initialize = db.event_log.index("joiner.initialize")
    join_rpc = db.event_log.index("joiner.join_chat")
    exact_update = db.event_log.index("sql.exact_update")
    exact_readback = db.event_log.index("sql.exact_readback")
    mutation_commit = db.event_log.index("transaction.commit")
    assert read_rollback < join_initialize < join_rpc < exact_update
    assert exact_update < exact_readback < mutation_commit

    update_sql = module.UPDATE_EXACT_JOIN_STATE_REGISTRY_ROW_QUERY
    assert "registry_id = :registry_id" in update_sql
    assert "source_kind = 'public_username'" in update_sql
    assert "source_value = :source_value" in update_sql
    assert ":normalized_source_value" in update_sql
    assert "desired_state = 'active'" in update_sql
    assert "access_state = 'resolved_not_joined'" in update_sql
    assert "chat_id = :chat_id" in update_sql
    assert "https://t[.]me/" in module.SELECT_EXACT_TARGET_ROWS_QUERY
    assert "http://t[.]me/" in module.SELECT_EXACT_TARGET_ROWS_QUERY
    assert "t[.]me/" in module.SELECT_EXACT_TARGET_ROWS_QUERY

    update_index = db.statements.index(_normalize(update_sql))
    update_params = db.params[update_index]
    assert update_params["registry_id"] == RAW_REGISTRY_ID
    assert update_params["source_value"] == prefixed_source
    assert update_params["normalized_source_value"] == NORMALIZED_PUBLIC_USERNAME
    assert update_params["chat_id"] == RAW_CHAT_ID

    owner_source = Path(module.__file__).read_text(encoding="utf-8")
    assert "bounded_history_ingest_runner._read_target_locator(" in owner_source
    assert "json.loads(" not in owner_source


@pytest.mark.parametrize(
    ("db", "expected_status"),
    (
        (
            FakeDatabaseConnection([]),
            "blocked_exact_target_missing",
        ),
        (
            FakeDatabaseConnection(
                [
                    _exact_registry_row(),
                    _exact_registry_row(
                        registry_id="22222222-2222-4222-8222-222222222222"
                    ),
                ]
            ),
            "blocked_exact_target_ambiguous",
        ),
        (
            FakeDatabaseConnection(
                [_exact_registry_row()],
                exact_select_override=[
                    {
                        "registry_id": RAW_REGISTRY_ID,
                        "source_value": "DifferentSensitiveChannel",
                        "desired_state": "active",
                        "access_state": "resolved_not_joined",
                        "chat_id": RAW_CHAT_ID,
                    }
                ],
            ),
            "blocked_exact_target_source_mismatch",
        ),
    ),
)
def test_exact_cardinality_or_source_mismatch_blocks_before_tdlib(
    db: FakeDatabaseConnection,
    expected_status: str,
) -> None:
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, joiner, _locator_calls = _run_exact_report(db=db, joiner=joiner)

    assert report["contract_status"] == expected_status
    assert joiner is not None
    assert joiner.initialized is False
    assert joiner.calls == []
    assert len(db.transactions) == 1
    assert db.transactions[0].rolled_back is True
    assert db.update_attempts == 0


@pytest.mark.parametrize(
    "row",
    (
        _exact_registry_row(registry_id="not-a-valid-uuid"),
        _exact_registry_row(chat_id=0),
        _exact_registry_row(access_state="forbidden"),
    ),
)
def test_exact_invalid_row_or_starting_state_blocks_before_tdlib(
    row: dict[str, Any],
) -> None:
    db = FakeDatabaseConnection([row])
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, joiner, _locator_calls = _run_exact_report(db=db, joiner=joiner)

    assert report["contract_status"] in {
        "blocked_exact_target_row_invalid",
        "blocked_exact_target_state_invalid",
    }
    assert joiner is not None
    assert joiner.initialized is False
    assert joiner.calls == []
    assert db.update_attempts == 0


def test_exact_selected_row_source_kind_mismatch_blocks_before_tdlib() -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_select_override=[
            {
                "registry_id": RAW_REGISTRY_ID,
                "source_value": RAW_PUBLIC_USERNAME,
                "source_kind": "invite_link",
                "desired_state": "active",
                "access_state": "resolved_not_joined",
                "chat_id": RAW_CHAT_ID,
            }
        ],
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, joiner, _locator_calls = _run_exact_report(db=db, joiner=joiner)

    assert report["contract_status"] == "blocked_exact_target_row_invalid"
    assert joiner is not None
    assert joiner.initialized is False
    assert joiner.calls == []
    assert db.update_attempts == 0


def test_exact_already_joined_is_idempotent_no_rpc_no_mutation() -> None:
    db = FakeDatabaseConnection([_exact_registry_row(access_state="joined")])
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, joiner, _locator_calls = _run_exact_report(db=db, joiner=joiner)

    assert report["contract_status"] == "exact_target_already_joined_noop"
    assert report["exact_target_noop"] is True
    assert joiner is not None
    assert joiner.initialized is False
    assert joiner.calls == []
    assert db.update_attempts == 0
    assert len(db.transactions) == 1
    assert db.transactions[0].rolled_back is True


@pytest.mark.parametrize(
    "status",
    (
        "join_requested",
        "forbidden",
        "not_found",
        "response_timeout",
        "transport_error",
        "tdlib_error",
        "response_shape_error",
        "authorization_lost",
        "unknown_error",
    ),
)
def test_exact_non_joined_results_never_open_mutation_transaction(
    status: str,
) -> None:
    db = FakeDatabaseConnection([_exact_registry_row()])
    joiner = FakeJoiner(
        {
            RAW_CHAT_ID: _join_result(
                status,
                failure_class=status,
                function_response_types_seen=("error",),
            )
        }
    )

    report, db, joiner, _locator_calls = _run_exact_report(db=db, joiner=joiner)

    assert report["contract_status"] == "exact_target_join_completed_no_mutation"
    assert joiner is not None
    assert joiner.calls == [RAW_CHAT_ID]
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert db.update_attempts == 0
    assert len(db.transactions) == 1
    assert db.transactions[0].rolled_back is True
    assert "sql.exact_update" not in db.event_log


def test_exact_already_participant_canonical_joined_commits() -> None:
    module = _module()
    already_participant = module._join_result_from_tdlib_payload(
        {
            "@type": "error",
            "code": 400,
            "message": "USER_ALREADY_PARTICIPANT",
        },
        response_extra_matched=True,
        function_response_types_seen=("error",),
    )
    assert already_participant.status == "joined"
    db = FakeDatabaseConnection([_exact_registry_row()])
    joiner = FakeJoiner({RAW_CHAT_ID: already_participant})

    report, db, _joiner, _locator_calls = _run_exact_report(
        db=db,
        joiner=joiner,
    )

    assert report["contract_status"] == "exact_target_join_registry_updated"
    assert db.rows[0]["access_state"] == "joined"
    assert db.update_attempts == 1
    assert db.transactions[1].committed is True


@pytest.mark.parametrize("forced_rowcount", (0, 2))
def test_exact_guarded_update_requires_exactly_one_row_and_rolls_back(
    forced_rowcount: int,
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_update_rowcount=forced_rowcount,
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, _joiner, _locator_calls = _run_exact_report(
        db=db,
        joiner=joiner,
    )

    assert report["contract_status"] == "blocked_exact_target_concurrent_mismatch"
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert db.update_attempts == 1
    assert len(db.transactions) == 2
    assert db.transactions[1].committed is False
    assert db.transactions[1].rolled_back is True
    assert "sql.exact_readback" not in db.event_log


def test_exact_durable_readback_mismatch_rolls_back_guarded_update() -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_readback_override=[],
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, db, _joiner, _locator_calls = _run_exact_report(
        db=db,
        joiner=joiner,
    )

    assert report["contract_status"] == "blocked_exact_target_readback_mismatch"
    assert report["exact_target_durable_readback_matched"] is False
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert db.update_attempts == 1
    assert db.transactions[1].committed is False
    assert db.transactions[1].rolled_back is True
    assert db.event_log[-2:] == ["transaction.rollback", "joiner.close"]


def test_exact_success_rerun_is_no_rpc_no_additional_mutation() -> None:
    db = FakeDatabaseConnection([_exact_registry_row()])
    first_joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    first_report, db, _joiner, _locator_calls = _run_exact_report(
        db=db,
        joiner=first_joiner,
    )
    assert first_report["contract_status"] == "exact_target_join_registry_updated"
    assert db.update_attempts == 1

    rerun_joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})
    rerun_report, db, rerun_joiner, _locator_calls = _run_exact_report(
        db=db,
        joiner=rerun_joiner,
    )

    assert rerun_report["contract_status"] == "exact_target_already_joined_noop"
    assert rerun_joiner is not None
    assert rerun_joiner.calls == []
    assert rerun_joiner.initialized is False
    assert db.update_attempts == 1


def test_exact_rendered_report_omits_locator_source_registry_and_chat_values() -> None:
    prefixed_source = f"https://t.me/{RAW_PUBLIC_USERNAME}"
    db = FakeDatabaseConnection(
        [_exact_registry_row(source_value=prefixed_source)]
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    report, _db, _joiner, _locator_calls = _run_exact_report(
        db=db,
        joiner=joiner,
    )
    rendered = _module().render_json(report)

    for forbidden in (
        RAW_LOCATOR_PATH,
        RAW_LOCATOR_CONTENT,
        RAW_PUBLIC_USERNAME,
        NORMALIZED_PUBLIC_USERNAME,
        prefixed_source,
        RAW_REGISTRY_ID,
        str(RAW_CHAT_ID),
        "/safe/unit/runtime.env",
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_TELEGRAM_SECRET,
    ):
        assert forbidden not in rendered


def _assert_cleanup_failure_is_sanitized(value: str) -> None:
    for forbidden in CLEANUP_FAILURE_FORBIDDEN_VALUES:
        assert forbidden not in value
    assert CLEANUP_FAILURE_SENTINEL not in value
    assert "Traceback" not in value


def test_exact_read_rollback_failure_blocks_joiner_and_is_sanitized(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        read_rollback_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    result, db, joiner, _locator_calls = _run_exact_result(db=db, joiner=joiner)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_exact_target_read_rollback_failed"
    )
    assert result.report["exact_target_mutation_outcome"] == "not_attempted"
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "read_rollback_failed"
    ]
    assert result.report["exact_target_read_rollback_succeeded"] is False
    assert result.report["exact_target_mutation_rollback_succeeded"] is None
    assert result.report["exact_target_transport_close_succeeded"] is None
    assert result.report["exact_target_connection_cleanup_succeeded"] is True
    assert joiner is not None
    assert joiner.initialized is False
    assert joiner.calls == []
    assert len(db.transactions) == 1
    assert db.transactions[0].rollback_called is True
    assert db.transactions[0].rollback_succeeded is False
    assert "joiner.initialize" not in db.event_log
    assert db.update_attempts == 0
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_exact_mutation_rollback_failure_after_rowcount_mismatch_is_uncertain(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_update_rowcount=0,
        mutation_rollback_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    result, db, _joiner, _locator_calls = _run_exact_result(db=db, joiner=joiner)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_exact_target_mutation_rollback_failed"
    )
    assert result.report["exact_target_mutation_outcome"] == (
        "unknown_after_rollback_failure"
    )
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "mutation_rollback_failed"
    ]
    assert result.report["exact_target_mutation_rollback_succeeded"] is False
    assert result.report["exact_target_durable_readback_matched"] is False
    assert result.report["registry_join_mutation_performed"] is False
    assert db.transactions[1].commit_called is False
    assert db.transactions[1].rollback_called is True
    assert db.transactions[1].rollback_succeeded is False
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_exact_mutation_rollback_failure_after_readback_mismatch_is_uncertain(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_readback_override=[],
        mutation_rollback_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    result, db, _joiner, _locator_calls = _run_exact_result(db=db, joiner=joiner)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_exact_target_mutation_rollback_failed"
    )
    assert result.report["exact_target_mutation_outcome"] == (
        "unknown_after_rollback_failure"
    )
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "mutation_rollback_failed"
    ]
    assert result.report["exact_target_mutation_rollback_succeeded"] is False
    assert result.report["exact_target_durable_readback_matched"] is False
    assert result.report["registry_join_mutation_performed"] is False
    assert db.transactions[1].commit_called is False
    assert db.transactions[1].rollback_called is True
    assert db.transactions[1].rollback_succeeded is False
    assert db.rows[0]["access_state"] == "joined"
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_exact_joiner_close_failure_before_commit_preserves_rolled_back_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_update_rowcount=0,
    )
    joiner = FakeJoiner(
        {RAW_CHAT_ID: _join_result("joined")},
        fail_close=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )

    result, db, joiner, _locator_calls = _run_exact_result(db=db, joiner=joiner)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_exact_target_concurrent_mismatch"
    )
    assert result.report["exact_target_mutation_outcome"] == "rolled_back"
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "transport_close_failed"
    ]
    assert result.report["exact_target_mutation_rollback_succeeded"] is True
    assert result.report["exact_target_transport_close_succeeded"] is False
    assert joiner is not None
    assert joiner.close_attempted is True
    assert joiner.close_succeeded is False
    assert db.close_attempted is True
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_exact_connection_cleanup_failure_before_commit_is_fixed_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row(access_state="joined")],
        connection_close_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )

    result, db, joiner, _locator_calls = _run_exact_result(db=db, joiner=None)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_exact_target_connection_cleanup_failed"
    )
    assert result.report["exact_target_mutation_outcome"] == "not_attempted"
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "connection_cleanup_failed"
    ]
    assert result.report["exact_target_connection_cleanup_succeeded"] is False
    assert joiner is None
    assert db.close_attempted is True
    assert db.close_succeeded is False
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_exact_joiner_close_failure_after_commit_preserves_durable_mutation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection([_exact_registry_row()])
    joiner = FakeJoiner(
        {RAW_CHAT_ID: _join_result("joined")},
        fail_close=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )

    result, db, joiner, _locator_calls = _run_exact_result(db=db, joiner=joiner)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "exact_target_cleanup_failed_after_commit"
    )
    assert result.report["exact_target_mutation_outcome"] == "committed_durable"
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "transport_close_failed"
    ]
    assert result.report["exact_target_durable_readback_matched"] is True
    assert result.report["registry_join_mutation_performed"] is True
    assert result.report["side_effects"]["database_mutation_performed"] is True
    assert result.report["side_effects"]["telegram_channel_registry_updated"] is True
    assert db.transactions[1].commit_called is True
    assert db.transactions[1].committed is True
    assert db.transactions[1].rollback_called is False
    assert db.rows[0]["access_state"] == "joined"
    assert joiner is not None
    assert joiner.close_attempted is True
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_production_joiner_close_failure_reaches_exact_postcommit_cleanup_state(
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    resolver_module = _resolver_module()
    db = FakeDatabaseConnection([_exact_registry_row()])
    joiner, client = _production_joiner_with_injected_close(
        _join_result("joined"),
        close_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
        event_log=db.event_log,
    )

    result, db, returned_joiner, _locator_calls = _run_exact_result(
        db=db,
        joiner=joiner,
    )
    stdout, stderr = capsys.readouterr()

    assert returned_joiner is joiner
    assert type(joiner) is module.TDLibResolvedNotJoinedJoiner
    assert joiner.close.__func__ is module.TDLibResolvedNotJoinedJoiner.close
    assert (
        joiner._base.close.__func__
        is resolver_module.TDLibPublicUsernameResolver.close
    )
    assert joiner.calls == [RAW_CHAT_ID]
    assert client.close_attempted is True
    assert client.close_succeeded is False
    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "exact_target_cleanup_failed_after_commit"
    )
    assert result.report["exact_target_mutation_outcome"] == "committed_durable"
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "transport_close_failed"
    ]
    assert result.report["exact_target_transport_close_succeeded"] is False
    assert result.report["exact_target_durable_readback_matched"] is True
    assert result.report["registry_join_mutation_performed"] is True
    assert result.report["side_effects"]["database_mutation_performed"] is True
    assert result.report["side_effects"]["telegram_channel_registry_updated"] is True
    assert db.transactions[1].commit_called is True
    assert db.transactions[1].committed is True
    assert db.transactions[1].rollback_called is False
    assert db.rows[0]["access_state"] == "joined"
    assert db.event_log.index("transaction.commit") < db.event_log.index(
        "production_client.close"
    )
    assert stdout == ""
    assert stderr == ""
    rendered = json.dumps(result.report, sort_keys=True)
    _assert_cleanup_failure_is_sanitized(rendered + stdout + stderr)
    assert "RuntimeError" not in rendered + stdout + stderr
    assert "Traceback" not in rendered + stdout + stderr


def test_exact_connection_cleanup_failure_after_commit_preserves_durable_mutation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        connection_close_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )
    joiner = FakeJoiner({RAW_CHAT_ID: _join_result("joined")})

    result, db, _joiner, _locator_calls = _run_exact_result(db=db, joiner=joiner)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "exact_target_cleanup_failed_after_commit"
    )
    assert result.report["exact_target_mutation_outcome"] == "committed_durable"
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "connection_cleanup_failed"
    ]
    assert result.report["exact_target_durable_readback_matched"] is True
    assert result.report["registry_join_mutation_performed"] is True
    assert result.report["side_effects"]["database_mutation_performed"] is True
    assert result.report["side_effects"]["telegram_channel_registry_updated"] is True
    assert db.transactions[1].commit_called is True
    assert db.transactions[1].committed is True
    assert db.transactions[1].rollback_called is False
    assert db.rows[0]["access_state"] == "joined"
    assert db.close_attempted is True
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_exact_multiple_cleanup_failures_are_collected_in_deterministic_order(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_update_rowcount=0,
        mutation_rollback_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
        connection_close_failure=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )
    joiner = FakeJoiner(
        {RAW_CHAT_ID: _join_result("joined")},
        fail_close=RuntimeError(CLEANUP_FAILURE_SENTINEL),
    )

    result, db, joiner, _locator_calls = _run_exact_result(db=db, joiner=joiner)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_exact_target_mutation_rollback_failed"
    )
    assert result.report["exact_target_mutation_outcome"] == (
        "unknown_after_rollback_failure"
    )
    assert result.report["exact_target_cleanup_failure_codes"] == [
        "mutation_rollback_failed",
        "transport_close_failed",
        "connection_cleanup_failed",
    ]
    assert result.report["exact_target_mutation_rollback_succeeded"] is False
    assert result.report["exact_target_transport_close_succeeded"] is False
    assert result.report["exact_target_connection_cleanup_succeeded"] is False
    assert joiner is not None
    assert joiner.close_attempted is True
    assert db.close_attempted is True
    rollback_indexes = [
        index
        for index, event in enumerate(db.event_log)
        if event == "transaction.rollback"
    ]
    assert len(rollback_indexes) == 2
    assert rollback_indexes[1] < db.event_log.index("joiner.close")
    assert db.event_log.index("joiner.close") < db.event_log.index("connection.close")
    assert capsys.readouterr() == ("", "")
    _assert_cleanup_failure_is_sanitized(json.dumps(result.report, sort_keys=True))


def test_exact_cli_emergency_firewall_emits_one_sanitized_json_object(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()

    def raise_unhandled_failure(**_kwargs: Any) -> Any:
        raise RuntimeError(CLEANUP_FAILURE_SENTINEL)

    monkeypatch.setattr(module, "generate_report", raise_unhandled_failure)

    exit_code = module.main(
        [
            "--runtime-env-path",
            "/safe/unit/runtime.env",
            "--target-locator-path",
            "/private/exact-target-locator.json",
            "--approved-tdlib-join-resolved-not-joined",
            "--approved-registry-join-mutation",
        ]
    )
    stdout, stderr = capsys.readouterr()
    report = json.loads(stdout)

    assert exit_code == 1
    assert stdout == module.render_json(report) + "\n"
    assert stderr == ""
    assert report["contract_status"] == "blocked_exact_target_unhandled_failure"
    assert report["checks_failed"] == ["exact_target.unhandled_failure"]
    assert report["exact_target_mutation_outcome"] == "not_attempted"
    assert report["exact_target_cleanup_failure_codes"] == []
    _assert_cleanup_failure_is_sanitized(stdout + stderr)


def test_two_hop_exact_rebind_then_rejoin_uses_one_source_bound_durable_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    join_module = _module()
    resolve_module = _resolver_module()
    rebound_chat_id = RAW_CHAT_ID + 101
    unrelated_chat_id = RAW_CHAT_ID + 202
    locator_message_id = 919191919191
    unrelated_registry_id = "22222222-2222-4222-8222-222222222222"
    stored_source_value = f"https://t.me/{RAW_PUBLIC_USERNAME}"
    initial_rows = [
        _exact_registry_row(
            source_value=stored_source_value,
            access_state="joined",
            chat_id=RAW_CHAT_ID,
        ),
        _exact_registry_row(
            registry_id=unrelated_registry_id,
            source_value="UnrelatedSensitiveChannel",
            access_state="joined",
            chat_id=unrelated_chat_id,
        ),
    ]
    unrelated_before = copy.deepcopy(initial_rows[1])

    class TwoHopFakeDatabaseConnection(FakeDatabaseConnection):
        def execute(
            self,
            statement: str,
            params: dict[str, Any] | None = None,
        ) -> FakeResult:
            params = params or {}
            normalized = _normalize(statement)

            if normalized == _normalize(resolve_module.SELECT_EXACT_TARGET_ROWS_QUERY):
                self.statements.append(normalized)
                self.params.append(dict(params))
                self.event_log.append("sql.exact_resolve_select")
                rows = [
                    row
                    for row in self.rows
                    if row["source_kind"] == "public_username"
                    and row["desired_state"] == "active"
                    and resolve_module._normalize_exact_source_identity(
                        row["source_value"]
                    )
                    == params["normalized_source_value"]
                ][:2]
                return FakeResult(
                    rows=[
                        {
                            "registry_id": row["registry_id"],
                            "source_value": row["source_value"],
                            "source_kind": row["source_kind"],
                            "desired_state": row["desired_state"],
                            "access_state": row["access_state"],
                            "chat_id": row["chat_id"],
                        }
                        for row in rows
                    ]
                )

            if normalized == _normalize(
                resolve_module.UPDATE_EXACT_JOINED_REGISTRY_ROW_QUERY
            ):
                self.statements.append(normalized)
                self.params.append(dict(params))
                self.event_log.append("sql.exact_resolve_update")
                self.update_attempts += 1
                for row in self.rows:
                    if (
                        row["registry_id"] == params["registry_id"]
                        and row["source_kind"] == "public_username"
                        and row["source_value"] == params["source_value"]
                        and resolve_module._normalize_exact_source_identity(
                            row["source_value"]
                        )
                        == params["normalized_source_value"]
                        and row["desired_state"] == "active"
                        and row["access_state"] == "joined"
                        and row["chat_id"] == params["old_chat_id"]
                    ):
                        row["chat_id"] = params["chat_id"]
                        row["username_snapshot"] = params["username_snapshot"]
                        if params["title_snapshot"] is not None:
                            row["title_snapshot"] = params["title_snapshot"]
                        if params["chat_type"] is not None:
                            row["chat_type"] = params["chat_type"]
                        row["last_resolved_at"] = params["resolved_at"]
                        row["access_state"] = "resolved_not_joined"
                        row["updated_at"] = params["resolved_at"]
                        self.updated_rows += 1
                        return FakeResult(rowcount=1)
                return FakeResult(rowcount=0)

            if normalized == _normalize(
                resolve_module.SELECT_EXACT_RESOLVED_READBACK_QUERY
            ):
                self.statements.append(normalized)
                self.params.append(dict(params))
                self.event_log.append("sql.exact_resolve_readback")
                return FakeResult(
                    rows=[
                        {
                            "registry_id": row["registry_id"],
                            "source_value": row["source_value"],
                            "source_kind": row["source_kind"],
                            "desired_state": row["desired_state"],
                            "access_state": row["access_state"],
                            "chat_id": row["chat_id"],
                        }
                        for row in self.rows
                        if row["registry_id"] == params["registry_id"]
                        and row["source_kind"] == "public_username"
                        and row["source_value"] == params["source_value"]
                        and resolve_module._normalize_exact_source_identity(
                            row["source_value"]
                        )
                        == params["normalized_source_value"]
                        and row["desired_state"] == "active"
                        and row["access_state"] == "resolved_not_joined"
                        and row["chat_id"] == params["chat_id"]
                    ]
                )

            return super().execute(statement, params)

    class TwoHopFakeResolver:
        def __init__(self) -> None:
            self.initialized = False
            self.closed = False
            self.calls: list[str] = []

        async def initialize(self) -> None:
            self.initialized = True

        async def resolve_public_username(self, username: str) -> Any:
            self.calls.append(username)
            return resolve_module.PublicUsernameResolveResult(
                status="resolved",
                chat_id=rebound_chat_id,
                username_snapshot=RAW_PUBLIC_USERNAME,
                title_snapshot=RAW_TITLE,
                chat_type="channel",
                function_response_types_seen=("chat",),
                response_extra_matched=True,
            )

        async def close(self) -> None:
            self.closed = True

    locator_calls: list[object] = []

    def strict_locator_reader(value: object | None) -> dict[str, Any]:
        locator_calls.append(value)
        return {
            "source_value": NORMALIZED_PUBLIC_USERNAME,
            "target_message_id": locator_message_id,
        }

    monkeypatch.setattr(
        join_module.bounded_history_ingest_runner,
        "_read_target_locator",
        strict_locator_reader,
    )

    resolver_db = TwoHopFakeDatabaseConnection(copy.deepcopy(initial_rows))
    resolver = TwoHopFakeResolver()
    resolve_result = resolve_module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        target_locator_path=RAW_LOCATOR_PATH,
        dry_run=False,
        approved_tdlib_public_username_resolve=True,
        approved_registry_resolve_mutation=True,
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: resolver_db,
        public_username_resolver_factory=lambda _values: resolver,
    )

    assert resolve_result.exit_code == 0, resolve_result.report
    assert resolve_result.report["contract_status"] == (
        "exact_target_resolved_not_joined_updated"
    )
    assert resolve_result.report["exact_target_match_count_bucket"] == "one"
    assert resolver.initialized is True
    assert resolver.calls == [NORMALIZED_PUBLIC_USERNAME]
    assert resolver_db.update_attempts == 1
    resolve_update_index = resolver_db.statements.index(
        _normalize(resolve_module.UPDATE_EXACT_JOINED_REGISTRY_ROW_QUERY)
    )
    resolve_update_params = resolver_db.params[resolve_update_index]
    assert resolve_update_params["registry_id"] == RAW_REGISTRY_ID
    assert resolve_update_params["source_value"] == stored_source_value
    assert resolve_update_params["normalized_source_value"] == (
        NORMALIZED_PUBLIC_USERNAME
    )
    assert resolve_update_params["old_chat_id"] == RAW_CHAT_ID
    assert resolve_update_params["chat_id"] == rebound_chat_id

    join_db = TwoHopFakeDatabaseConnection(resolver_db.rows)
    joiner = FakeJoiner({rebound_chat_id: _join_result("joined")})
    join_result = join_module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        target_locator_path=RAW_LOCATOR_PATH,
        dry_run=False,
        approved_tdlib_join_resolved_not_joined=True,
        approved_registry_join_mutation=True,
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: join_db,
        resolved_not_joined_joiner_factory=lambda _values: joiner,
    )

    assert join_result.exit_code == 0
    assert join_result.report["contract_status"] == "exact_target_join_registry_updated"
    assert join_result.report["exact_target_match_count_bucket"] == "one"
    assert joiner.calls == [rebound_chat_id]
    assert join_db.update_attempts == 1
    join_update_index = join_db.statements.index(
        _normalize(join_module.UPDATE_EXACT_JOIN_STATE_REGISTRY_ROW_QUERY)
    )
    join_update_params = join_db.params[join_update_index]
    assert join_update_params["registry_id"] == RAW_REGISTRY_ID
    assert join_update_params["source_value"] == stored_source_value
    assert join_update_params["normalized_source_value"] == NORMALIZED_PUBLIC_USERNAME
    assert join_update_params["chat_id"] == rebound_chat_id

    target_row = next(
        row for row in join_db.rows if row["registry_id"] == RAW_REGISTRY_ID
    )
    assert target_row["desired_state"] == "active"
    assert target_row["access_state"] == "joined"
    assert target_row["chat_id"] == rebound_chat_id
    unrelated_after = next(
        row for row in join_db.rows if row["registry_id"] == unrelated_registry_id
    )
    assert unrelated_after == unrelated_before

    rerun_db = TwoHopFakeDatabaseConnection(join_db.rows)
    rerun_joiner = FakeJoiner({rebound_chat_id: _join_result("joined")})
    rerun_result = join_module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        target_locator_path=RAW_LOCATOR_PATH,
        dry_run=False,
        approved_tdlib_join_resolved_not_joined=True,
        approved_registry_join_mutation=True,
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: rerun_db,
        resolved_not_joined_joiner_factory=lambda _values: rerun_joiner,
    )

    assert rerun_result.exit_code == 0
    assert rerun_result.report["contract_status"] == "exact_target_already_joined_noop"
    assert rerun_joiner.initialized is False
    assert rerun_joiner.calls == []
    assert rerun_db.update_attempts == 0
    assert resolver_db.update_attempts + join_db.update_attempts + rerun_db.update_attempts == 2
    assert locator_calls == [RAW_LOCATOR_PATH, RAW_LOCATOR_PATH, RAW_LOCATOR_PATH]

    rendered_reports = json.dumps(
        [resolve_result.report, join_result.report, rerun_result.report],
        sort_keys=True,
    )
    for forbidden in (
        RAW_LOCATOR_PATH,
        RAW_LOCATOR_CONTENT,
        RAW_PUBLIC_USERNAME,
        NORMALIZED_PUBLIC_USERNAME,
        RAW_TITLE,
        stored_source_value,
        RAW_REGISTRY_ID,
        unrelated_registry_id,
        str(RAW_CHAT_ID),
        str(rebound_chat_id),
        str(unrelated_chat_id),
        str(locator_message_id),
        "/safe/unit/runtime.env",
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_TELEGRAM_SECRET,
    ):
        assert forbidden not in rendered_reports
