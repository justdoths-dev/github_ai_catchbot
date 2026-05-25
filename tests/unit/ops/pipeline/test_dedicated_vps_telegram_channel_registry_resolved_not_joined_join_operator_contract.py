from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any


FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-for-resolved-not-joined-join@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-secret-resolved-not-joined-join@127.0.0.1:6379/0"
FAKE_DATABASE_PASSWORD = "unit-db-password-for-resolved-not-joined-join"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-resolved-not-joined-join"
RAW_PUBLIC_USERNAME = "SensitiveAlphaChannel"
RAW_TITLE = "Sensitive Alpha Channel Title"
RAW_CHAT_ID = 9876543210123
RAW_TDLIB_PAYLOAD_VALUE = "unit-raw-tdlib-payload-value-resolved-not-joined-join"
RAW_INVITE_LINK = "https://t.me/+sensitiveInviteLinkForJoinOperator"
RAW_EXTRA = "raw-extra-join-operator"
RAW_TEMP_PATH = "/tmp/sensitive-join-operator-path"


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
    def __init__(self, db: "FakeDatabaseConnection") -> None:
        self._db = db
        self._snapshot = copy.deepcopy(db.rows)
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
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
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available
        self.fail_select_1 = fail_select_1
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.closed = False
        self.transaction: FakeTransaction | None = None
        self.update_attempts = 0
        self.updated_rows = 0

    def begin(self) -> FakeTransaction:
        self.transaction = FakeTransaction(self)
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

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
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


class FakeJoiner:
    def __init__(
        self,
        responses: dict[int, Any],
        *,
        fail_initialize: Exception | None = None,
    ) -> None:
        self.responses = responses
        self.fail_initialize = fail_initialize
        self.initialized = False
        self.closed = False
        self.calls: list[int] = []
        self.tdlib_send_called = False
        self.tdlib_receive_called = False
        self.history_called = False

    async def initialize(self) -> None:
        if self.fail_initialize is not None:
            raise self.fail_initialize
        self.initialized = True

    async def join_chat(self, chat_id: int) -> Any:
        self.calls.append(chat_id)
        self.tdlib_send_called = True
        response = self.responses.get(chat_id)
        if isinstance(response, Exception):
            raise response
        self.tdlib_receive_called = True
        return response

    async def close(self) -> None:
        self.closed = True


def _module():
    from scripts.ops import (
        dedicated_vps_telegram_channel_registry_resolved_not_joined_join_operator
        as module,
    )

    return module


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
