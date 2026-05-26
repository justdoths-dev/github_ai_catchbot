from __future__ import annotations

from pathlib import Path
from typing import Any


FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-history-probe@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-secret-history-probe@127.0.0.1:6379/0"
FAKE_DATABASE_PASSWORD = "unit-db-password-history-probe"
FAKE_REDIS_PASSWORD = "unit-redis-secret-history-probe"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-history-probe"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_ID = 444555666
RAW_SOURCE_VALUE = "SensitiveHistoryProbeChannel"
RAW_USERNAME = "SensitiveHistoryProbeUsername"
RAW_TITLE = "Sensitive History Probe Channel Title"
RAW_INVITE_LINK = "https://t.me/+sensitiveInviteLinkForHistoryProbe"
RAW_TDLIB_PAYLOAD_VALUE = "unit-raw-tdlib-payload-value-history-probe"
RAW_TEMP_PATH = "/tmp/sensitive-history-probe-path"
RAW_PHONE = "+15555550125"


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

        forbidden_sql = (" INSERT ", " UPDATE ", " DELETE ")
        padded = f" {normalized.upper()} "
        assert not any(marker in padded for marker in forbidden_sql), statement

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
            result = _module().HistoryChatProbeResult(status="empty")
        if isinstance(result, Exception):
            raise result
        return result

    async def close(self) -> None:
        self.closed = True


def _module():
    from scripts.ops import (
        dedicated_vps_joined_channel_collector_bounded_history_message_availability_probe
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
        "HISTORY_PROBE_TEMP_PATH": RAW_TEMP_PATH,
        "HISTORY_PROBE_INVITE_LINK": RAW_INVITE_LINK,
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


def _run_report(
    tmp_path: Path,
    *,
    db: FakeDatabaseConnection | None = None,
    approved_tdlib: bool = False,
    approved_history: bool = False,
    probe: FakeHistoryProbe | None = None,
    history_probe_max_chats: int = 3,
    history_probe_history_limit: int = 3,
    runtime_env_reader: Any | None = None,
) -> tuple[dict[str, Any], FakeDatabaseConnection, FakeHistoryProbe | None]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        approved_tdlib_readiness_probe=approved_tdlib,
        approved_history_message_availability_probe=approved_history,
        history_probe_max_chats=history_probe_max_chats,
        history_probe_history_limit=history_probe_history_limit,
        runtime_env_reader=runtime_env_reader or _runtime_env_reader(tmp_path),
        database_connection_factory=lambda _database_url: fake_db,
        history_probe_factory=(
            (lambda _values, _max, _timeout, _overall: probe)
            if probe is not None
            else None
        ),
    )
    return result.report, fake_db, probe


def _message_text_result() -> Any:
    return _module().HistoryChatProbeResult(
        status="history",
        message_count=1,
        message_bearing_count=1,
        content_type_counts=(("messageText", 1),),
    )


def test_default_mode_does_not_call_get_chat_history_or_mutate_db(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_message_text_result()])

    report, db, probe = _run_report(tmp_path, probe=probe)

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_availability_ready"
    )
    assert report["history_probe_approved"] is False
    assert report["history_probe_attempted"] is False
    assert report["tdlib_readiness_probe_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert report["database_mutation_performed"] is False
    assert report["redis_mutation_performed"] is False
    assert db.transaction is not None
    assert db.transaction.rolled_back is True
    assert probe is not None
    assert probe.initialized is False
    assert probe.fetch_calls == []


def test_history_probe_flag_without_tdlib_readiness_approval_blocks(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_message_text_result()])

    report, _db, probe = _run_report(
        tmp_path,
        approved_tdlib=False,
        approved_history=True,
        probe=probe,
    )

    assert report["contract_status"] == (
        "blocked_history_message_availability_probe_not_ready"
    )
    assert "approval.tdlib_readiness_probe_required" in report["checks_failed"]
    assert report["history_probe_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert probe is not None
    assert probe.initialized is False


def test_tdlib_not_ready_blocks_before_history_fetch(tmp_path: Path) -> None:
    probe = FakeHistoryProbe(
        results=[_message_text_result()],
        status="not_ready",
        final_authorization_state="authorizationStateWaitCode",
    )

    report, _db, probe = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        probe=probe,
    )

    assert report["contract_status"] == (
        "blocked_history_message_availability_probe_not_ready"
    )
    assert "tdlib.not_ready" in report["checks_failed"]
    assert report["tdlib_ready_probe_final_authorization_state"] == (
        "authorizationStateWaitCode"
    )
    assert report["history_probe_attempted"] is False
    assert report["tdlib_history_fetch_called"] is False
    assert probe is not None
    assert probe.fetch_calls == []
    assert probe.closed is True


def test_joined_row_absence_blocks_without_tdlib(tmp_path: Path) -> None:
    db = FakeDatabaseConnection(
        [_registry_row("registry-unjoined", access_state="resolved_not_joined")]
    )
    probe = FakeHistoryProbe(results=[_message_text_result()])

    report, _db, probe = _run_report(
        tmp_path,
        db=db,
        approved_tdlib=True,
        approved_history=True,
        probe=probe,
    )

    assert report["contract_status"] == (
        "blocked_history_message_availability_probe_not_ready"
    )
    assert "registry.no_active_joined_rows" in report["checks_failed"]
    assert report["joined_row_count_bucket"] == "zero"
    assert report["tdlib_readiness_probe_attempted"] is False
    assert probe is not None
    assert probe.initialized is False


def test_valid_approved_history_probe_observes_fake_message_text(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_message_text_result()])

    report, _db, probe = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        probe=probe,
        history_probe_history_limit=2,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_availability_observed"
    )
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["history_probe_attempted"] is True
    assert report["history_probe_status"] == "observed"
    assert report["history_probe_selected_chats_bucket"] == "one"
    assert report["history_probe_get_chat_history_requests_bucket"] == "one"
    assert report["history_probe_chats_with_history_bucket"] == "one"
    assert report["history_probe_message_bearing_messages_observed_bucket"] == "one"
    assert report["history_probe_content_type_buckets"] == {"messageText": "one"}
    assert report["history_probe_message_bearing_observed"] is True
    assert report["database_mutation_performed"] is False
    assert report["redis_mutation_performed"] is False
    assert report["source_messages_written"] is False
    assert report["event_outbox_written"] is False
    assert report["tdlib_history_fetch_called"] is True
    assert probe is not None
    assert probe.fetch_calls == [{"chat_id": RAW_CHAT_ID, "limit": 2}]
    assert probe.closed is True


def test_empty_histories_return_no_messages_observed(tmp_path: Path) -> None:
    db = FakeDatabaseConnection(
        [
            _registry_row("registry-1", chat_id=RAW_CHAT_ID),
            _registry_row("registry-2", chat_id=RAW_CHAT_ID + 1, priority_weight=90),
        ]
    )
    probe = FakeHistoryProbe(
        results=[
            _module().HistoryChatProbeResult(status="empty"),
            _module().HistoryChatProbeResult(status="empty"),
        ]
    )

    report, _db, probe = _run_report(
        tmp_path,
        db=db,
        approved_tdlib=True,
        approved_history=True,
        probe=probe,
        history_probe_max_chats=2,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_availability_no_messages_observed"
    )
    assert report["history_probe_status"] == "no_messages_observed"
    assert report["history_probe_selected_chats_bucket"] == "two_to_five"
    assert report["history_probe_get_chat_history_requests_bucket"] == "two_to_five"
    assert report["history_probe_empty_history_chats_bucket"] == "two_to_five"
    assert report["history_probe_message_bearing_observed"] is False
    assert probe is not None
    assert len(probe.fetch_calls) == 2


def test_access_denied_per_chat_increments_bucket_and_leaks_no_identifiers(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(
        results=[_module().HistoryChatProbeResult(status="access_denied")]
    )

    report, _db, _probe = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        probe=probe,
    )
    rendered = _module().render_json(report)

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_history_message_availability_no_messages_observed"
    )
    assert report["history_probe_access_denied_bucket"] == "one"
    assert str(RAW_CHAT_ID) not in rendered
    assert RAW_USERNAME not in rendered
    assert RAW_SOURCE_VALUE not in rendered
    assert RAW_TITLE not in rendered
    assert RAW_INVITE_LINK not in rendered


def test_forbidden_tdlib_request_side_effects_block(tmp_path: Path) -> None:
    forbidden_cases = {
        "setAuthenticationPhoneNumber": "tdlib_auth_attempted",
        "checkAuthenticationCode": "tdlib_code_submitted",
        "checkAuthenticationPassword": "tdlib_password_submitted",
        "joinChat": "tdlib_join_called",
        "joinChatByInviteLink": "tdlib_join_called",
        "searchPublicChat": "tdlib_search_public_chat_called",
        "sendMessage": "tdlib_send_message_called",
        "getMessageLink": "tdlib_get_message_link_called",
    }

    for request_type, flag in forbidden_cases.items():
        probe = FakeHistoryProbe(
            results=[_message_text_result()],
            readiness_request_types_sent=["getAuthorizationState", request_type],
        )

        report, _db, _probe = _run_report(
            tmp_path,
            approved_tdlib=True,
            approved_history=True,
            probe=probe,
        )

        assert report["contract_status"] == "blocked_forbidden_side_effect_detected"
        assert report[flag] is True
        assert report["history_probe_attempted"] is False


def test_forbidden_request_reported_during_history_fetch_blocks(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(
        results=[
            _module().HistoryChatProbeResult(
                status="history",
                message_count=1,
                message_bearing_count=1,
                content_type_counts=(("messageText", 1),),
                request_types_sent=("getChatHistory", "sendMessage"),
            )
        ]
    )

    report, _db, _probe = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
        probe=probe,
    )

    assert report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert report["tdlib_history_fetch_called"] is True
    assert report["tdlib_send_message_called"] is True
    assert report["database_mutation_performed"] is False


def test_output_redaction_excludes_sensitive_runtime_and_tdlib_values(
    tmp_path: Path,
) -> None:
    probe = FakeHistoryProbe(results=[_message_text_result()])

    report, _db, _probe = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_history=True,
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
    ]
    for raw_value in forbidden_values:
        assert raw_value not in rendered
