from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_telegram_channel_registry_public_username_resolve_operator.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-for-public-username-resolve@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_DATABASE_PASSWORD = "unit-db-password-for-public-username-resolve"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-public-username-resolve"
RAW_PUBLIC_USERNAME = "PrivateAlphaChannel"
RAW_PUBLIC_USERNAME_TWO = "PrivateBetaChannel"
RAW_PUBLIC_USERNAME_THREE = "PrivateGammaChannel"
RAW_CHAT_ID = 9876543210123
READY_PROBE_FIELD_NAMES = (
    "tdlib_ready_probe_attempted",
    "tdlib_ready_probe_status",
    "tdlib_ready_probe_observation_count_bucket",
    "tdlib_ready_probe_request_types_sent",
    "tdlib_ready_probe_update_types_seen",
    "tdlib_ready_probe_authorization_states_seen",
    "tdlib_ready_probe_final_authorization_state",
    "tdlib_ready_probe_error_class",
    "tdlib_ready_probe_error_code",
    "tdlib_ready_probe_manual_intervention_required",
)


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
        table_available: bool = True,
        fail_select_1: bool = False,
        break_update_guard_for: set[str] | None = None,
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available
        self.fail_select_1 = fail_select_1
        self.break_update_guard_for = break_update_guard_for or set()
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.closed = False
        self.transaction = FakeTransaction()
        self.update_attempts = 0
        self.updated_rows = 0

    def begin(self) -> FakeTransaction:
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
                        "source_value": row["source_value"],
                    }
                    for row in rows
                ]
            )

        if normalized == _normalize(module.UPDATE_RESOLVED_REGISTRY_ROW_QUERY):
            self.update_attempts += 1
            registry_id = params["registry_id"]
            if registry_id in self.break_update_guard_for:
                for row in self.rows:
                    if row["registry_id"] == registry_id:
                        row["access_state"] = "forbidden"
            for row in self.rows:
                if (
                    row["registry_id"] == registry_id
                    and row["source_kind"] == "public_username"
                    and row["desired_state"] == "active"
                    and row["access_state"] == "unresolved"
                    and row["chat_id"] is None
                ):
                    row["chat_id"] = params["chat_id"]
                    row["username_snapshot"] = params["username_snapshot"]
                    row["title_snapshot"] = params["title_snapshot"]
                    row["chat_type"] = params["chat_type"]
                    row["last_resolved_at"] = params["resolved_at"]
                    row["access_state"] = "resolved_not_joined"
                    row["updated_at"] = params["resolved_at"]
                    self.updated_rows += 1
                    return FakeResult(rowcount=1)
            return FakeResult(rowcount=0)

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.closed = True

    def _target_rows(self) -> list[dict[str, Any]]:
        return [
            row
            for row in self.rows
            if row["source_kind"] == "public_username"
            and row["desired_state"] == "active"
            and row["access_state"] == "unresolved"
            and row["chat_id"] is None
        ]


class FakeResolver:
    def __init__(
        self,
        responses: dict[str, Any],
        *,
        fail_initialize: Exception | None = None,
    ) -> None:
        self.responses = responses
        self.fail_initialize = fail_initialize
        self.initialized = False
        self.closed = False
        self.calls: list[str] = []
        self.auth_code_called = False
        self.auth_password_called = False
        self.join_called = False
        self.history_called = False

    async def initialize(self) -> None:
        if self.fail_initialize is not None:
            raise self.fail_initialize
        self.initialized = True

    async def resolve_public_username(self, username: str) -> Any:
        self.calls.append(username)
        response = self.responses.get(username)
        if isinstance(response, Exception):
            raise response
        return response

    async def close(self) -> None:
        self.closed = True


TransportPayload = dict[str, Any] | None
TransportPayloadFactory = Any


class FakeTDLibTransport:
    def __init__(self, payloads: list[TransportPayload | TransportPayloadFactory]) -> None:
        self.payloads = list(payloads)
        self.initialized = False
        self.closed = False
        self.sent_requests: list[dict[str, Any]] = []
        self.receive_timeouts: list[float] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def send(self, request: dict[str, Any]) -> None:
        self.sent_requests.append(request)

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        self.receive_timeouts.append(timeout)
        if not self.payloads:
            return None
        payload = self.payloads.pop(0)
        if callable(payload):
            payload = payload(self)
        return payload

    async def close(self) -> None:
        self.closed = True


def _module():
    from scripts.ops import (
        dedicated_vps_telegram_channel_registry_public_username_resolve_operator as module,
    )

    return module


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "TELEGRAM_API_HASH": FAKE_TELEGRAM_SECRET,
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_PHONE_NUMBER": "+10000000000",
        "TDLIB_DB_ENCRYPTION_KEY": "fake-tdlib-key",
        "TDLIB_STATE_DIR": "/safe/unit/tdlib-state",
        "TDLIB_FILES_DIR": "/safe/unit/tdlib-files",
    }


def _runtime_env_for_tdlib_transport(tmp_path: Path) -> dict[str, str]:
    values = _runtime_env("/safe/unit/runtime.env")
    values["TDLIB_STATE_DIR"] = str(tmp_path / "tdlib-state")
    values["TDLIB_FILES_DIR"] = str(tmp_path / "tdlib-files")
    values["COLLECTOR_SINGLETON_LOCK_PATH"] = str(tmp_path / "tdlib-state" / "collector.lock")
    return values


def _registry_row(
    registry_id: str,
    source_value: str = RAW_PUBLIC_USERNAME,
    *,
    source_kind: str = "public_username",
    desired_state: str = "active",
    access_state: str = "unresolved",
    chat_id: int | None = None,
) -> dict[str, Any]:
    return {
        "registry_id": registry_id,
        "source_kind": source_kind,
        "source_value": source_value,
        "desired_state": desired_state,
        "access_state": access_state,
        "chat_id": chat_id,
        "username_snapshot": None,
        "title_snapshot": None,
        "chat_type": None,
        "last_resolved_at": None,
        "updated_at": None,
        "priority_weight": 100,
    }


def _resolved(
    *,
    chat_id: int = RAW_CHAT_ID,
    username_snapshot: str = "ResolvedAlphaChannel",
    title_snapshot: str = "Resolved Alpha",
    chat_type: str = "channel",
) -> Any:
    return _module().PublicUsernameResolveResult(
        status="resolved",
        chat_id=chat_id,
        username_snapshot=username_snapshot,
        title_snapshot=title_snapshot,
        chat_type=chat_type,
    )


def _not_found() -> Any:
    return _module().PublicUsernameResolveResult(status="not_found")


def _unsupported() -> Any:
    return _module().PublicUsernameResolveResult(status="unsupported_chat_type")


def _run_report(
    *,
    db: FakeDatabaseConnection | None = None,
    resolver: FakeResolver | None = None,
    dry_run: bool = True,
    approved_tdlib: bool = False,
    approved_mutation: bool = False,
    runtime_env_reader=_runtime_env,
    limit: int | None = None,
) -> tuple[dict[str, Any], FakeDatabaseConnection, FakeResolver | None]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    fake_resolver = resolver
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        dry_run=dry_run,
        approved_tdlib_public_username_resolve=approved_tdlib,
        approved_registry_resolve_mutation=approved_mutation,
        limit=limit,
        runtime_env_reader=runtime_env_reader,
        database_connection_factory=lambda _database_url: fake_db,
        public_username_resolver_factory=(
            (lambda _values: fake_resolver) if fake_resolver is not None else None
        ),
    )
    return result.report, fake_db, fake_resolver


def _run_report_with_tdlib_transport(
    *,
    tmp_path: Path,
    transport: FakeTDLibTransport,
    db: FakeDatabaseConnection | None = None,
    approved_mutation: bool = False,
) -> tuple[dict[str, Any], FakeDatabaseConnection, Any]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    resolver_holder: dict[str, Any] = {}

    def resolver_factory(values: dict[str, str]) -> Any:
        resolver = module.TDLibPublicUsernameResolver(values, transport=transport)
        resolver_holder["resolver"] = resolver
        return resolver

    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        dry_run=False,
        approved_tdlib_public_username_resolve=True,
        approved_registry_resolve_mutation=approved_mutation,
        runtime_env_reader=lambda _path: _runtime_env_for_tdlib_transport(tmp_path),
        database_connection_factory=lambda _database_url: fake_db,
        public_username_resolver_factory=resolver_factory,
    )
    return result.report, fake_db, resolver_holder["resolver"]


def _auth_update(state_type: str) -> dict[str, Any]:
    return {
        "@type": "updateAuthorizationState",
        "authorization_state": {"@type": state_type},
    }


def _response_for_last_request(
    request_type: str,
    payload: dict[str, Any],
) -> TransportPayloadFactory:
    def response(transport: FakeTDLibTransport) -> dict[str, Any]:
        for request in reversed(transport.sent_requests):
            if request.get("@type") == request_type:
                extra = request.get("@extra")
                return {**payload, "@extra": extra}
        raise AssertionError(f"{request_type} was not sent")

    return response


def _ready_probe_response() -> TransportPayloadFactory:
    return _response_for_last_request(
        "getAuthorizationState",
        {"@type": "authorizationStateReady"},
    )


def _public_chat_response() -> TransportPayloadFactory:
    return _response_for_last_request(
        "searchPublicChat",
        {
            "@type": "chat",
            "id": RAW_CHAT_ID,
            "title": "Resolved Alpha",
            "usernames": {"active_usernames": ["ResolvedAlphaChannel"]},
            "type": {"@type": "chatTypeSupergroup", "is_channel": True},
        },
    )


def _sent_request_types(transport: FakeTDLibTransport) -> list[str]:
    return [request.get("@type", "") for request in transport.sent_requests]


def _assert_ready_probe_fields_present(report: dict[str, Any]) -> None:
    for field_name in READY_PROBE_FIELD_NAMES:
        assert field_name in report, field_name


def _render(report: dict[str, Any]) -> str:
    return _module().render_json(report)


def test_dry_run_reads_unresolved_public_username_targets_without_tdlib_or_mutation() -> None:
    report, db, resolver = _run_report()

    assert report["contract_status"] == "dry_run_public_username_resolve_plan_ready"
    assert report["runtime_env_read"] is True
    assert report["database_connected"] is True
    assert report["target_rows_checked"] is True
    assert report["target_row_count_bucket"] == "one"
    assert report["dry_run"] is True
    assert resolver is None
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert db.transaction.committed is False
    assert report["tdlib_resolve_attempted"] is False
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["tdlib_initialized"] is False
    assert report["side_effects"]["tdlib_send_called"] is False
    assert report["side_effects"]["tdlib_receive_called"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False
    _assert_ready_probe_fields_present(report)
    assert report["tdlib_ready_probe_attempted"] is False
    assert report["tdlib_ready_probe_status"] == "not_attempted"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "zero"
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == []
    assert report["tdlib_ready_probe_authorization_states_seen"] == []
    assert report["tdlib_ready_probe_final_authorization_state"] is None
    assert report["tdlib_ready_probe_manual_intervention_required"] is False


def test_without_approval_flags_stays_dry_run_and_blocks_tdlib_and_db_mutation() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    report, db, resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=False,
        approved_mutation=False,
    )

    assert report["contract_status"] == "dry_run_public_username_resolve_plan_ready"
    assert report["dry_run"] is True
    assert resolver.calls == []
    assert db.update_attempts == 0
    assert report["side_effects"]["telegram_api_called"] is False


def test_mutation_approval_without_tdlib_approval_fails_closed() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    report, db, resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=False,
        approved_mutation=True,
    )

    assert report["contract_status"] == "blocked_approval_required"
    assert "approval.tdlib_resolve_required" in report["checks_failed"]
    assert resolver.calls == []
    assert db.update_attempts == 0


def test_approved_tdlib_resolve_without_mutation_calls_fake_resolver_only() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    report, db, resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=False,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert resolver.initialized is True
    assert resolver.closed is True
    assert resolver.calls == [RAW_PUBLIC_USERNAME]
    assert report["tdlib_resolve_attempted"] is True
    assert report["resolved_count_bucket"] == "one"
    assert report["updated_row_count_bucket"] == "zero"
    assert report["skipped_row_count_bucket"] == "one"
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False


def test_tdlib_ready_update_allows_public_username_resolve_without_mutation(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert transport.initialized is True
    assert transport.closed is True
    assert resolver.tdlib_send_called is True
    assert resolver.tdlib_receive_called is True
    assert _sent_request_types(transport) == ["getAuthorizationState", "searchPublicChat"]
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "one"
    assert report["tdlib_ready_probe_request_types_sent"] == ["getAuthorizationState"]
    assert report["tdlib_ready_probe_update_types_seen"] == ["updateAuthorizationState"]
    assert report["tdlib_ready_probe_authorization_states_seen"] == ["authorizationStateReady"]
    assert report["tdlib_ready_probe_final_authorization_state"] == "authorizationStateReady"
    assert report["tdlib_ready_probe_error_class"] is None
    assert report["tdlib_ready_probe_error_code"] is None
    assert report["tdlib_ready_probe_manual_intervention_required"] is False
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert report["tdlib_resolve_attempted"] is True
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False


def test_tdlib_get_authorization_state_ready_without_ready_update_allows_resolve(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _ready_probe_response(),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert _sent_request_types(transport) == ["getAuthorizationState", "searchPublicChat"]
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "one"
    assert report["tdlib_ready_probe_request_types_sent"] == ["getAuthorizationState"]
    assert report["tdlib_ready_probe_update_types_seen"] == []
    assert report["tdlib_ready_probe_authorization_states_seen"] == ["authorizationStateReady"]
    assert report["tdlib_ready_probe_final_authorization_state"] == "authorizationStateReady"
    assert db.update_attempts == 0
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True


def test_tdlib_bootstrap_readiness_requests_are_summarized_without_values(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _auth_update("authorizationStateWaitEncryptionKey"),
            _ready_probe_response(),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert _sent_request_types(transport) == [
        "getAuthorizationState",
        "setTdlibParameters",
        "getAuthorizationState",
        "checkDatabaseEncryptionKey",
        "getAuthorizationState",
        "searchPublicChat",
    ]
    assert report["tdlib_ready_probe_request_types_sent"] == [
        "getAuthorizationState",
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
    ]
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters",
        "authorizationStateWaitEncryptionKey",
        "authorizationStateReady",
    ]
    assert report["tdlib_ready_probe_final_authorization_state"] == "authorizationStateReady"
    assert report["tdlib_ready_probe_status"] == "ready"
    assert db.update_attempts == 0


@pytest.mark.parametrize(
    "state_type",
    [
        "authorizationStateWaitPhoneNumber",
        "authorizationStateWaitCode",
        "authorizationStateWaitPassword",
        "authorizationStateWaitOtherDeviceConfirmation",
    ],
)
def test_manual_tdlib_authorization_states_fail_closed_without_auth_submission(
    tmp_path: Path,
    state_type: str,
) -> None:
    transport = FakeTDLibTransport([_auth_update(state_type)])

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )

    sent_types = _sent_request_types(transport)
    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert "tdlib.not_ready" in report["checks_failed"]
    assert db.update_attempts == 0
    assert "getAuthorizationState" in sent_types
    assert "searchPublicChat" not in sent_types
    assert "setAuthenticationPhoneNumber" not in sent_types
    assert "checkAuthenticationCode" not in sent_types
    assert "checkAuthenticationPassword" not in sent_types
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "manual_intervention_required"
    assert report["tdlib_ready_probe_request_types_sent"] == ["getAuthorizationState"]
    assert report["tdlib_ready_probe_update_types_seen"] == ["updateAuthorizationState"]
    assert report["tdlib_ready_probe_authorization_states_seen"] == [state_type]
    assert report["tdlib_ready_probe_final_authorization_state"] == state_type
    assert report["tdlib_ready_probe_manual_intervention_required"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


@pytest.mark.parametrize(
    "state_type",
    [
        "authorizationStateClosing",
        "authorizationStateClosed",
        "authorizationStateLoggingOut",
    ],
)
def test_closed_or_closing_tdlib_authorization_states_fail_closed(
    tmp_path: Path,
    state_type: str,
) -> None:
    transport = FakeTDLibTransport([_auth_update(state_type)])

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )

    sent_types = _sent_request_types(transport)
    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert db.update_attempts == 0
    assert "searchPublicChat" not in sent_types
    assert "joinChat" not in sent_types
    assert "joinChatByInviteLink" not in sent_types
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "not_ready"
    assert report["tdlib_ready_probe_request_types_sent"] == ["getAuthorizationState"]
    assert report["tdlib_ready_probe_update_types_seen"] == ["updateAuthorizationState"]
    assert report["tdlib_ready_probe_authorization_states_seen"] == [state_type]
    assert report["tdlib_ready_probe_final_authorization_state"] == state_type
    assert report["tdlib_ready_probe_manual_intervention_required"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_tdlib_readiness_probe_times_out_with_bounded_receive_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    monkeypatch.setattr(module, "DEFAULT_TDLIB_AUTH_MAX_UPDATES", 3)
    monkeypatch.setattr(module, "DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC", 0)
    transport = FakeTDLibTransport([None, None, None])

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )

    sent_types = _sent_request_types(transport)
    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert sent_types == ["getAuthorizationState", "getAuthorizationState", "getAuthorizationState"]
    assert len(transport.receive_timeouts) == 3
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "timed_out"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "zero"
    assert report["tdlib_ready_probe_request_types_sent"] == ["getAuthorizationState"]
    assert report["tdlib_ready_probe_update_types_seen"] == []
    assert report["tdlib_ready_probe_authorization_states_seen"] == []
    assert report["tdlib_ready_probe_final_authorization_state"] is None
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_tdlib_readiness_error_reports_only_redacted_error_fields(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            {
                "@type": "error",
                "code": 401,
                "message": f"{RAW_PUBLIC_USERNAME} {RAW_CHAT_ID} {FAKE_TELEGRAM_SECRET}",
            }
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )
    rendered = _render(report)

    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert _sent_request_types(transport) == ["getAuthorizationState"]
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "tdlib_error"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "one"
    assert report["tdlib_ready_probe_request_types_sent"] == ["getAuthorizationState"]
    assert report["tdlib_ready_probe_update_types_seen"] == []
    assert report["tdlib_ready_probe_authorization_states_seen"] == []
    assert report["tdlib_ready_probe_error_class"] == "tdlib_error"
    assert report["tdlib_ready_probe_error_code"] == 401
    assert "searchPublicChat" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert RAW_PUBLIC_USERNAME not in rendered
    assert str(RAW_CHAT_ID) not in rendered
    assert FAKE_TELEGRAM_SECRET not in rendered


def test_approved_tdlib_resolve_with_mutation_updates_only_successful_rows() -> None:
    db = FakeDatabaseConnection(
        [
            _registry_row("registry-1", RAW_PUBLIC_USERNAME),
            _registry_row("registry-2", RAW_PUBLIC_USERNAME_TWO),
            _registry_row("registry-3", RAW_PUBLIC_USERNAME_THREE),
        ]
    )
    resolver = FakeResolver(
        {
            RAW_PUBLIC_USERNAME: _resolved(),
            RAW_PUBLIC_USERNAME_TWO: _not_found(),
            RAW_PUBLIC_USERNAME_THREE: _unsupported(),
        }
    )

    report, db, resolver = _run_report(
        db=db,
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "public_username_resolve_partial"
    assert resolver.calls == [
        RAW_PUBLIC_USERNAME,
        RAW_PUBLIC_USERNAME_TWO,
        RAW_PUBLIC_USERNAME_THREE,
    ]
    assert db.update_attempts == 1
    assert db.updated_rows == 1
    assert db.rows[0]["chat_id"] == RAW_CHAT_ID
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert db.rows[1]["chat_id"] is None
    assert db.rows[1]["access_state"] == "unresolved"
    assert db.rows[2]["chat_id"] is None
    assert db.rows[2]["access_state"] == "unresolved"
    assert report["resolved_count_bucket"] == "one"
    assert report["unresolved_count_bucket"] == "two_to_five"
    assert report["updated_row_count_bucket"] == "one"
    assert report["skipped_row_count_bucket"] == "two_to_five"
    assert report["registry_resolve_mutation_performed"] is True
    assert report["side_effects"]["database_mutation_performed"] is True
    assert report["side_effects"]["telegram_channel_registry_updated"] is True
    assert report["side_effects"]["telegram_channel_registry_inserted"] is False
    assert report["side_effects"]["telegram_channel_registry_deleted"] is False
    assert db.transaction.committed is True


def test_update_sql_is_guarded_by_registry_source_state_and_null_chat_id() -> None:
    db = FakeDatabaseConnection(
        [_registry_row("registry-1", RAW_PUBLIC_USERNAME)],
        break_update_guard_for={"registry-1"},
    )
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})

    report, db, _resolver = _run_report(
        db=db,
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    update_sql = _normalize(_module().UPDATE_RESOLVED_REGISTRY_ROW_QUERY).upper()
    assert "WHERE REGISTRY_ID = :REGISTRY_ID" in update_sql
    assert "SOURCE_KIND = 'PUBLIC_USERNAME'" in update_sql
    assert "DESIRED_STATE = 'ACTIVE'" in update_sql
    assert "ACCESS_STATE = 'UNRESOLVED'" in update_sql
    assert "CHAT_ID IS NULL" in update_sql
    assert report["contract_status"] == "public_username_resolve_partial"
    assert db.update_attempts == 1
    assert db.updated_rows == 0
    assert db.rows[0]["chat_id"] is None
    assert report["side_effects"]["database_mutation_performed"] is False


def test_no_insert_delete_or_non_registry_update_sql_is_used() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    report, db, _resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "public_username_resolve_registry_updated"
    for statement in db.statements:
        upper_statement = statement.upper()
        assert " INSERT " not in f" {upper_statement} "
        assert " DELETE " not in f" {upper_statement} "
        assert " TRUNCATE " not in f" {upper_statement} "
        if upper_statement.startswith("UPDATE "):
            assert upper_statement.startswith("UPDATE TELEGRAM_CHANNEL_REGISTRY")
            assert "EVENT_OUTBOX" not in upper_statement
            assert "SOURCE_MESSAGES" not in upper_statement
            assert "SOURCE_MESSAGE_VERSIONS" not in upper_statement


def test_no_downstream_or_redis_mutations_are_possible() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    report, db, _resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    forbidden_sql_fragments = (
        "EVENT_OUTBOX",
        "SOURCE_MESSAGES",
        "SOURCE_MESSAGE_VERSIONS",
        "REDIS",
    )
    for statement in db.statements:
        upper_statement = statement.upper()
        for fragment in forbidden_sql_fragments:
            assert fragment not in upper_statement

    for key in (
        "redis_mutation_performed",
        "source_messages_written",
        "source_message_versions_written",
        "event_outbox_written",
        "outbox_relay_started",
        "router_normalizer_started",
        "live_collector_started",
        "collector_runtime_started",
        "notifier_transport_enabled",
        "alembic_upgrade_run",
        "alembic_downgrade_run",
        "alembic_stamp_run",
        "docker_or_systemd_changed",
        "files_mutated_outside_repo",
    ):
        assert report["side_effects"][key] is False, key


def test_resolved_not_joined_is_the_only_success_state_and_joined_is_never_set() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    _report, db, _resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    update_statements = [stmt for stmt in db.statements if stmt.upper().startswith("UPDATE ")]
    assert update_statements
    for statement in update_statements:
        assert "RESOLVED_NOT_JOINED" in statement.upper()
        assert "ACCESS_STATE = 'JOINED'" not in statement.upper()
    assert db.rows[0]["access_state"] == "resolved_not_joined"


def test_chat_id_and_raw_username_remain_unprinted_in_output() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    report, _db, _resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )
    rendered = _render(report)

    assert str(RAW_CHAT_ID) not in rendered
    assert RAW_PUBLIC_USERNAME not in rendered
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_DATABASE_PASSWORD not in rendered
    assert FAKE_TELEGRAM_SECRET not in rendered


def test_tdlib_auth_code_password_join_and_history_paths_are_not_called() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    called_attrs: list[str] = []
    called_names: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                called_attrs.append(node.func.attr)
            elif isinstance(node.func, ast.Name):
                called_names.append(node.func.id)

    for forbidden_call in (
        "build_set_authentication_phone_number_request",
        "setAuthenticationPhoneNumber",
        "build_check_authentication_code_request",
        "build_check_authentication_password_request",
        "checkAuthenticationCode",
        "checkAuthenticationPassword",
        "build_join_chat_request",
        "build_join_chat_by_invite_link_request",
        "build_get_chat_history_request",
    ):
        assert forbidden_call not in called_attrs
        assert forbidden_call not in called_names


def test_unsupported_chat_types_are_skipped_and_not_updated() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _unsupported()})
    report, db, _resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["unresolved_count_bucket"] == "one"
    assert report["updated_row_count_bucket"] == "zero"
    assert db.update_attempts == 0
    assert db.rows[0]["chat_id"] is None
    assert db.rows[0]["access_state"] == "unresolved"


def test_all_side_effect_fields_are_present_and_correct_across_modes() -> None:
    module = _module()
    dry_report, _db, _resolver = _run_report()
    resolve_report, _db, _resolver = _run_report(
        resolver=FakeResolver({RAW_PUBLIC_USERNAME: _resolved()}),
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=False,
    )
    mutation_report, _db, _resolver = _run_report(
        resolver=FakeResolver({RAW_PUBLIC_USERNAME: _resolved()}),
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    for report in (dry_report, resolve_report, mutation_report):
        assert set(report["side_effects"]) == set(module.SIDE_EFFECT_FLAG_NAMES)
        _assert_ready_probe_fields_present(report)

    for value in dry_report["side_effects"].values():
        assert value is False

    assert resolve_report["side_effects"]["tdlib_initialized"] is True
    assert resolve_report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert resolve_report["side_effects"]["database_mutation_performed"] is False
    assert resolve_report["side_effects"]["telegram_channel_registry_updated"] is False

    assert mutation_report["side_effects"]["tdlib_initialized"] is True
    assert mutation_report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert mutation_report["side_effects"]["database_mutation_performed"] is True
    assert mutation_report["side_effects"]["telegram_channel_registry_updated"] is True
    for key in (
        "telegram_channel_registry_inserted",
        "telegram_channel_registry_deleted",
        "tdlib_auth_attempted",
        "tdlib_join_called",
        "tdlib_history_fetch_called",
        "source_messages_written",
        "source_message_versions_written",
        "event_outbox_written",
    ):
        assert mutation_report["side_effects"][key] is False, key


def test_missing_runtime_env_or_db_access_fails_closed_without_leaks() -> None:
    unreadable_runtime = _module().generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=lambda _path: (_ for _ in ()).throw(
            OSError(f"cannot read {FAKE_TELEGRAM_SECRET}")
        ),
        database_connection_factory=lambda _database_url: FakeDatabaseConnection(),
    ).report
    rendered_unreadable = _render(unreadable_runtime)

    assert unreadable_runtime["contract_status"] == "blocked_runtime_env_unreadable"
    assert unreadable_runtime["runtime_env_read"] is False
    assert FAKE_TELEGRAM_SECRET not in rendered_unreadable
    assert RAW_PUBLIC_USERNAME not in rendered_unreadable

    db = FakeDatabaseConnection([_registry_row("registry-1")], fail_select_1=True)
    database_unavailable = _module().generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: db,
    ).report
    rendered_database_unavailable = _render(database_unavailable)

    assert database_unavailable["contract_status"] == "blocked_database_unavailable"
    assert "database.connection" in database_unavailable["checks_failed"]
    assert FAKE_DATABASE_URL not in rendered_database_unavailable
    assert FAKE_DATABASE_PASSWORD not in rendered_database_unavailable
    assert RAW_PUBLIC_USERNAME not in rendered_database_unavailable


def test_no_target_rows_returns_blocked_no_unresolved_public_username_rows() -> None:
    db = FakeDatabaseConnection([])
    report, db, _resolver = _run_report(db=db)

    assert report["contract_status"] == "blocked_no_unresolved_public_username_rows"
    assert report["target_rows_checked"] is True
    assert report["target_row_count_bucket"] == "zero"
    assert db.update_attempts == 0


def test_tdlib_not_ready_fails_closed_without_db_mutation() -> None:
    module = _module()
    resolver = FakeResolver(
        {},
        fail_initialize=module.TDLibNotReady("auth code required"),
    )

    report, db, resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert resolver.calls == []
    assert db.update_attempts == 0
    _assert_ready_probe_fields_present(report)
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_auth_attempted"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_cli_invalid_runtime_env_output_has_no_secret_or_stderr(tmp_path: Path) -> None:
    missing_runtime = tmp_path / "missing-runtime.env"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(missing_runtime),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert result.stderr == ""
    assert str(missing_runtime) not in result.stdout
    assert FAKE_DATABASE_URL not in result.stdout
    assert FAKE_TELEGRAM_SECRET not in result.stdout
