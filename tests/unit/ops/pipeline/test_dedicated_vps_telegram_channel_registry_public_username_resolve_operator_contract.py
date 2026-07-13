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
FAKE_REDIS_URL = "redis://:unit-redis-secret-public-username-resolve@127.0.0.1:6379/0"
FAKE_DATABASE_PASSWORD = "unit-db-password-for-public-username-resolve"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-public-username-resolve"
RAW_TDLIB_PAYLOAD_VALUE = "unit-raw-tdlib-payload-value-public-username-resolve"
RAW_PUBLIC_USERNAME = "PrivateAlphaChannel"
RAW_PUBLIC_USERNAME_TWO = "PrivateBetaChannel"
RAW_PUBLIC_USERNAME_THREE = "PrivateGammaChannel"
RAW_CHAT_ID = 9876543210123
RAW_OLD_CHAT_ID = -1001234567890
RAW_NEW_CHAT_ID = -1009876543210
RAW_REGISTRY_ID = "11111111-1111-4111-8111-111111111111"
RAW_LOCATOR_PATH = "/private/unit/exact-target-locator.json"
CLEANUP_EXCEPTION_SENTINELS = (
    "postgresql://user:secret@host/database",
    "redis://:secret@host/0",
    "/private/exact-target-locator.json",
    "raw-public-source",
    "raw-registry-id",
    "raw-chat-id",
    "raw-message-id",
    "raw-telegram-secret",
)
CLEANUP_EXCEPTION_MESSAGE = " ".join(CLEANUP_EXCEPTION_SENTINELS)
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
    "tdlib_ready_probe_auth_max_updates",
    "tdlib_ready_probe_receive_timeout_sec",
    "tdlib_ready_probe_overall_timeout_sec",
    "tdlib_ready_probe_manual_intervention_required",
    "tdlib_ready_probe_parameter_bootstrap_attempted",
    "tdlib_ready_probe_encryption_key_check_attempted",
    "tdlib_ready_probe_transport_closed",
    "tdlib_ready_probe_last_tdlib_object_type",
    "tdlib_ready_probe_timed_out_after_state",
    "tdlib_ready_probe_function_response_types_seen",
    "tdlib_ready_probe_set_parameters_response_type",
    "tdlib_ready_probe_set_parameters_error_code",
    "tdlib_ready_probe_set_parameters_error_class",
    "tdlib_ready_probe_encryption_key_response_type",
    "tdlib_ready_probe_encryption_key_error_code",
    "tdlib_ready_probe_encryption_key_error_class",
    "tdlib_ready_helper_reused",
    "tdlib_ready_helper_status",
    "tdlib_ready_helper_manual_intervention_required",
)
RESOLVE_CLASSIFICATION_FIELD_NAMES = (
    "resolve_attempt_count_bucket",
    "resolve_resolved_count_bucket",
    "resolve_not_found_count_bucket",
    "resolve_access_denied_count_bucket",
    "resolve_unsupported_chat_type_count_bucket",
    "resolve_response_timeout_count_bucket",
    "resolve_transport_error_count_bucket",
    "resolve_tdlib_error_count_bucket",
    "resolve_response_shape_error_count_bucket",
    "resolve_authorization_lost_count_bucket",
    "resolve_unknown_error_count_bucket",
    "resolve_failure_classes_seen",
    "resolve_tdlib_error_codes_seen",
    "resolve_function_response_types_seen",
    "resolve_response_extra_matched_count_bucket",
    "resolve_response_without_extra_count_bucket",
    "resolve_response_wrong_extra_count_bucket",
)
SINGLE_RESOLVE_RPC_DIAGNOSTIC_FIELD_NAMES = (
    "single_resolve_rpc_diagnostic_enabled",
    "single_resolve_rpc_target_selected",
    "single_resolve_rpc_target_index_bucket",
    "single_resolve_rpc_request_sent",
    "single_resolve_rpc_request_extra_present",
    "single_resolve_rpc_max_updates",
    "single_resolve_rpc_receive_timeout_sec",
    "single_resolve_rpc_max_duration_sec",
    "single_resolve_rpc_send_error_class",
    "single_resolve_rpc_receive_attempt_count_bucket",
    "single_resolve_rpc_observation_count_bucket",
    "single_resolve_rpc_empty_receive_count_bucket",
    "single_resolve_rpc_inbound_object_types_seen",
    "single_resolve_rpc_function_response_types_seen",
    "single_resolve_rpc_update_types_seen",
    "single_resolve_rpc_update_budget_exhausted",
    "single_resolve_rpc_duration_exhausted",
    "single_resolve_rpc_update_pressure_observed",
    "single_resolve_rpc_authorization_states_seen",
    "single_resolve_rpc_final_authorization_state",
    "single_resolve_rpc_response_extra_matched",
    "single_resolve_rpc_response_without_extra_count_bucket",
    "single_resolve_rpc_response_wrong_extra_count_bucket",
    "single_resolve_rpc_result_class",
    "single_resolve_rpc_tdlib_error_codes_seen",
    "single_resolve_rpc_timed_out",
    "single_resolve_rpc_operator_next_action",
)
POST_READY_DRAIN_FIELD_NAMES = (
    "tdlib_post_ready_drain_attempted",
    "tdlib_post_ready_drain_max_updates",
    "tdlib_post_ready_drain_timeout_sec",
    "tdlib_post_ready_drain_quiet_empty_receive_target",
    "tdlib_post_ready_drain_quiet_timeout_sec",
    "tdlib_post_ready_drain_receive_attempt_count_bucket",
    "tdlib_post_ready_drain_observation_count_bucket",
    "tdlib_post_ready_drain_empty_receive_count_bucket",
    "tdlib_post_ready_drain_quiet_empty_receive_streak_bucket",
    "tdlib_post_ready_drain_inbound_object_types_seen",
    "tdlib_post_ready_drain_function_response_types_seen",
    "tdlib_post_ready_drain_update_types_seen",
    "tdlib_post_ready_drain_authorization_states_seen",
    "tdlib_post_ready_drain_final_authorization_state",
    "tdlib_post_ready_drain_response_without_extra_count_bucket",
    "tdlib_post_ready_drain_response_wrong_extra_count_bucket",
    "tdlib_post_ready_drain_budget_exhausted",
    "tdlib_post_ready_drain_authorization_lost",
    "tdlib_post_ready_drain_quiet_window_reached",
)
SYNC_SETTLE_DIAGNOSTIC_FIELD_NAMES = (
    "tdlib_sync_settle_diagnostic_enabled",
    "tdlib_sync_settle_attempted",
    "tdlib_sync_settle_max_updates",
    "tdlib_sync_settle_receive_timeout_sec",
    "tdlib_sync_settle_quiet_empty_receive_target",
    "tdlib_sync_settle_max_duration_sec",
    "tdlib_sync_settle_receive_attempt_count_bucket",
    "tdlib_sync_settle_observation_count_bucket",
    "tdlib_sync_settle_empty_receive_count_bucket",
    "tdlib_sync_settle_quiet_empty_receive_streak_bucket",
    "tdlib_sync_settle_inbound_object_types_seen",
    "tdlib_sync_settle_update_types_seen",
    "tdlib_sync_settle_function_response_types_seen",
    "tdlib_sync_settle_authorization_states_seen",
    "tdlib_sync_settle_final_authorization_state",
    "tdlib_sync_settle_response_without_extra_count_bucket",
    "tdlib_sync_settle_response_wrong_extra_count_bucket",
    "tdlib_sync_settle_quiet_window_reached",
    "tdlib_sync_settle_update_budget_exhausted",
    "tdlib_sync_settle_duration_exhausted",
    "tdlib_sync_settle_authorization_lost",
    "tdlib_sync_settle_search_sent",
    "tdlib_sync_settle_operator_next_action",
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
    def __init__(
        self,
        connection: "FakeDatabaseConnection | None" = None,
        *,
        label: str = "transaction",
        rollback_failure: Exception | None = None,
    ) -> None:
        self.connection = connection
        self.label = label
        self.rollback_failure = rollback_failure
        self.commit_called = False
        self.committed = False
        self.rollback_attempted = False
        self.rollback_succeeded: bool | None = None
        self.rolled_back = False
        self._rows_snapshot = (
            [dict(row) for row in connection.rows] if connection is not None else []
        )
        self._updated_rows_snapshot = (
            connection.updated_rows if connection is not None else 0
        )

    def commit(self) -> None:
        self.commit_called = True
        self.committed = True
        if self.connection is not None:
            self.connection.events.append("transaction:commit")

    def rollback(self) -> None:
        if self.committed or self.rolled_back:
            return
        self.rollback_attempted = True
        if self.connection is not None:
            self.connection.events.append(f"transaction:{self.label}:rollback_attempt")
        if self.rollback_failure is not None:
            self.rollback_succeeded = False
            raise self.rollback_failure
        self.rolled_back = True
        self.rollback_succeeded = True
        if self.connection is not None:
            self.connection.rows[:] = [dict(row) for row in self._rows_snapshot]
            self.connection.updated_rows = self._updated_rows_snapshot
            self.connection.events.append("transaction:rollback")


class FakeDatabaseConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table_available: bool = True,
        fail_select_1: bool = False,
        break_update_guard_for: set[str] | None = None,
        events: list[str] | None = None,
        exact_select_source_value_override: str | None = None,
        break_exact_update_guard: bool = False,
        exact_update_rowcount_override: int | None = None,
        break_exact_readback: bool = False,
        fail_read_rollback: bool = False,
        fail_mutation_rollback: bool = False,
        fail_close: bool = False,
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available
        self.fail_select_1 = fail_select_1
        self.break_update_guard_for = break_update_guard_for or set()
        self.events = events if events is not None else []
        self.exact_select_source_value_override = exact_select_source_value_override
        self.break_exact_update_guard = break_exact_update_guard
        self.exact_update_rowcount_override = exact_update_rowcount_override
        self.break_exact_readback = break_exact_readback
        self.fail_read_rollback = fail_read_rollback
        self.fail_mutation_rollback = fail_mutation_rollback
        self.fail_close = fail_close
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.closed = False
        self.close_attempted = False
        self.transaction = FakeTransaction()
        self.transactions: list[FakeTransaction] = []
        self.update_attempts = 0
        self.updated_rows = 0

    def begin(self) -> FakeTransaction:
        transaction_index = len(self.transactions)
        label = "read" if transaction_index == 0 else "mutation"
        rollback_failure = None
        if transaction_index == 0 and self.fail_read_rollback:
            rollback_failure = RuntimeError(CLEANUP_EXCEPTION_MESSAGE)
        elif transaction_index > 0 and self.fail_mutation_rollback:
            rollback_failure = RuntimeError(CLEANUP_EXCEPTION_MESSAGE)
        self.transaction = FakeTransaction(
            self,
            label=label,
            rollback_failure=rollback_failure,
        )
        self.transactions.append(self.transaction)
        self.events.append("transaction:begin")
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

        if normalized == _normalize(module.SELECT_EXACT_TARGET_ROWS_QUERY):
            self.events.append("database:exact_select")
            rows = [
                row
                for row in self.rows
                if row["source_kind"] == "public_username"
                and row["desired_state"] == "active"
                and module._normalize_exact_source_identity(row["source_value"])
                == params["normalized_source_value"]
            ][:2]
            selected_rows = []
            for row in rows:
                selected = {
                    "registry_id": row["registry_id"],
                    "source_value": row["source_value"],
                    "source_kind": row["source_kind"],
                    "desired_state": row["desired_state"],
                    "access_state": row["access_state"],
                    "chat_id": row["chat_id"],
                }
                if self.exact_select_source_value_override is not None:
                    selected["source_value"] = self.exact_select_source_value_override
                selected_rows.append(selected)
            return FakeResult(rows=selected_rows)

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

        if normalized == _normalize(module.UPDATE_EXACT_JOINED_REGISTRY_ROW_QUERY):
            self.events.append("database:exact_update")
            self.update_attempts += 1
            if self.break_exact_update_guard:
                for row in self.rows:
                    if row["registry_id"] == params["registry_id"]:
                        row["access_state"] = "forbidden"
            for row in self.rows:
                if (
                    row["registry_id"] == params["registry_id"]
                    and row["source_kind"] == "public_username"
                    and row["source_value"] == params["source_value"]
                    and module._normalize_exact_source_identity(row["source_value"])
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
                    rowcount = self.exact_update_rowcount_override
                    return FakeResult(rowcount=1 if rowcount is None else rowcount)
            return FakeResult(
                rowcount=(
                    0
                    if self.exact_update_rowcount_override is None
                    else self.exact_update_rowcount_override
                )
            )

        if normalized == _normalize(module.SELECT_EXACT_RESOLVED_READBACK_QUERY):
            self.events.append("database:exact_readback")
            if self.break_exact_readback:
                return FakeResult(rows=[])
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
                and module._normalize_exact_source_identity(row["source_value"])
                == params["normalized_source_value"]
                and row["desired_state"] == "active"
                and row["access_state"] == "resolved_not_joined"
                and row["chat_id"] == params["chat_id"]
            ]
            return FakeResult(rows=rows)

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.close_attempted = True
        self.events.append("database:close_attempt")
        if self.fail_close:
            raise RuntimeError(CLEANUP_EXCEPTION_MESSAGE)
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
        drain_summary: Any | None = None,
        fail_drain: Exception | None = None,
        fail_close: bool = False,
        events: list[str] | None = None,
    ) -> None:
        self.responses = responses
        self.fail_initialize = fail_initialize
        self.drain_summary = drain_summary
        self.fail_drain = fail_drain
        self.fail_close = fail_close
        self.events = events if events is not None else []
        self.initialized = False
        self.closed = False
        self.close_attempted = False
        self.calls: list[str] = []
        self.drain_calls = 0
        self.auth_code_called = False
        self.auth_password_called = False
        self.join_called = False
        self.history_called = False
        self.diagnostic_calls: list[str] = []

    async def initialize(self) -> None:
        self.events.append("resolver:initialize")
        if self.fail_initialize is not None:
            raise self.fail_initialize
        self.initialized = True

    async def drain_post_ready_updates(self) -> Any:
        self.drain_calls += 1
        if self.fail_drain is not None:
            raise self.fail_drain
        if self.drain_summary is not None:
            return self.drain_summary
        summary = _module().TDLibPostReadyDrainSummary()
        summary.mark_attempted()
        return summary

    async def resolve_public_username(self, username: str) -> Any:
        self.events.append("resolver:resolve")
        self.calls.append(username)
        response = self.responses.get(username)
        if isinstance(response, Exception):
            raise response
        return response

    async def diagnose_single_resolve_rpc(self, username: str) -> Any:
        self.diagnostic_calls.append(username)
        response = self.responses.get(username)
        if isinstance(response, Exception):
            raise response
        return response

    async def diagnose_post_ready_sync_settle(self) -> Any:
        response = self.responses.get("sync_settle")
        if isinstance(response, Exception):
            raise response
        if response is not None:
            return response
        summary = _module().TDLibSyncSettleDiagnosticSummary(enabled=True)
        summary.mark_attempted()
        summary.record_empty_receive()
        summary.record_empty_receive()
        summary.record_empty_receive()
        summary.apply_next_action()
        return summary

    async def close(self) -> None:
        self.close_attempted = True
        self.events.append("resolver:close_attempt")
        if self.fail_close:
            raise RuntimeError(CLEANUP_EXCEPTION_MESSAGE)
        self.closed = True


TransportPayload = dict[str, Any] | None
TransportPayloadFactory = Any


class FakeTDLibTransport:
    def __init__(
        self,
        payloads: list[TransportPayload | TransportPayloadFactory],
        *,
        fail_close: bool = False,
    ) -> None:
        self.payloads = list(payloads)
        self.fail_close = fail_close
        self.initialized = False
        self.closed = False
        self.close_attempted = False
        self.sent_requests: list[dict[str, Any]] = []
        self.receive_timeouts: list[float] = []
        self.events: list[str] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def send(self, request: dict[str, Any]) -> None:
        self.events.append(f"send:{request.get('@type', '')}")
        self.sent_requests.append(request)

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        self.receive_timeouts.append(timeout)
        if not self.payloads:
            self.events.append("receive:none")
            return None
        payload = self.payloads.pop(0)
        if callable(payload):
            try:
                payload = payload(self)
            except AssertionError as exc:
                if "was not sent" in str(exc):
                    self.payloads.insert(0, payload)
                    self.events.append("receive:none")
                    return None
                raise
        payload_type = payload.get("@type", "") if isinstance(payload, dict) else "none"
        self.events.append(f"receive:{payload_type}")
        return payload

    async def close(self) -> None:
        self.close_attempted = True
        if self.fail_close:
            raise RuntimeError(CLEANUP_EXCEPTION_MESSAGE)
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
        "REDIS_URL": FAKE_REDIS_URL,
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


def _exact_registry_row(
    *,
    source_value: str = RAW_PUBLIC_USERNAME,
    access_state: str = "joined",
    chat_id: int | None = RAW_OLD_CHAT_ID,
    registry_id: str = RAW_REGISTRY_ID,
    desired_state: str = "active",
) -> dict[str, Any]:
    return _registry_row(
        registry_id,
        source_value,
        desired_state=desired_state,
        access_state=access_state,
        chat_id=chat_id,
    )


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
    diagnose_single_resolve_rpc: bool = False,
    diagnose_tdlib_post_ready_sync_settle: bool = False,
) -> tuple[dict[str, Any], FakeDatabaseConnection, FakeResolver | None]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    fake_resolver = resolver
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        dry_run=dry_run,
        approved_tdlib_public_username_resolve=approved_tdlib,
        approved_registry_resolve_mutation=approved_mutation,
        diagnose_single_resolve_rpc=diagnose_single_resolve_rpc,
        diagnose_tdlib_post_ready_sync_settle=(
            diagnose_tdlib_post_ready_sync_settle
        ),
        limit=limit,
        runtime_env_reader=runtime_env_reader,
        database_connection_factory=lambda _database_url: fake_db,
        public_username_resolver_factory=(
            (lambda _values: fake_resolver) if fake_resolver is not None else None
        ),
    )
    return result.report, fake_db, fake_resolver


def _run_exact_report(
    monkeypatch: pytest.MonkeyPatch,
    *,
    db: FakeDatabaseConnection | None = None,
    resolver: FakeResolver | None = None,
    locator_source_value: str = "privatealphachannel",
    dry_run: bool = False,
    approved_tdlib: bool = True,
    approved_mutation: bool = True,
    limit: int | None = None,
    diagnose_single_resolve_rpc: bool = False,
    diagnose_tdlib_post_ready_sync_settle: bool = False,
    result_holder: list[Any] | None = None,
) -> tuple[
    dict[str, Any],
    FakeDatabaseConnection,
    FakeResolver,
    list[object],
    list[str],
]:
    module = _module()
    events = db.events if db is not None else []
    fake_db = db or FakeDatabaseConnection([_exact_registry_row()], events=events)
    fake_resolver = resolver or FakeResolver(
        {locator_source_value: _resolved(chat_id=RAW_NEW_CHAT_ID)},
        events=events,
    )
    fake_resolver.events = events
    locator_calls: list[object] = []

    def strict_locator_reader(path: object) -> dict[str, Any]:
        locator_calls.append(path)
        return {"source_value": locator_source_value}

    monkeypatch.setattr(
        module.bounded_history_ingest_runner,
        "_read_target_locator",
        strict_locator_reader,
    )
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        target_locator_path=RAW_LOCATOR_PATH,
        dry_run=dry_run,
        approved_tdlib_public_username_resolve=approved_tdlib,
        approved_registry_resolve_mutation=approved_mutation,
        limit=limit,
        diagnose_single_resolve_rpc=diagnose_single_resolve_rpc,
        diagnose_tdlib_post_ready_sync_settle=(
            diagnose_tdlib_post_ready_sync_settle
        ),
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: fake_db,
        public_username_resolver_factory=lambda _values: fake_resolver,
    )
    if result_holder is not None:
        result_holder.append(result)
    return result.report, fake_db, fake_resolver, locator_calls, events


def _run_report_with_tdlib_transport(
    *,
    tmp_path: Path,
    transport: FakeTDLibTransport,
    db: FakeDatabaseConnection | None = None,
    approved_mutation: bool = False,
    tdlib_auth_max_updates: int | None = None,
    tdlib_receive_timeout_sec: float | None = None,
    tdlib_overall_timeout_sec: float | None = None,
    tdlib_post_ready_drain_max_updates: int | None = None,
    tdlib_post_ready_drain_timeout_sec: float | None = None,
    tdlib_post_ready_drain_quiet_empty_receives: int | None = None,
    tdlib_post_ready_drain_quiet_timeout_sec: float | None = None,
    tdlib_single_rpc_max_updates: int | None = None,
    tdlib_single_rpc_receive_timeout_sec: float | None = None,
    tdlib_single_rpc_max_duration_sec: float | None = None,
    diagnose_single_resolve_rpc: bool = False,
    diagnose_tdlib_post_ready_sync_settle: bool = False,
    tdlib_sync_settle_max_updates: int | None = None,
    tdlib_sync_settle_receive_timeout_sec: float | None = None,
    tdlib_sync_settle_quiet_empty_receives: int | None = None,
    tdlib_sync_settle_max_duration_sec: float | None = None,
    monotonic_clock: Any | None = None,
) -> tuple[dict[str, Any], FakeDatabaseConnection, Any]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    resolver_holder: dict[str, Any] = {}
    auth_max_updates = (
        module.DEFAULT_TDLIB_AUTH_MAX_UPDATES
        if tdlib_auth_max_updates is None
        else tdlib_auth_max_updates
    )
    receive_timeout_sec = (
        module.DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC
        if tdlib_receive_timeout_sec is None
        else tdlib_receive_timeout_sec
    )
    overall_timeout_sec = (
        module.DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC
        if tdlib_overall_timeout_sec is None
        else tdlib_overall_timeout_sec
    )
    post_ready_drain_max_updates = (
        module.DEFAULT_TDLIB_POST_READY_DRAIN_MAX_UPDATES
        if tdlib_post_ready_drain_max_updates is None
        else tdlib_post_ready_drain_max_updates
    )
    post_ready_drain_timeout_sec = (
        module.DEFAULT_TDLIB_POST_READY_DRAIN_TIMEOUT_SEC
        if tdlib_post_ready_drain_timeout_sec is None
        else tdlib_post_ready_drain_timeout_sec
    )
    post_ready_drain_quiet_empty_receives = (
        module.DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_EMPTY_RECEIVES
        if tdlib_post_ready_drain_quiet_empty_receives is None
        else tdlib_post_ready_drain_quiet_empty_receives
    )
    post_ready_drain_quiet_timeout_sec = (
        module.DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_TIMEOUT_SEC
        if tdlib_post_ready_drain_quiet_timeout_sec is None
        else tdlib_post_ready_drain_quiet_timeout_sec
    )
    single_rpc_max_updates = (
        module.DEFAULT_TDLIB_SINGLE_RPC_MAX_UPDATES
        if tdlib_single_rpc_max_updates is None
        else tdlib_single_rpc_max_updates
    )
    single_rpc_receive_timeout_sec = (
        module.DEFAULT_TDLIB_SINGLE_RPC_RECEIVE_TIMEOUT_SEC
        if tdlib_single_rpc_receive_timeout_sec is None
        else tdlib_single_rpc_receive_timeout_sec
    )
    single_rpc_max_duration_sec = (
        module.DEFAULT_TDLIB_SINGLE_RPC_MAX_DURATION_SEC
        if tdlib_single_rpc_max_duration_sec is None
        else tdlib_single_rpc_max_duration_sec
    )
    sync_settle_max_updates = (
        module.DEFAULT_TDLIB_SYNC_SETTLE_MAX_UPDATES
        if tdlib_sync_settle_max_updates is None
        else tdlib_sync_settle_max_updates
    )
    sync_settle_receive_timeout_sec = (
        module.DEFAULT_TDLIB_SYNC_SETTLE_RECEIVE_TIMEOUT_SEC
        if tdlib_sync_settle_receive_timeout_sec is None
        else tdlib_sync_settle_receive_timeout_sec
    )
    sync_settle_quiet_empty_receives = (
        module.DEFAULT_TDLIB_SYNC_SETTLE_QUIET_EMPTY_RECEIVES
        if tdlib_sync_settle_quiet_empty_receives is None
        else tdlib_sync_settle_quiet_empty_receives
    )
    sync_settle_max_duration_sec = (
        module.DEFAULT_TDLIB_SYNC_SETTLE_MAX_DURATION_SEC
        if tdlib_sync_settle_max_duration_sec is None
        else tdlib_sync_settle_max_duration_sec
    )

    def resolver_factory(values: dict[str, str]) -> Any:
        resolver = module.TDLibPublicUsernameResolver(
            values,
            transport=transport,
            auth_max_updates=auth_max_updates,
            receive_timeout_sec=receive_timeout_sec,
            overall_timeout_sec=overall_timeout_sec,
            post_ready_drain_max_updates=post_ready_drain_max_updates,
            post_ready_drain_timeout_sec=post_ready_drain_timeout_sec,
            post_ready_drain_quiet_empty_receives=(
                post_ready_drain_quiet_empty_receives
            ),
            post_ready_drain_quiet_timeout_sec=post_ready_drain_quiet_timeout_sec,
            single_rpc_max_updates=single_rpc_max_updates,
            single_rpc_receive_timeout_sec=single_rpc_receive_timeout_sec,
            single_rpc_max_duration_sec=single_rpc_max_duration_sec,
            sync_settle_max_updates=sync_settle_max_updates,
            sync_settle_receive_timeout_sec=sync_settle_receive_timeout_sec,
            sync_settle_quiet_empty_receives=sync_settle_quiet_empty_receives,
            sync_settle_max_duration_sec=sync_settle_max_duration_sec,
            monotonic_clock=monotonic_clock,
        )
        resolver_holder["resolver"] = resolver
        return resolver

    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        dry_run=False,
        approved_tdlib_public_username_resolve=True,
        approved_registry_resolve_mutation=approved_mutation,
        diagnose_single_resolve_rpc=diagnose_single_resolve_rpc,
        diagnose_tdlib_post_ready_sync_settle=(
            diagnose_tdlib_post_ready_sync_settle
        ),
        tdlib_auth_max_updates=auth_max_updates,
        tdlib_receive_timeout_sec=receive_timeout_sec,
        tdlib_overall_timeout_sec=overall_timeout_sec,
        tdlib_post_ready_drain_max_updates=post_ready_drain_max_updates,
        tdlib_post_ready_drain_timeout_sec=post_ready_drain_timeout_sec,
        tdlib_post_ready_drain_quiet_empty_receives=(
            post_ready_drain_quiet_empty_receives
        ),
        tdlib_post_ready_drain_quiet_timeout_sec=post_ready_drain_quiet_timeout_sec,
        tdlib_single_rpc_max_updates=single_rpc_max_updates,
        tdlib_single_rpc_receive_timeout_sec=single_rpc_receive_timeout_sec,
        tdlib_single_rpc_max_duration_sec=single_rpc_max_duration_sec,
        tdlib_sync_settle_max_updates=sync_settle_max_updates,
        tdlib_sync_settle_receive_timeout_sec=sync_settle_receive_timeout_sec,
        tdlib_sync_settle_quiet_empty_receives=sync_settle_quiet_empty_receives,
        tdlib_sync_settle_max_duration_sec=sync_settle_max_duration_sec,
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


def _public_chat_error_response(
    *,
    code: int | str = 400,
    message: str,
) -> TransportPayloadFactory:
    return _response_for_last_request(
        "searchPublicChat",
        {
            "@type": "error",
            "code": code,
            "message": message,
        },
    )


def _public_chat_private_response() -> TransportPayloadFactory:
    return _response_for_last_request(
        "searchPublicChat",
        {
            "@type": "chat",
            "id": RAW_CHAT_ID,
            "title": RAW_TDLIB_PAYLOAD_VALUE,
            "type": {"@type": "chatTypePrivate"},
        },
    )


def _public_chat_malformed_response() -> TransportPayloadFactory:
    return _response_for_last_request(
        "searchPublicChat",
        {
            "@type": "chat",
            "title": RAW_TDLIB_PAYLOAD_VALUE,
            "type": {"@type": "chatTypeSupergroup", "is_channel": True},
        },
    )


def _sent_request_types(transport: FakeTDLibTransport) -> list[str]:
    return [request.get("@type", "") for request in transport.sent_requests]


def _assert_ready_probe_fields_present(report: dict[str, Any]) -> None:
    for field_name in READY_PROBE_FIELD_NAMES:
        assert field_name in report, field_name


def _assert_resolve_classification_fields_present(report: dict[str, Any]) -> None:
    for field_name in RESOLVE_CLASSIFICATION_FIELD_NAMES:
        assert field_name in report, field_name


def _assert_single_resolve_rpc_diagnostic_fields_present(report: dict[str, Any]) -> None:
    for field_name in SINGLE_RESOLVE_RPC_DIAGNOSTIC_FIELD_NAMES:
        assert field_name in report, field_name


def _assert_post_ready_drain_fields_present(report: dict[str, Any]) -> None:
    for field_name in POST_READY_DRAIN_FIELD_NAMES:
        assert field_name in report, field_name


def _assert_sync_settle_diagnostic_fields_present(report: dict[str, Any]) -> None:
    for field_name in SYNC_SETTLE_DIAGNOSTIC_FIELD_NAMES:
        assert field_name in report, field_name


def _render(report: dict[str, Any]) -> str:
    return _module().render_json(report)


def _assert_sensitive_values_absent(rendered: str, tmp_path: Path | None = None) -> None:
    assert RAW_PUBLIC_USERNAME not in rendered
    assert RAW_PUBLIC_USERNAME.lower() not in rendered
    assert str(RAW_CHAT_ID) not in rendered
    assert str(RAW_OLD_CHAT_ID) not in rendered
    assert str(RAW_NEW_CHAT_ID) not in rendered
    assert RAW_REGISTRY_ID not in rendered
    assert RAW_LOCATOR_PATH not in rendered
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_REDIS_URL not in rendered
    assert FAKE_DATABASE_PASSWORD not in rendered
    assert FAKE_TELEGRAM_SECRET not in rendered
    assert RAW_TDLIB_PAYLOAD_VALUE not in rendered
    assert "fake-tdlib-key" not in rendered
    assert "ZmFrZS10ZGxpYi1rZXk=" not in rendered
    for sentinel in CLEANUP_EXCEPTION_SENTINELS:
        assert sentinel not in rendered
    if tmp_path is not None:
        assert str(tmp_path) not in rendered


def test_cli_tdlib_readiness_budget_options_are_passed_to_report_generation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    captured_kwargs: dict[str, Any] = {}

    def fake_generate_report(**kwargs: Any) -> Any:
        captured_kwargs.update(kwargs)
        return module.ScriptResult(
            exit_code=0,
            report={"contract_status": "captured"},
        )

    monkeypatch.setattr(module, "generate_report", fake_generate_report)

    exit_code = module.main(
        [
            "--runtime-env-path",
            "/safe/unit/runtime.env",
            "--approved-tdlib-public-username-resolve",
            "--tdlib-auth-max-updates",
            "7",
            "--tdlib-receive-timeout-sec",
            "0.25",
            "--tdlib-overall-timeout-sec",
            "9.5",
            "--tdlib-single-rpc-max-updates",
            "19",
            "--tdlib-single-rpc-receive-timeout-sec",
            "0.45",
            "--tdlib-single-rpc-max-duration-sec",
            "23.5",
            "--tdlib-post-ready-drain-max-updates",
            "11",
            "--tdlib-post-ready-drain-timeout-sec",
            "0.05",
            "--tdlib-post-ready-drain-quiet-empty-receives",
            "4",
            "--tdlib-post-ready-drain-quiet-timeout-sec",
            "0.15",
            "--diagnose-tdlib-post-ready-sync-settle",
            "--tdlib-sync-settle-max-updates",
            "13",
            "--tdlib-sync-settle-receive-timeout-sec",
            "0.35",
            "--tdlib-sync-settle-quiet-empty-receives",
            "5",
            "--tdlib-sync-settle-max-duration-sec",
            "17.5",
        ]
    )
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert json.loads(stdout) == {"contract_status": "captured"}
    assert captured_kwargs["tdlib_auth_max_updates"] == 7
    assert captured_kwargs["tdlib_receive_timeout_sec"] == 0.25
    assert captured_kwargs["tdlib_overall_timeout_sec"] == 9.5
    assert captured_kwargs["tdlib_single_rpc_max_updates"] == 19
    assert captured_kwargs["tdlib_single_rpc_receive_timeout_sec"] == 0.45
    assert captured_kwargs["tdlib_single_rpc_max_duration_sec"] == 23.5
    assert captured_kwargs["tdlib_post_ready_drain_max_updates"] == 11
    assert captured_kwargs["tdlib_post_ready_drain_timeout_sec"] == 0.05
    assert captured_kwargs["tdlib_post_ready_drain_quiet_empty_receives"] == 4
    assert captured_kwargs["tdlib_post_ready_drain_quiet_timeout_sec"] == 0.15
    assert captured_kwargs["diagnose_tdlib_post_ready_sync_settle"] is True
    assert captured_kwargs["tdlib_sync_settle_max_updates"] == 13
    assert captured_kwargs["tdlib_sync_settle_receive_timeout_sec"] == 0.35
    assert captured_kwargs["tdlib_sync_settle_quiet_empty_receives"] == 5
    assert captured_kwargs["tdlib_sync_settle_max_duration_sec"] == 17.5


def test_generate_report_passes_single_rpc_wait_options_to_default_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    captured_kwargs: dict[str, Any] = {}

    def fake_default_resolver_factory(
        _values: dict[str, str],
        **kwargs: Any,
    ) -> FakeResolver:
        captured_kwargs.update(kwargs)
        return FakeResolver(
            {
                RAW_PUBLIC_USERNAME: module.SingleResolveRpcDiagnosticResult(
                    enabled=True,
                    request_sent=True,
                    request_extra_present=True,
                    max_updates=kwargs["single_rpc_max_updates"],
                    receive_timeout_sec=kwargs["single_rpc_receive_timeout_sec"],
                    max_duration_sec=kwargs["single_rpc_max_duration_sec"],
                    receive_attempt_count=1,
                    observation_count=1,
                    function_response_types_seen=("chat",),
                    response_extra_matched=True,
                    result_class="resolved",
                )
            }
        )

    monkeypatch.setattr(module, "_default_resolver_factory", fake_default_resolver_factory)
    db = FakeDatabaseConnection([_registry_row("registry-1")])

    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        dry_run=False,
        approved_tdlib_public_username_resolve=True,
        diagnose_single_resolve_rpc=True,
        tdlib_single_rpc_max_updates=31,
        tdlib_single_rpc_receive_timeout_sec=0.75,
        tdlib_single_rpc_max_duration_sec=12.5,
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: db,
    )

    assert result.report["contract_status"] == "single_resolve_rpc_diagnostic_completed"
    assert captured_kwargs["single_rpc_max_updates"] == 31
    assert captured_kwargs["single_rpc_receive_timeout_sec"] == 0.75
    assert captured_kwargs["single_rpc_max_duration_sec"] == 12.5
    assert result.report["single_resolve_rpc_max_updates"] == 31
    assert result.report["single_resolve_rpc_receive_timeout_sec"] == 0.75
    assert result.report["single_resolve_rpc_max_duration_sec"] == 12.5


def test_cli_help_includes_single_resolve_rpc_diagnostic_flag() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--help",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert "--diagnose-single-resolve-rpc" in result.stdout


def test_cli_help_includes_single_resolve_rpc_wait_options() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--help",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--tdlib-single-rpc-max-updates" in result.stdout
    assert "--tdlib-single-rpc-receive-timeout-sec" in result.stdout
    assert "--tdlib-single-rpc-max-duration-sec" in result.stdout


def test_cli_help_includes_post_ready_drain_options() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--help",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert "--tdlib-post-ready-drain-max-updates" in result.stdout
    assert "--tdlib-post-ready-drain-timeout-sec" in result.stdout
    assert "--tdlib-post-ready-drain-quiet-empty-receives" in result.stdout
    assert "--tdlib-post-ready-drain-quiet-timeout-sec" in result.stdout


def test_cli_help_includes_sync_settle_diagnostic_options() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--help",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert "--diagnose-tdlib-post-ready-sync-settle" in result.stdout
    assert "--tdlib-sync-settle-max-updates" in result.stdout
    assert "--tdlib-sync-settle-receive-timeout-sec" in result.stdout
    assert "--tdlib-sync-settle-quiet-empty-receives" in result.stdout
    assert "--tdlib-sync-settle-max-duration-sec" in result.stdout


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
    for key in (
        "redis_mutation_performed",
        "tdlib_join_called",
        "tdlib_history_fetch_called",
        "live_collector_started",
        "collector_runtime_started",
        "source_messages_written",
        "source_message_versions_written",
        "event_outbox_written",
        "outbox_relay_started",
        "router_normalizer_started",
        "notifier_transport_enabled",
    ):
        assert report["side_effects"][key] is False, key
    _assert_ready_probe_fields_present(report)
    _assert_post_ready_drain_fields_present(report)
    _assert_sync_settle_diagnostic_fields_present(report)
    _assert_resolve_classification_fields_present(report)
    _assert_single_resolve_rpc_diagnostic_fields_present(report)
    assert report["tdlib_ready_probe_attempted"] is False
    assert report["tdlib_ready_probe_status"] == "not_attempted"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "zero"
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == []
    assert report["tdlib_ready_probe_authorization_states_seen"] == []
    assert report["tdlib_ready_probe_final_authorization_state"] is None
    assert report["tdlib_ready_probe_manual_intervention_required"] is False
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is False
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is False
    assert report["tdlib_ready_probe_transport_closed"] is False
    assert report["tdlib_ready_probe_last_tdlib_object_type"] is None
    assert report["tdlib_ready_probe_timed_out_after_state"] is None
    assert report["tdlib_ready_probe_function_response_types_seen"] == []
    assert report["tdlib_ready_probe_set_parameters_response_type"] is None
    assert report["tdlib_ready_probe_set_parameters_error_code"] is None
    assert report["tdlib_ready_probe_set_parameters_error_class"] is None
    assert report["tdlib_ready_probe_encryption_key_response_type"] is None
    assert report["tdlib_ready_probe_encryption_key_error_code"] is None
    assert report["tdlib_ready_probe_encryption_key_error_class"] is None
    assert report["tdlib_ready_helper_reused"] is False
    assert report["tdlib_ready_helper_status"] == "not_attempted"
    assert report["tdlib_ready_helper_manual_intervention_required"] is False
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert report["tdlib_post_ready_drain_receive_attempt_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_observation_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_authorization_lost"] is False
    assert report["tdlib_sync_settle_diagnostic_enabled"] is False
    assert report["tdlib_sync_settle_attempted"] is False
    assert report["tdlib_sync_settle_search_sent"] is False
    assert report["single_resolve_rpc_diagnostic_enabled"] is False
    assert report["single_resolve_rpc_target_selected"] is False
    assert report["single_resolve_rpc_request_sent"] is False
    assert report["single_resolve_rpc_result_class"] == "not_attempted"


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


def test_single_resolve_rpc_diagnostic_requires_tdlib_approval_without_mutation() -> None:
    resolver = FakeResolver({RAW_PUBLIC_USERNAME: _resolved()})
    report, db, resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=False,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
    )

    assert report["contract_status"] == "blocked_approval_required"
    assert "approval.tdlib_resolve_required" in report["checks_failed"]
    assert report["single_resolve_rpc_diagnostic_enabled"] is True
    assert report["single_resolve_rpc_target_selected"] is False
    assert resolver.calls == []
    assert resolver.diagnostic_calls == []
    assert db.update_attempts == 0
    assert report["side_effects"]["telegram_api_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_sync_settle_diagnostic_requires_tdlib_approval_without_mutation() -> None:
    resolver = FakeResolver({})
    report, db, resolver = _run_report(
        resolver=resolver,
        dry_run=False,
        approved_tdlib=False,
        approved_mutation=False,
        diagnose_tdlib_post_ready_sync_settle=True,
    )

    assert report["contract_status"] == "blocked_approval_required"
    assert "approval.tdlib_resolve_required" in report["checks_failed"]
    assert report["tdlib_sync_settle_diagnostic_enabled"] is True
    assert report["tdlib_sync_settle_attempted"] is False
    assert report["tdlib_sync_settle_search_sent"] is False
    assert resolver.calls == []
    assert resolver.diagnostic_calls == []
    assert db.update_attempts == 0
    assert report["side_effects"]["telegram_api_called"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_sync_settle_diagnostic_does_not_require_target_row(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport([_auth_update("authorizationStateReady"), None])
    db = FakeDatabaseConnection([])

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        db=db,
        diagnose_tdlib_post_ready_sync_settle=True,
        tdlib_sync_settle_quiet_empty_receives=1,
    )

    assert report["contract_status"] == "tdlib_sync_settle_diagnostic_quiet_window_reached"
    assert report["target_row_count_bucket"] == "zero"
    assert report["tdlib_sync_settle_attempted"] is True
    assert report["tdlib_sync_settle_quiet_window_reached"] is True
    assert _sent_request_types(transport) == []
    assert db.update_attempts == 0


def test_sync_settle_diagnostic_does_not_send_search_or_mutate_when_approved(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
        diagnose_tdlib_post_ready_sync_settle=True,
        tdlib_sync_settle_receive_timeout_sec=0.2,
        tdlib_sync_settle_quiet_empty_receives=2,
    )

    assert report["contract_status"] == "tdlib_sync_settle_diagnostic_quiet_window_reached"
    assert report["approved_registry_resolve_mutation"] is True
    assert report["tdlib_sync_settle_diagnostic_enabled"] is True
    assert report["tdlib_sync_settle_attempted"] is True
    assert report["tdlib_sync_settle_quiet_empty_receive_target"] == 2
    assert report["tdlib_sync_settle_receive_timeout_sec"] == 0.2
    assert report["tdlib_sync_settle_receive_attempt_count_bucket"] == "two_to_five"
    assert report["tdlib_sync_settle_empty_receive_count_bucket"] == "two_to_five"
    assert report["tdlib_sync_settle_quiet_empty_receive_streak_bucket"] == "two_to_five"
    assert report["tdlib_sync_settle_quiet_window_reached"] is True
    assert report["tdlib_sync_settle_search_sent"] is False
    assert report["tdlib_resolve_attempted"] is False
    assert report["registry_resolve_mutation_performed"] is False
    assert _sent_request_types(transport) == []
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert db.transaction.committed is False
    assert any(
        statement == _normalize(_module().SET_TRANSACTION_READ_ONLY_QUERY)
        for statement in db.statements
    )
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False


def test_sync_settle_diagnostic_non_empty_update_resets_quiet_streak(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            {"@type": "updateConnectionState"},
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        diagnose_tdlib_post_ready_sync_settle=True,
        tdlib_sync_settle_max_updates=3,
        tdlib_sync_settle_quiet_empty_receives=2,
    )

    assert report["contract_status"] == "tdlib_sync_settle_diagnostic_update_budget_exhausted"
    assert report["tdlib_sync_settle_quiet_window_reached"] is False
    assert report["tdlib_sync_settle_update_budget_exhausted"] is True
    assert report["tdlib_sync_settle_empty_receive_count_bucket"] == "two_to_five"
    assert report["tdlib_sync_settle_quiet_empty_receive_streak_bucket"] == "one"
    assert report["tdlib_sync_settle_update_types_seen"] == ["updateConnectionState"]
    assert _sent_request_types(transport) == []
    assert db.update_attempts == 0


def test_sync_settle_diagnostic_stale_ok_is_reported_without_search_response(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            {"@type": "ok", "@extra": "stale-public-username-extra"},
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        diagnose_tdlib_post_ready_sync_settle=True,
        tdlib_sync_settle_max_updates=2,
        tdlib_sync_settle_quiet_empty_receives=2,
    )
    rendered = _render(report)

    assert report["contract_status"] == "tdlib_sync_settle_diagnostic_update_budget_exhausted"
    assert report["tdlib_sync_settle_function_response_types_seen"] == ["ok"]
    assert report["tdlib_sync_settle_response_wrong_extra_count_bucket"] == "one"
    assert report["tdlib_sync_settle_search_sent"] is False
    assert report["single_resolve_rpc_response_extra_matched"] is False
    assert report["single_resolve_rpc_function_response_types_seen"] == []
    assert report["tdlib_resolve_attempted"] is False
    assert "stale-public-username-extra" not in rendered
    assert _sent_request_types(transport) == []
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_sync_settle_diagnostic_max_update_exhaustion_is_reported(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            {"@type": "updateOption", "name": RAW_TDLIB_PAYLOAD_VALUE},
            {"@type": "updateSupergroup", "supergroup": {"id": RAW_CHAT_ID}},
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        diagnose_tdlib_post_ready_sync_settle=True,
        tdlib_sync_settle_max_updates=2,
        tdlib_sync_settle_quiet_empty_receives=3,
    )
    rendered = _render(report)

    assert report["contract_status"] == "tdlib_sync_settle_diagnostic_update_budget_exhausted"
    assert report["tdlib_sync_settle_update_budget_exhausted"] is True
    assert report["tdlib_sync_settle_quiet_window_reached"] is False
    assert report["tdlib_sync_settle_observation_count_bucket"] == "two_to_five"
    assert report["tdlib_sync_settle_update_types_seen"] == [
        "updateOption",
        "updateSupergroup",
    ]
    assert str(RAW_CHAT_ID) not in rendered
    assert RAW_TDLIB_PAYLOAD_VALUE not in rendered
    assert _sent_request_types(transport) == []
    assert db.update_attempts == 0


def test_sync_settle_diagnostic_max_duration_exhaustion_is_reported(
    tmp_path: Path,
) -> None:
    ticks = iter([0.0, 0.0, 301.0])
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            {"@type": "updateOption", "name": "version"},
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        diagnose_tdlib_post_ready_sync_settle=True,
        tdlib_sync_settle_max_updates=5,
        tdlib_sync_settle_max_duration_sec=300.0,
        monotonic_clock=lambda: next(ticks, 301.0),
    )

    assert report["contract_status"] == "tdlib_sync_settle_diagnostic_duration_exhausted"
    assert report["tdlib_sync_settle_duration_exhausted"] is True
    assert report["tdlib_sync_settle_update_budget_exhausted"] is False
    assert report["tdlib_sync_settle_quiet_window_reached"] is False
    assert report["tdlib_sync_settle_receive_attempt_count_bucket"] == "one"
    assert report["tdlib_sync_settle_observation_count_bucket"] == "one"
    assert _sent_request_types(transport) == []
    assert db.update_attempts == 0


def test_sync_settle_diagnostic_authorization_lost_blocks_search_and_mutation(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _auth_update("authorizationStateClosing"),
        ]
    )

    result = _module().generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        dry_run=False,
        approved_tdlib_public_username_resolve=True,
        approved_registry_resolve_mutation=True,
        diagnose_tdlib_post_ready_sync_settle=True,
        runtime_env_reader=lambda _path: _runtime_env_for_tdlib_transport(tmp_path),
        database_connection_factory=lambda _database_url: FakeDatabaseConnection(
            [_registry_row("registry-1")]
        ),
        public_username_resolver_factory=lambda values: _module().TDLibPublicUsernameResolver(
            values,
            transport=transport,
            sync_settle_max_updates=2,
        ),
    )
    report = result.report

    assert result.exit_code == 1
    assert report["contract_status"] == "tdlib_sync_settle_diagnostic_authorization_lost"
    assert "tdlib.sync_settle_authorization_lost" in report["checks_failed"]
    assert report["tdlib_sync_settle_authorization_lost"] is True
    assert report["tdlib_sync_settle_authorization_states_seen"] == [
        "authorizationStateClosing"
    ]
    assert report["tdlib_sync_settle_final_authorization_state"] == (
        "authorizationStateClosing"
    )
    assert report["tdlib_sync_settle_search_sent"] is False
    assert report["tdlib_resolve_attempted"] is False
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert _sent_request_types(transport) == []


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
    assert resolver.drain_calls == 0
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
    assert report["tdlib_post_ready_drain_attempted"] is False


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
    assert _sent_request_types(transport) == ["searchPublicChat"]
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "one"
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == ["updateAuthorizationState"]
    assert report["tdlib_ready_probe_authorization_states_seen"] == ["authorizationStateReady"]
    assert report["tdlib_ready_probe_final_authorization_state"] == "authorizationStateReady"
    assert report["tdlib_ready_probe_error_class"] is None
    assert report["tdlib_ready_probe_error_code"] is None
    assert report["tdlib_ready_probe_manual_intervention_required"] is False
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "ready"
    assert report["tdlib_ready_helper_manual_intervention_required"] is False
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert report["tdlib_resolve_attempted"] is True
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False


def test_production_resolver_close_failure_preserves_broad_primary_result(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ],
        fail_close=True,
    )

    report, db, resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    captured = capsys.readouterr()
    rendered = _render(report)
    observed_output = "\n".join((rendered, captured.out, captured.err))

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert report["exact_target_mode"] is False
    assert transport.initialized is True
    assert transport.close_attempted is True
    assert transport.closed is False
    assert resolver.tdlib_send_called is True
    assert resolver.tdlib_receive_called is True
    assert _sent_request_types(transport) == ["searchPublicChat"]
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert report["tdlib_resolve_attempted"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False
    assert captured.out == ""
    assert captured.err == ""
    for unsafe_text in (
        "RuntimeError",
        "TDLibTransportError",
        "Traceback",
        "Failed to close TDLib transport",
    ):
        assert unsafe_text not in observed_output
    _assert_sensitive_values_absent(observed_output, tmp_path)


def test_tdlib_auth_ready_helper_update_allows_resolve_without_local_probe(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert _sent_request_types(transport) == ["searchPublicChat"]
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "one"
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == ["updateAuthorizationState"]
    assert report["tdlib_ready_probe_authorization_states_seen"] == ["authorizationStateReady"]
    assert report["tdlib_ready_probe_final_authorization_state"] == "authorizationStateReady"
    assert db.update_attempts == 0
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True


def test_tdlib_ready_after_set_parameters_without_function_response_allows_resolve(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            {
                "@type": "updateOption",
                "name": "version",
                "value": {"@type": "optionValueString"},
            },
            _auth_update("authorizationStateWaitTdlibParameters"),
            {
                "@type": "updateOption",
                "name": "connection_state",
                "value": {"@type": "optionValueEmpty"},
            },
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        tdlib_auth_max_updates=6,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert _sent_request_types(transport) == [
        "setTdlibParameters",
        "searchPublicChat",
    ]
    assert "checkDatabaseEncryptionKey" not in _sent_request_types(transport)
    assert "joinChat" not in _sent_request_types(transport)
    assert "getChatHistory" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["tdlib_ready_probe_auth_max_updates"] == 6
    assert report["tdlib_ready_probe_receive_timeout_sec"] == (
        _module().DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC
    )
    assert report["tdlib_ready_probe_overall_timeout_sec"] == (
        _module().DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC
    )
    assert report["tdlib_ready_probe_request_types_sent"] == ["setTdlibParameters"]
    assert report["tdlib_ready_probe_update_types_seen"] == [
        "updateOption",
        "updateAuthorizationState",
    ]
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters",
        "authorizationStateReady",
    ]
    assert report["tdlib_ready_probe_final_authorization_state"] == "authorizationStateReady"
    assert report["tdlib_ready_probe_function_response_types_seen"] == []
    assert report["tdlib_ready_probe_set_parameters_response_type"] is None
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is False
    assert report["tdlib_ready_helper_status"] == "ready"
    assert report["tdlib_resolve_attempted"] is True
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["tdlib_join_called"] is False
    assert report["side_effects"]["tdlib_history_fetch_called"] is False
    assert report["side_effects"]["live_collector_started"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["source_messages_written"] is False
    assert report["side_effects"]["source_message_versions_written"] is False
    assert report["side_effects"]["event_outbox_written"] is False


def test_tdlib_readiness_probe_short_budget_wait_parameters_noise_fails_closed(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            {
                "@type": "updateOption",
                "name": "version",
                "value": {"@type": "optionValueString"},
            },
            _auth_update("authorizationStateWaitTdlibParameters"),
            {
                "@type": "updateOption",
                "name": "connection_state",
                "value": {"@type": "optionValueEmpty"},
            },
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
        tdlib_auth_max_updates=3,
    )

    sent_types = _sent_request_types(transport)
    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert "tdlib.not_ready" in report["checks_failed"]
    assert sent_types == ["setTdlibParameters"]
    assert "searchPublicChat" not in sent_types
    assert "joinChat" not in sent_types
    assert "getChatHistory" not in sent_types
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert report["runtime_env_read"] is True
    assert report["database_connected"] is True
    assert report["target_rows_checked"] is True
    assert report["tdlib_ready_probe_status"] == "timed_out"
    assert report["tdlib_ready_probe_auth_max_updates"] == 3
    assert report["tdlib_ready_probe_request_types_sent"] == ["setTdlibParameters"]
    assert report["tdlib_ready_probe_update_types_seen"] == [
        "updateOption",
        "updateAuthorizationState",
    ]
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters",
    ]
    assert (
        report["tdlib_ready_probe_final_authorization_state"]
        == "authorizationStateWaitTdlibParameters"
    )
    assert (
        report["tdlib_ready_probe_timed_out_after_state"]
        == "authorizationStateWaitTdlibParameters"
    )
    assert report["tdlib_resolve_attempted"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["tdlib_join_called"] is False
    assert report["side_effects"]["tdlib_history_fetch_called"] is False
    assert report["side_effects"]["live_collector_started"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False
    assert report["side_effects"]["source_messages_written"] is False
    assert report["side_effects"]["source_message_versions_written"] is False
    assert report["side_effects"]["event_outbox_written"] is False
    assert "TDLib readiness probe timed out" in report["operator_next_action"]
    assert "runtime env or DB access" not in report["operator_next_action"]
    assert "Fix runtime env" not in report["operator_next_action"]


def test_tdlib_bootstrap_readiness_requests_are_summarized_without_values(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _auth_update("authorizationStateWaitEncryptionKey"),
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    rendered = _render(report)
    assert _sent_request_types(transport) == [
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "searchPublicChat",
    ]
    assert report["tdlib_ready_probe_request_types_sent"] == [
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
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is True
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is True
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "ready"
    assert all(
        str(request.get("@extra", "")).startswith("auth:")
        for request in transport.sent_requests
        if request.get("@type") in {"setTdlibParameters", "checkDatabaseEncryptionKey"}
    )
    assert not any(
        str(request.get("@extra", "")).startswith(_module().SCRIPT_NAME)
        for request in transport.sent_requests
        if request.get("@type") in {"setTdlibParameters", "checkDatabaseEncryptionKey"}
    )
    assert report["tdlib_ready_probe_transport_closed"] is False
    assert report["tdlib_ready_probe_last_tdlib_object_type"] == "updateAuthorizationState"
    assert report["tdlib_ready_probe_timed_out_after_state"] is None
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_tdlib_set_parameters_error_is_sanitized_and_blocks_before_key_check(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request(
                "setTdlibParameters",
                {
                    "@type": "error",
                    "code": 400,
                    "message": (
                        f"{RAW_PUBLIC_USERNAME} {RAW_CHAT_ID} "
                        f"{FAKE_TELEGRAM_SECRET} {tmp_path}"
                    ),
                },
            ),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )
    rendered = _render(report)

    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert "tdlib.not_ready" in report["checks_failed"]
    assert _sent_request_types(transport) == [
        "setTdlibParameters",
    ]
    assert "checkDatabaseEncryptionKey" not in _sent_request_types(transport)
    assert "searchPublicChat" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_status"] == "tdlib_error"
    assert report["tdlib_ready_probe_function_response_types_seen"] == ["error"]
    assert report["tdlib_ready_probe_set_parameters_response_type"] == "error"
    assert report["tdlib_ready_probe_set_parameters_error_code"] == 400
    assert report["tdlib_ready_probe_set_parameters_error_class"] == "tdlib_error"
    assert report["tdlib_ready_probe_encryption_key_response_type"] is None
    assert report["tdlib_ready_probe_encryption_key_error_code"] is None
    assert report["tdlib_ready_probe_encryption_key_error_class"] is None
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is True
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is False
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters",
    ]
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "degraded"
    assert report["tdlib_resolve_attempted"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_tdlib_set_parameters_ok_without_wait_encryption_key_times_out_without_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    monkeypatch.setattr(module, "DEFAULT_TDLIB_AUTH_MAX_UPDATES", 4)
    monkeypatch.setattr(module, "DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC", 0)
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request("setTdlibParameters", {"@type": "ok"}),
            None,
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )

    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert _sent_request_types(transport) == [
        "setTdlibParameters",
    ]
    assert "checkDatabaseEncryptionKey" not in _sent_request_types(transport)
    assert "searchPublicChat" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_status"] == "timed_out"
    assert report["tdlib_ready_probe_function_response_types_seen"] == ["ok"]
    assert report["tdlib_ready_probe_set_parameters_response_type"] == "ok"
    assert report["tdlib_ready_probe_set_parameters_error_code"] is None
    assert report["tdlib_ready_probe_set_parameters_error_class"] is None
    assert report["tdlib_ready_probe_encryption_key_response_type"] is None
    assert report["tdlib_ready_probe_encryption_key_error_code"] is None
    assert report["tdlib_ready_probe_encryption_key_error_class"] is None
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is True
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is False
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters"
    ]
    assert (
        report["tdlib_ready_probe_timed_out_after_state"]
        == "authorizationStateWaitTdlibParameters"
    )
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "degraded"
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_tdlib_encryption_key_error_is_sanitized_and_blocks_before_search(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request("setTdlibParameters", {"@type": "ok"}),
            _auth_update("authorizationStateWaitEncryptionKey"),
            _response_for_last_request(
                "checkDatabaseEncryptionKey",
                {
                    "@type": "error",
                    "code": "KEY_INVALID",
                    "message": (
                        f"{RAW_PUBLIC_USERNAME} {RAW_CHAT_ID} "
                        f"{FAKE_TELEGRAM_SECRET} {tmp_path}"
                    ),
                },
            ),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )
    rendered = _render(report)

    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert _sent_request_types(transport) == [
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
    ]
    assert "searchPublicChat" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_status"] == "tdlib_error"
    assert report["tdlib_ready_probe_function_response_types_seen"] == ["ok", "error"]
    assert report["tdlib_ready_probe_set_parameters_response_type"] == "ok"
    assert report["tdlib_ready_probe_set_parameters_error_code"] is None
    assert report["tdlib_ready_probe_set_parameters_error_class"] is None
    assert report["tdlib_ready_probe_encryption_key_response_type"] == "error"
    assert report["tdlib_ready_probe_encryption_key_error_code"] == "KEY_INVALID"
    assert report["tdlib_ready_probe_encryption_key_error_class"] == "tdlib_error"
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is True
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is True
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "degraded"
    assert report["tdlib_resolve_attempted"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_tdlib_parameter_and_encryption_key_ok_then_ready_allows_search(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request("setTdlibParameters", {"@type": "ok"}),
            _auth_update("authorizationStateWaitEncryptionKey"),
            _response_for_last_request("checkDatabaseEncryptionKey", {"@type": "ok"}),
            _auth_update("authorizationStateReady"),
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
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "searchPublicChat",
    ]
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["tdlib_ready_probe_function_response_types_seen"] == ["ok"]
    assert report["tdlib_ready_probe_set_parameters_response_type"] == "ok"
    assert report["tdlib_ready_probe_encryption_key_response_type"] == "ok"
    assert report["tdlib_ready_probe_set_parameters_error_code"] is None
    assert report["tdlib_ready_probe_set_parameters_error_class"] is None
    assert report["tdlib_ready_probe_encryption_key_error_code"] is None
    assert report["tdlib_ready_probe_encryption_key_error_class"] is None
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters",
        "authorizationStateWaitEncryptionKey",
        "authorizationStateReady",
    ]
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "ready"
    assert report["tdlib_resolve_attempted"] is True
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False


def test_tdlib_set_parameters_without_transition_blocks_with_bootstrap_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    monkeypatch.setattr(module, "DEFAULT_TDLIB_AUTH_MAX_UPDATES", 3)
    monkeypatch.setattr(module, "DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC", 0)
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            None,
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )

    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert "tdlib.not_ready" in report["checks_failed"]
    assert _sent_request_types(transport) == [
        "setTdlibParameters",
    ]
    assert "checkDatabaseEncryptionKey" not in _sent_request_types(transport)
    assert "searchPublicChat" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_status"] == "timed_out"
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is True
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is False
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters"
    ]
    assert (
        report["tdlib_ready_probe_final_authorization_state"]
        == "authorizationStateWaitTdlibParameters"
    )
    assert (
        report["tdlib_ready_probe_timed_out_after_state"]
        == "authorizationStateWaitTdlibParameters"
    )
    assert report["tdlib_ready_probe_last_tdlib_object_type"] == "updateAuthorizationState"
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "degraded"
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_tdlib_encryption_key_check_without_ready_blocks_with_key_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    monkeypatch.setattr(module, "DEFAULT_TDLIB_AUTH_MAX_UPDATES", 3)
    monkeypatch.setattr(module, "DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC", 0)
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitEncryptionKey"),
            None,
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )

    assert report["contract_status"] == "blocked_tdlib_not_ready"
    assert _sent_request_types(transport) == [
        "checkDatabaseEncryptionKey",
    ]
    assert "setTdlibParameters" not in _sent_request_types(transport)
    assert "searchPublicChat" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_status"] == "timed_out"
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is False
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is True
    assert report["tdlib_ready_probe_authorization_states_seen"] == [
        "authorizationStateWaitEncryptionKey"
    ]
    assert (
        report["tdlib_ready_probe_final_authorization_state"]
        == "authorizationStateWaitEncryptionKey"
    )
    assert (
        report["tdlib_ready_probe_timed_out_after_state"]
        == "authorizationStateWaitEncryptionKey"
    )
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "degraded"
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


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
    assert sent_types == []
    assert "searchPublicChat" not in sent_types
    assert "setAuthenticationPhoneNumber" not in sent_types
    assert "checkAuthenticationCode" not in sent_types
    assert "checkAuthenticationPassword" not in sent_types
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "manual_intervention_required"
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == ["updateAuthorizationState"]
    assert report["tdlib_ready_probe_authorization_states_seen"] == [state_type]
    assert report["tdlib_ready_probe_final_authorization_state"] == state_type
    assert report["tdlib_ready_probe_manual_intervention_required"] is True
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is False
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is False
    assert report["tdlib_ready_probe_transport_closed"] is False
    assert report["tdlib_ready_probe_timed_out_after_state"] is None
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "manual_intervention_required"
    assert report["tdlib_ready_helper_manual_intervention_required"] is True
    assert report["side_effects"]["tdlib_send_called"] is False
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
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == ["updateAuthorizationState"]
    assert report["tdlib_ready_probe_authorization_states_seen"] == [state_type]
    assert report["tdlib_ready_probe_final_authorization_state"] == state_type
    assert report["tdlib_ready_probe_manual_intervention_required"] is False
    assert report["tdlib_ready_probe_transport_closed"] is True
    assert report["tdlib_ready_probe_timed_out_after_state"] is None
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] in {"closed", "degraded"}
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
    assert sent_types == []
    assert len(transport.receive_timeouts) == 3
    assert db.update_attempts == 0
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "timed_out"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "zero"
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == []
    assert report["tdlib_ready_probe_authorization_states_seen"] == []
    assert report["tdlib_ready_probe_final_authorization_state"] is None
    assert report["tdlib_ready_probe_parameter_bootstrap_attempted"] is False
    assert report["tdlib_ready_probe_encryption_key_check_attempted"] is False
    assert report["tdlib_ready_probe_timed_out_after_state"] is None
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "degraded"
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is False
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
    assert _sent_request_types(transport) == []
    assert report["tdlib_ready_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "tdlib_error"
    assert report["tdlib_ready_probe_observation_count_bucket"] == "one"
    assert report["tdlib_ready_probe_request_types_sent"] == []
    assert report["tdlib_ready_probe_update_types_seen"] == []
    assert report["tdlib_ready_probe_authorization_states_seen"] == []
    assert report["tdlib_ready_probe_error_class"] == "tdlib_error"
    assert report["tdlib_ready_probe_error_code"] == 401
    assert report["tdlib_ready_helper_reused"] is True
    assert report["tdlib_ready_helper_status"] == "degraded"
    assert "searchPublicChat" not in _sent_request_types(transport)
    assert db.update_attempts == 0
    assert RAW_PUBLIC_USERNAME not in rendered
    assert str(RAW_CHAT_ID) not in rendered
    assert FAKE_TELEGRAM_SECRET not in rendered


def test_public_username_resolve_not_found_error_is_classified_without_unknown_failure(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_error_response(
                message=f"USERNAME_NOT_OCCUPIED {RAW_PUBLIC_USERNAME} {RAW_CHAT_ID}"
            ),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_attempt_count_bucket"] == "one"
    assert report["resolve_not_found_count_bucket"] == "one"
    assert report["resolve_unknown_error_count_bucket"] == "zero"
    assert report["failed_resolve_count_bucket"] == "zero"
    assert report["unresolved_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["not_found"]
    assert report["resolve_tdlib_error_codes_seen"] == [400]
    assert report["resolve_function_response_types_seen"] == ["error"]
    assert report["resolve_response_extra_matched_count_bucket"] == "one"
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_public_username_resolve_private_error_is_classified_access_denied(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_error_response(
                message=f"CHANNEL_PRIVATE {RAW_PUBLIC_USERNAME} {RAW_TDLIB_PAYLOAD_VALUE}"
            ),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_access_denied_count_bucket"] == "one"
    assert report["resolve_unknown_error_count_bucket"] == "zero"
    assert report["failed_resolve_count_bucket"] == "zero"
    assert report["unresolved_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["access_denied"]
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_public_username_resolve_unknown_tdlib_error_reports_sanitized_code_only(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_error_response(
                code="SAFE_UNKNOWN_CODE",
                message=(
                    f"{RAW_PUBLIC_USERNAME} {RAW_CHAT_ID} "
                    f"{FAKE_TELEGRAM_SECRET} {RAW_TDLIB_PAYLOAD_VALUE}"
                ),
            ),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_tdlib_error_count_bucket"] == "one"
    assert report["resolve_unknown_error_count_bucket"] == "zero"
    assert report["failed_resolve_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["tdlib_error"]
    assert report["resolve_tdlib_error_codes_seen"] == ["SAFE_UNKNOWN_CODE"]
    assert "SAFE_UNKNOWN_CODE" in rendered
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_public_username_resolve_timeout_is_classified_without_matching_response(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        tdlib_single_rpc_max_updates=2,
        tdlib_single_rpc_receive_timeout_sec=0,
    )

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_response_timeout_count_bucket"] == "one"
    assert report["failed_resolve_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["response_timeout"]
    assert report["resolve_response_extra_matched_count_bucket"] == "zero"
    assert db.update_attempts == 0


def test_public_username_resolve_wrong_extra_is_counted_without_resolving(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
            None,
            {
                "@type": "chat",
                "@extra": "wrong-public-username-extra",
                "id": RAW_CHAT_ID,
                "title": RAW_TDLIB_PAYLOAD_VALUE,
                "type": {"@type": "chatTypeSupergroup", "is_channel": True},
            },
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        tdlib_single_rpc_max_updates=4,
        tdlib_single_rpc_receive_timeout_sec=0,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_response_timeout_count_bucket"] == "one"
    assert report["resolve_response_wrong_extra_count_bucket"] == "one"
    assert report["resolve_response_extra_matched_count_bucket"] == "zero"
    assert report["resolve_function_response_types_seen"] == ["chat"]
    assert report["resolved_count_bucket"] == "zero"
    assert db.update_attempts == 0
    assert "wrong-public-username-extra" not in rendered
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_public_username_resolve_without_extra_is_counted_without_resolving(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
            None,
            {
                "@type": "chat",
                "id": RAW_CHAT_ID,
                "title": RAW_TDLIB_PAYLOAD_VALUE,
                "type": {"@type": "chatTypeSupergroup", "is_channel": True},
            },
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        tdlib_single_rpc_max_updates=4,
        tdlib_single_rpc_receive_timeout_sec=0,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_response_timeout_count_bucket"] == "one"
    assert report["resolve_response_without_extra_count_bucket"] == "one"
    assert report["resolve_response_extra_matched_count_bucket"] == "zero"
    assert report["resolve_function_response_types_seen"] == ["chat"]
    assert report["resolved_count_bucket"] == "zero"
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_public_username_resolve_authorization_lost_is_classified_without_mutation(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
            None,
            _auth_update("authorizationStateClosing"),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
    )

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_authorization_lost_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["authorization_lost"]
    assert report["failed_resolve_count_bucket"] == "one"
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True


def test_authorization_lost_after_prior_update_rolls_back_and_reports_no_mutation() -> None:
    module = _module()
    db = FakeDatabaseConnection(
        [
            _registry_row("registry-1", RAW_PUBLIC_USERNAME),
            _registry_row("registry-2", RAW_PUBLIC_USERNAME_TWO),
        ]
    )
    resolver = FakeResolver(
        {
            RAW_PUBLIC_USERNAME: _resolved(),
            RAW_PUBLIC_USERNAME_TWO: module.TDLibNotReady("authorization lost"),
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
    assert resolver.calls == [RAW_PUBLIC_USERNAME, RAW_PUBLIC_USERNAME_TWO]
    assert db.update_attempts == 1
    assert db.transaction.rolled_back is True
    assert db.transaction.committed is False
    assert report["resolve_authorization_lost_count_bucket"] == "one"
    assert "authorization_lost" in report["resolve_failure_classes_seen"]
    assert report["updated_row_count_bucket"] == "zero"
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False
    assert "authorization was lost" in report["operator_next_action"]
    assert "No registry mutation was committed" in report["operator_next_action"]
    for key in (
        "redis_mutation_performed",
        "tdlib_join_called",
        "tdlib_history_fetch_called",
        "live_collector_started",
        "collector_runtime_started",
        "source_messages_written",
        "source_message_versions_written",
        "event_outbox_written",
    ):
        assert report["side_effects"][key] is False, key


def test_public_username_resolve_unsupported_chat_type_is_classified(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_private_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_unsupported_chat_type_count_bucket"] == "one"
    assert report["unresolved_count_bucket"] == "one"
    assert report["failed_resolve_count_bucket"] == "zero"
    assert report["resolve_failure_classes_seen"] == ["unsupported_chat_type"]
    assert report["resolve_function_response_types_seen"] == ["chat"]
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_public_username_resolve_malformed_chat_is_response_shape_error(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_malformed_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_response_shape_error_count_bucket"] == "one"
    assert report["failed_resolve_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["response_shape_error"]
    assert report["resolve_function_response_types_seen"] == ["chat"]
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_public_username_resolve_success_classification_keeps_no_mutation_path(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert report["resolved_count_bucket"] == "one"
    assert report["resolve_attempt_count_bucket"] == "one"
    assert report["resolve_resolved_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == []
    assert report["resolve_function_response_types_seen"] == ["chat"]
    assert report["resolve_response_extra_matched_count_bucket"] == "one"
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_single_rpc_diagnostic_observes_match_after_update_backlog_without_quiet_drain(
    tmp_path: Path,
) -> None:
    update_backlog = [
        {
            "@type": "updateOption",
            "name": RAW_TDLIB_PAYLOAD_VALUE,
            "value": {"@type": "optionValueString", "value": RAW_PUBLIC_USERNAME},
        }
        for _ in range(25)
    ]
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            *update_backlog,
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
        tdlib_single_rpc_max_updates=40,
        tdlib_single_rpc_receive_timeout_sec=0.5,
        tdlib_single_rpc_max_duration_sec=15.0,
    )
    rendered = _render(report)

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_completed"
    assert transport.events.index("send:searchPublicChat") < transport.events.index(
        "receive:updateOption"
    )
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert report["tdlib_post_ready_drain_quiet_window_reached"] is False
    assert report["tdlib_post_ready_drain_budget_exhausted"] is False
    assert report["single_resolve_rpc_max_updates"] == 40
    assert report["single_resolve_rpc_receive_timeout_sec"] == 0.5
    assert report["single_resolve_rpc_max_duration_sec"] == 15.0
    assert report["single_resolve_rpc_receive_attempt_count_bucket"] == (
        "twenty_one_to_fifty"
    )
    assert report["single_resolve_rpc_observation_count_bucket"] == (
        "twenty_one_to_fifty"
    )
    assert report["single_resolve_rpc_empty_receive_count_bucket"] == "zero"
    assert report["single_resolve_rpc_update_pressure_observed"] is True
    assert report["single_resolve_rpc_update_budget_exhausted"] is False
    assert report["single_resolve_rpc_duration_exhausted"] is False
    assert report["single_resolve_rpc_update_types_seen"] == ["updateOption"]
    assert report["single_resolve_rpc_function_response_types_seen"] == ["chat"]
    assert report["single_resolve_rpc_result_class"] == "resolved"
    assert report["single_resolve_rpc_response_extra_matched"] is True
    assert report["single_resolve_rpc_response_wrong_extra_count_bucket"] == "zero"
    assert all(
        timeout == 0.5
        for timeout in transport.receive_timeouts[1:]
    )
    assert report["tdlib_resolve_attempted"] is True
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_single_rpc_diagnostic_sends_search_before_stale_function_response(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            {"@type": "ok", "@extra": "stale-public-username-extra"},
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
        tdlib_single_rpc_max_updates=3,
    )
    rendered = _render(report)

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_completed"
    assert transport.events.index("send:searchPublicChat") < transport.events.index(
        "receive:ok"
    )
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert report["single_resolve_rpc_function_response_types_seen"] == ["ok", "chat"]
    assert report["single_resolve_rpc_response_wrong_extra_count_bucket"] == "one"
    assert report["single_resolve_rpc_response_extra_matched"] is True
    assert report["single_resolve_rpc_result_class"] == "resolved"
    assert db.update_attempts == 0
    assert "stale-public-username-extra" not in rendered
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_normal_no_mutation_resolve_sends_search_before_update_backlog(
    tmp_path: Path,
) -> None:
    update_backlog = [
        {
            "@type": "updateOption",
            "name": RAW_TDLIB_PAYLOAD_VALUE,
            "value": {"@type": "optionValueString", "value": RAW_PUBLIC_USERNAME},
        }
        for _ in range(25)
    ]
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            *update_backlog,
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert transport.events.index("send:searchPublicChat") < transport.events.index(
        "receive:updateOption"
    )
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert report["tdlib_post_ready_drain_observation_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_empty_receive_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_quiet_empty_receive_streak_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_quiet_window_reached"] is False
    assert report["tdlib_post_ready_drain_budget_exhausted"] is False
    assert report["tdlib_post_ready_drain_update_types_seen"] == []
    assert report["tdlib_post_ready_drain_authorization_lost"] is False
    assert report["resolve_function_response_types_seen"] == ["chat"]
    assert report["resolve_response_extra_matched_count_bucket"] == "one"
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_normal_no_mutation_resolve_update_budget_exhaustion_is_response_timeout(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            {"@type": "updateConnectionState"},
            {"@type": "updateOption", "name": "version"},
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        tdlib_single_rpc_max_updates=2,
        tdlib_single_rpc_receive_timeout_sec=0,
    )

    assert report["contract_status"] == "public_username_resolve_partial"
    assert transport.events.index("send:searchPublicChat") < transport.events.index(
        "receive:updateConnectionState"
    )
    assert report["resolve_response_timeout_count_bucket"] == "one"
    assert report["failed_resolve_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["response_timeout"]
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert len(transport.receive_timeouts) == 3
    assert _sent_request_types(transport) == ["searchPublicChat"]
    assert db.update_attempts == 0
    assert report["registry_resolve_mutation_performed"] is False


def test_normal_no_mutation_resolve_duration_exhaustion_is_response_timeout(
    tmp_path: Path,
) -> None:
    clock_values = iter([0.0, 0.0, 2.0])

    def monotonic_clock() -> float:
        return next(clock_values, 2.0)

    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        tdlib_single_rpc_max_updates=50,
        tdlib_single_rpc_receive_timeout_sec=1.0,
        tdlib_single_rpc_max_duration_sec=1.0,
        monotonic_clock=monotonic_clock,
    )

    assert report["contract_status"] == "public_username_resolve_partial"
    assert report["resolve_response_timeout_count_bucket"] == "one"
    assert report["failed_resolve_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["response_timeout"]
    assert report["resolve_response_extra_matched_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert transport.receive_timeouts[1:] == [1.0]
    assert _sent_request_types(transport) == ["searchPublicChat"]
    assert db.update_attempts == 0


def test_normal_no_mutation_resolve_counts_wrong_and_without_extra_responses(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            {"@type": "ok", "@extra": "stale-public-username-extra"},
            {
                "@type": "chat",
                "id": RAW_CHAT_ID,
                "title": RAW_TDLIB_PAYLOAD_VALUE,
                "type": {"@type": "chatTypeSupergroup", "is_channel": True},
            },
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )
    rendered = _render(report)

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert report["tdlib_post_ready_drain_function_response_types_seen"] == []
    assert report["resolve_function_response_types_seen"] == ["ok", "chat"]
    assert report["resolve_response_wrong_extra_count_bucket"] == "one"
    assert report["resolve_response_without_extra_count_bucket"] == "one"
    assert report["resolve_response_extra_matched_count_bucket"] == "one"
    assert db.update_attempts == 0
    assert "stale-public-username-extra" not in rendered
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_normal_mutation_resolve_authorization_lost_rolls_back_prior_update(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
            _auth_update("authorizationStateClosing"),
        ]
    )
    db = FakeDatabaseConnection(
        [
            _registry_row("registry-1", RAW_PUBLIC_USERNAME),
            _registry_row("registry-2", RAW_PUBLIC_USERNAME_TWO),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        db=db,
        approved_mutation=True,
    )

    assert report["contract_status"] == "public_username_resolve_partial"
    assert _sent_request_types(transport) == ["searchPublicChat", "searchPublicChat"]
    assert db.update_attempts == 1
    assert db.transaction.rolled_back is True
    assert db.transaction.committed is False
    assert report["resolve_authorization_lost_count_bucket"] == "one"
    assert report["resolve_failure_classes_seen"] == ["authorization_lost"]
    assert report["updated_row_count_bucket"] == "zero"
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False
    assert report["tdlib_post_ready_drain_attempted"] is False


def test_normal_no_mutation_resolve_keeps_post_ready_drain_report_not_attempted(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
    )

    assert report["contract_status"] == "public_username_resolve_completed_no_mutation"
    assert transport.events.index("send:searchPublicChat") < transport.events.index(
        "receive:none"
    )
    assert report["tdlib_post_ready_drain_attempted"] is False
    assert report["tdlib_post_ready_drain_receive_attempt_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_observation_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_empty_receive_count_bucket"] == "zero"
    assert report["tdlib_post_ready_drain_quiet_window_reached"] is False
    assert report["tdlib_post_ready_drain_budget_exhausted"] is False
    assert report["tdlib_post_ready_drain_authorization_lost"] is False
    assert report["tdlib_post_ready_drain_update_types_seen"] == []
    assert report["resolve_function_response_types_seen"] == ["chat"]
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_single_resolve_rpc_diagnostic_sends_one_search_after_readiness(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
    )
    rendered = _render(report)

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_completed"
    assert _sent_request_types(transport) == ["searchPublicChat"]
    assert report["single_resolve_rpc_diagnostic_enabled"] is True
    assert report["single_resolve_rpc_target_selected"] is True
    assert report["single_resolve_rpc_target_index_bucket"] == "one"
    assert report["single_resolve_rpc_request_sent"] is True
    assert report["single_resolve_rpc_request_extra_present"] is True
    assert report["single_resolve_rpc_response_extra_matched"] is True
    assert report["single_resolve_rpc_result_class"] == "resolved"
    assert report["single_resolve_rpc_function_response_types_seen"] == ["chat"]
    assert report["single_resolve_rpc_receive_attempt_count_bucket"] == "one"
    assert report["single_resolve_rpc_observation_count_bucket"] == "one"
    assert report["tdlib_resolve_attempted"] is True
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["tdlib_public_username_resolve_called"] is True
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_single_resolve_rpc_diagnostic_forces_no_mutation_even_when_requested(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
        diagnose_single_resolve_rpc=True,
    )

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_completed"
    assert report["approved_registry_resolve_mutation"] is True
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_updated"] is False
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True
    assert db.transaction.committed is False
    assert any(
        statement == _normalize(_module().SET_TRANSACTION_READ_ONLY_QUERY)
        for statement in db.statements
    )


def test_single_resolve_rpc_diagnostic_timeout_reports_no_function_response(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
        diagnose_single_resolve_rpc=True,
        tdlib_single_rpc_max_updates=2,
        tdlib_single_rpc_receive_timeout_sec=0,
        tdlib_single_rpc_max_duration_sec=60.0,
    )

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_response_timeout"
    assert report["single_resolve_rpc_timed_out"] is True
    assert report["single_resolve_rpc_result_class"] == "response_timeout"
    assert report["single_resolve_rpc_update_budget_exhausted"] is True
    assert report["single_resolve_rpc_duration_exhausted"] is False
    assert report["single_resolve_rpc_function_response_types_seen"] == []
    assert report["single_resolve_rpc_response_extra_matched"] is False
    assert report["single_resolve_rpc_receive_attempt_count_bucket"] == "two_to_five"
    assert report["single_resolve_rpc_observation_count_bucket"] == "zero"
    assert report["single_resolve_rpc_empty_receive_count_bucket"] == "two_to_five"
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0


def test_single_resolve_rpc_diagnostic_reports_duration_exhausted_first(
    tmp_path: Path,
) -> None:
    clock_values = iter([0.0, 0.0, 2.0])

    def monotonic_clock() -> float:
        return next(clock_values, 2.0)

    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            _public_chat_response(),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
        tdlib_single_rpc_max_updates=50,
        tdlib_single_rpc_receive_timeout_sec=1.0,
        tdlib_single_rpc_max_duration_sec=1.0,
        monotonic_clock=monotonic_clock,
    )

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_response_timeout"
    assert report["single_resolve_rpc_timed_out"] is True
    assert report["single_resolve_rpc_update_budget_exhausted"] is False
    assert report["single_resolve_rpc_duration_exhausted"] is True
    assert report["single_resolve_rpc_receive_attempt_count_bucket"] == "one"
    assert report["single_resolve_rpc_empty_receive_count_bucket"] == "one"
    assert report["single_resolve_rpc_response_extra_matched"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0


def test_single_resolve_rpc_diagnostic_wrong_extra_chat_is_bucketed_without_match(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
            None,
            {
                "@type": "chat",
                "@extra": "wrong-public-username-extra",
                "id": RAW_CHAT_ID,
                "title": RAW_TDLIB_PAYLOAD_VALUE,
                "type": {"@type": "chatTypeSupergroup", "is_channel": True},
            },
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
        tdlib_single_rpc_max_updates=4,
        tdlib_single_rpc_receive_timeout_sec=0,
    )
    rendered = _render(report)

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_response_timeout"
    assert report["single_resolve_rpc_result_class"] == "response_timeout"
    assert report["single_resolve_rpc_update_budget_exhausted"] is True
    assert report["single_resolve_rpc_duration_exhausted"] is False
    assert report["single_resolve_rpc_response_extra_matched"] is False
    assert report["single_resolve_rpc_response_wrong_extra_count_bucket"] == "one"
    assert report["single_resolve_rpc_response_without_extra_count_bucket"] == "zero"
    assert report["single_resolve_rpc_function_response_types_seen"] == ["chat"]
    assert db.update_attempts == 0
    assert "wrong-public-username-extra" not in rendered
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_single_resolve_rpc_diagnostic_response_without_extra_is_bucketed_without_match(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
            None,
            {
                "@type": "chat",
                "id": RAW_CHAT_ID,
                "title": RAW_TDLIB_PAYLOAD_VALUE,
                "type": {"@type": "chatTypeSupergroup", "is_channel": True},
            },
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
        tdlib_single_rpc_max_updates=4,
        tdlib_single_rpc_receive_timeout_sec=0,
    )
    rendered = _render(report)

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_response_timeout"
    assert report["single_resolve_rpc_result_class"] == "response_timeout"
    assert report["single_resolve_rpc_update_budget_exhausted"] is True
    assert report["single_resolve_rpc_duration_exhausted"] is False
    assert report["single_resolve_rpc_response_extra_matched"] is False
    assert report["single_resolve_rpc_response_without_extra_count_bucket"] == "one"
    assert report["single_resolve_rpc_response_wrong_extra_count_bucket"] == "zero"
    assert report["single_resolve_rpc_function_response_types_seen"] == ["chat"]
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_single_resolve_rpc_diagnostic_authorization_lost_reports_no_mutation(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            None,
            None,
            None,
            _auth_update("authorizationStateClosing"),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=True,
        diagnose_single_resolve_rpc=True,
    )

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_authorization_lost"
    assert report["single_resolve_rpc_result_class"] == "authorization_lost"
    assert report["single_resolve_rpc_timed_out"] is False
    assert report["single_resolve_rpc_authorization_states_seen"] == [
        "authorizationStateClosing"
    ]
    assert report["single_resolve_rpc_final_authorization_state"] == (
        "authorizationStateClosing"
    )
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert db.update_attempts == 0
    assert db.transaction.rolled_back is True


def test_single_resolve_rpc_diagnostic_tdlib_error_reports_safe_code_only(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateReady"),
            _public_chat_error_response(
                code="SAFE_PUBLIC_CHAT_ERROR",
                message=(
                    f"{RAW_PUBLIC_USERNAME} {RAW_CHAT_ID} "
                    f"{FAKE_TELEGRAM_SECRET} {RAW_TDLIB_PAYLOAD_VALUE}"
                ),
            ),
        ]
    )

    report, db, _resolver = _run_report_with_tdlib_transport(
        tmp_path=tmp_path,
        transport=transport,
        approved_mutation=False,
        diagnose_single_resolve_rpc=True,
    )
    rendered = _render(report)

    assert report["contract_status"] == "single_resolve_rpc_diagnostic_completed"
    assert report["single_resolve_rpc_result_class"] == "tdlib_error"
    assert report["single_resolve_rpc_tdlib_error_codes_seen"] == [
        "SAFE_PUBLIC_CHAT_ERROR"
    ]
    assert report["single_resolve_rpc_function_response_types_seen"] == ["error"]
    assert report["single_resolve_rpc_response_extra_matched"] is True
    assert "SAFE_PUBLIC_CHAT_ERROR" in rendered
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(rendered, tmp_path)


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
    assert FAKE_REDIS_URL not in rendered
    assert FAKE_DATABASE_PASSWORD not in rendered
    assert FAKE_TELEGRAM_SECRET not in rendered
    assert RAW_TDLIB_PAYLOAD_VALUE not in rendered


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
        "getChatHistory",
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
        _assert_post_ready_drain_fields_present(report)
        _assert_sync_settle_diagnostic_fields_present(report)
        _assert_resolve_classification_fields_present(report)
        _assert_single_resolve_rpc_diagnostic_fields_present(report)

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
    assert resolver.drain_calls == 0
    assert db.update_attempts == 0
    _assert_ready_probe_fields_present(report)
    _assert_post_ready_drain_fields_present(report)
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


def test_exact_mode_cli_is_source_selection_exclusive_and_reuses_strict_locator_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    parsed = module.build_parser().parse_args(
        [
            "--runtime-env-path",
            "/safe/unit/runtime.env",
            "--target-locator-path",
            RAW_LOCATOR_PATH,
            "--approved-tdlib-public-username-resolve",
            "--approved-registry-resolve-mutation",
        ]
    )
    assert parsed.target_locator_path == RAW_LOCATOR_PATH
    with pytest.raises(SystemExit):
        module.build_parser().parse_args(
            [
                "--runtime-env-path",
                "/safe/unit/runtime.env",
                "--target-locator-path",
                RAW_LOCATOR_PATH,
                "--limit",
                "1",
            ]
        )

    report, _db, _resolver, locator_calls, _events = _run_exact_report(monkeypatch)
    assert report["contract_status"] == "exact_target_resolved_not_joined_updated"
    assert locator_calls == [RAW_LOCATOR_PATH]

    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    assert not any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_read_target_locator"
        for node in ast.walk(tree)
    )
    assert "bounded_history_ingest_runner._read_target_locator" in SCRIPT.read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize(
    ("dry_run", "approved_tdlib", "approved_mutation"),
    (
        (True, True, True),
        (False, False, True),
        (False, True, False),
    ),
)
def test_exact_mode_requires_non_dry_run_and_both_existing_approvals(
    monkeypatch: pytest.MonkeyPatch,
    dry_run: bool,
    approved_tdlib: bool,
    approved_mutation: bool,
) -> None:
    report, db, resolver, locator_calls, _events = _run_exact_report(
        monkeypatch,
        dry_run=dry_run,
        approved_tdlib=approved_tdlib,
        approved_mutation=approved_mutation,
    )

    assert report["contract_status"] == "blocked_approval_required"
    assert locator_calls == []
    assert resolver.initialized is False
    assert resolver.calls == []
    assert db.transactions == []
    assert db.update_attempts == 0


def test_exact_locator_validation_error_is_sanitized_and_precedes_runtime_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()

    def invalid_locator(_path: object) -> dict[str, Any]:
        raise module.bounded_history_ingest_runner.BoundedHistoryIngestError(
            f"invalid {RAW_LOCATOR_PATH} {RAW_PUBLIC_USERNAME} {RAW_REGISTRY_ID}"
        )

    monkeypatch.setattr(
        module.bounded_history_ingest_runner,
        "_read_target_locator",
        invalid_locator,
    )
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        target_locator_path=RAW_LOCATOR_PATH,
        dry_run=False,
        approved_tdlib_public_username_resolve=True,
        approved_registry_resolve_mutation=True,
        runtime_env_reader=lambda _path: (_ for _ in ()).throw(
            AssertionError("runtime env must not be read after invalid locator")
        ),
    )
    rendered = _render(result.report)

    assert result.report["contract_status"] == "blocked_target_locator_invalid"
    assert result.report["runtime_env_read"] is False
    assert result.report["target_locator_read"] is False
    _assert_sensitive_values_absent(rendered)


@pytest.mark.parametrize(
    "diagnostic_kwargs",
    (
        {"diagnose_single_resolve_rpc": True},
        {"diagnose_tdlib_post_ready_sync_settle": True},
    ),
)
def test_exact_mode_blocks_broad_diagnostics_before_locator_or_db(
    monkeypatch: pytest.MonkeyPatch,
    diagnostic_kwargs: dict[str, bool],
) -> None:
    report, db, resolver, locator_calls, _events = _run_exact_report(
        monkeypatch,
        **diagnostic_kwargs,
    )

    assert report["contract_status"] == "blocked_exact_target_mode_conflict"
    assert locator_calls == []
    assert db.transactions == []
    assert resolver.calls == []


def test_broad_mode_never_reads_locator_and_keeps_existing_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()

    def forbidden_reader(_path: object) -> dict[str, Any]:
        raise AssertionError("broad mode must not read a target locator")

    monkeypatch.setattr(
        module.bounded_history_ingest_runner,
        "_read_target_locator",
        forbidden_reader,
    )
    report, db, resolver = _run_report(
        resolver=FakeResolver({RAW_PUBLIC_USERNAME: _resolved()}),
        dry_run=False,
        approved_tdlib=True,
        approved_mutation=True,
    )

    assert report["contract_status"] == "public_username_resolve_registry_updated"
    assert report["exact_target_mode"] is False
    assert resolver is not None and resolver.calls == [RAW_PUBLIC_USERNAME]
    assert db.update_attempts == 1


@pytest.mark.parametrize(
    ("rows", "expected_status"),
    (
        ([], "blocked_exact_target_missing"),
        (
            [
                _exact_registry_row(),
                _exact_registry_row(
                    registry_id="22222222-2222-4222-8222-222222222222"
                ),
            ],
            "blocked_exact_target_ambiguous",
        ),
        (
            [_exact_registry_row(registry_id="not-a-valid-registry-uuid")],
            "blocked_exact_target_invalid",
        ),
        (
            [_exact_registry_row(chat_id=None)],
            "blocked_exact_target_invalid",
        ),
        (
            [_exact_registry_row(access_state="forbidden")],
            "blocked_exact_target_invalid",
        ),
    ),
)
def test_exact_selection_cardinality_and_invalid_rows_block_before_tdlib(
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, Any]],
    expected_status: str,
) -> None:
    db = FakeDatabaseConnection(rows)
    report, db, resolver, locator_calls, _events = _run_exact_report(
        monkeypatch,
        db=db,
    )

    assert report["contract_status"] == expected_status
    assert locator_calls == [RAW_LOCATOR_PATH]
    assert resolver.initialized is False
    assert resolver.calls == []
    assert db.update_attempts == 0
    assert len(db.transactions) == 1
    assert db.transactions[0].rolled_back is True


def test_exact_locator_and_db_source_mismatch_blocks_before_tdlib(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_select_source_value_override="DifferentPublicChannel",
    )
    report, db, resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        db=db,
    )

    assert report["contract_status"] == "blocked_exact_target_source_mismatch"
    assert resolver.initialized is False
    assert resolver.calls == []
    assert db.update_attempts == 0


@pytest.mark.parametrize(
    ("stored_source_value", "resolved_chat_id"),
    (
        (RAW_PUBLIC_USERNAME, RAW_OLD_CHAT_ID),
        (RAW_PUBLIC_USERNAME, RAW_NEW_CHAT_ID),
        ("https://t.me/PrivateAlphaChannel", RAW_NEW_CHAT_ID),
        ("http://t.me/PrivateAlphaChannel", RAW_NEW_CHAT_ID),
        ("t.me/@PrivateAlphaChannel", RAW_NEW_CHAT_ID),
        ("@PrivateAlphaChannel", RAW_NEW_CHAT_ID),
    ),
)
def test_exact_resolve_same_or_changed_chat_commits_guarded_source_bound_readback(
    monkeypatch: pytest.MonkeyPatch,
    stored_source_value: str,
    resolved_chat_id: int,
) -> None:
    events: list[str] = []
    unrelated = _registry_row(
        "33333333-3333-4333-8333-333333333333",
        RAW_PUBLIC_USERNAME_TWO,
        access_state="joined",
        chat_id=-1005555555555,
    )
    db = FakeDatabaseConnection(
        [_exact_registry_row(source_value=stored_source_value), unrelated],
        events=events,
    )
    resolver = FakeResolver(
        {"privatealphachannel": _resolved(chat_id=resolved_chat_id)},
        events=events,
    )

    report, db, resolver, locator_calls, events = _run_exact_report(
        monkeypatch,
        db=db,
        resolver=resolver,
    )

    assert report["contract_status"] == "exact_target_resolved_not_joined_updated"
    assert report["exact_target_durable_readback_matched"] is True
    assert report["exact_target_mutation_outcome"] == "committed_durable"
    assert report["exact_target_cleanup_failure_codes"] == []
    assert report["exact_target_read_rollback_succeeded"] is True
    assert report["exact_target_mutation_rollback_succeeded"] is None
    assert report["exact_target_transport_close_succeeded"] is True
    assert report["exact_target_connection_cleanup_succeeded"] is True
    assert report["registry_resolve_mutation_performed"] is True
    assert locator_calls == [RAW_LOCATOR_PATH]
    assert resolver.calls == ["privatealphachannel"]
    assert db.update_attempts == 1
    assert db.updated_rows == 1
    assert db.rows[0]["registry_id"] == RAW_REGISTRY_ID
    assert db.rows[0]["chat_id"] == resolved_chat_id
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert db.rows[1] == unrelated
    assert len(db.transactions) == 2
    assert db.transactions[0].rolled_back is True
    assert db.transactions[0].committed is False
    assert db.transactions[1].committed is True
    assert events.index("transaction:rollback") < events.index("resolver:initialize")
    assert events.index("resolver:initialize") < events.index("resolver:resolve")
    transaction_begin_indexes = [
        index for index, event in enumerate(events) if event == "transaction:begin"
    ]
    assert len(transaction_begin_indexes) == 2
    assert events.index("resolver:resolve") < transaction_begin_indexes[1]
    assert transaction_begin_indexes[1] < events.index("database:exact_update")
    assert events.index("database:exact_update") < events.index("database:exact_readback")
    assert events.index("database:exact_readback") < events.index("transaction:commit")

    update_query = _module().UPDATE_EXACT_JOINED_REGISTRY_ROW_QUERY
    update_index = db.statements.index(_normalize(update_query))
    update_params = db.params[update_index]
    assert update_params["registry_id"] == RAW_REGISTRY_ID
    assert update_params["source_value"] == stored_source_value
    assert update_params["normalized_source_value"] == "privatealphachannel"
    assert update_params["old_chat_id"] == RAW_OLD_CHAT_ID
    assert update_params["chat_id"] == resolved_chat_id

    update_sql = _normalize(update_query).upper()
    assert "WHERE REGISTRY_ID = :REGISTRY_ID" in update_sql
    assert "SOURCE_VALUE = :SOURCE_VALUE" in update_sql
    assert ":NORMALIZED_SOURCE_VALUE" in update_sql
    assert "DESIRED_STATE = 'ACTIVE'" in update_sql
    assert "ACCESS_STATE = 'JOINED'" in update_sql
    assert "CHAT_ID = :OLD_CHAT_ID" in update_sql
    for prefix in ("HTTPS://T[.]ME/", "HTTP://T[.]ME/", "T[.]ME/", "^@"):
        assert prefix in update_sql

    readback_sql = _normalize(_module().SELECT_EXACT_RESOLVED_READBACK_QUERY).upper()
    assert "REGISTRY_ID = :REGISTRY_ID" in readback_sql
    assert "SOURCE_VALUE = :SOURCE_VALUE" in readback_sql
    assert ":NORMALIZED_SOURCE_VALUE" in readback_sql
    assert "ACCESS_STATE = 'RESOLVED_NOT_JOINED'" in readback_sql
    _assert_sensitive_values_absent(_render(report))


@pytest.mark.parametrize(
    "failure_class",
    (
        "not_found",
        "access_denied",
        "unsupported_chat_type",
        "response_timeout",
        "transport_error",
        "tdlib_error",
        "response_shape_error",
        "authorization_lost",
        "unknown_error",
    ),
)
def test_every_exact_resolve_failure_class_has_no_mutation_transaction(
    monkeypatch: pytest.MonkeyPatch,
    failure_class: str,
) -> None:
    module = _module()
    resolver = FakeResolver(
        {
            "privatealphachannel": module.PublicUsernameResolveResult(
                status=failure_class,
                failure_class=failure_class,
            )
        }
    )

    report, db, resolver, _locator_calls, events = _run_exact_report(
        monkeypatch,
        resolver=resolver,
    )

    assert report["contract_status"] == "exact_target_resolve_failed"
    assert report[f"resolve_{failure_class}_count_bucket"] == "one"
    assert resolver.calls == ["privatealphachannel"]
    assert db.update_attempts == 0
    assert len(db.transactions) == 1
    assert db.transactions[0].rolled_back is True
    assert "database:exact_update" not in events
    assert report["side_effects"]["database_mutation_performed"] is False


@pytest.mark.parametrize(
    ("db_kwargs", "expected_status"),
    (
        (
            {"break_exact_update_guard": True},
            "blocked_exact_target_concurrent_mismatch",
        ),
        (
            {"exact_update_rowcount_override": 2},
            "blocked_exact_target_concurrent_mismatch",
        ),
        (
            {"break_exact_readback": True},
            "blocked_exact_target_readback_mismatch",
        ),
    ),
)
def test_exact_update_count_concurrency_and_readback_failures_roll_back(
    monkeypatch: pytest.MonkeyPatch,
    db_kwargs: dict[str, Any],
    expected_status: str,
) -> None:
    db = FakeDatabaseConnection([_exact_registry_row()], **db_kwargs)
    report, db, _resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        db=db,
    )

    assert report["contract_status"] == expected_status
    assert db.update_attempts == 1
    assert len(db.transactions) == 2
    assert db.transactions[1].rolled_back is True
    assert db.transactions[1].committed is False
    assert report["exact_target_mutation_outcome"] == "rolled_back"
    assert report["exact_target_cleanup_failure_codes"] == []
    assert report["exact_target_mutation_rollback_succeeded"] is True
    assert db.rows[0]["chat_id"] == RAW_OLD_CHAT_ID
    assert db.rows[0]["access_state"] == "joined"
    assert db.updated_rows == 0
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["exact_target_durable_readback_matched"] is False
    _assert_sensitive_values_absent(_render(report))


def test_exact_resolved_not_joined_rerun_is_sanitized_no_rpc_no_mutation_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row(access_state="resolved_not_joined", chat_id=RAW_NEW_CHAT_ID)]
    )
    report, db, resolver, locator_calls, events = _run_exact_report(
        monkeypatch,
        db=db,
    )
    rendered = _render(report)

    assert report["contract_status"] == "exact_target_already_resolved_not_joined_noop"
    assert report["exact_target_noop"] is True
    assert report["exact_target_mutation_outcome"] == "not_attempted"
    assert report["exact_target_cleanup_failure_codes"] == []
    assert report["exact_target_read_rollback_succeeded"] is True
    assert report["exact_target_mutation_rollback_succeeded"] is None
    assert report["exact_target_transport_close_succeeded"] is None
    assert report["exact_target_connection_cleanup_succeeded"] is True
    assert locator_calls == [RAW_LOCATOR_PATH]
    assert resolver.initialized is False
    assert resolver.calls == []
    assert db.update_attempts == 0
    assert len(db.transactions) == 1
    assert db.transactions[0].rolled_back is True
    assert "resolver:initialize" not in events
    assert "resolver:resolve" not in events
    assert "database:exact_update" not in events
    _assert_sensitive_values_absent(rendered)


def test_exact_read_rollback_failure_blocks_resolver_and_reports_sanitized_cleanup_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = FakeDatabaseConnection([_exact_registry_row()], fail_read_rollback=True)
    result_holder: list[Any] = []
    report, db, resolver, _locator_calls, events = _run_exact_report(
        monkeypatch,
        db=db,
        result_holder=result_holder,
    )

    assert result_holder[0].exit_code != 0
    assert report["contract_status"] == "blocked_exact_target_read_rollback_failed"
    assert report["exact_target_mutation_outcome"] == "not_attempted"
    assert report["exact_target_cleanup_failure_codes"] == ["read_rollback_failed"]
    assert report["exact_target_read_rollback_succeeded"] is False
    assert report["exact_target_mutation_rollback_succeeded"] is None
    assert report["exact_target_transport_close_succeeded"] is None
    assert report["exact_target_connection_cleanup_succeeded"] is True
    assert db.transactions[0].rollback_attempted is True
    assert db.transactions[0].rollback_succeeded is False
    assert resolver.initialized is False
    assert resolver.calls == []
    assert resolver.close_attempted is False
    assert len(db.transactions) == 1
    assert db.update_attempts == 0
    assert "resolver:initialize" not in events
    assert "resolver:resolve" not in events
    assert "database:exact_update" not in events
    _assert_sensitive_values_absent(_render(report))


def test_exact_update_mismatch_rollback_failure_reports_unknown_sanitized_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        exact_update_rowcount_override=2,
        fail_mutation_rollback=True,
    )
    result_holder: list[Any] = []
    report, db, _resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        db=db,
        result_holder=result_holder,
    )

    mutation_transaction = db.transactions[1]
    assert result_holder[0].exit_code != 0
    assert (
        report["contract_status"]
        == "blocked_exact_target_mutation_rollback_failed"
    )
    assert report["exact_target_mutation_outcome"] == "unknown_after_rollback_failure"
    assert report["exact_target_cleanup_failure_codes"] == [
        "mutation_rollback_failed"
    ]
    assert report["exact_target_mutation_rollback_succeeded"] is False
    assert mutation_transaction.commit_called is False
    assert mutation_transaction.rollback_attempted is True
    assert mutation_transaction.rollback_succeeded is False
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["exact_target_durable_readback_matched"] is False
    _assert_sensitive_values_absent(_render(report))


def test_exact_readback_mismatch_rollback_failure_reports_unknown_sanitized_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        break_exact_readback=True,
        fail_mutation_rollback=True,
    )
    result_holder: list[Any] = []
    report, db, _resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        db=db,
        result_holder=result_holder,
    )

    mutation_transaction = db.transactions[1]
    assert result_holder[0].exit_code != 0
    assert (
        report["contract_status"]
        == "blocked_exact_target_mutation_rollback_failed"
    )
    assert report["exact_target_mutation_outcome"] == "unknown_after_rollback_failure"
    assert report["exact_target_cleanup_failure_codes"] == [
        "mutation_rollback_failed"
    ]
    assert report["exact_target_mutation_rollback_succeeded"] is False
    assert mutation_transaction.commit_called is False
    assert mutation_transaction.rollback_attempted is True
    assert mutation_transaction.rollback_succeeded is False
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["exact_target_durable_readback_matched"] is False
    _assert_sensitive_values_absent(_render(report))


def test_production_resolver_close_failure_reaches_exact_cleanup_wrapper(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    transport = FakeTDLibTransport([], fail_close=True)
    resolver = module.TDLibPublicUsernameResolver(
        _runtime_env_for_tdlib_transport(tmp_path),
        transport=transport,
    )
    report: dict[str, Any] = {
        "exact_target_cleanup_failure_codes": [],
        "exact_target_transport_close_succeeded": None,
    }

    module._attempt_exact_target_transport_close(report, resolver)
    captured = capsys.readouterr()
    rendered = module.render_json(report)
    observed_output = "\n".join((rendered, captured.out, captured.err))

    assert transport.close_attempted is True
    assert transport.closed is False
    assert report["exact_target_transport_close_succeeded"] is False
    assert report["exact_target_cleanup_failure_codes"] == [
        "transport_close_failed"
    ]
    assert captured.out == ""
    assert captured.err == ""
    for unsafe_text in (
        "RuntimeError",
        "TDLibTransportError",
        "Traceback",
        "Failed to close TDLib transport",
    ):
        assert unsafe_text not in observed_output
    _assert_sensitive_values_absent(observed_output, tmp_path)


def test_exact_resolver_close_failure_before_commit_preserves_primary_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = FakeResolver(
        {"privatealphachannel": _not_found()},
        fail_close=True,
    )
    result_holder: list[Any] = []
    report, db, resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        resolver=resolver,
        result_holder=result_holder,
    )

    assert result_holder[0].exit_code != 0
    assert report["contract_status"] == "exact_target_resolve_failed"
    assert report["exact_target_mutation_outcome"] == "not_attempted"
    assert report["exact_target_cleanup_failure_codes"] == ["transport_close_failed"]
    assert report["exact_target_read_rollback_succeeded"] is True
    assert report["exact_target_mutation_rollback_succeeded"] is None
    assert report["exact_target_transport_close_succeeded"] is False
    assert report["exact_target_connection_cleanup_succeeded"] is True
    assert resolver.close_attempted is True
    assert db.close_attempted is True
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(_render(report))


def test_exact_connection_cleanup_failure_before_commit_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = FakeDatabaseConnection([_exact_registry_row()], fail_close=True)
    resolver = FakeResolver({"privatealphachannel": _not_found()})
    result_holder: list[Any] = []
    report, db, resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        db=db,
        resolver=resolver,
        result_holder=result_holder,
    )

    assert result_holder[0].exit_code != 0
    assert (
        report["contract_status"]
        == "blocked_exact_target_connection_cleanup_failed"
    )
    assert report["exact_target_mutation_outcome"] == "not_attempted"
    assert report["exact_target_cleanup_failure_codes"] == [
        "connection_cleanup_failed"
    ]
    assert report["exact_target_read_rollback_succeeded"] is True
    assert report["exact_target_mutation_rollback_succeeded"] is None
    assert report["exact_target_transport_close_succeeded"] is True
    assert report["exact_target_connection_cleanup_succeeded"] is False
    assert resolver.close_attempted is True
    assert db.close_attempted is True
    assert db.update_attempts == 0
    _assert_sensitive_values_absent(_render(report))


def test_exact_resolver_close_failure_after_commit_preserves_durable_success_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    original_transport_close = module._attempt_exact_target_transport_close
    transport_close_calls: list[Any] = []

    def track_exact_transport_close(
        report: dict[str, Any],
        resolver: Any,
    ) -> None:
        transport_close_calls.append(resolver)
        original_transport_close(report, resolver)

    monkeypatch.setattr(
        module,
        "_attempt_exact_target_transport_close",
        track_exact_transport_close,
    )
    resolver = FakeResolver(
        {"privatealphachannel": _resolved(chat_id=RAW_NEW_CHAT_ID)},
        fail_close=True,
    )
    result_holder: list[Any] = []
    report, db, resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        resolver=resolver,
        result_holder=result_holder,
    )

    assert result_holder[0].exit_code != 0
    assert report["contract_status"] == "exact_target_cleanup_failed_after_commit"
    assert report["exact_target_mutation_outcome"] == "committed_durable"
    assert report["exact_target_cleanup_failure_codes"] == ["transport_close_failed"]
    assert report["exact_target_durable_readback_matched"] is True
    assert report["registry_resolve_mutation_performed"] is True
    assert report["side_effects"]["database_mutation_performed"] is True
    assert report["side_effects"]["telegram_channel_registry_updated"] is True
    assert report["exact_target_transport_close_succeeded"] is False
    assert report["exact_target_connection_cleanup_succeeded"] is True
    assert db.transactions[1].commit_called is True
    assert db.transactions[1].committed is True
    assert db.transactions[1].rollback_attempted is False
    assert resolver.close_attempted is True
    assert transport_close_calls == [resolver]
    _assert_sensitive_values_absent(_render(report))


def test_exact_connection_cleanup_failure_after_commit_preserves_durable_success_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = FakeDatabaseConnection([_exact_registry_row()], fail_close=True)
    result_holder: list[Any] = []
    report, db, resolver, _locator_calls, _events = _run_exact_report(
        monkeypatch,
        db=db,
        result_holder=result_holder,
    )

    assert result_holder[0].exit_code != 0
    assert report["contract_status"] == "exact_target_cleanup_failed_after_commit"
    assert report["exact_target_mutation_outcome"] == "committed_durable"
    assert report["exact_target_cleanup_failure_codes"] == [
        "connection_cleanup_failed"
    ]
    assert report["exact_target_durable_readback_matched"] is True
    assert report["registry_resolve_mutation_performed"] is True
    assert report["side_effects"]["database_mutation_performed"] is True
    assert report["side_effects"]["telegram_channel_registry_updated"] is True
    assert report["exact_target_transport_close_succeeded"] is True
    assert report["exact_target_connection_cleanup_succeeded"] is False
    assert db.transactions[1].commit_called is True
    assert db.transactions[1].committed is True
    assert db.transactions[1].rollback_attempted is False
    assert resolver.close_attempted is True
    assert db.close_attempted is True
    _assert_sensitive_values_absent(_render(report))


def test_exact_multiple_cleanup_failures_collect_deterministic_codes_and_attempt_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    db = FakeDatabaseConnection(
        [_exact_registry_row()],
        events=events,
        break_exact_readback=True,
        fail_mutation_rollback=True,
        fail_close=True,
    )
    resolver = FakeResolver(
        {"privatealphachannel": _resolved(chat_id=RAW_NEW_CHAT_ID)},
        fail_close=True,
        events=events,
    )
    result_holder: list[Any] = []
    report, db, resolver, _locator_calls, events = _run_exact_report(
        monkeypatch,
        db=db,
        resolver=resolver,
        result_holder=result_holder,
    )

    assert result_holder[0].exit_code != 0
    assert (
        report["contract_status"]
        == "blocked_exact_target_mutation_rollback_failed"
    )
    assert report["exact_target_mutation_outcome"] == "unknown_after_rollback_failure"
    assert report["exact_target_cleanup_failure_codes"] == [
        "mutation_rollback_failed",
        "transport_close_failed",
        "connection_cleanup_failed",
    ]
    assert report["exact_target_mutation_rollback_succeeded"] is False
    assert report["exact_target_transport_close_succeeded"] is False
    assert report["exact_target_connection_cleanup_succeeded"] is False
    assert db.transactions[1].commit_called is False
    assert db.transactions[1].rollback_attempted is True
    assert db.transactions[1].rollback_succeeded is False
    assert resolver.close_attempted is True
    assert db.close_attempted is True
    assert events.index("transaction:mutation:rollback_attempt") < events.index(
        "resolver:close_attempt"
    )
    assert events.index("resolver:close_attempt") < events.index(
        "database:close_attempt"
    )
    assert db.rows[0]["access_state"] == "resolved_not_joined"
    assert report["registry_resolve_mutation_performed"] is False
    assert report["side_effects"]["database_mutation_performed"] is False
    _assert_sensitive_values_absent(_render(report))


def test_exact_cli_emergency_firewall_emits_one_sanitized_json_object_without_stderr(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()

    def fail_generate_report(**_kwargs: Any) -> Any:
        raise RuntimeError(CLEANUP_EXCEPTION_MESSAGE)

    monkeypatch.setattr(module, "generate_report", fail_generate_report)

    with pytest.raises(RuntimeError):
        module.main(["--runtime-env-path", "/safe/unit/runtime.env"])
    capsys.readouterr()

    exit_code = module.main(
        [
            "--runtime-env-path",
            "/safe/unit/runtime.env",
            "--target-locator-path",
            RAW_LOCATOR_PATH,
            "--approved-tdlib-public-username-resolve",
            "--approved-registry-resolve-mutation",
        ]
    )
    captured = capsys.readouterr()
    report = json.loads(captured.out)

    assert exit_code != 0
    assert report["contract_status"] == "blocked_exact_target_unhandled_failure"
    assert report["exact_target_mutation_outcome"] == "not_attempted"
    assert report["exact_target_cleanup_failure_codes"] == []
    assert captured.out.endswith("\n")
    assert captured.err == ""
    assert "Traceback" not in captured.out
    _assert_sensitive_values_absent(captured.out)
