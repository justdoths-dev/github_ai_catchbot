from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_telegram_channel_registry_public_username_resolve_operator"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 200
DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC = 240.0
DEFAULT_TDLIB_RPC_TIMEOUT_SEC = 15.0
DEFAULT_TDLIB_RPC_MAX_UPDATES = 120
DEFAULT_TDLIB_SINGLE_RPC_MAX_UPDATES = DEFAULT_TDLIB_RPC_MAX_UPDATES
DEFAULT_TDLIB_SINGLE_RPC_RECEIVE_TIMEOUT_SEC = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC
DEFAULT_TDLIB_SINGLE_RPC_MAX_DURATION_SEC = 60.0
DEFAULT_TDLIB_POST_READY_DRAIN_MAX_UPDATES = 200
DEFAULT_TDLIB_POST_READY_DRAIN_TIMEOUT_SEC = 0.0
DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_EMPTY_RECEIVES = 3
DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_TIMEOUT_SEC = 0.25
DEFAULT_TDLIB_SYNC_SETTLE_MAX_UPDATES = 5000
DEFAULT_TDLIB_SYNC_SETTLE_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_SYNC_SETTLE_QUIET_EMPTY_RECEIVES = 3
DEFAULT_TDLIB_SYNC_SETTLE_MAX_DURATION_SEC = 300.0
TDLIB_READY_STATE = "authorizationStateReady"
TDLIB_BOOTSTRAP_AUTH_STATES = frozenset(
    {
        "authorizationStateWaitTdlibParameters",
        "authorizationStateWaitEncryptionKey",
    }
)
TDLIB_MANUAL_INTERVENTION_AUTH_STATES = frozenset(
    {
        "authorizationStateWaitPhoneNumber",
        "authorizationStateWaitCode",
        "authorizationStateWaitOtherDeviceConfirmation",
        "authorizationStateWaitPassword",
    }
)
TDLIB_CLOSED_AUTH_STATES = frozenset(
    {
        "authorizationStateLoggingOut",
        "authorizationStateClosing",
        "authorizationStateClosed",
    }
)
TDLIB_BLOCKED_AUTH_STATES = TDLIB_MANUAL_INTERVENTION_AUTH_STATES | TDLIB_CLOSED_AUTH_STATES
TDLIB_READY_PROBE_REQUEST_TYPES = frozenset(
    {
        "getAuthorizationState",
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
    }
)
TDLIB_BOOTSTRAP_FUNCTION_REQUEST_TYPES = frozenset(
    {
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
    }
)

SELECT_ONE_QUERY = "SELECT 1"
SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
COUNT_TARGET_ROWS_QUERY = """
SELECT COUNT(*)
FROM telegram_channel_registry
WHERE source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'unresolved'
  AND chat_id IS NULL
"""
SELECT_TARGET_ROWS_QUERY = """
SELECT registry_id, source_value
FROM telegram_channel_registry
WHERE source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'unresolved'
  AND chat_id IS NULL
ORDER BY priority_weight DESC, registry_id ASC
"""
SELECT_TARGET_ROWS_LIMIT_QUERY = """
SELECT registry_id, source_value
FROM telegram_channel_registry
WHERE source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'unresolved'
  AND chat_id IS NULL
ORDER BY priority_weight DESC, registry_id ASC
LIMIT :limit
"""
UPDATE_RESOLVED_REGISTRY_ROW_QUERY = """
UPDATE telegram_channel_registry
SET
  chat_id = :chat_id,
  username_snapshot = :username_snapshot,
  title_snapshot = COALESCE(:title_snapshot, title_snapshot),
  chat_type = COALESCE(:chat_type, chat_type),
  last_resolved_at = :resolved_at,
  access_state = 'resolved_not_joined',
  updated_at = :resolved_at
WHERE registry_id = :registry_id
  AND source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'unresolved'
  AND chat_id IS NULL
"""

SIDE_EFFECT_FLAG_NAMES = (
    "database_mutation_performed",
    "telegram_channel_registry_inserted",
    "telegram_channel_registry_updated",
    "telegram_channel_registry_deleted",
    "redis_mutation_performed",
    "telegram_api_called",
    "tdlib_initialized",
    "tdlib_send_called",
    "tdlib_receive_called",
    "tdlib_auth_attempted",
    "tdlib_public_username_resolve_called",
    "tdlib_join_called",
    "tdlib_history_fetch_called",
    "live_collector_started",
    "collector_runtime_started",
    "notifier_transport_enabled",
    "outbox_relay_started",
    "router_normalizer_started",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "alembic_upgrade_run",
    "alembic_downgrade_run",
    "alembic_stamp_run",
    "docker_or_systemd_changed",
    "files_mutated_outside_repo",
)

SUSPICIOUS_VALUE_FRAGMENTS = (
    "database_url",
    "redis_url",
    "telegram_api_hash",
    "telegram_api_id",
    "telegram_phone_number",
    "telegram_bot_token",
    "telegram_login_code",
    "tdlib_db_encryption_key",
    "api_hash",
    "api_id",
    "phone_number",
    "password",
    "secret",
    "token",
    "postgresql://",
    "postgresql+",
    "redis://",
    "https://t.me/+",
    "t.me/+",
    "joinchat",
)

ALLOWED_CHAT_TYPE_SUMMARIES = frozenset({"channel", "supergroup", "basic_group", "group"})
NOT_FOUND_ERROR_MARKERS = ("CHAT_NOT_FOUND", "USERNAME_NOT_OCCUPIED", "USERNAME_INVALID")
ACCESS_DENIED_ERROR_MARKERS = ("FORBIDDEN", "CHANNEL_PRIVATE", "USER_BANNED_IN_CHANNEL")
RESOLVE_FAILURE_CLASSES = frozenset(
    {
        "not_found",
        "access_denied",
        "unsupported_chat_type",
        "response_timeout",
        "transport_error",
        "tdlib_error",
        "response_shape_error",
        "authorization_lost",
        "unknown_error",
    }
)
RESOLVE_UNRESOLVED_STATUSES = frozenset(
    {
        "not_found",
        "access_denied",
        "unsupported_chat_type",
    }
)


class DatabaseConnection(Protocol):
    def begin(self) -> Any: ...

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


class PublicUsernameResolver(Protocol):
    async def initialize(self) -> None: ...

    async def drain_post_ready_updates(self) -> "TDLibPostReadyDrainSummary": ...

    async def resolve_public_username(self, username: str) -> "PublicUsernameResolveResult": ...

    async def diagnose_single_resolve_rpc(
        self,
        username: str,
    ) -> "SingleResolveRpcDiagnosticResult": ...

    async def diagnose_post_ready_sync_settle(self) -> "TDLibSyncSettleDiagnosticSummary": ...

    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseConnectionFactory = Callable[[str], DatabaseConnection]
PublicUsernameResolverFactory = Callable[[Mapping[str, str]], PublicUsernameResolver]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TargetRow:
    registry_id: str
    source_value: str
    normalized_username: str


@dataclass(frozen=True, slots=True)
class PublicUsernameResolveResult:
    status: str
    chat_id: int | None = None
    username_snapshot: str | None = None
    title_snapshot: str | None = None
    chat_type: str | None = None
    failure_class: str | None = None
    tdlib_error_code: int | str | None = None
    function_response_types_seen: tuple[str, ...] = ()
    response_extra_matched: bool = False
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0


@dataclass(frozen=True, slots=True)
class TDLibSingleResolveRpcWaitConfig:
    max_updates: int = DEFAULT_TDLIB_SINGLE_RPC_MAX_UPDATES
    receive_timeout_sec: float = DEFAULT_TDLIB_SINGLE_RPC_RECEIVE_TIMEOUT_SEC
    max_duration_sec: float = DEFAULT_TDLIB_SINGLE_RPC_MAX_DURATION_SEC


@dataclass(frozen=True, slots=True)
class SingleResolveRpcDiagnosticResult:
    enabled: bool = False
    target_selected: bool = False
    target_index_bucket: str = "zero"
    request_sent: bool = False
    request_extra_present: bool = False
    max_updates: int = DEFAULT_TDLIB_SINGLE_RPC_MAX_UPDATES
    receive_timeout_sec: float = DEFAULT_TDLIB_SINGLE_RPC_RECEIVE_TIMEOUT_SEC
    max_duration_sec: float = DEFAULT_TDLIB_SINGLE_RPC_MAX_DURATION_SEC
    send_error_class: str | None = None
    receive_attempt_count: int = 0
    observation_count: int = 0
    empty_receive_count: int = 0
    inbound_object_types_seen: tuple[str, ...] = ()
    function_response_types_seen: tuple[str, ...] = ()
    update_types_seen: tuple[str, ...] = ()
    update_budget_exhausted: bool = False
    duration_exhausted: bool = False
    update_pressure_observed: bool = False
    authorization_states_seen: tuple[str, ...] = ()
    final_authorization_state: str | None = None
    response_extra_matched: bool = False
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0
    result_class: str = "not_attempted"
    tdlib_error_codes_seen: tuple[int | str, ...] = ()
    timed_out: bool = False
    operator_next_action: str = "Single public username RPC diagnostic was not requested."

    def as_report_fields(self) -> dict[str, Any]:
        return {
            "single_resolve_rpc_diagnostic_enabled": self.enabled,
            "single_resolve_rpc_target_selected": self.target_selected,
            "single_resolve_rpc_target_index_bucket": self.target_index_bucket,
            "single_resolve_rpc_request_sent": self.request_sent,
            "single_resolve_rpc_request_extra_present": self.request_extra_present,
            "single_resolve_rpc_max_updates": self.max_updates,
            "single_resolve_rpc_receive_timeout_sec": self.receive_timeout_sec,
            "single_resolve_rpc_max_duration_sec": self.max_duration_sec,
            "single_resolve_rpc_send_error_class": self.send_error_class,
            "single_resolve_rpc_receive_attempt_count_bucket": _bucket_count(
                self.receive_attempt_count
            ),
            "single_resolve_rpc_observation_count_bucket": _bucket_count(
                self.observation_count
            ),
            "single_resolve_rpc_empty_receive_count_bucket": _bucket_count(
                self.empty_receive_count
            ),
            "single_resolve_rpc_inbound_object_types_seen": list(
                self.inbound_object_types_seen
            ),
            "single_resolve_rpc_function_response_types_seen": list(
                self.function_response_types_seen
            ),
            "single_resolve_rpc_update_types_seen": list(self.update_types_seen),
            "single_resolve_rpc_update_budget_exhausted": (
                self.update_budget_exhausted
            ),
            "single_resolve_rpc_duration_exhausted": self.duration_exhausted,
            "single_resolve_rpc_update_pressure_observed": (
                self.update_pressure_observed
            ),
            "single_resolve_rpc_authorization_states_seen": list(
                self.authorization_states_seen
            ),
            "single_resolve_rpc_final_authorization_state": (
                self.final_authorization_state
            ),
            "single_resolve_rpc_response_extra_matched": self.response_extra_matched,
            "single_resolve_rpc_response_without_extra_count_bucket": _bucket_count(
                self.response_without_extra_count
            ),
            "single_resolve_rpc_response_wrong_extra_count_bucket": _bucket_count(
                self.response_wrong_extra_count
            ),
            "single_resolve_rpc_result_class": self.result_class,
            "single_resolve_rpc_tdlib_error_codes_seen": list(
                self.tdlib_error_codes_seen
            ),
            "single_resolve_rpc_timed_out": self.timed_out,
            "single_resolve_rpc_operator_next_action": self.operator_next_action,
        }


@dataclass(slots=True)
class ResolveReportCounters:
    attempt_count: int = 0
    resolved_count: int = 0
    unresolved_count: int = 0
    failed_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    not_found_count: int = 0
    access_denied_count: int = 0
    unsupported_chat_type_count: int = 0
    response_timeout_count: int = 0
    transport_error_count: int = 0
    tdlib_error_count: int = 0
    response_shape_error_count: int = 0
    authorization_lost_count: int = 0
    unknown_error_count: int = 0
    response_extra_matched_count: int = 0
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0
    authorization_lost_seen: bool = False
    failure_classes_seen: list[str] = field(default_factory=list)
    tdlib_error_codes_seen: list[int | str] = field(default_factory=list)
    function_response_types_seen: list[str] = field(default_factory=list)

    def record(self, result: PublicUsernameResolveResult) -> PublicUsernameResolveResult:
        result = _canonical_resolve_result(result)
        self.attempt_count += 1
        self.response_without_extra_count += max(result.response_without_extra_count, 0)
        self.response_wrong_extra_count += max(result.response_wrong_extra_count, 0)
        if result.response_extra_matched:
            self.response_extra_matched_count += 1
        for response_type in result.function_response_types_seen:
            if _safe_tdlib_object_type(response_type) == response_type:
                _append_unique(self.function_response_types_seen, response_type)
        if (
            result.tdlib_error_code is not None
            and result.tdlib_error_code not in self.tdlib_error_codes_seen
        ):
            self.tdlib_error_codes_seen.append(result.tdlib_error_code)

        status = result.status
        if status == "resolved":
            self.resolved_count += 1
            return result

        failure_class = _safe_resolve_failure_class(result.failure_class or status)
        _append_unique(self.failure_classes_seen, failure_class)
        if status in RESOLVE_UNRESOLVED_STATUSES:
            self.unresolved_count += 1
        else:
            self.failed_count += 1

        if failure_class == "not_found":
            self.not_found_count += 1
        elif failure_class == "access_denied":
            self.access_denied_count += 1
        elif failure_class == "unsupported_chat_type":
            self.unsupported_chat_type_count += 1
        elif failure_class == "response_timeout":
            self.response_timeout_count += 1
        elif failure_class == "transport_error":
            self.transport_error_count += 1
        elif failure_class == "tdlib_error":
            self.tdlib_error_count += 1
        elif failure_class == "response_shape_error":
            self.response_shape_error_count += 1
        elif failure_class == "authorization_lost":
            self.authorization_lost_count += 1
            self.authorization_lost_seen = True
        else:
            self.unknown_error_count += 1
        return result


@dataclass(slots=True)
class TDLibReadyProbeSummary:
    attempted: bool = False
    status: str = "not_attempted"
    observation_count: int = 0
    request_types_sent: list[str] = field(default_factory=list)
    update_types_seen: list[str] = field(default_factory=list)
    authorization_states_seen: list[str] = field(default_factory=list)
    final_authorization_state: str | None = None
    error_class: str | None = None
    error_code: int | str | None = None
    auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES
    receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC
    overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC
    manual_intervention_required: bool = False
    parameter_bootstrap_attempted: bool = False
    encryption_key_check_attempted: bool = False
    transport_closed: bool = False
    last_tdlib_object_type: str | None = None
    timed_out_after_state: str | None = None
    function_response_types_seen: list[str] = field(default_factory=list)
    set_parameters_response_type: str | None = None
    set_parameters_error_code: int | str | None = None
    set_parameters_error_class: str | None = None
    encryption_key_response_type: str | None = None
    encryption_key_error_code: int | str | None = None
    encryption_key_error_class: str | None = None

    def configure_budget(
        self,
        *,
        auth_max_updates: int,
        receive_timeout_sec: float,
        overall_timeout_sec: float,
    ) -> None:
        self.auth_max_updates = auth_max_updates
        self.receive_timeout_sec = receive_timeout_sec
        self.overall_timeout_sec = overall_timeout_sec

    def mark_attempted(self) -> None:
        self.attempted = True
        if self.status == "not_attempted":
            self.status = "probing"

    def record_request(self, request: Mapping[str, Any]) -> None:
        request_type = request.get("@type")
        if isinstance(request_type, str) and request_type in TDLIB_READY_PROBE_REQUEST_TYPES:
            _append_unique(self.request_types_sent, request_type)
            if request_type == "setTdlibParameters":
                self.parameter_bootstrap_attempted = True
            elif request_type == "checkDatabaseEncryptionKey":
                self.encryption_key_check_attempted = True

    def record_payload(self, payload: Mapping[str, Any]) -> None:
        self.observation_count += 1
        payload_type = payload.get("@type")
        self.last_tdlib_object_type = _safe_tdlib_object_type(payload_type)
        if isinstance(payload_type, str) and payload_type.startswith("update"):
            _append_unique(self.update_types_seen, payload_type)
        if payload_type == "error":
            self.error_class = "tdlib_error"
            self.error_code = _safe_error_code(payload.get("code"))
            if self.status not in {"ready", "manual_intervention_required"}:
                self.status = "tdlib_error"

        state_type = _authorization_state_type_from_payload(payload)
        if state_type is not None:
            self.record_authorization_state(state_type)

    def record_function_response(
        self,
        request_type: str | None,
        payload: Mapping[str, Any],
    ) -> None:
        if request_type not in TDLIB_BOOTSTRAP_FUNCTION_REQUEST_TYPES:
            return
        response_type = _safe_tdlib_object_type(payload.get("@type"))
        if response_type is None:
            return

        _append_unique(self.function_response_types_seen, response_type)
        error_class = "tdlib_error" if response_type == "error" else None
        error_code = _safe_error_code(payload.get("code")) if response_type == "error" else None

        if request_type == "setTdlibParameters":
            self.set_parameters_response_type = response_type
            self.set_parameters_error_code = error_code
            self.set_parameters_error_class = error_class
        elif request_type == "checkDatabaseEncryptionKey":
            self.encryption_key_response_type = response_type
            self.encryption_key_error_code = error_code
            self.encryption_key_error_class = error_class

    def record_authorization_state(self, state_type: str) -> None:
        _append_unique(self.authorization_states_seen, state_type)
        self.final_authorization_state = state_type
        if state_type == TDLIB_READY_STATE:
            self.status = "ready"
        elif state_type in TDLIB_MANUAL_INTERVENTION_AUTH_STATES:
            self.manual_intervention_required = True
            self.status = "manual_intervention_required"
        elif state_type in TDLIB_CLOSED_AUTH_STATES:
            self.transport_closed = True
            self.status = "not_ready"

    def mark_timed_out(self) -> None:
        if self.status not in {
            "ready",
            "manual_intervention_required",
            "not_ready",
            "tdlib_error",
            "transport_error",
        }:
            self.status = "timed_out"
            self.timed_out_after_state = self.final_authorization_state

    def mark_transport_error(self, exc: Exception) -> None:
        self.error_class = type(exc).__name__
        if self.status not in {"ready", "manual_intervention_required", "not_ready"}:
            self.status = "transport_error"

    def as_report_fields(self) -> dict[str, Any]:
        return {
            "tdlib_ready_probe_attempted": self.attempted,
            "tdlib_ready_probe_status": self.status,
            "tdlib_ready_probe_observation_count_bucket": _bucket_count(
                self.observation_count
            ),
            "tdlib_ready_probe_request_types_sent": list(self.request_types_sent),
            "tdlib_ready_probe_update_types_seen": list(self.update_types_seen),
            "tdlib_ready_probe_authorization_states_seen": list(
                self.authorization_states_seen
            ),
            "tdlib_ready_probe_final_authorization_state": self.final_authorization_state,
            "tdlib_ready_probe_error_class": self.error_class,
            "tdlib_ready_probe_error_code": self.error_code,
            "tdlib_ready_probe_auth_max_updates": self.auth_max_updates,
            "tdlib_ready_probe_receive_timeout_sec": self.receive_timeout_sec,
            "tdlib_ready_probe_overall_timeout_sec": self.overall_timeout_sec,
            "tdlib_ready_probe_manual_intervention_required": (
                self.manual_intervention_required
            ),
            "tdlib_ready_probe_parameter_bootstrap_attempted": (
                self.parameter_bootstrap_attempted
            ),
            "tdlib_ready_probe_encryption_key_check_attempted": (
                self.encryption_key_check_attempted
            ),
            "tdlib_ready_probe_transport_closed": self.transport_closed,
            "tdlib_ready_probe_last_tdlib_object_type": self.last_tdlib_object_type,
            "tdlib_ready_probe_timed_out_after_state": self.timed_out_after_state,
            "tdlib_ready_probe_function_response_types_seen": list(
                self.function_response_types_seen
            ),
            "tdlib_ready_probe_set_parameters_response_type": (
                self.set_parameters_response_type
            ),
            "tdlib_ready_probe_set_parameters_error_code": (
                self.set_parameters_error_code
            ),
            "tdlib_ready_probe_set_parameters_error_class": (
                self.set_parameters_error_class
            ),
            "tdlib_ready_probe_encryption_key_response_type": (
                self.encryption_key_response_type
            ),
            "tdlib_ready_probe_encryption_key_error_code": (
                self.encryption_key_error_code
            ),
            "tdlib_ready_probe_encryption_key_error_class": (
                self.encryption_key_error_class
            ),
        }


@dataclass(slots=True)
class TDLibPostReadyDrainSummary:
    attempted: bool = False
    max_updates: int = DEFAULT_TDLIB_POST_READY_DRAIN_MAX_UPDATES
    timeout_sec: float = DEFAULT_TDLIB_POST_READY_DRAIN_TIMEOUT_SEC
    quiet_empty_receive_target: int = (
        DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_EMPTY_RECEIVES
    )
    quiet_timeout_sec: float = DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_TIMEOUT_SEC
    receive_attempt_count: int = 0
    observation_count: int = 0
    empty_receive_count: int = 0
    quiet_empty_receive_streak: int = 0
    inbound_object_types_seen: list[str] = field(default_factory=list)
    function_response_types_seen: list[str] = field(default_factory=list)
    update_types_seen: list[str] = field(default_factory=list)
    authorization_states_seen: list[str] = field(default_factory=list)
    final_authorization_state: str | None = None
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0
    budget_exhausted: bool = False
    authorization_lost: bool = False
    quiet_window_reached: bool = False

    def mark_attempted(self) -> None:
        self.attempted = True
        if self.final_authorization_state is None:
            self.final_authorization_state = TDLIB_READY_STATE

    def record_payload(self, payload: Mapping[str, Any]) -> None:
        self.observation_count += 1
        self.quiet_empty_receive_streak = 0
        payload_type = _safe_tdlib_object_type(payload.get("@type"))
        if payload_type is not None:
            _append_unique(self.inbound_object_types_seen, payload_type)
            if payload_type.startswith("update"):
                _append_unique(self.update_types_seen, payload_type)

        state_type = _authorization_state_type_from_payload(payload)
        if state_type is not None:
            safe_state_type = _safe_tdlib_object_type(state_type)
            if safe_state_type == state_type:
                _append_unique(self.authorization_states_seen, state_type)
                self.final_authorization_state = state_type
            if state_type != TDLIB_READY_STATE:
                self.authorization_lost = True

        response_type = _function_response_type_from_payload(payload)
        if response_type is None:
            return
        _append_unique(self.function_response_types_seen, response_type)
        if isinstance(payload.get("@extra"), str):
            self.response_wrong_extra_count += 1
        else:
            self.response_without_extra_count += 1

    def record_empty_receive(self) -> None:
        self.empty_receive_count += 1
        self.quiet_empty_receive_streak += 1
        if self.quiet_empty_receive_streak >= self.quiet_empty_receive_target:
            self.quiet_window_reached = True

    def as_report_fields(self) -> dict[str, Any]:
        return {
            "tdlib_post_ready_drain_attempted": self.attempted,
            "tdlib_post_ready_drain_max_updates": self.max_updates,
            "tdlib_post_ready_drain_timeout_sec": self.timeout_sec,
            "tdlib_post_ready_drain_quiet_empty_receive_target": (
                self.quiet_empty_receive_target
            ),
            "tdlib_post_ready_drain_quiet_timeout_sec": self.quiet_timeout_sec,
            "tdlib_post_ready_drain_receive_attempt_count_bucket": _bucket_count(
                self.receive_attempt_count
            ),
            "tdlib_post_ready_drain_observation_count_bucket": _bucket_count(
                self.observation_count
            ),
            "tdlib_post_ready_drain_empty_receive_count_bucket": _bucket_count(
                self.empty_receive_count
            ),
            "tdlib_post_ready_drain_quiet_empty_receive_streak_bucket": _bucket_count(
                self.quiet_empty_receive_streak
            ),
            "tdlib_post_ready_drain_inbound_object_types_seen": list(
                self.inbound_object_types_seen
            ),
            "tdlib_post_ready_drain_function_response_types_seen": list(
                self.function_response_types_seen
            ),
            "tdlib_post_ready_drain_update_types_seen": list(self.update_types_seen),
            "tdlib_post_ready_drain_authorization_states_seen": list(
                self.authorization_states_seen
            ),
            "tdlib_post_ready_drain_final_authorization_state": (
                self.final_authorization_state
            ),
            "tdlib_post_ready_drain_response_without_extra_count_bucket": _bucket_count(
                self.response_without_extra_count
            ),
            "tdlib_post_ready_drain_response_wrong_extra_count_bucket": _bucket_count(
                self.response_wrong_extra_count
            ),
            "tdlib_post_ready_drain_budget_exhausted": self.budget_exhausted,
            "tdlib_post_ready_drain_authorization_lost": self.authorization_lost,
            "tdlib_post_ready_drain_quiet_window_reached": (
                self.quiet_window_reached
            ),
        }


@dataclass(slots=True)
class TDLibSyncSettleDiagnosticSummary:
    enabled: bool = False
    attempted: bool = False
    max_updates: int = DEFAULT_TDLIB_SYNC_SETTLE_MAX_UPDATES
    receive_timeout_sec: float = DEFAULT_TDLIB_SYNC_SETTLE_RECEIVE_TIMEOUT_SEC
    quiet_empty_receive_target: int = (
        DEFAULT_TDLIB_SYNC_SETTLE_QUIET_EMPTY_RECEIVES
    )
    max_duration_sec: float = DEFAULT_TDLIB_SYNC_SETTLE_MAX_DURATION_SEC
    receive_attempt_count: int = 0
    observation_count: int = 0
    empty_receive_count: int = 0
    quiet_empty_receive_streak: int = 0
    inbound_object_types_seen: list[str] = field(default_factory=list)
    update_types_seen: list[str] = field(default_factory=list)
    function_response_types_seen: list[str] = field(default_factory=list)
    authorization_states_seen: list[str] = field(default_factory=list)
    final_authorization_state: str | None = None
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0
    quiet_window_reached: bool = False
    update_budget_exhausted: bool = False
    duration_exhausted: bool = False
    authorization_lost: bool = False
    search_sent: bool = False
    operator_next_action: str = (
        "TDLib post-ready sync-settle diagnostic was not requested."
    )

    def mark_attempted(self) -> None:
        self.enabled = True
        self.attempted = True
        if self.final_authorization_state is None:
            self.final_authorization_state = TDLIB_READY_STATE

    def record_payload(self, payload: Mapping[str, Any]) -> None:
        self.observation_count += 1
        self.quiet_empty_receive_streak = 0
        payload_type = _safe_tdlib_object_type(payload.get("@type"))
        if payload_type is not None:
            _append_unique(self.inbound_object_types_seen, payload_type)
            if payload_type.startswith("update"):
                _append_unique(self.update_types_seen, payload_type)

        state_type = _authorization_state_type_from_payload(payload)
        if state_type is not None:
            safe_state_type = _safe_tdlib_object_type(state_type)
            if safe_state_type == state_type:
                _append_unique(self.authorization_states_seen, state_type)
                self.final_authorization_state = state_type
            if state_type != TDLIB_READY_STATE:
                self.authorization_lost = True

        response_type = _function_response_type_from_payload(payload)
        if response_type is None:
            return
        _append_unique(self.function_response_types_seen, response_type)
        if isinstance(payload.get("@extra"), str):
            self.response_wrong_extra_count += 1
        else:
            self.response_without_extra_count += 1

    def record_empty_receive(self) -> None:
        self.empty_receive_count += 1
        self.quiet_empty_receive_streak += 1
        if self.quiet_empty_receive_streak >= self.quiet_empty_receive_target:
            self.quiet_window_reached = True

    def mark_update_budget_exhausted(self) -> None:
        self.update_budget_exhausted = True

    def mark_duration_exhausted(self) -> None:
        self.duration_exhausted = True

    def apply_next_action(self) -> None:
        if self.authorization_lost:
            self.operator_next_action = (
                "TDLib authorization changed away from ready during the no-search "
                "sync-settle diagnostic. No searchPublicChat request was sent and "
                "no registry mutation was committed; restore a ready TDLib session "
                "before any resolve run."
            )
        elif self.quiet_window_reached:
            self.operator_next_action = (
                "The no-search TDLib sync-settle diagnostic reached the configured "
                "quiet empty receive target. Treat this only as bounded idle "
                "evidence for the observed window."
            )
        elif self.duration_exhausted:
            self.operator_next_action = (
                "The no-search TDLib sync-settle diagnostic exhausted max duration "
                "before reaching a quiet window. Do not treat TDLib as idle; review "
                "the sanitized object-type buckets before the next bounded action."
            )
        elif self.update_budget_exhausted:
            self.operator_next_action = (
                "The no-search TDLib sync-settle diagnostic exhausted its receive "
                "budget before reaching a quiet window. This is not solved by "
                "sending searchPublicChat; review the sanitized backlog summary."
            )
        else:
            self.operator_next_action = (
                "The no-search TDLib sync-settle diagnostic completed. Review only "
                "the sanitized settle fields before any separate resolve action."
            )

    def as_report_fields(self) -> dict[str, Any]:
        return {
            "tdlib_sync_settle_diagnostic_enabled": self.enabled,
            "tdlib_sync_settle_attempted": self.attempted,
            "tdlib_sync_settle_max_updates": self.max_updates,
            "tdlib_sync_settle_receive_timeout_sec": self.receive_timeout_sec,
            "tdlib_sync_settle_quiet_empty_receive_target": (
                self.quiet_empty_receive_target
            ),
            "tdlib_sync_settle_max_duration_sec": self.max_duration_sec,
            "tdlib_sync_settle_receive_attempt_count_bucket": _bucket_count(
                self.receive_attempt_count
            ),
            "tdlib_sync_settle_observation_count_bucket": _bucket_count(
                self.observation_count
            ),
            "tdlib_sync_settle_empty_receive_count_bucket": _bucket_count(
                self.empty_receive_count
            ),
            "tdlib_sync_settle_quiet_empty_receive_streak_bucket": _bucket_count(
                self.quiet_empty_receive_streak
            ),
            "tdlib_sync_settle_inbound_object_types_seen": list(
                self.inbound_object_types_seen
            ),
            "tdlib_sync_settle_update_types_seen": list(self.update_types_seen),
            "tdlib_sync_settle_function_response_types_seen": list(
                self.function_response_types_seen
            ),
            "tdlib_sync_settle_authorization_states_seen": list(
                self.authorization_states_seen
            ),
            "tdlib_sync_settle_final_authorization_state": (
                self.final_authorization_state
            ),
            "tdlib_sync_settle_response_without_extra_count_bucket": _bucket_count(
                self.response_without_extra_count
            ),
            "tdlib_sync_settle_response_wrong_extra_count_bucket": _bucket_count(
                self.response_wrong_extra_count
            ),
            "tdlib_sync_settle_quiet_window_reached": self.quiet_window_reached,
            "tdlib_sync_settle_update_budget_exhausted": (
                self.update_budget_exhausted
            ),
            "tdlib_sync_settle_duration_exhausted": self.duration_exhausted,
            "tdlib_sync_settle_authorization_lost": self.authorization_lost,
            "tdlib_sync_settle_search_sent": self.search_sent,
            "tdlib_sync_settle_operator_next_action": self.operator_next_action,
        }


class TDLibTransportUnavailable(RuntimeError):
    pass


class TDLibNotReady(RuntimeError):
    pass


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import (  # noqa: E402
    dedicated_vps_tdlib_session_reuse_collector_readiness_preflight as session_preflight,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve active unresolved public_username rows in telegram_channel_registry. "
            "Default mode is a DB-read-only dry-run; TDLib and registry mutation both "
            "require explicit operator approval flags."
        )
    )
    parser.add_argument("--runtime-env-path", required=True)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--approved-tdlib-public-username-resolve", action="store_true")
    parser.add_argument("--approved-registry-resolve-mutation", action="store_true")
    parser.add_argument("--diagnose-single-resolve-rpc", action="store_true")
    parser.add_argument("--diagnose-tdlib-post-ready-sync-settle", action="store_true")
    parser.add_argument("--limit", type=_positive_int, default=None)
    parser.add_argument(
        "--tdlib-auth-max-updates",
        type=_positive_int_named("tdlib-auth-max-updates"),
        default=DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    )
    parser.add_argument(
        "--tdlib-receive-timeout-sec",
        type=_non_negative_float_named("tdlib-receive-timeout-sec"),
        default=DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-overall-timeout-sec",
        type=_non_negative_float_named("tdlib-overall-timeout-sec"),
        default=DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-single-rpc-max-updates",
        type=_positive_int_named("tdlib-single-rpc-max-updates"),
        default=DEFAULT_TDLIB_SINGLE_RPC_MAX_UPDATES,
    )
    parser.add_argument(
        "--tdlib-single-rpc-receive-timeout-sec",
        type=_non_negative_float_named("tdlib-single-rpc-receive-timeout-sec"),
        default=DEFAULT_TDLIB_SINGLE_RPC_RECEIVE_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-single-rpc-max-duration-sec",
        type=_non_negative_float_named("tdlib-single-rpc-max-duration-sec"),
        default=DEFAULT_TDLIB_SINGLE_RPC_MAX_DURATION_SEC,
    )
    parser.add_argument(
        "--tdlib-post-ready-drain-max-updates",
        type=_positive_int_named("tdlib-post-ready-drain-max-updates"),
        default=DEFAULT_TDLIB_POST_READY_DRAIN_MAX_UPDATES,
    )
    parser.add_argument(
        "--tdlib-post-ready-drain-timeout-sec",
        type=_non_negative_float_named("tdlib-post-ready-drain-timeout-sec"),
        default=DEFAULT_TDLIB_POST_READY_DRAIN_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-post-ready-drain-quiet-empty-receives",
        type=_positive_int_named("tdlib-post-ready-drain-quiet-empty-receives"),
        default=DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_EMPTY_RECEIVES,
    )
    parser.add_argument(
        "--tdlib-post-ready-drain-quiet-timeout-sec",
        type=_non_negative_float_named("tdlib-post-ready-drain-quiet-timeout-sec"),
        default=DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-sync-settle-max-updates",
        type=_positive_int_named("tdlib-sync-settle-max-updates"),
        default=DEFAULT_TDLIB_SYNC_SETTLE_MAX_UPDATES,
    )
    parser.add_argument(
        "--tdlib-sync-settle-receive-timeout-sec",
        type=_non_negative_float_named("tdlib-sync-settle-receive-timeout-sec"),
        default=DEFAULT_TDLIB_SYNC_SETTLE_RECEIVE_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-sync-settle-quiet-empty-receives",
        type=_positive_int_named("tdlib-sync-settle-quiet-empty-receives"),
        default=DEFAULT_TDLIB_SYNC_SETTLE_QUIET_EMPTY_RECEIVES,
    )
    parser.add_argument(
        "--tdlib-sync-settle-max-duration-sec",
        type=_non_negative_float_named("tdlib-sync-settle-max-duration-sec"),
        default=DEFAULT_TDLIB_SYNC_SETTLE_MAX_DURATION_SEC,
    )
    return parser


def _positive_int(raw: str) -> int:
    return _parse_positive_int(raw, field_name="limit")


def _positive_int_named(field_name: str) -> Callable[[str], int]:
    return lambda raw: _parse_positive_int(raw, field_name=field_name)


def _parse_positive_int(raw: str, *, field_name: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{field_name} must be a positive integer"
        ) from exc
    if value <= 0:
        raise argparse.ArgumentTypeError(f"{field_name} must be a positive integer")
    return value


def _non_negative_float_named(field_name: str) -> Callable[[str], float]:
    def parse(raw: str) -> float:
        try:
            value = float(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a finite non-negative number"
            ) from exc
        if not math.isfinite(value) or value < 0:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a finite non-negative number"
            )
        return value

    return parse


def _bucket_count(count: int | None) -> str:
    if count is None:
        return "unknown"
    if count == 0:
        return "zero"
    if count == 1:
        return "one"
    if count <= 5:
        return "two_to_five"
    if count <= 10:
        return "six_to_ten"
    if count <= 20:
        return "eleven_to_twenty"
    if count <= 50:
        return "twenty_one_to_fifty"
    return "more_than_fifty"


def _side_effects() -> dict[str, bool]:
    return {flag: False for flag in SIDE_EFFECT_FLAG_NAMES}


def _empty_ready_probe_report_fields() -> dict[str, Any]:
    return TDLibReadyProbeSummary().as_report_fields()


def _empty_ready_helper_report_fields() -> dict[str, Any]:
    return {
        "tdlib_ready_helper_reused": False,
        "tdlib_ready_helper_status": "not_attempted",
        "tdlib_ready_helper_manual_intervention_required": False,
    }


def _empty_post_ready_drain_report_fields() -> dict[str, Any]:
    return TDLibPostReadyDrainSummary().as_report_fields()


def _empty_sync_settle_diagnostic_report_fields(
    *,
    enabled: bool,
) -> dict[str, Any]:
    return TDLibSyncSettleDiagnosticSummary(enabled=enabled).as_report_fields()


def _empty_resolve_classification_report_fields() -> dict[str, Any]:
    return {
        "resolve_attempt_count_bucket": "zero",
        "resolve_resolved_count_bucket": "zero",
        "resolve_not_found_count_bucket": "zero",
        "resolve_access_denied_count_bucket": "zero",
        "resolve_unsupported_chat_type_count_bucket": "zero",
        "resolve_response_timeout_count_bucket": "zero",
        "resolve_transport_error_count_bucket": "zero",
        "resolve_tdlib_error_count_bucket": "zero",
        "resolve_response_shape_error_count_bucket": "zero",
        "resolve_authorization_lost_count_bucket": "zero",
        "resolve_unknown_error_count_bucket": "zero",
        "resolve_failure_classes_seen": [],
        "resolve_tdlib_error_codes_seen": [],
        "resolve_function_response_types_seen": [],
        "resolve_response_extra_matched_count_bucket": "zero",
        "resolve_response_without_extra_count_bucket": "zero",
        "resolve_response_wrong_extra_count_bucket": "zero",
    }


def _empty_single_resolve_rpc_diagnostic_report_fields(
    *,
    enabled: bool,
) -> dict[str, Any]:
    return SingleResolveRpcDiagnosticResult(enabled=enabled).as_report_fields()


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _append_unique_safe_error_code(values: list[int | str], value: Any) -> None:
    safe_value = _safe_error_code(value)
    if safe_value is not None and safe_value not in values:
        values.append(safe_value)


def _safe_error_code(value: Any) -> int | str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", value):
            return value
        return None


def _safe_tdlib_object_type(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", value):
        return value
    return "unrecognized"


def _safe_exception_class(exc: Exception) -> str:
    return _safe_tdlib_object_type(type(exc).__name__) or "unrecognized"


def _safe_resolve_failure_class(value: Any) -> str:
    if isinstance(value, str) and value in RESOLVE_FAILURE_CLASSES:
        return value
    return "unknown_error"


def _safe_resolve_status(value: Any) -> str:
    if value == "resolved":
        return "resolved"
    if isinstance(value, str) and value in RESOLVE_FAILURE_CLASSES:
        return value
    return "unknown_error"


def _canonical_resolve_result(
    result: PublicUsernameResolveResult,
) -> PublicUsernameResolveResult:
    status = _safe_resolve_status(result.status)
    failure_class = result.failure_class
    if status == "resolved" and result.chat_id is None:
        status = "response_shape_error"
        failure_class = "response_shape_error"
    elif status != "resolved":
        failure_class = _safe_resolve_failure_class(failure_class or status)
    if status == result.status and failure_class == result.failure_class:
        return result
    return replace(result, status=status, failure_class=failure_class)


def _resolve_failure_result(
    failure_class: str,
    *,
    tdlib_error_code: int | str | None = None,
    function_response_types_seen: Sequence[str] = (),
    response_extra_matched: bool = False,
    response_without_extra_count: int = 0,
    response_wrong_extra_count: int = 0,
) -> PublicUsernameResolveResult:
    safe_failure_class = _safe_resolve_failure_class(failure_class)
    safe_response_types = tuple(
        response_type
        for response_type in function_response_types_seen
        if _safe_tdlib_object_type(response_type) == response_type
    )
    return PublicUsernameResolveResult(
        status=safe_failure_class,
        failure_class=safe_failure_class,
        tdlib_error_code=_safe_error_code(tdlib_error_code),
        function_response_types_seen=safe_response_types,
        response_extra_matched=response_extra_matched,
        response_without_extra_count=max(response_without_extra_count, 0),
        response_wrong_extra_count=max(response_wrong_extra_count, 0),
    )


def _function_response_type_from_payload(payload: Mapping[str, Any]) -> str | None:
    payload_type = _safe_tdlib_object_type(payload.get("@type"))
    if payload_type is None:
        return None
    if payload_type.startswith("update") or payload_type.startswith("authorizationState"):
        return None
    return payload_type


def _append_function_response_type(
    values: list[str],
    payload: Mapping[str, Any],
) -> None:
    response_type = _function_response_type_from_payload(payload)
    if response_type is not None:
        _append_unique(values, response_type)


def _single_resolve_rpc_next_action(
    *,
    result_class: str,
    response_extra_matched: bool,
    timed_out: bool,
) -> str:
    if result_class == "authorization_lost":
        return (
            "TDLib authorization changed away from ready during the single "
            "searchPublicChat diagnostic. Keep registry mutation disabled and restore "
            "a ready TDLib session before any broader resolve run."
        )
    if result_class == "transport_error":
        return (
            "A TDLib send or receive transport error occurred during the single "
            "searchPublicChat diagnostic. Inspect transport class-level behavior only; "
            "do not paste secrets, payloads, paths, or private stderr."
        )
    if timed_out:
        return (
            "The single searchPublicChat request was sent, but no matching function "
            "response was observed inside the bounded receive loop. Keep registry "
            "mutation disabled until TDLib response correlation is understood."
        )
    if result_class == "resolved" and response_extra_matched:
        return (
            "A matching sanitized searchPublicChat chat response was observed. Review "
            "the diagnostic buckets before any separate approved registry mutation."
        )
    if result_class == "tdlib_error":
        return (
            "A matching TDLib error response was observed. Use only the sanitized safe "
            "error code and keep registry mutation disabled."
        )
    return (
        "The single searchPublicChat diagnostic completed. Review only the sanitized "
        "result class, object-type buckets, and extra-correlation fields before the "
        "next bounded action."
    )


def _authorization_state_type_from_payload(payload: Mapping[str, Any]) -> str | None:
    payload_type = payload.get("@type")
    if payload_type == "updateAuthorizationState":
        state = payload.get("authorization_state")
        if isinstance(state, Mapping):
            state_type = state.get("@type")
            return state_type if isinstance(state_type, str) else None
    if isinstance(payload_type, str) and payload_type.startswith("authorizationState"):
        return payload_type
    return None


def _base_report(
    *,
    dry_run: bool,
    approved_tdlib_public_username_resolve: bool,
    approved_registry_resolve_mutation: bool,
    diagnose_single_resolve_rpc: bool,
    diagnose_tdlib_post_ready_sync_settle: bool,
) -> dict[str, Any]:
    report = {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "target_rows_checked": False,
        "target_row_count_bucket": "unknown",
        "dry_run": dry_run,
        "approved_tdlib_public_username_resolve": approved_tdlib_public_username_resolve,
        "approved_registry_resolve_mutation": approved_registry_resolve_mutation,
        "tdlib_resolve_attempted": False,
        "registry_resolve_mutation_performed": False,
        "resolved_count_bucket": "zero",
        "unresolved_count_bucket": "zero",
        "failed_resolve_count_bucket": "zero",
        "updated_row_count_bucket": "zero",
        "skipped_row_count_bucket": "zero",
        "operator_next_action": (
            "Fix runtime env or DB access on the VPS without pasting runtime.env "
            "values, usernames, chat IDs, phone numbers, invite links, or Telegram "
            "secrets into ChatGPT."
        ),
        "side_effects": _side_effects(),
    }
    report.update(_empty_ready_probe_report_fields())
    report.update(_empty_ready_helper_report_fields())
    report.update(_empty_post_ready_drain_report_fields())
    report.update(
        _empty_sync_settle_diagnostic_report_fields(
            enabled=diagnose_tdlib_post_ready_sync_settle
        )
    )
    report.update(_empty_resolve_classification_report_fields())
    report.update(
        _empty_single_resolve_rpc_diagnostic_report_fields(
            enabled=diagnose_single_resolve_rpc
        )
    )
    return report


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.strip().split())


def _allowed_read_statements() -> set[str]:
    return {
        _normalize_sql(statement)
        for statement in (
            SELECT_ONE_QUERY,
            SET_TRANSACTION_READ_ONLY_QUERY,
            TABLE_AVAILABLE_QUERY,
            COUNT_TARGET_ROWS_QUERY,
            SELECT_TARGET_ROWS_QUERY,
            SELECT_TARGET_ROWS_LIMIT_QUERY,
        )
    }


def _assert_read_sql(statement: str) -> None:
    if _normalize_sql(statement) not in _allowed_read_statements():
        raise ValueError("SQL statement is not in the public username resolve read allowlist")


def _assert_update_sql(statement: str) -> None:
    if _normalize_sql(statement) != _normalize_sql(UPDATE_RESOLVED_REGISTRY_ROW_QUERY):
        raise ValueError("SQL statement is not in the public username resolve update allowlist")


def _execute_read(
    connection: DatabaseConnection,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    _assert_read_sql(statement)
    return connection.execute(statement, params or {})


def _execute_update(
    connection: DatabaseConnection,
    statement: str,
    params: dict[str, Any],
) -> Any:
    _assert_update_sql(statement)
    return connection.execute(statement, params)


def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if isinstance(first, (tuple, list)):
        return first[0] if first else None
    if hasattr(first, "_mapping"):
        return next(iter(first._mapping.values()))
    if isinstance(first, Mapping):
        return next(iter(first.values()))
    return first


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if hasattr(result, "mappings"):
        return list(result.mappings().all())
    if isinstance(result, list):
        return result
    return list(result)


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    if hasattr(row, "_mapping"):
        return row._mapping.get(key)
    if isinstance(row, (tuple, list)):
        return row[index] if len(row) > index else None
    return getattr(row, key, None)


class SqlAlchemyConnection:
    def __init__(self, raw_connection: Any, text_factory: Callable[[str], Any]) -> None:
        self._raw_connection = raw_connection
        self._text_factory = text_factory

    def begin(self) -> Any:
        return self._raw_connection.begin()

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any:
        return self._raw_connection.execute(self._text_factory(statement), params or {})

    def close(self) -> None:
        self._raw_connection.close()


class SqlAlchemyConnectionFactory:
    def __init__(self) -> None:
        self._engine: Any | None = None

    def __call__(self, database_url: str) -> DatabaseConnection:
        sqlalchemy = __import__("sqlalchemy")
        self._engine = sqlalchemy.create_engine(database_url, future=True)
        return SqlAlchemyConnection(self._engine.connect(), sqlalchemy.text)

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()


def _open_default_database_connection(
    database_url: str,
) -> tuple[DatabaseConnection, Callable[[], None]]:
    factory = SqlAlchemyConnectionFactory()
    connection = factory(database_url)

    def cleanup() -> None:
        connection.close()
        factory.dispose()

    return connection, cleanup


def _open_database_connection(
    database_url: str,
    database_connection_factory: DatabaseConnectionFactory | None,
) -> tuple[DatabaseConnection, Callable[[], None]]:
    if database_connection_factory is not None:
        connection = database_connection_factory(database_url)
        return connection, connection.close
    return _open_default_database_connection(database_url)


def _database_url_is_supported(database_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not scheme_match:
        return False
    scheme = scheme_match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _read_runtime_env(
    path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
) -> Mapping[str, str]:
    if runtime_env_reader is not None:
        return runtime_env_reader(path)
    return session_preflight.parse_runtime_env_file(path)


def _commit_transaction(transaction: Any | None) -> None:
    if transaction is not None and hasattr(transaction, "commit"):
        transaction.commit()


def _rollback_transaction(transaction: Any | None) -> None:
    if transaction is not None and hasattr(transaction, "rollback"):
        transaction.rollback()


def _close_connection(
    cleanup: Callable[[], None] | None,
    connection: DatabaseConnection | None,
) -> None:
    if cleanup is not None:
        cleanup()
    elif connection is not None:
        connection.close()


def _rowcount(result: Any) -> int:
    value = getattr(result, "rowcount", 0)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(value, 0)
    return 0


def _looks_suspicious(value: str) -> bool:
    lowered = value.strip().lower()
    if "=" in lowered:
        return True
    return any(fragment in lowered for fragment in SUSPICIOUS_VALUE_FRAGMENTS)


def _normalize_public_username(raw_value: Any) -> str | None:
    if not isinstance(raw_value, str):
        return None
    if raw_value != raw_value.strip() or re.search(r"\s", raw_value):
        return None
    if _looks_suspicious(raw_value):
        return None

    value = raw_value
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = value[len(prefix) :]
            break
    if value.startswith("@"):
        value = value[1:]

    if not value or "/" in value or _looks_suspicious(value):
        return None
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", value):
        return None
    return value


def _target_row_from_db_row(row: Any) -> TargetRow | None:
    registry_id = _row_value(row, "registry_id", 0)
    source_value = _row_value(row, "source_value", 1)
    normalized_username = _normalize_public_username(source_value)
    if normalized_username is None:
        return None
    try:
        registry_id_text = str(registry_id)
        uuid.UUID(registry_id_text)
    except (TypeError, ValueError):
        registry_id_text = str(registry_id)
        if not registry_id_text.strip():
            return None
    return TargetRow(
        registry_id=registry_id_text,
        source_value=str(source_value),
        normalized_username=normalized_username,
    )


def _count_target_rows(connection: DatabaseConnection) -> int:
    value = _scalar(_execute_read(connection, COUNT_TARGET_ROWS_QUERY))
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _load_target_rows(connection: DatabaseConnection, *, limit: int | None) -> tuple[TargetRow, ...]:
    if limit is None:
        raw_rows = _rows(_execute_read(connection, SELECT_TARGET_ROWS_QUERY))
    else:
        raw_rows = _rows(
            _execute_read(
                connection,
                SELECT_TARGET_ROWS_LIMIT_QUERY,
                {"limit": limit},
            )
        )
    target_rows = [_target_row_from_db_row(row) for row in raw_rows]
    return tuple(row for row in target_rows if row is not None)


def _update_resolved_row(
    connection: DatabaseConnection,
    *,
    row: TargetRow,
    resolved: PublicUsernameResolveResult,
    resolved_at: datetime,
) -> bool:
    if resolved.chat_id is None:
        return False
    username_snapshot = resolved.username_snapshot or row.normalized_username
    result = _execute_update(
        connection,
        UPDATE_RESOLVED_REGISTRY_ROW_QUERY,
        {
            "registry_id": row.registry_id,
            "chat_id": resolved.chat_id,
            "username_snapshot": username_snapshot,
            "title_snapshot": resolved.title_snapshot,
            "chat_type": resolved.chat_type,
            "resolved_at": resolved_at,
        },
    )
    return _rowcount(result) > 0


def _runtime_env_tdjson_library_path(runtime_env: Mapping[str, str]) -> str | None:
    candidate = runtime_env.get("TDJSON_LIBRARY_PATH")
    if not isinstance(candidate, str):
        return None
    stripped = candidate.strip()
    return stripped or None


def _manual_reuse_state_name(tdlib_state_type: str) -> str:
    if tdlib_state_type == "authorizationStateWaitPhoneNumber":
        return "waiting_phone_number"
    if tdlib_state_type in {
        "authorizationStateWaitCode",
        "authorizationStateWaitOtherDeviceConfirmation",
    }:
        return "waiting_code"
    if tdlib_state_type == "authorizationStateWaitPassword":
        return "waiting_password"
    return "degraded"


def _safe_ready_helper_status(value: Any) -> str:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", value):
        return value
    return "degraded"


def _blocked_login_code_prompt(_prompt_text: str) -> str:
    raise TDLibNotReady("Interactive Telegram login code entry is not allowed")


class _ReadySessionReuseAuthorizationFSM:
    """Auth helper FSM adapter that never submits login, code, or 2FA values."""

    def __init__(
        self,
        bootstrap_fsm: Any,
        transition_result_factory: Callable[..., Any],
    ) -> None:
        self._bootstrap_fsm = bootstrap_fsm
        self._transition_result_factory = transition_result_factory

    def handle_state(self, state: Mapping[str, Any]) -> Any:
        state_type = state.get("@type")
        if state_type in TDLIB_MANUAL_INTERVENTION_AUTH_STATES:
            return self._transition_result_factory(
                new_state=_manual_reuse_state_name(str(state_type)),
                requests=[],
                requires_manual_intervention=True,
                note=(
                    "Manual TDLib authorization is required before public username "
                    "resolve can reuse the existing session."
                ),
            )
        return self._bootstrap_fsm.handle_state(dict(state))


class _ReadySessionProbeClient:
    """Observer around TDLibClient for sanitized helper-readiness diagnostics."""

    def __init__(
        self,
        client: Any,
        ready_probe_summary: TDLibReadyProbeSummary,
    ) -> None:
        self._client = client
        self._ready_probe_summary = ready_probe_summary
        self._pending_request_types_by_extra: dict[str, str] = {}
        self.send_called = False
        self.receive_called = False

    async def initialize(self) -> None:
        self._ready_probe_summary.mark_attempted()
        await self._client.initialize()

    async def send(self, request: Mapping[str, Any]) -> None:
        self.send_called = True
        request_copy = dict(request)
        self._ready_probe_summary.record_request(request_copy)
        request_type = request_copy.get("@type")
        extra = request_copy.get("@extra")
        if isinstance(request_type, str) and isinstance(extra, str):
            self._pending_request_types_by_extra[extra] = request_type
        await self._client.send(request_copy)

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        self.receive_called = True
        payload = await self._client.receive(timeout)
        if isinstance(payload, Mapping):
            self._ready_probe_summary.record_payload(payload)
            extra = payload.get("@extra")
            request_type = (
                self._pending_request_types_by_extra.pop(extra, None)
                if isinstance(extra, str)
                else None
            )
            self._ready_probe_summary.record_function_response(request_type, payload)
        return payload

    async def close(self) -> None:
        return

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class TDLibPublicUsernameResolver:
    def __init__(
        self,
        runtime_env: Mapping[str, str],
        *,
        transport: Any | None = None,
        auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
        receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
        overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
        post_ready_drain_max_updates: int = DEFAULT_TDLIB_POST_READY_DRAIN_MAX_UPDATES,
        post_ready_drain_timeout_sec: float = DEFAULT_TDLIB_POST_READY_DRAIN_TIMEOUT_SEC,
        post_ready_drain_quiet_empty_receives: int = (
            DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_EMPTY_RECEIVES
        ),
        post_ready_drain_quiet_timeout_sec: float = (
            DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_TIMEOUT_SEC
        ),
        single_rpc_max_updates: int = DEFAULT_TDLIB_SINGLE_RPC_MAX_UPDATES,
        single_rpc_receive_timeout_sec: float = (
            DEFAULT_TDLIB_SINGLE_RPC_RECEIVE_TIMEOUT_SEC
        ),
        single_rpc_max_duration_sec: float = (
            DEFAULT_TDLIB_SINGLE_RPC_MAX_DURATION_SEC
        ),
        sync_settle_max_updates: int = DEFAULT_TDLIB_SYNC_SETTLE_MAX_UPDATES,
        sync_settle_receive_timeout_sec: float = (
            DEFAULT_TDLIB_SYNC_SETTLE_RECEIVE_TIMEOUT_SEC
        ),
        sync_settle_quiet_empty_receives: int = (
            DEFAULT_TDLIB_SYNC_SETTLE_QUIET_EMPTY_RECEIVES
        ),
        sync_settle_max_duration_sec: float = (
            DEFAULT_TDLIB_SYNC_SETTLE_MAX_DURATION_SEC
        ),
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        from src.services.collector_telegram.auth_entrypoint import TDLibAuthOnlyRunner
        from src.services.collector_telegram.auth_fsm import (
            AuthTransitionResult,
            AuthorizationFSM,
        )
        from src.services.collector_telegram.config import CollectorTelegramConfig
        from src.services.collector_telegram.tdlib_client import TDJsonTransport, TDLibClient

        self._config = CollectorTelegramConfig.from_env(runtime_env)
        self._transport = transport
        if self._transport is None:
            self._transport = TDJsonTransport(
                library_path=_runtime_env_tdjson_library_path(runtime_env)
            )
            self._transport.assert_available()
        self._client = TDLibClient(self._config, transport=self._transport)
        self._ready_probe_summary = TDLibReadyProbeSummary()
        self._ready_probe_summary.configure_budget(
            auth_max_updates=auth_max_updates,
            receive_timeout_sec=receive_timeout_sec,
            overall_timeout_sec=overall_timeout_sec,
        )
        self._ready_probe_client = _ReadySessionProbeClient(
            self._client,
            self._ready_probe_summary,
        )
        self._ready_helper_runner = TDLibAuthOnlyRunner(
            self._config,
            client=self._ready_probe_client,
            fsm=_ReadySessionReuseAuthorizationFSM(
                AuthorizationFSM(self._config),
                AuthTransitionResult,
            ),
            receive_timeout_sec=receive_timeout_sec,
            max_authorization_updates=auth_max_updates,
            approved_tdlib_auth_code_entry=False,
            login_code_prompt=_blocked_login_code_prompt,
            login_code_entry_is_interactive=lambda: False,
        )
        self._overall_timeout_sec = overall_timeout_sec
        self._post_ready_drain_summary = TDLibPostReadyDrainSummary(
            max_updates=post_ready_drain_max_updates,
            timeout_sec=post_ready_drain_timeout_sec,
            quiet_empty_receive_target=post_ready_drain_quiet_empty_receives,
            quiet_timeout_sec=post_ready_drain_quiet_timeout_sec,
        )
        self._single_rpc_wait_config = TDLibSingleResolveRpcWaitConfig(
            max_updates=single_rpc_max_updates,
            receive_timeout_sec=single_rpc_receive_timeout_sec,
            max_duration_sec=single_rpc_max_duration_sec,
        )
        self._sync_settle_summary = TDLibSyncSettleDiagnosticSummary(
            enabled=False,
            max_updates=sync_settle_max_updates,
            receive_timeout_sec=sync_settle_receive_timeout_sec,
            quiet_empty_receive_target=sync_settle_quiet_empty_receives,
            max_duration_sec=sync_settle_max_duration_sec,
        )
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._ready_helper_reused = False
        self._ready_helper_status = "not_attempted"
        self._ready_helper_manual_intervention_required = False
        self._request_sequence = 0
        self.tdlib_send_called = False
        self.tdlib_receive_called = False

    @property
    def tdlib_ready_probe_summary(self) -> Mapping[str, Any]:
        fields = self._ready_probe_summary.as_report_fields()
        fields.update(
            {
                "tdlib_ready_helper_reused": self._ready_helper_reused,
                "tdlib_ready_helper_status": self._ready_helper_status,
                "tdlib_ready_helper_manual_intervention_required": (
                    self._ready_helper_manual_intervention_required
                ),
            }
        )
        return fields

    @property
    def tdlib_post_ready_drain_summary(self) -> Mapping[str, Any]:
        return self._post_ready_drain_summary.as_report_fields()

    @property
    def tdlib_sync_settle_diagnostic_summary(self) -> Mapping[str, Any]:
        return self._sync_settle_summary.as_report_fields()

    async def initialize(self) -> None:
        try:
            self._config.ensure_runtime_dirs()
            auth_result = await asyncio.wait_for(
                self._ready_helper_runner.run_once(),
                timeout=self._overall_timeout_sec,
            )
            self.tdlib_send_called = self._ready_probe_client.send_called
            self.tdlib_receive_called = self._ready_probe_client.receive_called
            self._apply_ready_helper_result(auth_result)
            if not self._ready_helper_result_is_ready(auth_result):
                raise TDLibNotReady("TDLib ready session helper did not report ready")
        except TimeoutError as exc:
            self.tdlib_send_called = self._ready_probe_client.send_called
            self.tdlib_receive_called = self._ready_probe_client.receive_called
            self._ready_helper_reused = True
            self._ready_helper_status = "degraded"
            self._ready_probe_summary.mark_timed_out()
            raise TDLibNotReady("TDLib ready session helper timed out") from exc
        except TDLibNotReady:
            raise
        except Exception as exc:
            self._ready_probe_summary.mark_transport_error(exc)
            raise TDLibTransportUnavailable("TDLib transport unavailable") from exc

    async def close(self) -> None:
        try:
            await self._client.close()
        except Exception:
            return

    async def resolve_public_username(self, username: str) -> PublicUsernameResolveResult:
        request = self._client.build_search_public_chat_request(username).payload
        extra = self._next_extra("public_username_resolve")
        request = {**request, "@extra": extra}
        try:
            await self._send(request)
        except Exception:
            return _resolve_failure_result("transport_error")
        return await self._receive_response(extra)

    async def drain_post_ready_updates(self) -> TDLibPostReadyDrainSummary:
        self._post_ready_drain_summary.mark_attempted()
        for _ in range(self._post_ready_drain_summary.max_updates):
            self._post_ready_drain_summary.receive_attempt_count += 1
            timeout_sec = (
                self._post_ready_drain_summary.quiet_timeout_sec
                if self._post_ready_drain_summary.quiet_empty_receive_streak > 0
                else self._post_ready_drain_summary.timeout_sec
            )
            try:
                payload = await self._receive(timeout_sec)
            except Exception as exc:
                self._ready_probe_summary.mark_transport_error(exc)
                raise TDLibTransportUnavailable("TDLib transport unavailable") from exc
            if payload is None:
                self._post_ready_drain_summary.record_empty_receive()
                if self._post_ready_drain_summary.quiet_window_reached:
                    break
                continue
            if not isinstance(payload, Mapping):
                self._post_ready_drain_summary.quiet_empty_receive_streak = 0
                continue
            self._post_ready_drain_summary.record_payload(payload)
            if self._post_ready_drain_summary.authorization_lost:
                raise TDLibNotReady("TDLib authorization changed during post-ready drain")
        else:
            self._post_ready_drain_summary.budget_exhausted = True
        return self._post_ready_drain_summary

    async def diagnose_single_resolve_rpc(
        self,
        username: str,
    ) -> SingleResolveRpcDiagnosticResult:
        request = self._client.build_search_public_chat_request(username).payload
        extra = self._next_extra("single_resolve_rpc")
        request = {**request, "@extra": extra}
        request_extra_present = isinstance(request.get("@extra"), str)
        try:
            await self._send(request)
        except Exception as exc:
            return SingleResolveRpcDiagnosticResult(
                enabled=True,
                request_extra_present=request_extra_present,
                max_updates=self._single_rpc_wait_config.max_updates,
                receive_timeout_sec=(
                    self._single_rpc_wait_config.receive_timeout_sec
                ),
                max_duration_sec=self._single_rpc_wait_config.max_duration_sec,
                send_error_class=_safe_exception_class(exc),
                result_class="transport_error",
                operator_next_action=_single_resolve_rpc_next_action(
                    result_class="transport_error",
                    response_extra_matched=False,
                    timed_out=False,
                ),
            )
        return await self._receive_single_resolve_rpc_diagnostic(extra)

    async def diagnose_post_ready_sync_settle(self) -> TDLibSyncSettleDiagnosticSummary:
        self._sync_settle_summary.mark_attempted()
        started_at = self._monotonic_clock()
        for _ in range(self._sync_settle_summary.max_updates):
            if (
                self._monotonic_clock() - started_at
                >= self._sync_settle_summary.max_duration_sec
            ):
                self._sync_settle_summary.mark_duration_exhausted()
                break
            self._sync_settle_summary.receive_attempt_count += 1
            try:
                payload = await self._receive(
                    self._sync_settle_summary.receive_timeout_sec
                )
            except Exception as exc:
                self._ready_probe_summary.mark_transport_error(exc)
                raise TDLibTransportUnavailable("TDLib transport unavailable") from exc
            if payload is None:
                self._sync_settle_summary.record_empty_receive()
                if self._sync_settle_summary.quiet_window_reached:
                    break
                continue
            if not isinstance(payload, Mapping):
                self._sync_settle_summary.quiet_empty_receive_streak = 0
                continue
            self._sync_settle_summary.record_payload(payload)
            if self._sync_settle_summary.authorization_lost:
                break
        else:
            self._sync_settle_summary.mark_update_budget_exhausted()
        self._sync_settle_summary.apply_next_action()
        return self._sync_settle_summary

    def _apply_ready_helper_result(self, auth_result: Any) -> None:
        self._ready_helper_reused = True
        self._ready_helper_status = _safe_ready_helper_status(
            getattr(auth_result, "auth_entrypoint_status", None)
        )
        self._ready_helper_manual_intervention_required = bool(
            getattr(auth_result, "manual_intervention_required", False)
        )
        if self._ready_helper_result_is_ready(auth_result):
            if self._ready_probe_summary.status not in {"ready"}:
                self._ready_probe_summary.record_authorization_state(TDLIB_READY_STATE)
            return

        if self._ready_helper_manual_intervention_required:
            self._ready_probe_summary.manual_intervention_required = True
            self._ready_probe_summary.status = "manual_intervention_required"
            return

        if self._ready_helper_status in {"closed", "degraded"}:
            if self._ready_probe_summary.status == "probing":
                self._ready_probe_summary.mark_timed_out()
            if self._ready_helper_status == "closed":
                self._ready_probe_summary.transport_closed = True
                self._ready_probe_summary.status = "not_ready"
            return

        if self._ready_probe_summary.status == "probing":
            self._ready_probe_summary.mark_timed_out()

    @staticmethod
    def _ready_helper_result_is_ready(auth_result: Any) -> bool:
        return (
            getattr(auth_result, "auth_entrypoint_status", None) == "ready"
            and getattr(auth_result, "tdlib_auth_completed", False) is True
            and getattr(auth_result, "telegram_connected", False) is True
        )

    async def _receive_response(self, extra: str) -> PublicUsernameResolveResult:
        response_without_extra_count = 0
        response_wrong_extra_count = 0
        function_response_types_seen: list[str] = []
        for _ in range(DEFAULT_TDLIB_RPC_MAX_UPDATES):
            try:
                payload = await self._receive(DEFAULT_TDLIB_RPC_TIMEOUT_SEC)
            except Exception:
                return _resolve_failure_result(
                    "transport_error",
                    function_response_types_seen=function_response_types_seen,
                    response_without_extra_count=response_without_extra_count,
                    response_wrong_extra_count=response_wrong_extra_count,
                )
            if not isinstance(payload, dict):
                continue
            if payload.get("@extra") == extra:
                _append_function_response_type(function_response_types_seen, payload)
                return _resolve_result_from_tdlib_payload(
                    payload,
                    response_extra_matched=True,
                    response_without_extra_count=response_without_extra_count,
                    response_wrong_extra_count=response_wrong_extra_count,
                    function_response_types_seen=function_response_types_seen,
                )
            if payload.get("@type") == "updateAuthorizationState":
                state = payload.get("authorization_state")
                state_type = state.get("@type") if isinstance(state, dict) else None
                if state_type != TDLIB_READY_STATE:
                    return _resolve_failure_result(
                        "authorization_lost",
                        function_response_types_seen=function_response_types_seen,
                        response_without_extra_count=response_without_extra_count,
                        response_wrong_extra_count=response_wrong_extra_count,
                    )
            response_type = _function_response_type_from_payload(payload)
            if response_type is None:
                continue
            _append_unique(function_response_types_seen, response_type)
            if isinstance(payload.get("@extra"), str):
                response_wrong_extra_count += 1
            else:
                response_without_extra_count += 1
        return _resolve_failure_result(
            "response_timeout",
            function_response_types_seen=function_response_types_seen,
            response_without_extra_count=response_without_extra_count,
            response_wrong_extra_count=response_wrong_extra_count,
        )

    async def _receive_single_resolve_rpc_diagnostic(
        self,
        extra: str,
    ) -> SingleResolveRpcDiagnosticResult:
        wait_config = self._single_rpc_wait_config
        receive_attempt_count = 0
        observation_count = 0
        empty_receive_count = 0
        response_without_extra_count = 0
        response_wrong_extra_count = 0
        inbound_object_types_seen: list[str] = []
        function_response_types_seen: list[str] = []
        update_types_seen: list[str] = []
        authorization_states_seen: list[str] = []
        tdlib_error_codes_seen: list[int | str] = []
        final_authorization_state: str | None = None

        result_class = "response_timeout"
        response_extra_matched = False
        timed_out = True
        update_budget_exhausted = False
        duration_exhausted = False
        update_pressure_observed = False

        started_at = self._monotonic_clock()
        for _ in range(wait_config.max_updates):
            elapsed_sec = self._monotonic_clock() - started_at
            remaining_duration_sec = wait_config.max_duration_sec - elapsed_sec
            if remaining_duration_sec <= 0:
                duration_exhausted = True
                break
            receive_attempt_count += 1
            receive_timeout_sec = min(
                wait_config.receive_timeout_sec,
                max(remaining_duration_sec, 0.0),
            )
            try:
                payload = await self._receive(receive_timeout_sec)
            except Exception:
                result_class = "transport_error"
                timed_out = False
                break
            if payload is None:
                empty_receive_count += 1
                continue
            if not isinstance(payload, Mapping):
                continue

            observation_count += 1
            payload_type = _safe_tdlib_object_type(payload.get("@type"))
            if payload_type is not None:
                _append_unique(inbound_object_types_seen, payload_type)
                if payload_type.startswith("update"):
                    _append_unique(update_types_seen, payload_type)
                    update_pressure_observed = True
            if payload_type == "error":
                _append_unique_safe_error_code(tdlib_error_codes_seen, payload.get("code"))

            state_type = _authorization_state_type_from_payload(payload)
            if state_type is not None:
                safe_state_type = _safe_tdlib_object_type(state_type)
                if safe_state_type == state_type:
                    _append_unique(authorization_states_seen, state_type)
                    final_authorization_state = state_type
                if state_type != TDLIB_READY_STATE:
                    result_class = "authorization_lost"
                    timed_out = False
                    break

            response_type = _function_response_type_from_payload(payload)
            if response_type is not None:
                _append_unique(function_response_types_seen, response_type)

            raw_extra = payload.get("@extra")
            if raw_extra == extra:
                response_extra_matched = True
                timed_out = False
                if response_type is None:
                    result_class = "response_shape_error"
                else:
                    result = _resolve_result_from_tdlib_payload(
                        payload,
                        response_extra_matched=True,
                        response_without_extra_count=response_without_extra_count,
                        response_wrong_extra_count=response_wrong_extra_count,
                        function_response_types_seen=function_response_types_seen,
                    )
                    result_class = result.status
                    if result.tdlib_error_code is not None:
                        _append_unique_safe_error_code(
                            tdlib_error_codes_seen,
                            result.tdlib_error_code,
                        )
                break

            if response_type is None:
                continue
            if isinstance(raw_extra, str):
                response_wrong_extra_count += 1
            else:
                response_without_extra_count += 1
        else:
            update_budget_exhausted = True

        return SingleResolveRpcDiagnosticResult(
            enabled=True,
            request_sent=True,
            request_extra_present=True,
            max_updates=wait_config.max_updates,
            receive_timeout_sec=wait_config.receive_timeout_sec,
            max_duration_sec=wait_config.max_duration_sec,
            receive_attempt_count=receive_attempt_count,
            observation_count=observation_count,
            empty_receive_count=empty_receive_count,
            inbound_object_types_seen=tuple(inbound_object_types_seen),
            function_response_types_seen=tuple(function_response_types_seen),
            update_types_seen=tuple(update_types_seen),
            update_budget_exhausted=update_budget_exhausted,
            duration_exhausted=duration_exhausted,
            update_pressure_observed=update_pressure_observed,
            authorization_states_seen=tuple(authorization_states_seen),
            final_authorization_state=final_authorization_state,
            response_extra_matched=response_extra_matched,
            response_without_extra_count=response_without_extra_count,
            response_wrong_extra_count=response_wrong_extra_count,
            result_class=result_class,
            tdlib_error_codes_seen=tuple(tdlib_error_codes_seen),
            timed_out=timed_out,
            operator_next_action=_single_resolve_rpc_next_action(
                result_class=result_class,
                response_extra_matched=response_extra_matched,
                timed_out=timed_out,
            ),
        )

    async def _send(self, request: Mapping[str, Any]) -> None:
        self.tdlib_send_called = True
        await self._client.send(dict(request))

    async def _receive(self, timeout_sec: float) -> dict[str, Any] | None:
        self.tdlib_receive_called = True
        return await self._client.receive(timeout_sec)

    def _next_extra(self, label: str) -> str:
        self._request_sequence += 1
        return f"{SCRIPT_NAME}.{label}.{self._request_sequence}"


def _default_resolver_factory(
    runtime_env: Mapping[str, str],
    *,
    auth_max_updates: int,
    receive_timeout_sec: float,
    overall_timeout_sec: float,
    post_ready_drain_max_updates: int,
    post_ready_drain_timeout_sec: float,
    post_ready_drain_quiet_empty_receives: int,
    post_ready_drain_quiet_timeout_sec: float,
    single_rpc_max_updates: int,
    single_rpc_receive_timeout_sec: float,
    single_rpc_max_duration_sec: float,
    sync_settle_max_updates: int,
    sync_settle_receive_timeout_sec: float,
    sync_settle_quiet_empty_receives: int,
    sync_settle_max_duration_sec: float,
) -> PublicUsernameResolver:
    return TDLibPublicUsernameResolver(
        runtime_env,
        auth_max_updates=auth_max_updates,
        receive_timeout_sec=receive_timeout_sec,
        overall_timeout_sec=overall_timeout_sec,
        post_ready_drain_max_updates=post_ready_drain_max_updates,
        post_ready_drain_timeout_sec=post_ready_drain_timeout_sec,
        post_ready_drain_quiet_empty_receives=post_ready_drain_quiet_empty_receives,
        post_ready_drain_quiet_timeout_sec=post_ready_drain_quiet_timeout_sec,
        single_rpc_max_updates=single_rpc_max_updates,
        single_rpc_receive_timeout_sec=single_rpc_receive_timeout_sec,
        single_rpc_max_duration_sec=single_rpc_max_duration_sec,
        sync_settle_max_updates=sync_settle_max_updates,
        sync_settle_receive_timeout_sec=sync_settle_receive_timeout_sec,
        sync_settle_quiet_empty_receives=sync_settle_quiet_empty_receives,
        sync_settle_max_duration_sec=sync_settle_max_duration_sec,
    )


def _resolve_result_from_tdlib_payload(
    payload: Mapping[str, Any],
    *,
    response_extra_matched: bool = False,
    response_without_extra_count: int = 0,
    response_wrong_extra_count: int = 0,
    function_response_types_seen: Sequence[str] = (),
) -> PublicUsernameResolveResult:
    response_types = tuple(
        response_type
        for response_type in function_response_types_seen
        if _safe_tdlib_object_type(response_type) == response_type
    )
    if payload.get("@type") == "error":
        error_code = _safe_error_code(payload.get("code"))
        marker_text = f"{payload.get('code', '')} {payload.get('message', '')}".upper()
        if any(marker in marker_text for marker in NOT_FOUND_ERROR_MARKERS):
            return PublicUsernameResolveResult(
                status="not_found",
                failure_class="not_found",
                tdlib_error_code=error_code,
                function_response_types_seen=response_types,
                response_extra_matched=response_extra_matched,
                response_without_extra_count=response_without_extra_count,
                response_wrong_extra_count=response_wrong_extra_count,
            )
        if any(marker in marker_text for marker in ACCESS_DENIED_ERROR_MARKERS):
            return PublicUsernameResolveResult(
                status="access_denied",
                failure_class="access_denied",
                tdlib_error_code=error_code,
                function_response_types_seen=response_types,
                response_extra_matched=response_extra_matched,
                response_without_extra_count=response_without_extra_count,
                response_wrong_extra_count=response_wrong_extra_count,
            )
        return _resolve_failure_result(
            "tdlib_error",
            tdlib_error_code=error_code,
            function_response_types_seen=response_types,
            response_extra_matched=response_extra_matched,
            response_without_extra_count=response_without_extra_count,
            response_wrong_extra_count=response_wrong_extra_count,
        )

    chat_id = _safe_int(payload.get("id"))
    if chat_id is None:
        return _resolve_failure_result(
            "response_shape_error",
            function_response_types_seen=response_types,
            response_extra_matched=response_extra_matched,
            response_without_extra_count=response_without_extra_count,
            response_wrong_extra_count=response_wrong_extra_count,
        )
    chat_type = _extract_chat_type_summary(payload)
    if chat_type not in ALLOWED_CHAT_TYPE_SUMMARIES:
        return PublicUsernameResolveResult(
            status="unsupported_chat_type",
            failure_class="unsupported_chat_type",
            function_response_types_seen=response_types,
            response_extra_matched=response_extra_matched,
            response_without_extra_count=response_without_extra_count,
            response_wrong_extra_count=response_wrong_extra_count,
        )
    return PublicUsernameResolveResult(
        status="resolved",
        chat_id=chat_id,
        username_snapshot=_extract_username_snapshot(payload),
        title_snapshot=_extract_title_snapshot(payload),
        chat_type=chat_type,
        function_response_types_seen=response_types,
        response_extra_matched=response_extra_matched,
        response_without_extra_count=response_without_extra_count,
        response_wrong_extra_count=response_wrong_extra_count,
    )


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_title_snapshot(chat_payload: Mapping[str, Any]) -> str | None:
    value = chat_payload.get("title")
    return value if isinstance(value, str) and value.strip() else None


def _extract_username_snapshot(chat_payload: Mapping[str, Any]) -> str | None:
    usernames = chat_payload.get("usernames")
    if isinstance(usernames, Mapping):
        active = usernames.get("active_usernames")
        if isinstance(active, list) and active:
            candidate = active[0]
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    username = chat_payload.get("username")
    if isinstance(username, str) and username.strip():
        return username.strip()
    return None


def _extract_chat_type_summary(chat_payload: Mapping[str, Any]) -> str | None:
    raw_type = chat_payload.get("type")
    if not isinstance(raw_type, Mapping):
        return None
    type_name = raw_type.get("@type")
    if type_name == "chatTypeSupergroup":
        return "channel" if raw_type.get("is_channel") is True else "supergroup"
    if type_name == "chatTypeBasicGroup":
        return "basic_group"
    return None


def _merge_resolver_side_effects(
    report: dict[str, Any],
    resolver: PublicUsernameResolver | None,
) -> None:
    if resolver is None:
        return
    for flag_name in ("tdlib_send_called", "tdlib_receive_called"):
        if getattr(resolver, flag_name, False) is True:
            report["side_effects"][flag_name] = True
    ready_probe_summary = getattr(resolver, "tdlib_ready_probe_summary", None)
    if isinstance(ready_probe_summary, Mapping):
        for key in {
            *tuple(_empty_ready_probe_report_fields()),
            *tuple(_empty_ready_helper_report_fields()),
        }:
            if key in ready_probe_summary:
                report[key] = ready_probe_summary[key]
    post_ready_drain_summary = getattr(resolver, "tdlib_post_ready_drain_summary", None)
    if isinstance(post_ready_drain_summary, Mapping):
        for key in _empty_post_ready_drain_report_fields():
            if key in post_ready_drain_summary:
                report[key] = post_ready_drain_summary[key]
    sync_settle_summary = getattr(
        resolver,
        "tdlib_sync_settle_diagnostic_summary",
        None,
    )
    if isinstance(sync_settle_summary, Mapping):
        for key in _empty_sync_settle_diagnostic_report_fields(enabled=False):
            if key in sync_settle_summary:
                report[key] = sync_settle_summary[key]


def _post_ready_drain_authorization_lost_next_action() -> str:
    return (
        "TDLib authorization changed away from ready during the post-ready "
        "receive-only drain. No searchPublicChat request was sent and no registry "
        "mutation was committed; restore a ready TDLib session before retrying."
    )


def _sync_settle_contract_status(summary: TDLibSyncSettleDiagnosticSummary) -> str:
    if summary.authorization_lost:
        return "tdlib_sync_settle_diagnostic_authorization_lost"
    if summary.duration_exhausted:
        return "tdlib_sync_settle_diagnostic_duration_exhausted"
    if summary.update_budget_exhausted:
        return "tdlib_sync_settle_diagnostic_update_budget_exhausted"
    if summary.quiet_window_reached:
        return "tdlib_sync_settle_diagnostic_quiet_window_reached"
    return "tdlib_sync_settle_diagnostic_completed"


async def _diagnose_post_ready_sync_settle(
    *,
    resolver: PublicUsernameResolver,
    report: dict[str, Any],
) -> TDLibSyncSettleDiagnosticSummary:
    summary = await resolver.diagnose_post_ready_sync_settle()
    if not isinstance(summary, TDLibSyncSettleDiagnosticSummary):
        summary = TDLibSyncSettleDiagnosticSummary(enabled=True, attempted=True)
        summary.operator_next_action = (
            "The no-search TDLib sync-settle diagnostic returned an unexpected "
            "summary shape. Keep registry mutation disabled and inspect code-level "
            "diagnostic wiring only."
        )
    summary.enabled = True
    summary.search_sent = False
    summary.apply_next_action()
    report.update(summary.as_report_fields())
    if summary.receive_attempt_count > 0:
        report["side_effects"]["tdlib_receive_called"] = True
    report["operator_next_action"] = summary.operator_next_action
    return summary


async def _drain_post_ready_updates(
    *,
    resolver: PublicUsernameResolver,
    report: dict[str, Any],
) -> TDLibPostReadyDrainSummary:
    summary = await resolver.drain_post_ready_updates()
    if not isinstance(summary, TDLibPostReadyDrainSummary):
        summary = TDLibPostReadyDrainSummary()
    report.update(summary.as_report_fields())
    if summary.receive_attempt_count > 0:
        report["side_effects"]["tdlib_receive_called"] = True
    return summary


def _apply_single_resolve_rpc_diagnostic(
    report: dict[str, Any],
    diagnostic: SingleResolveRpcDiagnosticResult,
) -> None:
    report.update(diagnostic.as_report_fields())
    report["operator_next_action"] = diagnostic.operator_next_action


def _single_resolve_rpc_contract_status(
    diagnostic: SingleResolveRpcDiagnosticResult,
) -> str:
    if diagnostic.result_class == "authorization_lost":
        return "single_resolve_rpc_diagnostic_authorization_lost"
    if diagnostic.result_class == "transport_error":
        return "single_resolve_rpc_diagnostic_transport_error"
    if diagnostic.timed_out:
        return "single_resolve_rpc_diagnostic_response_timeout"
    return "single_resolve_rpc_diagnostic_completed"


async def _diagnose_single_resolve_rpc(
    *,
    row: TargetRow,
    resolver: PublicUsernameResolver,
    report: dict[str, Any],
) -> SingleResolveRpcDiagnosticResult:
    report["tdlib_resolve_attempted"] = True
    report["side_effects"]["telegram_api_called"] = True
    report["side_effects"]["tdlib_public_username_resolve_called"] = True
    try:
        diagnostic = await resolver.diagnose_single_resolve_rpc(row.normalized_username)
        if diagnostic.receive_attempt_count > 0:
            report["side_effects"]["tdlib_receive_called"] = True
    except TDLibNotReady:
        diagnostic = SingleResolveRpcDiagnosticResult(
            enabled=True,
            result_class="authorization_lost",
            operator_next_action=_single_resolve_rpc_next_action(
                result_class="authorization_lost",
                response_extra_matched=False,
                timed_out=False,
            ),
        )
    except TDLibTransportUnavailable:
        diagnostic = SingleResolveRpcDiagnosticResult(
            enabled=True,
            result_class="transport_error",
            operator_next_action=_single_resolve_rpc_next_action(
                result_class="transport_error",
                response_extra_matched=False,
                timed_out=False,
            ),
        )
    except Exception:
        diagnostic = SingleResolveRpcDiagnosticResult(
            enabled=True,
            result_class="unknown_error",
            operator_next_action=(
                "The single searchPublicChat diagnostic hit an unexpected class-level "
                "error. Keep registry mutation disabled and inspect code-level "
                "diagnostic wiring only."
            ),
        )

    if not isinstance(diagnostic, SingleResolveRpcDiagnosticResult):
        diagnostic = SingleResolveRpcDiagnosticResult(
            enabled=True,
            result_class="response_shape_error",
            operator_next_action=_single_resolve_rpc_next_action(
                result_class="response_shape_error",
                response_extra_matched=False,
                timed_out=False,
            ),
        )

    if diagnostic.request_sent or diagnostic.send_error_class is not None:
        report["side_effects"]["tdlib_send_called"] = True

    return replace(
        diagnostic,
        enabled=True,
        target_selected=True,
        target_index_bucket=_bucket_count(1),
    )


async def _resolve_rows(
    *,
    rows: Sequence[TargetRow],
    resolver: PublicUsernameResolver,
    report: dict[str, Any],
    connection: DatabaseConnection,
    approved_registry_resolve_mutation: bool,
) -> ResolveReportCounters:
    counters = ResolveReportCounters()

    for row in rows:
        report["tdlib_resolve_attempted"] = True
        report["side_effects"]["telegram_api_called"] = True
        report["side_effects"]["tdlib_send_called"] = True
        report["side_effects"]["tdlib_public_username_resolve_called"] = True
        try:
            resolved = await resolver.resolve_public_username(row.normalized_username)
            report["side_effects"]["tdlib_receive_called"] = True
        except TDLibNotReady:
            resolved = _resolve_failure_result("authorization_lost")
        except TDLibTransportUnavailable:
            resolved = _resolve_failure_result("transport_error")
        except Exception:
            resolved = _resolve_failure_result("unknown_error")

        if not isinstance(resolved, PublicUsernameResolveResult):
            resolved = _resolve_failure_result("response_shape_error")
        resolved = counters.record(resolved)
        if resolved.status == "authorization_lost":
            counters.skipped_count += counters.updated_count
            counters.updated_count = 0
            counters.skipped_count += 1
            break

        if resolved.status != "resolved" or resolved.chat_id is None:
            counters.skipped_count += 1
            continue

        if not approved_registry_resolve_mutation:
            counters.skipped_count += 1
            continue

        if _update_resolved_row(
            connection,
            row=row,
            resolved=resolved,
            resolved_at=datetime.now(timezone.utc),
        ):
            counters.updated_count += 1
        else:
            counters.skipped_count += 1

    return counters


def _apply_count_buckets(
    report: dict[str, Any],
    *,
    counters: ResolveReportCounters,
) -> None:
    report["resolved_count_bucket"] = _bucket_count(counters.resolved_count)
    report["unresolved_count_bucket"] = _bucket_count(counters.unresolved_count)
    report["failed_resolve_count_bucket"] = _bucket_count(counters.failed_count)
    report["updated_row_count_bucket"] = _bucket_count(counters.updated_count)
    report["skipped_row_count_bucket"] = _bucket_count(counters.skipped_count)
    report["resolve_attempt_count_bucket"] = _bucket_count(counters.attempt_count)
    report["resolve_resolved_count_bucket"] = _bucket_count(counters.resolved_count)
    report["resolve_not_found_count_bucket"] = _bucket_count(counters.not_found_count)
    report["resolve_access_denied_count_bucket"] = _bucket_count(
        counters.access_denied_count
    )
    report["resolve_unsupported_chat_type_count_bucket"] = _bucket_count(
        counters.unsupported_chat_type_count
    )
    report["resolve_response_timeout_count_bucket"] = _bucket_count(
        counters.response_timeout_count
    )
    report["resolve_transport_error_count_bucket"] = _bucket_count(
        counters.transport_error_count
    )
    report["resolve_tdlib_error_count_bucket"] = _bucket_count(
        counters.tdlib_error_count
    )
    report["resolve_response_shape_error_count_bucket"] = _bucket_count(
        counters.response_shape_error_count
    )
    report["resolve_authorization_lost_count_bucket"] = _bucket_count(
        counters.authorization_lost_count
    )
    report["resolve_unknown_error_count_bucket"] = _bucket_count(
        counters.unknown_error_count
    )
    report["resolve_failure_classes_seen"] = list(counters.failure_classes_seen)
    report["resolve_tdlib_error_codes_seen"] = list(counters.tdlib_error_codes_seen)
    report["resolve_function_response_types_seen"] = list(
        counters.function_response_types_seen
    )
    report["resolve_response_extra_matched_count_bucket"] = _bucket_count(
        counters.response_extra_matched_count
    )
    report["resolve_response_without_extra_count_bucket"] = _bucket_count(
        counters.response_without_extra_count
    )
    report["resolve_response_wrong_extra_count_bucket"] = _bucket_count(
        counters.response_wrong_extra_count
    )


def _final_success_status(
    *,
    approved_registry_resolve_mutation: bool,
    resolved_count: int,
    unresolved_count: int,
    failed_count: int,
    updated_count: int,
    skipped_count: int,
) -> str:
    has_partial = unresolved_count > 0 or failed_count > 0
    if approved_registry_resolve_mutation:
        if updated_count > 0 and not has_partial and skipped_count == 0:
            return "public_username_resolve_registry_updated"
        return "public_username_resolve_partial"
    if has_partial:
        return "public_username_resolve_partial"
    return "public_username_resolve_completed_no_mutation"


def _tdlib_not_ready_next_action(report: Mapping[str, Any]) -> str:
    if (
        report.get("tdlib_ready_probe_status") == "timed_out"
        and report.get("runtime_env_read") is True
        and report.get("database_connected") is True
        and report.get("target_rows_checked") is True
    ):
        return (
            "TDLib readiness probe timed out before authorizationStateReady. "
            "Review tdlib_ready_probe_* budget and transition fields, then rerun "
            "with a larger --tdlib-auth-max-updates or --tdlib-overall-timeout-sec "
            "if updateOption noise consumed the configured budget; do not change "
            "TDLib parameter shape based on this timeout alone."
        )
    return (
        "TDLib readiness did not reach authorizationStateReady. Review the "
        "tdlib_ready_probe_* fields and only continue with public username resolve "
        "after the existing session is ready."
    )


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    dry_run: bool = True,
    approved_tdlib_public_username_resolve: bool = False,
    approved_registry_resolve_mutation: bool = False,
    diagnose_single_resolve_rpc: bool = False,
    diagnose_tdlib_post_ready_sync_settle: bool = False,
    limit: int | None = None,
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
    tdlib_post_ready_drain_max_updates: int = (
        DEFAULT_TDLIB_POST_READY_DRAIN_MAX_UPDATES
    ),
    tdlib_post_ready_drain_timeout_sec: float = (
        DEFAULT_TDLIB_POST_READY_DRAIN_TIMEOUT_SEC
    ),
    tdlib_post_ready_drain_quiet_empty_receives: int = (
        DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_EMPTY_RECEIVES
    ),
    tdlib_post_ready_drain_quiet_timeout_sec: float = (
        DEFAULT_TDLIB_POST_READY_DRAIN_QUIET_TIMEOUT_SEC
    ),
    tdlib_single_rpc_max_updates: int = DEFAULT_TDLIB_SINGLE_RPC_MAX_UPDATES,
    tdlib_single_rpc_receive_timeout_sec: float = (
        DEFAULT_TDLIB_SINGLE_RPC_RECEIVE_TIMEOUT_SEC
    ),
    tdlib_single_rpc_max_duration_sec: float = (
        DEFAULT_TDLIB_SINGLE_RPC_MAX_DURATION_SEC
    ),
    tdlib_sync_settle_max_updates: int = DEFAULT_TDLIB_SYNC_SETTLE_MAX_UPDATES,
    tdlib_sync_settle_receive_timeout_sec: float = (
        DEFAULT_TDLIB_SYNC_SETTLE_RECEIVE_TIMEOUT_SEC
    ),
    tdlib_sync_settle_quiet_empty_receives: int = (
        DEFAULT_TDLIB_SYNC_SETTLE_QUIET_EMPTY_RECEIVES
    ),
    tdlib_sync_settle_max_duration_sec: float = (
        DEFAULT_TDLIB_SYNC_SETTLE_MAX_DURATION_SEC
    ),
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    public_username_resolver_factory: PublicUsernameResolverFactory | None = None,
) -> ScriptResult:
    effective_dry_run = bool(dry_run or not approved_tdlib_public_username_resolve)
    report = _base_report(
        dry_run=effective_dry_run,
        approved_tdlib_public_username_resolve=approved_tdlib_public_username_resolve,
        approved_registry_resolve_mutation=approved_registry_resolve_mutation,
        diagnose_single_resolve_rpc=diagnose_single_resolve_rpc,
        diagnose_tdlib_post_ready_sync_settle=(
            diagnose_tdlib_post_ready_sync_settle
        ),
    )

    try:
        values = _read_runtime_env(runtime_env_path, runtime_env_reader)
    except Exception:
        _set_status(report, "blocked_runtime_env_unreadable", "runtime_env.unreadable")
        return ScriptResult(exit_code=1, report=report)
    report["runtime_env_read"] = True

    database_url = values.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        _set_status(report, "blocked_database_unavailable", "database.url_missing")
        return ScriptResult(exit_code=1, report=report)
    if not _database_url_is_supported(database_url):
        _set_status(report, "blocked_database_unavailable", "database.url_unsupported")
        return ScriptResult(exit_code=1, report=report)

    connection: DatabaseConnection | None = None
    cleanup: Callable[[], None] | None = None
    transaction: Any | None = None
    transaction_committed = False
    resolver: PublicUsernameResolver | None = None
    mutation_mode_requested = bool(
        approved_tdlib_public_username_resolve
        and approved_registry_resolve_mutation
        and not effective_dry_run
        and not diagnose_single_resolve_rpc
        and not diagnose_tdlib_post_ready_sync_settle
    )

    try:
        try:
            connection, cleanup = _open_database_connection(
                database_url,
                database_connection_factory,
            )
            transaction = connection.begin()
            if not mutation_mode_requested:
                _execute_read(connection, SET_TRANSACTION_READ_ONLY_QUERY)
            _execute_read(connection, SELECT_ONE_QUERY)
            report["database_connected"] = True
            table_available = bool(
                _scalar(
                    _execute_read(
                        connection,
                        TABLE_AVAILABLE_QUERY,
                        {"qualified_table_name": "public.telegram_channel_registry"},
                    )
                )
            )
            if not table_available:
                _set_status(
                    report,
                    "blocked_database_unavailable",
                    "database.channel_registry_table_unavailable",
                )
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(report, "blocked_database_unavailable", "database.connection")
            return ScriptResult(exit_code=1, report=report)

        target_count = _count_target_rows(connection)
        report["target_rows_checked"] = True
        report["target_row_count_bucket"] = _bucket_count(target_count)
        if target_count == 0 and not diagnose_tdlib_post_ready_sync_settle:
            _set_status(
                report,
                "blocked_no_unresolved_public_username_rows",
                "registry.no_unresolved_public_username_rows",
            )
            report["operator_next_action"] = (
                "No unresolved active public_username registry rows are available "
                "for this resolve operator."
            )
            return ScriptResult(exit_code=1, report=report)

        if approved_registry_resolve_mutation and not approved_tdlib_public_username_resolve:
            _set_status(report, "blocked_approval_required", "approval.tdlib_resolve_required")
            report["operator_next_action"] = (
                "Registry resolve mutation requires both explicit TDLib public "
                "username resolve approval and explicit registry mutation approval."
            )
            return ScriptResult(exit_code=1, report=report)

        if (
            diagnose_tdlib_post_ready_sync_settle
            and not approved_tdlib_public_username_resolve
        ):
            _set_status(report, "blocked_approval_required", "approval.tdlib_resolve_required")
            report["operator_next_action"] = (
                "TDLib post-ready sync-settle diagnostic requires explicit TDLib "
                "public username resolve approval, but still sends no searchPublicChat "
                "request and performs no registry mutation."
            )
            report["tdlib_sync_settle_operator_next_action"] = report[
                "operator_next_action"
            ]
            return ScriptResult(exit_code=1, report=report)

        if diagnose_single_resolve_rpc and not approved_tdlib_public_username_resolve:
            _set_status(report, "blocked_approval_required", "approval.tdlib_resolve_required")
            report["operator_next_action"] = (
                "Single public username RPC diagnostic requires explicit TDLib public "
                "username resolve approval, but still performs no registry mutation."
            )
            report["single_resolve_rpc_operator_next_action"] = report[
                "operator_next_action"
            ]
            return ScriptResult(exit_code=1, report=report)

        if effective_dry_run:
            _set_status(report, "dry_run_public_username_resolve_plan_ready")
            report["operator_next_action"] = (
                "Review the unresolved public_username bucket. Re-run on the VPS "
                "with --approved-tdlib-public-username-resolve to resolve without "
                "mutation, and add --approved-registry-resolve-mutation only after "
                "operator approval."
            )
            return ScriptResult(exit_code=0, report=report)

        rows: tuple[TargetRow, ...] = ()
        if not diagnose_tdlib_post_ready_sync_settle:
            row_limit = 1 if diagnose_single_resolve_rpc else limit
            if diagnose_single_resolve_rpc and limit is not None:
                row_limit = min(limit, 1)
            rows = _load_target_rows(connection, limit=row_limit)
            if not rows:
                _set_status(
                    report,
                    "blocked_no_unresolved_public_username_rows",
                    "registry.no_valid_public_username_rows_selected",
                )
                return ScriptResult(exit_code=1, report=report)
            if diagnose_single_resolve_rpc:
                report["single_resolve_rpc_target_selected"] = True
                report["single_resolve_rpc_target_index_bucket"] = _bucket_count(1)

        try:
            if public_username_resolver_factory is None:
                resolver = _default_resolver_factory(
                    values,
                    auth_max_updates=tdlib_auth_max_updates,
                    receive_timeout_sec=tdlib_receive_timeout_sec,
                    overall_timeout_sec=tdlib_overall_timeout_sec,
                    post_ready_drain_max_updates=tdlib_post_ready_drain_max_updates,
                    post_ready_drain_timeout_sec=tdlib_post_ready_drain_timeout_sec,
                    post_ready_drain_quiet_empty_receives=(
                        tdlib_post_ready_drain_quiet_empty_receives
                    ),
                    post_ready_drain_quiet_timeout_sec=(
                        tdlib_post_ready_drain_quiet_timeout_sec
                    ),
                    single_rpc_max_updates=tdlib_single_rpc_max_updates,
                    single_rpc_receive_timeout_sec=(
                        tdlib_single_rpc_receive_timeout_sec
                    ),
                    single_rpc_max_duration_sec=(
                        tdlib_single_rpc_max_duration_sec
                    ),
                    sync_settle_max_updates=tdlib_sync_settle_max_updates,
                    sync_settle_receive_timeout_sec=(
                        tdlib_sync_settle_receive_timeout_sec
                    ),
                    sync_settle_quiet_empty_receives=(
                        tdlib_sync_settle_quiet_empty_receives
                    ),
                    sync_settle_max_duration_sec=(
                        tdlib_sync_settle_max_duration_sec
                    ),
                )
            else:
                resolver = public_username_resolver_factory(values)
            awaitable = resolver.initialize()
            asyncio.run(awaitable)
            report["side_effects"]["tdlib_initialized"] = True
            _merge_resolver_side_effects(report, resolver)
        except TDLibNotReady:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
            report["side_effects"]["tdlib_initialized"] = True
            _merge_resolver_side_effects(report, resolver)
            report["operator_next_action"] = _tdlib_not_ready_next_action(report)
            return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.transport_unavailable",
            )
            _merge_resolver_side_effects(report, resolver)
            return ScriptResult(exit_code=1, report=report)

        if diagnose_tdlib_post_ready_sync_settle:
            try:
                sync_settle_summary = asyncio.run(
                    _diagnose_post_ready_sync_settle(
                        resolver=resolver,
                        report=report,
                    )
                )
                _merge_resolver_side_effects(report, resolver)
            except TDLibTransportUnavailable:
                _set_status(
                    report,
                    "blocked_tdlib_transport_unavailable",
                    "tdlib.sync_settle_transport_unavailable",
                )
                _merge_resolver_side_effects(report, resolver)
                return ScriptResult(exit_code=1, report=report)
            except Exception:
                _set_status(
                    report,
                    "blocked_unexpected_error",
                    "tdlib.sync_settle_unexpected_error",
                )
                _merge_resolver_side_effects(report, resolver)
                return ScriptResult(exit_code=1, report=report)
            status = _sync_settle_contract_status(sync_settle_summary)
            _set_status(
                report,
                status,
                "tdlib.sync_settle_authorization_lost"
                if sync_settle_summary.authorization_lost
                else None,
            )
            return ScriptResult(
                exit_code=1 if sync_settle_summary.authorization_lost else 0,
                report=report,
            )

        if diagnose_single_resolve_rpc:
            diagnostic = asyncio.run(
                _diagnose_single_resolve_rpc(
                    row=rows[0],
                    resolver=resolver,
                    report=report,
                )
            )
            _apply_single_resolve_rpc_diagnostic(report, diagnostic)
            _merge_resolver_side_effects(report, resolver)
            _set_status(report, _single_resolve_rpc_contract_status(diagnostic))
            return ScriptResult(exit_code=0, report=report)

        try:
            drain_summary = asyncio.run(
                _drain_post_ready_updates(
                    resolver=resolver,
                    report=report,
                )
            )
            _merge_resolver_side_effects(report, resolver)
            if drain_summary.authorization_lost:
                _set_status(
                    report,
                    "blocked_tdlib_post_ready_drain_authorization_lost",
                    "tdlib.post_ready_drain_authorization_lost",
                )
                report["operator_next_action"] = (
                    _post_ready_drain_authorization_lost_next_action()
                )
                return ScriptResult(exit_code=1, report=report)
        except TDLibNotReady:
            _set_status(
                report,
                "blocked_tdlib_post_ready_drain_authorization_lost",
                "tdlib.post_ready_drain_authorization_lost",
            )
            _merge_resolver_side_effects(report, resolver)
            report["operator_next_action"] = (
                _post_ready_drain_authorization_lost_next_action()
            )
            return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.post_ready_drain_transport_unavailable",
            )
            _merge_resolver_side_effects(report, resolver)
            return ScriptResult(exit_code=1, report=report)

        try:
            resolve_counters = asyncio.run(
                _resolve_rows(
                    rows=rows,
                    resolver=resolver,
                    report=report,
                    connection=connection,
                    approved_registry_resolve_mutation=approved_registry_resolve_mutation,
                )
            )
        except TDLibNotReady:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
            _merge_resolver_side_effects(report, resolver)
            report["operator_next_action"] = _tdlib_not_ready_next_action(report)
            return ScriptResult(exit_code=1, report=report)
        _merge_resolver_side_effects(report, resolver)

        _apply_count_buckets(
            report,
            counters=resolve_counters,
        )
        report["registry_resolve_mutation_performed"] = resolve_counters.updated_count > 0
        report["side_effects"]["database_mutation_performed"] = (
            resolve_counters.updated_count > 0
        )
        report["side_effects"]["telegram_channel_registry_updated"] = (
            resolve_counters.updated_count > 0
        )

        status = _final_success_status(
            approved_registry_resolve_mutation=approved_registry_resolve_mutation,
            resolved_count=resolve_counters.resolved_count,
            unresolved_count=resolve_counters.unresolved_count,
            failed_count=resolve_counters.failed_count,
            updated_count=resolve_counters.updated_count,
            skipped_count=resolve_counters.skipped_count,
        )
        _set_status(report, status)
        if resolve_counters.authorization_lost_seen:
            report["operator_next_action"] = (
                "TDLib authorization was lost during public username resolve. "
                "No registry mutation was committed; restore a ready TDLib session "
                "before any separately approved registry mutation run."
            )
        elif approved_registry_resolve_mutation:
            report["operator_next_action"] = (
                "Resolved registry metadata was applied only where the guarded "
                "public_username/unresolved/chat_id-null UPDATE matched. Do not "
                "treat these rows as joined; use a separate approved join slice."
            )
        else:
            report["operator_next_action"] = (
                "TDLib public username resolve completed without DB mutation. "
                "Review coarse buckets before separately approving registry mutation."
            )

        if resolve_counters.updated_count > 0:
            _commit_transaction(transaction)
            transaction_committed = True
        return ScriptResult(exit_code=0, report=report)
    except Exception:
        _set_status(report, "blocked_unexpected_error", "unexpected_error")
        return ScriptResult(exit_code=1, report=report)
    finally:
        if resolver is not None:
            try:
                asyncio.run(resolver.close())
            except Exception:
                pass
        if not transaction_committed:
            _rollback_transaction(transaction)
        _close_connection(cleanup, connection)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        dry_run=args.dry_run,
        approved_tdlib_public_username_resolve=args.approved_tdlib_public_username_resolve,
        approved_registry_resolve_mutation=args.approved_registry_resolve_mutation,
        diagnose_single_resolve_rpc=args.diagnose_single_resolve_rpc,
        diagnose_tdlib_post_ready_sync_settle=(
            args.diagnose_tdlib_post_ready_sync_settle
        ),
        limit=args.limit,
        tdlib_auth_max_updates=args.tdlib_auth_max_updates,
        tdlib_receive_timeout_sec=args.tdlib_receive_timeout_sec,
        tdlib_overall_timeout_sec=args.tdlib_overall_timeout_sec,
        tdlib_post_ready_drain_max_updates=args.tdlib_post_ready_drain_max_updates,
        tdlib_post_ready_drain_timeout_sec=args.tdlib_post_ready_drain_timeout_sec,
        tdlib_post_ready_drain_quiet_empty_receives=(
            args.tdlib_post_ready_drain_quiet_empty_receives
        ),
        tdlib_post_ready_drain_quiet_timeout_sec=(
            args.tdlib_post_ready_drain_quiet_timeout_sec
        ),
        tdlib_single_rpc_max_updates=args.tdlib_single_rpc_max_updates,
        tdlib_single_rpc_receive_timeout_sec=(
            args.tdlib_single_rpc_receive_timeout_sec
        ),
        tdlib_single_rpc_max_duration_sec=args.tdlib_single_rpc_max_duration_sec,
        tdlib_sync_settle_max_updates=args.tdlib_sync_settle_max_updates,
        tdlib_sync_settle_receive_timeout_sec=(
            args.tdlib_sync_settle_receive_timeout_sec
        ),
        tdlib_sync_settle_quiet_empty_receives=(
            args.tdlib_sync_settle_quiet_empty_receives
        ),
        tdlib_sync_settle_max_duration_sec=args.tdlib_sync_settle_max_duration_sec,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
