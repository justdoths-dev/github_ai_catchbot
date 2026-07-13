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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = (
    "dedicated_vps_telegram_channel_registry_resolved_not_joined_join_operator"
)
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 200
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC = 240.0
DEFAULT_TDLIB_JOIN_RPC_MAX_UPDATES = 120
DEFAULT_TDLIB_JOIN_RPC_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_JOIN_RPC_MAX_DURATION_SEC = 60.0
TDLIB_READY_STATE = "authorizationStateReady"

SELECT_ONE_QUERY = "SELECT 1"
SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
COUNT_TARGET_ROWS_QUERY = """
SELECT COUNT(*)
FROM telegram_channel_registry
WHERE source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'resolved_not_joined'
  AND chat_id IS NOT NULL
"""
SELECT_TARGET_ROWS_QUERY = """
SELECT registry_id, chat_id
FROM telegram_channel_registry
WHERE source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'resolved_not_joined'
  AND chat_id IS NOT NULL
ORDER BY priority_weight DESC, registry_id ASC
"""
SELECT_TARGET_ROWS_LIMIT_QUERY = """
SELECT registry_id, chat_id
FROM telegram_channel_registry
WHERE source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'resolved_not_joined'
  AND chat_id IS NOT NULL
ORDER BY priority_weight DESC, registry_id ASC
LIMIT :limit
"""
SELECT_EXACT_TARGET_ROWS_QUERY = """
SELECT registry_id, source_value, source_kind, desired_state, access_state, chat_id
FROM telegram_channel_registry
WHERE source_kind = 'public_username'
  AND lower(
    regexp_replace(
      regexp_replace(
        source_value,
        '^(https://t[.]me/|http://t[.]me/|t[.]me/)',
        ''
      ),
      '^@',
      ''
    )
  ) = :normalized_source_value
  AND desired_state = 'active'
ORDER BY registry_id ASC
LIMIT 2
"""
UPDATE_JOIN_STATE_REGISTRY_ROW_QUERY = """
UPDATE telegram_channel_registry
SET
  access_state = :access_state,
  last_join_attempt_at = :attempted_at,
  updated_at = :attempted_at
WHERE registry_id = :registry_id
  AND source_kind = 'public_username'
  AND desired_state = 'active'
  AND access_state = 'resolved_not_joined'
  AND chat_id = :chat_id
  AND chat_id IS NOT NULL
"""
UPDATE_EXACT_JOIN_STATE_REGISTRY_ROW_QUERY = """
UPDATE telegram_channel_registry
SET
  access_state = 'joined',
  last_join_attempt_at = :attempted_at,
  updated_at = :attempted_at
WHERE registry_id = :registry_id
  AND source_kind = 'public_username'
  AND source_value = :source_value
  AND lower(
    regexp_replace(
      regexp_replace(
        source_value,
        '^(https://t[.]me/|http://t[.]me/|t[.]me/)',
        ''
      ),
      '^@',
      ''
    )
  ) = :normalized_source_value
  AND desired_state = 'active'
  AND access_state = 'resolved_not_joined'
  AND chat_id = :chat_id
  AND chat_id IS NOT NULL
"""
SELECT_EXACT_JOIN_READBACK_QUERY = """
SELECT registry_id, source_value, source_kind, desired_state, access_state, chat_id
FROM telegram_channel_registry
WHERE registry_id = :registry_id
  AND source_kind = 'public_username'
  AND source_value = :source_value
  AND lower(
    regexp_replace(
      regexp_replace(
        source_value,
        '^(https://t[.]me/|http://t[.]me/|t[.]me/)',
        ''
      ),
      '^@',
      ''
    )
  ) = :normalized_source_value
  AND desired_state = 'active'
  AND access_state = 'joined'
  AND chat_id = :chat_id
  AND chat_id IS NOT NULL
"""

SIDE_EFFECT_FLAG_NAMES = (
    "database_mutation_performed",
    "telegram_channel_registry_updated",
    "telegram_channel_registry_inserted",
    "telegram_channel_registry_deleted",
    "telegram_api_called",
    "tdlib_initialized",
    "tdlib_send_called",
    "tdlib_receive_called",
    "tdlib_join_called",
    "tdlib_history_fetch_called",
    "live_collector_started",
    "collector_runtime_started",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "redis_mutation_performed",
    "notifier_transport_enabled",
    "outbox_relay_started",
    "router_normalizer_started",
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

JOIN_SUCCESS_RESPONSE_TYPES = frozenset({"chat", "ok"})
JOIN_REQUEST_ERROR_MARKERS = (
    "INVITE_REQUEST_SENT",
    "JOIN_REQUEST",
    "REQUEST_SENT",
)
ALREADY_JOINED_ERROR_MARKERS = (
    "USER_ALREADY_PARTICIPANT",
    "ALREADY_PARTICIPANT",
)
FORBIDDEN_ERROR_MARKERS = (
    "FORBIDDEN",
    "CHANNEL_PRIVATE",
    "CHAT_ADMIN_REQUIRED",
    "USER_BANNED_IN_CHANNEL",
    "ACCESS_DENIED",
    "PRIVATE",
)
NOT_FOUND_ERROR_MARKERS = (
    "CHAT_NOT_FOUND",
    "USERNAME_NOT_OCCUPIED",
    "USERNAME_INVALID",
)
JOIN_RESULT_STATUSES = frozenset(
    {
        "joined",
        "join_requested",
        "forbidden",
        "not_found",
        "response_timeout",
        "transport_error",
        "tdlib_error",
        "response_shape_error",
        "authorization_lost",
        "unknown_error",
    }
)
JOIN_MUTATION_ACCESS_STATE_BY_STATUS = {
    "joined": "joined",
    "join_requested": "join_requested",
    "forbidden": "forbidden",
}


class DatabaseConnection(Protocol):
    def begin(self) -> Any: ...

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


class ResolvedNotJoinedJoiner(Protocol):
    async def initialize(self) -> None: ...

    async def join_chat(self, chat_id: int) -> "JoinChatResult": ...

    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseConnectionFactory = Callable[[str], DatabaseConnection]
ResolvedNotJoinedJoinerFactory = Callable[[Mapping[str, str]], ResolvedNotJoinedJoiner]


class TDLibTransportUnavailable(RuntimeError):
    pass


class TDLibNotReady(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TargetRow:
    registry_id: str
    chat_id: int


@dataclass(frozen=True, slots=True)
class ExactTargetRow:
    registry_id: str
    source_value: str
    normalized_source_value: str
    source_kind: str
    desired_state: str
    access_state: str
    chat_id: int


@dataclass(frozen=True, slots=True)
class JoinChatResult:
    status: str
    failure_class: str | None = None
    tdlib_error_code: int | str | None = None
    function_response_types_seen: tuple[str, ...] = ()
    response_extra_matched: bool = False
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0


@dataclass(frozen=True, slots=True)
class TDLibJoinRpcWaitConfig:
    max_updates: int = DEFAULT_TDLIB_JOIN_RPC_MAX_UPDATES
    receive_timeout_sec: float = DEFAULT_TDLIB_JOIN_RPC_RECEIVE_TIMEOUT_SEC
    max_duration_sec: float = DEFAULT_TDLIB_JOIN_RPC_MAX_DURATION_SEC


@dataclass(slots=True)
class _JoinRpcWaitResult:
    receive_attempt_count: int = 0
    observation_count: int = 0
    empty_receive_count: int = 0
    inbound_object_types_seen: list[str] = field(default_factory=list)
    function_response_types_seen: list[str] = field(default_factory=list)
    update_types_seen: list[str] = field(default_factory=list)
    authorization_states_seen: list[str] = field(default_factory=list)
    final_authorization_state: str | None = None
    update_budget_exhausted: bool = False
    duration_exhausted: bool = False
    response_extra_matched: bool = False
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0
    result_class: str = "response_timeout"
    tdlib_error_codes_seen: list[int | str] = field(default_factory=list)
    timed_out: bool = True
    result: JoinChatResult | None = None


@dataclass(slots=True)
class JoinReportCounters:
    attempt_count: int = 0
    joined_count: int = 0
    join_requested_count: int = 0
    forbidden_count: int = 0
    not_found_count: int = 0
    response_timeout_count: int = 0
    transport_error_count: int = 0
    tdlib_error_count: int = 0
    response_shape_error_count: int = 0
    authorization_lost_count: int = 0
    unknown_error_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    response_extra_matched_count: int = 0
    response_without_extra_count: int = 0
    response_wrong_extra_count: int = 0
    authorization_lost_seen: bool = False
    failure_classes_seen: list[str] = field(default_factory=list)
    function_response_types_seen: list[str] = field(default_factory=list)

    def record(self, result: JoinChatResult) -> JoinChatResult:
        result = _canonical_join_result(result)
        self.attempt_count += 1
        self.response_without_extra_count += max(result.response_without_extra_count, 0)
        self.response_wrong_extra_count += max(result.response_wrong_extra_count, 0)
        if result.response_extra_matched:
            self.response_extra_matched_count += 1
        for response_type in result.function_response_types_seen:
            if _safe_tdlib_object_type(response_type) == response_type:
                _append_unique(self.function_response_types_seen, response_type)

        status = result.status
        if status == "joined":
            self.joined_count += 1
        elif status == "join_requested":
            self.join_requested_count += 1
            _append_unique(self.failure_classes_seen, "join_requested")
        elif status == "forbidden":
            self.forbidden_count += 1
            _append_unique(self.failure_classes_seen, "forbidden")
        elif status == "not_found":
            self.not_found_count += 1
            _append_unique(self.failure_classes_seen, "not_found")
        elif status == "response_timeout":
            self.response_timeout_count += 1
            _append_unique(self.failure_classes_seen, "response_timeout")
        elif status == "transport_error":
            self.transport_error_count += 1
            _append_unique(self.failure_classes_seen, "transport_error")
        elif status == "tdlib_error":
            self.tdlib_error_count += 1
            _append_unique(self.failure_classes_seen, "tdlib_error")
        elif status == "response_shape_error":
            self.response_shape_error_count += 1
            _append_unique(self.failure_classes_seen, "response_shape_error")
        elif status == "authorization_lost":
            self.authorization_lost_count += 1
            self.authorization_lost_seen = True
            _append_unique(self.failure_classes_seen, "authorization_lost")
        else:
            self.unknown_error_count += 1
            _append_unique(self.failure_classes_seen, "unknown_error")
        return result


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import (  # noqa: E402
    dedicated_vps_tdlib_session_reuse_collector_readiness_preflight as session_preflight,
)
from src.services.collector_telegram import (  # noqa: E402
    bounded_history_ingest_runner,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Join active public_username telegram_channel_registry rows whose "
            "access_state is resolved_not_joined and chat_id is already present. "
            "Default mode is a DB-read-only dry-run; TDLib join and registry "
            "mutation each require explicit operator approval flags."
        )
    )
    parser.add_argument("--runtime-env-path", required=True)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--approved-tdlib-join-resolved-not-joined",
        action="store_true",
    )
    parser.add_argument("--approved-registry-join-mutation", action="store_true")
    target_selection = parser.add_mutually_exclusive_group()
    target_selection.add_argument("--target-locator-path", default=None)
    target_selection.add_argument("--limit", type=_positive_int, default=None)
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
        "--tdlib-join-rpc-max-updates",
        type=_positive_int_named("tdlib-join-rpc-max-updates"),
        default=DEFAULT_TDLIB_JOIN_RPC_MAX_UPDATES,
    )
    parser.add_argument(
        "--tdlib-join-rpc-receive-timeout-sec",
        type=_non_negative_float_named("tdlib-join-rpc-receive-timeout-sec"),
        default=DEFAULT_TDLIB_JOIN_RPC_RECEIVE_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-join-rpc-max-duration-sec",
        type=_non_negative_float_named("tdlib-join-rpc-max-duration-sec"),
        default=DEFAULT_TDLIB_JOIN_RPC_MAX_DURATION_SEC,
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


def _base_report(
    *,
    dry_run: bool,
    approved_tdlib_join_resolved_not_joined: bool,
    approved_registry_join_mutation: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "target_rows_checked": False,
        "target_row_count_bucket": "unknown",
        "dry_run": dry_run,
        "approved_tdlib_join_resolved_not_joined": (
            approved_tdlib_join_resolved_not_joined
        ),
        "approved_registry_join_mutation": approved_registry_join_mutation,
        "tdlib_join_attempted": False,
        "join_attempt_count_bucket": "zero",
        "join_success_count_bucket": "zero",
        "join_requested_count_bucket": "zero",
        "join_forbidden_count_bucket": "zero",
        "join_not_found_count_bucket": "zero",
        "join_response_timeout_count_bucket": "zero",
        "join_transport_error_count_bucket": "zero",
        "join_tdlib_error_count_bucket": "zero",
        "join_authorization_lost_count_bucket": "zero",
        "join_unknown_error_count_bucket": "zero",
        "join_response_shape_error_count_bucket": "zero",
        "join_failure_classes_seen": [],
        "join_function_response_types_seen": [],
        "join_response_extra_matched_count_bucket": "zero",
        "join_response_without_extra_count_bucket": "zero",
        "join_response_wrong_extra_count_bucket": "zero",
        "updated_row_count_bucket": "zero",
        "skipped_row_count_bucket": "zero",
        "registry_join_mutation_performed": False,
        "side_effects": _side_effects(),
        "operator_next_action": (
            "Fix runtime env or DB access on the VPS without pasting runtime.env "
            "values, DB URLs, Redis URLs, usernames, titles, chat IDs, phone "
            "numbers, api hashes, invite links, TDLib payloads, @extra values, "
            "or private stderr into ChatGPT."
        ),
    }


def _initialize_exact_target_report(report: dict[str, Any]) -> None:
    report.update(
        {
            "exact_target_mode": True,
            "target_locator_present": True,
            "target_locator_read": False,
            "exact_target_match_count_bucket": "unknown",
            "exact_target_noop": False,
            "exact_target_durable_readback_matched": False,
            "exact_target_mutation_outcome": "not_attempted",
            "exact_target_cleanup_failure_codes": [],
            "exact_target_read_rollback_succeeded": None,
            "exact_target_mutation_rollback_succeeded": None,
            "exact_target_transport_close_succeeded": None,
            "exact_target_connection_cleanup_succeeded": None,
        }
    )


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
            SELECT_EXACT_TARGET_ROWS_QUERY,
            SELECT_EXACT_JOIN_READBACK_QUERY,
        )
    }


def _assert_read_sql(statement: str) -> None:
    if _normalize_sql(statement) not in _allowed_read_statements():
        raise ValueError("SQL statement is not in the resolved-not-joined read allowlist")


def _assert_update_sql(statement: str) -> None:
    if _normalize_sql(statement) not in {
        _normalize_sql(UPDATE_JOIN_STATE_REGISTRY_ROW_QUERY),
        _normalize_sql(UPDATE_EXACT_JOIN_STATE_REGISTRY_ROW_QUERY),
    }:
        raise ValueError(
            "SQL statement is not in the resolved-not-joined update allowlist"
        )


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


def _record_exact_cleanup_failure(report: dict[str, Any], code: str) -> None:
    failures = report["exact_target_cleanup_failure_codes"]
    if code not in failures:
        failures.append(code)


def _rollback_exact_transaction(
    transaction: Any,
    *,
    report: dict[str, Any],
    result_field: str,
    failure_code: str,
) -> bool:
    try:
        _rollback_transaction(transaction)
    except Exception:
        report[result_field] = False
        _record_exact_cleanup_failure(report, failure_code)
        return False
    report[result_field] = True
    return True


def _close_exact_transport(
    joiner: ResolvedNotJoinedJoiner,
    *,
    report: dict[str, Any],
) -> None:
    try:
        asyncio.run(joiner.close())
    except Exception:
        report["exact_target_transport_close_succeeded"] = False
        _record_exact_cleanup_failure(report, "transport_close_failed")
        return
    report["exact_target_transport_close_succeeded"] = True


def _cleanup_exact_connection(
    cleanup: Callable[[], None] | None,
    connection: DatabaseConnection | None,
    *,
    report: dict[str, Any],
) -> None:
    if cleanup is None and connection is None:
        return
    try:
        _close_connection(cleanup, connection)
    except Exception:
        report["exact_target_connection_cleanup_succeeded"] = False
        _record_exact_cleanup_failure(report, "connection_cleanup_failed")
        return
    report["exact_target_connection_cleanup_succeeded"] = True


def _finalize_exact_target_cleanup(
    result: ScriptResult,
    *,
    report: dict[str, Any],
    read_transaction: Any | None,
    read_rollback_attempted: bool,
    mutation_transaction: Any | None,
    mutation_transaction_committed: bool,
    joiner: ResolvedNotJoinedJoiner | None,
    cleanup: Callable[[], None] | None,
    connection: DatabaseConnection | None,
) -> ScriptResult:
    if read_transaction is not None and not read_rollback_attempted:
        _rollback_exact_transaction(
            read_transaction,
            report=report,
            result_field="exact_target_read_rollback_succeeded",
            failure_code="read_rollback_failed",
        )

    if mutation_transaction is not None and not mutation_transaction_committed:
        if _rollback_exact_transaction(
            mutation_transaction,
            report=report,
            result_field="exact_target_mutation_rollback_succeeded",
            failure_code="mutation_rollback_failed",
        ):
            report["exact_target_mutation_outcome"] = "rolled_back"
        else:
            report["exact_target_mutation_outcome"] = (
                "unknown_after_rollback_failure"
            )

    if joiner is not None:
        _close_exact_transport(joiner, report=report)
    _cleanup_exact_connection(cleanup, connection, report=report)

    failures = report["exact_target_cleanup_failure_codes"]
    if not failures:
        return result

    if report["exact_target_read_rollback_succeeded"] is False:
        _set_status(
            report,
            "blocked_exact_target_read_rollback_failed",
            "exact_target.read_rollback_failed",
        )
    elif report["exact_target_mutation_rollback_succeeded"] is False:
        _set_status(
            report,
            "blocked_exact_target_mutation_rollback_failed",
            "exact_target.mutation_rollback_failed",
        )
    elif report["exact_target_mutation_outcome"] == "committed_durable":
        _set_status(
            report,
            "exact_target_cleanup_failed_after_commit",
            "exact_target.cleanup_failed_after_commit",
        )
    elif "connection_cleanup_failed" in failures:
        _set_status(
            report,
            "blocked_exact_target_connection_cleanup_failed",
            "exact_target.connection_cleanup_failed",
        )
    return ScriptResult(exit_code=1, report=report)


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


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def _safe_join_status(value: Any) -> str:
    if isinstance(value, str) and value in JOIN_RESULT_STATUSES:
        return value
    return "unknown_error"


def _safe_join_failure_class(value: Any) -> str:
    status = _safe_join_status(value)
    if status == "joined":
        return "unknown_error"
    return status


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _append_unique_safe_error_code(values: list[int | str], value: Any) -> None:
    safe_value = _safe_error_code(value)
    if safe_value is not None and safe_value not in values:
        values.append(safe_value)


def _function_response_type_from_payload(payload: Mapping[str, Any]) -> str | None:
    payload_type = _safe_tdlib_object_type(payload.get("@type"))
    if payload_type is None:
        return None
    if payload_type.startswith("update") or payload_type.startswith("authorizationState"):
        return None
    return payload_type


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


def _canonical_join_result(result: JoinChatResult) -> JoinChatResult:
    status = _safe_join_status(result.status)
    failure_class = result.failure_class
    if status == "joined":
        failure_class = None
    else:
        failure_class = _safe_join_failure_class(failure_class or status)
    response_types = tuple(
        response_type
        for response_type in result.function_response_types_seen
        if _safe_tdlib_object_type(response_type) == response_type
    )
    if (
        status == result.status
        and failure_class == result.failure_class
        and response_types == result.function_response_types_seen
    ):
        return result
    return JoinChatResult(
        status=status,
        failure_class=failure_class,
        tdlib_error_code=_safe_error_code(result.tdlib_error_code),
        function_response_types_seen=response_types,
        response_extra_matched=result.response_extra_matched,
        response_without_extra_count=max(result.response_without_extra_count, 0),
        response_wrong_extra_count=max(result.response_wrong_extra_count, 0),
    )


def _join_failure_result(
    failure_class: str,
    *,
    tdlib_error_code: int | str | None = None,
    function_response_types_seen: Sequence[str] = (),
    response_extra_matched: bool = False,
    response_without_extra_count: int = 0,
    response_wrong_extra_count: int = 0,
) -> JoinChatResult:
    safe_failure_class = _safe_join_failure_class(failure_class)
    response_types = tuple(
        response_type
        for response_type in function_response_types_seen
        if _safe_tdlib_object_type(response_type) == response_type
    )
    return JoinChatResult(
        status=safe_failure_class,
        failure_class=safe_failure_class,
        tdlib_error_code=_safe_error_code(tdlib_error_code),
        function_response_types_seen=response_types,
        response_extra_matched=response_extra_matched,
        response_without_extra_count=max(response_without_extra_count, 0),
        response_wrong_extra_count=max(response_wrong_extra_count, 0),
    )


def _join_result_from_tdlib_payload(
    payload: Mapping[str, Any],
    *,
    response_extra_matched: bool = False,
    response_without_extra_count: int = 0,
    response_wrong_extra_count: int = 0,
    function_response_types_seen: Sequence[str] = (),
) -> JoinChatResult:
    response_types = tuple(
        response_type
        for response_type in function_response_types_seen
        if _safe_tdlib_object_type(response_type) == response_type
    )
    response_type = _safe_tdlib_object_type(payload.get("@type"))
    if response_type == "error":
        error_code = _safe_error_code(payload.get("code"))
        marker_text = f"{payload.get('code', '')} {payload.get('message', '')}".upper()
        if any(marker in marker_text for marker in ALREADY_JOINED_ERROR_MARKERS):
            return JoinChatResult(
                status="joined",
                function_response_types_seen=response_types,
                response_extra_matched=response_extra_matched,
                response_without_extra_count=response_without_extra_count,
                response_wrong_extra_count=response_wrong_extra_count,
            )
        if any(marker in marker_text for marker in JOIN_REQUEST_ERROR_MARKERS):
            return JoinChatResult(
                status="join_requested",
                failure_class="join_requested",
                tdlib_error_code=error_code,
                function_response_types_seen=response_types,
                response_extra_matched=response_extra_matched,
                response_without_extra_count=response_without_extra_count,
                response_wrong_extra_count=response_wrong_extra_count,
            )
        if any(marker in marker_text for marker in FORBIDDEN_ERROR_MARKERS):
            return JoinChatResult(
                status="forbidden",
                failure_class="forbidden",
                tdlib_error_code=error_code,
                function_response_types_seen=response_types,
                response_extra_matched=response_extra_matched,
                response_without_extra_count=response_without_extra_count,
                response_wrong_extra_count=response_wrong_extra_count,
            )
        if any(marker in marker_text for marker in NOT_FOUND_ERROR_MARKERS):
            return JoinChatResult(
                status="not_found",
                failure_class="not_found",
                tdlib_error_code=error_code,
                function_response_types_seen=response_types,
                response_extra_matched=response_extra_matched,
                response_without_extra_count=response_without_extra_count,
                response_wrong_extra_count=response_wrong_extra_count,
            )
        return _join_failure_result(
            "tdlib_error",
            tdlib_error_code=error_code,
            function_response_types_seen=response_types,
            response_extra_matched=response_extra_matched,
            response_without_extra_count=response_without_extra_count,
            response_wrong_extra_count=response_wrong_extra_count,
        )
    if response_type in JOIN_SUCCESS_RESPONSE_TYPES:
        return JoinChatResult(
            status="joined",
            function_response_types_seen=response_types,
            response_extra_matched=response_extra_matched,
            response_without_extra_count=response_without_extra_count,
            response_wrong_extra_count=response_wrong_extra_count,
        )
    return _join_failure_result(
        "response_shape_error",
        function_response_types_seen=response_types,
        response_extra_matched=response_extra_matched,
        response_without_extra_count=response_without_extra_count,
        response_wrong_extra_count=response_wrong_extra_count,
    )


def _target_row_from_db_row(row: Any) -> TargetRow | None:
    registry_id = _row_value(row, "registry_id", 0)
    chat_id = _safe_int(_row_value(row, "chat_id", 1))
    if chat_id is None:
        return None
    try:
        registry_id_text = str(registry_id)
        uuid.UUID(registry_id_text)
    except (TypeError, ValueError):
        registry_id_text = str(registry_id)
        if not registry_id_text.strip():
            return None
    return TargetRow(registry_id=registry_id_text, chat_id=chat_id)


def _count_target_rows(connection: DatabaseConnection) -> int:
    value = _scalar(_execute_read(connection, COUNT_TARGET_ROWS_QUERY))
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _load_target_rows(
    connection: DatabaseConnection,
    *,
    limit: int | None,
) -> tuple[TargetRow, ...]:
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


def _normalize_exact_public_username(value: Any) -> str | None:
    from scripts.ops import (  # noqa: PLC0415
        dedicated_vps_telegram_channel_registry_public_username_resolve_operator
        as resolve_operator,
    )

    normalized = resolve_operator._normalize_public_username(value)
    if normalized is None:
        return None
    return normalized.lower()


def _exact_target_row_from_db_row(
    row: Any,
    *,
    locator_source_value: str,
) -> tuple[ExactTargetRow | None, str | None]:
    registry_id = _row_value(row, "registry_id", 0)
    source_value = _row_value(row, "source_value", 1)
    source_kind = _row_value(row, "source_kind", 2)
    desired_state = _row_value(row, "desired_state", 3)
    access_state = _row_value(row, "access_state", 4)
    raw_chat_id = _row_value(row, "chat_id", 5)

    normalized_source_value = _normalize_exact_public_username(source_value)
    if normalized_source_value is None:
        return None, "source_invalid"
    if normalized_source_value != locator_source_value:
        return None, "source_mismatch"
    if not isinstance(source_value, str):
        return None, "source_invalid"

    try:
        registry_id_text = str(uuid.UUID(str(registry_id)))
    except (AttributeError, TypeError, ValueError):
        return None, "registry_id_invalid"

    if source_kind != "public_username":
        return None, "source_kind_invalid"
    if type(raw_chat_id) is not int or raw_chat_id == 0:
        return None, "chat_id_invalid"
    if desired_state != "active":
        return None, "desired_state_invalid"
    if not isinstance(access_state, str):
        return None, "access_state_invalid"

    return (
        ExactTargetRow(
            registry_id=registry_id_text,
            source_value=source_value,
            normalized_source_value=normalized_source_value,
            source_kind=source_kind,
            desired_state=desired_state,
            access_state=access_state,
            chat_id=raw_chat_id,
        ),
        None,
    )


def _load_exact_target_rows(
    connection: DatabaseConnection,
    *,
    normalized_source_value: str,
) -> tuple[Any, ...]:
    return tuple(
        _rows(
            _execute_read(
                connection,
                SELECT_EXACT_TARGET_ROWS_QUERY,
                {"normalized_source_value": normalized_source_value},
            )
        )
    )


def _update_exact_join_state_row(
    connection: DatabaseConnection,
    *,
    row: ExactTargetRow,
    attempted_at: datetime,
) -> int:
    result = _execute_update(
        connection,
        UPDATE_EXACT_JOIN_STATE_REGISTRY_ROW_QUERY,
        {
            "registry_id": row.registry_id,
            "source_value": row.source_value,
            "normalized_source_value": row.normalized_source_value,
            "chat_id": row.chat_id,
            "attempted_at": attempted_at,
        },
    )
    return _rowcount(result)


def _exact_join_readback_matches(
    connection: DatabaseConnection,
    *,
    expected: ExactTargetRow,
) -> bool:
    raw_rows = _rows(
        _execute_read(
            connection,
            SELECT_EXACT_JOIN_READBACK_QUERY,
            {
                "registry_id": expected.registry_id,
                "source_value": expected.source_value,
                "normalized_source_value": expected.normalized_source_value,
                "chat_id": expected.chat_id,
            },
        )
    )
    if len(raw_rows) != 1:
        return False
    readback, error = _exact_target_row_from_db_row(
        raw_rows[0],
        locator_source_value=expected.normalized_source_value,
    )
    return bool(
        error is None
        and readback is not None
        and readback.registry_id == expected.registry_id
        and readback.source_value == expected.source_value
        and readback.normalized_source_value == expected.normalized_source_value
        and readback.source_kind == expected.source_kind == "public_username"
        and readback.desired_state == "active"
        and readback.access_state == "joined"
        and readback.chat_id == expected.chat_id
    )


def _update_join_state_row(
    connection: DatabaseConnection,
    *,
    row: TargetRow,
    access_state: str,
    attempted_at: datetime,
) -> bool:
    if access_state not in set(JOIN_MUTATION_ACCESS_STATE_BY_STATUS.values()):
        return False
    result = _execute_update(
        connection,
        UPDATE_JOIN_STATE_REGISTRY_ROW_QUERY,
        {
            "registry_id": row.registry_id,
            "chat_id": row.chat_id,
            "access_state": access_state,
            "attempted_at": attempted_at,
        },
    )
    return _rowcount(result) > 0


def _merge_joiner_side_effects(
    report: dict[str, Any],
    joiner: ResolvedNotJoinedJoiner | None,
) -> None:
    if joiner is None:
        return
    for flag_name in ("tdlib_send_called", "tdlib_receive_called"):
        if getattr(joiner, flag_name, False) is True:
            report["side_effects"][flag_name] = True


async def _wait_for_matching_join_response(
    *,
    extra: str,
    receive_payload: Callable[[float], Any],
    wait_config: TDLibJoinRpcWaitConfig,
    monotonic_clock: Callable[[], float] | None = None,
) -> JoinChatResult:
    clock = monotonic_clock or time.monotonic
    wait_result = _JoinRpcWaitResult()
    started_at = clock()

    for _ in range(wait_config.max_updates):
        elapsed_sec = clock() - started_at
        remaining_duration_sec = wait_config.max_duration_sec - elapsed_sec
        if remaining_duration_sec <= 0:
            wait_result.duration_exhausted = True
            break
        wait_result.receive_attempt_count += 1
        receive_timeout_sec = min(
            wait_config.receive_timeout_sec,
            max(remaining_duration_sec, 0.0),
        )
        try:
            payload = await receive_payload(receive_timeout_sec)
        except Exception:
            wait_result.result_class = "transport_error"
            wait_result.timed_out = False
            break
        if payload is None:
            wait_result.empty_receive_count += 1
            continue
        if not isinstance(payload, Mapping):
            continue

        wait_result.observation_count += 1
        payload_type = _safe_tdlib_object_type(payload.get("@type"))
        if payload_type is not None:
            _append_unique(wait_result.inbound_object_types_seen, payload_type)
            if payload_type.startswith("update"):
                _append_unique(wait_result.update_types_seen, payload_type)
        if payload_type == "error":
            _append_unique_safe_error_code(
                wait_result.tdlib_error_codes_seen,
                payload.get("code"),
            )

        state_type = _authorization_state_type_from_payload(payload)
        if state_type is not None:
            safe_state_type = _safe_tdlib_object_type(state_type)
            if safe_state_type == state_type:
                _append_unique(wait_result.authorization_states_seen, state_type)
                wait_result.final_authorization_state = state_type
            if state_type != TDLIB_READY_STATE:
                wait_result.result_class = "authorization_lost"
                wait_result.timed_out = False
                wait_result.result = _join_failure_result(
                    "authorization_lost",
                    function_response_types_seen=wait_result.function_response_types_seen,
                    response_without_extra_count=(
                        wait_result.response_without_extra_count
                    ),
                    response_wrong_extra_count=wait_result.response_wrong_extra_count,
                )
                break

        response_type = _function_response_type_from_payload(payload)
        if response_type is not None:
            _append_unique(wait_result.function_response_types_seen, response_type)

        raw_extra = payload.get("@extra")
        if raw_extra == extra:
            wait_result.response_extra_matched = True
            wait_result.timed_out = False
            if response_type is None:
                wait_result.result_class = "response_shape_error"
                wait_result.result = _join_failure_result(
                    "response_shape_error",
                    response_extra_matched=True,
                    function_response_types_seen=wait_result.function_response_types_seen,
                    response_without_extra_count=(
                        wait_result.response_without_extra_count
                    ),
                    response_wrong_extra_count=wait_result.response_wrong_extra_count,
                )
            else:
                result = _join_result_from_tdlib_payload(
                    payload,
                    response_extra_matched=True,
                    function_response_types_seen=wait_result.function_response_types_seen,
                    response_without_extra_count=(
                        wait_result.response_without_extra_count
                    ),
                    response_wrong_extra_count=wait_result.response_wrong_extra_count,
                )
                wait_result.result = result
                wait_result.result_class = result.status
                if result.tdlib_error_code is not None:
                    _append_unique_safe_error_code(
                        wait_result.tdlib_error_codes_seen,
                        result.tdlib_error_code,
                    )
            break

        if response_type is None:
            continue
        if isinstance(raw_extra, str):
            wait_result.response_wrong_extra_count += 1
        else:
            wait_result.response_without_extra_count += 1
    else:
        wait_result.update_budget_exhausted = True

    if wait_result.result is None:
        wait_result.result = _join_failure_result(
            wait_result.result_class,
            function_response_types_seen=wait_result.function_response_types_seen,
            response_extra_matched=wait_result.response_extra_matched,
            response_without_extra_count=wait_result.response_without_extra_count,
            response_wrong_extra_count=wait_result.response_wrong_extra_count,
        )
    return wait_result.result


class TDLibResolvedNotJoinedJoiner:
    def __init__(
        self,
        runtime_env: Mapping[str, str],
        *,
        transport: Any | None = None,
        auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
        receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
        overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
        join_rpc_max_updates: int = DEFAULT_TDLIB_JOIN_RPC_MAX_UPDATES,
        join_rpc_receive_timeout_sec: float = (
            DEFAULT_TDLIB_JOIN_RPC_RECEIVE_TIMEOUT_SEC
        ),
        join_rpc_max_duration_sec: float = DEFAULT_TDLIB_JOIN_RPC_MAX_DURATION_SEC,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        from scripts.ops import (  # noqa: PLC0415
            dedicated_vps_telegram_channel_registry_public_username_resolve_operator
            as resolve_operator,
        )

        self._resolve_operator = resolve_operator
        self._base = resolve_operator.TDLibPublicUsernameResolver(
            runtime_env,
            transport=transport,
            auth_max_updates=auth_max_updates,
            receive_timeout_sec=receive_timeout_sec,
            overall_timeout_sec=overall_timeout_sec,
        )
        self._join_rpc_wait_config = TDLibJoinRpcWaitConfig(
            max_updates=join_rpc_max_updates,
            receive_timeout_sec=join_rpc_receive_timeout_sec,
            max_duration_sec=join_rpc_max_duration_sec,
        )
        self._monotonic_clock = monotonic_clock or time.monotonic

    @property
    def tdlib_send_called(self) -> bool:
        return bool(getattr(self._base, "tdlib_send_called", False))

    @property
    def tdlib_receive_called(self) -> bool:
        return bool(getattr(self._base, "tdlib_receive_called", False))

    async def initialize(self) -> None:
        try:
            await self._base.initialize()
        except self._resolve_operator.TDLibNotReady as exc:
            raise TDLibNotReady("TDLib ready session helper did not report ready") from exc
        except self._resolve_operator.TDLibTransportUnavailable as exc:
            raise TDLibTransportUnavailable("TDLib transport unavailable") from exc

    async def join_chat(self, chat_id: int) -> JoinChatResult:
        request = self._base._client.build_join_chat_request(chat_id).payload
        extra = self._base._next_extra("resolved_not_joined_join")
        request = {**request, "@extra": extra}
        try:
            await self._base._send(request)
        except Exception:
            return _join_failure_result("transport_error")
        return await _wait_for_matching_join_response(
            extra=extra,
            receive_payload=self._base._receive,
            wait_config=self._join_rpc_wait_config,
            monotonic_clock=self._monotonic_clock,
        )

    async def close(self) -> None:
        await self._base.close()


def _default_joiner_factory(
    runtime_env: Mapping[str, str],
    *,
    auth_max_updates: int,
    receive_timeout_sec: float,
    overall_timeout_sec: float,
    join_rpc_max_updates: int,
    join_rpc_receive_timeout_sec: float,
    join_rpc_max_duration_sec: float,
) -> ResolvedNotJoinedJoiner:
    return TDLibResolvedNotJoinedJoiner(
        runtime_env,
        auth_max_updates=auth_max_updates,
        receive_timeout_sec=receive_timeout_sec,
        overall_timeout_sec=overall_timeout_sec,
        join_rpc_max_updates=join_rpc_max_updates,
        join_rpc_receive_timeout_sec=join_rpc_receive_timeout_sec,
        join_rpc_max_duration_sec=join_rpc_max_duration_sec,
    )


async def _join_rows(
    *,
    rows: Sequence[TargetRow],
    joiner: ResolvedNotJoinedJoiner,
    report: dict[str, Any],
    connection: DatabaseConnection,
    approved_registry_join_mutation: bool,
) -> JoinReportCounters:
    counters = JoinReportCounters()

    for row in rows:
        report["tdlib_join_attempted"] = True
        report["side_effects"]["telegram_api_called"] = True
        report["side_effects"]["tdlib_send_called"] = True
        report["side_effects"]["tdlib_join_called"] = True
        try:
            joined = await joiner.join_chat(row.chat_id)
            report["side_effects"]["tdlib_receive_called"] = True
        except TDLibNotReady:
            joined = _join_failure_result("authorization_lost")
        except TDLibTransportUnavailable:
            joined = _join_failure_result("transport_error")
        except Exception:
            joined = _join_failure_result("unknown_error")

        if not isinstance(joined, JoinChatResult):
            joined = _join_failure_result("response_shape_error")
        joined = counters.record(joined)
        if joined.status == "authorization_lost":
            counters.skipped_count += counters.updated_count
            counters.updated_count = 0
            counters.skipped_count += 1
            break

        access_state = JOIN_MUTATION_ACCESS_STATE_BY_STATUS.get(joined.status)
        if access_state is None:
            counters.skipped_count += 1
            continue

        if not approved_registry_join_mutation:
            counters.skipped_count += 1
            continue

        if _update_join_state_row(
            connection,
            row=row,
            access_state=access_state,
            attempted_at=datetime.now(timezone.utc),
        ):
            counters.updated_count += 1
        else:
            counters.skipped_count += 1

    return counters


def _apply_count_buckets(
    report: dict[str, Any],
    *,
    counters: JoinReportCounters,
) -> None:
    report["join_attempt_count_bucket"] = _bucket_count(counters.attempt_count)
    report["join_success_count_bucket"] = _bucket_count(counters.joined_count)
    report["join_requested_count_bucket"] = _bucket_count(
        counters.join_requested_count
    )
    report["join_forbidden_count_bucket"] = _bucket_count(counters.forbidden_count)
    report["join_not_found_count_bucket"] = _bucket_count(counters.not_found_count)
    report["join_response_timeout_count_bucket"] = _bucket_count(
        counters.response_timeout_count
    )
    report["join_transport_error_count_bucket"] = _bucket_count(
        counters.transport_error_count
    )
    report["join_tdlib_error_count_bucket"] = _bucket_count(counters.tdlib_error_count)
    report["join_authorization_lost_count_bucket"] = _bucket_count(
        counters.authorization_lost_count
    )
    report["join_unknown_error_count_bucket"] = _bucket_count(
        counters.unknown_error_count
    )
    report["join_response_shape_error_count_bucket"] = _bucket_count(
        counters.response_shape_error_count
    )
    report["join_failure_classes_seen"] = list(counters.failure_classes_seen)
    report["join_function_response_types_seen"] = list(
        counters.function_response_types_seen
    )
    report["join_response_extra_matched_count_bucket"] = _bucket_count(
        counters.response_extra_matched_count
    )
    report["join_response_without_extra_count_bucket"] = _bucket_count(
        counters.response_without_extra_count
    )
    report["join_response_wrong_extra_count_bucket"] = _bucket_count(
        counters.response_wrong_extra_count
    )
    report["updated_row_count_bucket"] = _bucket_count(counters.updated_count)
    report["skipped_row_count_bucket"] = _bucket_count(counters.skipped_count)


def _final_success_status(
    *,
    approved_registry_join_mutation: bool,
    counters: JoinReportCounters,
) -> str:
    if counters.authorization_lost_seen:
        return "resolved_not_joined_join_authorization_lost"
    hard_failures = (
        counters.response_timeout_count
        + counters.transport_error_count
        + counters.tdlib_error_count
        + counters.response_shape_error_count
        + counters.unknown_error_count
    )
    if approved_registry_join_mutation:
        if counters.updated_count > 0 and hard_failures == 0 and counters.skipped_count == 0:
            return "resolved_not_joined_join_registry_updated"
        return "resolved_not_joined_join_partial"
    if hard_failures > 0:
        return "resolved_not_joined_join_partial"
    return "resolved_not_joined_join_completed_no_mutation"


def _generate_exact_target_report(
    *,
    target_locator_path: str | Path,
    runtime_env_path: str | Path,
    dry_run: bool,
    approved_tdlib_join_resolved_not_joined: bool,
    approved_registry_join_mutation: bool,
    limit: int | None,
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
    tdlib_join_rpc_max_updates: int,
    tdlib_join_rpc_receive_timeout_sec: float,
    tdlib_join_rpc_max_duration_sec: float,
    runtime_env_reader: RuntimeEnvReader | None,
    database_connection_factory: DatabaseConnectionFactory | None,
    resolved_not_joined_joiner_factory: ResolvedNotJoinedJoinerFactory | None,
) -> ScriptResult:
    effective_dry_run = bool(dry_run or not approved_tdlib_join_resolved_not_joined)
    report = _base_report(
        dry_run=effective_dry_run,
        approved_tdlib_join_resolved_not_joined=(
            approved_tdlib_join_resolved_not_joined
        ),
        approved_registry_join_mutation=approved_registry_join_mutation,
    )
    _initialize_exact_target_report(report)

    if limit is not None:
        _set_status(
            report,
            "blocked_exact_target_selection_ambiguous",
            "exact_target.limit_not_allowed",
        )
        return ScriptResult(exit_code=1, report=report)
    if (
        dry_run
        or not approved_tdlib_join_resolved_not_joined
        or not approved_registry_join_mutation
    ):
        _set_status(
            report,
            "blocked_exact_target_approval_required",
            "approval.exact_target_join_and_mutation_required",
        )
        return ScriptResult(exit_code=1, report=report)

    try:
        locator = bounded_history_ingest_runner._read_target_locator(
            target_locator_path
        )
    except Exception:
        _set_status(
            report,
            "blocked_exact_target_locator_invalid",
            "exact_target.locator_invalid",
        )
        return ScriptResult(exit_code=1, report=report)
    report["target_locator_read"] = True

    locator_source_value = locator.get("source_value")
    normalized_locator_source = _normalize_exact_public_username(locator_source_value)
    if (
        not isinstance(locator_source_value, str)
        or normalized_locator_source is None
        or normalized_locator_source != locator_source_value
    ):
        _set_status(
            report,
            "blocked_exact_target_locator_invalid",
            "exact_target.locator_source_invalid",
        )
        return ScriptResult(exit_code=1, report=report)

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
    read_transaction: Any | None = None
    read_rollback_attempted = False
    mutation_transaction: Any | None = None
    mutation_transaction_committed = False
    joiner: ResolvedNotJoinedJoiner | None = None

    def _run_exact_target_operation() -> ScriptResult:
        nonlocal connection
        nonlocal cleanup
        nonlocal read_transaction
        nonlocal read_rollback_attempted
        nonlocal mutation_transaction
        nonlocal mutation_transaction_committed
        nonlocal joiner

        try:
            try:
                connection, cleanup = _open_database_connection(
                    database_url,
                    database_connection_factory,
                )
                read_transaction = connection.begin()
                _execute_read(connection, SET_TRANSACTION_READ_ONLY_QUERY)
                _execute_read(connection, SELECT_ONE_QUERY)
                report["database_connected"] = True
                table_available = bool(
                    _scalar(
                        _execute_read(
                            connection,
                            TABLE_AVAILABLE_QUERY,
                            {
                                "qualified_table_name": (
                                    "public.telegram_channel_registry"
                                )
                            },
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
                raw_rows = _load_exact_target_rows(
                    connection,
                    normalized_source_value=normalized_locator_source,
                )
            except Exception:
                _set_status(
                    report,
                    "blocked_database_unavailable",
                    "database.connection",
                )
                return ScriptResult(exit_code=1, report=report)
            finally:
                if read_transaction is not None:
                    read_rollback_attempted = True
                    _rollback_exact_transaction(
                        read_transaction,
                        report=report,
                        result_field="exact_target_read_rollback_succeeded",
                        failure_code="read_rollback_failed",
                    )

            if report["exact_target_read_rollback_succeeded"] is False:
                _set_status(
                    report,
                    "blocked_exact_target_read_rollback_failed",
                    "exact_target.read_rollback_failed",
                )
                return ScriptResult(exit_code=1, report=report)

            report["target_rows_checked"] = True
            report["target_row_count_bucket"] = _bucket_count(len(raw_rows))
            report["exact_target_match_count_bucket"] = _bucket_count(len(raw_rows))
            if not raw_rows:
                _set_status(
                    report,
                    "blocked_exact_target_missing",
                    "exact_target.missing",
                )
                return ScriptResult(exit_code=1, report=report)
            if len(raw_rows) != 1:
                _set_status(
                    report,
                    "blocked_exact_target_ambiguous",
                    "exact_target.multiple",
                )
                return ScriptResult(exit_code=1, report=report)

            row, row_error = _exact_target_row_from_db_row(
                raw_rows[0],
                locator_source_value=normalized_locator_source,
            )
            if row_error == "source_mismatch":
                _set_status(
                    report,
                    "blocked_exact_target_source_mismatch",
                    "exact_target.source_mismatch",
                )
                return ScriptResult(exit_code=1, report=report)
            if row is None:
                _set_status(
                    report,
                    "blocked_exact_target_row_invalid",
                    "exact_target.row_invalid",
                )
                return ScriptResult(exit_code=1, report=report)
            if row.access_state == "joined":
                report["exact_target_noop"] = True
                _set_status(report, "exact_target_already_joined_noop")
                report["operator_next_action"] = (
                    "The exact active registry target is already joined; no TDLib "
                    "RPC or registry mutation was performed."
                )
                return ScriptResult(exit_code=0, report=report)
            if row.access_state != "resolved_not_joined":
                _set_status(
                    report,
                    "blocked_exact_target_state_invalid",
                    "exact_target.access_state_invalid",
                )
                return ScriptResult(exit_code=1, report=report)

            try:
                if resolved_not_joined_joiner_factory is None:
                    joiner = _default_joiner_factory(
                        values,
                        auth_max_updates=tdlib_auth_max_updates,
                        receive_timeout_sec=tdlib_receive_timeout_sec,
                        overall_timeout_sec=tdlib_overall_timeout_sec,
                        join_rpc_max_updates=tdlib_join_rpc_max_updates,
                        join_rpc_receive_timeout_sec=(
                            tdlib_join_rpc_receive_timeout_sec
                        ),
                        join_rpc_max_duration_sec=tdlib_join_rpc_max_duration_sec,
                    )
                else:
                    joiner = resolved_not_joined_joiner_factory(values)
                asyncio.run(joiner.initialize())
                report["side_effects"]["tdlib_initialized"] = True
                _merge_joiner_side_effects(report, joiner)
            except TDLibNotReady:
                _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
                report["side_effects"]["tdlib_initialized"] = True
                _merge_joiner_side_effects(report, joiner)
                return ScriptResult(exit_code=1, report=report)
            except Exception:
                _set_status(
                    report,
                    "blocked_tdlib_transport_unavailable",
                    "tdlib.transport_unavailable",
                )
                _merge_joiner_side_effects(report, joiner)
                return ScriptResult(exit_code=1, report=report)

            report["tdlib_join_attempted"] = True
            report["side_effects"]["telegram_api_called"] = True
            report["side_effects"]["tdlib_send_called"] = True
            report["side_effects"]["tdlib_join_called"] = True
            try:
                joined = asyncio.run(joiner.join_chat(row.chat_id))
                report["side_effects"]["tdlib_receive_called"] = True
            except TDLibNotReady:
                joined = _join_failure_result("authorization_lost")
            except TDLibTransportUnavailable:
                joined = _join_failure_result("transport_error")
            except Exception:
                joined = _join_failure_result("unknown_error")
            _merge_joiner_side_effects(report, joiner)

            if not isinstance(joined, JoinChatResult):
                joined = _join_failure_result("response_shape_error")
            counters = JoinReportCounters()
            joined = counters.record(joined)
            if joined.status != "joined":
                counters.skipped_count = 1
                _apply_count_buckets(report, counters=counters)
                _set_status(
                    report,
                    "exact_target_join_completed_no_mutation",
                    f"join.{joined.status}",
                )
                report["operator_next_action"] = (
                    "The exact join result was not canonical joined; no registry "
                    "mutation was attempted."
                )
                return ScriptResult(exit_code=1, report=report)

            mutation_transaction = connection.begin()
            attempted_at = datetime.now(timezone.utc)
            if (
                _update_exact_join_state_row(
                    connection,
                    row=row,
                    attempted_at=attempted_at,
                )
                != 1
            ):
                counters.skipped_count = 1
                _apply_count_buckets(report, counters=counters)
                _set_status(
                    report,
                    "blocked_exact_target_concurrent_mismatch",
                    "exact_target.guarded_update_count_mismatch",
                )
                return ScriptResult(exit_code=1, report=report)
            if not _exact_join_readback_matches(connection, expected=row):
                counters.skipped_count = 1
                _apply_count_buckets(report, counters=counters)
                _set_status(
                    report,
                    "blocked_exact_target_readback_mismatch",
                    "exact_target.durable_readback_mismatch",
                )
                return ScriptResult(exit_code=1, report=report)

            _commit_transaction(mutation_transaction)
            mutation_transaction_committed = True
            report["exact_target_mutation_outcome"] = "committed_durable"
            counters.updated_count = 1
            _apply_count_buckets(report, counters=counters)
            report["exact_target_durable_readback_matched"] = True
            report["registry_join_mutation_performed"] = True
            report["side_effects"]["database_mutation_performed"] = True
            report["side_effects"]["telegram_channel_registry_updated"] = True
            _set_status(report, "exact_target_join_registry_updated")
            report["operator_next_action"] = (
                "The exact source-bound registry target was durably read back as active "
                "and joined after one guarded update."
            )
            return ScriptResult(exit_code=0, report=report)
        except Exception:
            _set_status(report, "blocked_unexpected_error", "unexpected_error")
            return ScriptResult(exit_code=1, report=report)

    result = _run_exact_target_operation()
    return _finalize_exact_target_cleanup(
        result,
        report=report,
        read_transaction=read_transaction,
        read_rollback_attempted=read_rollback_attempted,
        mutation_transaction=mutation_transaction,
        mutation_transaction_committed=mutation_transaction_committed,
        joiner=joiner,
        cleanup=cleanup,
        connection=connection,
    )


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    dry_run: bool = True,
    approved_tdlib_join_resolved_not_joined: bool = False,
    approved_registry_join_mutation: bool = False,
    target_locator_path: str | Path | None = None,
    limit: int | None = None,
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
    tdlib_join_rpc_max_updates: int = DEFAULT_TDLIB_JOIN_RPC_MAX_UPDATES,
    tdlib_join_rpc_receive_timeout_sec: float = (
        DEFAULT_TDLIB_JOIN_RPC_RECEIVE_TIMEOUT_SEC
    ),
    tdlib_join_rpc_max_duration_sec: float = DEFAULT_TDLIB_JOIN_RPC_MAX_DURATION_SEC,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    resolved_not_joined_joiner_factory: (
        ResolvedNotJoinedJoinerFactory | None
    ) = None,
) -> ScriptResult:
    if target_locator_path is not None:
        return _generate_exact_target_report(
            target_locator_path=target_locator_path,
            runtime_env_path=runtime_env_path,
            dry_run=dry_run,
            approved_tdlib_join_resolved_not_joined=(
                approved_tdlib_join_resolved_not_joined
            ),
            approved_registry_join_mutation=approved_registry_join_mutation,
            limit=limit,
            tdlib_auth_max_updates=tdlib_auth_max_updates,
            tdlib_receive_timeout_sec=tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec=tdlib_overall_timeout_sec,
            tdlib_join_rpc_max_updates=tdlib_join_rpc_max_updates,
            tdlib_join_rpc_receive_timeout_sec=(
                tdlib_join_rpc_receive_timeout_sec
            ),
            tdlib_join_rpc_max_duration_sec=tdlib_join_rpc_max_duration_sec,
            runtime_env_reader=runtime_env_reader,
            database_connection_factory=database_connection_factory,
            resolved_not_joined_joiner_factory=resolved_not_joined_joiner_factory,
        )
    effective_dry_run = bool(dry_run or not approved_tdlib_join_resolved_not_joined)
    report = _base_report(
        dry_run=effective_dry_run,
        approved_tdlib_join_resolved_not_joined=(
            approved_tdlib_join_resolved_not_joined
        ),
        approved_registry_join_mutation=approved_registry_join_mutation,
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
    joiner: ResolvedNotJoinedJoiner | None = None
    mutation_mode_requested = bool(
        approved_tdlib_join_resolved_not_joined
        and approved_registry_join_mutation
        and not effective_dry_run
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
        if target_count == 0:
            _set_status(
                report,
                "blocked_no_resolved_not_joined_rows",
                "registry.no_resolved_not_joined_rows",
            )
            report["operator_next_action"] = (
                "No active public_username registry rows with access_state "
                "resolved_not_joined and a non-null chat_id are available for "
                "this join operator."
            )
            return ScriptResult(exit_code=1, report=report)

        if (
            approved_registry_join_mutation
            and not approved_tdlib_join_resolved_not_joined
        ):
            _set_status(
                report,
                "blocked_approval_required",
                "approval.tdlib_join_required",
            )
            report["operator_next_action"] = (
                "Registry join mutation requires both explicit TDLib join approval "
                "and explicit registry mutation approval."
            )
            return ScriptResult(exit_code=1, report=report)

        if effective_dry_run:
            _set_status(report, "dry_run_resolved_not_joined_join_plan_ready")
            report["operator_next_action"] = (
                "Review the resolved_not_joined target bucket. Re-run on the VPS "
                "with --approved-tdlib-join-resolved-not-joined to send joinChat "
                "without mutation, and add --approved-registry-join-mutation only "
                "after operator approval."
            )
            return ScriptResult(exit_code=0, report=report)

        rows = _load_target_rows(connection, limit=limit)
        if not rows:
            _set_status(
                report,
                "blocked_no_resolved_not_joined_rows",
                "registry.no_valid_resolved_not_joined_rows_selected",
            )
            return ScriptResult(exit_code=1, report=report)

        try:
            if resolved_not_joined_joiner_factory is None:
                joiner = _default_joiner_factory(
                    values,
                    auth_max_updates=tdlib_auth_max_updates,
                    receive_timeout_sec=tdlib_receive_timeout_sec,
                    overall_timeout_sec=tdlib_overall_timeout_sec,
                    join_rpc_max_updates=tdlib_join_rpc_max_updates,
                    join_rpc_receive_timeout_sec=(
                        tdlib_join_rpc_receive_timeout_sec
                    ),
                    join_rpc_max_duration_sec=tdlib_join_rpc_max_duration_sec,
                )
            else:
                joiner = resolved_not_joined_joiner_factory(values)
            asyncio.run(joiner.initialize())
            report["side_effects"]["tdlib_initialized"] = True
            _merge_joiner_side_effects(report, joiner)
        except TDLibNotReady:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
            report["side_effects"]["tdlib_initialized"] = True
            _merge_joiner_side_effects(report, joiner)
            report["operator_next_action"] = (
                "TDLib readiness did not reach authorizationStateReady. Restore a "
                "ready existing session before any resolved_not_joined join run."
            )
            return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.transport_unavailable",
            )
            _merge_joiner_side_effects(report, joiner)
            return ScriptResult(exit_code=1, report=report)

        try:
            join_counters = asyncio.run(
                _join_rows(
                    rows=rows,
                    joiner=joiner,
                    report=report,
                    connection=connection,
                    approved_registry_join_mutation=(
                        approved_registry_join_mutation
                    ),
                )
            )
        except TDLibNotReady:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
            _merge_joiner_side_effects(report, joiner)
            return ScriptResult(exit_code=1, report=report)
        _merge_joiner_side_effects(report, joiner)

        _apply_count_buckets(report, counters=join_counters)
        if join_counters.authorization_lost_seen:
            report["registry_join_mutation_performed"] = False
            report["side_effects"]["database_mutation_performed"] = False
            report["side_effects"]["telegram_channel_registry_updated"] = False
        else:
            report["registry_join_mutation_performed"] = (
                join_counters.updated_count > 0
            )
            report["side_effects"]["database_mutation_performed"] = (
                join_counters.updated_count > 0
            )
            report["side_effects"]["telegram_channel_registry_updated"] = (
                join_counters.updated_count > 0
            )

        status = _final_success_status(
            approved_registry_join_mutation=approved_registry_join_mutation,
            counters=join_counters,
        )
        _set_status(report, status)
        if join_counters.authorization_lost_seen:
            report["operator_next_action"] = (
                "TDLib authorization was lost during resolved_not_joined join. "
                "No registry mutation was committed; restore a ready TDLib session "
                "before any separately approved registry mutation run."
            )
        elif approved_registry_join_mutation:
            report["operator_next_action"] = (
                "Join classifications were applied only where the guarded "
                "public_username/resolved_not_joined/chat_id-present UPDATE matched. "
                "Not-found join results were classified but left unmutated in this "
                "bounded slice."
            )
        else:
            report["operator_next_action"] = (
                "TDLib joinChat completed without DB mutation. Review coarse buckets "
                "before separately approving registry join mutation."
            )

        if join_counters.updated_count > 0 and not join_counters.authorization_lost_seen:
            _commit_transaction(transaction)
            transaction_committed = True
        return ScriptResult(exit_code=0, report=report)
    except Exception:
        _set_status(report, "blocked_unexpected_error", "unexpected_error")
        return ScriptResult(exit_code=1, report=report)
    finally:
        if joiner is not None:
            try:
                asyncio.run(joiner.close())
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
    exact_target_mode = args.target_locator_path is not None
    try:
        result = generate_report(
            runtime_env_path=args.runtime_env_path,
            dry_run=args.dry_run,
            approved_tdlib_join_resolved_not_joined=(
                args.approved_tdlib_join_resolved_not_joined
            ),
            approved_registry_join_mutation=args.approved_registry_join_mutation,
            target_locator_path=args.target_locator_path,
            limit=args.limit,
            tdlib_auth_max_updates=args.tdlib_auth_max_updates,
            tdlib_receive_timeout_sec=args.tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec=args.tdlib_overall_timeout_sec,
            tdlib_join_rpc_max_updates=args.tdlib_join_rpc_max_updates,
            tdlib_join_rpc_receive_timeout_sec=(
                args.tdlib_join_rpc_receive_timeout_sec
            ),
            tdlib_join_rpc_max_duration_sec=args.tdlib_join_rpc_max_duration_sec,
        )
    except Exception:
        if not exact_target_mode:
            raise
        emergency_report = _base_report(
            dry_run=bool(
                args.dry_run
                or not args.approved_tdlib_join_resolved_not_joined
            ),
            approved_tdlib_join_resolved_not_joined=(
                args.approved_tdlib_join_resolved_not_joined
            ),
            approved_registry_join_mutation=args.approved_registry_join_mutation,
        )
        _initialize_exact_target_report(emergency_report)
        _set_status(
            emergency_report,
            "blocked_exact_target_unhandled_failure",
            "exact_target.unhandled_failure",
        )
        result = ScriptResult(exit_code=1, report=emergency_report)
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
