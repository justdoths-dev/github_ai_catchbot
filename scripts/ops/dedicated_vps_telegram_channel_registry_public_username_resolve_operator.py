from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_telegram_channel_registry_public_username_resolve_operator"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 80
DEFAULT_TDLIB_RPC_TIMEOUT_SEC = 15.0
DEFAULT_TDLIB_RPC_MAX_UPDATES = 120

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


class DatabaseConnection(Protocol):
    def begin(self) -> Any: ...

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


class PublicUsernameResolver(Protocol):
    async def initialize(self) -> None: ...

    async def resolve_public_username(self, username: str) -> "PublicUsernameResolveResult": ...

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
    parser.add_argument("--limit", type=_positive_int, default=None)
    return parser


def _positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("limit must be a positive integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("limit must be a positive integer")
    return value


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
    approved_tdlib_public_username_resolve: bool,
    approved_registry_resolve_mutation: bool,
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


class TDLibPublicUsernameResolver:
    def __init__(
        self,
        runtime_env: Mapping[str, str],
        *,
        transport: Any | None = None,
    ) -> None:
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
        self._request_sequence = 0

    async def initialize(self) -> None:
        try:
            self._config.ensure_runtime_dirs()
            await self._client.initialize()
            await self._wait_until_ready()
        except TDLibNotReady:
            raise
        except Exception as exc:
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
            await self._client.send(request)
            response = await self._receive_response(extra)
        except Exception as exc:
            raise TDLibTransportUnavailable("TDLib public username resolve failed") from exc
        return _resolve_result_from_tdlib_payload(response)

    async def _wait_until_ready(self) -> None:
        from src.services.collector_telegram.tdlib_client import tdlib_json_bytes

        for _ in range(DEFAULT_TDLIB_AUTH_MAX_UPDATES):
            payload = await self._client.receive(DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC)
            if not isinstance(payload, dict):
                continue
            if payload.get("@type") == "error":
                raise TDLibNotReady("TDLib returned an authorization error")
            if payload.get("@type") != "updateAuthorizationState":
                continue
            state = payload.get("authorization_state")
            if not isinstance(state, dict):
                continue
            state_type = state.get("@type")
            if state_type == "authorizationStateReady":
                return
            if state_type == "authorizationStateWaitTdlibParameters":
                await self._client.send(
                    {
                        **self._client.build_set_tdlib_parameters_request().payload,
                        "@extra": self._next_extra("tdlib_parameters"),
                    }
                )
                continue
            if state_type == "authorizationStateWaitEncryptionKey":
                await self._client.send(
                    {
                        "@type": "checkDatabaseEncryptionKey",
                        "encryption_key": tdlib_json_bytes(
                            self._config.tdlib_db_encryption_key
                        ),
                        "@extra": self._next_extra("encryption_key"),
                    }
                )
                continue
            if state_type in {
                "authorizationStateWaitPhoneNumber",
                "authorizationStateWaitCode",
                "authorizationStateWaitOtherDeviceConfirmation",
                "authorizationStateWaitPassword",
                "authorizationStateLoggingOut",
                "authorizationStateClosing",
                "authorizationStateClosed",
            }:
                raise TDLibNotReady("TDLib authorization is not ready")
        raise TDLibNotReady("TDLib authorization did not become ready")

    async def _receive_response(self, extra: str) -> dict[str, Any]:
        for _ in range(DEFAULT_TDLIB_RPC_MAX_UPDATES):
            payload = await self._client.receive(DEFAULT_TDLIB_RPC_TIMEOUT_SEC)
            if not isinstance(payload, dict):
                continue
            if payload.get("@extra") == extra:
                return payload
            if payload.get("@type") == "updateAuthorizationState":
                state = payload.get("authorization_state")
                state_type = state.get("@type") if isinstance(state, dict) else None
                if state_type != "authorizationStateReady":
                    raise TDLibNotReady("TDLib authorization left ready state")
        raise TDLibTransportUnavailable("TDLib public username resolve timed out")

    def _next_extra(self, label: str) -> str:
        self._request_sequence += 1
        return f"{SCRIPT_NAME}.{label}.{self._request_sequence}"


def _default_resolver_factory(runtime_env: Mapping[str, str]) -> PublicUsernameResolver:
    return TDLibPublicUsernameResolver(runtime_env)


def _resolve_result_from_tdlib_payload(payload: Mapping[str, Any]) -> PublicUsernameResolveResult:
    if payload.get("@type") == "error":
        message = str(payload.get("message", "")).upper()
        if any(marker in message for marker in NOT_FOUND_ERROR_MARKERS):
            return PublicUsernameResolveResult(status="not_found")
        if any(marker in message for marker in ACCESS_DENIED_ERROR_MARKERS):
            return PublicUsernameResolveResult(status="access_denied")
        return PublicUsernameResolveResult(status="failed")

    chat_id = _safe_int(payload.get("id"))
    if chat_id is None:
        return PublicUsernameResolveResult(status="failed")
    chat_type = _extract_chat_type_summary(payload)
    if chat_type not in ALLOWED_CHAT_TYPE_SUMMARIES:
        return PublicUsernameResolveResult(status="unsupported_chat_type")
    return PublicUsernameResolveResult(
        status="resolved",
        chat_id=chat_id,
        username_snapshot=_extract_username_snapshot(payload),
        title_snapshot=_extract_title_snapshot(payload),
        chat_type=chat_type,
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


async def _resolve_rows(
    *,
    rows: Sequence[TargetRow],
    resolver: PublicUsernameResolver,
    report: dict[str, Any],
    connection: DatabaseConnection,
    approved_registry_resolve_mutation: bool,
) -> tuple[int, int, int, int, int]:
    resolved_count = 0
    unresolved_count = 0
    failed_count = 0
    updated_count = 0
    skipped_count = 0

    for row in rows:
        report["tdlib_resolve_attempted"] = True
        report["side_effects"]["telegram_api_called"] = True
        report["side_effects"]["tdlib_send_called"] = True
        report["side_effects"]["tdlib_public_username_resolve_called"] = True
        try:
            resolved = await resolver.resolve_public_username(row.normalized_username)
            report["side_effects"]["tdlib_receive_called"] = True
        except TDLibNotReady:
            raise
        except Exception:
            failed_count += 1
            skipped_count += 1
            continue

        if resolved.status != "resolved" or resolved.chat_id is None:
            unresolved_count += 1
            skipped_count += 1
            continue

        resolved_count += 1
        if not approved_registry_resolve_mutation:
            skipped_count += 1
            continue

        if _update_resolved_row(
            connection,
            row=row,
            resolved=resolved,
            resolved_at=datetime.now(timezone.utc),
        ):
            updated_count += 1
        else:
            skipped_count += 1

    return resolved_count, unresolved_count, failed_count, updated_count, skipped_count


def _apply_count_buckets(
    report: dict[str, Any],
    *,
    resolved_count: int,
    unresolved_count: int,
    failed_count: int,
    updated_count: int,
    skipped_count: int,
) -> None:
    report["resolved_count_bucket"] = _bucket_count(resolved_count)
    report["unresolved_count_bucket"] = _bucket_count(unresolved_count)
    report["failed_resolve_count_bucket"] = _bucket_count(failed_count)
    report["updated_row_count_bucket"] = _bucket_count(updated_count)
    report["skipped_row_count_bucket"] = _bucket_count(skipped_count)


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


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    dry_run: bool = True,
    approved_tdlib_public_username_resolve: bool = False,
    approved_registry_resolve_mutation: bool = False,
    limit: int | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    public_username_resolver_factory: PublicUsernameResolverFactory | None = None,
) -> ScriptResult:
    effective_dry_run = bool(dry_run or not approved_tdlib_public_username_resolve)
    report = _base_report(
        dry_run=effective_dry_run,
        approved_tdlib_public_username_resolve=approved_tdlib_public_username_resolve,
        approved_registry_resolve_mutation=approved_registry_resolve_mutation,
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

        if effective_dry_run:
            _set_status(report, "dry_run_public_username_resolve_plan_ready")
            report["operator_next_action"] = (
                "Review the unresolved public_username bucket. Re-run on the VPS "
                "with --approved-tdlib-public-username-resolve to resolve without "
                "mutation, and add --approved-registry-resolve-mutation only after "
                "operator approval."
            )
            return ScriptResult(exit_code=0, report=report)

        rows = _load_target_rows(connection, limit=limit)
        if not rows:
            _set_status(
                report,
                "blocked_no_unresolved_public_username_rows",
                "registry.no_valid_public_username_rows_selected",
            )
            return ScriptResult(exit_code=1, report=report)

        try:
            resolver_factory = public_username_resolver_factory or _default_resolver_factory
            resolver = resolver_factory(values)
            awaitable = resolver.initialize()
            asyncio.run(awaitable)
            report["side_effects"]["tdlib_initialized"] = True
        except TDLibNotReady:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
            report["side_effects"]["tdlib_initialized"] = True
            return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.transport_unavailable",
            )
            return ScriptResult(exit_code=1, report=report)

        try:
            (
                resolved_count,
                unresolved_count,
                failed_count,
                updated_count,
                skipped_count,
            ) = asyncio.run(
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
            return ScriptResult(exit_code=1, report=report)

        _apply_count_buckets(
            report,
            resolved_count=resolved_count,
            unresolved_count=unresolved_count,
            failed_count=failed_count,
            updated_count=updated_count,
            skipped_count=skipped_count,
        )
        report["registry_resolve_mutation_performed"] = updated_count > 0
        report["side_effects"]["database_mutation_performed"] = updated_count > 0
        report["side_effects"]["telegram_channel_registry_updated"] = updated_count > 0

        status = _final_success_status(
            approved_registry_resolve_mutation=approved_registry_resolve_mutation,
            resolved_count=resolved_count,
            unresolved_count=unresolved_count,
            failed_count=failed_count,
            updated_count=updated_count,
            skipped_count=skipped_count,
        )
        _set_status(report, status)
        if approved_registry_resolve_mutation:
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

        if updated_count > 0:
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
        limit=args.limit,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
