from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_telegram_channel_registry_seed_plan_readiness"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

BUCKET_LABELS = (
    "zero",
    "one",
    "two_to_five",
    "six_to_ten",
    "eleven_to_twenty",
    "twenty_one_to_fifty",
    "more_than_fifty",
    "unknown",
)

DESIRED_STATE_BUCKET_KEYS = ("active", "paused", "removed", "unsupported")
ACCESS_STATE_BUCKET_KEYS = (
    "unresolved",
    "joined",
    "join_requested",
    "forbidden",
    "not_found",
    "left",
    "unsupported",
)
SOURCE_KIND_BUCKET_KEYS = ("public_username", "invite_link", "chat_id", "unsupported")

TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
REGISTRY_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM telegram_channel_registry"
ACTIVE_JOINED_CHANNEL_COUNT_QUERY = (
    "SELECT COUNT(*) FROM telegram_channel_registry "
    "WHERE desired_state = 'active' AND access_state = 'joined' AND chat_id IS NOT NULL"
)
CHAT_ID_PRESENT_COUNT_QUERY = (
    "SELECT COUNT(*) FROM telegram_channel_registry WHERE chat_id IS NOT NULL"
)
UNRESOLVED_OR_NOT_JOINED_COUNT_QUERY = (
    "SELECT COUNT(*) FROM telegram_channel_registry "
    "WHERE desired_state = 'active' AND (access_state <> 'joined' OR chat_id IS NULL)"
)
DESIRED_STATE_COUNT_QUERY = """
SELECT
  CASE
    WHEN desired_state IN ('active', 'paused', 'removed') THEN desired_state
    ELSE 'unsupported'
  END AS desired_state_bucket,
  COUNT(*)
FROM telegram_channel_registry
GROUP BY 1
"""
ACCESS_STATE_COUNT_QUERY = """
SELECT
  CASE
    WHEN access_state IN (
      'unresolved',
      'joined',
      'join_requested',
      'forbidden',
      'not_found',
      'left'
    ) THEN access_state
    ELSE 'unsupported'
  END AS access_state_bucket,
  COUNT(*)
FROM telegram_channel_registry
GROUP BY 1
"""
SOURCE_KIND_COUNT_QUERY = """
SELECT
  CASE
    WHEN source_kind IN ('public_username', 'invite_link', 'chat_id') THEN source_kind
    ELSE 'unsupported'
  END AS source_kind_bucket,
  COUNT(*)
FROM telegram_channel_registry
GROUP BY 1
"""

FORBIDDEN_SQL_VERBS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "DROP",
    "ALTER",
    "CREATE",
    "GRANT",
    "REVOKE",
    "VACUUM",
    "ANALYZE",
)

SIDE_EFFECT_FLAG_NAMES = (
    "database_mutation_performed",
    "redis_mutation_performed",
    "telegram_api_called",
    "tdlib_initialized",
    "tdlib_send_called",
    "tdlib_receive_called",
    "tdlib_auth_attempted",
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


class DatabaseConnection(Protocol):
    def begin(self) -> Any: ...

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseConnectionFactory = Callable[[str], DatabaseConnection]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


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
            "Inspect Telegram channel registry seed readiness with read-only "
            "PostgreSQL aggregate queries. This script does not mutate DB/Redis, "
            "call Telegram or TDLib, start collectors, or print channel identifiers."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    return parser


def _seed_plan_input_contract() -> list[dict[str, Any]]:
    return [
        {
            "source_kind": "public_username",
            "required_fields": ["source_kind", "source_value"],
            "source_value_description": "Telegram public username without exposing it in ChatGPT",
            "initial_desired_state": "active",
            "initial_access_state": "unresolved",
            "chat_id_required_initially": False,
            "chat_id_resolution": "resolved later by approved registry sync/onboarding slice",
        },
        {
            "source_kind": "invite_link",
            "required_fields": ["source_kind", "source_value"],
            "source_value_description": "Private invite link; keep local/VPS-only and do not paste to ChatGPT",
            "initial_desired_state": "active",
            "initial_access_state": "unresolved",
            "chat_id_required_initially": False,
            "chat_id_resolution": "resolved later by approved registry sync/onboarding slice",
        },
        {
            "source_kind": "chat_id",
            "required_fields": ["source_kind", "source_value", "chat_id"],
            "source_value_description": "Known Telegram chat_id supplied locally by operator",
            "initial_desired_state": "active",
            "initial_access_state": "joined_if_already_joined_and_verified",
            "chat_id_required_initially": True,
            "chat_id_resolution": "operator supplied; still requires later verification before live collector",
        },
    ]


def _empty_count_bucket_map(keys: Sequence[str]) -> dict[str, str]:
    return {key: "zero" for key in keys}


def _side_effects() -> dict[str, bool]:
    return {flag: False for flag in SIDE_EFFECT_FLAG_NAMES}


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_readiness_checked": False,
        "database_connected": False,
        "channel_registry_checked": False,
        "channel_registry_table_available": False,
        "active_joined_channels_present": False,
        "active_joined_channel_count_bucket": "unknown",
        "registry_row_count_bucket": "unknown",
        "desired_state_count_buckets": _empty_count_bucket_map(DESIRED_STATE_BUCKET_KEYS),
        "access_state_count_buckets": _empty_count_bucket_map(ACCESS_STATE_BUCKET_KEYS),
        "source_kind_count_buckets": _empty_count_bucket_map(SOURCE_KIND_BUCKET_KEYS),
        "unresolved_or_not_joined_count_bucket": "unknown",
        "chat_id_present_count_bucket": "unknown",
        "seed_plan_required": False,
        "seed_plan_input_contract": _seed_plan_input_contract(),
        "operator_next_action": (
            "Fix runtime env or database access locally on the VPS; do not paste "
            "runtime.env values, channel identifiers, invite links, phone numbers, "
            "or secrets into ChatGPT."
        ),
        "side_effects": _side_effects(),
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.strip().split())


def _allowed_sql_statements() -> set[str]:
    return {
        _normalize_sql(statement)
        for statement in (
            "SET TRANSACTION READ ONLY",
            "SHOW transaction_read_only",
            "SELECT 1",
            TABLE_AVAILABLE_QUERY,
            REGISTRY_ROW_COUNT_QUERY,
            ACTIVE_JOINED_CHANNEL_COUNT_QUERY,
            CHAT_ID_PRESENT_COUNT_QUERY,
            UNRESOLVED_OR_NOT_JOINED_COUNT_QUERY,
            DESIRED_STATE_COUNT_QUERY,
            ACCESS_STATE_COUNT_QUERY,
            SOURCE_KIND_COUNT_QUERY,
        )
    }


def _assert_read_only_sql(statement: str) -> None:
    normalized = _normalize_sql(statement)
    upper_statement = normalized.upper()
    for verb in FORBIDDEN_SQL_VERBS:
        if re.search(rf"\b{verb}\b", upper_statement):
            raise ValueError(f"forbidden SQL verb detected: {verb}")
    if normalized not in _allowed_sql_statements():
        raise ValueError("SQL statement is not in the seed-plan readiness allowlist")


def _execute_read_only(
    connection: DatabaseConnection,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    _assert_read_only_sql(statement)
    return connection.execute(statement, params or {})


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
    return first


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return result
    return list(result)


def _row_pair(row: Any) -> tuple[str, int] | None:
    if isinstance(row, (tuple, list)) and len(row) >= 2:
        return str(row[0]), int(row[1] or 0)
    if hasattr(row, "_mapping"):
        values = list(row._mapping.values())
        if len(values) >= 2:
            return str(values[0]), int(values[1] or 0)
    return None


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


def _count_bucket_map(rows: Sequence[Any], allowed_keys: Sequence[str]) -> dict[str, str]:
    counts = {key: 0 for key in allowed_keys}
    for row in rows:
        pair = _row_pair(row)
        if pair is None:
            continue
        key, count = pair
        safe_key = key if key in counts else "unsupported"
        counts[safe_key] = counts.get(safe_key, 0) + count
    return {key: _bucket_count(counts.get(key, 0)) for key in allowed_keys}


def _read_only_confirmed(value: Any) -> bool:
    return str(value).strip().lower() in {"on", "true", "1", "yes"}


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


def _open_default_database_connection(database_url: str) -> tuple[DatabaseConnection, Callable[[], None]]:
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


def _query_scalar_count(connection: DatabaseConnection, statement: str) -> int:
    value = _scalar(_execute_read_only(connection, statement))
    return int(value or 0)


def _apply_registry_counts(report: dict[str, Any], connection: DatabaseConnection) -> None:
    registry_row_count = _query_scalar_count(connection, REGISTRY_ROW_COUNT_QUERY)
    active_joined_count = _query_scalar_count(connection, ACTIVE_JOINED_CHANNEL_COUNT_QUERY)
    unresolved_or_not_joined_count = _query_scalar_count(
        connection,
        UNRESOLVED_OR_NOT_JOINED_COUNT_QUERY,
    )
    chat_id_present_count = _query_scalar_count(connection, CHAT_ID_PRESENT_COUNT_QUERY)

    report["registry_row_count_bucket"] = _bucket_count(registry_row_count)
    report["active_joined_channel_count_bucket"] = _bucket_count(active_joined_count)
    report["active_joined_channels_present"] = active_joined_count > 0
    report["unresolved_or_not_joined_count_bucket"] = _bucket_count(
        unresolved_or_not_joined_count
    )
    report["chat_id_present_count_bucket"] = _bucket_count(chat_id_present_count)
    report["desired_state_count_buckets"] = _count_bucket_map(
        _rows(_execute_read_only(connection, DESIRED_STATE_COUNT_QUERY)),
        DESIRED_STATE_BUCKET_KEYS,
    )
    report["access_state_count_buckets"] = _count_bucket_map(
        _rows(_execute_read_only(connection, ACCESS_STATE_COUNT_QUERY)),
        ACCESS_STATE_BUCKET_KEYS,
    )
    report["source_kind_count_buckets"] = _count_bucket_map(
        _rows(_execute_read_only(connection, SOURCE_KIND_COUNT_QUERY)),
        SOURCE_KIND_BUCKET_KEYS,
    )

    if active_joined_count > 0:
        report["seed_plan_required"] = False
        report["operator_next_action"] = (
            "Startup-eligible channel registry rows are present. No seed plan is "
            "required for this check; continue only through separately approved "
            "collector startup readiness gates."
        )
        _set_status(report, "channel_registry_seed_readiness_passed")
        return

    report["seed_plan_required"] = True
    report["operator_next_action"] = (
        "Prepare local/VPS-only channel registry seed inputs using one accepted "
        "source_kind shape. Do not paste private channel identifiers, invite links, "
        "chat IDs, phone numbers, runtime.env values, or Telegram secrets into "
        "ChatGPT. Apply any DB seed through a separately approved registry "
        "seed/onboarding slice."
    )
    _set_status(
        report,
        "seed_required_no_active_joined_channels",
        "channel_registry.no_active_joined_channels",
    )


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
) -> ScriptResult:
    report = _base_report()

    try:
        values = _read_runtime_env(runtime_env_path, runtime_env_reader)
    except Exception:
        _set_status(report, "blocked_runtime_env_unreadable", "runtime_env.unreadable")
        return ScriptResult(exit_code=1, report=report)
    report["runtime_env_read"] = True

    database_url = values.get("DATABASE_URL")
    report["database_readiness_checked"] = True
    if not database_url or not database_url.strip():
        _set_status(report, "blocked_database_unavailable", "database.url_missing")
        return ScriptResult(exit_code=1, report=report)
    if not _database_url_is_supported(database_url):
        _set_status(report, "blocked_database_unavailable", "database.url_unsupported")
        return ScriptResult(exit_code=1, report=report)

    connection: DatabaseConnection | None = None
    cleanup: Callable[[], None] | None = None
    transaction: Any | None = None
    try:
        try:
            connection, cleanup = _open_database_connection(
                database_url,
                database_connection_factory,
            )
            transaction = connection.begin()
            _execute_read_only(connection, "SET TRANSACTION READ ONLY")
            if not _read_only_confirmed(
                _scalar(_execute_read_only(connection, "SHOW transaction_read_only"))
            ):
                _set_status(
                    report,
                    "blocked_database_unavailable",
                    "database.read_only_not_confirmed",
                )
                return ScriptResult(exit_code=1, report=report)
            _execute_read_only(connection, "SELECT 1")
            report["database_connected"] = True
        except Exception:
            _set_status(report, "blocked_database_unavailable", "database.connection")
            return ScriptResult(exit_code=1, report=report)

        report["channel_registry_checked"] = True
        try:
            table_available = bool(
                _scalar(
                    _execute_read_only(
                        connection,
                        TABLE_AVAILABLE_QUERY,
                        {"qualified_table_name": "public.telegram_channel_registry"},
                    )
                )
            )
            report["channel_registry_table_available"] = table_available
            if not table_available:
                _set_status(
                    report,
                    "blocked_channel_registry_unavailable",
                    "channel_registry.table_unavailable",
                )
                return ScriptResult(exit_code=1, report=report)

            _apply_registry_counts(report, connection)
            return ScriptResult(
                exit_code=0 if report["contract_status"] == "channel_registry_seed_readiness_passed" else 1,
                report=report,
            )
        except Exception:
            _set_status(
                report,
                "blocked_channel_registry_unavailable",
                "channel_registry.read_failed",
            )
            return ScriptResult(exit_code=1, report=report)
    except Exception:
        _set_status(report, "blocked_unexpected_error", "unexpected_error")
        return ScriptResult(exit_code=1, report=report)
    finally:
        if transaction is not None and hasattr(transaction, "rollback"):
            transaction.rollback()
        if cleanup is not None:
            cleanup()
        elif connection is not None:
            connection.close()


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(runtime_env_path=args.runtime_env_path)
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
