from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence


REPORT_TYPE = "dedicated_vps_collector_runtime_startup_readiness_gate_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

ACTIVE_JOINED_CHANNEL_COUNT_QUERY = (
    "SELECT count(*) FROM telegram_channel_registry "
    "WHERE desired_state='active' AND access_state='joined' AND chat_id IS NOT NULL"
)

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

SIDE_EFFECT_FLAGS = (
    "runtime_env_values_printed",
    "secret_values_printed",
    "database_values_printed",
    "redis_values_printed",
    "singleton_lock_file_created",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "tdlib_auth_attempted",
    "tdlib_initialized",
    "tdlib_send_called",
    "tdlib_receive_called",
    "live_collector_started",
    "collector_runtime_started",
    "notifier_transport_enabled",
    "outbox_relay_started",
    "router_normalizer_started",
    "docker_or_systemd_changed",
    "files_mutated",
    "database_mutation_performed",
    "redis_mutation_performed",
    "alembic_upgrade_run",
    "alembic_downgrade_run",
    "alembic_stamp_run",
    "alembic_revision_run",
)


class DatabaseConnection(Protocol):
    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


class RedisClient(Protocol):
    def ping(self) -> Any: ...

    def close(self) -> None: ...


TdjsonAvailabilityChecker = Callable[[Path, Mapping[str, str]], None]
CollectorConfigBuilder = Callable[[Path, Mapping[str, str]], Any]
DatabaseConnectionFactory = Callable[[str], DatabaseConnection]
RedisClientFactory = Callable[[str], RedisClient]


@dataclass(frozen=True, slots=True)
class GateResult:
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
            "Check dedicated VPS collector runtime startup readiness without "
            "starting TDLib, Telegram receive loops, collector runtime, notifier, "
            "outbox relay, Docker, systemd, or DB/Redis mutations."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--repo-root", default=None)
    return parser


def default_repo_root() -> Path:
    return ROOT


def _base_report(runtime_env_path: str) -> dict[str, Any]:
    report: dict[str, Any] = {
        "report_type": REPORT_TYPE,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "failures": [],
        "check_order": (
            "runtime_env",
            "collector_config",
            "tdjson",
            "tdlib_session_reuse_preflight",
            "database_select_1",
            "alembic_current",
            "redis_ping",
            "channel_registry_active_joined_count",
            "singleton_lock_path_parent_metadata",
        ),
        "runtime_env_path": runtime_env_path,
        "runtime_env_read": False,
        "collector_config_built": False,
        "tdjson_available": False,
        "tdlib_session_reuse_preflight_passed": False,
        "database_readiness_checked": False,
        "database_connected": False,
        "alembic_current_checked": False,
        "alembic_current_available": False,
        "redis_readiness_checked": False,
        "redis_connected": False,
        "channel_registry_checked": False,
        "active_joined_channel_count_bucket": "unknown",
        "active_joined_channels_present": False,
        "singleton_lock_path_checked": False,
        "singleton_lock_path_parent_exists": False,
        "singleton_lock_path_parent_is_dir": False,
        "singleton_lock_path_parent_writable": False,
    }
    for flag in SIDE_EFFECT_FLAGS:
        report[flag] = False
    return report


def _failure(
    report: dict[str, Any],
    check: str,
    message: str,
    *,
    error_type: str | None = None,
) -> None:
    report["checks_failed"].append(check)
    failure: dict[str, Any] = {"check": check, "message": message}
    if error_type is not None:
        failure["error_type"] = error_type
    report["failures"].append(failure)


def _build_collector_config(repo_root: Path, values: Mapping[str, str]) -> Any:
    config_module = session_preflight._load_collector_module(repo_root, "config")
    return config_module.CollectorTelegramConfig.from_env(values)


def _assert_tdjson_available(repo_root: Path, values: Mapping[str, str]) -> None:
    session_preflight._assert_tdjson_available(repo_root, values)


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.strip().split())


def _assert_read_only_sql(statement: str) -> None:
    normalized = _normalize_sql(statement)
    upper_statement = normalized.upper()
    for verb in FORBIDDEN_SQL_VERBS:
        if re.search(rf"\b{verb}\b", upper_statement):
            raise ValueError(f"forbidden SQL verb detected: {verb}")

    allowed = {
        "SELECT 1",
        "SELECT version_num FROM alembic_version",
        ACTIVE_JOINED_CHANNEL_COUNT_QUERY,
    }
    if normalized not in allowed:
        raise ValueError("SQL statement is not in the collector readiness allowlist")


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
    return first[0] if isinstance(first, (tuple, list)) else first


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return result
    return list(result)


def _first_cell(row: Any) -> Any:
    if isinstance(row, (tuple, list)):
        return row[0] if row else None
    if hasattr(row, "_mapping"):
        return next(iter(row._mapping.values()))
    return row


def _bucket_count(count: int | None) -> str:
    if count is None:
        return "unknown"
    if count == 0:
        return "zero"
    if count <= 5:
        return "one_to_five"
    if count <= 20:
        return "six_to_twenty"
    return "over_twenty"


class SqlAlchemyConnection:
    def __init__(self, raw_connection: Any, text_factory: Callable[[str], Any]) -> None:
        self._raw_connection = raw_connection
        self._text_factory = text_factory

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


class RedisPingClient:
    def __init__(self, raw_client: Any) -> None:
        self._raw_client = raw_client

    def ping(self) -> Any:
        return self._raw_client.ping()

    def close(self) -> None:
        close = getattr(self._raw_client, "close", None)
        if close is not None:
            close()


def _open_default_database_connection(database_url: str) -> tuple[DatabaseConnection, Callable[[], None]]:
    factory = SqlAlchemyConnectionFactory()
    connection = factory(database_url)

    def cleanup() -> None:
        connection.close()
        factory.dispose()

    return connection, cleanup


def _open_default_redis_client(redis_url: str) -> RedisClient:
    redis = __import__("redis")
    return RedisPingClient(redis.from_url(redis_url))


def _apply_collector_config_check(
    report: dict[str, Any],
    repo_root: Path,
    values: Mapping[str, str],
    collector_config_builder: CollectorConfigBuilder | None,
) -> Any | None:
    try:
        config = (
            _build_collector_config(repo_root, values)
            if collector_config_builder is None
            else collector_config_builder(repo_root, values)
        )
    except Exception as exc:
        _failure(
            report,
            "collector_config.invalid",
            "CollectorTelegramConfig could not be built from runtime env keys.",
            error_type=type(exc).__name__,
        )
        report["contract_status"] = "blocked_collector_config_invalid"
        return None

    report["collector_config_built"] = True
    return config


def _apply_tdjson_check(
    report: dict[str, Any],
    repo_root: Path,
    values: Mapping[str, str],
    tdjson_availability_checker: TdjsonAvailabilityChecker | None,
) -> bool:
    try:
        if tdjson_availability_checker is None:
            _assert_tdjson_available(repo_root, values)
        else:
            tdjson_availability_checker(repo_root, values)
    except Exception as exc:
        _failure(
            report,
            "tdjson.unavailable",
            "tdjson is not available through configured loader path resolution.",
            error_type=type(exc).__name__,
        )
        report["contract_status"] = "blocked_tdjson_unavailable"
        return False

    report["tdjson_available"] = True
    return True


def _apply_session_reuse_preflight(
    report: dict[str, Any],
    repo_root: Path,
    runtime_env_path: str | Path,
    tdjson_availability_checker: TdjsonAvailabilityChecker | None,
    collector_config_builder: CollectorConfigBuilder | None,
) -> bool:
    result = session_preflight.generate_report(
        repo_root=repo_root,
        runtime_env_path=runtime_env_path,
        tdjson_availability_checker=tdjson_availability_checker,
        collector_config_builder=collector_config_builder,
    )
    if result.report.get("contract_status") == "collector_readiness_preflight_passed":
        report["tdlib_session_reuse_preflight_passed"] = True
        return True

    _failure(
        report,
        "tdlib_session_reuse_preflight.failed",
        "Existing TDLib session reuse readiness preflight did not pass.",
    )
    report["contract_status"] = "blocked_tdlib_session_reuse_preflight_failed"
    return False


def _apply_database_select_1(
    report: dict[str, Any],
    connection: DatabaseConnection,
) -> bool:
    report["database_readiness_checked"] = True
    try:
        _execute_read_only(connection, "SELECT 1")
    except Exception as exc:
        _failure(
            report,
            "database.readiness",
            "PostgreSQL read-only readiness check failed.",
            error_type=type(exc).__name__,
        )
        report["contract_status"] = "blocked_database_unavailable"
        return False

    report["database_connected"] = True
    return True


def _apply_alembic_current_check(
    report: dict[str, Any],
    connection: DatabaseConnection,
) -> bool:
    report["alembic_current_checked"] = True
    try:
        rows = _rows(_execute_read_only(connection, "SELECT version_num FROM alembic_version"))
        versions = [str(value) for row in rows if (value := _first_cell(row)) is not None]
    except Exception as exc:
        _failure(
            report,
            "alembic_current.unavailable",
            "Alembic current state could not be checked read-only.",
            error_type=type(exc).__name__,
        )
        report["contract_status"] = "blocked_alembic_current_unavailable"
        return False

    report["alembic_current_available"] = bool(versions)
    report["alembic_current_revision_count_bucket"] = _bucket_count(len(versions))
    if not versions:
        _failure(
            report,
            "alembic_current.empty",
            "Alembic current state did not return any revision.",
        )
        report["contract_status"] = "blocked_alembic_current_unavailable"
        return False
    return True


def _apply_redis_ping(
    report: dict[str, Any],
    redis_url: str | None,
    redis_client_factory: RedisClientFactory | None,
) -> bool:
    report["redis_readiness_checked"] = True
    if not redis_url or not redis_url.strip():
        _failure(report, "redis.url_missing", "REDIS_URL is required for Redis ping readiness.")
        report["contract_status"] = "blocked_redis_unavailable"
        return False

    client: RedisClient | None = None
    try:
        client = (
            _open_default_redis_client(redis_url)
            if redis_client_factory is None
            else redis_client_factory(redis_url)
        )
        if client.ping() is not True:
            raise RuntimeError("redis ping returned a non-true response")
    except Exception as exc:
        _failure(
            report,
            "redis.ping",
            "Redis ping readiness check failed.",
            error_type=type(exc).__name__,
        )
        report["contract_status"] = "blocked_redis_unavailable"
        return False
    finally:
        if client is not None:
            client.close()

    report["redis_connected"] = True
    return True


def _apply_channel_registry_check(
    report: dict[str, Any],
    connection: DatabaseConnection,
) -> bool:
    report["channel_registry_checked"] = True
    try:
        value = _scalar(_execute_read_only(connection, ACTIVE_JOINED_CHANNEL_COUNT_QUERY))
        count = int(value or 0)
    except Exception as exc:
        _failure(
            report,
            "channel_registry.read_failed",
            "Active joined channel registry count could not be read safely.",
            error_type=type(exc).__name__,
        )
        report["contract_status"] = "blocked_database_unavailable"
        return False

    report["active_joined_channel_count_bucket"] = _bucket_count(count)
    report["active_joined_channels_present"] = count > 0
    if count <= 0:
        _failure(
            report,
            "channel_registry.no_active_joined_channels",
            "No active joined tracked channels are available for collector startup.",
        )
        report["contract_status"] = "blocked_no_active_joined_channels"
        return False
    return True


def _apply_singleton_lock_path_check(report: dict[str, Any], config: Any) -> bool:
    report["singleton_lock_path_checked"] = True
    lock_path_value = getattr(config, "singleton_lock_path", "") or ""
    parent = Path(str(lock_path_value)).expanduser().parent
    try:
        parent_exists = parent.exists()
        parent_is_dir = parent.is_dir()
        parent_writable = parent_exists and parent_is_dir and os.access(parent, os.W_OK)
    except OSError:
        parent_exists = False
        parent_is_dir = False
        parent_writable = False

    report["singleton_lock_path_parent_exists"] = parent_exists
    report["singleton_lock_path_parent_is_dir"] = parent_is_dir
    report["singleton_lock_path_parent_writable"] = parent_writable

    if not parent_exists or not parent_is_dir or not parent_writable:
        _failure(
            report,
            "singleton_lock_path.parent_unavailable",
            "Collector singleton lock path parent is missing or unusable.",
        )
        report["contract_status"] = "blocked_singleton_lock_path_unavailable"
        return False
    return True


def _open_database_connection(
    database_url: str,
    database_connection_factory: DatabaseConnectionFactory | None,
) -> tuple[DatabaseConnection, Callable[[], None]]:
    if database_connection_factory is not None:
        connection = database_connection_factory(database_url)
        return connection, connection.close
    return _open_default_database_connection(database_url)


def generate_report(
    *,
    repo_root: str | Path | None = None,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    tdjson_availability_checker: TdjsonAvailabilityChecker | None = None,
    collector_config_builder: CollectorConfigBuilder | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
) -> GateResult:
    resolved_repo_root = (
        Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    )
    report = _base_report(str(runtime_env_path))

    try:
        values = session_preflight.parse_runtime_env_file(runtime_env_path)
    except Exception as exc:
        _failure(
            report,
            "runtime_env.unreadable",
            "runtime env file could not be read safely.",
            error_type=type(exc).__name__,
        )
        return GateResult(exit_code=1, report=report)
    report["runtime_env_read"] = True

    config = _apply_collector_config_check(
        report,
        resolved_repo_root,
        values,
        collector_config_builder,
    )
    if config is None:
        return GateResult(exit_code=1, report=report)

    if not _apply_tdjson_check(
        report,
        resolved_repo_root,
        values,
        tdjson_availability_checker,
    ):
        return GateResult(exit_code=1, report=report)

    if not _apply_session_reuse_preflight(
        report,
        resolved_repo_root,
        runtime_env_path,
        tdjson_availability_checker,
        collector_config_builder,
    ):
        return GateResult(exit_code=1, report=report)

    database_url = values.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        report["database_readiness_checked"] = True
        _failure(report, "database.url_missing", "DATABASE_URL is required for readiness.")
        report["contract_status"] = "blocked_database_unavailable"
        return GateResult(exit_code=1, report=report)

    connection: DatabaseConnection | None = None
    cleanup: Callable[[], None] | None = None
    try:
        try:
            connection, cleanup = _open_database_connection(
                database_url,
                database_connection_factory,
            )
        except Exception as exc:
            report["database_readiness_checked"] = True
            _failure(
                report,
                "database.connection",
                "PostgreSQL connection could not be opened for read-only readiness.",
                error_type=type(exc).__name__,
            )
            report["contract_status"] = "blocked_database_unavailable"
            return GateResult(exit_code=1, report=report)

        if not _apply_database_select_1(report, connection):
            return GateResult(exit_code=1, report=report)

        if not _apply_alembic_current_check(report, connection):
            return GateResult(exit_code=1, report=report)

        if not _apply_redis_ping(
            report,
            values.get("REDIS_URL"),
            redis_client_factory,
        ):
            return GateResult(exit_code=1, report=report)

        if not _apply_channel_registry_check(report, connection):
            return GateResult(exit_code=1, report=report)
    finally:
        if cleanup is not None:
            cleanup()
        elif connection is not None:
            connection.close()

    if not _apply_singleton_lock_path_check(report, config):
        return GateResult(exit_code=1, report=report)

    report["contract_status"] = "collector_runtime_startup_readiness_gate_passed"
    return GateResult(exit_code=0, report=report)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        repo_root=args.repo_root,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
