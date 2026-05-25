from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import math
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_joined_channel_collector_startup_readiness_preflight"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_JOINED_ROW_LIMIT = 50
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 200
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC = 240.0
TDLIB_READY_STATE = "authorizationStateReady"

REQUIRED_TABLES = (
    "telegram_channel_registry",
    "telegram_raw_updates",
    "source_messages",
    "source_message_versions",
    "event_outbox",
)

SELECT_ONE_QUERY = "SELECT 1"
SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
COUNT_JOINED_ROWS_QUERY = """
SELECT COUNT(*)
FROM telegram_channel_registry
WHERE desired_state = 'active'
  AND access_state = 'joined'
  AND chat_id IS NOT NULL
"""
SELECT_JOINED_ROWS_LIMIT_QUERY = """
SELECT chat_id
FROM telegram_channel_registry
WHERE desired_state = 'active'
  AND access_state = 'joined'
  AND chat_id IS NOT NULL
ORDER BY priority_weight DESC, registry_id ASC
LIMIT :limit
"""

SIDE_EFFECT_FLAG_NAMES = (
    "database_mutation_performed",
    "telegram_channel_registry_updated",
    "telegram_raw_updates_written",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "redis_mutation_performed",
    "telegram_api_called",
    "tdlib_initialized",
    "tdlib_send_called",
    "tdlib_receive_called",
    "tdlib_auth_attempted",
    "tdlib_phone_number_submitted",
    "tdlib_code_submitted",
    "tdlib_password_submitted",
    "tdlib_join_called",
    "tdlib_history_fetch_called",
    "tdlib_public_username_resolve_called",
    "live_collector_started",
    "collector_runtime_started",
    "notifier_transport_enabled",
    "outbox_relay_started",
    "router_normalizer_started",
    "alembic_upgrade_run",
    "alembic_downgrade_run",
    "alembic_stamp_run",
    "docker_or_systemd_changed",
    "files_mutated_outside_repo",
)

COLLECTOR_IMPORTS = (
    ("collector_config_import_ok", "src.services.collector_telegram.config"),
    ("collector_runtime_import_ok", "src.services.collector_telegram.runtime"),
    ("collector_service_import_ok", "src.services.collector_telegram.service"),
    ("collector_repository_import_ok", "src.services.collector_telegram.repositories"),
    ("singleton_guard_import_ok", "src.services.collector_telegram.singleton_guard"),
)


class DatabaseConnection(Protocol):
    def begin(self) -> Any: ...

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


class TDLibReadinessProbe(Protocol):
    tdlib_send_called: bool
    tdlib_receive_called: bool

    @property
    def tdlib_ready_probe_summary(self) -> Mapping[str, Any]: ...

    async def initialize(self) -> None: ...

    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseConnectionFactory = Callable[[str], DatabaseConnection]
ModuleImporter = Callable[[str], Any]
TDLibReadinessProbeFactory = Callable[
    [Mapping[str, str], int, float, float],
    TDLibReadinessProbe,
]


class TDLibTransportUnavailable(RuntimeError):
    pass


class TDLibNotReady(RuntimeError):
    pass


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
            "Read-only collector startup readiness preflight for active joined "
            "telegram_channel_registry rows. This does not start collector runtime, "
            "fetch history, write ingest tables, write Redis, or mutate registry rows."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--approved-tdlib-readiness-probe",
        action="store_true",
        help=(
            "Initialize the existing-session TDLib readiness helper only; this "
            "does not submit phone, login code, password, joinChat, searchPublicChat, "
            "or getChatHistory requests."
        ),
    )
    parser.add_argument(
        "--joined-row-limit",
        type=_positive_int_named("joined-row-limit"),
        default=DEFAULT_JOINED_ROW_LIMIT,
    )
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
    return parser


def _positive_int_named(field_name: str) -> Callable[[str], int]:
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a positive integer"
            ) from exc
        if value <= 0:
            raise argparse.ArgumentTypeError(f"{field_name} must be a positive integer")
        return value

    return parse


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
    approved_tdlib_readiness_probe: bool,
) -> dict[str, Any]:
    side_effects = _side_effects()
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "required_tables_checked": [],
        "required_tables_available": {table: False for table in REQUIRED_TABLES},
        "joined_rows_checked": False,
        "joined_row_count_bucket": "unknown",
        "collector_config_import_ok": False,
        "collector_runtime_import_ok": False,
        "collector_service_import_ok": False,
        "collector_repository_import_ok": False,
        "singleton_guard_import_ok": False,
        "collector_config_contract_ok": False,
        "singleton_lock_path_configured": False,
        "singleton_lock_parent_available": False,
        "tdlib_readiness_probe_approved": approved_tdlib_readiness_probe,
        "tdlib_readiness_probe_attempted": False,
        "tdlib_ready_probe_status": "not_attempted",
        "tdlib_ready_probe_final_authorization_state": None,
        "tdlib_ready_helper_status": "not_attempted",
        "tdlib_ready_helper_manual_intervention_required": False,
        "tdlib_ready_probe_request_types_sent": [],
        "tdlib_ready_probe_authorization_states_seen": [],
        "tdlib_ready_probe_manual_intervention_required": False,
        "tdlib_ready_probe_error_class": None,
        "live_collector_start_approved_in_this_slice": False,
        "live_collector_started": False,
        "history_fetch_attempted": False,
        "source_messages_written": False,
        "source_message_versions_written": False,
        "event_outbox_written": False,
        "redis_mutation_performed": False,
        "database_mutation_performed": False,
        "operator_next_action": (
            "This is a read-only preflight. Do not start live collector in this "
            "slice; use a separately approved collector startup task after review."
        ),
        "side_effects": side_effects,
    }
    for flag, value in side_effects.items():
        report.setdefault(flag, value)
    return report


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _sync_report_side_effects(report: dict[str, Any]) -> None:
    side_effects = report["side_effects"]
    report["live_collector_started"] = side_effects["live_collector_started"]
    report["history_fetch_attempted"] = side_effects["tdlib_history_fetch_called"]
    report["source_messages_written"] = side_effects["source_messages_written"]
    report["source_message_versions_written"] = side_effects[
        "source_message_versions_written"
    ]
    report["event_outbox_written"] = side_effects["event_outbox_written"]
    report["redis_mutation_performed"] = side_effects["redis_mutation_performed"]
    report["database_mutation_performed"] = side_effects[
        "database_mutation_performed"
    ]


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.strip().split())


def _allowed_read_statements() -> set[str]:
    return {
        _normalize_sql(statement)
        for statement in (
            SELECT_ONE_QUERY,
            SET_TRANSACTION_READ_ONLY_QUERY,
            TABLE_AVAILABLE_QUERY,
            COUNT_JOINED_ROWS_QUERY,
            SELECT_JOINED_ROWS_LIMIT_QUERY,
        )
    }


def _assert_read_sql(statement: str) -> None:
    if _normalize_sql(statement) not in _allowed_read_statements():
        raise ValueError("SQL statement is not in the joined-channel read allowlist")


def _execute_read(
    connection: DatabaseConnection,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    _assert_read_sql(statement)
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


def _count_joined_rows(connection: DatabaseConnection) -> int:
    value = _scalar(_execute_read(connection, COUNT_JOINED_ROWS_QUERY))
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _check_required_tables(
    report: dict[str, Any],
    connection: DatabaseConnection,
) -> bool:
    all_available = True
    for table in REQUIRED_TABLES:
        report["required_tables_checked"].append(table)
        available = bool(
            _scalar(
                _execute_read(
                    connection,
                    TABLE_AVAILABLE_QUERY,
                    {"qualified_table_name": f"public.{table}"},
                )
            )
        )
        report["required_tables_available"][table] = available
        if not available:
            all_available = False
    return all_available


def _check_joined_rows(
    report: dict[str, Any],
    connection: DatabaseConnection,
    joined_row_limit: int,
) -> int:
    joined_count = _count_joined_rows(connection)
    report["joined_rows_checked"] = True
    report["joined_row_count_bucket"] = _bucket_count(joined_count)
    if joined_count > 0:
        _rows(
            _execute_read(
                connection,
                SELECT_JOINED_ROWS_LIMIT_QUERY,
                {"limit": joined_row_limit},
            )
        )
    return joined_count


def _import_collector_modules(
    report: dict[str, Any],
    module_importer: ModuleImporter | None,
) -> dict[str, Any] | None:
    importer = module_importer or importlib.import_module
    modules: dict[str, Any] = {}
    for field, module_name in COLLECTOR_IMPORTS:
        try:
            modules[module_name] = importer(module_name)
        except Exception:
            report[field] = False
            _set_status(
                report,
                "blocked_collector_import_contract_failed",
                f"collector_import.{module_name.rsplit('.', 1)[-1]}",
            )
            return None
        report[field] = True
    return modules


def _check_collector_config_and_singleton(
    report: dict[str, Any],
    modules: Mapping[str, Any],
    values: Mapping[str, str],
) -> bool:
    config_module = modules["src.services.collector_telegram.config"]
    try:
        config = config_module.CollectorTelegramConfig.from_env(values)
    except Exception:
        report["collector_config_contract_ok"] = False
        _set_status(
            report,
            "blocked_singleton_config_unavailable",
            "collector_config.contract",
        )
        return False

    report["collector_config_contract_ok"] = True
    lock_path_value = getattr(config, "singleton_lock_path", "")
    lock_path = Path(str(lock_path_value))
    report["singleton_lock_path_configured"] = bool(str(lock_path_value).strip())
    try:
        report["singleton_lock_parent_available"] = (
            lock_path.is_absolute()
            and lock_path.parent.exists()
            and lock_path.parent.is_dir()
        )
    except OSError:
        report["singleton_lock_parent_available"] = False

    if (
        not report["singleton_lock_path_configured"]
        or not report["singleton_lock_parent_available"]
    ):
        _set_status(
            report,
            "blocked_singleton_config_unavailable",
            "singleton.lock_path_unavailable",
        )
        return False
    return True


def _default_tdlib_readiness_probe_factory(
    runtime_env: Mapping[str, str],
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
) -> TDLibReadinessProbe:
    from scripts.ops import (  # noqa: PLC0415
        dedicated_vps_telegram_channel_registry_public_username_resolve_operator
        as resolve_operator,
    )

    return resolve_operator.TDLibPublicUsernameResolver(
        runtime_env,
        auth_max_updates=tdlib_auth_max_updates,
        receive_timeout_sec=tdlib_receive_timeout_sec,
        overall_timeout_sec=tdlib_overall_timeout_sec,
    )


def _safe_probe_summary(probe: TDLibReadinessProbe | None) -> Mapping[str, Any]:
    if probe is None:
        return {}
    try:
        summary = probe.tdlib_ready_probe_summary
    except Exception:
        return {}
    return summary if isinstance(summary, Mapping) else {}


def _merge_tdlib_probe_fields(
    report: dict[str, Any],
    probe: TDLibReadinessProbe | None,
) -> None:
    summary = _safe_probe_summary(probe)
    report["tdlib_readiness_probe_attempted"] = bool(
        summary.get("tdlib_ready_probe_attempted", True)
    )
    report["tdlib_ready_probe_status"] = _safe_text(
        summary.get("tdlib_ready_probe_status"),
        default=report["tdlib_ready_probe_status"],
    )
    report["tdlib_ready_probe_final_authorization_state"] = _safe_text(
        summary.get("tdlib_ready_probe_final_authorization_state"),
        default=report["tdlib_ready_probe_final_authorization_state"],
    )
    report["tdlib_ready_helper_status"] = _safe_text(
        summary.get("tdlib_ready_helper_status"),
        default=report["tdlib_ready_helper_status"],
    )
    report["tdlib_ready_helper_manual_intervention_required"] = bool(
        summary.get("tdlib_ready_helper_manual_intervention_required", False)
    )
    report["tdlib_ready_probe_manual_intervention_required"] = bool(
        summary.get("tdlib_ready_probe_manual_intervention_required", False)
    )
    report["tdlib_ready_probe_error_class"] = _safe_text(
        summary.get("tdlib_ready_probe_error_class"),
        default=None,
    )
    report["tdlib_ready_probe_request_types_sent"] = _safe_text_list(
        summary.get("tdlib_ready_probe_request_types_sent")
    )
    report["tdlib_ready_probe_authorization_states_seen"] = _safe_text_list(
        summary.get("tdlib_ready_probe_authorization_states_seen")
    )
    if probe is not None:
        report["side_effects"]["tdlib_send_called"] = bool(
            getattr(probe, "tdlib_send_called", False)
        )
        report["side_effects"]["tdlib_receive_called"] = bool(
            getattr(probe, "tdlib_receive_called", False)
        )
        if (
            report["side_effects"]["tdlib_send_called"]
            or report["side_effects"]["tdlib_receive_called"]
        ):
            report["side_effects"]["telegram_api_called"] = True


def _safe_text(value: Any, *, default: str | None = None) -> str | None:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", value):
        return value
    return default


def _safe_text_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    safe_values: list[str] = []
    for item in value:
        safe_item = _safe_text(item)
        if safe_item is not None and safe_item not in safe_values:
            safe_values.append(safe_item)
    return safe_values


def _tdlib_probe_is_ready(report: Mapping[str, Any]) -> bool:
    return (
        report.get("tdlib_ready_probe_status") == "ready"
        and report.get("tdlib_ready_probe_final_authorization_state")
        == TDLIB_READY_STATE
        and report.get("tdlib_ready_helper_status") == "ready"
    )


async def _close_probe(probe: TDLibReadinessProbe | None) -> None:
    if probe is None:
        return
    try:
        await probe.close()
    except Exception:
        return


def _run_tdlib_readiness_probe(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
    tdlib_readiness_probe_factory: TDLibReadinessProbeFactory | None,
) -> ScriptResult | None:
    if not report["tdlib_readiness_probe_approved"]:
        return None

    report["tdlib_readiness_probe_attempted"] = True
    report["side_effects"]["tdlib_initialized"] = True
    probe: TDLibReadinessProbe | None = None
    try:
        factory = (
            tdlib_readiness_probe_factory
            or _default_tdlib_readiness_probe_factory
        )
        probe = factory(
            values,
            tdlib_auth_max_updates,
            tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec,
        )
        asyncio.run(probe.initialize())
        _merge_tdlib_probe_fields(report, probe)
        if _tdlib_probe_is_ready(report):
            _set_status(report, "joined_channel_collector_startup_readiness_tdlib_ready")
            report["operator_next_action"] = (
                "TDLib session readiness reached authorizationStateReady and joined "
                "rows exist. This still does not start collector or prove ingest "
                "correctness; live collector startup remains outside this slice."
            )
            return ScriptResult(exit_code=0, report=report)
        _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
        return ScriptResult(exit_code=1, report=report)
    except TDLibNotReady:
        _merge_tdlib_probe_fields(report, probe)
        _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
        return ScriptResult(exit_code=1, report=report)
    except Exception:
        _merge_tdlib_probe_fields(report, probe)
        probe_status = report.get("tdlib_ready_probe_status")
        if probe is None:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.transport_unavailable",
            )
        elif report.get("tdlib_ready_probe_manual_intervention_required") is True:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.manual_intervention")
        elif probe_status in {"timed_out", "not_ready", "tdlib_error"}:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
        else:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.transport_unavailable",
            )
        return ScriptResult(exit_code=1, report=report)
    finally:
        asyncio.run(_close_probe(probe))


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approved_tdlib_readiness_probe: bool = False,
    joined_row_limit: int = DEFAULT_JOINED_ROW_LIMIT,
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    module_importer: ModuleImporter | None = None,
    tdlib_readiness_probe_factory: TDLibReadinessProbeFactory | None = None,
) -> ScriptResult:
    report = _base_report(
        approved_tdlib_readiness_probe=approved_tdlib_readiness_probe,
    )

    try:
        values = _read_runtime_env(runtime_env_path, runtime_env_reader)
    except Exception:
        _set_status(report, "blocked_runtime_env_unreadable", "runtime_env.unreadable")
        _sync_report_side_effects(report)
        return ScriptResult(exit_code=1, report=report)
    report["runtime_env_read"] = True

    modules = _import_collector_modules(report, module_importer)
    if modules is None:
        _sync_report_side_effects(report)
        return ScriptResult(exit_code=1, report=report)

    if not _check_collector_config_and_singleton(report, modules, values):
        _sync_report_side_effects(report)
        return ScriptResult(exit_code=1, report=report)

    database_url = values.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        _set_status(report, "blocked_database_unavailable", "database.url_missing")
        _sync_report_side_effects(report)
        return ScriptResult(exit_code=1, report=report)
    if not _database_url_is_supported(database_url):
        _set_status(report, "blocked_database_unavailable", "database.url_unsupported")
        _sync_report_side_effects(report)
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
            _execute_read(connection, SET_TRANSACTION_READ_ONLY_QUERY)
            _execute_read(connection, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not _check_required_tables(report, connection):
                _set_status(
                    report,
                    "blocked_required_tables_unavailable",
                    "database.required_tables_unavailable",
                )
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(report, "blocked_database_unavailable", "database.connection")
            return ScriptResult(exit_code=1, report=report)

        joined_count = _check_joined_rows(report, connection, joined_row_limit)
        if joined_count == 0:
            _set_status(
                report,
                "blocked_no_joined_channel_rows",
                "registry.no_active_joined_rows",
            )
            report["operator_next_action"] = (
                "No active joined registry rows with non-null chat_id exist. Do "
                "not start collector until registry state is reviewed."
            )
            return ScriptResult(exit_code=1, report=report)

        probe_result = _run_tdlib_readiness_probe(
            report=report,
            values=values,
            tdlib_auth_max_updates=tdlib_auth_max_updates,
            tdlib_receive_timeout_sec=tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec=tdlib_overall_timeout_sec,
            tdlib_readiness_probe_factory=tdlib_readiness_probe_factory,
        )
        if probe_result is not None:
            return probe_result

        _set_status(report, "joined_channel_collector_startup_readiness_dry_run_ready")
        report["operator_next_action"] = (
            "Joined rows, runtime env readability, DB reachability, required "
            "tables, collector imports, config, and singleton lock path are "
            "preflight-ready. This does not start collector and does not prove "
            "ingest correctness; live collector startup remains unsafe in this slice."
        )
        return ScriptResult(exit_code=0, report=report)
    except Exception:
        _set_status(report, "blocked_unexpected_error", "unexpected")
        return ScriptResult(exit_code=1, report=report)
    finally:
        _rollback_transaction(transaction)
        _close_connection(cleanup, connection)
        _sync_report_side_effects(report)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        approved_tdlib_readiness_probe=args.approved_tdlib_readiness_probe,
        joined_row_limit=args.joined_row_limit,
        tdlib_auth_max_updates=args.tdlib_auth_max_updates,
        tdlib_receive_timeout_sec=args.tdlib_receive_timeout_sec,
        tdlib_overall_timeout_sec=args.tdlib_overall_timeout_sec,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
