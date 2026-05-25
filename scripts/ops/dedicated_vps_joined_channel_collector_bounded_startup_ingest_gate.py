from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import math
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_joined_channel_collector_bounded_startup_ingest_gate"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_JOINED_ROW_LIMIT = 50
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 200
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC = 240.0
TDLIB_READY_STATE = "authorizationStateReady"

MAX_COLLECTOR_SMOKE_DURATION_SEC = 120
MAX_COLLECTOR_SMOKE_UPDATES = 100
MAX_COLLECTOR_SMOKE_DB_WRITES = 100
SAFE_TDLIB_UPDATE_TYPE_RE = re.compile(r"update[A-Z][A-Za-z0-9]{0,80}\Z")

MESSAGE_BEARING_UPDATE_TYPES = frozenset(
    {
        "updateNewMessage",
        "updateMessageContent",
        "updateMessageEdited",
        "updateDeleteMessages",
    }
)
RECONCILE_SIGNAL_UPDATE_TYPES = frozenset({"updateChatLastMessage"})

REQUIRED_TABLES = (
    "telegram_channel_registry",
    "telegram_raw_updates",
    "source_messages",
    "source_message_versions",
    "event_outbox",
)

COLLECTOR_OWNED_WRITE_TABLES = frozenset(
    {
        "telegram_raw_updates",
        "source_messages",
        "source_message_versions",
        "event_outbox",
    }
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
    "telegram_channel_registry_inserted",
    "telegram_channel_registry_deleted",
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
    "tdlib_search_public_chat_called",
    "tdlib_send_message_called",
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

AUTH_SUBMISSION_REQUEST_FLAGS = {
    "setAuthenticationPhoneNumber": (
        "tdlib_auth_attempted",
        "tdlib_phone_number_submitted",
    ),
    "checkAuthenticationCode": ("tdlib_auth_attempted", "tdlib_code_submitted"),
    "checkAuthenticationPassword": ("tdlib_auth_attempted", "tdlib_password_submitted"),
}

TDLIB_FORBIDDEN_REQUEST_FLAGS = {
    "joinChat": "tdlib_join_called",
    "joinChatByInviteLink": "tdlib_join_called",
    "getChatHistory": "tdlib_history_fetch_called",
    "searchPublicChat": "tdlib_public_username_resolve_called",
    "sendMessage": "tdlib_send_message_called",
}

SMOKE_ALLOWED_SIDE_EFFECTS = frozenset(
    {
        "database_mutation_performed",
        "telegram_raw_updates_written",
        "source_messages_written",
        "source_message_versions_written",
        "event_outbox_written",
        "telegram_api_called",
        "tdlib_initialized",
        "tdlib_send_called",
        "tdlib_receive_called",
        "live_collector_started",
        "collector_runtime_started",
    }
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


class CollectorSmokeRunner(Protocol):
    async def run(
        self,
        *,
        runtime_env: Mapping[str, str],
        bounds: "CollectorSmokeBounds",
    ) -> "CollectorSmokeResult": ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseConnectionFactory = Callable[[str], DatabaseConnection]
ModuleImporter = Callable[[str], Any]
TDLibReadinessProbeFactory = Callable[
    [Mapping[str, str], int, float, float],
    TDLibReadinessProbe,
]
CollectorSmokeRunnerFactory = Callable[[Mapping[str, str]], CollectorSmokeRunner]


class TDLibTransportUnavailable(RuntimeError):
    pass


class TDLibNotReady(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CollectorSmokeBounds:
    max_duration_sec: int
    max_updates: int
    max_db_writes: int


@dataclass(frozen=True, slots=True)
class CollectorSmokeResult:
    status: str = "completed"
    failure_class: str | None = None
    updates_observed: int = 0
    update_types_seen: tuple[tuple[str, int], ...] = ()
    message_bearing_updates_observed: int = 0
    message_bearing_updates_dispatched: int = 0
    control_updates_observed: int = 0
    reconcile_signal_updates_observed: int = 0
    control_updates_skipped_not_written: int = 0
    telegram_raw_updates_written: int = 0
    source_messages_written: int = 0
    source_message_versions_written: int = 0
    event_outbox_written: int = 0
    canonical_ingest_writes_observed: bool = False
    raw_only_writes_observed: bool = False
    message_ingest_not_proven: bool = False
    duration_exhausted: bool = False
    update_cap_exhausted: bool = False
    db_write_cap_exhausted: bool = False
    written_tables: tuple[str, ...] = ()
    side_effects: Mapping[str, bool] = field(default_factory=dict)


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
            "Operator gate for a bounded collector startup / ingest smoke against "
            "active joined telegram_channel_registry rows. Default mode is a "
            "read-only plan: no TDLib initialization, no collector startup, no "
            "Redis writes, and no ingest DB writes."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--approved-tdlib-readiness-probe",
        action="store_true",
        help=(
            "Initialize the existing-session TDLib readiness helper only; this "
            "must not submit phone, login code, password, joinChat, "
            "searchPublicChat, or getChatHistory requests."
        ),
    )
    parser.add_argument(
        "--approved-live-collector-startup-smoke",
        action="store_true",
        help=(
            "Authorize only the bounded collector startup smoke path after "
            "preconditions and smoke caps pass."
        ),
    )
    parser.add_argument(
        "--approved-collector-ingest-db-write",
        action="store_true",
        help=(
            "Authorize collector-owned ingest table writes during bounded smoke. "
            "This is valid only together with startup smoke approval."
        ),
    )
    parser.add_argument(
        "--approved-message-bearing-probe-mode",
        action="store_true",
        help=(
            "Authorize diagnostic-only message-bearing probe mode for the "
            "write-capable bounded startup smoke. Control/state/reconcile "
            "updates are counted but skipped before dispatcher/raw-update writes."
        ),
    )
    parser.add_argument(
        "--joined-row-limit",
        type=_positive_int_named("joined-row-limit"),
        default=DEFAULT_JOINED_ROW_LIMIT,
    )
    parser.add_argument("--collector-smoke-max-duration-sec", type=int, default=None)
    parser.add_argument("--collector-smoke-max-updates", type=int, default=None)
    parser.add_argument("--collector-smoke-max-db-writes", type=int, default=None)
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
    approved_live_collector_startup_smoke: bool,
    approved_collector_ingest_db_write: bool,
    approved_message_bearing_probe_mode: bool,
    collector_smoke_max_duration_sec: int | None,
    collector_smoke_max_updates: int | None,
    collector_smoke_max_db_writes: int | None,
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
        "live_collector_startup_smoke_approved": (
            approved_live_collector_startup_smoke
        ),
        "collector_ingest_db_write_approved": approved_collector_ingest_db_write,
        "collector_message_bearing_probe_mode_approved": (
            approved_message_bearing_probe_mode
        ),
        "collector_message_bearing_probe_mode": False,
        "collector_smoke_attempted": False,
        "collector_smoke_status": "not_attempted",
        "collector_smoke_failure_class": None,
        "collector_smoke_max_duration_sec": collector_smoke_max_duration_sec,
        "collector_smoke_max_updates": collector_smoke_max_updates,
        "collector_smoke_max_db_writes": collector_smoke_max_db_writes,
        "collector_smoke_duration_exhausted": False,
        "collector_smoke_update_cap_exhausted": False,
        "collector_smoke_db_write_cap_exhausted": False,
        "collector_smoke_updates_observed_bucket": "unknown",
        "collector_smoke_update_types_seen": {},
        "collector_smoke_message_bearing_updates_observed": False,
        "collector_smoke_message_bearing_updates_observed_bucket": "zero",
        "collector_smoke_message_bearing_updates_dispatched_bucket": "zero",
        "collector_smoke_control_updates_observed_bucket": "zero",
        "collector_smoke_reconcile_signal_updates_observed_bucket": "zero",
        "collector_smoke_control_updates_skipped_not_written_bucket": "zero",
        "collector_smoke_raw_updates_written_bucket": "zero",
        "collector_smoke_source_messages_written_bucket": "zero",
        "collector_smoke_source_message_versions_written_bucket": "zero",
        "collector_smoke_event_outbox_written_bucket": "zero",
        "collector_smoke_canonical_ingest_writes_observed": False,
        "collector_smoke_raw_only_writes_observed": False,
        "collector_smoke_message_ingest_not_proven": False,
        "collector_smoke_no_updates_observed": False,
        "live_collector_started": False,
        "collector_runtime_started": False,
        "history_fetch_attempted": False,
        "source_messages_written": False,
        "source_message_versions_written": False,
        "event_outbox_written": False,
        "telegram_raw_updates_written": False,
        "redis_mutation_performed": False,
        "database_mutation_performed": False,
        "operator_next_action": (
            "Fix runtime env or DB access on the VPS without pasting runtime.env "
            "values, DB URLs, Redis URLs, chat IDs, usernames, invite links, "
            "phone numbers, TDLib payloads, temp paths, or Telegram secrets."
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
    report["collector_runtime_started"] = side_effects["collector_runtime_started"]
    report["history_fetch_attempted"] = side_effects["tdlib_history_fetch_called"]
    report["source_messages_written"] = side_effects["source_messages_written"]
    report["source_message_versions_written"] = side_effects[
        "source_message_versions_written"
    ]
    report["event_outbox_written"] = side_effects["event_outbox_written"]
    report["telegram_raw_updates_written"] = side_effects[
        "telegram_raw_updates_written"
    ]
    report["redis_mutation_performed"] = side_effects["redis_mutation_performed"]
    report["database_mutation_performed"] = side_effects[
        "database_mutation_performed"
    ]
    for flag, value in side_effects.items():
        report[flag] = value


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


def _safe_text(value: Any, *, default: str | None = None) -> str | None:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", value):
        return value
    return default


def _safe_smoke_status(value: Any) -> str:
    return _safe_text(value, default="unknown") or "unknown"


def _safe_update_type(value: Any) -> str | None:
    if isinstance(value, str) and SAFE_TDLIB_UPDATE_TYPE_RE.fullmatch(value):
        return value
    return None


def _safe_text_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    safe_values: list[str] = []
    for item in value:
        safe_item = _safe_text(item)
        if safe_item is not None and safe_item not in safe_values:
            safe_values.append(safe_item)
    return safe_values


def _safe_non_negative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _classify_update_type(update_type: str) -> str:
    if update_type in MESSAGE_BEARING_UPDATE_TYPES:
        return "message_bearing"
    if update_type in RECONCILE_SIGNAL_UPDATE_TYPES:
        return "reconcile_signal"
    return "control_or_state"


def _safe_update_type_counts(value: Any) -> dict[str, int]:
    if isinstance(value, Mapping):
        items = value.items()
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = []
        for item in value:
            if (
                isinstance(item, Sequence)
                and not isinstance(item, (str, bytes, bytearray))
                and len(item) == 2
            ):
                items.append((item[0], item[1]))
    else:
        items = []

    counts: dict[str, int] = {}
    for update_type_value, count_value in items:
        update_type = _safe_update_type(update_type_value)
        if update_type is None:
            continue
        count = _safe_non_negative_int(count_value)
        if count <= 0:
            continue
        counts[update_type] = counts.get(update_type, 0) + count
    return dict(sorted(counts.items()))


def _bucket_update_type_counts(counts: Mapping[str, int]) -> dict[str, str]:
    return {update_type: _bucket_count(count) for update_type, count in counts.items()}


def _class_count_from_types(
    counts: Mapping[str, int],
    update_class: str,
) -> int:
    return sum(
        count
        for update_type, count in counts.items()
        if _classify_update_type(update_type) == update_class
    )


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
    _apply_tdlib_request_side_effects(report)


def _apply_tdlib_request_side_effects(report: dict[str, Any]) -> None:
    side_effects = report["side_effects"]
    for request_type in report["tdlib_ready_probe_request_types_sent"]:
        for flag in AUTH_SUBMISSION_REQUEST_FLAGS.get(request_type, ()):
            side_effects[flag] = True
        forbidden_flag = TDLIB_FORBIDDEN_REQUEST_FLAGS.get(request_type)
        if forbidden_flag is not None:
            side_effects[forbidden_flag] = True
    if side_effects["tdlib_search_public_chat_called"]:
        side_effects["tdlib_public_username_resolve_called"] = True


def _tdlib_probe_is_ready(report: Mapping[str, Any]) -> bool:
    return (
        report.get("tdlib_ready_probe_status") == "ready"
        and report.get("tdlib_ready_probe_final_authorization_state")
        == TDLIB_READY_STATE
        and report.get("tdlib_ready_helper_status") == "ready"
    )


def _tdlib_probe_forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    side_effects = report["side_effects"]
    return any(
        side_effects[flag]
        for flag in (
            "tdlib_auth_attempted",
            "tdlib_phone_number_submitted",
            "tdlib_code_submitted",
            "tdlib_password_submitted",
            "tdlib_join_called",
            "tdlib_history_fetch_called",
            "tdlib_public_username_resolve_called",
            "tdlib_search_public_chat_called",
            "tdlib_send_message_called",
        )
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
        if _tdlib_probe_forbidden_side_effect_detected(report):
            _set_status(
                report,
                "blocked_forbidden_side_effect_detected",
                "tdlib.forbidden_request",
            )
            return ScriptResult(exit_code=1, report=report)
        if _tdlib_probe_is_ready(report):
            _set_status(report, "joined_channel_collector_bounded_startup_tdlib_ready")
            report["operator_next_action"] = (
                "TDLib session readiness reached authorizationStateReady. This "
                "does not start collector runtime or write ingest tables."
            )
            return None
        _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
        return ScriptResult(exit_code=1, report=report)
    except TDLibNotReady:
        _merge_tdlib_probe_fields(report, probe)
        _set_status(report, "blocked_tdlib_not_ready", "tdlib.not_ready")
        return ScriptResult(exit_code=1, report=report)
    except Exception:
        _merge_tdlib_probe_fields(report, probe)
        if _tdlib_probe_forbidden_side_effect_detected(report):
            _set_status(
                report,
                "blocked_forbidden_side_effect_detected",
                "tdlib.forbidden_request",
            )
        elif probe is None:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.transport_unavailable",
            )
        elif report.get("tdlib_ready_probe_manual_intervention_required") is True:
            _set_status(report, "blocked_tdlib_not_ready", "tdlib.manual_intervention")
        else:
            _set_status(
                report,
                "blocked_tdlib_transport_unavailable",
                "tdlib.transport_unavailable",
            )
        return ScriptResult(exit_code=1, report=report)
    finally:
        asyncio.run(_close_probe(probe))


def _valid_smoke_bound(value: Any, *, upper_bound: int) -> bool:
    if isinstance(value, bool) or not isinstance(value, int):
        return False
    return 0 < value <= upper_bound


def _validate_smoke_bounds(report: dict[str, Any]) -> CollectorSmokeBounds | None:
    duration = report["collector_smoke_max_duration_sec"]
    updates = report["collector_smoke_max_updates"]
    writes = report["collector_smoke_max_db_writes"]
    if not (
        _valid_smoke_bound(duration, upper_bound=MAX_COLLECTOR_SMOKE_DURATION_SEC)
        and _valid_smoke_bound(updates, upper_bound=MAX_COLLECTOR_SMOKE_UPDATES)
        and _valid_smoke_bound(writes, upper_bound=MAX_COLLECTOR_SMOKE_DB_WRITES)
    ):
        _set_status(
            report,
            "blocked_invalid_smoke_bounds",
            "collector_smoke.invalid_bounds",
        )
        report["operator_next_action"] = (
            "Provide all smoke caps inside hard bounds: duration <= 120 seconds, "
            "updates <= 100, and DB writes <= 100."
        )
        return None
    return CollectorSmokeBounds(
        max_duration_sec=duration,
        max_updates=updates,
        max_db_writes=writes,
    )


def _merge_smoke_result(report: dict[str, Any], result: CollectorSmokeResult) -> None:
    side_effects = report["side_effects"]
    for flag, value in result.side_effects.items():
        if flag in side_effects:
            side_effects[flag] = bool(value)

    update_type_counts = _safe_update_type_counts(
        getattr(result, "update_types_seen", ())
    )
    message_bearing_updates_observed = _safe_non_negative_int(
        getattr(result, "message_bearing_updates_observed", 0)
    ) or _class_count_from_types(update_type_counts, "message_bearing")
    message_bearing_updates_dispatched = _safe_non_negative_int(
        getattr(result, "message_bearing_updates_dispatched", 0)
    )
    control_updates_observed = _safe_non_negative_int(
        getattr(result, "control_updates_observed", 0)
    ) or _class_count_from_types(update_type_counts, "control_or_state")
    reconcile_signal_updates_observed = _safe_non_negative_int(
        getattr(result, "reconcile_signal_updates_observed", 0)
    ) or _class_count_from_types(update_type_counts, "reconcile_signal")
    control_updates_skipped_not_written = _safe_non_negative_int(
        getattr(result, "control_updates_skipped_not_written", 0)
    )

    write_counts = {
        "telegram_raw_updates_written": _safe_non_negative_int(
            result.telegram_raw_updates_written
        ),
        "source_messages_written": _safe_non_negative_int(
            result.source_messages_written
        ),
        "source_message_versions_written": _safe_non_negative_int(
            result.source_message_versions_written
        ),
        "event_outbox_written": _safe_non_negative_int(result.event_outbox_written),
    }
    for flag, count in write_counts.items():
        if count > 0:
            side_effects[flag] = True
    if any(count > 0 for count in write_counts.values()):
        side_effects["database_mutation_performed"] = True

    canonical_ingest_writes_observed = any(
        count > 0
        for key, count in write_counts.items()
        if key
        in {
            "source_messages_written",
            "source_message_versions_written",
            "event_outbox_written",
        }
    )
    raw_only_writes_observed = (
        write_counts["telegram_raw_updates_written"] > 0
        and not canonical_ingest_writes_observed
    )
    message_ingest_proven = (
        canonical_ingest_writes_observed and message_bearing_updates_observed > 0
    )

    report["collector_smoke_status"] = _safe_smoke_status(
        getattr(result, "status", "completed")
    )
    report["collector_smoke_failure_class"] = _safe_text(
        getattr(result, "failure_class", None),
        default=None,
    )
    report["collector_smoke_duration_exhausted"] = result.duration_exhausted
    report["collector_smoke_update_cap_exhausted"] = result.update_cap_exhausted
    report["collector_smoke_db_write_cap_exhausted"] = result.db_write_cap_exhausted
    report["collector_smoke_updates_observed_bucket"] = _bucket_count(
        _safe_non_negative_int(result.updates_observed)
    )
    report["collector_smoke_update_types_seen"] = _bucket_update_type_counts(
        update_type_counts
    )
    report["collector_smoke_message_bearing_updates_observed"] = (
        message_bearing_updates_observed > 0
    )
    report["collector_smoke_message_bearing_updates_observed_bucket"] = (
        _bucket_count(message_bearing_updates_observed)
    )
    report["collector_smoke_message_bearing_updates_dispatched_bucket"] = (
        _bucket_count(message_bearing_updates_dispatched)
    )
    report["collector_smoke_control_updates_observed_bucket"] = _bucket_count(
        control_updates_observed
    )
    report["collector_smoke_reconcile_signal_updates_observed_bucket"] = (
        _bucket_count(reconcile_signal_updates_observed)
    )
    report["collector_smoke_control_updates_skipped_not_written_bucket"] = (
        _bucket_count(control_updates_skipped_not_written)
    )
    report["collector_smoke_raw_updates_written_bucket"] = _bucket_count(
        write_counts["telegram_raw_updates_written"]
    )
    report["collector_smoke_source_messages_written_bucket"] = _bucket_count(
        write_counts["source_messages_written"]
    )
    report["collector_smoke_source_message_versions_written_bucket"] = _bucket_count(
        write_counts["source_message_versions_written"]
    )
    report["collector_smoke_event_outbox_written_bucket"] = _bucket_count(
        write_counts["event_outbox_written"]
    )
    report["collector_smoke_canonical_ingest_writes_observed"] = (
        canonical_ingest_writes_observed
    )
    report["collector_smoke_raw_only_writes_observed"] = raw_only_writes_observed
    report["collector_smoke_message_ingest_not_proven"] = not message_ingest_proven
    report["collector_smoke_no_updates_observed"] = (
        _safe_non_negative_int(result.updates_observed) <= 0
    )


def _smoke_result_is_failure(result: CollectorSmokeResult) -> bool:
    return getattr(result, "status", "completed") != "completed" or bool(
        getattr(result, "failure_class", None)
    )


def _blocked_smoke_status(result: CollectorSmokeResult) -> str:
    if getattr(result, "failure_class", None) == "manual_authorization_required":
        return "blocked_collector_smoke_manual_authorization_required"
    return "blocked_collector_smoke_runner_failed"


def _smoke_inconsistent_observation(report: Mapping[str, Any]) -> bool:
    return bool(report.get("collector_smoke_canonical_ingest_writes_observed")) and not bool(
        report.get("collector_smoke_message_bearing_updates_observed")
    )


def _partial_smoke_result_from_exception(exc: Exception) -> CollectorSmokeResult | None:
    result = getattr(exc, "result", None)
    return result if _looks_like_smoke_result(result) else None


def _looks_like_smoke_result(value: Any) -> bool:
    return all(
        hasattr(value, field_name)
        for field_name in (
            "updates_observed",
            "telegram_raw_updates_written",
            "source_messages_written",
            "source_message_versions_written",
            "event_outbox_written",
            "written_tables",
            "side_effects",
        )
    )


def _smoke_forbidden_side_effect_detected(
    report: Mapping[str, Any],
    result: CollectorSmokeResult,
    bounds: CollectorSmokeBounds,
) -> bool:
    side_effects = report["side_effects"]
    for flag, value in side_effects.items():
        if value and flag not in SMOKE_ALLOWED_SIDE_EFFECTS:
            return True

    written_tables = {table for table in result.written_tables if table}
    if not written_tables.issubset(COLLECTOR_OWNED_WRITE_TABLES):
        return True

    total_writes = (
        max(result.telegram_raw_updates_written, 0)
        + max(result.source_messages_written, 0)
        + max(result.source_message_versions_written, 0)
        + max(result.event_outbox_written, 0)
    )
    return total_writes > bounds.max_db_writes


async def _run_smoke_runner(
    runner: CollectorSmokeRunner,
    *,
    runtime_env: Mapping[str, str],
    bounds: CollectorSmokeBounds,
) -> CollectorSmokeResult:
    return await runner.run(runtime_env=runtime_env, bounds=bounds)


def _default_collector_smoke_runner_factory(
    runtime_env: Mapping[str, str],
    *,
    message_bearing_probe_mode: bool = False,
) -> CollectorSmokeRunner:
    from src.services.collector_telegram.bounded_smoke_runner import (  # noqa: PLC0415
        build_default_bounded_collector_smoke_runner,
    )

    return build_default_bounded_collector_smoke_runner(
        runtime_env,
        message_bearing_probe_mode=message_bearing_probe_mode,
    )


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approved_tdlib_readiness_probe: bool = False,
    approved_live_collector_startup_smoke: bool = False,
    approved_collector_ingest_db_write: bool = False,
    approved_message_bearing_probe_mode: bool = False,
    joined_row_limit: int = DEFAULT_JOINED_ROW_LIMIT,
    collector_smoke_max_duration_sec: int | None = None,
    collector_smoke_max_updates: int | None = None,
    collector_smoke_max_db_writes: int | None = None,
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    module_importer: ModuleImporter | None = None,
    tdlib_readiness_probe_factory: TDLibReadinessProbeFactory | None = None,
    collector_smoke_runner_factory: CollectorSmokeRunnerFactory | None = None,
) -> ScriptResult:
    report = _base_report(
        approved_tdlib_readiness_probe=approved_tdlib_readiness_probe,
        approved_live_collector_startup_smoke=approved_live_collector_startup_smoke,
        approved_collector_ingest_db_write=approved_collector_ingest_db_write,
        approved_message_bearing_probe_mode=approved_message_bearing_probe_mode,
        collector_smoke_max_duration_sec=collector_smoke_max_duration_sec,
        collector_smoke_max_updates=collector_smoke_max_updates,
        collector_smoke_max_db_writes=collector_smoke_max_db_writes,
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

        if (
            approved_collector_ingest_db_write
            and not approved_live_collector_startup_smoke
        ):
            _set_status(
                report,
                "blocked_approval_required",
                "approval.live_collector_startup_smoke_required",
            )
            report["operator_next_action"] = (
                "Collector ingest DB-write approval is valid only with explicit "
                "bounded live collector startup smoke approval."
            )
            return ScriptResult(exit_code=1, report=report)

        if approved_message_bearing_probe_mode and not (
            approved_tdlib_readiness_probe
            and approved_live_collector_startup_smoke
            and approved_collector_ingest_db_write
        ):
            _set_status(
                report,
                "blocked_approval_required",
                "approval.message_bearing_probe_mode_requires_full_smoke_approvals",
            )
            report["operator_next_action"] = (
                "Message-bearing probe mode is diagnostic-only and requires "
                "TDLib readiness, bounded live startup smoke, ingest DB-write "
                "approval, and valid smoke caps."
            )
            return ScriptResult(exit_code=1, report=report)

        bounds: CollectorSmokeBounds | None = None
        if approved_live_collector_startup_smoke:
            bounds = _validate_smoke_bounds(report)
            if bounds is None:
                return ScriptResult(exit_code=1, report=report)
            if approved_message_bearing_probe_mode:
                report["collector_message_bearing_probe_mode"] = True

        probe_failure = _run_tdlib_readiness_probe(
            report=report,
            values=values,
            tdlib_auth_max_updates=tdlib_auth_max_updates,
            tdlib_receive_timeout_sec=tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec=tdlib_overall_timeout_sec,
            tdlib_readiness_probe_factory=tdlib_readiness_probe_factory,
        )
        if probe_failure is not None:
            return probe_failure

        if (
            approved_live_collector_startup_smoke
            and approved_collector_ingest_db_write
            and not approved_tdlib_readiness_probe
        ):
            _set_status(
                report,
                "blocked_approval_required",
                "approval.tdlib_readiness_probe_required",
            )
            report["operator_next_action"] = (
                "Bounded write-capable collector smoke requires explicit TDLib "
                "readiness probe approval and a ready existing session."
            )
            return ScriptResult(exit_code=1, report=report)

        if approved_live_collector_startup_smoke and not approved_collector_ingest_db_write:
            _set_status(
                report,
                "joined_channel_collector_bounded_startup_smoke_no_write_ready",
            )
            report["collector_smoke_updates_observed_bucket"] = "zero"
            report["collector_smoke_no_updates_observed"] = True
            report["operator_next_action"] = (
                "Startup smoke approval and caps are present, but ingest DB-write "
                "approval is absent. This run stops at no-write startup eligibility "
                "and does not start any writer runtime path."
            )
            return ScriptResult(exit_code=0, report=report)

        if approved_live_collector_startup_smoke and approved_collector_ingest_db_write:
            assert bounds is not None
            try:
                factory = (
                    collector_smoke_runner_factory
                    or _default_collector_smoke_runner_factory
                )
                if collector_smoke_runner_factory is None:
                    try:
                        runner = _default_collector_smoke_runner_factory(
                            values,
                            message_bearing_probe_mode=(
                                approved_message_bearing_probe_mode
                            ),
                        )
                    except TypeError:
                        if approved_message_bearing_probe_mode:
                            raise
                        runner = _default_collector_smoke_runner_factory(values)
                else:
                    runner = factory(values)
            except Exception:
                _set_status(
                    report,
                    "blocked_collector_smoke_runner_unavailable",
                    "collector_smoke.runner_unavailable",
                )
                return ScriptResult(exit_code=1, report=report)

            report["collector_smoke_attempted"] = True
            try:
                smoke_result = asyncio.run(
                    _run_smoke_runner(
                        runner,
                        runtime_env=values,
                        bounds=bounds,
                    )
                )
            except Exception as exc:
                partial_result = _partial_smoke_result_from_exception(exc)
                if partial_result is not None:
                    _merge_smoke_result(report, partial_result)
                    if _smoke_forbidden_side_effect_detected(
                        report,
                        partial_result,
                        bounds,
                    ):
                        _set_status(
                            report,
                            "blocked_forbidden_side_effect_detected",
                            "collector_smoke.forbidden_side_effect",
                        )
                    else:
                        _set_status(
                            report,
                            _blocked_smoke_status(partial_result),
                            "collector_smoke.runner_failed",
                        )
                    report["operator_next_action"] = (
                        "The bounded smoke failed after partial startup. Review "
                        "side-effect flags and write buckets before any retry."
                    )
                    return ScriptResult(exit_code=1, report=report)
                _set_status(
                    report,
                    "blocked_collector_smoke_runner_failed",
                    "collector_smoke.runner_failed",
                )
                return ScriptResult(exit_code=1, report=report)

            _merge_smoke_result(report, smoke_result)
            if _smoke_result_is_failure(smoke_result):
                _set_status(
                    report,
                    _blocked_smoke_status(smoke_result),
                    "collector_smoke.runner_failed",
                )
                report["operator_next_action"] = (
                    "The bounded smoke returned a fail-closed result. Review "
                    "side-effect flags and write buckets before any retry."
                )
                return ScriptResult(exit_code=1, report=report)
            if _smoke_forbidden_side_effect_detected(report, smoke_result, bounds):
                _set_status(
                    report,
                    "blocked_forbidden_side_effect_detected",
                    "collector_smoke.forbidden_side_effect",
                )
                report["operator_next_action"] = (
                    "The bounded smoke reported a forbidden side effect or a write "
                    "outside collector-owned ingest tables. Treat the run as blocked."
                )
                return ScriptResult(exit_code=1, report=report)
            if _smoke_inconsistent_observation(report):
                _set_status(
                    report,
                    "blocked_collector_smoke_inconsistent_observation",
                    "collector_smoke.inconsistent_observation",
                )
                report["operator_next_action"] = (
                    "The bounded smoke reported canonical source/version/outbox "
                    "writes without any sanitized message-bearing update type. Treat "
                    "the observation as inconsistent before any wider startup."
                )
                return ScriptResult(exit_code=1, report=report)

            if approved_message_bearing_probe_mode:
                if smoke_result.updates_observed <= 0 or not report[
                    "collector_smoke_message_bearing_updates_observed"
                ]:
                    _set_status(
                        report,
                        "joined_channel_collector_bounded_startup_message_bearing_probe_no_message_updates_observed",
                    )
                    report["operator_next_action"] = (
                        "Diagnostic message-bearing probe completed without "
                        "sanitized message-bearing updates. Control/state/reconcile "
                        "updates were counted and skipped before raw update writes."
                    )
                elif report["collector_smoke_canonical_ingest_writes_observed"]:
                    _set_status(
                        report,
                        "joined_channel_collector_bounded_startup_message_bearing_probe_message_ingest_writes_observed",
                    )
                    report["operator_next_action"] = (
                        "Diagnostic message-bearing probe observed message-bearing "
                        "updates and canonical source/version/outbox writes under caps."
                    )
                else:
                    _set_status(
                        report,
                        "joined_channel_collector_bounded_startup_message_bearing_probe_message_updates_observed_no_canonical_writes",
                    )
                    report["operator_next_action"] = (
                        "Diagnostic message-bearing probe observed or dispatched "
                        "message-bearing updates, but canonical source/version/outbox "
                        "writes stayed at zero."
                    )
            elif smoke_result.updates_observed <= 0:
                _set_status(
                    report,
                    "joined_channel_collector_bounded_startup_no_updates_observed",
                )
                report["operator_next_action"] = (
                    "The bounded collector smoke completed without observing live "
                    "updates. This is not a failure by itself; review side-effect "
                    "flags before any wider startup."
                )
            elif (
                report["collector_smoke_canonical_ingest_writes_observed"]
                and report["collector_smoke_message_bearing_updates_observed"]
            ):
                _set_status(
                    report,
                    "joined_channel_collector_bounded_startup_message_ingest_writes_observed",
                )
                report["operator_next_action"] = (
                    "The bounded smoke observed canonical source/version/outbox "
                    "ingest writes under caps. Review sanitized update-type and "
                    "write buckets before any wider collector startup."
                )
            elif report["collector_smoke_raw_only_writes_observed"]:
                _set_status(
                    report,
                    "joined_channel_collector_bounded_startup_raw_update_writes_observed",
                )
                report["operator_next_action"] = (
                    "The bounded smoke observed raw update journal writes under "
                    "caps, but no canonical source/version/outbox ingest writes. "
                    "Canonical message ingest is not proven by this run."
                )
            else:
                _set_status(
                    report,
                    "joined_channel_collector_bounded_startup_updates_observed_no_writes",
                )
                report["operator_next_action"] = (
                    "The bounded smoke observed live updates but no ingest writes. "
                    "Review the collector runner before widening startup."
                )
            return ScriptResult(exit_code=0, report=report)

        if approved_tdlib_readiness_probe:
            return ScriptResult(exit_code=0, report=report)

        _set_status(report, "joined_channel_collector_bounded_startup_ingest_gate_ready")
        report["operator_next_action"] = (
            "Joined rows, runtime env readability, DB reachability, required "
            "tables, collector imports, config, and singleton lock path are "
            "gate-ready. No TDLib, collector startup, Redis, Docker/systemd, "
            "Alembic, or DB mutation was performed."
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
        approved_live_collector_startup_smoke=(
            args.approved_live_collector_startup_smoke
        ),
        approved_collector_ingest_db_write=args.approved_collector_ingest_db_write,
        approved_message_bearing_probe_mode=args.approved_message_bearing_probe_mode,
        joined_row_limit=args.joined_row_limit,
        collector_smoke_max_duration_sec=args.collector_smoke_max_duration_sec,
        collector_smoke_max_updates=args.collector_smoke_max_updates,
        collector_smoke_max_db_writes=args.collector_smoke_max_db_writes,
        tdlib_auth_max_updates=args.tdlib_auth_max_updates,
        tdlib_receive_timeout_sec=args.tdlib_receive_timeout_sec,
        tdlib_overall_timeout_sec=args.tdlib_overall_timeout_sec,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
