from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence
from uuid import UUID


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_outbox_relay_source_message_route_readiness_probe"
REPORT_TYPE = "outbox_relay_source_message_route_readiness_probe_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_EVENTS = 3
MAX_EVENTS_HARD_LIMIT = 10
EXPECTED_QUEUE_NAME = "q.source.normalize"
EXPECTED_STAGE_NAME = "normalize"
SOURCE_MESSAGE_EVENT_TYPES = (
    "source_message.created.v1",
    "source_message.edited.v1",
    "source_message.deleted.v1",
    "source_message.reconciled.v1",
)
ALLOWED_REDIS_THIN_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}
FORBIDDEN_REDIS_FIELDS = {
    "payload_json",
    "raw_message_json",
    "raw_message_text",
    "text_body",
    "caption_text",
    "message_text",
    "database_url",
    "redis_url",
    "logical_post_key",
}
SIDE_EFFECT_REPORT_FIELDS = (
    "event_outbox_status_mutation_performed",
    "redis_mutation_performed",
    "source_tables_mutation_performed",
    "downstream_service_started",
    "docker_or_systemd_changed",
    "alembic_run",
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
COUNT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE status = 'pending'::outbox_status_enum
  AND event_type = ANY(CAST(:event_types AS text[]))
"""
SELECT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY = """
SELECT
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    fail_count,
    created_at
FROM event_outbox
WHERE status = 'pending'::outbox_status_enum
  AND event_type = ANY(CAST(:event_types AS text[]))
ORDER BY created_at ASC, event_id ASC
LIMIT :limit
"""
SELECT_SOURCE_MESSAGE_EXISTS_QUERY = """
SELECT EXISTS (
    SELECT 1
    FROM source_messages
    WHERE source_message_id = CAST(:source_message_id AS uuid)
)
"""
SELECT_SOURCE_MESSAGE_VERSION_EXISTS_QUERY = """
SELECT EXISTS (
    SELECT 1
    FROM source_message_versions
    WHERE source_message_id = CAST(:source_message_id AS uuid)
)
"""

REQUIRED_TABLES = ("event_outbox", "source_messages", "source_message_versions")


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

from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage  # noqa: E402
from src.services.outbox_relay.routing import (  # noqa: E402
    OutboxRouteResolver,
    UnsupportedOutboxEventTypeError,
)


def _bounded_positive_int_named(field_name: str, *, upper_bound: int) -> Callable[[str], int]:
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a positive integer"
            ) from exc
        if value <= 0 or value > upper_bound:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be between 1 and {upper_bound}"
            )
        return value

    return parse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "No-write outbox-relay readiness probe for pending source_message.* "
            "event_outbox rows. It validates deterministic q.source.normalize "
            "routing and Redis thin message shape without publishing or updating "
            "event_outbox."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--max-events",
        type=_bounded_positive_int_named("max-events", upper_bound=MAX_EVENTS_HARD_LIMIT),
        default=DEFAULT_MAX_EVENTS,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": "blocked_outbox_relay_source_message_route_readiness_failed",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "read_only_transaction": False,
        "pending_source_outbox_events_bucket": "zero",
        "selected_outbox_events_bucket": "zero",
        "supported_source_events_bucket": "zero",
        "unsupported_events_bucket": "zero",
        "route_q_source_normalize_bucket": "zero",
        "route_stage_normalize_bucket": "zero",
        "redis_thin_payload_shape_valid_bucket": "zero",
        "redis_payload_includes_large_json": False,
        "source_message_rehydrate_attempted": False,
        "source_message_rehydrate_succeeded_bucket": "zero",
        "source_version_rehydrate_succeeded_bucket": "zero",
        "event_outbox_status_mutation_performed": False,
        "redis_mutation_performed": False,
        "source_tables_mutation_performed": False,
        "downstream_service_started": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "raw_values_emitted": False,
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _read_runtime_env(
    path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
) -> Mapping[str, str]:
    if runtime_env_reader is not None:
        return runtime_env_reader(path)
    return parse_runtime_env_file(path)


def _database_url_is_supported(database_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not scheme_match:
        return False
    scheme = scheme_match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.strip().split())


def _allowed_read_statements() -> set[str]:
    return {
        _normalize_sql(statement)
        for statement in (
            SET_TRANSACTION_READ_ONLY_QUERY,
            SHOW_TRANSACTION_READ_ONLY_QUERY,
            SELECT_ONE_QUERY,
            TABLE_AVAILABLE_QUERY,
            COUNT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY,
            SELECT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY,
            SELECT_SOURCE_MESSAGE_EXISTS_QUERY,
            SELECT_SOURCE_MESSAGE_VERSION_EXISTS_QUERY,
        )
    }


def _assert_read_sql(statement: str) -> None:
    if _normalize_sql(statement) not in _allowed_read_statements():
        raise ValueError("SQL statement is not in the outbox route readiness read allowlist")


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


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


def _check_required_tables(connection: DatabaseConnection) -> bool:
    for table in REQUIRED_TABLES:
        available = bool(
            _scalar(
                _execute_read(
                    connection,
                    TABLE_AVAILABLE_QUERY,
                    {"qualified_table_name": f"public.{table}"},
                )
            )
        )
        if not available:
            return False
    return True


def _count_pending_source_outbox_events(connection: DatabaseConnection) -> int:
    value = _scalar(
        _execute_read(
            connection,
            COUNT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY,
            {"event_types": list(SOURCE_MESSAGE_EVENT_TYPES)},
        )
    )
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _select_pending_source_outbox_events(
    connection: DatabaseConnection,
    *,
    limit: int,
) -> list[OutboxEventRow]:
    result = _execute_read(
        connection,
        SELECT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY,
        {"event_types": list(SOURCE_MESSAGE_EVENT_TYPES), "limit": limit},
    )
    rows: list[OutboxEventRow] = []
    for raw_row in _rows(result):
        row = _mapping_from_row(raw_row)
        payload = row.get("payload_json") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        rows.append(
            OutboxEventRow(
                event_id=_coerce_uuid(row["event_id"]),
                event_type=str(row["event_type"]),
                aggregate_type=str(row["aggregate_type"]),
                aggregate_id=_coerce_uuid(row["aggregate_id"]),
                dedupe_key=str(row["dedupe_key"]),
                payload_json=payload if isinstance(payload, dict) else {},
                status=str(row["status"]),
                fail_count=int(row.get("fail_count") or 0),
                created_at=row.get("created_at") or datetime.now(timezone.utc),
            )
        )
    return rows


def _mapping_from_row(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    if hasattr(row, "_mapping"):
        return row._mapping
    raise TypeError("database row is not mapping-like")


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _source_message_exists(connection: DatabaseConnection, source_message_id: UUID) -> bool:
    return bool(
        _scalar(
            _execute_read(
                connection,
                SELECT_SOURCE_MESSAGE_EXISTS_QUERY,
                {"source_message_id": str(source_message_id)},
            )
        )
    )


def _source_message_version_exists(connection: DatabaseConnection, source_message_id: UUID) -> bool:
    return bool(
        _scalar(
            _execute_read(
                connection,
                SELECT_SOURCE_MESSAGE_VERSION_EXISTS_QUERY,
                {"source_message_id": str(source_message_id)},
            )
        )
    )


def build_redis_thin_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=str(row.event_id),
    )


def validate_redis_thin_payload_shape(fields: Mapping[str, Any]) -> tuple[bool, bool]:
    keys = set(fields)
    exact_field_set = keys == ALLOWED_REDIS_THIN_FIELDS
    forbidden_key_present = any(_is_forbidden_redis_field(key) for key in keys)
    large_json_included = "payload_json" in keys or forbidden_key_present
    return exact_field_set and not large_json_included, large_json_included


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return lowered in FORBIDDEN_REDIS_FIELDS or any(
        token in lowered
        for token in (
            "payload",
            "raw",
            "text",
            "caption",
            "database_url",
            "redis_url",
            "secret",
            "token",
            "password",
            "api_key",
            "apikey",
        )
    )


def _raw_values_from_rows(rows: Sequence[OutboxEventRow]) -> set[str]:
    values: set[str] = set()
    for row in rows:
        values.update(
            {
                str(row.event_id),
                str(row.aggregate_id),
                row.dedupe_key,
            }
        )
        payload_text = json.dumps(row.payload_json, sort_keys=True, default=str)
        values.add(payload_text)
        for key in (
            "source_message_id",
            "aggregate_id",
            "dedupe_key",
            "logical_post_key",
            "text_body",
            "caption_text",
            "text_surface",
            "message_text",
            "database_url",
            "redis_url",
        ):
            value = row.payload_json.get(key)
            if isinstance(value, str) and value:
                values.add(value)
    return {value for value in values if len(value) >= 6}


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values)


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    if not side_effect_flags:
        return
    for field in SIDE_EFFECT_REPORT_FIELDS:
        if bool(side_effect_flags.get(field, False)):
            report[field] = True


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    return any(bool(report[field]) for field in SIDE_EFFECT_REPORT_FIELDS) or bool(
        report["raw_values_emitted"]
    )


def _evaluate_selected_rows(
    *,
    report: dict[str, Any],
    rows: Sequence[OutboxEventRow],
    connection: DatabaseConnection,
    route_resolver: Any,
) -> set[str]:
    supported_count = 0
    unsupported_count = 0
    route_queue_ok_count = 0
    route_stage_ok_count = 0
    payload_shape_ok_count = 0
    source_message_rehydrate_count = 0
    source_version_rehydrate_count = 0
    contract_mismatch = False
    readiness_failed = False
    raw_values = _raw_values_from_rows(rows)

    report["selected_outbox_events_bucket"] = _bucket_count(len(rows))
    report["source_message_rehydrate_attempted"] = bool(rows)

    for row in rows:
        if row.event_type in SOURCE_MESSAGE_EVENT_TYPES:
            supported_count += 1
        else:
            unsupported_count += 1
            contract_mismatch = True
            continue

        try:
            route = route_resolver.resolve(row)
        except UnsupportedOutboxEventTypeError:
            unsupported_count += 1
            contract_mismatch = True
            continue

        if route.queue_name == EXPECTED_QUEUE_NAME:
            route_queue_ok_count += 1
        else:
            contract_mismatch = True

        if route.stage_name == EXPECTED_STAGE_NAME:
            route_stage_ok_count += 1
        else:
            contract_mismatch = True

        fields = build_redis_thin_message(row, route).as_stream_fields()
        shape_valid, includes_large_json = validate_redis_thin_payload_shape(fields)
        if includes_large_json:
            report["redis_payload_includes_large_json"] = True
        if shape_valid:
            payload_shape_ok_count += 1
        else:
            contract_mismatch = True

        if _source_message_exists(connection, row.aggregate_id):
            source_message_rehydrate_count += 1
        else:
            readiness_failed = True

        if _source_message_version_exists(connection, row.aggregate_id):
            source_version_rehydrate_count += 1
        else:
            readiness_failed = True

    report["supported_source_events_bucket"] = _bucket_count(supported_count)
    report["unsupported_events_bucket"] = _bucket_count(unsupported_count)
    report["route_q_source_normalize_bucket"] = _bucket_count(route_queue_ok_count)
    report["route_stage_normalize_bucket"] = _bucket_count(route_stage_ok_count)
    report["redis_thin_payload_shape_valid_bucket"] = _bucket_count(payload_shape_ok_count)
    report["source_message_rehydrate_succeeded_bucket"] = _bucket_count(
        source_message_rehydrate_count
    )
    report["source_version_rehydrate_succeeded_bucket"] = _bucket_count(
        source_version_rehydrate_count
    )

    if contract_mismatch:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_route_contract_mismatch",
            "outbox_relay.source_message_route_contract",
        )
    elif readiness_failed:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_route_readiness_failed",
            "source_message.rehydrate",
        )
    else:
        _set_status(report, "outbox_relay_source_message_route_readiness_ready")

    return raw_values


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_events: int = DEFAULT_MAX_EVENTS,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(
            report,
            "blocked_forbidden_side_effect_detected",
            "side_effect.forbidden",
        )
        return ScriptResult(exit_code=1, report=report)

    if max_events <= 0 or max_events > MAX_EVENTS_HARD_LIMIT:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_route_readiness_failed",
            "max_events.out_of_bounds",
        )
        return ScriptResult(exit_code=1, report=report)

    connection: DatabaseConnection | None = None
    cleanup: Callable[[], None] | None = None
    transaction: Any | None = None
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
        except Exception:
            _set_status(
                report,
                "blocked_outbox_relay_source_message_route_readiness_failed",
                "runtime_env.read",
            )
            return ScriptResult(exit_code=1, report=report)

        database_url = str(values.get("DATABASE_URL", "")).strip()
        if database_url:
            raw_values.add(database_url)
        redis_url = str(values.get("REDIS_URL", "")).strip()
        if redis_url:
            raw_values.add(redis_url)
        if not database_url:
            _set_status(
                report,
                "blocked_outbox_relay_source_message_route_readiness_failed",
                "database.url_missing",
            )
            return ScriptResult(exit_code=1, report=report)
        if not _database_url_is_supported(database_url):
            _set_status(
                report,
                "blocked_outbox_relay_source_message_route_readiness_failed",
                "database.url_unsupported",
            )
            return ScriptResult(exit_code=1, report=report)

        try:
            connection, cleanup = _open_database_connection(
                database_url,
                database_connection_factory,
            )
            transaction = connection.begin()
            _execute_read(connection, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only_value = _scalar(
                _execute_read(connection, SHOW_TRANSACTION_READ_ONLY_QUERY)
            )
            report["read_only_transaction"] = _transaction_read_only_enabled(
                read_only_value
            )
            _execute_read(connection, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not report["read_only_transaction"]:
                _set_status(
                    report,
                    "blocked_outbox_relay_source_message_route_readiness_failed",
                    "database.read_only_transaction",
                )
                return ScriptResult(exit_code=1, report=report)
            if not _check_required_tables(connection):
                _set_status(
                    report,
                    "blocked_outbox_relay_source_message_route_readiness_failed",
                    "database.required_tables",
                )
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_outbox_relay_source_message_route_readiness_failed",
                "database.connection_or_schema",
            )
            return ScriptResult(exit_code=1, report=report)

        pending_count = _count_pending_source_outbox_events(connection)
        report["pending_source_outbox_events_bucket"] = _bucket_count(pending_count)
        if pending_count <= 0:
            _set_status(
                report,
                "outbox_relay_source_message_route_readiness_no_pending_events",
            )
            return ScriptResult(exit_code=0, report=report)

        rows = _select_pending_source_outbox_events(connection, limit=max_events)
        raw_values.update(_raw_values_from_rows(rows))
        resolver = route_resolver or OutboxRouteResolver()
        raw_values.update(
            _evaluate_selected_rows(
                report=report,
                rows=rows,
                connection=connection,
                route_resolver=resolver,
            )
        )
        raw_values.add(str(runtime_env_path))
        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(
                report,
                "blocked_forbidden_side_effect_detected",
                "output.raw_values",
            )
            return ScriptResult(exit_code=1, report=report)
        if _forbidden_side_effect_detected(report):
            _set_status(
                report,
                "blocked_forbidden_side_effect_detected",
                "side_effect.forbidden",
            )
            return ScriptResult(exit_code=1, report=report)
        return ScriptResult(
            exit_code=0 if not report["contract_status"].startswith("blocked_") else 1,
            report=report,
        )
    except Exception:
        _set_status(
            report,
            "blocked_outbox_relay_source_message_route_readiness_failed",
            "unexpected",
        )
        return ScriptResult(exit_code=1, report=report)
    finally:
        _rollback_transaction(transaction)
        _close_connection(cleanup, connection)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        max_events=args.max_events,
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
