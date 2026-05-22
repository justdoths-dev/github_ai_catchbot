from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_telegram_channel_registry_seed_operator"
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

ACCEPTED_SOURCE_KIND_BUCKET_KEYS = ("public_username", "invite_link", "chat_id")

TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_ONE_QUERY = "SELECT 1"
EXISTING_REGISTRY_ROW_QUERY = (
    "SELECT 1 FROM telegram_channel_registry "
    "WHERE desired_state <> 'removed' "
    "AND source_kind = :source_kind "
    "AND source_value = :source_value "
    "LIMIT 1"
)
INSERT_PUBLIC_USERNAME_QUERY = """
INSERT INTO telegram_channel_registry (
  source_kind,
  source_value,
  desired_state,
  access_state,
  chat_id,
  username_snapshot,
  title_snapshot,
  chat_type,
  priority_weight,
  notes
)
VALUES (
  :source_kind,
  :source_value,
  :desired_state,
  'unresolved',
  NULL,
  NULL,
  NULL,
  NULL,
  :priority_weight,
  :notes
)
ON CONFLICT (source_kind, source_value) WHERE desired_state <> 'removed'
DO NOTHING
RETURNING registry_id
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


@dataclass(frozen=True, slots=True)
class SeedRow:
    source_kind: str
    source_value: str
    desired_state: str
    priority_weight: int
    notes: str


@dataclass(frozen=True, slots=True)
class InputValidation:
    rows: tuple[SeedRow, ...]
    row_count: int
    rejected_count: int
    duplicate_count: int
    checks_failed: tuple[str, ...]


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
            "Seed telegram_channel_registry rows from a local/VPS-only JSONL file. "
            "The default mode validates and plans only; DB mutation requires the "
            "explicit approved registry seed mutation flag."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--input-jsonl", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--approved-registry-seed-mutation",
        action="store_true",
        help="Allow INSERT-only telegram_channel_registry seed mutation.",
    )
    return parser


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


def _empty_source_kind_buckets() -> dict[str, str]:
    return {key: "zero" for key in ACCEPTED_SOURCE_KIND_BUCKET_KEYS}


def _base_report(
    *,
    dry_run: bool,
    approved_registry_seed_mutation: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "input_file_read": False,
        "input_rows_validated": False,
        "input_row_count_bucket": "unknown",
        "accepted_source_kind_buckets": _empty_source_kind_buckets(),
        "rejected_row_count_bucket": "unknown",
        "duplicate_input_count_bucket": "unknown",
        "existing_row_count_bucket": "unknown",
        "inserted_row_count_bucket": "zero",
        "skipped_existing_count_bucket": "unknown",
        "dry_run": dry_run,
        "approved_registry_seed_mutation": approved_registry_seed_mutation,
        "seed_mutation_performed": False,
        "operator_next_action": (
            "Fix runtime env or input access locally on the VPS; do not paste "
            "runtime.env values, channel identifiers, invite links, chat IDs, "
            "phone numbers, or Telegram secrets into ChatGPT."
        ),
        "side_effects": _side_effects(),
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.strip().split())


def _allowed_select_statements() -> set[str]:
    return {
        _normalize_sql(statement)
        for statement in (
            SELECT_ONE_QUERY,
            TABLE_AVAILABLE_QUERY,
            EXISTING_REGISTRY_ROW_QUERY,
        )
    }


def _assert_select_sql(statement: str) -> None:
    if _normalize_sql(statement) not in _allowed_select_statements():
        raise ValueError("SQL statement is not in the registry seed select allowlist")


def _assert_insert_sql(statement: str) -> None:
    normalized = _normalize_sql(statement)
    if normalized != _normalize_sql(INSERT_PUBLIC_USERNAME_QUERY):
        raise ValueError("SQL statement is not in the registry seed insert allowlist")


def _execute_select(
    connection: DatabaseConnection,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    _assert_select_sql(statement)
    return connection.execute(statement, params or {})


def _execute_insert(
    connection: DatabaseConnection,
    statement: str,
    params: dict[str, Any],
) -> Any:
    _assert_insert_sql(statement)
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
    return first


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
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


def _looks_suspicious(value: str) -> bool:
    lowered = value.strip().lower()
    if "=" in lowered:
        return True
    return any(fragment in lowered for fragment in SUSPICIOUS_VALUE_FRAGMENTS)


def _normalize_public_username(raw_value: Any) -> tuple[str | None, str | None]:
    if not isinstance(raw_value, str):
        return None, "source_value_invalid"
    if raw_value != raw_value.strip() or re.search(r"\s", raw_value):
        return None, "source_value_invalid"
    if _looks_suspicious(raw_value):
        return None, "source_value_suspicious"

    value = raw_value
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = value[len(prefix) :]
            break
    if value.startswith("@"):
        value = value[1:]

    if not value:
        return None, "source_value_empty"
    if "/" in value:
        return None, "source_value_path_not_supported"
    if _looks_suspicious(value):
        return None, "source_value_suspicious"
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", value):
        return None, "source_value_invalid"
    return value, None


def _validate_seed_row(row: Any, row_number: int) -> tuple[SeedRow | None, str | None]:
    if not isinstance(row, dict):
        return None, f"input.row_{row_number}.not_object"

    source_kind = row.get("source_kind")
    if source_kind != "public_username":
        if source_kind in {"invite_link", "chat_id"}:
            return None, f"input.row_{row_number}.source_kind_not_supported"
        return None, f"input.row_{row_number}.source_kind_invalid"

    source_value, error = _normalize_public_username(row.get("source_value"))
    if error is not None or source_value is None:
        return None, f"input.row_{row_number}.{error or 'source_value_invalid'}"

    desired_state = row.get("desired_state", "active")
    if desired_state != "active":
        return None, f"input.row_{row_number}.desired_state_not_supported"

    priority_weight = row.get("priority_weight", 100)
    if isinstance(priority_weight, bool) or not isinstance(priority_weight, int):
        return None, f"input.row_{row_number}.priority_weight_invalid"

    notes = row.get("notes") or "seeded_by_operator"
    if not isinstance(notes, str):
        return None, f"input.row_{row_number}.notes_invalid"

    return (
        SeedRow(
            source_kind="public_username",
            source_value=source_value,
            desired_state=desired_state,
            priority_weight=priority_weight,
            notes=notes,
        ),
        None,
    )


def _read_and_validate_input(path: str | Path) -> InputValidation:
    text = Path(path).read_text(encoding="utf-8")
    rows: list[SeedRow] = []
    checks_failed: list[str] = []
    seen: set[tuple[str, str]] = set()
    duplicate_count = 0
    row_count = 0

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        row_count += 1
        try:
            raw_row = json.loads(raw_line)
        except json.JSONDecodeError:
            checks_failed.append(f"input.row_{line_number}.invalid_json")
            continue

        seed_row, error = _validate_seed_row(raw_row, line_number)
        if error is not None or seed_row is None:
            checks_failed.append(error or f"input.row_{line_number}.invalid")
            continue

        key = (seed_row.source_kind, seed_row.source_value)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        rows.append(seed_row)

    if row_count == 0:
        checks_failed.append("input.no_rows")
    return InputValidation(
        rows=tuple(rows),
        row_count=row_count,
        rejected_count=len(checks_failed),
        duplicate_count=duplicate_count,
        checks_failed=tuple(checks_failed),
    )


def _apply_input_validation(report: dict[str, Any], validation: InputValidation) -> None:
    source_kind_counts = Counter(row.source_kind for row in validation.rows)
    report["input_file_read"] = True
    report["input_row_count_bucket"] = _bucket_count(validation.row_count)
    report["accepted_source_kind_buckets"] = {
        key: _bucket_count(source_kind_counts.get(key, 0))
        for key in ACCEPTED_SOURCE_KIND_BUCKET_KEYS
    }
    report["rejected_row_count_bucket"] = _bucket_count(validation.rejected_count)
    report["duplicate_input_count_bucket"] = _bucket_count(validation.duplicate_count)


def _registry_row_exists(connection: DatabaseConnection, row: SeedRow) -> bool:
    return bool(
        _scalar(
            _execute_select(
                connection,
                EXISTING_REGISTRY_ROW_QUERY,
                {
                    "source_kind": row.source_kind,
                    "source_value": row.source_value,
                },
            )
        )
    )


def _insert_seed_row(connection: DatabaseConnection, row: SeedRow) -> bool:
    inserted_marker = _scalar(
        _execute_insert(
            connection,
            INSERT_PUBLIC_USERNAME_QUERY,
            {
                "source_kind": row.source_kind,
                "source_value": row.source_value,
                "desired_state": row.desired_state,
                "priority_weight": row.priority_weight,
                "notes": row.notes,
            },
        )
    )
    return inserted_marker is not None


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


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    input_jsonl_path: str | Path | None = None,
    dry_run: bool = True,
    approved_registry_seed_mutation: bool = False,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
) -> ScriptResult:
    effective_dry_run = bool(dry_run or not approved_registry_seed_mutation)
    report = _base_report(
        dry_run=effective_dry_run,
        approved_registry_seed_mutation=approved_registry_seed_mutation,
    )

    try:
        values = _read_runtime_env(runtime_env_path, runtime_env_reader)
    except Exception:
        _set_status(report, "blocked_runtime_env_unreadable", "runtime_env.unreadable")
        return ScriptResult(exit_code=1, report=report)
    report["runtime_env_read"] = True

    if input_jsonl_path is None:
        _set_status(report, "blocked_input_file_unreadable", "input_file.missing")
        return ScriptResult(exit_code=1, report=report)

    try:
        validation = _read_and_validate_input(input_jsonl_path)
    except Exception:
        _set_status(report, "blocked_input_file_unreadable", "input_file.unreadable")
        return ScriptResult(exit_code=1, report=report)

    _apply_input_validation(report, validation)
    if validation.checks_failed:
        report["checks_failed"].extend(validation.checks_failed)
        _set_status(report, "blocked_input_validation_failed")
        return ScriptResult(exit_code=1, report=report)
    report["input_rows_validated"] = True

    if not effective_dry_run and not approved_registry_seed_mutation:
        _set_status(report, "blocked_approval_required", "approval.required")
        return ScriptResult(exit_code=1, report=report)

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
    try:
        try:
            connection, cleanup = _open_database_connection(
                database_url,
                database_connection_factory,
            )
            transaction = connection.begin()
            _execute_select(connection, SELECT_ONE_QUERY)
            report["database_connected"] = True
            table_available = bool(
                _scalar(
                    _execute_select(
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

        existing_rows = [row for row in validation.rows if _registry_row_exists(connection, row)]
        rows_to_insert = [row for row in validation.rows if row not in existing_rows]
        report["existing_row_count_bucket"] = _bucket_count(len(existing_rows))
        report["skipped_existing_count_bucket"] = _bucket_count(len(existing_rows))

        if effective_dry_run:
            _set_status(report, "dry_run_seed_plan_validated")
            report["operator_next_action"] = (
                "Review the local seed input and dry-run buckets. Re-run on the VPS "
                "with --approved-registry-seed-mutation only after operator approval."
            )
            return ScriptResult(exit_code=0, report=report)

        inserted_count = 0
        insert_attempt_count = 0
        for row in rows_to_insert:
            insert_attempt_count += 1
            if _insert_seed_row(connection, row):
                inserted_count += 1

        report["inserted_row_count_bucket"] = _bucket_count(inserted_count)
        report["side_effects"]["database_mutation_performed"] = insert_attempt_count > 0
        report["side_effects"]["telegram_channel_registry_inserted"] = inserted_count > 0
        report["seed_mutation_performed"] = inserted_count > 0

        if inserted_count > 0:
            _set_status(report, "registry_seed_inserted")
            report["operator_next_action"] = (
                "Registry seed rows were inserted. Do not start the live collector "
                "from this script; use the separately approved readiness gates next."
            )
        else:
            _set_status(report, "registry_seed_noop_all_existing")
            report["operator_next_action"] = (
                "No new registry seed rows were inserted. Existing active rows or "
                "safe conflict handling covered the input."
            )

        _commit_transaction(transaction)
        transaction_committed = True
        return ScriptResult(exit_code=0, report=report)
    except Exception:
        _set_status(report, "blocked_unexpected_error", "unexpected_error")
        return ScriptResult(exit_code=1, report=report)
    finally:
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
        input_jsonl_path=args.input_jsonl,
        dry_run=args.dry_run,
        approved_registry_seed_mutation=args.approved_registry_seed_mutation,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
