from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol, Sequence


REPORT_TYPE = "dedicated_vps_post_migration_db_acceptance_smoke_result_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
EXPECTED_TERMINAL_REVISION = "0004_judge_delivery_obs"

KEY_TABLES = (
    "source_messages",
    "event_outbox",
    "artifact_registry",
    "candidate_group_proposals",
    "candidate_evidence_bundles",
    "judge_runs",
    "judge_outputs",
    "analyses",
    "notification_plans",
    "notification_delivery_records",
    "job_attempts",
    "state_transitions",
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


class DbConnection(Protocol):
    def begin(self) -> Any: ...

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RuntimeEnvParseResult:
    values: dict[str, str]
    metadata: dict[str, Any]


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

    def __call__(self, database_url: str) -> DbConnection:
        sqlalchemy = __import__("sqlalchemy")
        self._engine = sqlalchemy.create_engine(database_url, future=True)
        return SqlAlchemyConnection(self._engine.connect(), sqlalchemy.text)

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import dedicated_vps_post_migration_db_acceptance_smoke_check as smoke_check


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the separately approved read-only post-migration DB acceptance smoke. "
            "Without --approved-read-only-db-smoke this tool reads no runtime env file "
            "and opens no DB connection."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--approved-read-only-db-smoke", action="store_true")
    return parser


def default_repo_root() -> Path:
    return ROOT


def _base_report(repo_root: Path, runtime_env_path: str) -> dict[str, Any]:
    return {
        "report_type": REPORT_TYPE,
        "contract_status": "approval_required",
        "checks_failed": [],
        "failures": [],
        "warnings": [],
        "repo_root": str(repo_root),
        "runtime_env_path": runtime_env_path,
        "runtime_env_read": False,
        "runtime_env_values_printed": False,
        "database_url_printed": False,
        "secret_values_printed": False,
        "database_connected": False,
        "db_write_performed": False,
        "redis_connected": False,
        "redis_mutation_performed": False,
        "alembic_run": False,
        "alembic_upgrade_run": False,
        "alembic_downgrade_run": False,
        "alembic_stamp_run": False,
        "alembic_revision_run": False,
        "app_runtime_started": False,
        "tdlib_auth_performed": False,
        "telegram_connected": False,
        "live_collector_started": False,
        "notifier_transport_enabled": False,
        "production_rollout_performed": False,
        "docker_used": False,
        "systemd_modified": False,
        "migration_files_modified": False,
        "expected_terminal_revision": EXPECTED_TERMINAL_REVISION,
        "observed_alembic_versions": [],
        "migration_files_inspected": [],
        "derived_revision_ids": [],
        "expected_table_count": 0,
        "present_table_count": 0,
        "missing_tables": [],
        "key_tables_queried": [],
        "key_table_query_failures": [],
        "index_check_summary": {"expected": 0, "present": 0, "missing": []},
        "constraint_check_summary": {"expected": 0, "present": 0, "missing": []},
        "read_only_transaction_requested": False,
        "read_only_transaction_confirmed": False,
    }


def _failure(report: dict[str, Any], check: str, message: str) -> None:
    report["checks_failed"].append(check)
    report["failures"].append({"check": check, "message": message})


def _warning(report: dict[str, Any], message: str) -> None:
    report["warnings"].append(message)


def _database_url_secret_fragments(database_url: str | None) -> list[str]:
    if not database_url:
        return []

    fragments = [database_url]
    authority_match = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://([^/@]+)@", database_url)
    if not authority_match:
        return fragments

    userinfo = authority_match.group(1)
    fragments.append(userinfo)
    if ":" in userinfo:
        password = userinfo.rsplit(":", 1)[1]
        if password:
            fragments.append(password)
    return fragments


def _redact_sensitive_text(
    text: str,
    *,
    database_url: str | None = None,
    extra_sensitive_values: Iterable[str] = (),
) -> str:
    redacted = text
    for value in sorted(
        {fragment for fragment in (*_database_url_secret_fragments(database_url), *extra_sensitive_values) if fragment},
        key=len,
        reverse=True,
    ):
        redacted = "<redacted>".join(redacted.split(value))

    redacted = re.sub(
        r"(?i)\b(DATABASE_URL\s*=\s*)([^\s\"']+)",
        r"\1<redacted>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)\b(postgresql(?:\+[A-Za-z0-9_.-]+)?://)([^\s\"'@]+)@([^\s\"']+)",
        r"\1<redacted>@\3",
        redacted,
    )
    redacted = re.sub(
        r"(?i)\b(password\s*[=:]\s*)([^\s,;\"']+)",
        r"\1<redacted>",
        redacted,
    )
    return redacted


def _load_migration_facts(report: dict[str, Any], repo_root: Path) -> Any:
    facts = smoke_check.derive_migration_facts(repo_root)
    report["migration_files_inspected"] = facts.migration_files
    report["derived_revision_ids"] = facts.revision_ids
    report["expected_table_count"] = len(facts.tables)
    report["index_check_summary"] = {"expected": len(facts.indexes), "present": 0, "missing": []}
    report["constraint_check_summary"] = {"expected": len(facts.constraints), "present": 0, "missing": []}
    return facts


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def parse_runtime_env_file(path: str | Path) -> RuntimeEnvParseResult:
    runtime_env_path = Path(path)
    values: dict[str, str] = {}
    metadata: dict[str, Any] = {
        "path": str(runtime_env_path),
        "keys_present": [],
        "database_url_present": False,
        "database_url_scheme": None,
        "database_url_has_credentials": False,
    }

    text = runtime_env_path.read_text(encoding="utf-8", errors="replace")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values[key] = _strip_optional_quotes(raw_value)

    metadata["keys_present"] = sorted(values)
    database_url = values.get("DATABASE_URL")
    metadata["database_url_present"] = database_url is not None
    if database_url:
        scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
        scheme = scheme_match.group(1).lower() if scheme_match else None
        metadata["database_url_scheme"] = scheme
        metadata["database_url_has_credentials"] = bool(re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://[^/@]+:[^/@]+@", database_url))
    return RuntimeEnvParseResult(values=values, metadata=metadata)


def _database_url_is_supported(database_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not scheme_match:
        return False
    scheme = scheme_match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _identifier(name: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return '"' + name + '"'


def _execute(connection: DbConnection, statement: str, params: dict[str, Any] | None = None) -> Any:
    upper_statement = statement.upper()
    for verb in FORBIDDEN_SQL_VERBS:
        if re.search(rf"\b{verb}\b", upper_statement):
            raise ValueError(f"forbidden SQL verb detected: {verb}")
    allowed = upper_statement.startswith("SELECT ") or upper_statement.startswith("SHOW ") or upper_statement == "SET TRANSACTION READ ONLY"
    if not allowed:
        raise ValueError("SQL statement is not in the read-only allowlist")
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


def _string_values(rows: Iterable[Any]) -> list[str]:
    return sorted(str(value) for row in rows if (value := _first_cell(row)) is not None)


def _read_only_confirmed(value: Any) -> bool:
    return str(value).strip().lower() in {"on", "true", "1", "yes"}


def _open_default_connection(database_url: str) -> tuple[DbConnection, Callable[[], None]]:
    factory = SqlAlchemyConnectionFactory()
    connection = factory(database_url)

    def cleanup() -> None:
        connection.close()
        factory.dispose()

    return connection, cleanup


def _run_db_checks(
    report: dict[str, Any],
    database_url: str,
    facts: Any,
    connect_factory: Callable[[str], DbConnection] | None,
) -> None:
    connection: DbConnection | None = None
    cleanup: Callable[[], None] | None = None
    transaction: Any | None = None
    try:
        if connect_factory is None:
            connection, cleanup = _open_default_connection(database_url)
        else:
            connection = connect_factory(database_url)
            cleanup = getattr(connection, "close", lambda: None)
        report["database_connected"] = True

        transaction = connection.begin()
        _execute(connection, "SET TRANSACTION READ ONLY")
        report["read_only_transaction_requested"] = True
        try:
            report["read_only_transaction_confirmed"] = _read_only_confirmed(
                _scalar(_execute(connection, "SHOW transaction_read_only"))
            )
            if not report["read_only_transaction_confirmed"]:
                _failure(report, "db.read_only_transaction_confirmed", "Read-only transaction could not be confirmed.")
        except Exception as exc:
            _warning(
                report,
                "Read-only transaction confirmation was unavailable: "
                f"{_redact_sensitive_text(str(exc), database_url=database_url)}",
            )

        alembic_exists = bool(_scalar(_execute(connection, "SELECT to_regclass(:qualified_table_name) IS NOT NULL", {"qualified_table_name": "public.alembic_version"})))
        if not alembic_exists:
            _failure(report, "db.alembic_version_table", "alembic_version table is missing.")
        else:
            observed_versions = _string_values(_rows(_execute(connection, "SELECT version_num FROM alembic_version")))
            report["observed_alembic_versions"] = observed_versions
            if observed_versions != [EXPECTED_TERMINAL_REVISION]:
                _failure(
                    report,
                    "db.alembic_terminal_revision",
                    f"Expected exactly {EXPECTED_TERMINAL_REVISION} in alembic_version.",
                )

        present_tables: list[str] = []
        for table in facts.tables:
            exists = bool(_scalar(_execute(connection, "SELECT to_regclass(:qualified_table_name) IS NOT NULL", {"qualified_table_name": f"public.{table}"})))
            if exists:
                present_tables.append(table)
        report["present_table_count"] = len(present_tables)
        report["missing_tables"] = [table for table in facts.tables if table not in set(present_tables)]
        if report["missing_tables"]:
            _failure(report, "db.expected_tables_present", "One or more migration-derived expected tables are missing.")

        for table in KEY_TABLES:
            try:
                _execute(connection, f"SELECT COUNT(*) FROM {_identifier(table)}")
                report["key_tables_queried"].append(table)
            except Exception as exc:
                report["key_table_query_failures"].append(
                    {"table": table, "message": _redact_sensitive_text(str(exc), database_url=database_url)}
                )
        if report["key_table_query_failures"]:
            _failure(report, "db.key_tables_queryable", "One or more key tables could not be queried read-only.")

        present_indexes = set(
            _string_values(
                _rows(
                    _execute(
                        connection,
                        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
                    )
                )
            )
        )
        missing_indexes = [index for index in facts.indexes if index not in present_indexes]
        report["index_check_summary"] = {
            "expected": len(facts.indexes),
            "present": len([index for index in facts.indexes if index in present_indexes]),
            "missing": missing_indexes,
        }
        if missing_indexes:
            _warning(report, "One or more migration-derived indexes were not observed.")

        present_constraints = set(
            _string_values(
                _rows(
                    _execute(
                        connection,
                        "SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = 'public'",
                    )
                )
            )
        )
        missing_constraints = [constraint for constraint in facts.constraints if constraint not in present_constraints]
        report["constraint_check_summary"] = {
            "expected": len(facts.constraints),
            "present": len([constraint for constraint in facts.constraints if constraint in present_constraints]),
            "missing": missing_constraints,
        }
        if missing_constraints:
            _warning(report, "One or more migration-derived constraints were not observed.")
    except Exception as exc:
        _failure(report, "db.read_only_smoke_execution", _redact_sensitive_text(str(exc), database_url=database_url))
    finally:
        if transaction is not None and hasattr(transaction, "rollback"):
            transaction.rollback()
        if cleanup is not None:
            cleanup()


def generate_report(
    repo_root: str | Path | None = None,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approved_read_only_db_smoke: bool = False,
    connect_factory: Callable[[str], DbConnection] | None = None,
) -> RunnerResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    report = _base_report(resolved_repo_root, str(runtime_env_path))
    facts = _load_migration_facts(report, resolved_repo_root)

    if EXPECTED_TERMINAL_REVISION not in facts.revision_ids:
        _failure(report, "migrations.terminal_revision", f"Expected migration revision {EXPECTED_TERMINAL_REVISION} was not derived.")

    if not approved_read_only_db_smoke:
        _failure(
            report,
            "approval.required",
            "Pass --approved-read-only-db-smoke only after separate operator approval for read-only VPS DB smoke execution.",
        )
        return RunnerResult(exit_code=2, report=report)

    report["runtime_env_read"] = True
    try:
        parsed_env = parse_runtime_env_file(runtime_env_path)
    except Exception as exc:
        _failure(report, "runtime_env.read", f"Unable to read runtime env file: {_redact_sensitive_text(str(exc))}")
        report["contract_status"] = "failed"
        return RunnerResult(exit_code=1, report=report)

    database_url = parsed_env.values.get("DATABASE_URL")
    report["runtime_env_metadata"] = parsed_env.metadata
    if not database_url:
        _failure(report, "runtime_env.database_url_required", "DATABASE_URL is required.")
    elif not _database_url_is_supported(database_url):
        _failure(report, "runtime_env.database_url_scheme", "DATABASE_URL must use a PostgreSQL URL scheme.")

    if not report["checks_failed"] and database_url:
        _run_db_checks(report, database_url, facts, connect_factory)

    report["contract_status"] = "failed" if report["checks_failed"] else "passed"
    return RunnerResult(exit_code=1 if report["checks_failed"] else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        repo_root=args.repo_root,
        runtime_env_path=args.runtime_env_path,
        approved_read_only_db_smoke=args.approved_read_only_db_smoke,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
