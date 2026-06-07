from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from urllib.parse import SplitResult, quote, unquote, urlsplit, urlunsplit


SCHEMA_VERSION = "local_test_db_fixture_provisioning_v1"
REQUIRED_TABLES = (
    "source_messages",
    "source_message_versions",
    "event_outbox",
    "normalization_runs",
    "artifact_registry",
    "artifact_observations",
    "candidate_group_proposals",
    "candidate_group_members",
)
LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "::1"}
SUPPORTED_SCHEMES = {"postgresql", "postgresql+psycopg", "postgresql+psycopg2"}
SAFE_TARGET_MARKERS = ("test", "local", "dev")
FORBIDDEN_TARGET_MARKERS = ("prod", "production", "live")
FORBIDDEN_TARGET_DATABASE_NAMES = {
    "default",
    "github_ai_catchbot",
    "main",
    "postgres",
    "template0",
    "template1",
}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


@dataclass(frozen=True, slots=True)
class ParsedDatabaseUrl:
    raw_url: str
    split: SplitResult
    scheme: str
    hostname: str
    database_name: str
    username: str | None
    password_present: bool


@dataclass(frozen=True, slots=True)
class MigrationInfra:
    present: bool
    alembic_ini: Path
    migrations_dir: Path
    versions_dir: Path


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]
    stdout_text: str | None = None


class LocalTestDatabaseExecutor(Protocol):
    def ensure_database(self, admin_database_url: str, target_database_name: str, target_owner: str | None) -> bool: ...
    def apply_existing_migrations(self, target_database_url: str, repo_root: Path) -> None: ...
    def fetch_present_tables(self, target_database_url: str, required_tables: Sequence[str]) -> set[str]: ...


class SqlAlchemyLocalTestDatabaseExecutor:
    def ensure_database(self, admin_database_url: str, target_database_name: str, target_owner: str | None) -> bool:
        sqlalchemy = _import_sqlalchemy()
        engine = sqlalchemy.create_engine(admin_database_url, future=True, isolation_level="AUTOCOMMIT")
        try:
            with engine.connect() as connection:
                exists = connection.execute(
                    sqlalchemy.text("SELECT 1 FROM pg_database WHERE datname = :database_name"),
                    {"database_name": target_database_name},
                ).scalar()
                if exists:
                    return False

                preparer = engine.dialect.identifier_preparer
                statement = f"CREATE DATABASE {preparer.quote_identifier(target_database_name)}"
                if target_owner:
                    statement += f" OWNER {preparer.quote_identifier(target_owner)}"
                connection.exec_driver_sql(statement)
                return True
        finally:
            engine.dispose()

    def apply_existing_migrations(self, target_database_url: str, repo_root: Path) -> None:
        env = dict(os.environ)
        env["DATABASE_URL"] = target_database_url
        completed = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=repo_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if completed.returncode != 0:
            raise RuntimeError("existing_migration_execution_failed")

    def fetch_present_tables(self, target_database_url: str, required_tables: Sequence[str]) -> set[str]:
        sqlalchemy = _import_sqlalchemy()
        sql, params = build_required_schema_check_sql(required_tables)
        engine = sqlalchemy.create_engine(target_database_url, future=True)
        try:
            with engine.connect() as connection:
                rows = connection.execute(sqlalchemy.text(sql), params).fetchall()
                return {str(row[0]) for row in rows}
        finally:
            engine.dispose()


def _import_sqlalchemy():
    import sqlalchemy

    return sqlalchemy


def redact_database_url(database_url: str | None) -> str:
    if not database_url:
        return ""
    try:
        parsed = urlsplit(database_url)
    except ValueError:
        return "<redacted-database-url>"

    if not parsed.scheme:
        return "<redacted-database-url>"

    userinfo = ""
    if parsed.username:
        userinfo = quote(unquote(parsed.username), safe="") + ":<redacted>@"
    elif "@" in parsed.netloc:
        userinfo = "<redacted>@"

    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parsed.port}" if parsed.port is not None else ""
    netloc = f"{userinfo}{host}{port}"
    query = _redact_query(parsed.query)

    if not parsed.netloc:
        return _urlunsplit_preserving_empty_netloc(parsed.scheme, "", parsed.path, query)

    return urlunsplit((parsed.scheme, netloc, parsed.path, query, ""))


def parse_database_url_for_guard(database_url: str) -> ParsedDatabaseUrl:
    value = database_url.strip()
    if not value:
        raise ValueError("admin_database_url_required")

    try:
        split = urlsplit(value)
        hostname = split.hostname or ""
        username = unquote(split.username) if split.username else None
        database_name = unquote(split.path.lstrip("/").split("/", 1)[0]) if split.path else ""
    except ValueError as exc:
        raise ValueError("admin_database_url_parse_failed") from exc

    return ParsedDatabaseUrl(
        raw_url=value,
        split=split,
        scheme=split.scheme.lower(),
        hostname=hostname.lower(),
        database_name=database_name,
        username=username,
        password_present=split.password is not None,
    )


def validate_local_admin_database_url(database_url: str | None) -> tuple[bool, list[str], ParsedDatabaseUrl | None]:
    if database_url is None or not database_url.strip():
        return False, ["admin_database_url_required"], None

    try:
        parsed = parse_database_url_for_guard(database_url)
    except ValueError as exc:
        return False, [str(exc)], None

    failures: list[str] = []
    if parsed.scheme not in SUPPORTED_SCHEMES:
        failures.append("admin_database_url_unsupported_scheme")
    if parsed.hostname not in LOCAL_HOSTS:
        failures.append("admin_database_url_remote_host_rejected")
    query_host = _query_host(parsed.split.query)
    if query_host and not _query_host_is_local(query_host):
        failures.append("admin_database_url_remote_query_host_rejected")
    if not parsed.database_name:
        failures.append("admin_database_url_database_name_required")

    return not failures, failures, parsed


def validate_target_database_name(target_database_name: str | None) -> tuple[bool, list[str]]:
    value = (target_database_name or "").strip()
    failures: list[str] = []
    if not value:
        return False, ["target_database_name_required"]

    normalized = value.lower()
    if not IDENTIFIER_RE.fullmatch(value):
        failures.append("target_database_name_unsafe_identifier")
    if not any(marker in normalized for marker in SAFE_TARGET_MARKERS):
        failures.append("target_database_name_missing_local_test_marker")
    if any(marker in normalized for marker in FORBIDDEN_TARGET_MARKERS):
        failures.append("target_database_name_forbidden_environment_marker")
    if normalized in FORBIDDEN_TARGET_DATABASE_NAMES:
        failures.append("target_database_name_forbidden_reserved_name")
    return not failures, failures


def build_target_database_url_redacted(admin_database_url: str, target_database_name: str) -> str:
    return redact_database_url(_replace_database_name(admin_database_url, target_database_name))


def discover_migration_infra(repo_root: Path | None = None) -> MigrationInfra:
    root = repo_root or _repo_root()
    alembic_ini = root / "alembic.ini"
    migrations_dir = root / "migrations"
    versions_dir = migrations_dir / "versions"
    present = (
        alembic_ini.is_file()
        and migrations_dir.is_dir()
        and (migrations_dir / "env.py").is_file()
        and versions_dir.is_dir()
        and any(versions_dir.glob("*.py"))
    )
    return MigrationInfra(
        present=present,
        alembic_ini=alembic_ini,
        migrations_dir=migrations_dir,
        versions_dir=versions_dir,
    )


def build_required_schema_check_sql(required_tables: Sequence[str] = REQUIRED_TABLES) -> tuple[str, dict[str, str]]:
    placeholders: list[str] = []
    params: dict[str, str] = {}
    for index, table_name in enumerate(required_tables):
        key = f"table_{index}"
        placeholders.append(f":{key}")
        params[key] = table_name
    sql = (
        "SELECT table_name\n"
        "FROM information_schema.tables\n"
        "WHERE table_schema = current_schema()\n"
        f"  AND table_name IN ({', '.join(placeholders)})\n"
        "ORDER BY table_name"
    )
    return sql, params


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Guard and provision a local PostgreSQL test database fixture for later DB-backed replay."
    )
    parser.add_argument("mode", choices=("check", "provision", "emit-env"))
    parser.add_argument("--admin-database-url")
    parser.add_argument("--target-database-name", required=True)
    parser.add_argument("--target-owner")
    parser.add_argument("--confirm-local-test-db-provisioning", action="store_true")
    parser.add_argument("--apply-existing-migrations", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    executor: LocalTestDatabaseExecutor | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env or os.environ
    root = repo_root or _repo_root()
    base_report = _base_report(args.mode)
    checks_failed: list[str] = []

    app_env_ok, app_env_failures = _validate_app_env(effective_env)
    base_report["app_env_guard_passed"] = app_env_ok
    checks_failed.extend(app_env_failures)

    target_ok, target_failures = validate_target_database_name(args.target_database_name)
    base_report["target_database_name_guard_passed"] = target_ok
    checks_failed.extend(target_failures)

    admin_ok = True
    parsed_admin: ParsedDatabaseUrl | None = None
    if args.mode in {"check", "provision"} or args.admin_database_url:
        admin_ok, admin_failures, parsed_admin = validate_local_admin_database_url(args.admin_database_url)
        checks_failed.extend(admin_failures)
    base_report["admin_database_url_guard_passed"] = admin_ok

    target_owner = _resolve_target_owner(args.target_owner)
    if target_owner.failure:
        checks_failed.append(target_owner.failure)

    if args.mode == "check":
        return _finish(base_report, checks_failed)

    if args.mode == "emit-env":
        report = dict(base_report)
        report["mode"] = "emit-env"
        if parsed_admin and target_ok:
            report["target_database_url_redacted"] = build_target_database_url_redacted(
                parsed_admin.raw_url,
                args.target_database_name,
            )
        else:
            report["target_database_url_redacted"] = "<redacted-local-postgresql-url>"
        report["local_test_database_url_ready"] = not checks_failed
        report["shell_guidance"] = _emit_env_guidance(report["target_database_url_redacted"])
        result = _finish(report, checks_failed)
        if args.json:
            return result
        return RunnerResult(
            exit_code=result.exit_code,
            report=result.report,
            stdout_text="\n".join(result.report["shell_guidance"]) + "\n",
        )

    if args.mode != "provision":
        checks_failed.append("unsupported_mode")
        return _finish(base_report, checks_failed)

    report = dict(base_report)
    report.update(
        {
            "database_created_or_already_exists": False,
            "migration_infra_present": False,
            "migrations_applied_or_already_current": False,
            "required_schema_verified": False,
            "required_tables_present": [],
            "local_test_database_url_ready": False,
        }
    )

    migration_infra = discover_migration_infra(root)
    report["migration_infra_present"] = migration_infra.present
    if args.apply_existing_migrations and not migration_infra.present:
        checks_failed.append("migration_infra_missing")

    if not args.confirm_local_test_db_provisioning:
        checks_failed.append("confirm_local_test_db_provisioning_required")

    if checks_failed:
        return _finish(report, checks_failed)

    if parsed_admin is None:
        checks_failed.append("admin_database_url_required")
        return _finish(report, checks_failed)

    target_database_url = _replace_database_name(parsed_admin.raw_url, args.target_database_name)

    if args.dry_run:
        report["database_mutation_attempted"] = False
        report["migration_mutation_attempted"] = False
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyLocalTestDatabaseExecutor()
    try:
        report["database_mutation_attempted"] = True
        active_executor.ensure_database(parsed_admin.raw_url, args.target_database_name, target_owner.value)
        report["database_created_or_already_exists"] = True

        if args.apply_existing_migrations:
            report["migration_mutation_attempted"] = True
            active_executor.apply_existing_migrations(target_database_url, root)
            report["migrations_applied_or_already_current"] = True

        present_tables = active_executor.fetch_present_tables(target_database_url, REQUIRED_TABLES)
    except Exception as exc:  # noqa: BLE001 - keep operator-facing output sanitized.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    required_present = [table for table in REQUIRED_TABLES if table in present_tables]
    missing_required = [table for table in REQUIRED_TABLES if table not in present_tables]
    report["required_tables_present"] = required_present
    report["required_schema_verified"] = not missing_required
    if missing_required:
        checks_failed.extend(f"required_table_missing:{table}" for table in missing_required)
    else:
        report["migrations_applied_or_already_current"] = True
        report["local_test_database_url_ready"] = True

    return _finish(report, checks_failed)


@dataclass(frozen=True, slots=True)
class _ResolvedOwner:
    value: str | None
    failure: str | None = None


def _resolve_target_owner(target_owner: str | None) -> _ResolvedOwner:
    value = (target_owner or "").strip()
    if not value:
        try:
            value = getpass.getuser().strip()
        except Exception:  # noqa: BLE001 - owner is optional when it cannot be derived.
            return _ResolvedOwner(value=None)
    if not value:
        return _ResolvedOwner(value=None)
    if not IDENTIFIER_RE.fullmatch(value):
        return _ResolvedOwner(value=None, failure="target_owner_unsafe_identifier")
    return _ResolvedOwner(value=value)


def _base_report(mode: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "mode": mode,
        "app_env_guard_passed": False,
        "admin_database_url_guard_passed": False,
        "target_database_name_guard_passed": False,
        "local_only": True,
        "production_db_write": False,
        "database_mutation_attempted": False,
        "migration_mutation_attempted": False,
        "raw_url_echoed": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _validate_app_env(env: Mapping[str, str]) -> tuple[bool, list[str]]:
    app_env = env.get("APP_ENV", "").strip().lower()
    if app_env in {"prod", "production", "live"}:
        return False, ["app_env_production_rejected"]
    return True, []


def _replace_database_name(database_url: str, target_database_name: str) -> str:
    split = urlsplit(database_url)
    path = "/" + quote(target_database_name, safe="")
    return _urlunsplit_preserving_empty_netloc(split.scheme, split.netloc, path, split.query)


def _urlunsplit_preserving_empty_netloc(scheme: str, netloc: str, path: str, query: str) -> str:
    """Preserve SQLAlchemy/psycopg socket URLs such as postgresql:///db?host=/var/run/postgresql.

    urllib.parse.urlunsplit collapses an empty-netloc URL into `scheme:/path`,
    which SQLAlchemy rejects for PostgreSQL URLs. For local Unix-socket URLs,
    the triple-slash form is intentional and must be preserved.
    """

    if netloc:
        return urlunsplit((scheme, netloc, path, query, ""))
    normalized_path = path if path.startswith("/") else f"/{path}"
    suffix = f"?{query}" if query else ""
    return f"{scheme}://{normalized_path}{suffix}"


def _redact_query(query: str) -> str:
    if not query:
        return ""
    redacted_parts: list[str] = []
    for part in query.split("&"):
        key = part.split("=", 1)[0]
        if key.lower() in {"password", "pass", "sslpassword"}:
            redacted_parts.append(f"{key}=<redacted>")
        else:
            redacted_parts.append(part)
    return "&".join(redacted_parts)


def _query_host(query: str) -> str | None:
    for part in query.split("&"):
        key, _, value = part.partition("=")
        if key.lower() == "host" and value:
            return unquote(value)
    return None


def _query_host_is_local(query_host: str) -> bool:
    normalized = query_host.strip().lower()
    return normalized.startswith("/") or normalized in LOCAL_HOSTS


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in {
        "existing_migration_execution_failed",
    }:
        return message
    return exc.__class__.__name__


def _emit_env_guidance(target_database_url_redacted: str) -> list[str]:
    return [
        "Set LOCAL_TEST_DATABASE_URL only in the invoking shell for this local fixture run.",
        f"export LOCAL_TEST_DATABASE_URL='{target_database_url_redacted}'",
        "Do not write LOCAL_TEST_DATABASE_URL to .env files or shared runtime configuration.",
    ]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    if result.stdout_text is not None:
        sys.stdout.write(result.stdout_text)
    else:
        sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
