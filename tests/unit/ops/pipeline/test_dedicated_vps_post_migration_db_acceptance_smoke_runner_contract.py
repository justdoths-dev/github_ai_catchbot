from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_post_migration_db_acceptance_smoke_runner.py"
FAKE_PASSWORD = "fake-db-password-for-redaction-test"
FAKE_DATABASE_URL = f"postgresql+psycopg://catchbot:{FAKE_PASSWORD}@127.0.0.1:5432/github_ai_catchbot"


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None, scalar: Any = None) -> None:
        self._rows = rows or []
        self._scalar = scalar

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class FakeTransaction:
    def __init__(self) -> None:
        self.rolled_back = False

    def rollback(self) -> None:
        self.rolled_back = True


class FakeConnection:
    def __init__(
        self,
        *,
        versions: list[str] | None = None,
        present_tables: set[str] | None = None,
        indexes: set[str] | None = None,
        constraints: set[str] | None = None,
        failing_key_tables: set[str] | None = None,
        key_table_failure_message: str | None = None,
    ) -> None:
        module = _module()
        facts = module.smoke_check.derive_migration_facts(ROOT)
        self.versions = versions if versions is not None else [module.EXPECTED_TERMINAL_REVISION]
        self.present_tables = present_tables if present_tables is not None else set(facts.tables)
        self.indexes = indexes if indexes is not None else set(facts.indexes)
        self.constraints = constraints if constraints is not None else set(facts.constraints)
        self.failing_key_tables = failing_key_tables or set()
        self.key_table_failure_message = key_table_failure_message
        self.statements: list[str] = []
        self.transaction = FakeTransaction()
        self.closed = False

    def begin(self) -> FakeTransaction:
        return self.transaction

    def close(self) -> None:
        self.closed = True

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        self.statements.append(statement)
        normalized = " ".join(statement.split())
        if normalized == "SET TRANSACTION READ ONLY":
            return FakeResult()
        if normalized == "SHOW transaction_read_only":
            return FakeResult(scalar="on")
        if normalized == "SELECT version_num FROM alembic_version":
            return FakeResult(rows=[(version,) for version in self.versions])
        if normalized == "SELECT to_regclass(:qualified_table_name) IS NOT NULL":
            qualified_name = (params or {}).get("qualified_table_name", "")
            table = str(qualified_name).split(".")[-1]
            return FakeResult(scalar=table == "alembic_version" or table in self.present_tables)
        if normalized.startswith("SELECT COUNT(*) FROM "):
            table = normalized.rsplit(" ", 1)[-1].strip('"')
            if table in self.failing_key_tables:
                raise RuntimeError(self.key_table_failure_message or f"forced query failure for {table}")
            return FakeResult(scalar=0)
        if normalized == "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'":
            return FakeResult(rows=[(index,) for index in sorted(self.indexes)])
        if normalized == "SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = 'public'":
            return FakeResult(rows=[(constraint,) for constraint in sorted(self.constraints)])
        raise AssertionError(f"unexpected SQL: {statement}")


def _module():
    from scripts.ops import dedicated_vps_post_migration_db_acceptance_smoke_runner as module

    return module


def _runtime_env(tmp_path: Path, database_url: str | None = None) -> Path:
    value = database_url or FAKE_DATABASE_URL
    path = tmp_path / "runtime.env"
    path.write_text(
        "\n".join(
            [
                "# fixture only",
                f"DATABASE_URL={value}",
                "REDIS_URL=redis://127.0.0.1:6379/0",
                "UNRELATED_KEY=ignored",
            ]
        ),
        encoding="utf-8",
    )
    return path


def test_runner_imports() -> None:
    module = _module()

    assert module.REPORT_TYPE == "dedicated_vps_post_migration_db_acceptance_smoke_result_v1"
    assert callable(module.main)


def test_format_json_without_approval_is_parseable_and_does_not_read_runtime_env(tmp_path: Path) -> None:
    missing_runtime_env = tmp_path / "missing-runtime.env"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(missing_runtime_env),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "approval_required"
    assert "approval.required" in report["checks_failed"]
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["redis_connected"] is False
    assert report["alembic_run"] is False
    assert report["app_runtime_started"] is False


def test_no_approval_output_does_not_expose_fake_secret_from_unread_fixture(tmp_path: Path) -> None:
    runtime_env = _runtime_env(tmp_path)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json", "--runtime-env-path", str(runtime_env)],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    assert FAKE_PASSWORD not in result.stdout
    assert "postgresql+psycopg://catchbot" not in result.stdout
    report = json.loads(result.stdout)
    assert report["runtime_env_read"] is False


def test_runtime_env_parser_returns_metadata_without_exposing_secret_values(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)

    parsed = module.parse_runtime_env_file(runtime_env)

    assert parsed.values["DATABASE_URL"].endswith("/github_ai_catchbot")
    assert parsed.metadata["database_url_present"] is True
    assert parsed.metadata["database_url_scheme"] == "postgresql+psycopg"
    assert FAKE_PASSWORD not in json.dumps(parsed.metadata)


def test_no_approval_side_effect_flags_remain_false(tmp_path: Path) -> None:
    module = _module()
    result = module.generate_report(runtime_env_path=tmp_path / "missing-runtime.env")
    report = result.report

    for key in (
        "runtime_env_read",
        "database_connected",
        "db_write_performed",
        "redis_connected",
        "redis_mutation_performed",
        "alembic_run",
        "alembic_upgrade_run",
        "alembic_downgrade_run",
        "alembic_stamp_run",
        "alembic_revision_run",
        "app_runtime_started",
        "tdlib_auth_performed",
        "telegram_connected",
        "live_collector_started",
        "notifier_transport_enabled",
        "production_rollout_performed",
        "docker_used",
        "systemd_modified",
        "migration_files_modified",
        "runtime_env_values_printed",
        "database_url_printed",
        "secret_values_printed",
    ):
        assert report[key] is False


def test_migration_facts_are_derived_from_real_migration_files() -> None:
    module = _module()
    result = module.generate_report(ROOT)
    report = result.report

    assert report["migration_files_inspected"]
    assert "0004_judge_delivery_obs" in report["derived_revision_ids"]
    assert report["expected_table_count"] >= len(module.KEY_TABLES)


def test_approved_path_passes_with_fake_db_connection(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)
    fake = FakeConnection()

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: fake,
    )

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == "passed"
    report_json = json.dumps(report)
    assert FAKE_PASSWORD not in report_json
    assert "postgresql+psycopg://catchbot" not in report_json
    assert report["runtime_env_read"] is True
    assert report["database_connected"] is True
    assert report["observed_alembic_versions"] == ["0004_judge_delivery_obs"]
    assert report["missing_tables"] == []
    assert set(report["key_tables_queried"]) == set(module.KEY_TABLES)
    assert report["key_table_query_failures"] == []
    assert report["read_only_transaction_requested"] is True
    assert report["read_only_transaction_confirmed"] is True
    assert fake.transaction.rolled_back is True
    assert fake.closed is True


def test_approved_path_fails_when_alembic_version_mismatches(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)
    fake = FakeConnection(versions=["0003_enrichment_bundles"])

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: fake,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == "failed"
    assert "db.alembic_terminal_revision" in result.report["checks_failed"]


def test_approved_path_fails_when_expected_tables_are_missing(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)
    facts = module.smoke_check.derive_migration_facts(ROOT)
    present_tables = set(facts.tables)
    present_tables.remove("source_messages")
    fake = FakeConnection(present_tables=present_tables)

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: fake,
    )

    assert result.exit_code == 1
    assert "db.expected_tables_present" in result.report["checks_failed"]
    assert "source_messages" in result.report["missing_tables"]


def test_approved_path_fails_when_key_table_query_fails(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)
    fake = FakeConnection(failing_key_tables={"notification_plans"})

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: fake,
    )

    assert result.exit_code == 1
    assert "db.key_tables_queryable" in result.report["checks_failed"]
    assert result.report["key_table_query_failures"] == [
        {"table": "notification_plans", "message": "forced query failure for notification_plans"}
    ]


def test_approved_path_redacts_database_url_from_connection_failure(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)

    def raise_sensitive_failure(database_url: str) -> FakeConnection:
        raise RuntimeError(f"connection failed for {database_url}")

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=raise_sensitive_failure,
    )

    report_json = json.dumps(result.report)
    assert result.exit_code == 1
    assert "db.read_only_smoke_execution" in result.report["checks_failed"]
    assert FAKE_PASSWORD not in report_json
    assert FAKE_DATABASE_URL not in report_json
    assert result.report["failures"][-1]["message"] == "connection failed for <redacted>"


def test_approved_path_redacts_database_url_from_key_table_query_failure(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)
    fake = FakeConnection(
        failing_key_tables={"notification_plans"},
        key_table_failure_message=(
            f"query failed with password={FAKE_PASSWORD} "
            f"userinfo=catchbot:{FAKE_PASSWORD} and url={FAKE_DATABASE_URL}"
        ),
    )

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: fake,
    )

    failure_message = result.report["key_table_query_failures"][0]["message"]
    assert result.exit_code == 1
    assert "db.key_tables_queryable" in result.report["checks_failed"]
    assert FAKE_PASSWORD not in failure_message
    assert FAKE_DATABASE_URL not in failure_message
    assert failure_message == "query failed with password=<redacted> userinfo=<redacted> and url=<redacted>"


def test_runtime_env_rejects_missing_database_url(tmp_path: Path) -> None:
    module = _module()
    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text("REDIS_URL=redis://127.0.0.1:6379/0\n", encoding="utf-8")

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: FakeConnection(),
    )

    assert result.exit_code == 1
    assert "runtime_env.database_url_required" in result.report["checks_failed"]
    assert result.report["database_connected"] is False


def test_runtime_env_rejects_non_postgresql_database_url(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path, database_url="mysql://user:password@127.0.0.1/db")

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: FakeConnection(),
    )

    assert result.exit_code == 1
    assert "runtime_env.database_url_scheme" in result.report["checks_failed"]
    assert result.report["database_connected"] is False


def test_sql_statements_executed_by_fake_are_read_only_only(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)
    fake = FakeConnection()

    module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_read_only_db_smoke=True,
        connect_factory=lambda _database_url: fake,
    )

    for statement in fake.statements:
        normalized = statement.upper()
        assert normalized.startswith(("SELECT ", "SHOW ", "SET TRANSACTION READ ONLY"))
        for verb in module.FORBIDDEN_SQL_VERBS:
            assert verb not in normalized


def test_runner_static_safety_contract() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    forbidden_import_roots = {
        "subprocess",
        "redis",
        "httpx",
        "requests",
        "urllib",
        "socket",
        "dotenv",
        "os",
    }
    imported_roots: set[str] = set()
    forbidden_write_calls: set[str] = set()
    open_write_modes: list[str] = []
    os_environ_references: list[str] = []
    shell_execution_calls: set[str] = set()
    mutating_sql_literals: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "os" and node.attr == "environ":
                os_environ_references.append("os.environ")
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in {
                "write_text",
                "write_bytes",
                "touch",
                "unlink",
                "mkdir",
                "rename",
                "replace",
            }:
                forbidden_write_calls.add(func.attr)
            if isinstance(func, ast.Name) and func.id == "open" and len(node.args) >= 2:
                mode_arg = node.args[1]
                if isinstance(mode_arg, ast.Constant) and isinstance(mode_arg.value, str):
                    if any(flag in mode_arg.value for flag in ("w", "a", "+")):
                        open_write_modes.append(mode_arg.value)
            if isinstance(func, ast.Attribute) and func.attr in {"run", "Popen", "call", "check_call", "check_output"}:
                shell_execution_calls.add(func.attr)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value.upper()
            if any(f" {verb} " in f" {value} " for verb in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER", "CREATE", "GRANT", "REVOKE", "VACUUM", "ANALYZE")):
                mutating_sql_literals.append(node.value)

    assert imported_roots.isdisjoint(forbidden_import_roots)
    assert os_environ_references == []
    assert forbidden_write_calls == set()
    assert open_write_modes == []
    assert shell_execution_calls == set()
    assert mutating_sql_literals == list(_module().FORBIDDEN_SQL_VERBS)


def test_runbook_documents_future_only_runner_boundaries() -> None:
    runbook = (ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_post_migration_db_acceptance_smoke.md").read_text(
        encoding="utf-8"
    )

    for phrase in (
        "Future/separately approved only",
        "--approved-read-only-db-smoke",
        "Codex must not execute this runner against the VPS DB",
        "must not `cat`, `source`, dot-source, or `export` values from",
        "The runner must not print `DATABASE_URL`, DB password, or secret values.",
        "The runner is read-only and performs no writes.",
        "The runner must not connect to Redis.",
        "The runner must not run Alembic.",
        "app runtime, TDLib, Telegram, live collector",
        "notifier transport, Docker, systemd, or production rollout",
        "Expected operator output is redacted JSON only.",
        "bring the redacted JSON back to ChatGPT",
    ):
        assert phrase in runbook
