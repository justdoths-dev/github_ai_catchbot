from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import local_test_db_fixture_provisioning_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SAFE_TARGET_DB = "github_ai_catchbot_test"
SECRET_VALUE = "local" + "_" + "secret"
SAFE_ADMIN_URL = "postgresql+psycopg://local_user:" + SECRET_VALUE + "@127.0.0.1:5432/postgres"


class FakeExecutor:
    def __init__(self, present_tables=None) -> None:
        self.calls: list[tuple] = []
        self.present_tables = set(present_tables or runner.REQUIRED_TABLES)

    def ensure_database(self, admin_database_url: str, target_database_name: str, target_owner: str | None) -> bool:
        self.calls.append(("ensure_database", admin_database_url, target_database_name, target_owner))
        return True

    def apply_existing_migrations(self, target_database_url: str, repo_root: Path) -> None:
        self.calls.append(("apply_existing_migrations", target_database_url, repo_root))

    def fetch_present_tables(self, target_database_url: str, required_tables) -> set[str]:
        self.calls.append(("fetch_present_tables", target_database_url, tuple(required_tables)))
        return self.present_tables


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, executor=None) -> runner.RunnerResult:
    return runner.run(_parse_args(*args), env=env or {"APP_ENV": "test"}, executor=executor, repo_root=ROOT)


def _run_cli(*args: str, app_env: str = "test") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "tools.local_test_db_fixture_provisioning_runner", *args],
        check=False,
        capture_output=True,
        cwd=ROOT,
        env={"APP_ENV": app_env, "PATH": ""},
        text=True,
        timeout=30,
    )


def test_check_mode_passes_for_local_admin_url_and_safe_target_db() -> None:
    result = _run(
        "check",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        SAFE_TARGET_DB,
        "--json",
    )

    assert result.exit_code == 0
    assert result.report == {
        "schema_version": "local_test_db_fixture_provisioning_v1",
        "status": "pass",
        "mode": "check",
        "app_env_guard_passed": True,
        "admin_database_url_guard_passed": True,
        "target_database_name_guard_passed": True,
        "local_only": True,
        "production_db_write": False,
        "database_mutation_attempted": False,
        "migration_mutation_attempted": False,
        "raw_url_echoed": False,
        "checks_failed": [],
    }


def test_rejects_app_env_prod() -> None:
    result = _run(
        "check",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        SAFE_TARGET_DB,
        env={"APP_ENV": "prod"},
    )

    assert result.exit_code == 1
    assert result.report["status"] == "fail"
    assert result.report["app_env_guard_passed"] is False
    assert result.report["checks_failed"] == ["app_env_production_rejected"]
    assert result.report["raw_url_echoed"] is False


def test_rejects_remote_host() -> None:
    result = _run(
        "check",
        "--admin-database-url",
        "postgresql+psycopg://user:secret@db.example.com:5432/postgres",
        "--target-database-name",
        SAFE_TARGET_DB,
    )

    assert result.exit_code == 1
    assert result.report["admin_database_url_guard_passed"] is False
    assert "admin_database_url_remote_host_rejected" in result.report["checks_failed"]
    assert result.report["raw_url_echoed"] is False


def test_rejects_github_ai_catchbot_target_database_name() -> None:
    result = _run(
        "check",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        "github_ai_catchbot",
    )

    assert result.exit_code == 1
    assert result.report["target_database_name_guard_passed"] is False
    assert "target_database_name_forbidden_reserved_name" in result.report["checks_failed"]
    assert result.report["raw_url_echoed"] is False


@pytest.mark.parametrize(
    "target_database_name",
    [
        "postgres",
        "template0",
        "template1",
        "default",
        "main",
        "github_ai_catchbot_prod_test",
        "github_ai_catchbot_live_local",
        "github_ai_catchbot_production_dev",
    ],
)
def test_rejects_prod_live_default_and_system_db_names(target_database_name: str) -> None:
    result = _run(
        "check",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        target_database_name,
    )

    assert result.exit_code == 1
    assert result.report["target_database_name_guard_passed"] is False
    assert result.report["checks_failed"]
    assert result.report["raw_url_echoed"] is False


def test_provision_without_confirmation_fails_before_any_mutation() -> None:
    executor = FakeExecutor()

    result = _run(
        "provision",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        SAFE_TARGET_DB,
        "--apply-existing-migrations",
        executor=executor,
    )

    assert result.exit_code == 1
    assert "confirm_local_test_db_provisioning_required" in result.report["checks_failed"]
    assert result.report["database_mutation_attempted"] is False
    assert result.report["migration_mutation_attempted"] is False
    assert executor.calls == []


def test_dry_run_does_not_execute_db_mutation() -> None:
    executor = FakeExecutor()

    result = _run(
        "provision",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        SAFE_TARGET_DB,
        "--confirm-local-test-db-provisioning",
        "--apply-existing-migrations",
        "--dry-run",
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report["status"] == "pass"
    assert result.report["database_mutation_attempted"] is False
    assert result.report["migration_mutation_attempted"] is False
    assert executor.calls == []


def test_provision_with_fake_injected_executor_calls_expected_operations_in_order() -> None:
    executor = FakeExecutor()

    result = _run(
        "provision",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        SAFE_TARGET_DB,
        "--target-owner",
        "local_owner",
        "--confirm-local-test-db-provisioning",
        "--apply-existing-migrations",
        "--no-dry-run",
        executor=executor,
    )

    assert result.exit_code == 0
    assert result.report["schema_version"] == "local_test_db_fixture_provisioning_v1"
    assert result.report["status"] == "pass"
    assert result.report["mode"] == "provision"
    assert result.report["database_created_or_already_exists"] is True
    assert result.report["migration_infra_present"] is True
    assert result.report["migrations_applied_or_already_current"] is True
    assert result.report["required_schema_verified"] is True
    assert result.report["required_tables_present"] == list(runner.REQUIRED_TABLES)
    assert result.report["local_test_database_url_ready"] is True
    assert result.report["raw_url_echoed"] is False
    assert result.report["checks_failed"] == []
    assert [call[0] for call in executor.calls] == [
        "ensure_database",
        "apply_existing_migrations",
        "fetch_present_tables",
    ]
    assert executor.calls[0] == ("ensure_database", SAFE_ADMIN_URL, SAFE_TARGET_DB, "local_owner")
    assert executor.calls[1][1] == SAFE_ADMIN_URL.replace("/postgres", f"/{SAFE_TARGET_DB}")
    assert executor.calls[2][1] == SAFE_ADMIN_URL.replace("/postgres", f"/{SAFE_TARGET_DB}")


def test_provision_apply_migrations_fails_safely_when_migration_infra_is_missing(tmp_path: Path) -> None:
    executor = FakeExecutor()
    args = _parse_args(
        "provision",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        SAFE_TARGET_DB,
        "--confirm-local-test-db-provisioning",
        "--apply-existing-migrations",
        "--no-dry-run",
    )

    result = runner.run(args, env={"APP_ENV": "test"}, executor=executor, repo_root=tmp_path)

    assert result.exit_code == 1
    assert result.report["status"] == "fail"
    assert result.report["migration_infra_present"] is False
    assert result.report["database_mutation_attempted"] is False
    assert result.report["migration_mutation_attempted"] is False
    assert result.report["checks_failed"] == ["migration_infra_missing"]
    assert executor.calls == []


def test_emit_env_never_prints_password_or_full_url() -> None:
    result = _run_cli(
        "emit-env",
        "--admin-database-url",
        SAFE_ADMIN_URL,
        "--target-database-name",
        SAFE_TARGET_DB,
    )

    assert result.returncode == 0
    assert "LOCAL_TEST_DATABASE_URL" in result.stdout
    assert SECRET_VALUE not in result.stdout
    assert SAFE_ADMIN_URL not in result.stdout
    assert SAFE_ADMIN_URL.replace("/postgres", f"/{SAFE_TARGET_DB}") not in result.stdout
    assert result.stderr == ""


@pytest.mark.parametrize(
    "mode_args",
    [
        (
            "check",
            "--admin-database-url",
            SAFE_ADMIN_URL,
            "--target-database-name",
            SAFE_TARGET_DB,
            "--json",
        ),
        (
            "emit-env",
            "--admin-database-url",
            SAFE_ADMIN_URL,
            "--target-database-name",
            SAFE_TARGET_DB,
            "--json",
        ),
    ],
)
def test_json_outputs_are_stable_and_mark_raw_url_not_echoed(mode_args) -> None:
    result = _run_cli(*mode_args)

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["schema_version"] == "local_test_db_fixture_provisioning_v1"
    assert report["status"] == "pass"
    assert report["raw_url_echoed"] is False
    assert report["checks_failed"] == []
    assert SAFE_ADMIN_URL not in result.stdout
    assert SECRET_VALUE not in result.stdout
    assert result.stderr == ""


def test_no_forbidden_runtime_modules_are_imported_or_invoked() -> None:
    source = (ROOT / "tools" / "local_test_db_fixture_provisioning_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint({"redis", "openai", "telegram", "docker", "systemd"})
    assert "TDLib" not in source
