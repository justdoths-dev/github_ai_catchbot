from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_post_migration_db_acceptance_smoke_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_post_migration_db_acceptance_smoke.md"


def _module():
    from scripts.ops import dedicated_vps_post_migration_db_acceptance_smoke_check as module

    return module


def _valid_runbook_text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def _write_runbook(tmp_path: Path, text: str) -> None:
    module = _module()
    runbook = tmp_path / module.RUNBOOK_PATH
    runbook.parent.mkdir(parents=True, exist_ok=True)
    runbook.write_text(text, encoding="utf-8")


def _copy_minimal_migration_fixture(tmp_path: Path) -> None:
    module = _module()
    migrations_dir = tmp_path / module.MIGRATIONS_DIR
    migrations_dir.mkdir(parents=True, exist_ok=True)
    (migrations_dir / "0004_fixture.py").write_text(
        'revision = "0004_judge_delivery_obs"\n'
        "from alembic import op\n\n"
        "def upgrade():\n"
        '    op.create_table("source_messages")\n',
        encoding="utf-8",
    )


def test_checker_imports() -> None:
    module = _module()

    assert module.REPORT_TYPE == "dedicated_vps_post_migration_db_acceptance_smoke_check_v1"
    assert callable(module.main)


def test_format_json_returns_parseable_json() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "passed"
    assert report["runbook_path"] == "ops/pipeline/runbooks/dedicated_vps_post_migration_db_acceptance_smoke.md"


def test_default_checker_result_passes_on_repo_text() -> None:
    result = _module().generate_report(ROOT)

    assert result.exit_code == 0
    assert result.report["contract_status"] == "passed"
    assert result.report["checks_failed"] == []
    assert result.report["failures"] == []


def test_side_effect_booleans_are_false() -> None:
    report = _module().generate_report(ROOT).report

    for key in (
        "runtime_env_read",
        "env_vars_read",
        "database_connected",
        "redis_connected",
        "alembic_run",
        "app_runtime_started",
        "tdlib_auth_performed",
        "telegram_connected",
        "live_collector_started",
        "notifier_transport_enabled",
        "production_rollout_performed",
        "docker_used",
        "systemd_modified",
        "migration_files_modified",
        "secret_values_printed",
    ):
        assert report[key] is False
    assert report["repo_text_only"] is True


def test_checker_derives_revision_and_table_from_actual_migrations() -> None:
    facts = _module().derive_migration_facts(ROOT)

    assert facts.migration_files
    assert facts.revision_ids
    assert facts.tables
    assert "0004_judge_delivery_obs" in facts.revision_ids


def test_checker_expects_terminal_revision() -> None:
    report = _module().generate_report(ROOT).report

    assert "0004_judge_delivery_obs" in report["derived_revision_ids"]


def test_runbook_includes_read_only_scope_and_non_goals() -> None:
    text = _valid_runbook_text()

    assert "read-only DB metadata/queryability smoke" in text
    for phrase in (
        "No app runtime",
        "No TDLib",
        "No Telegram",
        "No live collector",
        "No notifier transport",
        "No Redis mutation",
        "No Alembic mutation",
        "No production rollout",
        "No Docker or Docker Compose",
        "No systemd modification",
        "No migration edits",
        "No DB mutation",
    ):
        assert phrase in text


def test_runbook_prohibits_secret_printing_and_runtime_env_shell_loading() -> None:
    text = _valid_runbook_text()

    for phrase in (
        "Do not `cat /etc/github-ai-catchbot/runtime.env`",
        "Do not `source /etc/github-ai-catchbot/runtime.env`",
        "Do not dot-source `/etc/github-ai-catchbot/runtime.env`",
        "Do not `export DATABASE_URL`",
        "Do not `export REDIS_URL`",
        "Do not print `DATABASE_URL`",
        "Do not print DB password",
        "Do not print any secret value",
    ):
        assert phrase in text


def test_runbook_does_not_authorize_runtime_rollout_docker_systemd_migration_or_db_mutation() -> None:
    text = _valid_runbook_text()

    for phrase in (
        "Do not run app runtime",
        "Do not run TDLib auth",
        "Do not connect Telegram",
        "Do not start live collector",
        "Do not enable notifier transport",
        "Do not perform production rollout",
        "Do not use Docker or Docker Compose",
        "Do not modify systemd units",
        "Do not edit migration files",
        "Do not mutate the database",
    ):
        assert phrase in text


def test_checker_fails_against_temporary_unsafe_runbook_fixture(tmp_path: Path) -> None:
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\ncat /etc/github-ai-catchbot/runtime.env\n```\n")
    _copy_minimal_migration_fixture(tmp_path)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.forbidden_authorization" in result.report["checks_failed"]
    assert any("cat_runtime_env" in failure["check"] for failure in result.report["failures"])


def test_checker_fails_when_migration_files_are_absent(tmp_path: Path) -> None:
    _write_runbook(tmp_path, _valid_runbook_text())

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "migrations.files_found" in result.report["checks_failed"]
    assert "migrations.tables_derived" in result.report["checks_failed"]


def test_checker_implementation_structurally_avoids_forbidden_surfaces() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    forbidden_import_roots = {
        "subprocess",
        "socket",
        "os",
        "dotenv",
        "psycopg",
        "redis",
        "http",
        "urllib",
        "requests",
        "sqlalchemy",
    }
    imported_roots: set[str] = set()
    forbidden_write_calls: set[str] = set()
    open_write_modes: list[str] = []
    os_environ_references: list[str] = []

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

    assert imported_roots.isdisjoint(forbidden_import_roots)
    assert os_environ_references == []
    assert forbidden_write_calls == set()
    assert open_write_modes == []
