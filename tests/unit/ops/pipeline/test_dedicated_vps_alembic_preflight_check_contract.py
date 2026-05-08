from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_alembic_preflight_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_alembic_preflight.md"


def _module():
    from scripts.ops import dedicated_vps_alembic_preflight_check as module

    return module


def _valid_runbook_text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def _write_runbook(tmp_path: Path, text: str) -> None:
    module = _module()
    runbook = tmp_path / module.CHECKED_FILE
    runbook.parent.mkdir(parents=True, exist_ok=True)
    runbook.write_text(text, encoding="utf-8")


def _assert_required_marker_failure(tmp_path: Path, text: str, marker_name: str) -> None:
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_markers" in result.report["checks_failed"]
    assert any(
        failure["check"] == f"runbook.required_marker:{marker_name}"
        for failure in result.report["failures"]
    )


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_checker_json_passes_against_committed_runbook() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["report_type"] == "dedicated_vps_alembic_preflight_check_v1"
    assert report["contract_status"] == "passed"
    assert report["checked_file"] == "ops/pipeline/runbooks/dedicated_vps_alembic_preflight.md"
    assert report["checks_failed"] == []
    assert report["failures"] == []


def test_json_shape_stable() -> None:
    report = _module().generate_report(ROOT).report

    assert list(report) == [
        "report_type",
        "contract_status",
        "checked_file",
        "checks_failed",
        "failures",
        "authorization",
        "checker_side_effects",
    ]


def test_authorization_booleans_are_intended() -> None:
    authorization = _module().generate_report(ROOT).report["authorization"]

    assert authorization == {
        "repo_alembic_asset_check_present": True,
        "redacted_runtime_env_validation_present": True,
        "read_only_alembic_current_template_present": True,
        "runtime_env_file_read_by_checker": False,
        "env_vars_read_by_checker": False,
        "db_connection_by_checker": False,
        "alembic_execution_by_checker": False,
        "alembic_upgrade_authorized": False,
        "alembic_stamp_authorized": False,
        "alembic_revision_authorized": False,
        "app_runtime_authorized": False,
        "tdlib_telegram_authorized": False,
        "live_collector_authorized": False,
        "notifier_transport_authorized": False,
        "production_rollout_authorized": False,
    }


def test_checker_side_effect_booleans_are_all_false() -> None:
    side_effects = _module().generate_report(ROOT).report["checker_side_effects"]

    assert side_effects == {
        "host_inspection_performed": False,
        "secret_file_read": False,
        "env_vars_read": False,
        "commands_executed": False,
        "files_mutated": False,
        "database_connected": False,
        "redis_connected": False,
    }
    assert all(value is False for value in side_effects.values())


def test_missing_alembic_ini_marker_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("test -f alembic.ini", "test -f migration-config.ini")
    text = text.replace("`alembic.ini` exists", "`migration-config.ini` exists")

    _assert_required_marker_failure(tmp_path, text, "alembic_ini")


def test_missing_migrations_env_marker_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("migrations/env.py", "migrations/runtime.py")

    _assert_required_marker_failure(tmp_path, text, "migrations_env")


def test_missing_migrations_versions_marker_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("migrations/versions", "migrations/version-files")

    _assert_required_marker_failure(tmp_path, text, "migrations_versions")


def test_missing_runtime_env_redacted_validation_marker_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("/etc/github-ai-catchbot/runtime.env", "/etc/github-ai-catchbot/redacted.env")

    _assert_required_marker_failure(tmp_path, text, "runtime_env_path")


def test_missing_database_url_shape_markers_fail(tmp_path: Path) -> None:
    replacements = {
        "database_url_prefix": (
            "postgresql+psycopg://github_ai_catchbot_app:",
            "postgresql+psycopg://wrong_app:",
        ),
        "database_url_host_db": (
            "@127.0.0.1:5432/github_ai_catchbot",
            "@127.0.0.1:5432/wrong_database",
        ),
    }

    for marker_name, (old, new) in replacements.items():
        _assert_required_marker_failure(tmp_path, _valid_runbook_text().replace(old, new), marker_name)


def test_missing_safe_gate_markers_fail(tmp_path: Path) -> None:
    replacements = {
        "notification_send_disabled": ("ENABLE_NOTIFICATION_SEND=false", "ENABLE_NOTIFICATION_SEND=<disabled>"),
        "notifier_dry_run": ("NOTIFIER_TELEGRAM_DRY_RUN=true", "NOTIFIER_TELEGRAM_DRY_RUN=<dry-run>"),
        "replay_to_prod_disabled": ("ENABLE_REPLAY_TO_PROD_DB=false", "ENABLE_REPLAY_TO_PROD_DB=<disabled>"),
        "retry_promotion_disabled": (
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=<disabled>",
        ),
    }

    for marker_name, (old, new) in replacements.items():
        _assert_required_marker_failure(tmp_path, _valid_runbook_text().replace(old, new), marker_name)


def test_missing_separate_approval_marker_for_alembic_current_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("Run only after separate approval", "Run after review")

    _assert_required_marker_failure(tmp_path, text, "separate_approval")


def test_adding_cat_runtime_env_fails(tmp_path: Path) -> None:
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\ncat /etc/github-ai-catchbot/runtime.env\n```\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("cat_runtime_env" in failure["check"] for failure in result.report["failures"])


def test_adding_source_runtime_env_fails(tmp_path: Path) -> None:
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\nsource /etc/github-ai-catchbot/runtime.env\n```\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("source_runtime_env" in failure["check"] for failure in result.report["failures"])


def test_adding_dot_source_runtime_env_fails(tmp_path: Path) -> None:
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n. /etc/github-ai-catchbot/runtime.env\n```\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("dot_source_runtime_env" in failure["check"] for failure in result.report["failures"])


def test_adding_export_database_url_or_redis_url_fails(tmp_path: Path) -> None:
    snippets = {
        "export_database_url": "export DATABASE_URL=postgresql://example.invalid",
        "export_redis_url": "export REDIS_URL=redis://127.0.0.1:6379/0",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_adding_direct_shell_database_url_alembic_current_fails(tmp_path: Path) -> None:
    snippet = "DATABASE_URL=postgresql://user:secret@127.0.0.1:5432/github_ai_catchbot alembic current"
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("direct_database_url_alembic_current" in failure["check"] for failure in result.report["failures"])


def test_adding_alembic_upgrade_stamp_or_revision_fails(tmp_path: Path) -> None:
    snippets = {
        "alembic_upgrade": "alembic upgrade head",
        "alembic_stamp": "alembic stamp head",
        "alembic_revision": "alembic revision -m add_table",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_adding_runtime_tdlib_telegram_collector_notifier_rollout_authorization_fails(
    tmp_path: Path,
) -> None:
    snippets = {
        "app_runtime_start": "Start app runtime.",
        "tdlib_auth": "Perform TDLib auth.",
        "telegram_connection": "Connect to Telegram.",
        "live_collector_start": "Start live collector.",
        "notifier_transport": "Enable notifier transport.",
        "production_rollout": "Authorize production rollout.",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n{snippet}\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_adding_docker_systemd_or_repo_env_authorization_fails(tmp_path: Path) -> None:
    snippets = {
        "docker_execution": "docker compose up -d",
        "systemd_unit_changes": "sudo systemctl restart github-ai-catchbot",
        "repo_env_creation": "touch .env",
        "repo_env_dir_creation": "touch env/prod.env",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


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
