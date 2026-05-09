from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_alembic_upgrade_gate_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_alembic_upgrade_gate.md"


def _module():
    from scripts.ops import dedicated_vps_alembic_upgrade_gate_check as module

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
    assert report["report_type"] == "dedicated_vps_alembic_upgrade_gate_check_v1"
    assert report["contract_status"] == "passed"
    assert report["checked_file"] == "ops/pipeline/runbooks/dedicated_vps_alembic_upgrade_gate.md"
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
        "pre_upgrade_current_template_present": True,
        "explicit_upgrade_approval_checkpoint_present": True,
        "upgrade_head_template_present": True,
        "post_upgrade_current_template_present": True,
        "runtime_env_file_read_by_checker": False,
        "env_vars_read_by_checker": False,
        "db_connection_by_checker": False,
        "alembic_execution_by_checker": False,
        "alembic_upgrade_authorized_by_this_slice": False,
        "alembic_upgrade_template_after_approval_present": True,
        "alembic_downgrade_authorized": False,
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


def test_missing_alembic_asset_markers_fail(tmp_path: Path) -> None:
    replacements = {
        "alembic_ini": ("alembic.ini", "migration-config.ini"),
        "migrations_dir": ("migrations", "migration-dir"),
        "migrations_env": ("migrations/env.py", "migrations/runtime.py"),
        "migrations_versions": ("migrations/versions", "migrations/version-files"),
        "find_versions": ("find migrations/versions", "find migration-files"),
    }

    for marker_name, (old, new) in replacements.items():
        _assert_required_marker_failure(tmp_path, _valid_runbook_text().replace(old, new), marker_name)


def test_missing_runtime_env_redacted_validation_markers_fail(tmp_path: Path) -> None:
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
        "notifier_edits_disabled": ("NOTIFIER_TELEGRAM_ALLOW_EDITS=false", "NOTIFIER_TELEGRAM_ALLOW_EDITS=<disabled>"),
        "replay_to_prod_disabled": ("ENABLE_REPLAY_TO_PROD_DB=false", "ENABLE_REPLAY_TO_PROD_DB=<disabled>"),
        "retry_promotion_disabled": (
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=<disabled>",
        ),
    }

    for marker_name, (old, new) in replacements.items():
        _assert_required_marker_failure(tmp_path, _valid_runbook_text().replace(old, new), marker_name)


def test_missing_pre_upgrade_current_marker_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("pre_upgrade_alembic_current_exit_code", "before_upgrade_current_exit")

    _assert_required_marker_failure(tmp_path, text, "pre_upgrade_current_exit")


def test_missing_explicit_approval_checkpoint_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "STOP: do not run Block 5 unless the user explicitly approves Alembic upgrade\nexecution now",
        "STOP before Block 5.",
    )

    _assert_required_marker_failure(tmp_path, text, "explicit_stop_checkpoint")


def test_missing_upgrade_head_template_marker_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("python -m alembic upgrade head", "python -m alembic migrate latest")
    text = text.replace("alembic_upgrade_exit_code", "migration_upgrade_exit_code")

    _assert_required_marker_failure(tmp_path, text, "upgrade_exit")
    _assert_required_marker_failure(tmp_path, text, "alembic_upgrade_command")


def test_missing_post_upgrade_current_marker_fails(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("post_upgrade_alembic_current_exit_code", "after_upgrade_current_exit")

    _assert_required_marker_failure(tmp_path, text, "post_upgrade_current_exit")


def test_adding_cat_source_dot_source_or_export_runtime_env_fails(tmp_path: Path) -> None:
    snippets = {
        "cat_runtime_env": "cat /etc/github-ai-catchbot/runtime.env",
        "source_runtime_env": "source /etc/github-ai-catchbot/runtime.env",
        "dot_source_runtime_env": ". /etc/github-ai-catchbot/runtime.env",
        "export_database_url": "export DATABASE_URL=postgresql://example.invalid",
        "export_redis_url": "export REDIS_URL=redis://127.0.0.1:6379/0",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_adding_direct_shell_database_url_alembic_upgrade_head_fails(tmp_path: Path) -> None:
    snippet = "DATABASE_URL=postgresql://user:secret@127.0.0.1:5432/github_ai_catchbot alembic upgrade head"
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("direct_database_url_alembic_upgrade" in failure["check"] for failure in result.report["failures"])


def test_adding_direct_shell_database_url_alembic_current_fails(tmp_path: Path) -> None:
    snippet = "DATABASE_URL=postgresql://user:secret@127.0.0.1:5432/github_ai_catchbot alembic current"
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("direct_database_url_alembic_current" in failure["check"] for failure in result.report["failures"])


def test_adding_alembic_downgrade_stamp_or_revision_fails(tmp_path: Path) -> None:
    snippets = {
        "alembic_downgrade": "alembic downgrade -1",
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


def test_adding_docker_systemd_repo_env_or_migration_editing_authorization_fails(tmp_path: Path) -> None:
    snippets = {
        "docker_execution": "docker compose up -d",
        "systemd_unit_changes": "sudo systemctl restart github-ai-catchbot",
        "repo_env_creation": "touch .env",
        "repo_env_dir_creation": "touch env/prod.env",
        "migration_editing": "Edit migration files.",
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
