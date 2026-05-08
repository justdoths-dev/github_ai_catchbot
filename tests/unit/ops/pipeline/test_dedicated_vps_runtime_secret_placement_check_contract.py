from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_runtime_secret_placement_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_runtime_secret_placement.md"
DATABASE_URL_TEMPLATE = (
    "DATABASE_URL=postgresql+psycopg://github_ai_catchbot_app:"
    "<DB_PASSWORD_FROM_PASSWORD_MANAGER>@127.0.0.1:5432/github_ai_catchbot"
)


def _module():
    from scripts.ops import dedicated_vps_runtime_secret_placement_check as module

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


def test_help_works_without_runtime_environment() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    output = f"{result.stdout}\n{result.stderr}"
    assert result.returncode == 0
    assert "usage:" in output
    assert "--format {text,json}" in output
    assert "local repository text only" in output


def test_checker_json_passes_against_current_runbook() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["report_type"] == "dedicated_vps_runtime_secret_placement_check_v1"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["failures"] == []


def test_json_shape_has_stable_top_level_fields() -> None:
    report = _module().generate_report(ROOT).report

    assert list(report) == [
        "report_type",
        "contract_status",
        "checked_file",
        "secret_path",
        "secret_dir",
        "required_keys",
        "optional_keys",
        "expected_fixed_values",
        "checks_failed",
        "failures",
        "operator_authorization",
        "checker_side_effects",
    ]


def test_operator_authorization_booleans_are_exactly_intended() -> None:
    authorization = _module().generate_report(ROOT).report["operator_authorization"]

    assert authorization == {
        "secret_directory_create_operator_command_present": True,
        "runtime_secret_file_create_operator_command_present": True,
        "sudoedit_runtime_secret_file_operator_command_present": True,
        "redacted_validation_operator_command_present": True,
        "runtime_secret_file_path_authorized": True,
        "runtime_secret_file_outside_repo": True,
        "repo_env_creation_authorized": False,
        "repo_env_directory_creation_authorized": False,
        "secret_values_printing_authorized": False,
        "database_connection_authorized": False,
        "redis_connection_authorized": False,
        "alembic_authorized": False,
        "app_runtime_authorized": False,
        "tdlib_auth_authorized": False,
        "telegram_connection_authorized": False,
        "live_collector_authorized": False,
        "notifier_transport_authorized": False,
        "production_rollout_authorized": False,
        "docker_authorized": False,
        "docker_compose_authorized": False,
        "systemd_unit_modification_authorized": False,
    }


def test_checker_side_effect_booleans_are_all_false() -> None:
    side_effects = _module().generate_report(ROOT).report["checker_side_effects"]

    assert side_effects
    assert all(value is False for value in side_effects.values())


def test_secret_path_permissions_and_safe_create_commands_are_locked() -> None:
    report = _module().generate_report(ROOT).report
    text = RUNBOOK.read_text(encoding="utf-8")

    assert report["secret_path"] == "/etc/github-ai-catchbot/runtime.env"
    assert report["secret_dir"] == "/etc/github-ai-catchbot"
    assert "sudo install -d -o root -g deploy -m 0750 /etc/github-ai-catchbot" in text
    assert (
        "sudo install -o root -g deploy -m 0640 /dev/null /etc/github-ai-catchbot/runtime.env"
        in text
    )
    assert "sudoedit /etc/github-ai-catchbot/runtime.env" in text
    assert "root:deploy 0750" in text
    assert "root:deploy 0640" in text


def test_database_url_template_shape_is_locked() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert DATABASE_URL_TEMPLATE in text
    assert "<DB_PASSWORD_FROM_PASSWORD_MANAGER>" in text


def test_required_and_optional_key_sets_are_exact() -> None:
    module = _module()
    report = module.generate_report(ROOT).report

    assert tuple(report["required_keys"]) == module.REQUIRED_KEYS
    assert tuple(report["optional_keys"]) == module.OPTIONAL_KEYS
    assert report["expected_fixed_values"] == {
        "APP_ENV": "prod",
        "REDIS_URL": "redis://127.0.0.1:6379/0",
        "ENABLE_NOTIFICATION_SEND": "false",
        "NOTIFIER_TELEGRAM_DRY_RUN": "true",
        "NOTIFIER_TELEGRAM_ALLOW_EDITS": "false",
        "ENABLE_REPLAY_TO_PROD_DB": "false",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
    }


def test_runbook_has_required_sections_once() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    for section in _module().REQUIRED_SECTIONS:
        assert text.count(section.heading) == 1, section.heading


def test_missing_required_section_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("## Failure handling", "## Failure notes")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_sections" in result.report["checks_failed"]


def test_missing_required_key_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("DATABASE_URL=<secret value, not printed>\n", "")
    text = text.replace(f"{DATABASE_URL_TEMPLATE}\n", "")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_key_assignments" in result.report["checks_failed"]
    assert any(
        failure["check"] == "runbook.required_key_assignments"
        and "DATABASE_URL" in failure["missing_keys"]
        for failure in result.report["failures"]
    )


def test_missing_fixed_gate_value_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("ENABLE_NOTIFICATION_SEND=false", "ENABLE_NOTIFICATION_SEND=<disabled>")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.fixed_safe_values" in result.report["checks_failed"]
    assert "ENABLE_NOTIFICATION_SEND" in result.report["failures"][0]["missing_keys"]


def test_unapproved_key_assignment_causes_failure(tmp_path: Path) -> None:
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\nUNREVIEWED_SECRET=value\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.unauthorized_key_assignments" in result.report["checks_failed"]
    assert "UNREVIEWED_SECRET" in result.report["failures"][0]["unauthorized_keys"]


def test_forbidden_enabled_gate_values_cause_failure(tmp_path: Path) -> None:
    replacements = {
        "enable_notification_send_true": ("ENABLE_NOTIFICATION_SEND=false", "ENABLE_NOTIFICATION_SEND=true"),
        "notifier_dry_run_false": ("NOTIFIER_TELEGRAM_DRY_RUN=true", "NOTIFIER_TELEGRAM_DRY_RUN=false"),
        "notifier_allow_edits_true": (
            "NOTIFIER_TELEGRAM_ALLOW_EDITS=false",
            "NOTIFIER_TELEGRAM_ALLOW_EDITS=true",
        ),
        "replay_to_prod_true": ("ENABLE_REPLAY_TO_PROD_DB=false", "ENABLE_REPLAY_TO_PROD_DB=true"),
        "retry_promotion_true": (
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true",
        ),
    }

    for expected_check, (old, new) in replacements.items():
        _write_runbook(tmp_path, _valid_runbook_text().replace(old, new, 1))

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert "runbook.forbidden_authorization" in result.report["checks_failed"]
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_literal_database_url_value_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        DATABASE_URL_TEMPLATE,
        "DATABASE_URL=" + "postgres" + "ql://user:secret@127.0.0.1:5432/github_ai_catchbot",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("database_url_literal_assignment" in failure["check"] for failure in result.report["failures"])


def test_missing_exact_database_url_template_shape_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        DATABASE_URL_TEMPLATE,
        "DATABASE_URL=<replace inside editor; do not print>",
    )

    _assert_required_marker_failure(tmp_path, text, "database_url_template_shape")


def test_missing_db_password_placeholder_check_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'if "<DB_PASSWORD_FROM_PASSWORD_MANAGER>" in value:',
        'if value == "__db_password_placeholder_check_removed__":',
    )

    _assert_required_marker_failure(tmp_path, text, "db_password_placeholder_failure_check")


def test_missing_numeric_mode_extraction_marker_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'mode = f"{stat.S_IMODE(info.st_mode):04o}"[-3:]',
        "mode = str(info.st_mode)[-3:]",
    )

    _assert_required_marker_failure(tmp_path, text, "numeric_mode_extraction")


def test_textual_filemode_as_only_mode_extraction_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'mode = f"{stat.S_IMODE(info.st_mode):04o}"[-3:]',
        "mode = stat.filemode(info.st_mode)[-3:]",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_markers" in result.report["checks_failed"]
    assert "runbook.forbidden_authorization" in result.report["checks_failed"]
    assert any(
        failure["check"] == "runbook.required_marker:numeric_mode_extraction"
        for failure in result.report["failures"]
    )
    assert any(
        failure["check"] == "runbook.forbidden_authorization:textual_filemode_permission_extraction"
        for failure in result.report["failures"]
    )


def test_missing_generic_placeholder_check_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'if "<" in value and ">" in value:',
        'if value == "__generic_placeholder_check_removed__":',
    )

    _assert_required_marker_failure(tmp_path, text, "generic_placeholder_failure_check")


def test_missing_replace_inside_editor_placeholder_check_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'if "<replace inside editor; do not print>" in value:',
        'if value == "__editor_placeholder_check_removed__":',
    )

    _assert_required_marker_failure(tmp_path, text, "editor_placeholder_failure_check")


def test_missing_database_url_prefix_check_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'if not database_url.startswith("postgresql+psycopg://github_ai_catchbot_app:"):',
        'if not database_url.startswith("postgresql://wrong-user:"):',
    )

    _assert_required_marker_failure(tmp_path, text, "database_url_prefix_shape_check")


def test_missing_database_url_host_db_suffix_check_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'if "@127.0.0.1:5432/github_ai_catchbot" not in database_url:',
        'if "__host_db_suffix_check_removed__" not in database_url:',
    )

    _assert_required_marker_failure(tmp_path, text, "database_url_host_db_suffix_shape_check")


def test_runtime_and_infrastructure_command_additions_cause_failure(tmp_path: Path) -> None:
    snippets = {
        "ssh_command": "ssh deploy@example.invalid",
        "psql_command": "psql " + "postgres" + "ql://example.invalid",
        "redis_cli_command": "redis-cli -h 127.0.0.1 ping",
        "alembic_execution": "alembic upgrade head",
        "app_runtime_start": "python main.py",
        "docker_execution": "docker compose up -d",
        "systemctl_command": "sudo systemctl restart github-ai-catchbot",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1, snippet
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_runtime_env_cat_source_and_export_additions_cause_failure(tmp_path: Path) -> None:
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

        assert result.exit_code == 1, snippet
        assert "runbook.forbidden_authorization" in result.report["checks_failed"]
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_missing_runtime_env_cat_source_dot_source_export_prohibitions_cause_failure(
    tmp_path: Path,
) -> None:
    replacements = {
        "no_cat_runtime_env": (
            "Do not `cat /etc/github-ai-catchbot/runtime.env`",
            "Avoid printing the runtime secret file",
        ),
        "no_source_runtime_env": (
            "Do not `source /etc/github-ai-catchbot/runtime.env`",
            "Avoid loading the runtime secret file in the shell",
        ),
        "no_export_database_url": (
            "Do not `export DATABASE_URL`",
            "Avoid exporting the database URL",
        ),
        "no_export_redis_url": (
            "Do not `export REDIS_URL`",
            "Avoid exporting the Redis URL",
        ),
    }

    for marker_name, (old, new) in replacements.items():
        text = _valid_runbook_text().replace(old, new)

        _assert_required_marker_failure(tmp_path, text, marker_name)

    text = _valid_runbook_text().replace(
        "Do not `. /etc/github-ai-catchbot/runtime.env`",
        "Avoid dot-sourcing the runtime secret file",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_markers" in result.report["checks_failed"]
    assert any(
        failure["check"] == "runbook.required_marker:no_dot_source_runtime_env"
        for failure in result.report["failures"]
    )


def test_runtime_authorization_wording_additions_cause_failure(tmp_path: Path) -> None:
    snippets = {
        "tdlib_auth": "Perform TDLib auth.",
        "telegram_connection": "Connect to Telegram.",
        "live_collector_start": "Start live collector.",
        "notifier_transport": "Enable notifier transport.",
        "production_rollout": "Authorize production rollout.",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n{snippet}\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1, snippet
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_raw_non_loopback_ip_literal_causes_failure(tmp_path: Path) -> None:
    _write_runbook(tmp_path, f"{_valid_runbook_text()}\nserver ip 203.0.113.10\n")

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.raw_ip_literals" in result.report["checks_failed"]
    assert result.report["failures"][0]["ip_literals"] == ["203.0.113.10"]


def test_checker_implementation_avoids_host_network_secret_and_write_surfaces() -> None:
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

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
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
    assert forbidden_write_calls == set()
    assert open_write_modes == []
