from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_db_redis_operator_command_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_db_redis_operator_provisioning.md"


def _module():
    from scripts.ops import dedicated_vps_db_redis_operator_command_check as module

    return module


def _valid_runbook_text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def _write_runbook(tmp_path: Path, text: str) -> None:
    module = _module()
    runbook = tmp_path / module.CHECKED_FILE
    runbook.parent.mkdir(parents=True, exist_ok=True)
    runbook.write_text(text, encoding="utf-8")


def test_checker_json_passes_against_committed_operator_provisioning_runbook() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["report_type"] == "dedicated_vps_db_redis_operator_command_check_v1"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["failures"] == []


def test_json_shape_has_stable_top_level_fields() -> None:
    report = _module().generate_report(ROOT).report

    assert list(report) == [
        "report_type",
        "contract_status",
        "checked_file",
        "checks_failed",
        "failures",
        "operator_authorization",
        "checker_side_effects",
    ]


def test_operator_authorization_booleans_are_exactly_intended() -> None:
    authorization = _module().generate_report(ROOT).report["operator_authorization"]

    assert authorization == {
        "postgresql_apt_install_operator_command_present": True,
        "redis_apt_install_operator_command_present": True,
        "postgresql_systemd_operator_command_present": True,
        "redis_systemd_operator_command_present": True,
        "postgresql_local_bind_operator_command_present": True,
        "redis_local_bind_operator_command_present": True,
        "postgresql_role_database_operator_command_present": True,
        "local_health_check_operator_commands_present": True,
        "docker_install_authorized": False,
        "docker_compose_authorized": False,
        "env_creation_authorized": False,
        "secret_printing_authorized": False,
        "alembic_authorized": False,
        "app_runtime_authorized": False,
        "tdlib_telegram_authorized": False,
        "live_collector_authorized": False,
        "notifier_transport_authorized": False,
        "production_rollout_authorized": False,
    }


def test_checker_side_effect_booleans_are_all_false() -> None:
    side_effects = _module().generate_report(ROOT).report["checker_side_effects"]

    assert side_effects
    assert all(value is False for value in side_effects.values())


def test_missing_postgresql_apt_install_command_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("postgresql postgresql-contrib ", "")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_redis_apt_install" in failure["check"] for failure in result.report["failures"])


def test_missing_redis_apt_install_command_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(" redis-server redis-tools", "")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_redis_apt_install" in failure["check"] for failure in result.report["failures"])


def test_missing_docker_compose_future_candidate_not_discarded_wording_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "Docker Compose remains a future full app stack candidate and is not\n"
        "  discarded.",
        "Docker Compose is deferred.",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any(
        "docker_compose_future_candidate_not_discarded" in failure["check"]
        for failure in result.report["failures"]
    )


def test_missing_postgresql_local_bind_command_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("set listen_addresses \"'127.0.0.1'\"", "set listen_addresses 'localhost'")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_listen_addresses_local" in failure["check"] for failure in result.report["failures"])


def test_unquoted_postgresql_listen_addresses_command_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "set listen_addresses \"'127.0.0.1'\"",
        "set listen_addresses '127.0.0.1'",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_listen_addresses_local" in failure["check"] for failure in result.report["failures"])


def test_missing_redis_local_bind_command_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("bind 127.0.0.1 ::1", "bind localhost")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("redis_bind_loopback" in failure["check"] for failure in result.report["failures"])


def test_redis_config_reads_must_use_sudo(tmp_path: Path) -> None:
    text = (
        _valid_runbook_text()
        .replace("if sudo grep -qE '^[#[:space:]]*bind ' /etc/redis/redis.conf; then", "if grep -qE '^[#[:space:]]*bind ' /etc/redis/redis.conf; then")
        .replace("if sudo grep -qE '^[#[:space:]]*protected-mode ' /etc/redis/redis.conf; then", "if grep -qE '^[#[:space:]]*protected-mode ' /etc/redis/redis.conf; then")
        .replace("if sudo grep -qE '^[#[:space:]]*supervised ' /etc/redis/redis.conf; then", "if grep -qE '^[#[:space:]]*supervised ' /etc/redis/redis.conf; then")
        .replace("sudo grep -E '^(bind|protected-mode|supervised) ' /etc/redis/redis.conf", "grep -E '^(bind|protected-mode|supervised) ' /etc/redis/redis.conf")
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    failed = {failure["check"] for failure in result.report["failures"]}
    assert any("redis_bind_sudo_grep" in check for check in failed)
    assert any("redis_protected_mode_sudo_grep" in check for check in failed)
    assert any("redis_supervised_sudo_grep" in check for check in failed)
    assert any("redis_final_sudo_grep" in check for check in failed)


def test_missing_no_public_5432_or_6379_wording_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("no public 5432", "no external PostgreSQL exposure")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("no_public_5432" in failure["check"] for failure in result.report["failures"])

    text = _valid_runbook_text().replace("public 6379", "external Redis exposure")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("no_public_6379" in failure["check"] for failure in result.report["failures"])


def test_missing_wildcard_public_bind_detection_markers_causes_failure(tmp_path: Path) -> None:
    replacements = {
        "postgresql_star_public_bind_check": ("*:5432", "STAR_PG_PORT"),
        "postgresql_triple_colon_public_bind_check": (":::5432", "TRIPLE_COLON_PG_PORT"),
        "redis_star_public_bind_check": ("*:6379", "STAR_REDIS_PORT"),
        "redis_triple_colon_public_bind_check": (":::6379", "TRIPLE_COLON_REDIS_PORT"),
    }

    for expected_check, (marker, replacement) in replacements.items():
        text = _valid_runbook_text().replace(marker, replacement)
        _write_runbook(tmp_path, text)

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_missing_ufw_active_check_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("Status: active", "UFW active")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("ufw_status_active_check" in failure["check"] for failure in result.report["failures"])


def test_missing_postgresql_cluster_count_guard_causes_failure(tmp_path: Path) -> None:
    text = (
        _valid_runbook_text()
        .replace("PG_CLUSTER_COUNT", "PG_CLUSTER_TOTAL")
        .replace("expected exactly one PostgreSQL cluster", "expected a PostgreSQL cluster")
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    failed = {failure["check"] for failure in result.report["failures"]}
    assert any("postgresql_cluster_count_guard" in check for check in failed)
    assert any("postgresql_exactly_one_cluster_guard" in check for check in failed)


def test_missing_postgresql_cluster_online_guard_causes_failure(tmp_path: Path) -> None:
    text = (
        _valid_runbook_text()
        .replace('$4 == "online"', '$4 == "running"')
        .replace("is not online", "is not ready")
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_cluster_online_guard" in failure["check"] for failure in result.report["failures"])


def test_missing_postgresql_cluster_status_after_restart_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        'sudo systemctl restart postgresql redis-server\n\n'
        'PG_CLUSTER_COUNT="$(pg_lsclusters --no-header | awk \'END {print NR}\')"',
        'sudo systemctl restart postgresql redis-server\n\n'
        'PG_CLUSTER_TOTAL="$(pg_lsclusters --no-header | awk \'END {print NR}\')"',
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_cluster_status_after_restart" in failure["check"] for failure in result.report["failures"])


def test_missing_postgresql_readiness_after_restart_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "pg_isready -h 127.0.0.1 -p 5432\nsudo systemctl is-active postgresql redis-server",
        "sudo systemctl is-active postgresql redis-server",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any(
        "postgresql_restart_readiness_before_systemd_status" in failure["check"]
        for failure in result.report["failures"]
    )


def test_missing_interactive_password_safe_command_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("sudo -u postgres psql -c '\\password github_ai_catchbot_app'", "")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("interactive_password_prompt" in failure["check"] for failure in result.report["failures"])


def test_missing_db_or_role_names_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("github_ai_catchbot_app", "catchbot_app")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_app_role" in failure["check"] for failure in result.report["failures"])

    text = _valid_runbook_text().replace("github_ai_catchbot", "catchbotdb")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("postgresql_app_database" in failure["check"] for failure in result.report["failures"])


def test_missing_local_health_checks_causes_failure(tmp_path: Path) -> None:
    text = (
        _valid_runbook_text()
        .replace("pg_isready -h 127.0.0.1 -p 5432", "pg_isready")
        .replace("redis-cli -h 127.0.0.1 -p 6379 PING", "redis-cli PING")
        .replace("ss -ltn", "socket-listen-check")
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    failed = {failure["check"] for failure in result.report["failures"]}
    assert any("postgresql_local_health" in check for check in failed)
    assert any("redis_local_health" in check for check in failed)
    assert any("ss_ltn_check" in check for check in failed)


def test_adding_docker_install_or_docker_compose_execution_wording_causes_failure(tmp_path: Path) -> None:
    snippets = {
        "docker_install_command": "sudo apt-get install -y docker.io",
        "docker_compose_execution": "docker compose up -d",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_adding_env_creation_or_database_redis_url_creation_causes_failure(tmp_path: Path) -> None:
    snippets = {
        "env_creation": "touch .env",
        "database_url_assignment": "DATABASE_URL=postgresql://example",
        "redis_url_assignment": "REDIS_URL=redis://example",
    }

    for expected_check, snippet in snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_adding_literal_password_assignment_causes_failure(tmp_path: Path) -> None:
    snippets = {
        "literal_password_assignment": "PGPASSWORD=secret",
        "literal_password_assignment_alt": "CATCHBOT_DB_PASSWORD=secret",
    }

    for snippet in snippets.values():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n```bash\n{snippet}\n```\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1
        assert any("literal_password_assignment" in failure["check"] for failure in result.report["failures"])


def test_adding_runtime_authorization_wording_causes_failure(tmp_path: Path) -> None:
    snippets = {
        "alembic_execution": "Run Alembic now.",
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


def test_checker_implementation_avoids_host_network_secret_and_write_surfaces() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    forbidden_import_roots = {
        "subprocess",
        "socket",
        "os",
        "dotenv",
        "psycopg",
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
    assert "redis" not in imported_roots
    assert forbidden_write_calls == set()
    assert open_write_modes == []
    text = SCRIPT.read_text(encoding="utf-8")
    assert "os.environ" not in text
    assert "subprocess" not in text
    assert "socket." not in text
