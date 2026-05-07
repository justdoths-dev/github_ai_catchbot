from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_db_redis_package_vs_docker_decision_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_db_redis_package_vs_docker_decision.md"


def _module():
    from scripts.ops import dedicated_vps_db_redis_package_vs_docker_decision_check as module

    return module


def _write_runbook(tmp_path: Path, text: str) -> None:
    module = _module()
    runbook = tmp_path / module.CHECKED_FILE
    runbook.parent.mkdir(parents=True, exist_ok=True)
    runbook.write_text(text, encoding="utf-8")


def _valid_runbook_text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


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
    assert report["check_name"] == "dedicated_vps_db_redis_package_vs_docker_decision_check"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["failures"] == []


def test_json_shape_has_stable_top_level_fields() -> None:
    report = _module().generate_report(ROOT).report

    assert list(report) == [
        "check_name",
        "contract_status",
        "checked_file",
        "selected_decision",
        "docker_compose_status",
        "checks_failed",
        "failures",
        "authorization",
        "side_effects",
    ]


def test_all_authorization_booleans_are_false() -> None:
    report = _module().generate_report(ROOT).report

    assert report["authorization"]
    assert all(value is False for value in report["authorization"].values())


def test_all_side_effect_booleans_are_false() -> None:
    report = _module().generate_report(ROOT).report

    assert report["side_effects"]
    assert all(value is False for value in report["side_effects"].values())


def test_selected_decision_is_host_apt_systemd_postgresql_and_redis() -> None:
    report = _module().generate_report(ROOT).report

    assert report["selected_decision"] == "host_apt_systemd_postgresql_and_redis"


def test_docker_compose_is_preserved_as_future_full_app_stack_candidate() -> None:
    report = _module().generate_report(ROOT).report

    assert report["docker_compose_status"] == "future_full_app_stack_candidate_not_discarded"
    assert "Docker Compose remains a future full app stack candidate" in RUNBOOK.read_text(encoding="utf-8")
    assert "not discarded" in RUNBOOK.read_text(encoding="utf-8")


def test_missing_host_apt_systemd_decision_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "First DB/Redis provisioning should use host apt/systemd PostgreSQL and host\n"
        "apt/systemd Redis.",
        "First DB/Redis provisioning should use a later undecided path.",
    ).replace(
        "host apt/systemd PostgreSQL + host apt/systemd Redis",
        "undecided DB/Redis path",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_markers" in result.report["checks_failed"]
    assert any("selected_host_apt_systemd_db_redis" in failure["check"] for failure in result.report["failures"])


def test_missing_docker_not_discarded_statement_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace(
        "Docker Compose remains a future full app stack candidate and is not\n"
        "  discarded.",
        "Docker Compose may be considered later.",
    ).replace(
        "This does not discard Docker Compose.",
        "This keeps deployment details separate.",
    ).replace(
        "Docker Compose remains a future full app\n"
        "stack candidate and is not discarded.",
        "Docker Compose may be considered later.",
    )
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert "runbook.required_markers" in result.report["checks_failed"]
    assert any("docker_compose_not_discarded" in failure["check"] for failure in result.report["failures"])


def test_missing_no_public_5432_or_6379_causes_failure(tmp_path: Path) -> None:
    text = _valid_runbook_text().replace("no public 5432", "no external database exposure")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("no_public_5432" in failure["check"] for failure in result.report["failures"])

    text = _valid_runbook_text().replace("no public 6379", "no external cache exposure")
    _write_runbook(tmp_path, text)

    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert any("no_public_6379" in failure["check"] for failure in result.report["failures"])


def test_authorizing_install_start_connect_secret_alembic_runtime_telegram_notifier_rollout_fails(
    tmp_path: Path,
) -> None:
    forbidden_snippets = {
        "apt_install_now": "Run apt install now.",
        "docker_install_now": "Install Docker now.",
        "postgresql_install_now": "Install PostgreSQL now.",
        "redis_install_now": "Install Redis now.",
        "service_start_restart_now": "systemctl start postgresql",
        "db_connection_now": "DB connection now.",
        "redis_connection_now": "Redis connection now.",
        "env_creation": "Create `.env` for the operator.",
        "secret_placement": "Place secrets in the repo.",
        "alembic_now": "Run Alembic now.",
        "app_runtime_start": "Start app runtime.",
        "tdlib_auth": "Perform TDLib auth.",
        "telegram_connection": "Connect to Telegram now.",
        "live_collector_start": "Start live collector.",
        "notifier_transport": "Enable notifier transport.",
        "production_rollout": "Authorize production rollout.",
    }

    for expected_check, snippet in forbidden_snippets.items():
        _write_runbook(tmp_path, f"{_valid_runbook_text()}\n{snippet}\n")

        result = _module().generate_report(tmp_path)

        assert result.exit_code == 1, snippet
        assert "runbook.forbidden_authorization" in result.report["checks_failed"]
        assert any(expected_check in failure["check"] for failure in result.report["failures"])


def test_checker_does_not_read_env_or_inspect_host_services_structurally() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    lower_text = text.lower()

    assert "subprocess" not in text
    assert "import socket" not in lower_text
    assert "socket." not in lower_text
    assert "systemctl" not in lower_text
    assert "redis-cli" not in lower_text
    assert "psql" not in lower_text
    assert "os.environ" not in text
    assert "dotenv" not in lower_text
    assert "read_text" in text
    assert "write_text" not in text
