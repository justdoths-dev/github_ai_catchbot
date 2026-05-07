from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_db_redis_provisioning_plan_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_db_redis_provisioning_plan.md"


def _module():
    from scripts.ops import dedicated_vps_db_redis_provisioning_plan_check as module

    return module


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
    assert "--format {json}" in output
    assert "local repository text only" in output


def test_current_repo_plan_contract_passes() -> None:
    result = _module().generate_report(ROOT)

    assert result.exit_code == 0
    assert result.report["report_type"] == "dedicated_vps_db_redis_provisioning_plan_check_v1"
    assert result.report["contract_status"] == "passed"
    assert result.report["checks_failed"] == []
    assert result.report["failures"] == []


def test_checker_outputs_required_json_fields() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert set(report) >= {
        "report_type",
        "contract_status",
        "checks_failed",
        "failures",
        "side_effects",
        "authorization",
    }


def test_forbidden_side_effect_booleans_are_all_false() -> None:
    report = _module().generate_report(ROOT).report

    assert report["side_effects"]
    assert all(value is False for value in report["side_effects"].values())


def test_authorization_booleans_are_all_false() -> None:
    report = _module().generate_report(ROOT).report

    assert report["authorization"]
    assert all(value is False for value in report["authorization"].values())


def test_missing_plan_fails(tmp_path: Path) -> None:
    result = _module().generate_report(tmp_path)

    assert result.exit_code == 1
    assert result.report["contract_status"] == "failed"
    assert result.report["checks_failed"] == ["plan.exists"]
    assert result.report["failures"][0]["path"] == _module().PLAN_PATH


def test_missing_required_phrase_fails(tmp_path: Path) -> None:
    module = _module()
    plan = tmp_path / module.PLAN_PATH
    plan.parent.mkdir(parents=True)
    plan.write_text("no public 5432\n", encoding="utf-8")

    result = module.generate_report(tmp_path)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["plan.required_phrases"]
    missing = result.report["failures"][0]["missing_phrases"]
    assert "no_public_6379" in missing
    assert "postgresql_durable_system_of_record" in missing


def test_runbook_contains_required_contract_phrases() -> None:
    text = RUNBOOK.read_text(encoding="utf-8").lower()

    assert "no public 5432" in text
    assert "no public 6379" in text
    assert "localhost/internal network only" in text
    assert "secret placement remains unauthorized" in text
    assert ".env creation remains unauthorized" in text
    assert "alembic migration remains unauthorized" in text
    assert "live collector remains unauthorized" in text
    assert "notifier transport remains unauthorized" in text
    assert "postgresql durable system of record" in text
    assert "redis queue/lock/short-lived execution state" in text


def test_script_does_not_import_runtime_or_host_execution_modules() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    lower_text = text.lower()

    assert "subprocess" not in text
    assert "import socket" not in lower_text
    assert "socket." not in lower_text
    assert "psycopg" not in text.lower()
    assert "import redis" not in lower_text
    assert "from redis" not in lower_text
    assert "redis." not in lower_text
    assert "docker " not in lower_text
    assert "docker." not in lower_text
    assert "systemctl" not in text.lower()
    assert "os.environ" not in text
    assert ".env" in text


def test_script_does_not_mutate_files(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    plan = tmp_path / module.PLAN_PATH
    plan.parent.mkdir(parents=True)
    plan.write_text(
        "\n".join(requirement.phrase for requirement in module.REQUIRED_PHRASES) + "\n",
        encoding="utf-8",
    )

    def fail_mutation(*args, **kwargs):  # noqa: ANN001
        raise AssertionError("plan checker must not mutate files")

    monkeypatch.setattr(module.Path, "write_text", fail_mutation)
    monkeypatch.setattr(module.Path, "touch", fail_mutation)

    result = module.generate_report(tmp_path)

    assert result.exit_code == 0
    assert result.report["side_effects"]["files_mutated"] is False
