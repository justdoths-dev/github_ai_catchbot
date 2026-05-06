from __future__ import annotations

import getpass
import json
import socket
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_baseline_preflight_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_baseline_preflight_check.md"


def _module():
    from scripts.ops import dedicated_vps_baseline_preflight_check as module

    return module


def _synthetic_repo(tmp_path: Path, *, include_core: bool = True, include_venv: bool = True) -> Path:
    repo = tmp_path / "synthetic-repo"
    repo.mkdir()
    if include_core:
        (repo / "docs" / "project-source").mkdir(parents=True)
        (repo / "scripts" / "ops").mkdir(parents=True)
        (repo / "ops" / "pipeline" / "runbooks").mkdir(parents=True)
        (repo / "tests").mkdir()
        (repo / "pyproject.toml").write_text("[project]\nname = 'synthetic'\n", encoding="utf-8")
    if include_venv:
        python_dir = repo / "venv" / "bin"
        python_dir.mkdir(parents=True)
        python_path = python_dir / "python"
        python_path.write_text("# synthetic python marker\n", encoding="utf-8")
        python_path.chmod(0o700)
    return repo


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_help_works_without_real_env() -> None:
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
    assert "--mode {schema,current-host}" in output
    assert "metadata-only dedicated VPS baseline preflight" in output


def test_default_schema_mode_passes_without_production_env(monkeypatch) -> None:
    module = _module()
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_API_HASH", raising=False)

    report = module.generate_report()

    assert report["report_type"] == "dedicated_vps_baseline_preflight_v1"
    assert report["mode"] == "schema"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["host_metadata"]["system"] == "not_checked"


def test_default_schema_mode_includes_dedicated_vps_topology() -> None:
    report = _module().generate_report()

    assert report["deployment_topology"]["expected_deployment_topology"] == "dedicated_vps"


def test_default_schema_mode_states_shared_with_trading_bot_false() -> None:
    report = _module().generate_report()

    assert report["deployment_topology"]["shared_with_trading_bot"] is False


def test_default_schema_mode_states_trading_bot_repo_not_inspected() -> None:
    report = _module().generate_report()

    assert report["deployment_topology"]["trading_bot_repo_inspected"] is False
    assert report["side_effects"]["trading_bot_repo_inspected"] is False


def test_default_schema_mode_states_trading_bot_paths_not_touched() -> None:
    report = _module().generate_report()

    assert report["deployment_topology"]["trading_bot_paths_touched"] is False
    assert report["side_effects"]["trading_bot_paths_touched"] is False


def test_current_host_mode_uses_synthetic_repo_metadata(tmp_path: Path) -> None:
    module = _module()
    repo = _synthetic_repo(tmp_path)

    report = module.generate_report(
        mode="current-host",
        repo_root=repo,
        platform_system=lambda: "Linux",
        platform_machine=lambda: "x86_64",
        version_info=(3, 12),
        os_name="posix",
        prefix="synthetic-venv",
        base_prefix="synthetic-base",
    )

    assert report["contract_status"] == "passed"
    assert report["host_metadata"] == {
        "system": "Linux",
        "machine": "x86_64",
        "python_major_minor": "3.12",
        "is_posix": True,
    }
    assert report["repo_metadata"]["repo_root_detected"] is True
    assert report["repo_metadata"]["pyproject_present"] is True
    assert report["venv_metadata"]["running_inside_venv"] is True


def test_current_host_mode_fails_python_version_below_3_12(tmp_path: Path) -> None:
    module = _module()
    repo = _synthetic_repo(tmp_path)

    report = module.generate_report(mode="current-host", repo_root=repo, version_info=(3, 11))

    assert report["contract_status"] == "failed"
    assert "python_version_below_3_12" in report["checks_failed"]


def test_current_host_mode_passes_supported_python_version(tmp_path: Path) -> None:
    module = _module()
    repo = _synthetic_repo(tmp_path)

    report = module.generate_report(mode="current-host", repo_root=repo, version_info=(3, 12))

    assert report["contract_status"] == "passed"
    assert report["venv_metadata"]["python_version_supported"] is True


def test_current_host_mode_fails_missing_core_repo_files(tmp_path: Path) -> None:
    module = _module()
    repo = _synthetic_repo(tmp_path, include_core=False)

    report = module.generate_report(mode="current-host", repo_root=repo, version_info=(3, 12))

    assert report["contract_status"] == "failed"
    assert "pyproject_present_missing" in report["checks_failed"]
    assert "docs_project_source_present_missing" in report["checks_failed"]
    assert all(set(failure) == {"check", "reason_code"} for failure in report["failures"])


def test_current_host_mode_does_not_print_raw_repo_path(tmp_path: Path) -> None:
    module = _module()
    repo = _synthetic_repo(tmp_path)

    report = module.generate_report(mode="current-host", repo_root=repo, version_info=(3, 12))
    rendered = module.render_json(report)

    assert str(repo) not in rendered
    assert str(tmp_path) not in rendered
    assert report["redaction"]["raw_paths_printed"] is False


def test_current_host_mode_does_not_print_username_home_hostname_or_ip(tmp_path: Path) -> None:
    module = _module()
    repo = _synthetic_repo(tmp_path)

    report = module.generate_report(
        mode="current-host",
        repo_root=repo,
        platform_system=lambda: "Linux",
        platform_machine=lambda: "x86_64",
        version_info=(3, 12),
    )
    rendered = module.render_json(report)
    forbidden = {
        getpass.getuser(),
        str(Path.home()),
        socket.gethostname(),
        "127.0.0.1",
        "::1",
    }

    for value in forbidden:
        if value:
            assert value not in rendered
    assert report["redaction"]["hostname_printed"] is False
    assert report["redaction"]["username_printed"] is False
    assert report["redaction"]["home_path_printed"] is False
    assert report["redaction"]["ip_address_printed"] is False


def test_script_does_not_call_subprocess() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "subprocess" not in text


def test_script_does_not_invoke_git() -> None:
    text = SCRIPT.read_text(encoding="utf-8").lower()

    assert "git " not in text
    assert "git(" not in text
    assert "git." not in text


def test_script_does_not_inspect_trading_bot_repo() -> None:
    text = SCRIPT.read_text(encoding="utf-8").lower()

    assert "trading-bot" not in text
    assert "trading_bot_repo_inspected" in text
    assert "trading_bot_paths_touched" in text


def test_script_does_not_create_files_or_directories(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    repo = _synthetic_repo(tmp_path)

    def fail_create(*args, **kwargs):  # noqa: ANN001
        raise AssertionError("preflight must not create files or directories")

    monkeypatch.setattr(module.Path, "mkdir", fail_create)
    monkeypatch.setattr(module.Path, "write_text", fail_create)
    monkeypatch.setattr(module.Path, "touch", fail_create)
    monkeypatch.setattr(module.Path, "open", fail_create)

    report = module.generate_report(mode="current-host", repo_root=repo, version_info=(3, 12))

    assert report["contract_status"] == "passed"
    assert report["side_effects"]["production_files_created"] is False


def test_script_does_not_invoke_docker_or_systemd() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "os.system" not in text
    assert "systemctl" not in text
    assert "docker compose" not in text
    assert "docker-compose" not in text


def test_script_does_not_connect_to_db_redis_or_network() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "create_async_engine" not in text
    assert "psycopg" not in text
    assert "Redis.from_url" not in text
    assert "socket" not in text
    assert "urllib" not in text
    assert "requests" not in text


def test_live_ingest_authorized_is_false() -> None:
    report = _module().generate_report()

    assert report["authorization"]["live_ingest_authorized"] is False


def test_production_rollout_authorized_is_false() -> None:
    report = _module().generate_report()

    assert report["authorization"]["production_rollout_authorized"] is False
    assert "Dedicated VPS baseline preflight success does not authorize live ingest or production rollout." in report[
        "notes"
    ]


def test_runbook_includes_dedicated_vps_and_trading_bot_separation_warning() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    for marker in (
        "dedicated `github_ai_catchbot` VPS",
        "must not be shared with the existing trading-bot VPS",
        "separate operational failure domains",
        "This check does not inspect the trading-bot repository.",
        "No trading-bot path is touched.",
        "No production rollout is authorized.",
    ):
        assert marker in text


def test_json_cli_default_schema_mode_passes() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["report_type"] == "dedicated_vps_baseline_preflight_v1"
    assert report["mode"] == "schema"
    assert report["contract_status"] == "passed"
