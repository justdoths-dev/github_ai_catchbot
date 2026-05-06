from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "collector_restricted_environment_inventory_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "collector_restricted_environment_inventory_check.md"

SECRET_SENTINELS = (
    "SYNTHETIC_API_HASH_VALUE_DO_NOT_PRINT",
    "SYNTHETIC_2FA_VALUE_DO_NOT_PRINT",
    "SYNTHETIC_TDLIB_DB_KEY_DO_NOT_PRINT",
)
DATABASE_URL_SENTINEL = "postgresql+asyncpg://collector:database-secret@db.example.invalid/catchbot"
REDIS_URL_SENTINEL = "redis://:redis-secret@redis.example.invalid:6379/0"
PHONE_SENTINEL = "+15551234567"


def _module():
    from scripts.ops import collector_restricted_environment_inventory_check as module

    return module


def _synthetic_env(tmp_path: Path) -> dict[str, str]:
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    api_hash_file = secret_dir / "api_hash"
    password_file = secret_dir / "telegram_2fa"
    encryption_file = secret_dir / "tdlib_db_key"
    api_hash_file.write_text(SECRET_SENTINELS[0], encoding="utf-8")
    password_file.write_text(SECRET_SENTINELS[1], encoding="utf-8")
    encryption_file.write_text(SECRET_SENTINELS[2], encoding="utf-8")

    lock_parent = tmp_path / "locks"
    lock_parent.mkdir()

    return {
        "APP_ENV": "dev",
        "DATABASE_URL": DATABASE_URL_SENTINEL,
        "REDIS_URL": REDIS_URL_SENTINEL,
        "COLLECTOR_MODE": "replay",
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_API_HASH_FILE": str(api_hash_file),
        "TELEGRAM_PHONE_NUMBER": PHONE_SENTINEL,
        "TELEGRAM_2FA_PASSWORD_FILE": str(password_file),
        "TDLIB_STATE_DIR": str(tmp_path / "tdlib-state"),
        "TDLIB_FILES_DIR": str(tmp_path / "tdlib-files"),
        "TDLIB_DB_ENCRYPTION_KEY_FILE": str(encryption_file),
        "COLLECTOR_SINGLETON_LOCK_PATH": str(lock_parent / "collector.lock"),
    }


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_report_type_and_authorization_are_locked() -> None:
    module = _module()

    report = module.generate_report()

    assert module.REPORT_TYPE == "collector_restricted_environment_inventory_v1"
    assert report["report_type"] == "collector_restricted_environment_inventory_v1"
    assert report["authorization"]["live_ingest_authorized"] is False
    assert report["authorization"]["production_rollout_authorized"] is False


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
    assert "--mode {schema,current-env}" in output


def test_default_json_report_has_expected_type_and_passes_without_prod_env(monkeypatch) -> None:
    module = _module()
    monkeypatch.setattr(os, "environ", {})

    report = module.generate_report()

    assert report["report_type"] == "collector_restricted_environment_inventory_v1"
    assert report["mode"] == "schema"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["inventory"]["required"]["DATABASE_URL"]["status"] == "not_checked"


def test_current_env_mode_reports_missing_required_env_names_as_failures() -> None:
    module = _module()

    report = module.generate_report(mode="current-env", environ={})

    assert report["contract_status"] == "failed"
    assert "APP_ENV.missing" in report["checks_failed"]
    assert "DATABASE_URL.missing" in report["checks_failed"]
    assert "REDIS_URL.missing" in report["checks_failed"]
    assert "TELEGRAM_API_HASH_FILE.missing" in report["checks_failed"]
    assert "COLLECTOR_SINGLETON_LOCK_PATH.missing" not in report["checks_failed"]


def test_current_env_mode_with_synthetic_env_and_temp_files_passes(tmp_path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["path_metadata"]["TELEGRAM_API_HASH_FILE"] == {
        "checked": True,
        "path_present": True,
        "exists": True,
        "is_file": True,
        "readable": True,
        "parent_exists": True,
    }
    assert report["path_metadata"]["TDLIB_STATE_DIR"]["exists"] is False
    assert report["path_metadata"]["TDLIB_STATE_DIR"]["writable_or_creatable"] is True
    assert report["path_metadata"]["COLLECTOR_SINGLETON_LOCK_PATH"]["parent_writable"] is True


def test_optional_tuning_variables_are_not_required(tmp_path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)

    report = module.generate_report(mode="current-env", environ=env)

    for name, entry in report["inventory"]["optional"].items():
        assert entry["status"] == "missing", name
    assert report["contract_status"] == "passed"


def test_singleton_lock_missing_is_not_applicable_when_tdlib_state_dir_present(tmp_path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    env.pop("COLLECTOR_SINGLETON_LOCK_PATH")

    report = module.generate_report(mode="current-env", environ=env)

    entry = report["inventory"]["required"]["COLLECTOR_SINGLETON_LOCK_PATH"]
    assert entry["status"] == "not_applicable"
    assert report["path_metadata"]["COLLECTOR_SINGLETON_LOCK_PATH"]["checked"] is False
    assert report["contract_status"] == "passed"


def test_secret_values_urls_phone_and_raw_paths_do_not_appear_in_output(tmp_path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)

    report = module.generate_report(mode="current-env", environ=env)
    rendered = module.render_json(report)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json", "--mode", "current-env"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
        env={**env, "PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(ROOT)},
    )
    combined = f"{result.stdout}\n{result.stderr}\n{rendered}"

    assert result.returncode == 0
    for forbidden in (
        *SECRET_SENTINELS,
        DATABASE_URL_SENTINEL,
        REDIS_URL_SENTINEL,
        PHONE_SENTINEL,
        str(tmp_path),
        env["TELEGRAM_API_HASH_FILE"],
        env["TDLIB_STATE_DIR"],
        env["COLLECTOR_SINGLETON_LOCK_PATH"],
    ):
        assert forbidden not in combined


def test_secret_file_contents_are_not_read(tmp_path, monkeypatch) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)

    def fail_read_text(self, *args, **kwargs):  # noqa: ANN001
        raise AssertionError(f"secret file contents must not be read: {self}")

    monkeypatch.setattr(module.Path, "read_text", fail_read_text)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert report["redaction"]["secret_file_contents_read"] is False


def test_tdlib_directories_are_not_created(tmp_path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    state_dir = Path(env["TDLIB_STATE_DIR"])
    files_dir = Path(env["TDLIB_FILES_DIR"])

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert not state_dir.exists()
    assert not files_dir.exists()
    assert report["side_effects"]["production_files_created"] is False


def test_singleton_lock_is_not_acquired_or_created(tmp_path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    lock_path = Path(env["COLLECTOR_SINGLETON_LOCK_PATH"])

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert not lock_path.exists()
    assert report["side_effects"]["singleton_lock_acquired"] is False


def test_no_forbidden_runtime_imports_or_invocations() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "subprocess" not in text
    assert "create_async_engine" not in text
    assert "Redis.from_url" not in text
    assert "redis.Redis" not in text
    assert "import socket" not in text
    assert "import requests" not in text
    assert "from requests" not in text
    assert "import httpx" not in text
    assert "from httpx" not in text
    assert "DockerClient" not in text
    assert "systemctl" not in text
    assert "tdjson" not in text


def test_side_effect_and_redaction_booleans_are_false() -> None:
    module = _module()

    report = module.generate_report()

    assert report["redaction"] == {
        "env_values_printed": False,
        "secret_values_printed": False,
        "secret_file_contents_read": False,
        "raw_paths_printed": False,
        "database_url_printed": False,
        "redis_url_printed": False,
        "phone_number_printed": False,
    }
    assert report["side_effects"] == {
        "tdlib_started": False,
        "telegram_called": False,
        "db_connection_attempted": False,
        "redis_connection_attempted": False,
        "external_network_attempted": False,
        "docker_invoked": False,
        "systemd_invoked": False,
        "env_or_feature_flags_mutated": False,
        "production_files_created": False,
        "singleton_lock_acquired": False,
    }


def test_runbook_contains_hard_boundary_warnings() -> None:
    text = RUNBOOK.read_text(encoding="utf-8").lower()

    assert "inventory success does not authorize live ingest or production rollout" in text
    assert "this does not start tdlib" in text
    assert "this does not call telegram" in text
    assert "this does not connect to postgresql" in text
    assert "this does not connect to redis" in text
    assert "this does not invoke docker" in text
    assert "systemd" in text
    assert "this does not create tdlib directories" in text
    assert "this does not acquire the real configured singleton lock" in text
    assert "this does not read secret file contents" in text
    assert "this does not print env values" in text


def test_render_json_is_deterministic_json() -> None:
    module = _module()

    rendered = module.render_json(module.generate_report())

    assert json.loads(rendered)["report_type"] == "collector_restricted_environment_inventory_v1"


@pytest.mark.parametrize("args", [[], ["--format", "json"]])
def test_cli_outputs_default_json(args) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["report_type"] == "collector_restricted_environment_inventory_v1"
    assert report["mode"] == "schema"
