from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "collector_sensitive_path_permission_check.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "collector_sensitive_path_permission_check.md"

SECRET_SENTINELS = (
    "SYNTHETIC_API_HASH_VALUE_DO_NOT_PRINT",
    "SYNTHETIC_2FA_VALUE_DO_NOT_PRINT",
    "SYNTHETIC_TDLIB_DB_KEY_DO_NOT_PRINT",
)
DATABASE_URL_SENTINEL = "postgresql+asyncpg://collector:database-secret@db.example.invalid/catchbot"
REDIS_URL_SENTINEL = "redis://:redis-secret@redis.example.invalid:6379/0"
PHONE_SENTINEL = "+15551234567"


def _module():
    from scripts.ops import collector_sensitive_path_permission_check as module

    return module


def _write_secret(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)


def _synthetic_env(tmp_path: Path) -> dict[str, str]:
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    api_hash_file = secret_dir / "api_hash"
    password_file = secret_dir / "telegram_2fa"
    encryption_file = secret_dir / "tdlib_db_key"
    _write_secret(api_hash_file, SECRET_SENTINELS[0])
    _write_secret(password_file, SECRET_SENTINELS[1])
    _write_secret(encryption_file, SECRET_SENTINELS[2])

    state_parent = tmp_path / "state-parent"
    files_parent = tmp_path / "files-parent"
    lock_parent = tmp_path / "locks"
    state_parent.mkdir()
    files_parent.mkdir()
    lock_parent.mkdir()
    for path in (state_parent, files_parent, lock_parent):
        path.chmod(0o700)

    return {
        "TELEGRAM_API_HASH_FILE": str(api_hash_file),
        "TELEGRAM_2FA_PASSWORD_FILE": str(password_file),
        "TDLIB_DB_ENCRYPTION_KEY_FILE": str(encryption_file),
        "TDLIB_STATE_DIR": str(state_parent / "tdlib-state"),
        "TDLIB_FILES_DIR": str(files_parent / "tdlib-files"),
        "COLLECTOR_SINGLETON_LOCK_PATH": str(lock_parent / "collector.lock"),
        "DATABASE_URL": DATABASE_URL_SENTINEL,
        "REDIS_URL": REDIS_URL_SENTINEL,
        "TELEGRAM_PHONE_NUMBER": PHONE_SENTINEL,
    }


def _rendered_current_env_output(env: dict[str, str]) -> str:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json", "--mode", "current-env"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
        env={**env, "PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(ROOT)},
    )
    return f"{result.stdout}\n{result.stderr}"


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_report_type_and_authorization_are_locked() -> None:
    module = _module()

    report = module.generate_report()

    assert module.REPORT_TYPE == "collector_sensitive_path_permission_v1"
    assert report["report_type"] == "collector_sensitive_path_permission_v1"
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

    assert report["report_type"] == "collector_sensitive_path_permission_v1"
    assert report["mode"] == "schema"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["secret_file_permissions"]["TELEGRAM_API_HASH_FILE"]["permission_status"] == "not_checked"


def test_current_env_mode_with_secure_synthetic_files_dirs_passes(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["secret_file_permissions"]["TELEGRAM_API_HASH_FILE"]["permission_status"] == "ok"
    assert report["tdlib_path_permissions"]["TDLIB_STATE_DIR"]["permission_status"] == "ok"
    assert report["tdlib_path_permissions"]["TDLIB_STATE_DIR"]["exists"] is False
    assert report["tdlib_path_permissions"]["TDLIB_STATE_DIR"]["writable_by_process_or_parent"] is True
    assert (
        report["singleton_lock_parent_permissions"]["COLLECTOR_SINGLETON_LOCK_PATH"]["permission_status"]
        == "ok"
    )


def test_current_env_mode_fails_missing_required_secret_file_path(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    env.pop("TELEGRAM_API_HASH_FILE")

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TELEGRAM_API_HASH_FILE.missing" in report["checks_failed"]


def test_current_env_mode_fails_secret_file_that_is_not_file(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    directory = tmp_path / "secret-dir"
    directory.mkdir()
    env["TELEGRAM_API_HASH_FILE"] = str(directory)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TELEGRAM_API_HASH_FILE.not_file" in report["checks_failed"]


def test_current_env_mode_fails_unreadable_secret_file_with_monkeypatch(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    real_access = module.os.access

    def fake_access(path, mode, *args, **kwargs):  # noqa: ANN001
        if Path(path) == Path(env["TELEGRAM_API_HASH_FILE"]) and mode == os.R_OK:
            return False
        return real_access(path, mode, *args, **kwargs)

    monkeypatch.setattr(module.os, "access", fake_access)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TELEGRAM_API_HASH_FILE.unreadable" in report["checks_failed"]


def test_current_env_mode_fails_world_readable_secret_file(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    Path(env["TELEGRAM_API_HASH_FILE"]).chmod(0o604)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TELEGRAM_API_HASH_FILE.world_readable" in report["checks_failed"]


def test_current_env_mode_fails_group_writable_secret_file(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    Path(env["TELEGRAM_API_HASH_FILE"]).chmod(0o620)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TELEGRAM_API_HASH_FILE.group_writable" in report["checks_failed"]


def test_current_env_mode_fails_world_writable_secret_file(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    Path(env["TELEGRAM_API_HASH_FILE"]).chmod(0o602)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TELEGRAM_API_HASH_FILE.world_writable" in report["checks_failed"]


def test_current_env_mode_fails_symlink_secret_file(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    target = tmp_path / "target-secret"
    _write_secret(target, "target")
    symlink_path = tmp_path / "secret-link"
    symlink_path.symlink_to(target)
    env["TELEGRAM_API_HASH_FILE"] = str(symlink_path)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TELEGRAM_API_HASH_FILE.symlink" in report["checks_failed"]


def test_current_env_mode_fails_tdlib_existing_path_that_is_not_directory(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    bad_path = tmp_path / "tdlib-state-file"
    bad_path.write_text("not a dir", encoding="utf-8")
    bad_path.chmod(0o600)
    env["TDLIB_STATE_DIR"] = str(bad_path)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TDLIB_STATE_DIR.not_dir" in report["checks_failed"]


def test_current_env_mode_fails_tdlib_or_parent_world_writable_path(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    world_parent = tmp_path / "world-parent"
    world_parent.mkdir()
    world_parent.chmod(0o777)
    env["TDLIB_STATE_DIR"] = str(world_parent / "missing-tdlib-state")

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "TDLIB_STATE_DIR.world_writable" in report["checks_failed"]


def test_current_env_mode_does_not_create_missing_tdlib_directories(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    state_dir = Path(env["TDLIB_STATE_DIR"])
    files_dir = Path(env["TDLIB_FILES_DIR"])

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert not state_dir.exists()
    assert not files_dir.exists()
    assert report["side_effects"]["production_files_created"] is False


def test_current_env_mode_fails_singleton_lock_parent_missing(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    env["COLLECTOR_SINGLETON_LOCK_PATH"] = str(tmp_path / "missing-parent" / "collector.lock")

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "COLLECTOR_SINGLETON_LOCK_PATH.missing" in report["checks_failed"]


def test_current_env_mode_fails_singleton_lock_parent_world_writable(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    world_parent = tmp_path / "world-locks"
    world_parent.mkdir()
    world_parent.chmod(0o777)
    env["COLLECTOR_SINGLETON_LOCK_PATH"] = str(world_parent / "collector.lock")

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert "COLLECTOR_SINGLETON_LOCK_PATH.world_writable" in report["checks_failed"]


def test_singleton_lock_missing_is_not_applicable_when_tdlib_state_dir_present(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    env.pop("COLLECTOR_SINGLETON_LOCK_PATH")

    report = module.generate_report(mode="current-env", environ=env)

    entry = report["singleton_lock_parent_permissions"]["COLLECTOR_SINGLETON_LOCK_PATH"]
    assert entry["permission_status"] == "not_applicable"
    assert entry["checked"] is False
    assert report["contract_status"] == "passed"


def test_current_env_mode_does_not_create_singleton_lock_file_or_acquire_lock(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    lock_path = Path(env["COLLECTOR_SINGLETON_LOCK_PATH"])

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert not lock_path.exists()
    assert report["side_effects"]["singleton_lock_acquired"] is False


def test_secret_file_contents_are_not_read(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)

    def fail_read_text(self, *args, **kwargs):  # noqa: ANN001
        raise AssertionError(f"secret file contents must not be read: {self}")

    def fail_open(self, *args, **kwargs):  # noqa: ANN001
        raise AssertionError(f"secret file contents must not be opened: {self}")

    monkeypatch.setattr(module.Path, "read_text", fail_read_text)
    monkeypatch.setattr(module.Path, "open", fail_open)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "passed"
    assert report["redaction"]["secret_file_contents_read"] is False
    assert report["side_effects"]["secret_file_contents_read"] is False


def test_raw_values_paths_uid_gid_and_mode_bits_do_not_appear_in_output(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)

    report = module.generate_report(mode="current-env", environ=env)
    rendered = module.render_json(report)
    combined = f"{_rendered_current_env_output(env)}\n{rendered}"
    stat_result = Path(env["TELEGRAM_API_HASH_FILE"]).stat()
    forbidden = (
        *SECRET_SENTINELS,
        DATABASE_URL_SENTINEL,
        REDIS_URL_SENTINEL,
        PHONE_SENTINEL,
        str(tmp_path),
        env["TELEGRAM_API_HASH_FILE"],
        env["TDLIB_STATE_DIR"],
        env["COLLECTOR_SINGLETON_LOCK_PATH"],
        str(stat_result.st_uid),
        str(stat_result.st_gid),
        str(stat.S_IMODE(stat_result.st_mode)),
        oct(stat.S_IMODE(stat_result.st_mode)),
    )

    for value in forbidden:
        assert value not in combined
    assert report["redaction"]["uid_gid_printed"] is False
    assert report["redaction"]["mode_bits_printed"] is False
    assert report["redaction"]["database_url_printed"] is False
    assert report["redaction"]["redis_url_printed"] is False
    assert report["redaction"]["phone_number_printed"] is False


def test_failures_identify_env_name_and_reason_code_only(tmp_path: Path) -> None:
    module = _module()
    env = _synthetic_env(tmp_path)
    Path(env["TELEGRAM_API_HASH_FILE"]).chmod(0o604)

    report = module.generate_report(mode="current-env", environ=env)

    assert report["contract_status"] == "failed"
    assert report["failures"]
    assert all(set(failure) == {"env_name", "reason_code"} for failure in report["failures"])


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
    assert report["report_type"] == "collector_sensitive_path_permission_v1"
    assert report["mode"] == "schema"
    assert report["contract_status"] == "passed"


def test_live_and_rollout_authorization_remain_false(tmp_path: Path) -> None:
    module = _module()
    report = module.generate_report(mode="current-env", environ=_synthetic_env(tmp_path))

    assert report["authorization"]["live_ingest_authorized"] is False
    assert report["authorization"]["production_rollout_authorized"] is False
    assert "Permission check success does not authorize live ingest or production rollout." in report["notes"]


def test_runbook_includes_hard_boundary_warnings() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    for marker in (
        "This is not live ingest.",
        "This is not TDLib auth.",
        "No DB connection is attempted.",
        "No Redis connection is attempted.",
        "No real singleton lock is acquired.",
        "No secret file content is read.",
        "No live ingest is authorized.",
        "No production rollout is authorized.",
    ):
        assert marker in text


def test_no_forbidden_runtime_imports_or_invocations() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "subprocess" not in text
    assert "create_async_engine" not in text
    assert "Redis.from_url" not in text
    assert "CollectorSingletonGuard" not in text
    assert ".read_text(" not in text
    assert ".open(" not in text
