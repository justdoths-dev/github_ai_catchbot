from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_tdlib_session_reuse_collector_readiness_preflight.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password@127.0.0.1:6379/0"
FAKE_API_HASH = "0123456789abcdef0123456789abcdef"
FAKE_PHONE_NUMBER = "+15551234567"
FAKE_TDLIB_KEY = "unit-tdlib-encryption-key"
FAKE_LOGIN_CODE = "12345"
FAKE_2FA_PASSWORD = "unit two factor password"


def _module():
    from scripts.ops import (
        dedicated_vps_tdlib_session_reuse_collector_readiness_preflight as module,
    )

    return module


def _write_runtime_env(
    tmp_path: Path,
    *,
    create_state_dir: bool = True,
    create_files_dir: bool = True,
    state_entries: int = 1,
    **overrides: str | None,
) -> Path:
    state_dir = tmp_path / "tdlib-state"
    files_dir = tmp_path / "tdlib-files"
    if create_state_dir:
        state_dir.mkdir()
        for index in range(state_entries):
            (state_dir / f"entry-{index}.bin").write_text("fixture", encoding="utf-8")
    if create_files_dir:
        files_dir.mkdir()

    values: dict[str, str] = {
        "APP_ENV": "prod",
        "COLLECTOR_MODE": "live",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "TELEGRAM_API_ID": "123456",
        "TELEGRAM_API_HASH": FAKE_API_HASH,
        "TELEGRAM_PHONE_NUMBER": FAKE_PHONE_NUMBER,
        "TELEGRAM_2FA_PASSWORD": FAKE_2FA_PASSWORD,
        "TELEGRAM_LOGIN_CODE": FAKE_LOGIN_CODE,
        "TDLIB_DB_ENCRYPTION_KEY": FAKE_TDLIB_KEY,
        "TDLIB_STATE_DIR": str(state_dir),
        "TDLIB_FILES_DIR": str(files_dir),
    }
    for key, value in overrides.items():
        if value is None:
            values.pop(key, None)
        else:
            values[key] = value

    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()),
        encoding="utf-8",
    )
    return runtime_env


def _noop_tdjson(_repo_root: Path, _values: dict[str, str]) -> None:
    return None


def _report(tmp_path: Path, **overrides: str | None) -> dict[str, object]:
    module = _module()
    runtime_env = _write_runtime_env(tmp_path, **overrides)
    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
    )
    return result.report


def _render(report: dict[str, object]) -> str:
    return _module().render_json(report)


def test_default_no_runtime_env_shape_is_safe(tmp_path: Path) -> None:
    module = _module()
    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=tmp_path / "missing-runtime.env",
        tdjson_availability_checker=_noop_tdjson,
    )

    report = result.report
    assert result.exit_code != 0
    assert report["report_type"] == module.REPORT_TYPE
    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert report["runtime_env_read"] is False
    assert "runtime_env.unreadable" in report["checks_failed"]
    for key in module.SIDE_EFFECT_FLAGS:
        assert report[key] is False


def test_cli_outputs_json_without_runtime_values_on_missing_env(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(tmp_path / "missing-runtime.env"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert report["runtime_env_values_printed"] is False
    assert report["secret_values_printed"] is False


def test_runtime_env_unreadable_fails_closed(tmp_path: Path) -> None:
    report = _module().generate_report(
        repo_root=ROOT,
        runtime_env_path=tmp_path,
        tdjson_availability_checker=_noop_tdjson,
    ).report

    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert report["runtime_env_read"] is False
    assert "runtime_env.unreadable" in report["checks_failed"]


def test_missing_required_keys_fail_closed_without_printing_values(tmp_path: Path) -> None:
    report = _report(tmp_path, TELEGRAM_API_HASH=None)
    rendered = _render(report)

    assert report["contract_status"] == "blocked_runtime_env_required_keys_missing"
    assert report["required_runtime_keys_present"]["TELEGRAM_API_HASH"] is False
    assert report["required_runtime_keys_nonempty"]["TELEGRAM_API_HASH"] is False
    assert "runtime_env.required_keys" in report["checks_failed"]
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_REDIS_URL not in rendered
    assert FAKE_PHONE_NUMBER not in rendered
    assert FAKE_TDLIB_KEY not in rendered


def test_placeholder_like_required_values_fail_closed_without_printing_values(
    tmp_path: Path,
) -> None:
    report = _report(tmp_path, TELEGRAM_API_HASH="CHANGE_ME")
    rendered = _render(report)

    assert report["contract_status"] == "blocked_runtime_env_placeholder_like_values"
    assert report["placeholder_like_required_values_detected"] is True
    assert report["placeholder_like_required_keys"] == ["TELEGRAM_API_HASH"]
    assert "CHANGE_ME" not in rendered


def test_invalid_collector_config_fails_closed_without_printing_values(
    tmp_path: Path,
) -> None:
    report = _report(tmp_path, COLLECTOR_MODE="replay")
    rendered = _render(report)

    assert report["contract_status"] == "blocked_collector_config_invalid"
    assert report["collector_config_built"] is False
    assert report["collector_config_error_type"] == "ConfigurationError"
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_API_HASH not in rendered
    assert FAKE_PHONE_NUMBER not in rendered


def test_tdjson_unavailable_fails_closed_before_readiness_pass(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _write_runtime_env(tmp_path)

    def unavailable(_repo_root: Path, _values: dict[str, str]) -> None:
        raise OSError("native loader unavailable")

    report = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=unavailable,
    ).report

    assert report["contract_status"] == "blocked_tdjson_unavailable"
    assert report["tdjson_available"] is False
    assert report["tdjson_error_type"] == "OSError"
    assert report["tdlib_session_reuse_candidate"] is True


def test_valid_config_tdjson_and_session_metadata_pass(tmp_path: Path) -> None:
    report = _report(tmp_path)

    assert report["contract_status"] == "collector_readiness_preflight_passed"
    assert report["runtime_env_read"] is True
    assert report["tdjson_available"] is True
    assert report["collector_config_built"] is True
    assert report["TELEGRAM_API_ID_present"] is True
    assert report["TELEGRAM_API_ID_positive_int"] is True
    assert report["TELEGRAM_API_HASH_hex32_like"] is True
    assert report["TELEGRAM_PHONE_NUMBER_e164_like"] is True
    assert report["TDLIB_DB_ENCRYPTION_KEY_nontrivial_len"] is True
    assert report["tdlib_state_dir_present"] is True
    assert report["tdlib_state_dir_is_dir"] is True
    assert report["tdlib_files_dir_present"] is True
    assert report["tdlib_files_dir_is_dir"] is True
    assert report["tdlib_session_metadata_checked"] is True
    assert report["tdlib_session_file_count_bucket"] == "one_to_five"
    assert report["tdlib_session_reuse_candidate"] is True
    assert report["checks_failed"] == []


def test_tdlib_state_dir_absent_blocks_after_safe_checks(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _write_runtime_env(tmp_path, create_state_dir=False)
    report = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
    ).report

    assert report["contract_status"] == "blocked_tdlib_state_dir_missing"
    assert report["tdlib_state_dir_present"] is False
    assert report["tdlib_session_reuse_candidate"] is False


def test_empty_state_dir_blocks_session_reuse_confirmation(tmp_path: Path) -> None:
    runtime_env = _write_runtime_env(tmp_path, state_entries=0)
    report = _module().generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
    ).report

    assert report["contract_status"] == "blocked_tdlib_session_reuse_not_confirmed"
    assert report["tdlib_state_dir_has_entries"] is False
    assert report["tdlib_session_file_count_bucket"] == "zero"
    assert report["tdlib_session_reuse_candidate"] is False


def test_report_excludes_runtime_env_secret_and_auth_values(tmp_path: Path) -> None:
    report = _report(tmp_path)
    rendered = _render(report)

    forbidden_fragments = (
        FAKE_DATABASE_URL,
        "unit-db-password",
        FAKE_REDIS_URL,
        "unit-redis-password",
        FAKE_API_HASH,
        FAKE_PHONE_NUMBER,
        FAKE_LOGIN_CODE,
        FAKE_2FA_PASSWORD,
        FAKE_TDLIB_KEY,
    )
    for fragment in forbidden_fragments:
        assert fragment not in rendered
    assert report["runtime_env_values_printed"] is False
    assert report["secret_values_printed"] is False
    assert report["tdlib_session_values_printed"] is False
    assert report["login_code_value_printed"] is False
    assert report["login_code_value_stored"] is False


def test_script_imports_no_runtime_service_main_or_auth_modules() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)

    forbidden_import_fragments = (
        "src.services.collector_telegram.runtime",
        "src.services.collector_telegram.service",
        "src.services.collector_telegram.main",
        "src.services.collector_telegram.auth_entrypoint",
        "psycopg",
        "redis",
        "alembic",
    )
    assert not [
        name
        for name in imported
        if any(fragment in name for fragment in forbidden_import_fragments)
    ]

    source = SCRIPT.read_text(encoding="utf-8")
    forbidden_source_fragments = (
        "CollectorTelegramService",
        "CollectorRuntime",
        "run_tdlib_auth_only_once",
        "TDLibAuthOnlyRunner",
    )
    for fragment in forbidden_source_fragments:
        assert fragment not in source


def test_script_does_not_call_auth_runtime_db_redis_or_tdjson_initialize() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    forbidden_calls: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if isinstance(function, ast.Name) and function.id in {
            "run_tdlib_auth_only_once",
            "TDLibAuthOnlyRunner",
        }:
            forbidden_calls.append(function.id)
        if isinstance(function, ast.Attribute) and function.attr in {
            "initialize",
            "connect",
            "execute",
        }:
            forbidden_calls.append(function.attr)

    assert forbidden_calls == []


def test_source_does_not_target_codex_caches_venv_or_logs() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    for fragment in (".codex", "__pycache__", "venv", "/var/log"):
        assert fragment not in source


def test_generate_report_does_not_mutate_fixture_files(tmp_path: Path) -> None:
    runtime_env = _write_runtime_env(tmp_path)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))

    result = _module().generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
    )

    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert result.report["files_mutated"] is False
    assert after == before
