from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_tdlib_parameters_redacted_config_diagnostic.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password@127.0.0.1:6379/0"
FAKE_API_HASH = "0123456789abcdef0123456789abcdef"
FAKE_PHONE_NUMBER = "+15551234567"
FAKE_TDLIB_KEY = "unit-tdlib-db-encryption-key"


def _module():
    from scripts.ops import (
        dedicated_vps_tdlib_parameters_redacted_config_diagnostic as module,
    )

    return module


def _runtime_env(tmp_path: Path, **overrides: str | None) -> dict[str, str]:
    state_dir = tmp_path / "tdlib-state"
    files_dir = tmp_path / "tdlib-files"
    state_dir.mkdir(exist_ok=True)
    files_dir.mkdir(exist_ok=True)

    values = {
        "APP_ENV": "prod",
        "COLLECTOR_MODE": "live",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "TELEGRAM_API_ID": "123456",
        "TELEGRAM_API_HASH": FAKE_API_HASH,
        "TELEGRAM_PHONE_NUMBER": FAKE_PHONE_NUMBER,
        "TDLIB_DB_ENCRYPTION_KEY": FAKE_TDLIB_KEY,
        "TDLIB_STATE_DIR": str(state_dir),
        "TDLIB_FILES_DIR": str(files_dir),
    }
    for key, value in overrides.items():
        if value is None:
            values.pop(key, None)
        else:
            values[key] = value
    return values


def _tdjson_ok(_values: dict[str, str]) -> dict[str, bool]:
    return {
        "tdjson_library_path_present": True,
        "tdjson_default_path_checked": True,
        "tdjson_available_import_check_performed": True,
        "tdjson_available": True,
    }


def _report(
    values: dict[str, str],
    **kwargs: Any,
) -> dict[str, Any]:
    result = _module().generate_report(
        repo_root=ROOT,
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=lambda _path: values,
        tdjson_availability_checker=_tdjson_ok,
        **kwargs,
    )
    return result.report


def _render(report: dict[str, Any]) -> str:
    return json.dumps(report, sort_keys=True)


def _payload(tmp_path: Path, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "@type": "setTdlibParameters",
        "use_test_dc": False,
        "database_directory": str(tmp_path / "tdlib-state"),
        "files_directory": str(tmp_path / "tdlib-files"),
        "use_file_database": True,
        "use_chat_info_database": True,
        "use_message_database": True,
        "use_secret_chats": False,
        "api_id": 123456,
        "api_hash": FAKE_API_HASH,
        "system_language_code": "en",
        "device_model": "catchbot-vps",
        "system_version": "linux",
        "application_version": "0.1.0",
        "database_encryption_key": "encoded-key-redacted-by-report",
    }
    for key, value in overrides.items():
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = value
    return payload


def _builder(payload: dict[str, Any]):
    def build(_repo_root: Path, _config: object, _values: dict[str, str]) -> dict[str, Any]:
        return dict(payload)

    return build


def test_matching_auth_and_resolve_shapes_pass_with_default_builders(tmp_path: Path) -> None:
    report = _report(_runtime_env(tmp_path))

    assert report["contract_status"] == "tdlib_parameters_redacted_config_diagnostic_passed"
    assert report["runtime_env_read"] is True
    assert report["collector_config_built"] is True
    assert report["auth_path_parameters_inspected"] is True
    assert report["resolve_path_parameters_inspected"] is True
    assert report["parameter_shapes_equivalent"] is True
    assert report["required_parameter_fields_missing"] == []
    assert report["required_parameter_field_types_ok"] is True
    assert report["tdlib_state_dir_present"] is True
    assert report["tdlib_state_dir_is_dir"] is True
    assert report["tdlib_state_dir_writable"] is True
    assert report["tdlib_files_dir_present"] is True
    assert report["tdlib_files_dir_is_dir"] is True
    assert report["tdlib_files_dir_writable"] is True
    assert report["tdlib_db_encryption_key_source_kind"] == "env"
    assert report["tdlib_db_encryption_key_non_empty"] is True
    assert report["tdjson_available"] is True


def test_shape_mismatch_returns_blocked_parameter_shape_mismatch(tmp_path: Path) -> None:
    base = _payload(tmp_path)
    resolve = {**base, "extra_shape_flag": True}
    report = _report(
        _runtime_env(tmp_path),
        auth_parameter_builder=_builder(base),
        resolve_parameter_builder=_builder(resolve),
    )

    assert report["contract_status"] == "blocked_parameter_shape_mismatch"
    assert report["parameter_shapes_equivalent"] is False
    assert any("missing_in_auth: extra_shape_flag" in item for item in report["differences_summary"])
    assert "extra_shape_flag" in _render(report)
    assert FAKE_API_HASH not in _render(report)


def test_missing_required_parameter_fields_fail_closed(tmp_path: Path) -> None:
    for field in ("api_id", "api_hash", "database_directory", "files_directory"):
        payload = _payload(tmp_path, **{field: None})
        report = _report(
            _runtime_env(tmp_path),
            auth_parameter_builder=_builder(payload),
            resolve_parameter_builder=_builder(payload),
        )

        assert report["contract_status"] == "blocked_required_parameter_missing"
        assert field in report["required_parameter_fields_missing"]
        assert report["required_parameter_fields_present"][field] is False


def test_wrong_api_id_and_bool_parameter_types_fail_closed(tmp_path: Path) -> None:
    cases = (
        ("api_id", "123456", "api_id.expected_positive_int"),
        ("use_message_database", "true", "use_message_database.expected_bool"),
        ("use_secret_chats", "false", "use_secret_chats.expected_bool"),
    )
    for field, value, expected_fragment in cases:
        payload = _payload(tmp_path, **{field: value})
        report = _report(
            _runtime_env(tmp_path),
            auth_parameter_builder=_builder(payload),
            resolve_parameter_builder=_builder(payload),
        )

        assert report["contract_status"] == "blocked_required_parameter_type_invalid"
        assert any(
            expected_fragment in failure
            for failure in report["required_parameter_field_type_failures"]
        )


def test_empty_db_encryption_key_source_fails_closed(tmp_path: Path) -> None:
    report = _report(_runtime_env(tmp_path, TDLIB_DB_ENCRYPTION_KEY=""))
    rendered = _render(report)

    assert report["contract_status"] == "blocked_required_parameter_missing"
    assert report["collector_config_built"] is False
    assert report["tdlib_db_encryption_key_configured"] is True
    assert report["tdlib_db_encryption_key_source_kind"] == "env"
    assert report["tdlib_db_encryption_key_non_empty"] is False
    assert "database_encryption_key" in report["required_parameter_fields_missing"]
    assert "TDLIB_DB_ENCRYPTION_KEY=" not in rendered
    assert FAKE_TDLIB_KEY not in rendered


def test_path_metadata_and_secret_file_source_do_not_leak_paths_or_values(
    tmp_path: Path,
) -> None:
    secret_file = tmp_path / "tdlib-db-encryption-key-super-secret-name.txt"
    secret_file.write_text(FAKE_TDLIB_KEY, encoding="utf-8")
    values = _runtime_env(
        tmp_path,
        TDLIB_DB_ENCRYPTION_KEY=None,
        TDLIB_DB_ENCRYPTION_KEY_FILE=str(secret_file),
    )

    report = _report(values)
    rendered = _render(report)

    assert report["contract_status"] == "tdlib_parameters_redacted_config_diagnostic_passed"
    assert report["tdlib_db_encryption_key_source_kind"] == "file"
    assert report["tdlib_db_encryption_key_non_empty"] is True
    assert report["tdlib_database_directory_kind"] == "absolute_path"
    assert report["tdlib_files_directory_kind"] == "absolute_path"
    assert str(tmp_path) not in rendered
    assert secret_file.name not in rendered
    assert FAKE_TDLIB_KEY not in rendered


def test_raw_runtime_env_and_secret_values_do_not_appear_in_output(tmp_path: Path) -> None:
    report = _report(_runtime_env(tmp_path))
    rendered = _render(report)

    forbidden_fragments = (
        FAKE_DATABASE_URL,
        "unit-db-password",
        FAKE_REDIS_URL,
        "unit-redis-password",
        FAKE_API_HASH,
        FAKE_PHONE_NUMBER,
        FAKE_TDLIB_KEY,
        "APP_ENV=prod",
        "COLLECTOR_MODE=live",
    )
    for fragment in forbidden_fragments:
        assert fragment not in rendered


def test_side_effects_all_remain_false(tmp_path: Path) -> None:
    module = _module()
    report = _report(_runtime_env(tmp_path))

    assert set(report["side_effects"]) == set(module.SIDE_EFFECT_FLAG_NAMES)
    assert all(value is False for value in report["side_effects"].values())


def test_source_does_not_initialize_real_tdlib_or_send_receive() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    forbidden_fragments = (
        "TDJsonTransport(",
        "assert_available(",
        ".initialize(",
        ".send(",
        ".receive(",
        "td_json_client_create",
        "td_json_client_send",
        "td_json_client_receive",
        "searchPublicChat",
    )
    for fragment in forbidden_fragments:
        assert fragment not in source


def test_tdjson_availability_check_is_import_or_path_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _module()
    monkeypatch.setattr(module.ctypes.util, "find_library", lambda name: "libtdjson.so")
    source = SCRIPT.read_text(encoding="utf-8")
    payload = _payload(tmp_path)

    report = module.generate_report(
        repo_root=ROOT,
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=lambda _path: _runtime_env(tmp_path),
        auth_parameter_builder=_builder(payload),
        resolve_parameter_builder=_builder(payload),
    ).report

    assert report["tdjson_available_import_check_performed"] is True
    assert report["tdjson_available"] is True
    assert "ctypes.CDLL" not in source
    assert "TDJsonTransport(" not in source
    assert "telegram_api_called" in source


def test_differences_summary_is_redacted_and_useful(tmp_path: Path) -> None:
    auth = _payload(tmp_path)
    resolve = _payload(tmp_path, api_hash=123456)
    report = _report(
        _runtime_env(tmp_path),
        auth_parameter_builder=_builder(auth),
        resolve_parameter_builder=_builder(resolve),
    )
    rendered = _render(report)

    assert report["contract_status"] == "blocked_required_parameter_type_invalid"
    assert any("type_mismatch: api_hash" in item for item in report["differences_summary"])
    assert "auth=non_empty_string" in " ".join(report["differences_summary"])
    assert "resolve=int" in " ".join(report["differences_summary"])
    assert FAKE_API_HASH not in rendered
    assert FAKE_PHONE_NUMBER not in rendered
    assert FAKE_TDLIB_KEY not in rendered
