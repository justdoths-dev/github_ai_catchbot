from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.services.collector_telegram.runtime_env_overlay import (
    COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS,
    build_collector_runtime_env_overlay,
)


SENTINEL_VALUES = (
    "SENTINEL_DATABASE_URL_VALUE",
    "SENTINEL_REDIS_URL_VALUE",
    "SENTINEL_TELEGRAM_API_HASH_VALUE",
    "SENTINEL_TELEGRAM_PHONE_NUMBER_VALUE",
    "SENTINEL_TDLIB_STATE_PATH_VALUE",
    "SENTINEL_TDLIB_ENCRYPTION_VALUE",
    "SENTINEL_OPENAI_KEY_FILE_VALUE",
    "SENTINEL_X_BEARER_TOKEN_VALUE",
    "SENTINEL_TELEGRAM_BOT_TOKEN_VALUE",
    "SENTINEL_UNKNOWN_VALUE",
)


def _write_env(tmp_path: Path, body: str, *, mode: int = 0o600) -> Path:
    path = tmp_path / "fixture-runtime.env"
    path.write_text(body, encoding="utf-8")
    path.chmod(mode)
    return path


def _integrated_env_body() -> str:
    return "\n".join(
        (
            "APP_ENV=prod",
            "COLLECTOR_MODE=live",
            "DATABASE_URL=SENTINEL_DATABASE_URL_VALUE",
            "REDIS_URL=SENTINEL_REDIS_URL_VALUE",
            "TELEGRAM_API_ID=12345",
            "TELEGRAM_API_HASH=SENTINEL_TELEGRAM_API_HASH_VALUE",
            "TELEGRAM_PHONE_NUMBER=SENTINEL_TELEGRAM_PHONE_NUMBER_VALUE",
            "TDLIB_STATE_DIR=SENTINEL_TDLIB_STATE_PATH_VALUE",
            "TDLIB_DB_ENCRYPTION_KEY=SENTINEL_TDLIB_ENCRYPTION_VALUE",
            "LOG_LEVEL=INFO",
            "ENABLE_NOTIFICATION_SEND=true",
            "ENABLE_REPLAY_TO_PROD_DB=false",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false",
            "NOTIFIER_TELEGRAM_ALLOW_EDITS=false",
            "NOTIFIER_TELEGRAM_DRY_RUN=true",
            "OPENAI_API_KEY_FILE=SENTINEL_OPENAI_KEY_FILE_VALUE",
            "TELEGRAM_BOT_TOKEN=SENTINEL_TELEGRAM_BOT_TOKEN_VALUE",
            "TELEGRAM_OPERATOR_CHAT_ID=123456789",
            "X_BEARER_TOKEN=SENTINEL_X_BEARER_TOKEN_VALUE",
            "UNKNOWN_EXTRA=SENTINEL_UNKNOWN_VALUE",
            "",
        )
    )


def test_integrated_env_builds_collector_only_child_overlay_and_reports_ignored_key_names(tmp_path: Path) -> None:
    path = _write_env(tmp_path, _integrated_env_body())

    result = build_collector_runtime_env_overlay(path)
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is True
    assert result.reason_code == "collector_runtime_env_overlay_ready"
    assert set(result.child_overlay) <= set(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
    assert result.child_overlay["DATABASE_URL"] == "SENTINEL_DATABASE_URL_VALUE"
    assert "UNKNOWN_EXTRA" not in result.child_overlay
    assert "OPENAI_API_KEY_FILE" not in result.child_overlay
    assert "TELEGRAM_BOT_TOKEN" not in result.child_overlay
    assert "X_BEARER_TOKEN" not in result.child_overlay
    assert result.ignored_unknown_keys == ("UNKNOWN_EXTRA",)
    assert "OPENAI_API_KEY_FILE" in result.ignored_forbidden_keys
    assert "TELEGRAM_BOT_TOKEN" in result.ignored_forbidden_keys
    assert "X_BEARER_TOKEN" in result.ignored_forbidden_keys
    assert result.to_sanitized_dict()["source_runtime_env_allows_extra_keys"] is True
    assert result.to_sanitized_dict()["source_unknown_keys_ignored"] is True
    assert result.to_sanitized_dict()["source_forbidden_keys_ignored"] is True
    assert result.to_sanitized_dict()["child_overlay_only"] is True
    assert result.to_sanitized_dict()["child_overlay_rejects_unknown_keys"] is True
    assert result.to_sanitized_dict()["child_overlay_rejects_forbidden_keys"] is True
    assert result.to_sanitized_dict()["runtime_env_values_printed"] is False
    assert result.to_sanitized_dict()["runtime_env_file_contents_printed"] is False

    for value in SENTINEL_VALUES:
        assert value not in rendered


def test_missing_required_collector_key_blocks(tmp_path: Path) -> None:
    path = _write_env(tmp_path, _integrated_env_body().replace("DATABASE_URL=SENTINEL_DATABASE_URL_VALUE\n", ""))

    result = build_collector_runtime_env_overlay(path)

    assert result.ok is False
    assert result.reason_code == "missing_required_collector_runtime_env_keys"
    assert result.missing_required_keys == ("DATABASE_URL",)


def test_missing_required_collector_secret_group_blocks(tmp_path: Path) -> None:
    body = _integrated_env_body().replace("TELEGRAM_API_HASH=SENTINEL_TELEGRAM_API_HASH_VALUE\n", "")
    path = _write_env(tmp_path, body)

    result = build_collector_runtime_env_overlay(path)

    assert result.ok is False
    assert result.reason_code == "missing_required_collector_runtime_env_groups"
    assert result.missing_required_groups == ("TELEGRAM_API_HASH|TELEGRAM_API_HASH_FILE",)


def test_duplicate_key_blocks_without_printing_values(tmp_path: Path) -> None:
    path = _write_env(tmp_path, _integrated_env_body() + "APP_ENV=prod\n")

    result = build_collector_runtime_env_overlay(path)
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.reason_code == "duplicate_runtime_env_keys"
    assert result.duplicate_keys == ("APP_ENV",)
    for value in SENTINEL_VALUES:
        assert value not in rendered


def test_invalid_env_line_blocks_without_printing_raw_line(tmp_path: Path) -> None:
    path = _write_env(tmp_path, _integrated_env_body() + "THIS IS NOT AN ENV LINE\n")

    result = build_collector_runtime_env_overlay(path)
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.reason_code == "invalid_runtime_env_line"
    assert result.invalid_line_numbers == (21,)
    assert "THIS IS NOT AN ENV LINE" not in rendered


@pytest.mark.parametrize("mode", [0o600, 0o400])
def test_owner_only_permissions_pass_where_supported(tmp_path: Path, mode: int) -> None:
    if os.name == "nt":
        pytest.skip("POSIX permission bits are not stable on Windows")
    path = _write_env(tmp_path, _integrated_env_body(), mode=mode)

    result = build_collector_runtime_env_overlay(path)

    assert result.ok is True
    assert result.reason_code == "collector_runtime_env_overlay_ready"
    assert result.to_sanitized_dict()["file_permission_checked"] is True
    assert result.to_sanitized_dict()["file_permission_mode"] == f"{mode:04o}"


@pytest.mark.parametrize("membership_source", ["effective", "supplementary"])
def test_restricted_group_read_only_permissions_pass_for_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, membership_source: str
) -> None:
    if os.name == "nt":
        pytest.skip("POSIX permission bits are not stable on Windows")
    path = _write_env(tmp_path, _integrated_env_body(), mode=0o640)
    file_gid = path.stat().st_gid
    non_member_gid = file_gid + 1
    if membership_source == "effective":
        monkeypatch.setattr(os, "getegid", lambda: file_gid)
        monkeypatch.setattr(os, "getgroups", lambda: [])
    else:
        monkeypatch.setattr(os, "getegid", lambda: non_member_gid)
        monkeypatch.setattr(os, "getgroups", lambda: [file_gid])

    result = build_collector_runtime_env_overlay(path)

    assert result.ok is True
    assert result.reason_code == "collector_runtime_env_overlay_ready"
    assert result.to_sanitized_dict()["file_permission_checked"] is True
    assert result.to_sanitized_dict()["file_permission_mode"] == "0640"


@pytest.mark.parametrize("mode", [0o644, 0o660, 0o650, 0o641, 0o664])
def test_unsafe_group_or_other_permissions_block_where_supported(tmp_path: Path, mode: int) -> None:
    if os.name == "nt":
        pytest.skip("POSIX permission bits are not stable on Windows")
    path = _write_env(tmp_path, _integrated_env_body(), mode=mode)

    result = build_collector_runtime_env_overlay(path)
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is False
    assert result.reason_code == "runtime_env_file_permissions_too_open"
    assert report["file_permission_checked"] is True
    assert report["file_permission_mode"] == f"{mode:04o}"
    assert report["runtime_env_values_printed"] is False
    assert report["runtime_env_file_contents_printed"] is False
    assert report["runtime_env_file_path_printed"] is False
    for value in SENTINEL_VALUES:
        assert value not in rendered


def test_group_read_only_permissions_block_for_non_member_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if os.name == "nt":
        pytest.skip("POSIX permission bits are not stable on Windows")
    path = _write_env(tmp_path, _integrated_env_body(), mode=0o640)
    file_gid = path.stat().st_gid
    non_member_gid = file_gid + 1
    monkeypatch.setattr(os, "getegid", lambda: non_member_gid)
    monkeypatch.setattr(os, "getgroups", lambda: [non_member_gid])

    result = build_collector_runtime_env_overlay(path)
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert result.reason_code == "runtime_env_file_permissions_too_open"
    assert report["file_permission_checked"] is True
    assert report["file_permission_mode"] == "0640"
    assert report["runtime_env_values_printed"] is False
    assert report["runtime_env_file_contents_printed"] is False
    assert report["runtime_env_file_path_printed"] is False


def test_missing_file_blocks() -> None:
    result = build_collector_runtime_env_overlay("/tmp/definitely-missing-catchbot-runtime-fixture.env")

    assert result.ok is False
    assert result.reason_code == "runtime_env_file_missing"
    assert result.to_sanitized_dict()["runtime_env_file_path_printed"] is False
