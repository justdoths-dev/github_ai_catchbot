from __future__ import annotations

import json

import pytest

from services.maintenance import main as maintenance_main


def _fail_from_env(cls):
    raise AssertionError("systemd-rollout must not load MaintenanceConfig")


def _assert_sanitized_block(output: str, *, reason_code: str, missing_env_file) -> None:
    payload = json.loads(output)
    assert payload["schema_version"] == "maintenance_systemd_rollout_report_v1"
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == reason_code
    assert payload["unit_plan_created"] is False
    assert payload["install_attempted"] is False
    assert payload["start_attempted"] is False
    assert payload["rollback_attempted"] is False
    assert str(missing_env_file) not in output
    assert "sentinel-secret" not in output


def _assert_sanitized_diagnostic_block(output: str, *, reason_code: str, missing_env_file) -> None:
    payload = json.loads(output)
    assert payload["schema_version"] == "maintenance_systemd_diagnostic_report_v1"
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == reason_code
    assert payload["service_name"] == "github-ai-catchbot-maintenance.service"
    assert payload["service_file_present"] is False
    assert payload["service_enabled"] is False
    assert payload["service_active"] is False
    assert payload["redactions_applied"]["runtime_env_path_omitted"] is True
    assert payload["redactions_applied"]["unit_file_content_omitted"] is True
    assert str(missing_env_file) not in output
    assert "sentinel-secret" not in output


@pytest.mark.asyncio
async def test_invalid_target_blocks_before_env_file_or_config_load(monkeypatch, tmp_path, capsys) -> None:
    missing_env_file = tmp_path / "sentinel-secret-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "systemd-rollout",
            "--mode",
            "plan",
            "--target",
            "not-maintenance-worker",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 2
    _assert_sanitized_block(output, reason_code="target_not_allowed", missing_env_file=missing_env_file)


@pytest.mark.asyncio
async def test_diagnose_invalid_target_blocks_before_env_file_or_config_load(monkeypatch, tmp_path, capsys) -> None:
    missing_env_file = tmp_path / "sentinel-secret-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "systemd-rollout",
            "--mode",
            "diagnose",
            "--target",
            "not-maintenance-worker",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 2
    _assert_sanitized_diagnostic_block(
        output,
        reason_code="target_not_allowed",
        missing_env_file=missing_env_file,
    )


@pytest.mark.asyncio
async def test_install_without_confirmation_blocks_before_env_file_or_config_load(monkeypatch, tmp_path, capsys) -> None:
    missing_env_file = tmp_path / "sentinel-secret-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "systemd-rollout",
            "--mode",
            "install",
            "--target",
            "maintenance-worker",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 2
    _assert_sanitized_block(output, reason_code="install_confirm_missing", missing_env_file=missing_env_file)


@pytest.mark.asyncio
async def test_plan_with_confirmation_blocks_before_env_file_or_config_load(monkeypatch, tmp_path, capsys) -> None:
    missing_env_file = tmp_path / "sentinel-secret-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "systemd-rollout",
            "--mode",
            "plan",
            "--target",
            "maintenance-worker",
            "--confirm",
            "install",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 2
    _assert_sanitized_block(
        output,
        reason_code="confirm_not_allowed_for_read_only",
        missing_env_file=missing_env_file,
    )


@pytest.mark.asyncio
async def test_start_without_confirmation_blocks_before_env_file_or_config_load(monkeypatch, tmp_path, capsys) -> None:
    missing_env_file = tmp_path / "sentinel-secret-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "systemd-rollout",
            "--mode",
            "start",
            "--target",
            "maintenance-worker",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 2
    _assert_sanitized_block(output, reason_code="start_confirm_missing", missing_env_file=missing_env_file)


@pytest.mark.asyncio
async def test_rollback_without_confirmation_blocks_before_env_file_or_config_load(monkeypatch, tmp_path, capsys) -> None:
    missing_env_file = tmp_path / "sentinel-secret-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "systemd-rollout",
            "--mode",
            "rollback",
            "--target",
            "maintenance-worker",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 2
    _assert_sanitized_block(output, reason_code="rollback_confirm_missing", missing_env_file=missing_env_file)
