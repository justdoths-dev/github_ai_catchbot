from __future__ import annotations

import json

import pytest

from services.maintenance import main as maintenance_main

from tests.unit.services.maintenance.test_batch_recovery_validation import _config


@pytest.mark.asyncio
async def test_delivery_gate_does_not_require_confirm(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_run_delivery_gate(config, args):
        calls.append(args.mode)
        assert config.database_url
        return 0

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_delivery_gate", fake_run_delivery_gate)

    exit_code = await maintenance_main._run(["delivery-gate", "--mode", "restricted", "--format", "json"])

    assert exit_code == 0
    assert calls == ["restricted"]


@pytest.mark.asyncio
async def test_replay_selected_requires_operator_confirmation_before_runner_is_called(monkeypatch, capsys) -> None:
    calls: list[str] = []

    async def fake_run_batch_recovery(config, args):
        calls.append(args.recovery_mode)
        return 0

    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fake_run_batch_recovery)

    exit_code = await maintenance_main._run(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--requested-by",
            "ops",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "batch_recovery_operator_confirmation_required"
    assert payload["created_count"] == 0
    assert calls == []


@pytest.mark.asyncio
async def test_retry_selected_due_requires_confirm_write_before_runner_is_called(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_run_batch_recovery(config, args):
        calls.append(args.recovery_mode)
        return 0

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fake_run_batch_recovery)

    with pytest.raises(SystemExit) as exc:
        await maintenance_main._run(
            [
                "batch-recovery",
                "retry-selected-due",
                "--plan-id",
                "00000000-0000-0000-0000-000000000002",
                "--requested-by",
                "ops",
            ]
        )

    assert exc.value.code == 2
    assert calls == []


@pytest.mark.asyncio
async def test_invalid_confirm_value_is_parse_error_before_runner_is_called(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_run_batch_recovery(config, args):
        calls.append(args.recovery_mode)
        return 0

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fake_run_batch_recovery)

    with pytest.raises(SystemExit) as exc:
        await maintenance_main._run(
            [
                "batch-recovery",
                "replay-selected",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "ops",
                "--confirm",
                "yes",
            ]
        )

    assert exc.value.code == 2
    assert calls == []


@pytest.mark.asyncio
async def test_batch_recovery_with_confirm_routes_to_one_shot_runner(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    async def fake_run_batch_recovery(config, args):
        calls.append((args.recovery_mode, args.confirm))
        assert config.redis_url
        return 0

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fake_run_batch_recovery)

    exit_code = await maintenance_main._run(
        [
            "batch-recovery",
            "retry-selected-due",
            "--plan-id",
            "00000000-0000-0000-0000-000000000002",
            "--requested-by",
            "ops",
            "--confirm",
            "write",
        ]
    )

    assert exit_code == 0
    assert calls == [("retry-selected-due", "write")]


@pytest.mark.asyncio
async def test_gate_warn_and_fail_exit_codes(monkeypatch) -> None:
    async def fake_warn(config, args):
        return 3

    async def fake_fail(config, args):
        return 2

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_delivery_gate", fake_warn)
    assert await maintenance_main._run(["delivery-gate", "--mode", "full", "--format", "json"]) == 3

    monkeypatch.setattr(maintenance_main, "_run_delivery_gate", fake_fail)
    assert await maintenance_main._run(["delivery-gate", "--mode", "full", "--format", "json"]) == 2
