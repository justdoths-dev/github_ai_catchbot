from __future__ import annotations

import pytest

from services.maintenance import main as maintenance_main

from tests.unit.services.maintenance.test_batch_recovery_validation import _config


@pytest.mark.asyncio
async def test_selected_due_retry_cli_routes_to_one_shot_recovery(monkeypatch) -> None:
    calls: list[tuple[str, list[str], str]] = []

    async def fake_run_batch_recovery(config, args):
        calls.append((args.recovery_mode, args.plan_id, args.requested_by))
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
        ]
    )

    assert exit_code == 0
    assert calls == [
        (
            "retry-selected-due",
            ["00000000-0000-0000-0000-000000000002"],
            "ops",
        )
    ]
