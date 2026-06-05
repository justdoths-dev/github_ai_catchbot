from __future__ import annotations

import pytest

from services.maintenance import main as maintenance_main

from tests.unit.services.maintenance.test_batch_recovery_validation import _config


@pytest.mark.asyncio
async def test_batch_replay_request_cli_routes_to_one_shot_recovery(monkeypatch) -> None:
    calls: list[tuple[str, list[str], str]] = []

    async def fake_run_batch_recovery(config, args):
        calls.append((args.recovery_mode, args.plan_id, args.requested_by))
        assert config.database_url
        return 0

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fake_run_batch_recovery)

    exit_code = await maintenance_main._run(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--requested-by",
            "ops",
            "--operator-confirmed",
        ]
    )

    assert exit_code == 0
    assert calls == [
        (
            "replay-selected",
            ["00000000-0000-0000-0000-000000000001"],
            "ops",
        )
    ]
