from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

import pytest

from services.maintenance import main as maintenance_main
from tests.component.services.maintenance._batch_recovery_fakes import FakeSelectedPlanReplayRepository
from tests.unit.services.maintenance.test_batch_recovery_validation import _config, _row


@pytest.mark.asyncio
async def test_confirmed_cli_replay_selected_preserves_notifier_and_upstream_rows() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])
    repository.notification_plans = {
        row.notification_plan_id: {
            "notification_plan_id": row.notification_plan_id,
            "analysis_id": row.analysis_id,
            "candidate_group_id": row.candidate_group_id,
            "status": "suppressed",
        }
    }
    repository.notification_delivery_records = {
        row.notification_plan_id: {
            "notification_plan_id": row.notification_plan_id,
            "delivery_status": "suppressed",
            "telegram_response_json": {"send_disabled": True},
        }
    }
    judge_output_id = uuid4()
    bundle_id = uuid4()
    repository.analyses = {
        row.analysis_id: {
            "analysis_id": row.analysis_id,
            "candidate_group_id": row.candidate_group_id,
            "delivery_decision": "send_now",
        }
    }
    repository.judge_outputs = {
        judge_output_id: {
            "judge_output_id": judge_output_id,
            "analysis_id": row.analysis_id,
            "schema_version": "judge_output_v1",
        }
    }
    repository.candidates = {
        row.candidate_group_id: {
            "candidate_group_id": row.candidate_group_id,
            "status": "ready_for_analysis",
        }
    }
    repository.bundles = {
        bundle_id: {
            "candidate_group_id": row.candidate_group_id,
            "bundle_version": 1,
        }
    }
    before = deepcopy(
        {
            "notification_plans": repository.notification_plans,
            "notification_delivery_records": repository.notification_delivery_records,
            "analyses": repository.analyses,
            "judge_outputs": repository.judge_outputs,
            "candidates": repository.candidates,
            "bundles": repository.bundles,
            "event_outbox": repository.event_outbox,
            "job_attempts": repository.job_attempts,
        }
    )
    args = maintenance_main.build_parser().parse_args(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            str(row.notification_plan_id),
            "--requested-by",
            "test/operator",
            "--operator-confirmed",
        ]
    )

    exit_code = await maintenance_main.run_replay_selected_batch_recovery(args, repository, emit_json=lambda _: None)

    assert exit_code == 0
    assert repository.replay_requests == [
        {
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": row.notification_plan_id,
            "requested_by": "test/operator",
            "status": "requested",
        }
    ]
    assert {
        "notification_plans": repository.notification_plans,
        "notification_delivery_records": repository.notification_delivery_records,
        "analyses": repository.analyses,
        "judge_outputs": repository.judge_outputs,
        "candidates": repository.candidates,
        "bundles": repository.bundles,
        "event_outbox": repository.event_outbox,
        "job_attempts": repository.job_attempts,
    } == before


@pytest.mark.asyncio
async def test_delivery_gate_cli_path_does_not_call_batch_recovery(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_run_delivery_gate(config, args):
        calls.append(f"delivery-gate:{args.mode}")
        assert config.database_url
        return 0

    async def fail_batch_recovery(config, args):
        raise AssertionError("delivery-gate must not call batch recovery")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(lambda cls: _config()))
    monkeypatch.setattr(maintenance_main, "_run_delivery_gate", fake_run_delivery_gate)
    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fail_batch_recovery)

    exit_code = await maintenance_main._run(["delivery-gate", "--mode", "restricted", "--format", "json"])

    assert exit_code == 0
    assert calls == ["delivery-gate:restricted"]
