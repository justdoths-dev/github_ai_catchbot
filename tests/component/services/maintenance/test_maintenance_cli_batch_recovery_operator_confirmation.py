from __future__ import annotations

import json

import pytest

from services.maintenance import main as maintenance_main
from tests.component.services.maintenance._batch_recovery_fakes import FakeSelectedPlanReplayRepository
from tests.unit.services.maintenance.test_batch_recovery_validation import _row


@pytest.mark.asyncio
async def test_missing_operator_confirmation_blocks_writes_and_emits_json_result() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])
    emitted: list[str] = []
    args = maintenance_main.build_parser().parse_args(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            str(row.notification_plan_id),
            "--requested-by",
            "test/operator",
        ]
    )

    exit_code = await maintenance_main.run_replay_selected_batch_recovery(args, repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "batch_recovery_operator_confirmation_required"
    assert payload["requested_count"] == 1
    assert payload["created_count"] == 0
    assert payload["skipped_count"] == 1
    assert payload["results"][0]["reason_code"] == "batch_recovery_operator_confirmation_required"
    assert repository.load_calls == []
    assert repository.replay_requests == []
    assert repository.event_outbox == []
    assert repository.job_attempts == []
    assert repository.notification_plan_mutations == []
    assert repository.notification_delivery_record_mutations == []


@pytest.mark.asyncio
async def test_confirmed_replay_selected_creates_replay_request_only() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])
    emitted: list[str] = []
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

    exit_code = await maintenance_main.run_replay_selected_batch_recovery(args, repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert exit_code == 0
    assert payload["status"] == "completed"
    assert payload["requested_count"] == 1
    assert payload["created_count"] == 1
    assert payload["skipped_count"] == 0
    assert payload["results"][0]["notification_plan_id"] == str(row.notification_plan_id)
    assert payload["results"][0]["replay_request_created"] is True
    assert repository.replay_requests == [
        {
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": row.notification_plan_id,
            "requested_by": "test/operator",
            "status": "requested",
        }
    ]
    assert repository.event_outbox == []
    assert repository.job_attempts == []
    assert repository.notification_plan_mutations == []
    assert repository.notification_delivery_record_mutations == []


@pytest.mark.asyncio
async def test_invalid_uuid_is_deterministic_validation_failure_without_writes() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])
    emitted: list[str] = []
    args = maintenance_main.build_parser().parse_args(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            "not-a-uuid",
            "--requested-by",
            "test/operator",
            "--operator-confirmed",
        ]
    )

    exit_code = await maintenance_main.run_replay_selected_batch_recovery(args, repository, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "invalid_notification_plan_id"
    assert payload["requested_count"] == 1
    assert payload["created_count"] == 0
    assert payload["skipped_count"] == 1
    assert payload["results"][0]["notification_plan_id"] == "not-a-uuid"
    assert payload["results"][0]["reason_code"] == "invalid_notification_plan_id"
    assert repository.load_calls == []
    assert repository.replay_requests == []
    assert repository.event_outbox == []
    assert repository.job_attempts == []
