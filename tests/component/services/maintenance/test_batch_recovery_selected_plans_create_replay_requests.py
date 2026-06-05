from __future__ import annotations

from copy import deepcopy

import pytest

from services.maintenance.batch_recovery import (
    BATCH_RECOVERY_OPEN_REPLAY_EXISTS,
    BATCH_RECOVERY_REPLAY_REQUEST_CREATED,
    prepare_delivery_replay_requests_for_selected_plans,
)

from ._batch_recovery_fakes import FakeSelectedPlanReplayRepository
from tests.unit.services.maintenance.test_batch_recovery_validation import _row


@pytest.mark.asyncio
async def test_send_disabled_selected_plan_creates_replay_request_only() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])
    rows_before = deepcopy(repository.rows)

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert result.status == "completed"
    assert result.requested_count == 1
    assert result.created_count == 1
    assert result.skipped_count == 0
    assert result.results[0].action == "replay_request_created"
    assert result.results[0].reason_code == BATCH_RECOVERY_REPLAY_REQUEST_CREATED
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
    assert repository.rows == rows_before


@pytest.mark.asyncio
async def test_duplicate_call_is_idempotent_for_open_replay_request() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])

    first = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )
    second = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert first.created_count == 1
    assert second.created_count == 0
    assert second.skipped_count == 1
    assert second.results[0].reason_code == BATCH_RECOVERY_OPEN_REPLAY_EXISTS
    assert len(repository.replay_requests) == 1
