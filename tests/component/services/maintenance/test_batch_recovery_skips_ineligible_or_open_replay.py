from __future__ import annotations

from dataclasses import replace

import pytest

from services.maintenance.batch_recovery import (
    BATCH_RECOVERY_ALREADY_DELIVERED,
    BATCH_RECOVERY_DELIVERY_DLQ_BLOCKS_REPLAY,
    BATCH_RECOVERY_OPEN_REPLAY_EXISTS,
    BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED,
    BATCH_RECOVERY_REPLAY_REQUEST_CREATED,
    prepare_delivery_replay_requests_for_selected_plans,
)

from ._batch_recovery_fakes import FakeSelectedPlanReplayRepository
from tests.unit.services.maintenance.test_batch_recovery_validation import _row


@pytest.mark.asyncio
async def test_missing_operator_confirmation_rejects_without_writes() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=False,
    )

    assert result.status == "rejected"
    assert result.reason_code == BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED
    assert result.created_count == 0
    assert result.skipped_count == 1
    assert result.results[0].reason_code == BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED
    assert repository.load_calls == []
    assert repository.replay_requests == []
    assert repository.event_outbox == []
    assert repository.notification_plan_mutations == []
    assert repository.notification_delivery_record_mutations == []


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_status", ["sent", "edited"])
async def test_already_delivered_plans_are_skipped(delivery_status: str) -> None:
    row = _row(delivery_status=delivery_status)
    repository = FakeSelectedPlanReplayRepository([row])

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert result.created_count == 0
    assert result.skipped_count == 1
    assert result.results[0].reason_code == BATCH_RECOVERY_ALREADY_DELIVERED
    assert repository.replay_requests == []


@pytest.mark.asyncio
async def test_open_replay_request_is_skipped_without_duplicate_insert() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True, has_open_replay_request=True)
    repository = FakeSelectedPlanReplayRepository([row])
    repository.add_open_replay_request(row.notification_plan_id)

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert result.created_count == 0
    assert result.results[0].reason_code == BATCH_RECOVERY_OPEN_REPLAY_EXISTS
    assert len(repository.replay_requests) == 1


@pytest.mark.asyncio
async def test_delivery_dlq_blocks_without_explicit_manual_action_or_hint() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True, has_delivery_dlq=True)
    repository = FakeSelectedPlanReplayRepository([row])

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert result.created_count == 0
    assert result.results[0].reason_code == BATCH_RECOVERY_DELIVERY_DLQ_BLOCKS_REPLAY
    assert repository.replay_requests == []


@pytest.mark.asyncio
async def test_delivery_dlq_explicit_replay_action_allows_replay_request() -> None:
    row = replace(
        _row(delivery_status="failed_retryable", has_delivery_dlq=True),
        delivery_dlq_next_manual_action="request_explicit_delivery_replay",
        delivery_dlq_replay_hint="delivery_replay_from_notification_plan",
    )
    repository = FakeSelectedPlanReplayRepository([row])

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert result.created_count == 1
    assert result.results[0].reason_code == BATCH_RECOVERY_REPLAY_REQUEST_CREATED
    assert repository.replay_requests[0]["root_object_id"] == row.notification_plan_id
