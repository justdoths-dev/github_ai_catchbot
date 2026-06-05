from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

from services.maintenance.batch_recovery import (
    BATCH_RECOVERY_ALREADY_DELIVERED,
    BATCH_RECOVERY_DELIVERY_DLQ_BLOCKS_REPLAY,
    BATCH_RECOVERY_NOT_REPLAY_CANDIDATE,
    BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED,
    BATCH_RECOVERY_REPLAY_REQUEST_CREATED,
    classify_selected_plan_for_delivery_replay,
    prepare_delivery_replay_requests_for_selected_plans,
)
from services.maintenance.models import SelectedPlanRecoveryRow

from .test_batch_recovery_validation import _row


class RecordingRecoveryRepository:
    def __init__(self, rows: list[SelectedPlanRecoveryRow]) -> None:
        self.rows = {row.notification_plan_id: row for row in rows}
        self.inserted_plan_ids: list[UUID] = []
        self.load_calls: list[list[UUID]] = []

    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]):
        self.load_calls.append(notification_plan_ids)
        return [self.rows[plan_id] for plan_id in notification_plan_ids if plan_id in self.rows]

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[UUID], requested_by: str) -> int:
        del requested_by
        self.inserted_plan_ids.extend(plan_ids)
        return len(plan_ids)


def test_send_disabled_suppressed_row_is_replay_candidate() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)

    assert classify_selected_plan_for_delivery_replay(row) is None


def test_already_delivered_rows_are_skipped() -> None:
    assert classify_selected_plan_for_delivery_replay(_row(delivery_status="sent")) == BATCH_RECOVERY_ALREADY_DELIVERED
    assert (
        classify_selected_plan_for_delivery_replay(_row(delivery_status="edited"))
        == BATCH_RECOVERY_ALREADY_DELIVERED
    )


def test_delivery_dlq_blocks_without_explicit_replay_action() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True, has_delivery_dlq=True)

    assert classify_selected_plan_for_delivery_replay(row) == BATCH_RECOVERY_DELIVERY_DLQ_BLOCKS_REPLAY


def test_delivery_dlq_allows_explicit_replay_action_or_hint() -> None:
    manual_action_row = replace(
        _row(delivery_status="failed_retryable", has_delivery_dlq=True),
        delivery_dlq_next_manual_action="request_explicit_delivery_replay",
    )
    replay_hint_row = replace(
        _row(delivery_status="failed_retryable", has_delivery_dlq=True),
        delivery_dlq_replay_hint="delivery_replay_from_notification_plan",
    )

    assert classify_selected_plan_for_delivery_replay(manual_action_row) is None
    assert classify_selected_plan_for_delivery_replay(replay_hint_row) is None


def test_failed_retryable_without_explicit_dlq_replay_is_not_candidate() -> None:
    assert classify_selected_plan_for_delivery_replay(_row(delivery_status="failed_retryable")) == (
        BATCH_RECOVERY_NOT_REPLAY_CANDIDATE
    )


@pytest.mark.asyncio
async def test_missing_operator_confirmation_rejects_without_load_or_insert() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = RecordingRecoveryRepository([row])

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=False,
    )

    assert result.status == "rejected"
    assert result.reason_code == BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED
    assert result.requested_count == 1
    assert result.created_count == 0
    assert result.skipped_count == 1
    assert result.results[0].reason_code == BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED
    assert repository.load_calls == []
    assert repository.inserted_plan_ids == []


@pytest.mark.asyncio
async def test_selected_result_order_matches_input_order() -> None:
    ids = [
        UUID("00000000-0000-0000-0000-000000000003"),
        UUID("00000000-0000-0000-0000-000000000001"),
        UUID("00000000-0000-0000-0000-000000000002"),
    ]
    rows = [
        replace(_row(delivery_status="suppressed", send_disabled=True), notification_plan_id=plan_id)
        for plan_id in ids
    ]
    repository = RecordingRecoveryRepository(rows)

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=ids,
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert [row.notification_plan_id for row in result.results] == ids
    assert [row.reason_code for row in result.results] == [BATCH_RECOVERY_REPLAY_REQUEST_CREATED] * 3
