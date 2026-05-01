from __future__ import annotations

from uuid import UUID

import pytest

from services.maintenance.batch_recovery_tool import DeliveryBatchRecoveryTool

from .test_batch_recovery_validation import _config, _row


class DuplicateSafeReplayRepository:
    def __init__(self, rows, existing_open: set[UUID] | None = None) -> None:
        self.rows = {row.notification_plan_id: row for row in rows}
        self.existing_open = existing_open or set()
        self.insert_calls: list[list[UUID]] = []

    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]):
        return [self.rows[plan_id] for plan_id in notification_plan_ids if plan_id in self.rows]

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[UUID], requested_by: str) -> int:
        del requested_by
        self.insert_calls.append(plan_ids)
        inserted = 0
        for plan_id in plan_ids:
            if plan_id not in self.existing_open:
                self.existing_open.add(plan_id)
                inserted += 1
        return inserted

    async def insert_manual_retry_intent_outbox(self, **kwargs) -> bool:
        raise AssertionError("retry intent is not part of replay-selected")


@pytest.mark.asyncio
async def test_replay_selected_deduplicates_raw_plan_ids_before_insert() -> None:
    row = _row(delivery_status="failed_terminal")
    repository = DuplicateSafeReplayRepository([row])

    result = await DeliveryBatchRecoveryTool(_config(), repository=repository).replay_selected(
        plan_ids=[row.notification_plan_id, row.notification_plan_id],
        requested_by="ops",
    )

    assert result.selected_count == 2
    assert result.accepted_count == 1
    assert result.emitted_count == 1
    assert result.skipped_count == 1
    assert result.skipped_reason_codes == {"duplicate_notification_plan_id": 1}
    assert repository.insert_calls == [[row.notification_plan_id]]


@pytest.mark.asyncio
async def test_replay_selected_insert_path_accounts_for_duplicate_open_replay_requests() -> None:
    first = _row(delivery_status="failed_terminal")
    second = _row(delivery_status="failed_terminal")
    repository = DuplicateSafeReplayRepository([first, second], existing_open={second.notification_plan_id})

    result = await DeliveryBatchRecoveryTool(_config(), repository=repository).replay_selected(
        plan_ids=[first.notification_plan_id, second.notification_plan_id],
        requested_by="ops",
    )

    assert result.accepted_count == 2
    assert result.emitted_count == 1
    assert result.skipped_count == 1
    assert result.skipped_reason_codes == {"open_replay_request_exists_at_insert": 1}
    assert repository.insert_calls == [[first.notification_plan_id, second.notification_plan_id]]


@pytest.mark.asyncio
async def test_replay_selected_counts_invalid_ids_and_insert_duplicates_as_skipped() -> None:
    first = _row(delivery_status="failed_terminal")
    second = _row(delivery_status="failed_terminal")
    repository = DuplicateSafeReplayRepository([first, second], existing_open={second.notification_plan_id})

    result = await DeliveryBatchRecoveryTool(_config(), repository=repository).replay_selected(
        plan_ids=["not-a-uuid", first.notification_plan_id, second.notification_plan_id, second.notification_plan_id],
        requested_by="ops",
    )

    assert result.accepted_count == 2
    assert result.emitted_count == 1
    assert result.skipped_count == 3
    assert result.skipped_reason_codes == {
        "invalid_notification_plan_id": 1,
        "duplicate_notification_plan_id": 1,
        "open_replay_request_exists_at_insert": 1,
    }
    assert repository.insert_calls == [[first.notification_plan_id, second.notification_plan_id]]
