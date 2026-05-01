from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.maintenance.batch_recovery_tool import (
    DeliveryBatchRecoveryTool,
    manual_retry_intent_dedupe_key,
    manual_retry_intent_payload,
)

from .test_batch_recovery_validation import FakeRecoveryRepository, _config, _row


class DedupeFalseRetryRepository(FakeRecoveryRepository):
    async def insert_manual_retry_intent_outbox(self, **kwargs) -> bool:
        self.retry_intents.append(kwargs)
        return False


@pytest.mark.asyncio
async def test_retry_selected_due_emits_notification_plan_created_manual_retry_intent() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = _row(delivery_status="failed_retryable", send_after=now, attempt_count=2)
    repository = FakeRecoveryRepository([row])

    result = await DeliveryBatchRecoveryTool(
        _config(),
        repository=repository,
        now_fn=lambda: now,
    ).retry_selected_due(plan_ids=[row.notification_plan_id], requested_by="ops")

    assert result.accepted_count == 1
    assert result.emitted_count == 1
    emitted = repository.retry_intents[0]
    assert emitted["dedupe_key"] == f"notify:manual-retry-intent:{row.notification_plan_id}:2:1767225600"
    assert "recovery_batch_id" not in emitted["dedupe_key"]
    assert emitted["payload_json"]["retry_reason"] == "manual_selected_due_retry"
    assert emitted["payload_json"]["previous_attempt_count"] == 2
    assert emitted["payload_json"]["send_after"] is None
    assert emitted["payload_json"]["recovery_batch_id"] == result.recovery_batch_id


def test_manual_retry_intent_payload_contains_required_fields() -> None:
    row = _row(delivery_status="failed_retryable")
    payload = manual_retry_intent_payload(row=row, recovery_batch_id="batch-1")

    assert {
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "delivery_decision",
        "urgency_profile",
        "target_chat_id",
        "target_thread_id",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
        "send_after",
        "retry_reason",
        "previous_attempt_count",
        "recovery_batch_id",
    } <= payload.keys()


def test_manual_retry_intent_dedupe_key_does_not_include_recovery_batch_id() -> None:
    row = _row(delivery_status="failed_retryable")

    assert manual_retry_intent_dedupe_key(row) == manual_retry_intent_dedupe_key(row)
    assert "batch" not in manual_retry_intent_dedupe_key(row)


@pytest.mark.asyncio
async def test_retry_selected_due_deduplicates_raw_plan_ids_before_insert() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = _row(delivery_status="failed_retryable", send_after=now, attempt_count=2)
    repository = FakeRecoveryRepository([row])

    result = await DeliveryBatchRecoveryTool(
        _config(),
        repository=repository,
        now_fn=lambda: now,
    ).retry_selected_due(
        plan_ids=[row.notification_plan_id, row.notification_plan_id],
        requested_by="ops",
    )

    assert result.selected_count == 2
    assert result.accepted_count == 1
    assert result.emitted_count == 1
    assert result.skipped_count == 1
    assert result.skipped_reason_codes == {"duplicate_notification_plan_id": 1}
    assert len(repository.retry_intents) == 1


@pytest.mark.asyncio
async def test_retry_selected_due_counts_insert_dedupe_false_as_skipped() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = _row(delivery_status="failed_retryable", send_after=now, attempt_count=2)
    repository = DedupeFalseRetryRepository([row])

    result = await DeliveryBatchRecoveryTool(
        _config(),
        repository=repository,
        now_fn=lambda: now,
    ).retry_selected_due(plan_ids=[row.notification_plan_id], requested_by="ops")

    assert result.selected_count == 1
    assert result.accepted_count == 1
    assert result.emitted_count == 0
    assert result.skipped_count == 1
    assert result.skipped_reason_codes == {"manual_retry_intent_exists_at_insert": 1}
