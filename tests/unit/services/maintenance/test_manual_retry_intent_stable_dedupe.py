from __future__ import annotations

from datetime import datetime, timezone

from services.maintenance.batch_recovery_tool import manual_retry_intent_dedupe_key

from .test_batch_recovery_validation import _row


def test_manual_retry_intent_dedupe_key_uses_durable_row_state_only() -> None:
    send_after = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    row = _row(delivery_status="failed_retryable", send_after=send_after, attempt_count=2)

    assert manual_retry_intent_dedupe_key(row) == (
        f"notify:manual-retry-intent:{row.notification_plan_id}:2:1767225600"
    )
    assert "recovery_batch_id" not in manual_retry_intent_dedupe_key(row)
