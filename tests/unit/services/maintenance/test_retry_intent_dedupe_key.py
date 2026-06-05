from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from services.maintenance.delivery_retry import retry_intent_dedupe_key


def test_retry_intent_dedupe_key_is_stable_for_same_failed_due_attempt() -> None:
    notification_plan_id = uuid4()
    send_after = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert retry_intent_dedupe_key(
        notification_plan_id=notification_plan_id,
        latest_attempt_count=2,
        send_after=send_after,
    ) == retry_intent_dedupe_key(
        notification_plan_id=notification_plan_id,
        latest_attempt_count=2,
        send_after=send_after,
    )


def test_retry_intent_dedupe_key_changes_for_new_attempt_or_due_time() -> None:
    notification_plan_id = uuid4()
    send_after = datetime(2026, 1, 1, tzinfo=timezone.utc)

    original = retry_intent_dedupe_key(
        notification_plan_id=notification_plan_id,
        latest_attempt_count=2,
        send_after=send_after,
    )

    assert retry_intent_dedupe_key(
        notification_plan_id=notification_plan_id,
        latest_attempt_count=3,
        send_after=send_after,
    ) != original
    assert retry_intent_dedupe_key(
        notification_plan_id=notification_plan_id,
        latest_attempt_count=2,
        send_after=send_after + timedelta(seconds=1),
    ) != original
