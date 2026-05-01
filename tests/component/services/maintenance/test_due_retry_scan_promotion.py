from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.maintenance.service import MaintenanceService

from ._fakes import FakeRepository, config, latest_delivery_record, plan


@pytest.mark.asyncio
async def test_due_retry_scan_promotes_without_q_maintenance_event_and_is_idempotent() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now - timedelta(seconds=1))
    original_plan = notification_plan
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=1,
    )
    service = MaintenanceService(config(), repository=repository, now_fn=lambda: now)

    first_processed = await service.promote_due_retries_once()
    second_processed = await service.promote_due_retries_once()

    assert first_processed == 1
    assert second_processed == 0
    assert len(repository.plan_created_outbox) == 1
    row = repository.plan_created_outbox[0]
    assert row["event_type"] == "notification.plan.created.v1"
    assert row["payload_json"]["notification_plan_id"] == str(notification_plan.notification_plan_id)
    assert row["payload_json"]["retry_reason"] == "due_retry_promotion"
    assert row["payload_json"]["retry_attempt"] == 2
    assert repository.plans[notification_plan.notification_plan_id] == original_plan
    assert repository.events == {}
