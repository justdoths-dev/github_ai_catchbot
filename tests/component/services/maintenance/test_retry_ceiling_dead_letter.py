from __future__ import annotations

import pytest

from services.maintenance.service import MaintenanceService

from ._fakes import FakeRepository, config, latest_delivery_record, plan


@pytest.mark.asyncio
async def test_retry_ceiling_creates_dead_letter_and_no_retry_intent_idempotently() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.delivery_attempt_counts[notification_plan.notification_plan_id] = 3
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="failed_retryable",
        attempt_count=3,
    )
    service = MaintenanceService(config(max_attempts=3), repository=repository)

    await service.promote_due_retries_once()
    await service.promote_due_retries_once()

    assert repository.plan_created_outbox == []
    assert len(repository.dead_letters) == 1
    assert repository.dead_letters[0]["last_error_code"] == "max_notification_retry_attempts_exceeded"
    assert repository.dead_letters[0]["retry_count"] == 3
