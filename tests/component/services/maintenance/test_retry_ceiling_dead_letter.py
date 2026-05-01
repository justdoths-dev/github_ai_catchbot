from __future__ import annotations

import pytest

from services.maintenance.service import MaintenanceService

from ._fakes import FakeRepository, config, outbox_event, plan


@pytest.mark.asyncio
async def test_retry_ceiling_creates_dead_letter_and_no_retry_intent_idempotently() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.delivery_attempt_counts[notification_plan.notification_plan_id] = 3
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "delivery_status": "failed_retryable",
            "attempt_count": 3,
        },
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(max_attempts=3), repository=repository)

    await service.handle_maintenance_trigger_event(event.event_id)
    await service.handle_maintenance_trigger_event(event.event_id)

    assert repository.plan_created_outbox == []
    assert len(repository.dead_letters) == 1
    assert repository.dead_letters[0]["last_error_code"] == "max_notification_retry_attempts_exceeded"
    assert repository.dead_letters[0]["retry_count"] == 3
