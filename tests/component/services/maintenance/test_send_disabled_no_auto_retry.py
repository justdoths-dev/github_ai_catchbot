from __future__ import annotations

import pytest

from services.maintenance.service import MaintenanceService

from ._fakes import FakeRepository, config, outbox_event, plan


@pytest.mark.asyncio
async def test_send_disabled_suppressed_result_does_not_auto_retry_or_mutate_plan() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="suppressed", send_after=None, suppress_reason_code="notification_send_flag_disabled")
    original_plan = notification_plan
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "delivery_status": "suppressed",
            "transport_error_code": "notification_send_flag_disabled",
        },
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    await service.handle_maintenance_trigger_event(event.event_id)

    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert repository.plans[notification_plan.notification_plan_id] == original_plan
