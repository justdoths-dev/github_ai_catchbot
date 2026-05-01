from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.maintenance.service import MaintenanceService
from services.outbox_relay.models import OutboxEventRow
from services.outbox_relay.routing import OutboxRouteResolver
from tests.component.services.maintenance._fakes import FakeRepository, config, outbox_event, plan


def _route_row(row: dict) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=row["aggregate_id"],
        event_type=row["event_type"],
        aggregate_type=row["aggregate_type"],
        aggregate_id=row["aggregate_id"],
        dedupe_key=row["dedupe_key"],
        payload_json=row["payload_json"],
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_delivery_result_routes_to_maintenance_and_retry_reenters_notification_send() -> None:
    resolver = OutboxRouteResolver()
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.delivery_attempt_counts[notification_plan.notification_plan_id] = 1
    delivery_result_event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "delivery_status": "failed_retryable",
        },
    )
    repository.events[delivery_result_event.event_id] = delivery_result_event

    initial_route = resolver.resolve(
        OutboxEventRow(
            event_id=delivery_result_event.event_id,
            event_type=delivery_result_event.event_type,
            aggregate_type=delivery_result_event.aggregate_type,
            aggregate_id=delivery_result_event.aggregate_id,
            dedupe_key="delivery-result",
            payload_json=delivery_result_event.payload_json,
            status="pending",
            fail_count=0,
            created_at=datetime.now(timezone.utc),
        )
    )
    await MaintenanceService(config(), repository=repository).handle_maintenance_trigger_event(delivery_result_event.event_id)
    retry_route = resolver.resolve(_route_row(repository.plan_created_outbox[0]))

    assert initial_route.queue_name == "q.maintenance"
    assert repository.plan_created_outbox[0]["event_type"] == "notification.plan.created.v1"
    assert retry_route.queue_name == "q.notification.send"
