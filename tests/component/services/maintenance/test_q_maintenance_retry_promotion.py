from __future__ import annotations

import pytest

from services.maintenance.models import StreamMessage
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import MaintenanceQueueWorker

from ._fakes import FakeConsumer, FakeRepository, config, outbox_event, plan


@pytest.mark.asyncio
async def test_worker_rehydrates_delivery_result_and_emits_retry_intent_without_plan_mutation() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    original_plan = notification_plan
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.delivery_attempt_counts[notification_plan.notification_plan_id] = 1
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "delivery_status": "failed_retryable",
            "attempt_count": 1,
        },
    )
    repository.events[event.event_id] = event
    consumer = FakeConsumer(
        [
            StreamMessage(
                stream="q.maintenance",
                message_id="1-0",
                fields={
                    "trigger_event_id": str(event.event_id),
                    "delivery_status": "sent",
                    "notification_plan_id": "do-not-trust-this",
                },
            )
        ]
    )
    service = MaintenanceService(config(), repository=repository)
    worker = MaintenanceQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()
    await service.handle_maintenance_trigger_event(event.event_id)

    assert result.processed == 1
    assert consumer.acked == ["1-0"]
    assert len(repository.plan_created_outbox) == 1
    row = repository.plan_created_outbox[0]
    assert row["event_type"] == "notification.plan.created.v1"
    assert row["payload_json"]["notification_plan_id"] == str(notification_plan.notification_plan_id)
    assert row["payload_json"]["retry_attempt"] == 2
    assert repository.plans[notification_plan.notification_plan_id] == original_plan
    assert len(repository.plan_created_outbox) == 1
