from __future__ import annotations

import pytest

from services.maintenance.models import StreamMessage
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import MaintenanceQueueWorker
from services.outbox_relay.eligibility import DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE

from ._fakes import FakeConsumer, FakeRepository, config, latest_delivery_record, outbox_event, plan


@pytest.mark.asyncio
async def test_worker_rehydrates_delivery_result_and_records_retryable_without_retry_intent_or_plan_mutation() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    original_plan = notification_plan
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.delivery_attempt_counts[notification_plan.notification_plan_id] = 1
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="failed_retryable",
        attempt_count=1,
    )
    repository.delivery_records[latest.notification_delivery_record_id] = latest
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "notification_delivery_record_id": str(latest.notification_delivery_record_id),
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

    assert result.processed == 1
    assert consumer.acked == ["1-0"]
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["attempt_status"] == "succeeded"
    assert repository.job_attempts[0]["error_code"] == DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE
    assert repository.plans[notification_plan.notification_plan_id] == original_plan
