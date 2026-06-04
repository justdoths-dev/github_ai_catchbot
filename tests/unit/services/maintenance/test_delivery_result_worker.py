from __future__ import annotations

from pathlib import Path

import pytest

from services.maintenance.models import StreamMessage
from services.maintenance.retry_policy import (
    DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
    DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
)
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import MaintenanceQueueWorker
from tests.component.services.maintenance._fakes import (
    FakeConsumer,
    FakeRepository,
    config,
    latest_delivery_record,
    outbox_event,
    plan,
)


ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.asyncio
async def test_sent_delivery_result_worker_writes_exactly_one_maintenance_marker() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=1,
    )
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "notification_delivery_record_id": str(latest.notification_delivery_record_id),
            "delivery_status": "sent",
        },
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is True
    assert result.classification == "terminal_success"
    assert result.action == "mark_terminal_success"
    assert result.marker_written is True
    assert result.retry_intent_written is False
    assert result.dead_letter_written is False
    assert result.replay_request_written is False
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1
    marker = repository.job_attempts[0]
    assert marker["stage_name"] == DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME
    assert marker["queue_name"] == "q.maintenance"
    assert marker["root_object_type"] == "notification_delivery_record"
    assert marker["root_object_id"] == latest.notification_delivery_record_id
    assert marker["attempt_status"] == "succeeded"
    assert marker["error_code"] == DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE


@pytest.mark.asyncio
async def test_existing_sent_success_marker_blocks_duplicate_write() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=1,
    )
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "notification_delivery_record_id": str(latest.notification_delivery_record_id),
            "delivery_status": "sent",
        },
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    first = await service.handle_maintenance_trigger_event(event.event_id)
    second = await service.handle_maintenance_trigger_event(event.event_id)

    assert first is not None
    assert first.marker_written is True
    assert second is not None
    assert second.action == "already_marked"
    assert second.marker_written is False
    assert second.already_marked is True
    assert len(repository.job_attempts) == 1
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_failed_retryable_result_consumer_records_candidate_without_retry_intent() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="failed_retryable",
        attempt_count=1,
    )
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest
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
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.classification == "retryable_candidate"
    assert result.action == "record_retryable_interpretation"
    assert result.reason_code == "failed_retryable_deferred_to_due_scan"
    assert result.marker_written is False
    assert result.retry_intent_written is False
    assert result.dead_letter_written is False
    assert result.replay_request_written is False
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["attempt_status"] == "failed_retryable"
    assert repository.job_attempts[0]["error_code"] == "delivery_result_failed_retryable_due_scan_candidate"


@pytest.mark.asyncio
async def test_unsupported_event_type_is_ignored_without_db_write() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    event = outbox_event(
        "notification.plan.created.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={"notification_plan_id": str(notification_plan.notification_plan_id)},
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "unsupported"
    assert result.action == "unsupported"
    assert repository.job_attempts == []
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_worker_uses_trigger_event_rehydration_not_stream_business_fields() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=1,
    )
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(notification_plan.notification_plan_id),
            "notification_delivery_record_id": str(latest.notification_delivery_record_id),
            "delivery_status": "failed_retryable",
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
                    "notification_plan_id": "do-not-trust-this",
                    "notification_delivery_record_id": "do-not-trust-this",
                    "delivery_status": "failed_retryable",
                    "retry_reason": "do-not-trust-this",
                },
            )
        ]
    )
    service = MaintenanceService(config(), repository=repository)
    worker = MaintenanceQueueWorker(config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["root_object_id"] == latest.notification_delivery_record_id
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


def test_delivery_result_worker_does_not_mutate_notifier_or_upstream_tables() -> None:
    forbidden_mutations = [
        "update notification_plans",
        "insert into notification_plans",
        "delete from notification_plans",
        "update notification_renders",
        "insert into notification_renders",
        "delete from notification_renders",
        "update notification_delivery_records",
        "insert into notification_delivery_records",
        "delete from notification_delivery_records",
        "insert into state_transitions",
        "update analyses",
        "insert into analyses",
        "update judge_outputs",
        "insert into judge_outputs",
        "update candidate_group_proposals",
        "insert into candidate_group_proposals",
        "update candidate_evidence_bundles",
        "insert into candidate_evidence_bundles",
    ]
    for relative_path in [
        "src/services/maintenance/delivery_result_worker.py",
        "src/services/maintenance/service.py",
        "src/services/maintenance/repositories.py",
    ]:
        text = (ROOT / relative_path).read_text(encoding="utf-8").lower()
        for forbidden in forbidden_mutations:
            assert forbidden not in text
