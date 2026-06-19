from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from services.maintenance.models import StreamMessage
from services.outbox_relay.eligibility import (
    DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
    DELIVERY_RESULT_FAILED_TERMINAL_DLQ_RECEIPT_CODE,
    DELIVERY_RESULT_SUPERSEDED_NOOP_RECEIPT_CODE,
    DELIVERY_RESULT_SUPPRESSED_NOOP_RECEIPT_CODE,
    DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
    EVENT_OUTBOX_ROOT_OBJECT_TYPE,
    MAINTENANCE_DELIVERY_RESULT_STAGE,
    MAINTENANCE_QUEUE_NAME,
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


def _add_delivery_record(repository: FakeRepository, record) -> None:
    repository.delivery_records[record.notification_delivery_record_id] = record
    repository.latest_delivery_records[record.notification_plan_id] = record


def _delivery_result_payload(notification_plan, record, **overrides):
    payload = {
        "notification_plan_id": str(notification_plan.notification_plan_id),
        "notification_delivery_record_id": str(record.notification_delivery_record_id),
        "delivery_status": record.delivery_status,
        "attempt_count": record.attempt_count,
    }
    if record.transport_error_code is not None:
        payload["transport_error_code"] = record.transport_error_code
    if record.transport_error_class is not None:
        payload["transport_error_class"] = record.transport_error_class
    payload.update(overrides)
    return payload


def _assert_delivery_receipt(marker: dict, *, event_id, receipt_code: str) -> None:
    assert marker["stage_name"] == MAINTENANCE_DELIVERY_RESULT_STAGE
    assert marker["queue_name"] == MAINTENANCE_QUEUE_NAME
    assert marker["root_object_type"] == EVENT_OUTBOX_ROOT_OBJECT_TYPE
    assert marker["root_object_id"] == event_id
    assert marker["attempt_status"] == "succeeded"
    assert marker["error_code"] == receipt_code


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
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
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
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
    )


@pytest.mark.asyncio
async def test_edited_delivery_result_records_success_without_retry_or_replay() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="edited", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="edited",
        attempt_count=2,
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.classification == "terminal_success"
    assert result.action == "mark_terminal_success"
    assert result.retry_intent_written is False
    assert result.dead_letter_written is False
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
    )


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
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
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
async def test_missing_delivery_result_event_is_ignored_without_db_write() -> None:
    repository = FakeRepository()
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(uuid4())

    assert result is not None
    assert result.processed is False
    assert result.reason_code == "event_outbox_missing"
    assert repository.job_attempts == []
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_missing_notification_plan_fails_closed_without_receipt() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=1,
    )
    repository.delivery_records[latest.notification_delivery_record_id] = latest
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "identity_invalid"
    assert result.reason_code == "notification_plan_missing"
    assert repository.job_attempts == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_missing_exact_delivery_record_fails_closed_without_receipt() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    missing_record = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=1,
    )
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, missing_record),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "identity_invalid"
    assert result.reason_code == "exact_delivery_record_missing"
    assert repository.job_attempts == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_delivery_record_plan_mismatch_fails_closed_without_receipt() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    other_plan = plan(status="sent", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    mismatched_record = latest_delivery_record(
        notification_plan_id=other_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=1,
    )
    repository.delivery_records[mismatched_record.notification_delivery_record_id] = mismatched_record
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            **_delivery_result_payload(other_plan, mismatched_record),
            "notification_plan_id": str(notification_plan.notification_plan_id),
        },
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "identity_invalid"
    assert result.reason_code == "delivery_result_identity_mismatch"
    assert repository.job_attempts == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_delivery_payload_identity_mismatch_fails_closed_without_receipt() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=1,
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest, delivery_status="failed_retryable"),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "identity_invalid"
    assert result.reason_code == "delivery_result_identity_mismatch"
    assert repository.job_attempts == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_unknown_delivery_status_fails_closed_without_receipt() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="failed_retryable")
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="transport_weird",
        attempt_count=1,
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "identity_invalid"
    assert result.reason_code == "delivery_status_unknown"
    assert repository.job_attempts == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
@pytest.mark.parametrize("older_status", ["suppressed", "failed_retryable"])
async def test_older_delivery_result_followed_by_sent_becomes_superseded_noop(older_status: str) -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None, suppress_reason_code="dry_run_skip_transport")
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    base_time = datetime.now(timezone.utc)
    older = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status=older_status,
        attempt_count=1,
        transport_error_code="dry_run_skip_transport" if older_status == "suppressed" else "telegram_retryable_error",
        transport_error_class=None if older_status == "suppressed" else "retryable",
        created_at=base_time - timedelta(minutes=1),
    )
    newer = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="sent",
        attempt_count=2,
        transport_error_code=None,
        transport_error_class=None,
        created_at=base_time,
    )
    repository.delivery_records[older.notification_delivery_record_id] = older
    repository.delivery_records[newer.notification_delivery_record_id] = newer
    repository.latest_delivery_records[notification_plan.notification_plan_id] = newer
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, older),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is True
    assert result.classification == "superseded_noop"
    assert result.action == "mark_superseded_noop_success"
    assert result.retry_intent_written is False
    assert result.dead_letter_written is False
    assert len(repository.job_attempts) == 1
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_SUPERSEDED_NOOP_RECEIPT_CODE,
    )


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
    _add_delivery_record(repository, latest)
    repository.delivery_attempt_counts[notification_plan.notification_plan_id] = 1
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.classification == "retryable_candidate"
    assert result.action == "record_retryable_interpretation"
    assert result.reason_code == "failed_retryable_deferred_to_due_scan"
    assert result.marker_written is True
    assert result.retry_intent_written is False
    assert result.dead_letter_written is False
    assert result.replay_request_written is False
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
    )


@pytest.mark.asyncio
async def test_suppressed_dry_run_result_records_logical_noop_without_retry() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="suppressed", send_after=None, suppress_reason_code="dry_run_skip_transport")
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="suppressed",
        attempt_count=0,
        transport_error_code="dry_run_skip_transport",
        transport_error_class=None,
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.classification == "logical_noop_success"
    assert result.action == "mark_logical_noop_success"
    assert result.reason_code == "dry_run_skip_transport"
    assert result.retry_intent_written is False
    assert result.dead_letter_written is False
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_SUPPRESSED_NOOP_RECEIPT_CODE,
    )


@pytest.mark.asyncio
async def test_suppressed_send_disabled_result_records_distinct_noop_without_retry() -> None:
    repository = FakeRepository()
    notification_plan = plan(
        status="suppressed",
        send_after=None,
        suppress_reason_code="notification_send_flag_disabled",
    )
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="suppressed",
        attempt_count=0,
        transport_error_code="notification_send_flag_disabled",
        transport_error_class=None,
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.classification == "logical_noop_success"
    assert result.action == "mark_logical_noop_success"
    assert result.reason_code == "notification_send_flag_disabled"
    assert result.retry_intent_written is False
    assert result.dead_letter_written is False
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_SUPPRESSED_NOOP_RECEIPT_CODE,
    )


@pytest.mark.asyncio
async def test_suppressed_unknown_reason_records_generic_noop_without_retry() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="suppressed", send_after=None, suppress_reason_code="policy_suppressed")
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="suppressed",
        attempt_count=0,
        transport_error_code="policy_suppressed",
        transport_error_class=None,
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.classification == "logical_noop_success"
    assert result.reason_code == "suppressed_other_known_reason"
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
    assert len(repository.job_attempts) == 1


@pytest.mark.asyncio
async def test_failed_terminal_result_creates_delivery_dlq_without_retry_intent() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="failed_terminal",
        attempt_count=3,
        transport_error_code="notify_transport_terminal_chat_access",
        transport_error_class="terminal",
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.classification == "terminal_failure"
    assert result.action == "record_terminal_failure"
    assert result.retry_intent_written is False
    assert result.dead_letter_written is True
    assert repository.plan_created_outbox == []
    assert len(repository.job_attempts) == 1
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_FAILED_TERMINAL_DLQ_RECEIPT_CODE,
    )
    assert len(repository.dead_letters) == 1
    assert repository.dead_letters[0]["stage_name"] == "maintenance_delivery_result"
    assert repository.dead_letters[0]["last_error_code"] == "notify_transport_terminal_chat_access"
    assert repository.dead_letters[0]["next_manual_action"] == "request_delivery_replay_after_operator_fix"
    assert repository.dead_letters[0]["replay_hint"] == "delivery_replay_from_notification_plan"


@pytest.mark.asyncio
async def test_failed_terminal_result_rerun_creates_no_duplicate_dlq_or_receipt() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="failed_terminal",
        attempt_count=3,
        transport_error_code="notify_transport_terminal_chat_access",
        transport_error_class="terminal",
    )
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    first = await service.handle_maintenance_trigger_event(event.event_id)
    second = await service.handle_maintenance_trigger_event(event.event_id)

    assert first is not None
    assert first.dead_letter_written is True
    assert second is not None
    assert second.action == "already_marked"
    assert second.dead_letter_written is False
    assert len(repository.job_attempts) == 1
    assert len(repository.dead_letters) == 1


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
async def test_malformed_delivery_event_missing_payload_plan_id_fails_closed_without_db_write() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={"delivery_status": "failed_retryable"},
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "identity_invalid"
    assert result.action == "fail_closed"
    assert result.reason_code == "invalid_delivery_result_payload"
    assert repository.job_attempts == []
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_delivery_event_payload_plan_id_mismatch_fails_closed_without_db_write() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    other_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json={
            "notification_plan_id": str(other_plan.notification_plan_id),
            "delivery_status": "failed_retryable",
        },
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "identity_invalid"
    assert result.action == "fail_closed"
    assert result.reason_code == "invalid_delivery_result_payload"
    assert repository.job_attempts == []
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_wrong_aggregate_type_delivery_event_fails_closed_without_db_write() -> None:
    repository = FakeRepository()
    notification_plan = plan()
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        aggregate_type="analysis",
        payload_json={"notification_plan_id": str(notification_plan.notification_plan_id)},
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(), repository=repository)

    result = await service.handle_maintenance_trigger_event(event.event_id)

    assert result is not None
    assert result.processed is False
    assert result.classification == "unsupported"
    assert result.action == "unsupported"
    assert result.reason_code == "unsupported_aggregate_type"
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
    _add_delivery_record(repository, latest)
    event = outbox_event(
        "notification.delivery.result.v1",
        aggregate_id=notification_plan.notification_plan_id,
        payload_json=_delivery_result_payload(notification_plan, latest),
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
    _assert_delivery_receipt(
        repository.job_attempts[0],
        event_id=event.event_id,
        receipt_code=DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
    )
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
