from __future__ import annotations

from copy import deepcopy

import pytest

from services.maintenance.batch_recovery import (
    BATCH_RECOVERY_OPEN_REPLAY_EXISTS,
    BATCH_RECOVERY_REPLAY_REQUEST_CREATED,
    prepare_delivery_replay_requests_for_selected_plans,
)
from services.maintenance.models import NotificationPlanRecord, OutboxEvent, ReplayRequestRecord
from services.maintenance.repositories import replay_requested_from_outbox
from services.maintenance.service import MaintenanceService

from ._batch_recovery_fakes import AtomicReplayInsertSession, FakeSelectedPlanReplayRepository
from ._fakes import FakeRepository, config
from tests.unit.services.maintenance.test_batch_recovery_validation import _row


@pytest.mark.asyncio
async def test_send_disabled_selected_plan_creates_atomic_replay_request_and_event() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    session = AtomicReplayInsertSession()
    repository = FakeSelectedPlanReplayRepository([row], atomic_session=session)
    rows_before = deepcopy(repository.rows)

    async with repository.transaction():
        result = await prepare_delivery_replay_requests_for_selected_plans(
            repository=repository,
            selected_plan_ids=[row.notification_plan_id],
            requested_by="test/operator",
            operator_confirmed=True,
        )

    assert result.status == "completed"
    assert result.requested_count == 1
    assert result.created_count == 1
    assert result.skipped_count == 0
    assert result.results[0].action == "replay_request_created"
    assert result.results[0].reason_code == BATCH_RECOVERY_REPLAY_REQUEST_CREATED
    assert result.results[0].replay_request_created is True
    assert len(repository.replay_requests) == 1
    replay_request = repository.replay_requests[0]
    assert replay_request == {
        "replay_request_id": replay_request["replay_request_id"],
        "replay_type": "delivery",
        "root_object_type": "notification_plan",
        "root_object_id": row.notification_plan_id,
        "requested_by": "test/operator",
        "status": "requested",
    }
    assert len(repository.event_outbox) == 1
    replay_event = repository.event_outbox[0]
    assert replay_event == {
        "event_id": replay_event["event_id"],
        "event_type": "replay.requested.v1",
        "aggregate_type": "replay_request",
        "aggregate_id": replay_request["replay_request_id"],
        "dedupe_key": f"maintenance:replay-requested:v1:{replay_request['replay_request_id']}",
        "payload_json": {
            "replay_request_id": str(replay_request["replay_request_id"]),
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": str(row.notification_plan_id),
            "replay_reason": "batch_recovery_replay_selected",
        },
        "status": "pending",
    }
    assert repository.job_attempts == []
    assert repository.notification_plan_mutations == []
    assert repository.notification_delivery_record_mutations == []
    assert repository.rows == rows_before


@pytest.mark.asyncio
async def test_duplicate_call_is_idempotent_for_open_replay_request() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    session = AtomicReplayInsertSession()
    repository = FakeSelectedPlanReplayRepository([row], atomic_session=session)

    async with repository.transaction():
        first = await prepare_delivery_replay_requests_for_selected_plans(
            repository=repository,
            selected_plan_ids=[row.notification_plan_id],
            requested_by="test/operator",
            operator_confirmed=True,
        )
    async with repository.transaction():
        second = await prepare_delivery_replay_requests_for_selected_plans(
            repository=repository,
            selected_plan_ids=[row.notification_plan_id],
            requested_by="test/operator",
            operator_confirmed=True,
        )

    assert first.created_count == 1
    assert second.created_count == 0
    assert second.skipped_count == 1
    assert second.results[0].reason_code == BATCH_RECOVERY_OPEN_REPLAY_EXISTS
    assert len(repository.replay_requests) == 1
    assert len(repository.event_outbox) == 1
    assert session.execute_calls == 1


@pytest.mark.asyncio
async def test_batch_recovery_event_is_accepted_by_existing_parser_and_service() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    producer_session = AtomicReplayInsertSession()
    producer_repository = FakeSelectedPlanReplayRepository([row], atomic_session=producer_session)

    async with producer_repository.transaction():
        result = await prepare_delivery_replay_requests_for_selected_plans(
            repository=producer_repository,
            selected_plan_ids=[row.notification_plan_id],
            requested_by="test/operator",
            operator_confirmed=True,
        )

    assert result.created_count == 1
    replay_request_row = producer_repository.replay_requests[0]
    replay_event_row = producer_repository.event_outbox[0]
    replay_event = OutboxEvent(
        event_id=replay_event_row["event_id"],
        event_type=replay_event_row["event_type"],
        aggregate_type=replay_event_row["aggregate_type"],
        aggregate_id=replay_event_row["aggregate_id"],
        payload_json=replay_event_row["payload_json"],
        status=replay_event_row["status"],
    )
    parsed = replay_requested_from_outbox(replay_event)

    assert parsed is not None
    assert parsed.replay_request_id == replay_request_row["replay_request_id"]
    assert parsed.root_object_id == row.notification_plan_id
    assert parsed.replay_reason == "batch_recovery_replay_selected"

    consumer_repository = FakeRepository()
    consumer_repository.events[replay_event.event_id] = replay_event
    consumer_repository.replay_requests[parsed.replay_request_id] = ReplayRequestRecord(
        replay_request_id=parsed.replay_request_id,
        replay_type="delivery",
        root_object_type="notification_plan",
        root_object_id=row.notification_plan_id,
        status="requested",
        requested_by="test/operator",
    )
    consumer_repository.plans[row.notification_plan_id] = NotificationPlanRecord(
        notification_plan_id=row.notification_plan_id,
        analysis_id=row.analysis_id,
        candidate_group_id=row.candidate_group_id,
        delivery_decision=row.delivery_decision,
        urgency_profile=row.urgency_profile,
        target_chat_id=row.target_chat_id,
        target_thread_id=row.target_thread_id,
        render_profile=row.render_profile,
        dedupe_subject_key=row.dedupe_subject_key,
        material_change_hash=row.material_change_hash,
        send_after=row.send_after,
        suppress_reason_code=None,
        status=row.plan_status,
    )
    assert consumer_repository.plan_created_outbox == []

    decision = await MaintenanceService(
        config(app_env="test"),
        repository=consumer_repository,
    ).handle_replay_trigger_event(
        replay_event.event_id,
    )

    assert decision is not None
    assert decision.action == "emit_replay_intent"
    assert len(consumer_repository.plan_created_outbox) == 1
    assert consumer_repository.plan_created_outbox[0]["event_type"] == "notification.plan.created.v1"
