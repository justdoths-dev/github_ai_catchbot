from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from uuid import uuid4

import pytest

from services.maintenance.models import ReplayRequestRecord
from services.maintenance.service import MaintenanceService

from ._fakes import FakeRepository, config, latest_delivery_record, outbox_event, plan


@pytest.mark.asyncio
async def test_replay_requested_delivery_dispatches_notification_plan_created_replay_intent() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="suppressed", send_after=None, suppress_reason_code="notification_send_disabled")
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = replace(
        latest_delivery_record(
            notification_plan_id=notification_plan.notification_plan_id,
            delivery_status="suppressed",
            attempt_count=1,
            transport_error_code="notification_send_flag_disabled",
            transport_error_class="send_disabled",
        ),
        telegram_response_json={"send_disabled": True, "reason_code": "notification_send_flag_disabled"},
    )
    replay_request_id = uuid4()
    repository.replay_requests[replay_request_id] = ReplayRequestRecord(
        replay_request_id=replay_request_id,
        replay_type="delivery",
        root_object_type="notification_plan",
        root_object_id=notification_plan.notification_plan_id,
        status="requested",
    )
    event = outbox_event(
        "replay.requested.v1",
        aggregate_id=replay_request_id,
        payload_json={
            "replay_request_id": str(replay_request_id),
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": str(notification_plan.notification_plan_id),
            "replay_reason": "operator_note",
        },
    )
    repository.events[event.event_id] = event
    plans_before = deepcopy(repository.plans)
    delivery_records_before = deepcopy(repository.latest_delivery_records)

    await MaintenanceService(config(app_env="test"), repository=repository).handle_replay_trigger_event(event.event_id)

    assert repository.replay_status_updates == [
        (replay_request_id, "dispatched"),
        (replay_request_id, "completed"),
    ]
    assert repository.replay_requests[replay_request_id].status == "completed"
    assert len(repository.plan_created_outbox) == 1
    emitted = repository.plan_created_outbox[0]
    payload = emitted["payload_json"]
    assert emitted["event_type"] == "notification.plan.created.v1"
    assert emitted["aggregate_type"] == "analysis"
    assert emitted["aggregate_id"] == notification_plan.analysis_id
    assert emitted["status"] == "pending"
    assert emitted["dedupe_key"] == f"notify:replay-intent:{replay_request_id}"
    assert payload["notification_plan_id"] == str(notification_plan.notification_plan_id)
    assert payload["analysis_id"] == str(notification_plan.analysis_id)
    assert payload["candidate_group_id"] == str(notification_plan.candidate_group_id)
    assert payload["delivery_decision"] == notification_plan.delivery_decision
    assert payload["urgency_profile"] == notification_plan.urgency_profile
    assert payload["target_chat_id"] == notification_plan.target_chat_id
    assert payload["target_thread_id"] == notification_plan.target_thread_id
    assert payload["render_profile"] == notification_plan.render_profile
    assert payload["dedupe_subject_key"] == notification_plan.dedupe_subject_key
    assert payload["material_change_hash"] == notification_plan.material_change_hash
    assert payload["send_after"] is None
    assert payload["replay_reason"] == "explicit_delivery_replay"
    assert payload["replay_request_id"] == str(replay_request_id)
    assert repository.plans == plans_before
    assert repository.latest_delivery_records == delivery_records_before
    assert repository.upstream_recompute_calls == 0
    assert repository.job_attempts == [
        {
            "queue_name": "q.replay",
            "root_object_id": notification_plan.notification_plan_id,
            "error_code": None,
            "stage_name": "maintenance",
            "root_object_type": "notification_plan",
            "attempt_status": "succeeded",
        }
    ]
