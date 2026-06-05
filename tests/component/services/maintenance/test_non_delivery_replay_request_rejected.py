from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

import pytest

from services.maintenance.models import ReplayRequestRecord
from services.maintenance.service import MaintenanceService

from ._fakes import FakeRepository, config, latest_delivery_record, outbox_event, plan


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("replay_type", "root_object_type"),
    [
        ("source", "notification_plan"),
        ("delivery", "candidate_group"),
    ],
)
async def test_non_delivery_replay_request_is_rejected_without_notification_intent(
    replay_type: str,
    root_object_type: str,
) -> None:
    repository = FakeRepository()
    notification_plan = plan(status="suppressed", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        delivery_status="suppressed",
        attempt_count=1,
        transport_error_code="notification_send_flag_disabled",
        transport_error_class="send_disabled",
    )
    replay_request_id = uuid4()
    root_object_id = notification_plan.notification_plan_id if root_object_type == "notification_plan" else uuid4()
    repository.replay_requests[replay_request_id] = ReplayRequestRecord(
        replay_request_id=replay_request_id,
        replay_type=replay_type,
        root_object_type=root_object_type,
        root_object_id=root_object_id,
        status="requested",
    )
    event = outbox_event(
        "replay.requested.v1",
        aggregate_id=replay_request_id,
        payload_json={
            "replay_request_id": str(replay_request_id),
            "replay_type": replay_type,
            "root_object_type": root_object_type,
            "root_object_id": str(root_object_id),
        },
    )
    repository.events[event.event_id] = event
    plans_before = deepcopy(repository.plans)
    delivery_records_before = deepcopy(repository.latest_delivery_records)

    await MaintenanceService(config(app_env="test"), repository=repository).handle_replay_trigger_event(event.event_id)

    assert repository.replay_status_updates == [(replay_request_id, "unsupported_in_stage41")]
    assert repository.replay_requests[replay_request_id].status == "unsupported_in_stage41"
    assert repository.plan_created_outbox == []
    assert repository.plans == plans_before
    assert repository.latest_delivery_records == delivery_records_before
    assert repository.upstream_recompute_calls == 0
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["queue_name"] == "q.replay"
    assert repository.job_attempts[0]["root_object_id"] == root_object_id
    assert repository.job_attempts[0]["attempt_status"] == "failed_terminal"
    assert repository.job_attempts[0]["error_code"] in {"unsupported_replay_type", "unsupported_replay_root"}


@pytest.mark.asyncio
async def test_delivery_replay_request_missing_notification_plan_fails_without_notification_intent() -> None:
    repository = FakeRepository()
    missing_plan_id = uuid4()
    replay_request_id = uuid4()
    repository.replay_requests[replay_request_id] = ReplayRequestRecord(
        replay_request_id=replay_request_id,
        replay_type="delivery",
        root_object_type="notification_plan",
        root_object_id=missing_plan_id,
        status="requested",
    )
    event = outbox_event(
        "replay.requested.v1",
        aggregate_id=replay_request_id,
        payload_json={
            "replay_request_id": str(replay_request_id),
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": str(missing_plan_id),
        },
    )
    repository.events[event.event_id] = event

    await MaintenanceService(config(app_env="test"), repository=repository).handle_replay_trigger_event(event.event_id)

    assert repository.replay_status_updates == [(replay_request_id, "failed")]
    assert repository.replay_requests[replay_request_id].status == "failed"
    assert repository.plan_created_outbox == []
    assert repository.plans == {}
    assert repository.latest_delivery_records == {}
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["queue_name"] == "q.replay"
    assert repository.job_attempts[0]["root_object_id"] == missing_plan_id
    assert repository.job_attempts[0]["attempt_status"] == "failed_terminal"
    assert repository.job_attempts[0]["error_code"] == "notification_plan_missing"
