from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

import pytest

from services.maintenance.models import ReplayRequestRecord
from services.maintenance.service import MaintenanceService

from ._fakes import FakeRepository, config, latest_delivery_record, outbox_event, plan


def _prepare_valid_delivery_replay(repository: FakeRepository):
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
    repository.replay_requests[replay_request_id] = ReplayRequestRecord(
        replay_request_id=replay_request_id,
        replay_type="delivery",
        root_object_type="notification_plan",
        root_object_id=notification_plan.notification_plan_id,
        status="requested",
    )
    event = outbox_event(
        "replay.requested.v1",
        aggregate_type="replay_request",
        aggregate_id=replay_request_id,
        payload_json={
            "replay_request_id": str(replay_request_id),
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": str(notification_plan.notification_plan_id),
        },
    )
    repository.events[event.event_id] = event
    return notification_plan, replay_request_id, event


@pytest.mark.asyncio
async def test_prod_replay_guard_blocks_dispatch_without_opt_in() -> None:
    repository = FakeRepository()
    notification_plan, replay_request_id, event = _prepare_valid_delivery_replay(repository)
    plans_before = deepcopy(repository.plans)
    delivery_records_before = deepcopy(repository.latest_delivery_records)

    await MaintenanceService(
        config(app_env="prod", enable_replay_to_prod_db=False),
        repository=repository,
    ).handle_replay_trigger_event(event.event_id)

    assert repository.replay_status_updates == [(replay_request_id, "rejected_by_env_guard")]
    assert repository.replay_requests[replay_request_id].status == "rejected_by_env_guard"
    assert repository.plan_created_outbox == []
    assert repository.plans == plans_before
    assert repository.latest_delivery_records == delivery_records_before
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["root_object_id"] == notification_plan.notification_plan_id
    assert repository.job_attempts[0]["attempt_status"] == "failed_terminal"
    assert repository.job_attempts[0]["error_code"] == "rejected_by_env_guard"


@pytest.mark.asyncio
async def test_prod_replay_guard_allows_dispatch_with_explicit_opt_in() -> None:
    repository = FakeRepository()
    notification_plan, replay_request_id, event = _prepare_valid_delivery_replay(repository)

    await MaintenanceService(
        config(app_env="prod", enable_replay_to_prod_db=True),
        repository=repository,
    ).handle_replay_trigger_event(event.event_id)

    assert repository.replay_status_updates == [
        (replay_request_id, "dispatched"),
        (replay_request_id, "completed"),
    ]
    assert repository.replay_requests[replay_request_id].status == "completed"
    assert len(repository.plan_created_outbox) == 1
    emitted = repository.plan_created_outbox[0]
    assert emitted["event_type"] == "notification.plan.created.v1"
    assert emitted["dedupe_key"] == f"notify:replay-intent:{replay_request_id}"
    assert emitted["payload_json"]["notification_plan_id"] == str(notification_plan.notification_plan_id)
    assert emitted["payload_json"]["replay_request_id"] == str(replay_request_id)
