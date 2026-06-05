from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from uuid import uuid4

import pytest

from services.maintenance.models import ReplayRequestRecord
from services.maintenance.service import MaintenanceService
from tests.component.services.maintenance._fakes import (
    FakeRepository,
    config,
    latest_delivery_record,
    outbox_event,
    plan,
)


@pytest.mark.asyncio
async def test_explicit_delivery_replay_preserves_upstream_and_notifier_owned_rows() -> None:
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
    repository.analyses = {
        notification_plan.analysis_id: {
            "analysis_id": notification_plan.analysis_id,
            "candidate_group_id": notification_plan.candidate_group_id,
            "delivery_decision": "send_now",
            "version": 1,
        }
    }
    judge_output_id = uuid4()
    bundle_id = uuid4()
    repository.judge_outputs = {
        judge_output_id: {
            "judge_output_id": judge_output_id,
            "analysis_id": notification_plan.analysis_id,
            "schema_version": "judge_output_v1",
        }
    }
    repository.candidates = {
        notification_plan.candidate_group_id: {
            "candidate_group_id": notification_plan.candidate_group_id,
            "status": "ready_for_analysis",
        }
    }
    repository.bundles = {
        bundle_id: {
            "candidate_group_id": notification_plan.candidate_group_id,
            "bundle_version": 1,
        }
    }
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
        },
    )
    repository.events[event.event_id] = event
    upstream_before = deepcopy(
        {
            "analyses": repository.analyses,
            "judge_outputs": repository.judge_outputs,
            "candidates": repository.candidates,
            "bundles": repository.bundles,
        }
    )
    plans_before = deepcopy(repository.plans)
    delivery_records_before = deepcopy(repository.latest_delivery_records)

    await MaintenanceService(config(app_env="test"), repository=repository).handle_replay_trigger_event(event.event_id)

    assert {
        "analyses": repository.analyses,
        "judge_outputs": repository.judge_outputs,
        "candidates": repository.candidates,
        "bundles": repository.bundles,
    } == upstream_before
    assert repository.plans == plans_before
    assert repository.latest_delivery_records == delivery_records_before
    assert repository.upstream_recompute_calls == 0
    assert repository.replay_status_updates == [
        (replay_request_id, "dispatched"),
        (replay_request_id, "completed"),
    ]
    assert len(repository.plan_created_outbox) == 1
    emitted = repository.plan_created_outbox[0]
    assert emitted["event_type"] == "notification.plan.created.v1"
    assert emitted["status"] == "pending"
    assert emitted["dedupe_key"] == f"notify:replay-intent:{replay_request_id}"
    assert emitted["payload_json"]["notification_plan_id"] == str(notification_plan.notification_plan_id)
    assert emitted["payload_json"]["analysis_id"] == str(notification_plan.analysis_id)
    assert emitted["payload_json"]["candidate_group_id"] == str(notification_plan.candidate_group_id)
    assert emitted["payload_json"]["send_after"] is None
    assert emitted["payload_json"]["replay_reason"] == "explicit_delivery_replay"
