from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

import pytest

from services.maintenance.batch_recovery import prepare_delivery_replay_requests_for_selected_plans
from tests.component.services.maintenance._batch_recovery_fakes import FakeSelectedPlanReplayRepository
from tests.unit.services.maintenance.test_batch_recovery_validation import _row


@pytest.mark.asyncio
async def test_selected_batch_recovery_preserves_notifier_and_upstream_rows() -> None:
    row = _row(delivery_status="suppressed", send_disabled=True)
    repository = FakeSelectedPlanReplayRepository([row])
    repository.notification_plans = {
        row.notification_plan_id: {
            "notification_plan_id": row.notification_plan_id,
            "analysis_id": row.analysis_id,
            "candidate_group_id": row.candidate_group_id,
            "status": "suppressed",
        }
    }
    repository.notification_delivery_records = {
        row.notification_plan_id: {
            "notification_plan_id": row.notification_plan_id,
            "delivery_status": "suppressed",
            "telegram_response_json": {"send_disabled": True},
        }
    }
    judge_output_id = uuid4()
    bundle_id = uuid4()
    repository.analyses = {
        row.analysis_id: {
            "analysis_id": row.analysis_id,
            "candidate_group_id": row.candidate_group_id,
            "delivery_decision": "send_now",
        }
    }
    repository.judge_outputs = {
        judge_output_id: {
            "judge_output_id": judge_output_id,
            "analysis_id": row.analysis_id,
            "schema_version": "judge_output_v1",
        }
    }
    repository.candidates = {
        row.candidate_group_id: {
            "candidate_group_id": row.candidate_group_id,
            "status": "ready_for_analysis",
        }
    }
    repository.bundles = {
        bundle_id: {
            "candidate_group_id": row.candidate_group_id,
            "bundle_version": 1,
        }
    }
    before = deepcopy(
        {
            "notification_plans": repository.notification_plans,
            "notification_delivery_records": repository.notification_delivery_records,
            "analyses": repository.analyses,
            "judge_outputs": repository.judge_outputs,
            "candidates": repository.candidates,
            "bundles": repository.bundles,
            "event_outbox": repository.event_outbox,
            "job_attempts": repository.job_attempts,
        }
    )

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=[row.notification_plan_id],
        requested_by="test/operator",
        operator_confirmed=True,
    )

    assert result.created_count == 1
    assert repository.replay_requests == [
        {
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": row.notification_plan_id,
            "requested_by": "test/operator",
            "status": "requested",
        }
    ]
    assert {
        "notification_plans": repository.notification_plans,
        "notification_delivery_records": repository.notification_delivery_records,
        "analyses": repository.analyses,
        "judge_outputs": repository.judge_outputs,
        "candidates": repository.candidates,
        "bundles": repository.bundles,
        "event_outbox": repository.event_outbox,
        "job_attempts": repository.job_attempts,
    } == before
