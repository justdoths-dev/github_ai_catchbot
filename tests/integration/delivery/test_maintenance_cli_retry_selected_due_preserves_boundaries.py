from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from services.maintenance import main as maintenance_main
from services.maintenance.models import SelectedPlanRecoveryRow
from tests.unit.services.maintenance.test_batch_recovery_validation import _config, _row


class BoundaryRetryRepository:
    def __init__(self, rows: list[SelectedPlanRecoveryRow]) -> None:
        self.rows = {row.notification_plan_id: row for row in rows}
        self.load_calls: list[list[UUID]] = []
        self.replay_requests: list[dict] = []
        self.event_outbox: list[dict] = []
        self.notification_plans: dict = {}
        self.notification_renders: dict = {}
        self.notification_delivery_records: dict = {}
        self.state_transitions: list[dict] = []
        self.analyses: dict = {}
        self.judge_outputs: dict = {}
        self.candidates: dict = {}
        self.bundles: dict = {}
        self.redis_dispatches: list[dict] = []
        self.notifier_calls: list[dict] = []

    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]):
        self.load_calls.append(notification_plan_ids)
        return [self.rows[plan_id] for plan_id in notification_plan_ids if plan_id in self.rows]

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[UUID], requested_by: str) -> int:
        raise AssertionError("retry-selected-due must not create replay_requests")

    async def insert_manual_retry_intent_outbox(
        self,
        *,
        row: SelectedPlanRecoveryRow,
        recovery_batch_id: str,
        dedupe_key: str,
        payload_json: dict,
    ) -> bool:
        self.event_outbox.append(
            {
                "event_type": "notification.plan.created.v1",
                "aggregate_type": "analysis",
                "aggregate_id": row.analysis_id,
                "status": "pending",
                "dedupe_key": dedupe_key,
                "payload_json": payload_json,
                "recovery_batch_id": recovery_batch_id,
            }
        )
        return True


@pytest.mark.asyncio
async def test_confirmed_retry_selected_due_appends_only_manual_retry_intent_outbox() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = _row(delivery_status="failed_retryable", send_after=now - timedelta(seconds=1), attempt_count=2)
    repository = BoundaryRetryRepository([row])
    judge_output_id = uuid4()
    bundle_id = uuid4()
    render_id = uuid4()
    delivery_record_id = uuid4()
    repository.notification_plans = {
        row.notification_plan_id: {
            "notification_plan_id": row.notification_plan_id,
            "analysis_id": row.analysis_id,
            "candidate_group_id": row.candidate_group_id,
            "status": "failed_retryable",
            "send_after": row.send_after,
        }
    }
    repository.notification_renders = {
        render_id: {
            "notification_plan_id": row.notification_plan_id,
            "render_profile": row.render_profile,
        }
    }
    repository.notification_delivery_records = {
        delivery_record_id: {
            "notification_plan_id": row.notification_plan_id,
            "delivery_status": "failed_retryable",
            "attempt_count": 2,
        }
    }
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
            "notification_renders": repository.notification_renders,
            "notification_delivery_records": repository.notification_delivery_records,
            "state_transitions": repository.state_transitions,
            "analyses": repository.analyses,
            "judge_outputs": repository.judge_outputs,
            "candidates": repository.candidates,
            "bundles": repository.bundles,
            "replay_requests": repository.replay_requests,
            "redis_dispatches": repository.redis_dispatches,
            "notifier_calls": repository.notifier_calls,
        }
    )
    args = maintenance_main.build_parser().parse_args(
        [
            "batch-recovery",
            "retry-selected-due",
            "--plan-id",
            str(row.notification_plan_id),
            "--requested-by",
            "test/operator",
            "--confirm",
            "write",
        ]
    )

    exit_code = await maintenance_main.run_retry_selected_due_batch_recovery(
        _config(),
        args,
        repository,
        emit_json=lambda _: None,
    )

    assert exit_code == 0
    assert repository.load_calls == [[row.notification_plan_id]]
    assert len(repository.event_outbox) == 1
    event = repository.event_outbox[0]
    assert event["event_type"] == "notification.plan.created.v1"
    assert event["status"] == "pending"
    assert event["payload_json"]["notification_plan_id"] == str(row.notification_plan_id)
    assert event["payload_json"]["retry_reason"] == "manual_selected_due_retry"
    assert event["payload_json"]["previous_attempt_count"] == 2
    assert {
        "notification_plans": repository.notification_plans,
        "notification_renders": repository.notification_renders,
        "notification_delivery_records": repository.notification_delivery_records,
        "state_transitions": repository.state_transitions,
        "analyses": repository.analyses,
        "judge_outputs": repository.judge_outputs,
        "candidates": repository.candidates,
        "bundles": repository.bundles,
        "replay_requests": repository.replay_requests,
        "redis_dispatches": repository.redis_dispatches,
        "notifier_calls": repository.notifier_calls,
    } == before
