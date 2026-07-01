from __future__ import annotations

import pytest

from services.maintenance.delivery_operations_gate import DeliveryOperationsGate
from tests.unit.services.maintenance.test_delivery_gate_runner import _config
from tests.unit.services.maintenance.test_delivery_operations_gate import _snapshot


class ReadOnlyGateRepository:
    def __init__(self) -> None:
        self.load_count = 0
        self.event_outbox_appends = 0
        self.replay_request_writes = 0
        self.job_attempt_writes = 0
        self.notification_plan_mutations = 0
        self.notification_delivery_record_mutations = 0

    async def load_delivery_gate_snapshot(self):
        self.load_count += 1
        return _snapshot(open_delivery_dlq_count=1, replay_guard_reject_count_24h=1)

    async def insert_plan_created_outbox(self, **kwargs):
        self.event_outbox_appends += 1
        raise AssertionError("gate report generation must not append event_outbox")

    async def insert_replay_requests_for_selected_plans(self, **kwargs):
        self.replay_request_writes += 1
        raise AssertionError("gate report generation must not write replay_requests")

    async def insert_job_attempt(self, **kwargs):
        self.job_attempt_writes += 1
        raise AssertionError("gate report generation must not write job_attempts")

    async def update_notification_plan(self, **kwargs):
        self.notification_plan_mutations += 1
        raise AssertionError("gate report generation must not mutate notification_plans")

    async def insert_delivery_record(self, **kwargs):
        self.notification_delivery_record_mutations += 1
        raise AssertionError("gate report generation must not mutate notification_delivery_records")


@pytest.mark.asyncio
async def test_delivery_operations_gate_report_generation_is_read_only() -> None:
    repository = ReadOnlyGateRepository()
    report = await DeliveryOperationsGate(_config(), repository=repository).run(
        mode="full",
        operator_review_passed=True,
    )

    assert report.gate_status == "fail"
    assert repository.load_count == 1
    assert repository.event_outbox_appends == 0
    assert repository.replay_request_writes == 0
    assert repository.job_attempt_writes == 0
    assert repository.notification_plan_mutations == 0
    assert repository.notification_delivery_record_mutations == 0
    assert "delivery_gate_open_dlq_present" in report.blocking_reason_codes
    assert "delivery_gate_replay_guard_rejects_present" in report.blocking_reason_codes
