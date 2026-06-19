from __future__ import annotations

import pytest

from services.maintenance.delivery_operations_gate import DeliveryOperationsGate
from tests.unit.services.maintenance.test_delivery_gate_runner import _config
from tests.unit.services.maintenance.test_delivery_operations_gate import FakeGateRepository, _snapshot


@pytest.mark.asyncio
async def test_delivery_gate_snapshot_generates_restricted_report() -> None:
    repository = FakeGateRepository(_snapshot(open_delivery_dlq_count=1))
    report = await DeliveryOperationsGate(_config(), repository=repository).run(mode="restricted")

    assert repository.load_count == 1
    assert report.mode == "restricted"
    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_open_dlq_present"]
    assert [metric.metric_name for metric in report.metrics] == [
        "enable_notification_send",
        "notifier_telegram_dry_run",
        "maintenance_retry_promotion",
        "success_rate_1h",
        "high_source_to_delivery_p95_sec",
        "due_retry_oldest_lag_sec",
        "open_delivery_dlq_count",
        "unexpected_send_disabled_count",
    ]


@pytest.mark.asyncio
async def test_delivery_gate_snapshot_generates_full_report_with_warn_only_review() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(_snapshot(duplicate_noop_ratio_1h=0.25)),
    ).run(mode="full", operator_review_passed=True)

    assert report.mode == "full"
    assert report.gate_status == "warn"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == ["delivery_gate_duplicate_noop_ratio_review_required"]
    assert report.metrics[-1].metric_name == "duplicate_noop_ratio_1h"
    assert report.metrics[-1].severity == "warn"
    assert report.metrics[-1].passed is False
