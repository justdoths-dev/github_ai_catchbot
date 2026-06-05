from __future__ import annotations

import pytest

from services.maintenance.delivery_operations_gate import DeliveryOperationsGate
from services.maintenance.models import DeliveryGateSnapshot
from tests.unit.services.maintenance.test_delivery_gate_runner import _config


class FakeGateRepository:
    def __init__(self, snapshot: DeliveryGateSnapshot) -> None:
        self.snapshot = snapshot
        self.load_count = 0

    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot:
        self.load_count += 1
        return self.snapshot


def _snapshot(**overrides) -> DeliveryGateSnapshot:
    values = {
        "success_rate_1h": 1.0,
        "success_rate_24h": 1.0,
        "high_source_to_delivery_p95_sec": 10.0,
        "plan_to_transport_p95_sec": 8.0,
        "due_retry_oldest_lag_sec": None,
        "open_delivery_dlq_count": 0,
        "oldest_delivery_dlq_age_sec": None,
        "unexpected_send_disabled_count": 0,
        "replay_guard_reject_count_24h": 0,
        "retry_ceiling_exceeded_count_24h": 0,
        "duplicate_noop_ratio_1h": 0.0,
    }
    values.update(overrides)
    return DeliveryGateSnapshot(**values)


@pytest.mark.asyncio
async def test_restricted_gate_passes_minimal_healthy_snapshot() -> None:
    repository = FakeGateRepository(_snapshot())
    report = await DeliveryOperationsGate(_config(), repository=repository).run(mode="restricted")

    assert report.mode == "restricted"
    assert report.gate_status == "pass"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == []
    assert [metric.metric_name for metric in report.metrics] == [
        "success_rate_1h",
        "high_source_to_delivery_p95_sec",
        "due_retry_oldest_lag_sec",
        "open_delivery_dlq_count",
        "unexpected_send_disabled_count",
    ]
    assert report.recommended_flag_patch == {
        "ENABLE_NOTIFICATION_SEND": True,
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
        "NOTIFIER_TELEGRAM_DRY_RUN": False,
    }
    assert repository.load_count == 1


@pytest.mark.asyncio
async def test_restricted_gate_fails_on_open_dlq() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(_snapshot(open_delivery_dlq_count=1)),
    ).run(mode="restricted")

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_open_dlq_present"]
    assert report.recommended_flag_patch == {
        "ENABLE_NOTIFICATION_SEND": False,
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": False,
        "NOTIFIER_TELEGRAM_DRY_RUN": False,
    }


@pytest.mark.asyncio
async def test_restricted_gate_fails_on_unexpected_send_disabled_rows() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(_snapshot(unexpected_send_disabled_count=1)),
    ).run(mode="restricted")

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_unexpected_send_disabled_rows_present"]


@pytest.mark.asyncio
async def test_restricted_gate_fails_when_success_rate_or_high_latency_is_missing() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(_snapshot(success_rate_1h=None, high_source_to_delivery_p95_sec=None)),
    ).run(mode="restricted")

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == [
        "delivery_gate_success_rate_missing",
        "delivery_gate_high_e2e_p95_too_high",
    ]


@pytest.mark.asyncio
async def test_full_gate_warns_when_operator_review_is_missing() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(_snapshot()),
    ).run(mode="full")

    assert report.gate_status == "warn"
    assert report.warning_reason_codes == ["delivery_gate_operator_review_required"]
    assert report.blocking_reason_codes == []
    assert report.operator_review_required is True
    assert report.operator_review_passed is None


@pytest.mark.asyncio
async def test_full_gate_fails_on_replay_guard_rejects() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(_snapshot(replay_guard_reject_count_24h=1)),
    ).run(mode="full", operator_review_passed=True)

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_prod_replay_guard_rejects_present"]


@pytest.mark.asyncio
async def test_full_gate_uses_stable_metric_and_reason_order() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(
            _snapshot(
                success_rate_1h=0.1,
                high_source_to_delivery_p95_sec=130.0,
                due_retry_oldest_lag_sec=130.0,
                open_delivery_dlq_count=1,
                oldest_delivery_dlq_age_sec=3600.0,
                unexpected_send_disabled_count=1,
                success_rate_24h=0.2,
                replay_guard_reject_count_24h=1,
                retry_ceiling_exceeded_count_24h=1,
                duplicate_noop_ratio_1h=0.5,
            )
        ),
    ).run(mode="full")

    assert [metric.metric_name for metric in report.metrics] == [
        "success_rate_1h",
        "high_source_to_delivery_p95_sec",
        "due_retry_oldest_lag_sec",
        "open_delivery_dlq_count",
        "unexpected_send_disabled_count",
        "success_rate_24h",
        "replay_guard_reject_count_24h",
        "retry_ceiling_exceeded_count_24h",
        "oldest_delivery_dlq_age_sec",
        "duplicate_noop_ratio_1h",
    ]
    assert report.blocking_reason_codes == [
        "delivery_gate_success_rate_below_threshold",
        "delivery_gate_high_e2e_p95_too_high",
        "delivery_gate_due_retry_lag_too_high",
        "delivery_gate_open_dlq_present",
        "delivery_gate_unexpected_send_disabled_rows_present",
        "delivery_gate_24h_success_rate_below_threshold",
        "delivery_gate_prod_replay_guard_rejects_present",
        "delivery_gate_retry_ceiling_exceeded_rows_present",
        "delivery_gate_delivery_dlq_oldest_age_too_high",
    ]
    assert report.warning_reason_codes == [
        "delivery_gate_duplicate_noop_ratio_review_required",
        "delivery_gate_operator_review_required",
    ]
