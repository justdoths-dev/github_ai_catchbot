from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.maintenance.delivery_operations_gate import DeliveryOperationsGate
from services.maintenance.delivery_operations_gate import evaluate_delivery_operations_gate
from services.maintenance.models import DeliveryGateSnapshot
from services.maintenance.repositories import MaintenanceRepository
from tests.unit.services.maintenance.test_delivery_gate_runner import _config


REFERENCE_NOW = datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc)
STALE_SENT_OUTSIDE_RECENT_WINDOW = {
    "delivery_status": "sent",
    "posted_at": datetime(2026, 5, 23, 0, 0, tzinfo=timezone.utc),
    "sent_at": datetime(2026, 6, 4, 9, 4, 28, 780000, tzinfo=timezone.utc),
    "created_at": REFERENCE_NOW - timedelta(days=3),
    "source_to_delivery_sec": 983068.78,
}
RECENT_HIGH_LATENCY_SENT = {
    "delivery_status": "sent",
    "posted_at": REFERENCE_NOW - timedelta(seconds=130),
    "sent_at": REFERENCE_NOW,
    "created_at": REFERENCE_NOW - timedelta(minutes=5),
    "source_to_delivery_sec": 130.0,
}
RECENT_ACCEPTABLE_LATENCY_SENT = {
    "delivery_status": "sent",
    "posted_at": REFERENCE_NOW - timedelta(seconds=60),
    "sent_at": REFERENCE_NOW,
    "created_at": REFERENCE_NOW - timedelta(minutes=5),
    "source_to_delivery_sec": 60.0,
}


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
async def test_restricted_gate_fails_only_on_success_rate_when_cold_start_metrics_are_missing() -> None:
    report = await DeliveryOperationsGate(
        _config(),
        repository=FakeGateRepository(_snapshot(success_rate_1h=None, high_source_to_delivery_p95_sec=None)),
    ).run(mode="restricted")

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_success_rate_missing"]
    high_metric = next(metric for metric in report.metrics if metric.metric_name == "high_source_to_delivery_p95_sec")
    assert high_metric.observed_value is None
    assert high_metric.passed is True


@pytest.mark.asyncio
async def test_stale_sent_row_outside_recent_window_does_not_add_high_e2e_blocker() -> None:
    assert STALE_SENT_OUTSIDE_RECENT_WINDOW["source_to_delivery_sec"] > (
        _config().delivery_gate_max_high_source_to_delivery_p95_sec
    )
    session = FakeSnapshotSession(
        _snapshot_row(success_rate_1h=None, high_source_to_delivery_p95_sec=None)
    )
    snapshot = await MaintenanceRepository(session).load_delivery_gate_snapshot()
    high_source_sql = _cte_fragment(
        session.executed_sql[0],
        start="high_source_p95 AS",
        end="plan_transport_p95 AS",
    )

    assert "ndr.created_at >= now() - interval '1 hour'" in " ".join(high_source_sql.split())
    report = evaluate_delivery_operations_gate(config=_config(), snapshot=snapshot, mode="restricted")

    assert snapshot.success_rate_1h is None
    assert snapshot.high_source_to_delivery_p95_sec is None
    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_success_rate_missing"]


def test_recent_high_latency_sent_row_inside_window_still_blocks_gate() -> None:
    assert RECENT_HIGH_LATENCY_SENT["created_at"] >= REFERENCE_NOW - timedelta(hours=1)
    report = evaluate_delivery_operations_gate(
        config=_config(),
        snapshot=_snapshot(
            success_rate_1h=1.0,
            high_source_to_delivery_p95_sec=RECENT_HIGH_LATENCY_SENT["source_to_delivery_sec"],
        ),
        mode="restricted",
    )

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_high_e2e_p95_too_high"]


def test_recent_acceptable_latency_sent_row_inside_window_passes_high_e2e_metric() -> None:
    assert RECENT_ACCEPTABLE_LATENCY_SENT["created_at"] >= REFERENCE_NOW - timedelta(hours=1)
    report = evaluate_delivery_operations_gate(
        config=_config(),
        snapshot=_snapshot(
            success_rate_1h=1.0,
            high_source_to_delivery_p95_sec=RECENT_ACCEPTABLE_LATENCY_SENT["source_to_delivery_sec"],
        ),
        mode="restricted",
    )

    high_metric = next(metric for metric in report.metrics if metric.metric_name == "high_source_to_delivery_p95_sec")
    assert report.gate_status == "pass"
    assert report.blocking_reason_codes == []
    assert high_metric.passed is True


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


class FakeSnapshotSession:
    def __init__(self, row: dict[str, object]) -> None:
        self.row = row
        self.executed_sql: list[str] = []

    async def execute(self, statement, params=None):
        del params
        self.executed_sql.append(str(statement))
        return FakeSnapshotResult(self.row)


class FakeSnapshotResult:
    def __init__(self, row: dict[str, object]) -> None:
        self.row = row

    def mappings(self):
        return self

    def one(self):
        return self.row


def _snapshot_row(**overrides) -> dict[str, object]:
    row = {
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
    row.update(overrides)
    return row


def _cte_fragment(sql: str, *, start: str, end: str) -> str:
    lower_sql = sql.lower()
    start_index = lower_sql.index(start.lower())
    end_index = lower_sql.index(end.lower(), start_index)
    return sql[start_index:end_index]
