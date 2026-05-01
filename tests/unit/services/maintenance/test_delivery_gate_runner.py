from __future__ import annotations

import pytest

from services.maintenance.config import MaintenanceConfig
from services.maintenance.delivery_gate_runner import DeliveryGateRunner
from services.maintenance.models import DeliveryGateSnapshot


class FakeGateRepository:
    def __init__(self, snapshot: DeliveryGateSnapshot) -> None:
        self.snapshot = snapshot
        self.load_count = 0

    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot:
        self.load_count += 1
        return self.snapshot


def _config(
    *,
    send_enabled: bool = True,
    dry_run: bool = False,
    retry_promotion_enabled: bool = True,
) -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        maintenance_consumer_name="test",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
        replay_consumer_name="test",
        batch_size=50,
        block_ms=100,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=send_enabled,
        notifier_telegram_dry_run=dry_run,
        enable_delivery_retry_promotion=retry_promotion_enabled,
        enable_replay_to_prod_db=False,
        delivery_gate_min_success_rate_1h=0.99,
        delivery_gate_min_success_rate_24h=0.99,
        delivery_gate_max_high_source_to_delivery_p95_sec=120,
        delivery_gate_max_plan_to_transport_p95_sec=120,
        delivery_gate_max_due_retry_lag_sec=120,
        delivery_gate_max_open_dlq_count=0,
        delivery_gate_max_send_disabled_count=0,
        delivery_gate_max_replay_guard_reject_count=0,
        delivery_gate_require_operator_review_for_full=True,
        log_level="INFO",
    )


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
async def test_restricted_gate_passes_on_healthy_snapshot() -> None:
    repository = FakeGateRepository(_snapshot())
    report = await DeliveryGateRunner(_config(), repository=repository).run(mode="restricted")

    assert report.gate_status == "pass"
    assert report.blocking_reason_codes == []
    assert report.operator_review_required is False
    assert report.recommended_flag_patch == {
        "ENABLE_NOTIFICATION_SEND": True,
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
        "NOTIFIER_TELEGRAM_DRY_RUN": False,
    }
    assert repository.load_count == 1


@pytest.mark.asyncio
async def test_restricted_gate_fails_on_open_dlq() -> None:
    report = await DeliveryGateRunner(
        _config(),
        repository=FakeGateRepository(_snapshot(open_delivery_dlq_count=1)),
    ).run(mode="restricted")

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == ["delivery_gate_open_dlq_present"]


@pytest.mark.asyncio
async def test_restricted_gate_fails_when_runtime_flags_are_not_ready() -> None:
    report = await DeliveryGateRunner(
        _config(send_enabled=False, dry_run=True, retry_promotion_enabled=False),
        repository=FakeGateRepository(_snapshot()),
    ).run(mode="restricted")

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes[:3] == [
        "delivery_gate_flag_send_disabled",
        "delivery_gate_dry_run_enabled",
        "delivery_gate_retry_promotion_disabled",
    ]
    assert report.recommended_flag_patch == {
        "ENABLE_NOTIFICATION_SEND": False,
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": False,
        "NOTIFIER_TELEGRAM_DRY_RUN": False,
    }


@pytest.mark.asyncio
async def test_full_gate_warns_when_only_operator_review_is_missing() -> None:
    report = await DeliveryGateRunner(_config(), repository=FakeGateRepository(_snapshot())).run(mode="full")

    assert report.gate_status == "warn"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == ["delivery_gate_operator_review_required"]
    assert report.operator_review_required is True
    assert report.operator_review_passed is None


@pytest.mark.asyncio
async def test_full_gate_fails_on_replay_guard_rejects_or_retry_ceiling_rows() -> None:
    report = await DeliveryGateRunner(
        _config(),
        repository=FakeGateRepository(
            _snapshot(replay_guard_reject_count_24h=1, retry_ceiling_exceeded_count_24h=1)
        ),
    ).run(mode="full", operator_review_passed=True)

    assert report.gate_status == "fail"
    assert report.blocking_reason_codes == [
        "delivery_gate_prod_replay_guard_rejects_present",
        "delivery_gate_retry_ceiling_exceeded_rows_present",
    ]


@pytest.mark.asyncio
async def test_recommended_flag_patch_is_output_only_and_deterministic() -> None:
    repository = FakeGateRepository(_snapshot())
    first = await DeliveryGateRunner(_config(), repository=repository).run(mode="restricted")
    second = await DeliveryGateRunner(_config(), repository=repository).run(mode="restricted")

    assert first.recommended_flag_patch == second.recommended_flag_patch
    assert list(first.recommended_flag_patch) == [
        "ENABLE_NOTIFICATION_SEND",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
        "NOTIFIER_TELEGRAM_DRY_RUN",
    ]
