from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.maintenance.service import MaintenanceService

from tests.component.services.maintenance._fakes import FakeRepository, config, latest_delivery_record, plan


@pytest.mark.asyncio
async def test_due_failed_retryable_enabled_below_ceiling_emits_retry_intent() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now - timedelta(seconds=1))
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=1,
    )
    service = MaintenanceService(config(), repository=repository, now_fn=lambda: now)

    processed = await service.promote_due_retries_once()

    assert processed == 1
    assert len(repository.plan_created_outbox) == 1
    assert repository.plan_created_outbox[0]["payload_json"]["retry_attempt"] == 2
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_future_send_after_noops() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now + timedelta(minutes=5))
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=1,
    )
    service = MaintenanceService(config(), repository=repository, now_fn=lambda: now)

    processed = await service.promote_due_retries_once()

    assert processed == 0
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_retry_promotion_disabled_noops_without_loading_candidates() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now - timedelta(seconds=1))
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    service = MaintenanceService(config(enable_retry=False), repository=repository, now_fn=lambda: now)

    processed = await service.promote_due_retries_once()

    assert processed == 0
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_ceiling_reached_dead_letters_without_retry_intent() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now - timedelta(seconds=1))
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=3,
    )
    service = MaintenanceService(config(max_attempts=3), repository=repository, now_fn=lambda: now)

    processed = await service.promote_due_retries_once()

    assert processed == 1
    assert repository.plan_created_outbox == []
    assert len(repository.dead_letters) == 1
    assert repository.dead_letters[0]["last_error_code"] == "max_notification_retry_attempts_exceeded"


@pytest.mark.asyncio
async def test_suppressed_send_disabled_and_dry_run_rows_are_not_selected_or_processed() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    send_disabled = plan(
        status="suppressed",
        send_after=now - timedelta(seconds=1),
        suppress_reason_code="notification_send_flag_disabled",
    )
    dry_run = plan(
        status="suppressed",
        send_after=now - timedelta(seconds=1),
        suppress_reason_code="dry_run_skip_transport",
    )
    repository.plans[send_disabled.notification_plan_id] = send_disabled
    repository.plans[dry_run.notification_plan_id] = dry_run
    service = MaintenanceService(config(), repository=repository, now_fn=lambda: now)

    processed = await service.promote_due_retries_once()

    assert processed == 0
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []
