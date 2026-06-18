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
    assert repository.plan_created_outbox[0]["event_type"] == "notification.plan.created.v1"
    assert repository.plan_created_outbox[0]["aggregate_type"] == "analysis"
    assert repository.plan_created_outbox[0]["aggregate_id"] == notification_plan.analysis_id
    assert repository.plan_created_outbox[0]["dedupe_key"] == (
        f"notify:retry-intent:{notification_plan.notification_plan_id}:1:{int(notification_plan.send_after.timestamp())}"
    )
    assert repository.plan_created_outbox[0]["payload_json"]["notification_plan_id"] == (
        str(notification_plan.notification_plan_id)
    )
    assert repository.plan_created_outbox[0]["payload_json"]["analysis_id"] == str(notification_plan.analysis_id)
    assert repository.plan_created_outbox[0]["payload_json"]["candidate_group_id"] == (
        str(notification_plan.candidate_group_id)
    )
    assert repository.plan_created_outbox[0]["payload_json"]["send_after"] is None
    assert repository.plan_created_outbox[0]["payload_json"]["retry_attempt"] == 2
    assert repository.plan_created_outbox[0]["payload_json"]["previous_attempt_count"] == 1
    assert repository.plan_created_outbox[0]["payload_json"]["retry_reason"] == "due_retry_promotion"
    assert repository.dead_letters == []


@pytest.mark.asyncio
async def test_due_retry_dedupe_key_is_stable_across_repeated_execution() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now - timedelta(seconds=1))
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=2,
    )
    service = MaintenanceService(config(max_attempts=5), repository=repository, now_fn=lambda: now)

    first = await service.promote_due_retries_once()
    second = await service.promote_due_retries_once()

    assert first == 1
    assert second == 0
    assert len(repository.plan_created_outbox) == 1
    assert repository.plan_created_outbox[0]["dedupe_key"] == (
        f"notify:retry-intent:{notification_plan.notification_plan_id}:2:{int(notification_plan.send_after.timestamp())}"
    )


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
    assert repository.dead_letters[0]["stage_name"] == "maintenance_delivery_retry"
    assert repository.dead_letters[0]["queue_name"] == "q.maintenance"
    assert repository.dead_letters[0]["root_object_type"] == "notification_plan"
    assert repository.dead_letters[0]["next_manual_action"] == "request_delivery_replay_after_operator_fix"
    assert repository.dead_letters[0]["replay_hint"] == "delivery_replay_from_notification_plan"


@pytest.mark.asyncio
async def test_missing_or_nonpositive_attempt_count_noops_without_retry_or_dlq() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now - timedelta(seconds=1))
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=0,
    )
    service = MaintenanceService(config(), repository=repository, now_fn=lambda: now)

    processed = await service.promote_due_retries_once()

    assert processed == 0
    assert repository.plan_created_outbox == []
    assert repository.dead_letters == []


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


@pytest.mark.asyncio
async def test_due_retry_promoter_does_not_mutate_notification_plans() -> None:
    now = datetime.now(timezone.utc)
    repository = FakeRepository()
    notification_plan = plan(send_after=now - timedelta(seconds=1))
    before = notification_plan
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    repository.latest_delivery_records[notification_plan.notification_plan_id] = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=1,
    )
    service = MaintenanceService(config(), repository=repository, now_fn=lambda: now)

    await service.promote_due_retries_once()

    assert repository.plans[notification_plan.notification_plan_id] == before
