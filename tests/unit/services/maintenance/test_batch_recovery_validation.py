from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.maintenance.batch_recovery_tool import DeliveryBatchRecoveryTool
from services.maintenance.config import MaintenanceConfig
from services.maintenance.models import SelectedPlanRecoveryRow


ROOT = Path(__file__).resolve().parents[4]


class FakeRecoveryRepository:
    def __init__(self, rows: list[SelectedPlanRecoveryRow]) -> None:
        self.rows = {row.notification_plan_id: row for row in rows}
        self.replay_inserts: list[UUID] = []
        self.retry_intents: list[dict] = []

    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]):
        return [self.rows[plan_id] for plan_id in notification_plan_ids if plan_id in self.rows]

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[UUID], requested_by: str) -> int:
        del requested_by
        self.replay_inserts.extend(plan_ids)
        return len(plan_ids)

    async def insert_manual_retry_intent_outbox(self, **kwargs) -> bool:
        self.retry_intents.append(kwargs)
        return True


def _config() -> MaintenanceConfig:
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
        enable_notification_send=True,
        notifier_telegram_dry_run=False,
        enable_delivery_retry_promotion=True,
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


def _row(
    *,
    delivery_status: str | None = "failed_retryable",
    send_after=None,
    attempt_count: int | None = 1,
    send_disabled: bool = False,
    has_open_replay_request: bool = False,
    has_delivery_dlq: bool = False,
    delivery_dlq_next_manual_action: str | None = None,
    delivery_dlq_replay_hint: str | None = None,
) -> SelectedPlanRecoveryRow:
    now = datetime.now(timezone.utc)
    return SelectedPlanRecoveryRow(
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        plan_status="failed_retryable",
        delivery_status=delivery_status,
        attempt_count=attempt_count,
        send_after=send_after if send_after is not None else now - timedelta(minutes=1),
        telegram_chat_id=12345,
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject",
        material_change_hash="material",
        urgency_profile="high",
        delivery_decision="send_now",
        send_disabled=send_disabled,
        has_open_replay_request=has_open_replay_request,
        has_delivery_dlq=has_delivery_dlq,
        delivery_dlq_next_manual_action=delivery_dlq_next_manual_action,
        delivery_dlq_replay_hint=delivery_dlq_replay_hint,
    )


@pytest.mark.asyncio
async def test_replay_selected_accepts_send_disabled_suppress_failed_terminal_and_dlq_rows() -> None:
    rows = [
        _row(delivery_status="suppressed", send_disabled=True),
        _row(delivery_status="failed_terminal"),
        _row(
            delivery_status="failed_retryable",
            has_delivery_dlq=True,
            delivery_dlq_next_manual_action="request_explicit_delivery_replay",
        ),
    ]
    repository = FakeRecoveryRepository(rows)
    result = await DeliveryBatchRecoveryTool(_config(), repository=repository).replay_selected(
        plan_ids=[row.notification_plan_id for row in rows],
        requested_by="ops",
    )

    assert result.accepted_count == 3
    assert result.emitted_count == 3
    assert repository.replay_inserts == [row.notification_plan_id for row in rows]


@pytest.mark.asyncio
async def test_replay_selected_rejects_failed_retryable_due_rows() -> None:
    row = _row(delivery_status="failed_retryable")
    repository = FakeRecoveryRepository([row])
    result = await DeliveryBatchRecoveryTool(_config(), repository=repository).replay_selected(
        plan_ids=[row.notification_plan_id],
        requested_by="ops",
    )

    assert result.accepted_count == 0
    assert result.skipped_reason_codes == {"batch_recovery_not_replay_candidate": 1}
    assert repository.replay_inserts == []


@pytest.mark.asyncio
async def test_replay_selected_skips_open_replay_requests() -> None:
    row = _row(delivery_status="failed_terminal", has_open_replay_request=True)
    result = await DeliveryBatchRecoveryTool(_config(), repository=FakeRecoveryRepository([row])).replay_selected(
        plan_ids=[row.notification_plan_id],
        requested_by="ops",
    )

    assert result.accepted_count == 0
    assert result.skipped_reason_codes == {"batch_recovery_open_replay_exists": 1}


@pytest.mark.asyncio
async def test_retry_selected_due_accepts_only_due_retryable_below_retry_ceiling() -> None:
    now = datetime.now(timezone.utc)
    accepted = _row(delivery_status="failed_retryable", send_after=now - timedelta(seconds=1), attempt_count=2)
    terminal = _row(delivery_status="failed_terminal", send_after=now - timedelta(seconds=1), attempt_count=1)
    not_due = _row(delivery_status="failed_retryable", send_after=now + timedelta(seconds=30), attempt_count=1)
    ceiling = _row(delivery_status="failed_retryable", send_after=now - timedelta(seconds=1), attempt_count=3)
    disabled = _row(delivery_status="failed_retryable", send_after=now - timedelta(seconds=1), send_disabled=True)
    rows = [accepted, terminal, not_due, ceiling, disabled]
    repository = FakeRecoveryRepository(rows)

    result = await DeliveryBatchRecoveryTool(
        _config(),
        repository=repository,
        now_fn=lambda: now,
    ).retry_selected_due(plan_ids=[row.notification_plan_id for row in rows], requested_by="ops")

    assert result.accepted_count == 1
    assert result.emitted_count == 1
    assert result.skipped_reason_codes == {
        "status_is_not_failed_retryable": 1,
        "send_after_not_due_yet": 1,
        "retry_ceiling_exceeded": 1,
        "send_disabled_rows_require_replay": 1,
    }
    assert repository.retry_intents[0]["row"].notification_plan_id == accepted.notification_plan_id


def test_stage43_control_plane_does_not_mutate_notification_plans() -> None:
    for path in [
        ROOT / "src" / "services" / "maintenance" / "batch_recovery_tool.py",
        ROOT / "src" / "services" / "maintenance" / "delivery_gate_runner.py",
        ROOT / "src" / "services" / "maintenance" / "repositories.py",
    ]:
        text = path.read_text(encoding="utf-8").lower()
        assert "update notification_plans" not in text
        assert "delete from notification_plans" not in text
        assert "insert into notification_plans" not in text
