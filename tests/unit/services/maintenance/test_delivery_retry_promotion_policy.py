from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from services.maintenance.delivery_retry import evaluate_retry_promotion
from services.maintenance.models import NotificationPlanRecord


def _plan(*, status: str = "failed_retryable", send_after=None, suppress_reason_code: str | None = None):
    now = datetime.now(timezone.utc)
    return NotificationPlanRecord(
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject",
        material_change_hash="material",
        send_after=send_after if send_after is not None else now - timedelta(minutes=1),
        suppress_reason_code=suppress_reason_code,
        status=status,
    )


def test_failed_retryable_due_enabled_below_ceiling_emits_retry_intent() -> None:
    plan = _plan()

    decision = evaluate_retry_promotion(
        delivery_status="failed_retryable",
        plan=plan,
        latest_attempt_count=2,
        max_attempts=3,
        enabled=True,
        now=datetime.now(timezone.utc),
    )

    assert decision.action == "emit_retry_intent"
    assert decision.retry_attempt == 3
    assert decision.payload is not None
    assert decision.payload["notification_plan_id"] == str(plan.notification_plan_id)
    assert decision.payload["retry_reason"] == "due_retry_promotion"


def test_failed_retryable_future_send_after_noops() -> None:
    decision = evaluate_retry_promotion(
        delivery_status="failed_retryable",
        plan=_plan(send_after=datetime.now(timezone.utc) + timedelta(minutes=5)),
        latest_attempt_count=1,
        max_attempts=3,
        enabled=True,
        now=datetime.now(timezone.utc),
    )

    assert decision.action == "noop"
    assert decision.reason_code == "notification_plan_not_due"


def test_retry_promotion_disabled_noops() -> None:
    decision = evaluate_retry_promotion(
        delivery_status="failed_retryable",
        plan=_plan(),
        latest_attempt_count=1,
        max_attempts=3,
        enabled=False,
        now=datetime.now(timezone.utc),
    )

    assert decision.action == "noop"
    assert decision.reason_code == "retry_promotion_disabled"


def test_suppressed_send_disabled_is_not_auto_retried() -> None:
    decision = evaluate_retry_promotion(
        delivery_status="suppressed",
        plan=_plan(status="suppressed", suppress_reason_code="notification_send_flag_disabled"),
        latest_attempt_count=0,
        max_attempts=3,
        enabled=True,
        now=datetime.now(timezone.utc),
    )

    assert decision.action == "noop"
    assert decision.reason_code == "delivery_status_not_retryable"


def test_terminal_failure_is_not_auto_retried() -> None:
    decision = evaluate_retry_promotion(
        delivery_status="failed_terminal",
        plan=_plan(status="failed_terminal"),
        latest_attempt_count=1,
        max_attempts=3,
        enabled=True,
        now=datetime.now(timezone.utc),
    )

    assert decision.action == "noop"
    assert decision.reason_code == "delivery_status_not_retryable"


def test_retry_ceiling_reached_uses_dead_letter_path() -> None:
    decision = evaluate_retry_promotion(
        delivery_status="failed_retryable",
        plan=_plan(),
        latest_attempt_count=3,
        max_attempts=3,
        enabled=True,
        now=datetime.now(timezone.utc),
    )

    assert decision.action == "dead_letter_retry_ceiling"
    assert decision.reason_code == "max_notification_retry_attempts_exceeded"
