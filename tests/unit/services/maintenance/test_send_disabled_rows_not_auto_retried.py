from __future__ import annotations

from datetime import datetime, timezone

from services.maintenance.delivery_retry import evaluate_retry_promotion
from services.maintenance.models import NotificationPlanRecord
from services.maintenance.retry_policy import classify_delivery_result_dry_run_noop

from .test_delivery_retry_promotion_policy import _plan


def test_send_disabled_suppressed_delivery_status_blocks_retry_intent() -> None:
    plan = _plan(status="suppressed", suppress_reason_code="notification_send_flag_disabled")

    decision = evaluate_retry_promotion(
        delivery_status="suppressed",
        plan=plan,
        latest_attempt_count=1,
        max_attempts=5,
        enabled=True,
        now=datetime.now(timezone.utc),
    )

    assert isinstance(plan, NotificationPlanRecord)
    assert decision.action == "noop"
    assert decision.reason_code == "delivery_status_not_retryable"
    assert decision.payload is None


def test_send_disabled_suppressed_delivery_result_is_explicit_replay_only() -> None:
    decision = classify_delivery_result_dry_run_noop(
        delivery_status="suppressed",
        delivery_reason="notification_send_flag_disabled",
    )

    assert decision.action == "block"
    assert decision.maintenance_classification == "suppressed_not_auto_retryable"
    assert decision.retry_intent_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
