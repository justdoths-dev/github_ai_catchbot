from __future__ import annotations

from services.maintenance.retry_policy import (
    DELIVERY_RESULT_NOOP_CLASSIFICATION,
    DELIVERY_RESULT_NOOP_ERROR_CODE,
    classify_delivery_result_dry_run_noop,
)


def test_suppressed_dry_run_skip_transport_is_logical_noop_success() -> None:
    decision = classify_delivery_result_dry_run_noop(
        delivery_status="suppressed",
        delivery_reason="dry_run_skip_transport",
    )

    assert decision.action == "mark_logical_noop_success"
    assert decision.maintenance_classification == DELIVERY_RESULT_NOOP_CLASSIFICATION
    assert decision.reason_code == DELIVERY_RESULT_NOOP_ERROR_CODE
    assert decision.auto_retry_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
    assert decision.retry_intent_allowed is False


def test_suppressed_notification_send_disabled_is_not_auto_retryable_but_not_this_target() -> None:
    decision = classify_delivery_result_dry_run_noop(
        delivery_status="suppressed",
        delivery_reason="notification_send_flag_disabled",
    )

    assert decision.action == "block"
    assert decision.maintenance_classification == "suppressed_not_auto_retryable"
    assert decision.auto_retry_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
    assert decision.retry_intent_allowed is False


def test_failed_retryable_is_future_retry_candidate_not_dry_run_noop_success() -> None:
    decision = classify_delivery_result_dry_run_noop(
        delivery_status="failed_retryable",
        delivery_reason="telegram_retryable_5xx",
    )

    assert decision.action == "block"
    assert decision.maintenance_classification == "out_of_scope"
    assert decision.reason_code == "failed_retryable_requires_due_retry_path"
    assert decision.future_auto_retry_candidate is True
    assert decision.auto_retry_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
    assert decision.retry_intent_allowed is False


def test_wrong_suppressed_reason_is_blocked_without_recovery_side_effects() -> None:
    decision = classify_delivery_result_dry_run_noop(
        delivery_status="suppressed",
        delivery_reason="unexpected_reason",
    )

    assert decision.action == "block"
    assert decision.maintenance_classification == "out_of_scope"
    assert decision.auto_retry_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
    assert decision.retry_intent_allowed is False
