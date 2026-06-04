from __future__ import annotations

from services.maintenance.retry_policy import (
    DELIVERY_RESULT_SENT_SUCCESS_CLASSIFICATION,
    DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
    classify_delivery_result_sent_success,
)


def test_sent_delivery_result_is_terminal_success_without_retry_replay_or_dlq() -> None:
    decision = classify_delivery_result_sent_success(delivery_status="sent")

    assert decision.action == "mark_terminal_success"
    assert decision.maintenance_classification == DELIVERY_RESULT_SENT_SUCCESS_CLASSIFICATION
    assert decision.reason_code == DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE
    assert decision.auto_retry_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
    assert decision.retry_intent_allowed is False


def test_failed_retryable_is_future_retry_candidate_not_sent_success() -> None:
    decision = classify_delivery_result_sent_success(delivery_status="failed_retryable")

    assert decision.action == "block"
    assert decision.maintenance_classification == "out_of_scope"
    assert decision.reason_code == "failed_retryable_requires_due_retry_path"
    assert decision.future_auto_retry_candidate is True
    assert decision.auto_retry_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
    assert decision.retry_intent_allowed is False


def test_suppressed_delivery_result_is_not_sent_success_target() -> None:
    decision = classify_delivery_result_sent_success(delivery_status="suppressed")

    assert decision.action == "block"
    assert decision.maintenance_classification == "out_of_scope"
    assert decision.reason_code == "delivery_result_not_sent_success_target"
    assert decision.auto_retry_allowed is False
    assert decision.dead_letter_allowed is False
    assert decision.replay_dispatch_allowed is False
    assert decision.retry_intent_allowed is False
