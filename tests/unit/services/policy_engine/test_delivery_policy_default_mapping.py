from __future__ import annotations

from services.policy_engine.delivery_policy import DeliveryPolicy


def test_inspect_now_maps_to_send_now_high() -> None:
    decision = DeliveryPolicy().evaluate(verdict="inspect_now")

    assert decision.delivery_decision == "send_now"
    assert decision.urgency_profile == "high"
    assert decision.suppress_reason_code is None


def test_later_maps_to_send_now_normal_silent_by_default() -> None:
    decision = DeliveryPolicy().evaluate(verdict="later")

    assert decision.delivery_decision == "send_now"
    assert decision.urgency_profile == "normal_silent"
    assert decision.suppress_reason_code is None


def test_later_maps_to_suppress_when_later_delivery_disabled() -> None:
    decision = DeliveryPolicy(enable_later_delivery=False).evaluate(verdict="later")

    assert decision.delivery_decision == "suppress"
    assert decision.urgency_profile == "suppressed"
    assert decision.suppress_reason_code == "later_delivery_disabled"


def test_skip_maps_to_suppress_suppressed() -> None:
    decision = DeliveryPolicy().evaluate(verdict="skip")

    assert decision.delivery_decision == "suppress"
    assert decision.urgency_profile == "suppressed"
    assert decision.suppress_reason_code == "policy_verdict_skip"
