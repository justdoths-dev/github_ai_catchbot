from __future__ import annotations

from .models import DeliveryDecisionResult, Verdict


class DeliveryPolicy:
    def __init__(
        self,
        *,
        enable_later_delivery: bool = True,
        enable_silent_later: bool = True,
    ) -> None:
        self._enable_later_delivery = enable_later_delivery
        self._enable_silent_later = enable_silent_later

    def evaluate(self, *, verdict: Verdict) -> DeliveryDecisionResult:
        if verdict == "inspect_now":
            return DeliveryDecisionResult(delivery_decision="send_now", urgency_profile="high")

        if verdict == "later":
            if not self._enable_later_delivery:
                return DeliveryDecisionResult(
                    delivery_decision="suppress",
                    urgency_profile="suppressed",
                    suppress_reason_code="later_delivery_disabled",
                )
            return DeliveryDecisionResult(
                delivery_decision="send_now",
                urgency_profile="normal_silent",
            )

        return DeliveryDecisionResult(
            delivery_decision="suppress",
            urgency_profile="suppressed",
            suppress_reason_code="policy_verdict_skip",
        )
