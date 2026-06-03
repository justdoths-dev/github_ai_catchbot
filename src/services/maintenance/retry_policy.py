from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


DELIVERY_RESULT_NOOP_STAGE_NAME = "maintenance_delivery_result"
DELIVERY_RESULT_NOOP_ERROR_CODE = "delivery_result_suppressed_dry_run_noop"
DELIVERY_RESULT_NOOP_CLASSIFICATION = "logical_noop_success"

DeliveryResultNoopAction = Literal["mark_logical_noop_success", "block"]


@dataclass(slots=True, frozen=True)
class DeliveryResultDryRunNoopDecision:
    action: DeliveryResultNoopAction
    maintenance_classification: str
    reason_code: str
    auto_retry_allowed: bool
    dead_letter_allowed: bool
    replay_dispatch_allowed: bool
    retry_intent_allowed: bool
    future_auto_retry_candidate: bool = False


def classify_delivery_result_dry_run_noop(
    *,
    delivery_status: str,
    delivery_reason: str | None,
) -> DeliveryResultDryRunNoopDecision:
    if delivery_status == "suppressed" and delivery_reason == "dry_run_skip_transport":
        return DeliveryResultDryRunNoopDecision(
            action="mark_logical_noop_success",
            maintenance_classification=DELIVERY_RESULT_NOOP_CLASSIFICATION,
            reason_code=DELIVERY_RESULT_NOOP_ERROR_CODE,
            auto_retry_allowed=False,
            dead_letter_allowed=False,
            replay_dispatch_allowed=False,
            retry_intent_allowed=False,
        )
    if delivery_status == "failed_retryable":
        return DeliveryResultDryRunNoopDecision(
            action="block",
            maintenance_classification="out_of_scope",
            reason_code="failed_retryable_requires_due_retry_path",
            auto_retry_allowed=False,
            dead_letter_allowed=False,
            replay_dispatch_allowed=False,
            retry_intent_allowed=False,
            future_auto_retry_candidate=True,
        )
    if delivery_status == "suppressed" and delivery_reason == "notification_send_flag_disabled":
        return DeliveryResultDryRunNoopDecision(
            action="block",
            maintenance_classification="suppressed_not_auto_retryable",
            reason_code="notification_send_flag_disabled_not_dry_run_noop_target",
            auto_retry_allowed=False,
            dead_letter_allowed=False,
            replay_dispatch_allowed=False,
            retry_intent_allowed=False,
        )
    return DeliveryResultDryRunNoopDecision(
        action="block",
        maintenance_classification="out_of_scope",
        reason_code="delivery_result_not_dry_run_noop_target",
        auto_retry_allowed=False,
        dead_letter_allowed=False,
        replay_dispatch_allowed=False,
        retry_intent_allowed=False,
    )
