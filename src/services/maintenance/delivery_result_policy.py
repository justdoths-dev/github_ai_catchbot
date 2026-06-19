from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from ..outbox_relay.eligibility import (
    DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
    DELIVERY_RESULT_FAILED_TERMINAL_DLQ_RECEIPT_CODE,
    DELIVERY_RESULT_SUPERSEDED_NOOP_RECEIPT_CODE,
    DELIVERY_RESULT_SUPPRESSED_NOOP_RECEIPT_CODE,
    DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
)

from .models import DeliveryResultEvent, LatestDeliveryRecord, NotificationPlanRecord


DeliveryResultOutcome = Literal[
    "identity_invalid",
    "terminal_success",
    "suppressed_noop",
    "superseded_noop",
    "failed_retryable",
    "failed_terminal",
]

KNOWN_DELIVERY_STATUSES = frozenset(
    {
        "sent",
        "edited",
        "suppressed",
        "failed_retryable",
        "failed_terminal",
    }
)

SUPPRESSED_REASON_CODES = frozenset(
    {
        "dry_run_skip_transport",
        "notification_send_flag_disabled",
        "same_material_already_delivered",
        "telegram_edit_not_modified_noop",
        "digest_runtime_disabled",
    }
)


@dataclass(frozen=True, slots=True)
class DeliveryResultDecision:
    outcome: DeliveryResultOutcome
    is_current: bool
    should_write_dlq: bool
    should_emit_retry_intent: bool
    is_explicit_replay_candidate: bool
    receipt_code: str | None
    reason_code: str


def decide_delivery_result(
    *,
    event: DeliveryResultEvent,
    exact_record: LatestDeliveryRecord,
    latest_record: LatestDeliveryRecord,
    plan: NotificationPlanRecord,
    later_success_exists: bool,
    now: datetime,
    retry_max_attempts: int,
) -> DeliveryResultDecision:
    del event, now, retry_max_attempts

    delivery_status = exact_record.delivery_status
    if delivery_status not in KNOWN_DELIVERY_STATUSES:
        return DeliveryResultDecision(
            outcome="identity_invalid",
            is_current=False,
            should_write_dlq=False,
            should_emit_retry_intent=False,
            is_explicit_replay_candidate=False,
            receipt_code=None,
            reason_code="delivery_status_unknown",
        )

    is_current = exact_record.notification_delivery_record_id == latest_record.notification_delivery_record_id
    if not is_current or later_success_exists:
        return DeliveryResultDecision(
            outcome="superseded_noop",
            is_current=False,
            should_write_dlq=False,
            should_emit_retry_intent=False,
            is_explicit_replay_candidate=False,
            receipt_code=DELIVERY_RESULT_SUPERSEDED_NOOP_RECEIPT_CODE,
            reason_code="delivery_result_superseded_by_later_delivery",
        )

    if delivery_status in {"sent", "edited"}:
        return DeliveryResultDecision(
            outcome="terminal_success",
            is_current=True,
            should_write_dlq=False,
            should_emit_retry_intent=False,
            is_explicit_replay_candidate=False,
            receipt_code=DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
            reason_code="delivery_result_terminal_success",
        )

    if delivery_status == "suppressed":
        reason_code = _suppressed_reason_code(plan=plan, exact_record=exact_record)
        return DeliveryResultDecision(
            outcome="suppressed_noop",
            is_current=True,
            should_write_dlq=False,
            should_emit_retry_intent=False,
            is_explicit_replay_candidate=reason_code == "notification_send_flag_disabled",
            receipt_code=DELIVERY_RESULT_SUPPRESSED_NOOP_RECEIPT_CODE,
            reason_code=reason_code,
        )

    if delivery_status == "failed_terminal":
        return DeliveryResultDecision(
            outcome="failed_terminal",
            is_current=True,
            should_write_dlq=True,
            should_emit_retry_intent=False,
            is_explicit_replay_candidate=True,
            receipt_code=DELIVERY_RESULT_FAILED_TERMINAL_DLQ_RECEIPT_CODE,
            reason_code="delivery_result_failed_terminal",
        )

    return DeliveryResultDecision(
        outcome="failed_retryable",
        is_current=True,
        should_write_dlq=False,
        should_emit_retry_intent=False,
        is_explicit_replay_candidate=False,
        receipt_code=DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
        reason_code="failed_retryable_deferred_to_due_scan",
    )


def _suppressed_reason_code(*, plan: NotificationPlanRecord, exact_record: LatestDeliveryRecord) -> str:
    candidates = [
        exact_record.transport_error_code,
        plan.suppress_reason_code,
    ]
    if isinstance(exact_record.telegram_response_json, dict):
        candidates.extend(
            [
                exact_record.telegram_response_json.get("reason_code"),
                exact_record.telegram_response_json.get("suppress_reason_code"),
                exact_record.telegram_response_json.get("delivery_reason"),
            ]
        )
    for candidate in candidates:
        if isinstance(candidate, str) and candidate in SUPPRESSED_REASON_CODES:
            return candidate
    return "suppressed_other_known_reason"
