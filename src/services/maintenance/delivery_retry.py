from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from .models import DeliveryRetryDecision, NotificationPlanRecord


RETRY_INTENT_EVENT_TYPE = "notification.plan.created.v1"
DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
MAINTENANCE_QUEUE_NAME = "q.maintenance"

REQUIRED_RETRY_PAYLOAD_FIELDS = {
    "notification_plan_id",
    "analysis_id",
    "candidate_group_id",
    "delivery_decision",
    "urgency_profile",
    "target_chat_id",
    "target_thread_id",
    "render_profile",
    "dedupe_subject_key",
    "material_change_hash",
    "send_after",
    "suppress_reason_code",
    "retry_reason",
    "retry_attempt",
}


def evaluate_retry_promotion(
    *,
    delivery_status: str,
    plan: NotificationPlanRecord | None,
    latest_attempt_count: int,
    max_attempts: int,
    enabled: bool,
    now: datetime | None = None,
) -> DeliveryRetryDecision:
    if delivery_status != "failed_retryable":
        return DeliveryRetryDecision(action="noop", reason_code="delivery_status_not_retryable")
    if not enabled:
        return DeliveryRetryDecision(action="noop", reason_code="retry_promotion_disabled")
    if plan is None:
        return DeliveryRetryDecision(action="noop", reason_code="notification_plan_missing")
    if plan.status != "failed_retryable":
        return DeliveryRetryDecision(action="noop", reason_code="notification_plan_status_not_retryable")
    if plan.send_after is None:
        return DeliveryRetryDecision(action="noop", reason_code="notification_plan_not_due")
    now_utc = _as_utc(now or datetime.now(timezone.utc))
    if _as_utc(plan.send_after) > now_utc:
        return DeliveryRetryDecision(action="noop", reason_code="notification_plan_not_due")
    if latest_attempt_count >= max_attempts:
        return DeliveryRetryDecision(action="dead_letter_retry_ceiling", reason_code="max_notification_retry_attempts_exceeded")

    retry_attempt = latest_attempt_count + 1
    payload = build_retry_intent_payload(
        plan=plan,
        retry_reason="due_retry_promotion",
        retry_attempt=retry_attempt,
    )
    return DeliveryRetryDecision(
        action="emit_retry_intent",
        reason_code="due_retry_promotion",
        retry_attempt=retry_attempt,
        dedupe_key=retry_intent_dedupe_key(
            notification_plan_id=plan.notification_plan_id,
            latest_attempt_count=latest_attempt_count,
            send_after=plan.send_after,
        ),
        payload=payload,
    )


def build_retry_intent_payload(
    *,
    plan: NotificationPlanRecord,
    retry_reason: str,
    retry_attempt: int,
) -> dict[str, Any]:
    return {
        "notification_plan_id": str(plan.notification_plan_id),
        "analysis_id": str(plan.analysis_id),
        "candidate_group_id": str(plan.candidate_group_id),
        "delivery_decision": plan.delivery_decision,
        "urgency_profile": plan.urgency_profile,
        "target_chat_id": plan.target_chat_id,
        "target_thread_id": plan.target_thread_id,
        "render_profile": plan.render_profile,
        "dedupe_subject_key": plan.dedupe_subject_key,
        "material_change_hash": plan.material_change_hash,
        "send_after": None,
        "suppress_reason_code": plan.suppress_reason_code,
        "retry_reason": retry_reason,
        "retry_attempt": retry_attempt,
    }


def retry_intent_dedupe_key(
    *,
    notification_plan_id: UUID,
    latest_attempt_count: int,
    send_after: datetime | None,
) -> str:
    if send_after is None:
        send_after_key = "none"
    else:
        send_after_key = str(int(_as_utc(send_after).timestamp()))
    return f"notify:retry-intent:{notification_plan_id}:{latest_attempt_count}:{send_after_key}"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
