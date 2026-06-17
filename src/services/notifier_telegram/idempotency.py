from __future__ import annotations

from uuid import UUID

from .models import (
    NotifierIdempotencyClassification,
    NotifierIdempotencyReadback,
    NotifierPlanIdempotencySnapshot,
)


TERMINAL_PLAN_STATUSES = {"sent", "edited", "suppressed", "failed_terminal"}
SENT_PLAN_STATUSES = {"sent", "edited"}
PENDING_PLAN_STATUSES = {"planned", "queued"}


def classify_notifier_idempotency_state(
    snapshots: list[NotifierPlanIdempotencySnapshot],
) -> NotifierIdempotencyReadback:
    plan_count = len(snapshots)
    render_count = sum(snapshot.render_count for snapshot in snapshots)
    delivery_record_count = sum(snapshot.delivery_record_count for snapshot in snapshots)
    sent_delivery_count = sum(snapshot.sent_delivery_count for snapshot in snapshots)
    suppressed_delivery_count = sum(snapshot.suppressed_delivery_count for snapshot in snapshots)
    terminal_delivery_count = sum(snapshot.terminal_delivery_count for snapshot in snapshots)
    retryable_failure_count = sum(snapshot.retryable_failure_count for snapshot in snapshots)
    sent_delivery_chat_id_present_count = sum(
        snapshot.sent_delivery_chat_id_present_count for snapshot in snapshots
    )
    sent_delivery_message_id_present_count = sum(
        snapshot.sent_delivery_message_id_present_count for snapshot in snapshots
    )

    classifications: list[NotifierIdempotencyClassification] = []
    statuses = {snapshot.status for snapshot in snapshots}

    if plan_count == 0:
        classifications.append("no_existing_plan")
    else:
        if plan_count > 1:
            classifications.append("existing_duplicate_plans")
        if sent_delivery_count > 1:
            classifications.append("existing_duplicate_sent_deliveries")
        if sent_delivery_count > 0 or statuses.intersection(SENT_PLAN_STATUSES):
            classifications.append("existing_plan_sent")
        if terminal_delivery_count > 0 or statuses.intersection({"sent", "edited", "failed_terminal"}):
            classifications.append("existing_terminal_delivery")
        if suppressed_delivery_count > 0 or "suppressed" in statuses:
            classifications.append("existing_plan_suppressed")
        if retryable_failure_count > 0 or "failed_retryable" in statuses:
            classifications.append("existing_retryable_failure")
        if render_count > 0 and delivery_record_count == 0:
            classifications.append("existing_plan_rendered")
        if statuses.intersection(PENDING_PLAN_STATUSES) and render_count == 0 and delivery_record_count == 0:
            classifications.append("existing_plan_pending")
        if not classifications:
            classifications.append("existing_plan_pending")

    primary = _primary_classification(classifications)
    return NotifierIdempotencyReadback(
        primary_classification=primary,
        classifications=tuple(_dedupe_preserve_order(classifications)),
        plan_count=plan_count,
        render_count=render_count,
        delivery_record_count=delivery_record_count,
        sent_delivery_count=sent_delivery_count,
        suppressed_delivery_count=suppressed_delivery_count,
        terminal_delivery_count=terminal_delivery_count,
        retryable_failure_count=retryable_failure_count,
        sent_delivery_chat_id_present_count=sent_delivery_chat_id_present_count,
        sent_delivery_message_id_present_count=sent_delivery_message_id_present_count,
        plan_id_suffixes=tuple(_id_suffix(snapshot.notification_plan_id) for snapshot in snapshots),
    )


def should_noop_before_concretization(readback: NotifierIdempotencyReadback) -> bool:
    classifications = set(readback.classifications)
    if classifications.intersection(
        {"existing_duplicate_sent_deliveries", "existing_plan_sent", "existing_terminal_delivery"}
    ):
        return True
    return "existing_plan_suppressed" in classifications and readback.suppressed_delivery_count > 0


def should_fail_closed_before_concretization(readback: NotifierIdempotencyReadback) -> bool:
    classifications = set(readback.classifications)
    if "existing_duplicate_plans" not in classifications:
        return False
    if classifications.intersection(
        {"existing_duplicate_sent_deliveries", "existing_plan_sent", "existing_terminal_delivery"}
    ):
        return False
    if "existing_plan_suppressed" in classifications and readback.suppressed_delivery_count > 0:
        return False
    return True


def idempotency_noop_reason(readback: NotifierIdempotencyReadback) -> str:
    classifications = set(readback.classifications)
    if "existing_duplicate_sent_deliveries" in classifications:
        return "duplicate_existing_state"
    if "existing_plan_sent" in classifications:
        return "notification_duplicate_noop"
    if "existing_plan_suppressed" in classifications:
        return "notification_existing_suppressed_noop"
    if "existing_terminal_delivery" in classifications:
        return "notification_already_delivered_noop"
    return "notification_idempotent_noop"


def idempotency_transition_plan_id(
    snapshots: list[NotifierPlanIdempotencySnapshot],
    fallback: UUID,
) -> UUID:
    if not snapshots:
        return fallback
    for snapshot in snapshots:
        if snapshot.sent_delivery_count > 0 or snapshot.status in SENT_PLAN_STATUSES:
            return snapshot.notification_plan_id
    for snapshot in snapshots:
        if snapshot.terminal_delivery_count > 0 or snapshot.status in TERMINAL_PLAN_STATUSES:
            return snapshot.notification_plan_id
    return snapshots[0].notification_plan_id


def _primary_classification(
    classifications: list[NotifierIdempotencyClassification],
) -> NotifierIdempotencyClassification:
    priority: tuple[NotifierIdempotencyClassification, ...] = (
        "existing_duplicate_sent_deliveries",
        "existing_duplicate_plans",
        "existing_plan_sent",
        "existing_plan_suppressed",
        "existing_terminal_delivery",
        "existing_retryable_failure",
        "existing_plan_rendered",
        "existing_plan_pending",
        "no_existing_plan",
    )
    present = set(classifications)
    for value in priority:
        if value in present:
            return value
    return classifications[0]


def _dedupe_preserve_order(
    values: list[NotifierIdempotencyClassification],
) -> list[NotifierIdempotencyClassification]:
    seen: set[NotifierIdempotencyClassification] = set()
    deduped: list[NotifierIdempotencyClassification] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _id_suffix(value: UUID) -> str:
    return str(value)[-8:]
