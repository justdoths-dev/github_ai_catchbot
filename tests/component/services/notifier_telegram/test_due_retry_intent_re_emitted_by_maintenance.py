from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4


@dataclass(frozen=True, slots=True)
class FailedRetryablePlanRow:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime
    status: str


def _due_retry_intent(row: FailedRetryablePlanRow, *, now: datetime) -> dict | None:
    if row.status != "failed_retryable" or row.send_after > now:
        return None
    return {
        "event_type": "notification.plan.created.v1",
        "aggregate_type": "notification_plan",
        "aggregate_id": str(row.notification_plan_id),
        "dedupe_key": f"notify:retry-intent:{row.notification_plan_id}:{int(row.send_after.timestamp())}",
        "payload_json": {
            "notification_plan_id": str(row.notification_plan_id),
            "analysis_id": str(row.analysis_id),
            "candidate_group_id": str(row.candidate_group_id),
            "delivery_decision": row.delivery_decision,
            "urgency_profile": row.urgency_profile,
            "target_chat_id": row.target_chat_id,
            "target_thread_id": row.target_thread_id,
            "render_profile": row.render_profile,
            "dedupe_subject_key": row.dedupe_subject_key,
            "material_change_hash": row.material_change_hash,
            "send_after": None,
            "retry_reason": "due_retry_promotion",
        },
    }


def _row(*, send_after: datetime, status: str = "failed_retryable") -> FailedRetryablePlanRow:
    return FailedRetryablePlanRow(
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
        send_after=send_after,
        status=status,
    )


def test_due_failed_retryable_plan_becomes_retry_intent_without_plan_mutation() -> None:
    now = datetime.now(timezone.utc)
    row = _row(send_after=now - timedelta(seconds=1))
    before = asdict(row)

    event = _due_retry_intent(row, now=now)

    assert asdict(row) == before
    assert event is not None
    assert event["event_type"] == "notification.plan.created.v1"
    assert event["aggregate_type"] == "notification_plan"
    assert event["aggregate_id"] == str(row.notification_plan_id)
    assert event["dedupe_key"].startswith(f"notify:retry-intent:{row.notification_plan_id}:")
    assert event["payload_json"]["notification_plan_id"] == str(row.notification_plan_id)
    assert event["payload_json"]["send_after"] is None
    assert event["payload_json"]["retry_reason"] == "due_retry_promotion"


def test_not_due_or_non_retryable_plan_does_not_emit_retry_intent() -> None:
    now = datetime.now(timezone.utc)

    assert _due_retry_intent(_row(send_after=now + timedelta(minutes=5)), now=now) is None
    assert _due_retry_intent(_row(send_after=now - timedelta(seconds=1), status="suppressed"), now=now) is None
