from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.maintenance.delivery_replay import (
    REQUIRED_REPLAY_PAYLOAD_FIELDS,
    REPLAY_QUEUE_NAME,
    build_replay_intent_payload,
    replay_intent_dedupe_key,
)
from services.maintenance.delivery_retry import (
    MAINTENANCE_QUEUE_NAME,
    REQUIRED_RETRY_PAYLOAD_FIELDS,
    build_retry_intent_payload,
    retry_intent_dedupe_key,
)
from services.maintenance.models import NotificationPlanRecord


def _plan():
    return NotificationPlanRecord(
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision="send_now",
        urgency_profile="normal_silent",
        target_chat_id=12345,
        target_thread_id=777,
        render_profile="telegram_single_alert_v1",
        dedupe_subject_key="subject",
        material_change_hash="material",
        send_after=datetime.now(timezone.utc),
        suppress_reason_code=None,
        status="failed_retryable",
    )


def test_retry_intent_payload_contains_required_notifier_fields() -> None:
    payload = build_retry_intent_payload(
        plan=_plan(),
        retry_reason="due_retry_promotion",
        previous_attempt_count=1,
        retry_attempt=2,
    )

    assert REQUIRED_RETRY_PAYLOAD_FIELDS <= payload.keys()
    assert payload["send_after"] is None
    assert payload["previous_attempt_count"] == 1
    assert payload["retry_attempt"] == 2


def test_replay_intent_payload_contains_required_notifier_fields() -> None:
    replay_request_id = uuid4()
    payload = build_replay_intent_payload(
        plan=_plan(),
        replay_request_id=replay_request_id,
        replay_reason="explicit_delivery_replay",
    )

    assert REQUIRED_REPLAY_PAYLOAD_FIELDS <= payload.keys()
    assert payload["send_after"] is None
    assert payload["replay_request_id"] == str(replay_request_id)


def test_dedupe_keys_are_deterministic() -> None:
    plan = _plan()
    replay_request_id = uuid4()

    assert retry_intent_dedupe_key(
        notification_plan_id=plan.notification_plan_id,
        latest_attempt_count=1,
        send_after=plan.send_after,
    ) == retry_intent_dedupe_key(
        notification_plan_id=plan.notification_plan_id,
        latest_attempt_count=1,
        send_after=plan.send_after,
    )
    assert replay_intent_dedupe_key(replay_request_id) == replay_intent_dedupe_key(replay_request_id)


def test_no_new_queue_names() -> None:
    assert MAINTENANCE_QUEUE_NAME == "q.maintenance"
    assert REPLAY_QUEUE_NAME == "q.replay"
