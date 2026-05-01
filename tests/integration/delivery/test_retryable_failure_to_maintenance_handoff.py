from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.notifier_telegram.telegram_client import TelegramTransportRetryableError
from services.outbox_relay.models import OutboxEventRow
from services.outbox_relay.routing import OutboxRouteResolver

from tests.component.services.notifier_telegram.test_due_retry_intent_re_emitted_by_maintenance import (
    FailedRetryablePlanRow,
    _due_retry_intent,
)
from tests.component.services.notifier_telegram._fakes import config, repo_with_valid_case, service


class RetryableClient:
    async def send_message(self, **kwargs):
        raise TelegramTransportRetryableError("temporary", error_code="telegram_5xx_retryable")


@pytest.mark.asyncio
async def test_retryable_failure_sets_send_after_and_handoff_routes_to_maintenance() -> None:
    repository, intent = repo_with_valid_case()

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=RetryableClient(),
    ).handle_intent(intent)

    plan = repository.plans[intent.notification_plan_id]
    assert plan.status == "failed_retryable"
    assert plan.send_after is not None
    assert plan.send_after > datetime.now(timezone.utc)
    assert repository.delivery_outbox[0]["delivery_status"] == "failed_retryable"

    route = OutboxRouteResolver().resolve(
        OutboxEventRow(
            event_id=intent.trigger_event_id,
            event_type="notification.delivery.result.v1",
            aggregate_type="notification_plan",
            aggregate_id=intent.notification_plan_id,
            dedupe_key=f"notification-delivery-result:{intent.notification_plan_id}",
            payload_json=repository.delivery_outbox[0],
            status="pending",
            fail_count=0,
            created_at=datetime.now(timezone.utc),
        )
    )
    assert route.queue_name == "q.maintenance"

    retry_event = _due_retry_intent(
        FailedRetryablePlanRow(
            notification_plan_id=plan.notification_plan_id,
            analysis_id=plan.analysis_id,
            candidate_group_id=plan.candidate_group_id,
            delivery_decision=plan.delivery_decision,
            urgency_profile=plan.urgency_profile,
            target_chat_id=plan.target_chat_id,
            target_thread_id=plan.target_thread_id,
            render_profile=plan.render_profile or "telegram_single_alert_high_v1",
            dedupe_subject_key=plan.dedupe_subject_key,
            material_change_hash=plan.material_change_hash,
            send_after=plan.send_after,
            status=plan.status,
        ),
        now=plan.send_after,
    )
    assert retry_event is not None
    assert retry_event["event_type"] == "notification.plan.created.v1"
    assert retry_event["payload_json"]["notification_plan_id"] == str(intent.notification_plan_id)
