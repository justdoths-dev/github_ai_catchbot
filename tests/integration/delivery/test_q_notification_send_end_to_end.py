from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.notifier_telegram.models import StreamMessage
from services.notifier_telegram.worker import NotifierTelegramWorker
from services.outbox_relay.models import OutboxEventRow, RedisQueuedMessage
from services.outbox_relay.routing import OutboxRouteResolver

from tests.component.services.notifier_telegram._fakes import FakeConsumer, RaisingTelegramClient, config, repo_with_valid_case, service


def _outbox_row(event_type: str, *, aggregate_id, payload_json: dict | None = None) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=aggregate_id,
        event_type=event_type,
        aggregate_type="notification_plan",
        aggregate_id=aggregate_id,
        dedupe_key=f"dedupe:{event_type}:{aggregate_id}",
        payload_json=payload_json or {},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_notification_plan_created_routes_to_notifier_and_result_routes_to_maintenance() -> None:
    repository, intent = repo_with_valid_case()
    resolver = OutboxRouteResolver()
    plan_row = _outbox_row("notification.plan.created.v1", aggregate_id=intent.trigger_event_id)

    notify_route = resolver.resolve(plan_row)
    message = RedisQueuedMessage(
        job_id=str(plan_row.event_id),
        stage_name=notify_route.stage_name,
        root_object_type=plan_row.aggregate_type,
        root_object_id=str(plan_row.aggregate_id),
        idempotency_key=plan_row.dedupe_key,
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=str(plan_row.event_id),
    )
    consumer = FakeConsumer(
        [
            StreamMessage(
                stream=notify_route.queue_name,
                message_id="1-0",
                fields=message.as_stream_fields(),
            )
        ]
    )
    worker = NotifierTelegramWorker(
        config(dry_run=False, enable_notification_send=False),
        consumer=consumer,
        service=service(repository, cfg=config(dry_run=False, enable_notification_send=False), client=RaisingTelegramClient()),
    )

    result = await worker.run_once()

    assert notify_route.queue_name == "q.notification.send"
    assert result.processed == 1
    assert consumer.acked == ["1-0"]
    assert repository.loaded_trigger_ids == [intent.trigger_event_id]
    assert repository.plans[intent.notification_plan_id].status == "suppressed"
    assert repository.delivery_outbox[0]["delivery_status"] == "suppressed"

    result_row = _outbox_row("notification.delivery.result.v1", aggregate_id=intent.notification_plan_id)
    maintenance_route = resolver.resolve(result_row)
    assert maintenance_route.queue_name == "q.maintenance"
    assert maintenance_route.stage_name == "maintenance"
