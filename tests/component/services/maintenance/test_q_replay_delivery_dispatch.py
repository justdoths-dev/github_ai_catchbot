from __future__ import annotations

from uuid import uuid4

import pytest

from services.maintenance.models import ReplayRequestRecord, StreamMessage
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import ReplayQueueWorker

from ._fakes import FakeConsumer, FakeRepository, config, outbox_event, plan


@pytest.mark.asyncio
async def test_worker_rehydrates_replay_request_and_dispatches_delivery_intent() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="sent", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    replay_request_id = uuid4()
    repository.replay_requests[replay_request_id] = ReplayRequestRecord(
        replay_request_id=replay_request_id,
        replay_type="delivery",
        root_object_type="notification_plan",
        root_object_id=notification_plan.notification_plan_id,
        status="pending",
    )
    event = outbox_event(
        "replay.requested.v1",
        aggregate_id=replay_request_id,
        payload_json={
            "replay_request_id": str(replay_request_id),
            "replay_type": "full_pipeline",
            "root_object_type": "analysis",
            "root_object_id": str(uuid4()),
            "replay_reason": "operator_recovery",
        },
    )
    repository.events[event.event_id] = event
    consumer = FakeConsumer(
        [StreamMessage(stream="q.replay", message_id="1-0", fields={"trigger_event_id": str(event.event_id)})]
    )
    service = MaintenanceService(config(app_env="replay"), repository=repository)
    worker = ReplayQueueWorker(config(app_env="replay"), consumer=consumer, service=service)

    result = await worker.run_once()
    await service.handle_replay_trigger_event(event.event_id)

    assert result.processed == 1
    assert consumer.acked == ["1-0"]
    assert len(repository.plan_created_outbox) == 1
    assert repository.plan_created_outbox[0]["payload_json"]["notification_plan_id"] == str(
        notification_plan.notification_plan_id
    )
    assert repository.plan_created_outbox[0]["payload_json"]["replay_request_id"] == str(replay_request_id)
    assert repository.replay_requests[replay_request_id].status == "completed"
    assert repository.upstream_recompute_calls == 0
    assert len(repository.plan_created_outbox) == 1
