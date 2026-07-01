from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.maintenance.models import ReplayRequestRecord
from services.maintenance.service import MaintenanceService
from services.outbox_relay.models import OutboxEventRow
from services.outbox_relay.routing import OutboxRouteResolver
from tests.component.services.maintenance._fakes import FakeRepository, config, outbox_event, plan


@pytest.mark.asyncio
async def test_replay_requested_routes_to_replay_and_delivery_replay_reenters_notifier_only() -> None:
    resolver = OutboxRouteResolver()
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
    replay_event = outbox_event(
        "replay.requested.v1",
        aggregate_type="replay_request",
        aggregate_id=replay_request_id,
        payload_json={
            "replay_request_id": str(replay_request_id),
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": str(notification_plan.notification_plan_id),
        },
    )
    repository.events[replay_event.event_id] = replay_event

    replay_route = resolver.resolve(
        OutboxEventRow(
            event_id=replay_event.event_id,
            event_type=replay_event.event_type,
            aggregate_type=replay_event.aggregate_type,
            aggregate_id=replay_event.aggregate_id,
            dedupe_key="replay-requested",
            payload_json=replay_event.payload_json,
            status="pending",
            fail_count=0,
            created_at=datetime.now(timezone.utc),
        )
    )
    await MaintenanceService(config(app_env="replay"), repository=repository).handle_replay_trigger_event(replay_event.event_id)
    emitted = repository.plan_created_outbox[0]
    notifier_route = resolver.resolve(
        OutboxEventRow(
            event_id=emitted["aggregate_id"],
            event_type=emitted["event_type"],
            aggregate_type=emitted["aggregate_type"],
            aggregate_id=emitted["aggregate_id"],
            dedupe_key=emitted["dedupe_key"],
            payload_json=emitted["payload_json"],
            status="pending",
            fail_count=0,
            created_at=datetime.now(timezone.utc),
        )
    )

    assert replay_route.queue_name == "q.replay"
    assert emitted["event_type"] == "notification.plan.created.v1"
    assert emitted["payload_json"]["notification_plan_id"] == str(notification_plan.notification_plan_id)
    assert emitted["payload_json"]["replay_request_id"] == str(replay_request_id)
    assert notifier_route.queue_name == "q.notification.send"
    assert repository.upstream_recompute_calls == 0
