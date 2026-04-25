from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.outbox_relay.config import OutboxRelayConfig
from services.outbox_relay.models import OutboxEventRow
from services.outbox_relay.routing import OutboxRouteResolver
from services.outbox_relay.service import OutboxRelayService


class _UnusedRepository:
    async def fetch_pending_batch(self, *, limit: int):  # pragma: no cover - not used here
        raise AssertionError("not expected")


class _UnusedPublisher:
    async def publish(self, route, message):  # pragma: no cover - not used here
        raise AssertionError("not expected")


def test_stream_message_is_id_only_payload() -> None:
    config = OutboxRelayConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        poll_interval_ms=1000,
        batch_size=10,
        xadd_maxlen=10000,
        log_level="INFO",
    )
    service = OutboxRelayService(
        config,
        repository=_UnusedRepository(),
        publisher=_UnusedPublisher(),
        route_resolver=OutboxRouteResolver(),
    )
    row = OutboxEventRow(
        event_id=uuid4(),
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=uuid4(),
        dedupe_key="srcmsg:create:1:1",
        payload_json={
            "source_message_id": str(uuid4()),
            "current_version_no": 1,
            "logical_post_key": "chat:1:1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )

    route = OutboxRouteResolver().resolve(row)
    message = service._build_stream_message(row, route)
    fields = message.as_stream_fields()

    assert message.job_id == str(row.event_id)
    assert message.trigger_event_id == str(row.event_id)
    assert message.root_object_type == row.aggregate_type
    assert message.root_object_id == str(row.aggregate_id)
    assert message.idempotency_key == row.dedupe_key
    assert set(fields) == {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "pipeline_run_id",
        "not_before",
        "trigger_event_id",
    }
    assert "payload_json" not in fields
    assert fields["pipeline_run_id"] == ""
    assert fields["not_before"] == ""
