from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.outbox_relay.config import OutboxRelayConfig
from services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from services.outbox_relay.routing import OutboxRouteResolver
from services.outbox_relay.service import OutboxRelayService


@dataclass
class _JobAttempt:
    stage_name: str
    queue_name: str
    root_object_type: str
    root_object_id: UUID
    attempt_status: str
    error_code: str | None


class FakeRepository:
    def __init__(self, rows: list[OutboxEventRow]) -> None:
        self.rows = rows
        self.published_event_ids: list[UUID] = []
        self.failed_event_ids: list[UUID] = []
        self.last_error_by_event_id: dict[UUID, str] = {}
        self.job_attempts: list[_JobAttempt] = []

    async def fetch_pending_batch(self, *, limit: int) -> list[OutboxEventRow]:
        return self.rows[:limit]

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        self.published_event_ids.append(event_id)

    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None:
        self.failed_event_ids.append(event_id)
        self.last_error_by_event_id[event_id] = error_text

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        self.job_attempts.append(
            _JobAttempt(
                stage_name=stage_name,
                queue_name=queue_name,
                root_object_type=root_object_type,
                root_object_id=root_object_id,
                attempt_status=attempt_status,
                error_code=error_code,
            )
        )


class FailingPublisher:
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        raise RuntimeError("redis unavailable")


@pytest.mark.asyncio
async def test_publish_failure_marks_outbox_failed_and_writes_retryable_attempt() -> None:
    row = OutboxEventRow(
        event_id=uuid4(),
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=uuid4(),
        dedupe_key="srcmsg:create:abc:1",
        payload_json={"source_message_id": str(uuid4())},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    repository = FakeRepository([row])
    service = OutboxRelayService(
        OutboxRelayConfig(
            app_env="test",
            database_url="postgresql+psycopg://example",
            redis_url="redis://example",
            poll_interval_ms=1000,
            batch_size=10,
            xadd_maxlen=10000,
            log_level="INFO",
        ),
        repository=repository,
        publisher=FailingPublisher(),
        route_resolver=OutboxRouteResolver(),
    )

    processed = await service.run_once()

    assert processed == 1
    assert repository.published_event_ids == []
    assert repository.failed_event_ids == [row.event_id]
    assert repository.last_error_by_event_id[row.event_id] == "redis unavailable"
    assert len(repository.job_attempts) == 1
    attempt = repository.job_attempts[0]
    assert attempt.stage_name == "normalize"
    assert attempt.queue_name == "q.source.normalize"
    assert attempt.root_object_type == row.aggregate_type
    assert attempt.root_object_id == row.aggregate_id
    assert attempt.attempt_status == "failed_retryable"
    assert attempt.error_code == "RuntimeError"
