from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True, frozen=True)
class OutboxEventRow:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str
    fail_count: int
    created_at: datetime


@dataclass(slots=True, frozen=True)
class QueueRoute:
    queue_name: str
    stage_name: str


@dataclass(slots=True, frozen=True)
class RedisQueuedMessage:
    job_id: str
    stage_name: str
    root_object_type: str
    root_object_id: str
    idempotency_key: str
    pipeline_run_id: str | None
    not_before: str | None
    trigger_event_id: str

    def as_stream_fields(self) -> dict[str, str]:
        return {
            "job_id": self.job_id,
            "stage_name": self.stage_name,
            "root_object_type": self.root_object_type,
            "root_object_id": self.root_object_id,
            "idempotency_key": self.idempotency_key,
            "pipeline_run_id": self.pipeline_run_id or "",
            "not_before": self.not_before or "",
            "trigger_event_id": self.trigger_event_id,
        }


def redis_queued_message_from_outbox_row(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=str(row.event_id),
    )
