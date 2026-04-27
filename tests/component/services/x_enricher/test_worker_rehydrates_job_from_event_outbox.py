from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.x_enricher.config import XEnricherConfig
from services.x_enricher.models import ArtifactEnrichmentJob
from services.x_enricher.redis_streams import StreamMessage
from services.x_enricher.repositories import XEnricherRepository
from services.x_enricher.worker import XEnricherWorker


class FakeConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self.message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self):
        return [self.message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class FakeService:
    def __init__(self, job: ArtifactEnrichmentJob) -> None:
        self.job = job
        self.rehydrated_trigger_ids: list[str] = []
        self.handled_jobs: list[ArtifactEnrichmentJob] = []

    async def rehydrate_job(self, trigger_event_id: str):
        self.rehydrated_trigger_ids.append(trigger_event_id)
        return self.job

    async def handle_job(self, job: ArtifactEnrichmentJob):
        self.handled_jobs.append(job)


class SingleRowResult:
    def __init__(self, row) -> None:
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class SingleRowSession:
    def __init__(self, row) -> None:
        self.row = row

    def in_transaction(self) -> bool:
        return False

    async def execute(self, statement, params=None):
        return SingleRowResult(self.row)


def _config() -> XEnricherConfig:
    return XEnricherConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.artifact.enrich.x",
        consumer_group="x-enricher",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        x_api_base_url="https://api.x.com",
        x_bearer_token="token",
        request_timeout_sec=1,
        request_max_ids=100,
        depth_budget_default=1,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_worker_uses_trigger_event_id_to_rehydrate_before_ack() -> None:
    trigger_event_id = uuid4()
    job = ArtifactEnrichmentJob(
        trigger_event_id=trigger_event_id,
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=uuid4(),
        artifact_id=uuid4(),
        artifact_type="x_post",
        provider_route="x",
        refresh_mode="standard",
        depth_budget=1,
        requested_at=datetime.now(timezone.utc),
    )
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.artifact.enrich.x",
            message_id="1-0",
            fields={
                "job_id": "untrusted",
                "stage_name": "enrich_x",
                "root_object_id": str(uuid4()),
                "trigger_event_id": str(trigger_event_id),
            },
        )
    )
    service = FakeService(job)
    worker = XEnricherWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert service.rehydrated_trigger_ids == [str(trigger_event_id)]
    assert service.handled_jobs == [job]
    assert consumer.acked == ["1-0"]


@pytest.mark.asyncio
async def test_repository_rehydrates_requested_at_from_event_created_at_when_payload_lacks_field() -> None:
    trigger_event_id = uuid4()
    created_at = datetime(2026, 4, 27, 0, 0, tzinfo=timezone.utc)
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    repository = XEnricherRepository(
        SingleRowSession(
            {
                "event_id": trigger_event_id,
                "event_type": "artifact.enrich.requested.v1",
                "created_at": created_at,
                "payload_json": {
                    "candidate_group_id": str(candidate_group_id),
                    "artifact_id": str(artifact_id),
                    "artifact_type": "x_post",
                    "canonical_id": "x:post:1881234567890123456",
                    "provider_route": "x",
                    "refresh_mode": "standard",
                    "depth_budget": 1,
                },
            }
        )
    )

    job = await repository.load_job_by_trigger_event_id(trigger_event_id)

    assert job is not None
    assert job.trigger_event_id == trigger_event_id
    assert job.candidate_group_id == candidate_group_id
    assert job.artifact_id == artifact_id
    assert job.requested_at == created_at
