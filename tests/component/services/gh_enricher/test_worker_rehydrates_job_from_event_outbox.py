from __future__ import annotations

from uuid import uuid4

import pytest

from services.gh_enricher.config import GhEnricherConfig
from services.gh_enricher.models import ArtifactEnrichmentJob
from services.gh_enricher.redis_streams import StreamMessage
from services.gh_enricher.worker import GhEnricherWorker


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


def _config() -> GhEnricherConfig:
    return GhEnricherConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.artifact.enrich.github",
        consumer_group="gh-enricher",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        github_api_base_url="https://api.github.com",
        github_app_id=None,
        github_installation_id=None,
        github_private_key=None,
        request_timeout_sec=1,
        sample_max_files=20,
        sample_excerpt_chars=1200,
        max_file_bytes=131072,
        stale_after_sec=21600,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_worker_uses_trigger_event_id_to_rehydrate_before_ack() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    job = ArtifactEnrichmentJob(
        trigger_event_id=trigger_event_id,
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        artifact_type="github_repo",
        provider_route="github",
        refresh_mode="standard",
        depth_budget=1,
    )
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.artifact.enrich.github",
            message_id="1-0",
            fields={
                "job_id": "untrusted",
                "stage_name": "enrich_github",
                "root_object_id": str(uuid4()),
                "trigger_event_id": str(trigger_event_id),
            },
        )
    )
    service = FakeService(job)
    worker = GhEnricherWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert service.rehydrated_trigger_ids == [str(trigger_event_id)]
    assert service.handled_jobs == [job]
    assert consumer.acked == ["1-0"]
