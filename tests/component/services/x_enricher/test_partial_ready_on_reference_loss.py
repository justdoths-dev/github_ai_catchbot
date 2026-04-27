from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.x_enricher.models import ArtifactEnrichmentJob, ArtifactRecord
from services.x_enricher.response_mapper import XResponseMapper
from services.x_enricher.service import XEnricherService
from services.x_enricher.url_discovery import XUrlDiscovery


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self, artifact: ArtifactRecord) -> None:
        self.artifact = artifact
        self.runs = []
        self.snapshots = []
        self.x_children = []
        self.discovered_urls = []
        self.current_updates = []
        self.outbox = []

    def transaction(self):
        return _Tx()

    async def load_artifact(self, artifact_id):
        return self.artifact

    async def load_current_snapshot(self, snapshot_id):
        return None

    async def insert_enrichment_run_if_absent(self, **kwargs):
        self.runs.append(kwargs)
        return uuid4()

    async def mark_enrichment_run_started(self, run_id):
        pass

    async def mark_enrichment_run_finished(self, **kwargs):
        pass

    async def insert_snapshot(self, **kwargs):
        self.snapshots.append(kwargs)
        return uuid4()

    async def upsert_x_post_child(self, **kwargs):
        self.x_children.append(kwargs)

    async def insert_discovered_url(self, **kwargs):
        self.discovered_urls.append(kwargs)

    async def update_artifact_current_snapshot(self, **kwargs):
        self.current_updates.append(kwargs)

    async def insert_snapshot_updated_outbox(self, **kwargs):
        self.outbox.append(kwargs)


class Config:
    pass


class ReferenceLossXClient:
    def default_request_profile(self):
        return object()

    async def get_posts_by_ids(self, *, post_ids, profile):
        return {
            "data": [
                {
                    "id": "1881234567890123456",
                    "text": "Root with missing reference",
                    "author_id": "42",
                    "edit_history_tweet_ids": ["1881234567890123456"],
                    "referenced_tweets": [{"type": "quoted", "id": "1880000000000000000"}],
                }
            ],
            "includes": {"users": [{"id": "42", "username": "dev"}]},
            "errors": [{"resource_id": "1880000000000000000", "title": "Referenced post unavailable"}],
        }


@pytest.mark.asyncio
async def test_partial_ready_when_reference_include_is_missing() -> None:
    artifact_id = uuid4()
    artifact = ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type="x_post",
        canonical_id="x:post:1881234567890123456",
        canonical_url="https://x.com/dev/status/1881234567890123456",
        normalized_host="x.com",
        artifact_key_json={"post_id": "1881234567890123456"},
        current_snapshot_id=None,
        current_status=None,
    )
    repository = FakeRepository(artifact)
    service = XEnricherService(
        Config(),
        repository=repository,
        x_api_client=ReferenceLossXClient(),
        response_mapper=XResponseMapper(),
        url_discovery=XUrlDiscovery(),
    )

    result = await service.handle_job(
        ArtifactEnrichmentJob(
            trigger_event_id=uuid4(),
            event_type="artifact.enrich.requested.v1",
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="x_post",
            provider_route="x",
            refresh_mode="standard",
            depth_budget=1,
            requested_at=datetime.now(timezone.utc),
        )
    )

    assert result.status == "partial_ready"
    draft = repository.snapshots[0]["draft"]
    assert "partial_errors_present" in draft.fetch_anomalies
    assert "x_referenced_posts_missing" in draft.evidence_limitations
