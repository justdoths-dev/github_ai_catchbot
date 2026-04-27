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


class FakeXClient:
    def default_request_profile(self):
        return object()

    async def get_posts_by_ids(self, *, post_ids, profile):
        assert post_ids == ["1881234567890123456"]
        return {
            "data": [
                {
                    "id": "1881234567890123456",
                    "text": "Root post https://t.co/root",
                    "author_id": "42",
                    "conversation_id": "1881234567890123456",
                    "edit_history_tweet_ids": ["1881234567890123456"],
                    "entities": {
                        "urls": [
                            {
                                "url": "https://t.co/root",
                                "expanded_url": "https://github.com/openai/openai-python",
                            }
                        ]
                    },
                }
            ],
            "includes": {"users": [{"id": "42", "username": "dev", "name": "Dev"}]},
        }


class Config:
    pass


@pytest.mark.asyncio
async def test_x_snapshot_write_updates_current_pointer_and_emits_outbox() -> None:
    artifact_id = uuid4()
    candidate_group_id = uuid4()
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
        x_api_client=FakeXClient(),
        response_mapper=XResponseMapper(),
        url_discovery=XUrlDiscovery(),
    )

    result = await service.handle_job(
        ArtifactEnrichmentJob(
            trigger_event_id=uuid4(),
            event_type="artifact.enrich.requested.v1",
            candidate_group_id=candidate_group_id,
            artifact_id=artifact_id,
            artifact_type="x_post",
            provider_route="x",
            refresh_mode="standard",
            depth_budget=1,
            requested_at=datetime.now(timezone.utc),
        )
    )

    assert result.emitted_snapshot_updated is True
    assert result.status == "ready"
    assert result.content_anchor == "xpost:1881234567890123456:1881234567890123456"
    assert repository.snapshots[0]["draft"].snapshot_type == "x_post"
    assert repository.x_children[0]["draft"].post_id == "1881234567890123456"
    assert repository.current_updates[0]["artifact_id"] == artifact_id
    assert repository.current_updates[0]["status"] == "ready"
    assert repository.outbox[0]["artifact_id"] == artifact_id
    assert repository.outbox[0]["candidate_group_id"] == candidate_group_id
    assert repository.discovered_urls
    assert repository.discovered_urls[0]["draft"].observed_url == "https://github.com/openai/openai-python"
