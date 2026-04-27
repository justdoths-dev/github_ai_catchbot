from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.gh_enricher.fetch_planner import GitHubFetchPlanner
from services.gh_enricher.file_sampler import GitHubFileSampler
from services.gh_enricher.models import ArtifactEnrichmentJob, ArtifactRecord, CurrentSnapshotRef
from services.gh_enricher.service import GhEnricherService
from services.gh_enricher.url_discovery import GitHubUrlDiscovery


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self, artifact: ArtifactRecord, current_snapshot: CurrentSnapshotRef) -> None:
        self.artifact = artifact
        self.current_snapshot = current_snapshot
        self.runs = []
        self.snapshots = []
        self.outbox = []

    def transaction(self):
        return _Tx()

    async def load_artifact(self, artifact_id):
        return self.artifact

    async def load_current_snapshot(self, snapshot_id):
        return self.current_snapshot

    async def insert_enrichment_run_if_absent(self, **kwargs):
        self.runs.append(kwargs)
        return uuid4()

    async def insert_snapshot(self, **kwargs):
        self.snapshots.append(kwargs)
        return uuid4()

    async def insert_snapshot_updated_outbox(self, **kwargs):
        self.outbox.append(kwargs)


class FailingGitHubClient:
    def __getattr__(self, name):
        raise AssertionError("standard current snapshot refresh should not call GitHub")


class Config:
    sample_max_files = 20
    sample_excerpt_chars = 1200
    max_file_bytes = 131072
    github_app_id = None
    github_installation_id = None


@pytest.mark.asyncio
async def test_standard_refresh_short_circuits_when_current_snapshot_is_ready() -> None:
    artifact_id = uuid4()
    snapshot_id = uuid4()
    artifact = ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type="github_repo",
        canonical_id="github_repo:openai/openai-python",
        canonical_url="https://github.com/openai/openai-python",
        normalized_host="github.com",
        artifact_key_json={"owner": "openai", "repo": "openai-python"},
        current_snapshot_id=snapshot_id,
        current_status="ready",
    )
    current = CurrentSnapshotRef(
        snapshot_id=snapshot_id,
        status="ready",
        fetched_at=datetime.now(timezone.utc),
        content_anchor="commit:abc123",
    )
    repository = FakeRepository(artifact, current)
    service = GhEnricherService(
        Config(),
        repository=repository,
        github_client=FailingGitHubClient(),
        fetch_planner=GitHubFetchPlanner(),
        file_sampler=GitHubFileSampler(),
        url_discovery=GitHubUrlDiscovery(),
    )

    result = await service.handle_job(
        ArtifactEnrichmentJob(
            trigger_event_id=uuid4(),
            event_type="artifact.enrich.requested.v1",
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
        )
    )

    assert result.snapshot_id == snapshot_id
    assert result.status == "ready"
    assert result.content_anchor == "commit:abc123"
    assert result.emitted_snapshot_updated is True
    assert repository.runs == []
    assert repository.snapshots == []
    assert repository.outbox == [
        {
            "artifact_id": artifact_id,
            "snapshot_id": snapshot_id,
            "status": "ready",
            "content_anchor": "commit:abc123",
        }
    ]
