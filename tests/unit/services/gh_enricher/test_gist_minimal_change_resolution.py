from __future__ import annotations

from uuid import uuid4

import pytest

from services.gh_enricher.fetch_planner import GitHubFetchPlanner
from services.gh_enricher.file_sampler import GitHubFileSampler
from services.gh_enricher.models import ArtifactEnrichmentJob, ArtifactRecord
from services.gh_enricher.service import GhEnricherService
from services.gh_enricher.url_discovery import GitHubUrlDiscovery


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self, artifact: ArtifactRecord) -> None:
        self.artifact = artifact
        self.repo_children = []
        self.file_samples = []
        self.snapshots = []
        self.outbox = []

    def transaction(self):
        return _Tx()

    async def load_artifact(self, artifact_id):
        return self.artifact

    async def load_current_snapshot(self, snapshot_id):
        return None

    async def insert_enrichment_run_if_absent(self, **kwargs):
        return uuid4()

    async def mark_enrichment_run_started(self, run_id):
        pass

    async def mark_enrichment_run_finished(self, **kwargs):
        pass

    async def insert_snapshot(self, **kwargs):
        self.snapshots.append(kwargs)
        return uuid4()

    async def insert_github_repo_child(self, **kwargs):
        self.repo_children.append(kwargs)

    async def insert_github_file_sample(self, **kwargs):
        self.file_samples.append(kwargs)

    async def insert_discovered_url(self, **kwargs):
        pass

    async def update_artifact_current_snapshot(self, **kwargs):
        pass

    async def insert_snapshot_updated_outbox(self, **kwargs):
        self.outbox.append(kwargs)


class FakeGitHubClient:
    async def get_gist(self, gist_id, *, auth_mode):
        return {
            "description": "example gist",
            "public": True,
            "owner": {"login": "octo"},
            "files": {
                "demo.py": {"language": "Python", "truncated": False},
            },
        }


class Config:
    sample_max_files = 20
    sample_excerpt_chars = 1200
    max_file_bytes = 131072
    github_app_id = None
    github_installation_id = None


@pytest.mark.asyncio
async def test_gist_writes_parent_snapshot_only_without_repo_child() -> None:
    artifact_id = uuid4()
    artifact = ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type="github_gist",
        canonical_id="github_gist:abc123",
        canonical_url="https://gist.github.com/octo/abc123",
        normalized_host="gist.github.com",
        artifact_key_json={"gist_id": "abc123"},
        current_snapshot_id=None,
        current_status=None,
    )
    repository = FakeRepository(artifact)
    service = GhEnricherService(
        Config(),
        repository=repository,
        github_client=FakeGitHubClient(),
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
            artifact_type="github_gist",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
        )
    )

    assert result.emitted_snapshot_updated is True
    assert repository.snapshots[0]["plan"].snapshot_type == "github_gist"
    assert repository.snapshots[0]["plan"].repo_child is None
    assert repository.repo_children == []
    assert repository.file_samples == []
