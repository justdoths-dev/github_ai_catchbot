from __future__ import annotations

import base64
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
        self.runs = []
        self.snapshots = []
        self.repo_children = []
        self.file_samples = []
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

    async def insert_github_repo_child(self, **kwargs):
        self.repo_children.append(kwargs)

    async def insert_github_file_sample(self, **kwargs):
        self.file_samples.append(kwargs)

    async def insert_discovered_url(self, **kwargs):
        self.discovered_urls.append(kwargs)

    async def update_artifact_current_snapshot(self, **kwargs):
        self.current_updates.append(kwargs)

    async def insert_snapshot_updated_outbox(self, **kwargs):
        self.outbox.append(kwargs)


class FakeGitHubClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_repo(self, owner, repo, *, auth_mode):
        self.calls.append("repo")
        return {
            "full_name": f"{owner}/{repo}",
            "default_branch": "main",
            "description": "SDK",
            "homepage": "https://sdk.example.dev",
            "license": {"spdx_id": "MIT"},
            "topics": ["ai"],
            "language": "Python",
            "stargazers_count": 10,
            "subscribers_count": 2,
            "forks_count": 1,
            "open_issues_count": 0,
            "archived": False,
            "fork": False,
            "is_template": False,
        }

    async def get_default_branch_head(self, owner, repo, default_branch, *, auth_mode):
        self.calls.append("head")
        return {"sha": "abc123"}

    async def get_tree(self, owner, repo, ref, *, recursive, auth_mode):
        self.calls.append("tree")
        return {
            "truncated": False,
            "tree": [
                {"type": "blob", "path": "README.md"},
                {"type": "blob", "path": "pyproject.toml"},
                {"type": "blob", "path": "tests/test_sdk.py"},
            ],
        }

    async def get_contents(self, owner, repo, path, *, ref, auth_mode):
        self.calls.append(f"contents:{path}")
        text = f"{path}\nhttps://example.com/{path}"
        return {
            "encoding": "base64",
            "content": base64.b64encode(text.encode()).decode(),
            "size": len(text),
        }

    async def get_releases(self, owner, repo, *, auth_mode):
        self.calls.append("releases")
        return [{"published_at": "2026-04-01T00:00:00Z", "assets": [{"download_count": 3}], "prerelease": False}]


class Config:
    sample_max_files = 20
    sample_excerpt_chars = 1200
    max_file_bytes = 131072
    github_app_id = None
    github_installation_id = None


@pytest.mark.asyncio
async def test_repo_snapshot_write_updates_current_pointer_and_emits_outbox() -> None:
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    artifact = ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type="github_repo",
        canonical_id="github_repo:openai/openai-python",
        canonical_url="https://github.com/openai/openai-python",
        normalized_host="github.com",
        artifact_key_json={"owner": "openai", "repo": "openai-python"},
        current_snapshot_id=None,
        current_status=None,
    )
    repository = FakeRepository(artifact)
    client = FakeGitHubClient()
    service = GhEnricherService(
        Config(),
        repository=repository,
        github_client=client,
        fetch_planner=GitHubFetchPlanner(),
        file_sampler=GitHubFileSampler(),
        url_discovery=GitHubUrlDiscovery(),
    )

    result = await service.handle_job(
        ArtifactEnrichmentJob(
            trigger_event_id=uuid4(),
            event_type="artifact.enrich.requested.v1",
            candidate_group_id=candidate_group_id,
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
        )
    )

    assert result.emitted_snapshot_updated is True
    assert result.status == "ready"
    assert repository.snapshots[0]["plan"].snapshot_type == "github_repo"
    assert repository.repo_children[0]["repo"].repo_full_name == "openai/openai-python"
    assert len(repository.file_samples) == 3
    assert repository.current_updates[0]["artifact_id"] == artifact_id
    assert repository.current_updates[0]["status"] == "ready"
    assert repository.outbox[0]["artifact_id"] == artifact_id
    assert repository.outbox[0]["content_anchor"] == "commit:abc123"
    assert repository.discovered_urls
    assert all(item["draft"].parent_candidate_group_id == candidate_group_id for item in repository.discovered_urls)
