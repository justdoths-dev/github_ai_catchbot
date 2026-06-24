from __future__ import annotations

import base64
from uuid import UUID, uuid4

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


class ConflictRepository:
    def __init__(self, artifact: ArtifactRecord, *, existing_status: str) -> None:
        self.artifact = artifact
        self.status_by_key: dict[str, str] = {}
        self.run_key_by_id: dict[UUID, str] = {}
        self.existing_status = existing_status
        self.job_key: str | None = None
        self.insert_calls = 0
        self.claim_calls = 0
        self.status_loads = 0
        self.snapshots = []
        self.outbox = []
        self.started_run_ids = []

    def transaction(self):
        return _Tx()

    async def load_artifact(self, artifact_id):
        assert artifact_id == self.artifact.artifact_id
        return self.artifact

    async def load_current_snapshot(self, snapshot_id):
        assert snapshot_id is None
        return None

    async def insert_enrichment_run_if_absent(self, **kwargs):
        self.insert_calls += 1
        self.job_key = kwargs["job_idempotency_key"]
        self.status_by_key[self.job_key] = self.existing_status
        return None

    async def claim_failed_transient_enrichment_run_for_retry(self, *, job_idempotency_key: str):
        self.claim_calls += 1
        if self.status_by_key.get(job_idempotency_key) != "failed_transient":
            return None
        run_id = uuid4()
        self.status_by_key[job_idempotency_key] = "fetching"
        self.run_key_by_id[run_id] = job_idempotency_key
        return run_id

    async def load_enrichment_run_status_by_job_idempotency_key(self, *, job_idempotency_key: str):
        self.status_loads += 1
        return self.status_by_key.get(job_idempotency_key)

    async def mark_enrichment_run_started(self, run_id):
        self.started_run_ids.append(run_id)

    async def mark_enrichment_run_finished(self, **kwargs):
        run_id = kwargs["run_id"]
        key = self.run_key_by_id[run_id]
        self.status_by_key[key] = kwargs["status"]

    async def insert_snapshot(self, **kwargs):
        self.snapshots.append(kwargs)
        return uuid4()

    async def insert_github_repo_child(self, **kwargs):
        del kwargs

    async def insert_github_file_sample(self, **kwargs):
        del kwargs

    async def insert_discovered_url(self, **kwargs):
        del kwargs

    async def update_artifact_current_snapshot(self, **kwargs):
        del kwargs

    async def insert_snapshot_updated_outbox(self, **kwargs):
        self.outbox.append(kwargs)
        return uuid4()


class FakeGitHubClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_repo(self, owner, repo, *, auth_mode):
        self.calls.append("repo")
        return {
            "full_name": f"{owner}/{repo}",
            "default_branch": "main",
            "description": "fixture",
            "homepage": None,
            "license": {"spdx_id": "MIT"},
            "topics": ["ai"],
            "language": "Python",
            "stargazers_count": 1,
            "subscribers_count": 1,
            "forks_count": 0,
            "open_issues_count": 0,
            "archived": False,
            "fork": False,
            "is_template": False,
        }

    async def get_default_branch_head(self, owner, repo, default_branch, *, auth_mode):
        del owner, repo, default_branch, auth_mode
        self.calls.append("head")
        return {"sha": "abc123"}

    async def get_tree(self, owner, repo, ref, *, recursive, auth_mode):
        del owner, repo, ref, recursive, auth_mode
        self.calls.append("tree")
        return {"truncated": False, "tree": [{"type": "blob", "path": "README.md"}]}

    async def get_contents(self, owner, repo, path, *, ref, auth_mode):
        del owner, repo, ref, auth_mode
        self.calls.append(f"contents:{path}")
        text = "# Fixture\nbounded provider retry\n"
        return {
            "encoding": "base64",
            "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
            "size": len(text),
        }

    async def get_releases(self, owner, repo, *, auth_mode):
        del owner, repo, auth_mode
        self.calls.append("releases")
        return []


class Config:
    sample_max_files = 20
    sample_excerpt_chars = 1200
    max_file_bytes = 131072
    github_app_id = None
    github_installation_id = None


def _artifact(artifact_id: UUID) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type="github_repo",
        canonical_id="github_repo:owner_fixture/repo_fixture",
        canonical_url=None,
        normalized_host="github.com",
        artifact_key_json={"owner": "owner_fixture", "repo": "repo_fixture"},
        current_snapshot_id=None,
        current_status=None,
    )


def _job(artifact_id: UUID) -> ArtifactEnrichmentJob:
    return ArtifactEnrichmentJob(
        trigger_event_id=uuid4(),
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=uuid4(),
        artifact_id=artifact_id,
        artifact_type="github_repo",
        provider_route="github",
        refresh_mode="standard",
        depth_budget=1,
    )


def _service(repository: ConflictRepository, client: FakeGitHubClient) -> GhEnricherService:
    return GhEnricherService(
        Config(),
        repository=repository,
        github_client=client,
        fetch_planner=GitHubFetchPlanner(),
        file_sampler=GitHubFileSampler(),
        url_discovery=GitHubUrlDiscovery(),
    )


@pytest.mark.asyncio
async def test_failed_transient_conflict_without_current_snapshot_retries_and_emits_snapshot_updated() -> None:
    artifact_id = uuid4()
    repository = ConflictRepository(_artifact(artifact_id), existing_status="failed_transient")
    client = FakeGitHubClient()

    result = await _service(repository, client).handle_job(_job(artifact_id))

    assert result.status == "ready"
    assert result.emitted_snapshot_updated is True
    assert result.snapshot_id is not None
    assert repository.insert_calls == 1
    assert repository.claim_calls == 1
    assert repository.status_loads == 0
    assert repository.started_run_ids == []
    assert repository.job_key is not None
    assert repository.status_by_key[repository.job_key] == "ready"
    assert len(repository.snapshots) == 1
    assert len(repository.outbox) == 1
    assert client.calls[:3] == ["repo", "head", "tree"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_status",
    ["pending", "fetching", "failed_permanent", "access_denied", "rate_limited"],
)
async def test_existing_non_retryable_conflict_without_current_snapshot_returns_status_without_github_read(
    existing_status: str,
) -> None:
    artifact_id = uuid4()
    repository = ConflictRepository(_artifact(artifact_id), existing_status=existing_status)
    client = FakeGitHubClient()

    result = await _service(repository, client).handle_job(_job(artifact_id))

    assert result.status == existing_status
    assert result.snapshot_id is None
    assert result.emitted_snapshot_updated is False
    assert repository.insert_calls == 1
    assert repository.claim_calls == 1
    assert repository.status_loads == 1
    assert repository.snapshots == []
    assert repository.outbox == []
    assert client.calls == []
