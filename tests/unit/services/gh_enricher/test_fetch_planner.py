from __future__ import annotations

from uuid import uuid4

from services.gh_enricher.fetch_planner import GitHubFetchPlanner
from services.gh_enricher.models import ArtifactRecord


def _artifact(artifact_type: str, key: dict, canonical_url: str | None = None) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=uuid4(),
        artifact_type=artifact_type,
        canonical_id=f"{artifact_type}:example",
        canonical_url=canonical_url,
        normalized_host="github.com",
        artifact_key_json=key,
        current_snapshot_id=None,
        current_status=None,
    )


def test_builds_repo_locator_from_artifact_key_json() -> None:
    locator = GitHubFetchPlanner().build_locator(_artifact("github_repo", {"owner": "OpenAI", "repo": "openai-python"}))

    assert locator.artifact_type == "github_repo"
    assert locator.owner == "OpenAI"
    assert locator.repo == "openai-python"


def test_builds_subpath_locator_with_ref_and_path() -> None:
    locator = GitHubFetchPlanner().build_locator(
        _artifact(
            "github_subpath",
            {"owner": "OpenAI", "repo": "openai-python", "ref": "main", "path": "src/openai/__init__.py"},
        )
    )

    assert locator.artifact_type == "github_subpath"
    assert locator.ref == "main"
    assert locator.path == "src/openai/__init__.py"


def test_builds_repo_page_locator() -> None:
    locator = GitHubFetchPlanner().build_locator(
        _artifact("github_repo_page", {"owner": "OpenAI", "repo": "openai-python", "page_path": "issues/1"})
    )

    assert locator.artifact_type == "github_repo_page"
    assert locator.page_path == "issues/1"


def test_builds_gist_locator() -> None:
    locator = GitHubFetchPlanner().build_locator(_artifact("github_gist", {"gist_id": "abc123"}))

    assert locator.artifact_type == "github_gist"
    assert locator.gist_id == "abc123"


def test_falls_back_to_canonical_url_for_repo_identity() -> None:
    locator = GitHubFetchPlanner().build_locator(
        _artifact("github_repo", {}, canonical_url="https://github.com/OpenAI/openai-python")
    )

    assert locator.owner == "OpenAI"
    assert locator.repo == "openai-python"
