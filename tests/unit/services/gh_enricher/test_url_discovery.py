from __future__ import annotations

from uuid import uuid4

from services.gh_enricher.models import GitHubFileSample, GitHubRepoProjection
from services.gh_enricher.url_discovery import GitHubUrlDiscovery


def test_discovers_urls_from_readme_and_sampled_files() -> None:
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    projection = GitHubRepoProjection(
        repo_full_name="openai/openai-python",
        default_branch="main",
        resolved_ref="sha",
        content_anchor_commit_sha="sha",
        repo_flags_json=None,
        license_spdx=None,
        topics_json=None,
        readme_excerpt="See https://example.com/readme.",
        detected_build_systems_json=None,
        detected_languages_json=None,
        key_paths_json=None,
        test_paths_json=None,
        ci_paths_json=None,
        examples_paths_json=None,
        docs_paths_json=None,
        release_summary_json=None,
        sampled_files=[
            GitHubFileSample(
                path="docs/usage.md",
                role="docs",
                size_bytes=20,
                content_hash="hash",
                excerpt="Docs at https://docs.example.dev/start",
            )
        ],
    )

    observations = GitHubUrlDiscovery().discover(
        candidate_group_id=candidate_group_id,
        parent_artifact_id=artifact_id,
        repo_projection=projection,
    )

    assert [item.observed_url for item in observations] == [
        "https://example.com/readme",
        "https://docs.example.dev/start",
    ]
    assert observations[0].parent_candidate_group_id == candidate_group_id
    assert observations[0].parent_artifact_id == artifact_id
    assert observations[0].discovery_reason == "github_readme_embedded_link"
    assert observations[1].context_path == "sampled_files[docs/usage.md].url[0]"
