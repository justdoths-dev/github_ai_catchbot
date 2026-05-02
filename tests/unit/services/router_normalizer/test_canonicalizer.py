from __future__ import annotations

from services.router_normalizer.canonicalizer import canonicalize_resolved_urls
from services.router_normalizer.models import ResolvedUrl


def _url(value: str) -> ResolvedUrl:
    return ResolvedUrl(
        observed_url=value,
        normalized_url=value,
        resolved_url=None,
        source_kind="entity",
    )


def test_canonicalizes_github_repo_root() -> None:
    artifact = canonicalize_resolved_urls([_url("https://github.com/OpenAI/openai-python")])[0]

    assert artifact.artifact_type == "github_repo"
    assert artifact.canonical_id == "github:repo:openai/openai-python"
    assert artifact.provider_route == "github"


def test_canonicalizes_github_repo_root_to_locked_colon_id_contract() -> None:
    artifact = canonicalize_resolved_urls([_url("https://github.com/octocat/Hello-World")])[0]

    assert artifact.artifact_type == "github_repo"
    assert artifact.canonical_id == "github:repo:octocat/hello-world"
    assert artifact.canonical_url == "https://github.com/octocat/hello-world"


def test_canonicalizes_github_subpath_with_inferred_repo_anchor() -> None:
    artifact = canonicalize_resolved_urls(
        [_url("https://github.com/OpenAI/openai-python/blob/main/src/openai/__init__.py")]
    )[0]

    assert artifact.artifact_type == "github_subpath"
    assert artifact.canonical_id == "github:subpath:openai/openai-python:main:src/openai/__init__.py"
    assert artifact.inferred_repo is not None
    assert artifact.inferred_repo.canonical_id == "github:repo:openai/openai-python"


def test_canonicalizes_github_repo_page_with_inferred_repo_anchor() -> None:
    artifact = canonicalize_resolved_urls([_url("https://github.com/OpenAI/openai-python/issues/123")])[0]

    assert artifact.artifact_type == "github_repo_page"
    assert artifact.canonical_id == "github:repo_page:openai/openai-python:issues/123"
    assert artifact.inferred_repo is not None
    assert artifact.inferred_repo.canonical_id == "github:repo:openai/openai-python"


def test_canonicalizes_github_gist() -> None:
    artifact = canonicalize_resolved_urls([_url("https://gist.github.com/alice/ABC123")])[0]

    assert artifact.artifact_type == "github_gist"
    assert artifact.canonical_id == "github:gist:abc123"


def test_canonicalizes_x_post_by_post_id() -> None:
    artifact = canonicalize_resolved_urls([_url("https://x.com/someone/status/1234567890?s=20")])[0]

    assert artifact.artifact_type == "x_post"
    assert artifact.canonical_id == "x:post:1234567890"
    assert artifact.provider_route == "x"
