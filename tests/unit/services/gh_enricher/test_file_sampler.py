from __future__ import annotations

import base64

from services.gh_enricher.file_sampler import GitHubFileSampler


def test_select_paths_uses_role_priority_and_max_files() -> None:
    sampler = GitHubFileSampler()
    paths = sampler.select_paths(
        [
            {"type": "blob", "path": "tests/test_app.py"},
            {"type": "blob", "path": "docs/usage.md"},
            {"type": "blob", "path": "package.json"},
            {"type": "blob", "path": "README.md"},
            {"type": "blob", "path": ".github/workflows/ci.yml"},
        ],
        max_files=3,
    )

    assert [(path.role, path.path) for path in paths] == [
        ("README", "README.md"),
        ("manifest", "package.json"),
        ("ci", ".github/workflows/ci.yml"),
    ]


def test_build_sample_hashes_and_truncates_excerpt() -> None:
    sample = GitHubFileSampler().build_sample(
        path="README.md",
        role="README",
        raw_text="abcdef",
        size_bytes=6,
        excerpt_chars=3,
    )

    assert sample.excerpt == "abc"
    assert sample.content_hash is not None
    assert sample.size_bytes == 6


def test_decode_contents_response_base64() -> None:
    payload = {"encoding": "base64", "content": base64.b64encode("hello".encode()).decode()}

    assert GitHubFileSampler.decode_contents_response(payload) == "hello"
