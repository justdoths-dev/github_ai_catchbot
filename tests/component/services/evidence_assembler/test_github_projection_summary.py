from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.evidence_assembler.models import BundleRefreshTarget, SnapshotRecord
from services.evidence_assembler.service import EvidenceAssemblerService

from ._fakes import FakeRepository, add_candidate, config


RAW_SOURCE_URL = "https://example.invalid/mcp"
SOURCE_TEXT = f"MCP setup guide: connect and use this repo. See {RAW_SOURCE_URL}."


@pytest.mark.asyncio
async def test_github_primary_summary_includes_sanitized_capped_projection_context() -> None:
    repository = FakeRepository()
    repository.source_text = SOURCE_TEXT
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(
        repository,
        candidate_group_id=candidate_group_id,
        primary_artifact_id=artifact_id,
        artifact_type="github_repo",
    )
    repository.snapshots[artifact_id] = SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="github",
        snapshot_type="github_repo",
        status="ready",
        fetched_at=None,
        content_anchor="commit:abc123",
        normalized_projection={
            "artifact_type": "github_repo",
            "repo_full_name": "example/example-tool",
            "name": "example-tool",
            "title": "Example Tool",
            "description": "Developer workflow helper " + ("with practical setup details " * 20) + RAW_SOURCE_URL,
            "language": "Python",
            "license_spdx": "MIT",
            "stars": 123,
            "forks_count": 4,
            "watchers": 5,
            "open_issues_count": 6,
            "default_branch": "main",
            "pushed_at": "2026-06-01T00:00:00Z",
            "updated_at": "2026-06-02T00:00:00Z",
            "created_at": "2026-05-01T00:00:00Z",
            "topics_json": [f"topic-{index}" for index in range(10)] + [RAW_SOURCE_URL],
            "key_paths_json": ["README.md", "pyproject.toml", "src/example_tool/__init__.py"],
            "config_paths_json": ["config/settings.toml"],
            "docs_paths_json": ["docs/setup.md"],
            "test_paths_json": ["tests/test_example_tool.py"],
            "ci_paths_json": [".github/workflows/test.yml"],
            "file_samples": [{"path": f"sample_{index}.py", "excerpt": RAW_SOURCE_URL} for index in range(10)],
            "detected_build_systems_json": ["python", "uv"],
            "setup_indicators": ["docs/setup.md"],
            "install_indicators": ["pip install example-tool"],
            "repo_flags_json": {"archived": False, "fork": False, "template": False},
            "tree_truncated": False,
            "evidence_limitations": ["readme_excerpt_missing", RAW_SOURCE_URL],
        },
    )
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    await EvidenceAssemblerService(config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    primary_summary = repository.bundles[0][1].primary_summary
    github_context = primary_summary["github_context"]
    assert github_context["repository"] == "example/example-tool"
    assert github_context["name"] == "example-tool"
    assert github_context["language"] == "Python"
    assert github_context["license"] == "MIT"
    assert github_context["stars"] == 123
    assert github_context["forks"] == 4
    assert github_context["watchers"] == 5
    assert github_context["open_issues"] == 6
    assert github_context["default_branch"] == "main"
    assert github_context["pushed_at"] == "2026-06-01"
    assert github_context["updated_at"] == "2026-06-02"
    assert github_context["created_at"] == "2026-05-01"
    assert github_context["readme_present"] is True
    assert github_context["docs_present"] is True
    assert github_context["config_present"] is True
    assert github_context["setup_present"] is True
    assert github_context["install_present"] is True
    assert github_context["file_sample_count"] == 10
    assert github_context["tree_truncated"] is False
    assert github_context["archived"] is False
    assert github_context["fork"] is False
    assert github_context["template"] is False
    assert len(github_context["description"]) <= 240
    assert github_context["description"].endswith("...")
    assert len(github_context["topics"]) == 8
    assert github_context["topics_count"] == 10
    assert github_context["topics_capped"] is True
    assert len(github_context["notable_files"]) == 8
    assert github_context["notable_files_count"] > 8
    assert github_context["notable_files_capped"] is True
    assert github_context["package_tooling"] == ["python", "uv"]
    assert github_context["evidence_limitations"] == ["readme_excerpt_missing"]
    assert primary_summary["source_context_signals"] == {
        "source_text_present": True,
        "source_text_chars_bucket": "1-120",
        "regex_url_count": 1,
        "regex_url_count_capped": False,
        "contains_mcp_token": True,
        "contains_setup_signal": True,
        "contains_connect_signal": True,
        "contains_use_signal": True,
        "signal_count": 4,
    }
    serialized_context = json.dumps(github_context, sort_keys=True)
    assert "http://" not in serialized_context
    assert "https://" not in serialized_context
    assert RAW_SOURCE_URL not in serialized_context


@pytest.mark.asyncio
async def test_non_github_primary_summary_does_not_include_github_context() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(
        repository,
        candidate_group_id=candidate_group_id,
        primary_artifact_id=artifact_id,
        artifact_type="web_article",
    )
    repository.snapshots[artifact_id] = SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="web",
        snapshot_type="web_article",
        status="ready",
        fetched_at=None,
        content_anchor="web:article",
        normalized_projection={"repo_full_name": "example/not-github", "description": "web evidence"},
    )
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    await EvidenceAssemblerService(config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    assert "github_context" not in repository.bundles[0][1].primary_summary


@pytest.mark.asyncio
async def test_github_context_participates_in_bundle_hash_when_snapshot_id_is_unchanged() -> None:
    repository = FakeRepository()
    repository.source_text = SOURCE_TEXT
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    snapshot_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(
        repository,
        candidate_group_id=candidate_group_id,
        primary_artifact_id=artifact_id,
        artifact_type="github_repo",
    )
    repository.snapshots[artifact_id] = SnapshotRecord(
        snapshot_id=snapshot_id,
        artifact_id=artifact_id,
        provider="github",
        snapshot_type="github_repo",
        status="ready",
        fetched_at=None,
        content_anchor="commit:abc123",
        normalized_projection={"repo_full_name": "example/example-tool", "stars": 1},
    )
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]
    service = EvidenceAssemblerService(config(), repository=repository)  # type: ignore[arg-type]

    first = await service.handle_trigger_event(trigger_event_id)
    first_hash = repository.bundles[0][1].bundle_input_hash
    repository.snapshots[artifact_id] = SnapshotRecord(
        snapshot_id=snapshot_id,
        artifact_id=artifact_id,
        provider="github",
        snapshot_type="github_repo",
        status="ready",
        fetched_at=None,
        content_anchor="commit:abc123",
        normalized_projection={"repo_full_name": "example/example-tool", "stars": 2},
    )
    second = await service.handle_trigger_event(trigger_event_id)

    assert first[0].reused_existing_bundle is False
    assert second[0].reused_existing_bundle is False
    assert len(repository.bundles) == 2
    assert repository.bundles[1][1].bundle_input_hash != first_hash
    assert repository.bundles[1][1].primary_summary["github_context"]["stars"] == 2
