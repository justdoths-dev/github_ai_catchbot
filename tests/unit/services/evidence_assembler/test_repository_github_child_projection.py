from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.evidence_assembler.repositories import EvidenceAssemblerRepository


class FakeExecuteResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> "FakeExecuteResult":
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeAsyncSession:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.execute_calls: list[tuple[Any, dict[str, Any] | None]] = []

    def in_transaction(self) -> bool:
        return False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeExecuteResult:
        self.execute_calls.append((statement, params))
        return FakeExecuteResult(self._rows)


def _row(**overrides: Any) -> dict[str, Any]:
    artifact_id = uuid4()
    snapshot_id = uuid4()
    row: dict[str, Any] = {
        "artifact_id": str(artifact_id),
        "snapshot_id": str(snapshot_id),
        "provider": "github",
        "snapshot_type": "github_repo",
        "status": "ready",
        "fetched_at": datetime.now(timezone.utc),
        "content_anchor": "commit:abc123",
        "auth_mode": "anonymous_degraded",
        "normalized_projection": {
            "artifact_type": "github_repo",
            "description": "thin parent projection",
            "stars": 7,
        },
        "evidence_limitations": ["github_app_auth_unavailable; using anonymous public API mode"],
        "fetch_anomalies": [],
        "github_repo_snapshot_id": str(snapshot_id),
        "github_repo_full_name": "owner_fixture/repo_fixture",
        "github_default_branch": "main",
        "github_resolved_ref": "abc123",
        "github_content_anchor_commit_sha": "abc123",
        "github_repo_flags_json": {"archived": False, "fork": False, "template": False},
        "github_license_spdx": "MIT",
        "github_topics_json": ["ai", "developer-tools"],
        "github_readme_present": True,
        "github_readme_excerpt": "README child excerpt with setup and usage details.",
        "github_detected_build_systems_json": ["python"],
        "github_detected_languages_json": ["Python"],
        "github_key_paths_json": ["README.md", "pyproject.toml"],
        "github_test_paths_json": [],
        "github_ci_paths_json": [".github/workflows/test.yml"],
        "github_examples_paths_json": [],
        "github_docs_paths_json": [],
        "github_release_summary_json": {
            "release_count_recent": 1,
            "latest_release_published_at": "2026-06-01T00:00:00Z",
            "has_release_assets": True,
        },
        "github_file_sample_count": 2,
        "github_file_samples_json": [
            {"path": "README.md", "role": "README", "excerpt": "README file excerpt"},
            {"path": "pyproject.toml", "role": "manifest", "excerpt": "Manifest file excerpt"},
        ],
        "github_file_sample_roles_json": ["README", "manifest"],
    }
    row.update(overrides)
    return row


@pytest.mark.asyncio
async def test_load_current_snapshots_merges_sanitized_github_child_projection() -> None:
    row = _row()
    artifact_id = UUID(row["artifact_id"])
    session = FakeAsyncSession([row])
    repository = EvidenceAssemblerRepository(session)

    snapshots = await repository.load_current_snapshots([artifact_id])

    snapshot = snapshots[artifact_id]
    projection = snapshot.normalized_projection
    assert projection is not None
    assert projection["description"] == "thin parent projection"
    assert projection["stars"] == 7
    assert projection["repo_full_name"] == "owner_fixture/repo_fixture"
    assert projection["default_branch"] == "main"
    assert projection["readme_present"] is True
    assert projection["readme_excerpt"] == "README child excerpt with setup and usage details."
    assert projection["detected_build_systems_json"] == ["python"]
    assert projection["detected_languages_json"] == ["Python"]
    assert projection["key_paths_json"] == ["README.md", "pyproject.toml"]
    assert projection["config_paths_json"] == ["pyproject.toml"]
    assert projection["file_sample_count"] == 2
    assert projection["file_sample_roles"] == ["README", "manifest"]
    assert projection["file_samples"] == [
        {"path": "README.md", "role": "README", "excerpt": "README file excerpt"},
        {"path": "pyproject.toml", "role": "manifest", "excerpt": "Manifest file excerpt"},
    ]
    assert projection["release_summary_json"]["release_count_recent"] == 1
    assert projection["auth_mode"] == "anonymous_degraded"
    assert projection["evidence_limitations"] == [
        "github_app_auth_unavailable; using anonymous public API mode"
    ]
    assert snapshot.evidence_limitations == [
        "github_app_auth_unavailable; using anonymous public API mode"
    ]

    serialized_projection = json.dumps(projection, sort_keys=True)
    assert "raw_blob_ref" not in serialized_projection
    assert "content_hash" not in serialized_projection

    statement = str(session.execute_calls[0][0])
    assert "artifact_snapshot_github_repo" in statement
    assert "artifact_snapshot_github_file_samples" in statement
    assert "readme_excerpt" in statement
    assert "excerpt" in statement
    assert "raw_blob_ref" not in statement
    assert "content_hash" not in statement
    assert "size_bytes" not in statement
    assert session.execute_calls[0][1] == {"artifact_ids": [str(artifact_id)]}


@pytest.mark.asyncio
async def test_load_current_snapshots_keeps_non_github_projection_unmerged() -> None:
    row = _row(
        provider="web",
        snapshot_type="web_article",
        auth_mode="public_web",
        normalized_projection={"repo_full_name": "example/not-github", "description": "web evidence"},
        evidence_limitations=["web_low_evidence"],
        github_repo_snapshot_id=None,
        github_repo_full_name=None,
        github_default_branch=None,
        github_readme_excerpt=None,
        github_readme_present=None,
        github_file_sample_count=0,
        github_file_samples_json=None,
        github_file_sample_roles_json=None,
    )
    artifact_id = UUID(row["artifact_id"])
    repository = EvidenceAssemblerRepository(FakeAsyncSession([row]))

    snapshots = await repository.load_current_snapshots([artifact_id])

    assert snapshots[artifact_id].normalized_projection == {
        "repo_full_name": "example/not-github",
        "description": "web evidence",
    }
