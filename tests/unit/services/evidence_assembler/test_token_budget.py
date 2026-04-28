from __future__ import annotations

from uuid import uuid4

from services.evidence_assembler.models import SnapshotRecord
from services.evidence_assembler.token_budget import TokenBudgetProfiler


def _snapshot(snapshot_type: str) -> SnapshotRecord:
    artifact_id = uuid4()
    return SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="test",
        snapshot_type=snapshot_type,
        status="ready",
        fetched_at=None,
        content_anchor="anchor",
    )


def test_token_budget_profiles_are_deterministic() -> None:
    profiler = TokenBudgetProfiler()

    assert profiler.choose(primary_snapshot=_snapshot("github_repo"), supporting_snapshot_count=0, discovered_links_count=0) == "small"
    assert profiler.choose(primary_snapshot=_snapshot("github_repo"), supporting_snapshot_count=1, discovered_links_count=0) == "medium"
    assert profiler.choose(primary_snapshot=_snapshot("github_repo"), supporting_snapshot_count=3, discovered_links_count=0) == "large"
    assert profiler.choose(primary_snapshot=_snapshot("web_article"), supporting_snapshot_count=2, discovered_links_count=0) == "medium"
