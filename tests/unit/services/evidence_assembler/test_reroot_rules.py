from __future__ import annotations

from uuid import uuid4

from services.evidence_assembler.models import CandidateMemberRecord, SnapshotRecord
from services.evidence_assembler.reroot_rules import RerootRules


def _snapshot(artifact_id, snapshot_type="github_repo", status="ready"):
    return SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="test",
        snapshot_type=snapshot_type,
        status=status,
        fetched_at=None,
        content_anchor="anchor",
    )


def test_reroot_to_supporting_repo_when_primary_snapshot_missing() -> None:
    x_post = uuid4()
    repo = uuid4()
    decision = RerootRules().decide(
        current_primary_artifact_id=x_post,
        members=[
            CandidateMemberRecord(x_post, "x_post", "primary", 0),
            CandidateMemberRecord(repo, "github_repo", "supporting", 1),
        ],
        current_snapshots={repo: _snapshot(repo)},
    )

    assert decision.changed is True
    assert decision.to_artifact_id == repo


def test_keep_existing_github_repo_primary() -> None:
    repo = uuid4()
    other_repo = uuid4()
    decision = RerootRules().decide(
        current_primary_artifact_id=repo,
        members=[
            CandidateMemberRecord(repo, "github_repo", "primary", 0),
            CandidateMemberRecord(other_repo, "github_repo", "supporting", 1),
        ],
        current_snapshots={repo: _snapshot(repo), other_repo: _snapshot(other_repo)},
    )

    assert decision.changed is False


def test_reroot_tiebreak_uses_role_order_and_artifact_id() -> None:
    primary = uuid4()
    later = uuid4()
    earlier = uuid4()
    decision = RerootRules().decide(
        current_primary_artifact_id=primary,
        members=[
            CandidateMemberRecord(primary, "web_article", "primary", 0),
            CandidateMemberRecord(later, "github_repo", "supporting", 2),
            CandidateMemberRecord(earlier, "github_repo", "supporting", 1),
        ],
        current_snapshots={later: _snapshot(later), earlier: _snapshot(earlier)},
    )

    assert decision.to_artifact_id == earlier
