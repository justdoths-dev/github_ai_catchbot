from __future__ import annotations

from uuid import uuid4

from services.evidence_assembler.models import BundleMemberDraft, SnapshotRecord
from services.evidence_assembler.readiness import ReadinessEvaluator


def _snapshot(status: str, snapshot_type: str = "github_repo") -> SnapshotRecord:
    artifact_id = uuid4()
    return SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="test",
        snapshot_type=snapshot_type,
        status=status,
        fetched_at=None,
        content_anchor="anchor",
    )


def _member(snapshot: SnapshotRecord) -> BundleMemberDraft:
    return BundleMemberDraft(snapshot.artifact_id, snapshot.snapshot_id, "primary", 0)


def test_not_ready_without_primary_snapshot() -> None:
    assert ReadinessEvaluator().is_ready_for_analysis(
        primary_snapshot=None,
        bundle_members=[],
        token_budget_profile="small",
    ) is False


def test_low_evidence_text_idea_can_be_ready_when_formed() -> None:
    snapshot = _snapshot("low_evidence", "text_idea")
    assert ReadinessEvaluator().is_ready_for_analysis(
        primary_snapshot=snapshot,
        bundle_members=[_member(snapshot)],
        token_budget_profile="small",
    ) is True


def test_low_evidence_web_article_is_not_ready() -> None:
    snapshot = _snapshot("low_evidence", "web_article")
    assert ReadinessEvaluator().is_ready_for_analysis(
        primary_snapshot=snapshot,
        bundle_members=[_member(snapshot)],
        token_budget_profile="small",
    ) is False


def test_low_evidence_x_post_is_not_ready() -> None:
    snapshot = _snapshot("low_evidence", "x_post")
    assert ReadinessEvaluator().is_ready_for_analysis(
        primary_snapshot=snapshot,
        bundle_members=[_member(snapshot)],
        token_budget_profile="small",
    ) is False


def test_ready_and_partial_ready_non_text_primary_remain_ready() -> None:
    evaluator = ReadinessEvaluator()
    for status in ("ready", "partial_ready"):
        snapshot = _snapshot(status, "web_article")
        assert evaluator.is_ready_for_analysis(
            primary_snapshot=snapshot,
            bundle_members=[_member(snapshot)],
            token_budget_profile="small",
        ) is True


def test_failed_and_empty_bundle_are_not_ready() -> None:
    failed = _snapshot("failed_permanent")
    ready = _snapshot("ready")

    assert ReadinessEvaluator().is_ready_for_analysis(
        primary_snapshot=failed,
        bundle_members=[_member(failed)],
        token_budget_profile="small",
    ) is False
    assert ReadinessEvaluator().is_ready_for_analysis(
        primary_snapshot=ready,
        bundle_members=[],
        token_budget_profile="small",
    ) is False
