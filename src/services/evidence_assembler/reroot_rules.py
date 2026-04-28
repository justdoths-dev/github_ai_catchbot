from __future__ import annotations

from .models import CandidateMemberRecord, RerootDecision, SnapshotRecord


_ROLE_PRECEDENCE = {
    "primary": 0,
    "supporting": 1,
    "inferred_anchor": 2,
}
_UNUSABLE_STATES = {"failed_transient", "failed_permanent", "rate_limited", "access_denied", "unsupported"}


class RerootRules:
    READY_REPO_STATES = {"ready", "partial_ready"}

    def decide(
        self,
        *,
        current_primary_artifact_id: object,
        members: list[CandidateMemberRecord],
        current_snapshots: dict[object, SnapshotRecord],
    ) -> RerootDecision:
        artifact_types = {member.artifact_id: member.artifact_type for member in members}
        current_type = artifact_types.get(current_primary_artifact_id)
        if current_type == "github_repo":
            return RerootDecision(False, current_primary_artifact_id, current_primary_artifact_id, None)

        repo_member = self._best_ready_repo_member(members=members, current_snapshots=current_snapshots)
        if repo_member is None:
            return RerootDecision(False, current_primary_artifact_id, current_primary_artifact_id, None)

        current_snapshot = current_snapshots.get(current_primary_artifact_id)
        current_unusable = current_snapshot is None or current_snapshot.status in _UNUSABLE_STATES
        if current_type in {"github_subpath", "github_repo_page"} and current_unusable:
            return RerootDecision(True, current_primary_artifact_id, repo_member.artifact_id, "primary_unusable_repo_anchor")
        if current_type in {"x_post", "web_article", "text_idea"} and current_unusable:
            return RerootDecision(True, current_primary_artifact_id, repo_member.artifact_id, f"{current_type}_unusable_repo_anchor")
        return RerootDecision(False, current_primary_artifact_id, current_primary_artifact_id, None)

    def _best_ready_repo_member(
        self,
        *,
        members: list[CandidateMemberRecord],
        current_snapshots: dict[object, SnapshotRecord],
    ) -> CandidateMemberRecord | None:
        candidates = [
            member
            for member in members
            if member.artifact_type == "github_repo"
            and member.artifact_id in current_snapshots
            and current_snapshots[member.artifact_id].status in self.READY_REPO_STATES
        ]
        if not candidates:
            return None
        return sorted(
            candidates,
            key=lambda member: (
                _ROLE_PRECEDENCE.get(member.member_role, 9),
                member.member_order if member.member_order is not None else 999999,
                str(member.artifact_id),
            ),
        )[0]
