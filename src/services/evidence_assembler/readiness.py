from __future__ import annotations

from .models import BundleMemberDraft, SnapshotRecord


_UNUSABLE_STATES = {"failed_transient", "failed_permanent", "rate_limited", "access_denied", "unsupported"}
_READY_STATES = {"ready", "partial_ready", "low_evidence"}


class ReadinessEvaluator:
    def is_ready_for_analysis(
        self,
        *,
        primary_snapshot: SnapshotRecord | None,
        bundle_members: list[BundleMemberDraft],
        token_budget_profile: str | None,
    ) -> bool:
        if primary_snapshot is None:
            return False
        if not bundle_members:
            return False
        if not token_budget_profile:
            return False
        if primary_snapshot.status in _UNUSABLE_STATES:
            return False
        return primary_snapshot.status in _READY_STATES
