from __future__ import annotations

from .models import SnapshotRecord


class TokenBudgetProfiler:
    def choose(
        self,
        *,
        primary_snapshot: SnapshotRecord | None,
        supporting_snapshot_count: int,
        discovered_links_count: int,
    ) -> str:
        if primary_snapshot is None:
            return "small"

        snapshot_type = primary_snapshot.snapshot_type
        if snapshot_type in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
            if supporting_snapshot_count >= 3 or discovered_links_count >= 6:
                return "large"
            if supporting_snapshot_count >= 1 or discovered_links_count >= 2:
                return "medium"
            return "small"
        if snapshot_type == "x_post":
            return "medium" if supporting_snapshot_count >= 2 else "small"
        if snapshot_type in {"web_article", "text_idea"}:
            return "medium" if supporting_snapshot_count >= 2 or discovered_links_count >= 4 else "small"
        return "small"
