from __future__ import annotations

from typing import Any

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
        if current_type == "x_post" and self._x_snapshot_links_to_repo(current_snapshot, repo_member):
            return RerootDecision(
                True,
                current_primary_artifact_id,
                repo_member.artifact_id,
                "x_post_discovered_github_repo_supporting_reroot",
            )
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

    def _x_snapshot_links_to_repo(
        self,
        current_snapshot: SnapshotRecord | None,
        repo_member: CandidateMemberRecord,
    ) -> bool:
        if current_snapshot is None or current_snapshot.status not in self.READY_REPO_STATES:
            return False
        repo_url = _normalize_url(repo_member.canonical_url)
        if repo_url is None:
            return False
        return repo_url in {_normalize_url(url) for url in _x_projection_urls(current_snapshot.normalized_projection)}


def _x_projection_urls(projection: dict[str, Any] | None) -> list[str]:
    if not projection:
        return []
    urls = _extract_urls_from_post(projection.get("root_post"))
    for referenced in projection.get("referenced_posts") or []:
        if not isinstance(referenced, dict):
            continue
        urls.extend(_extract_urls_from_post(referenced.get("raw_post")))
    return urls


def _extract_urls_from_post(post: Any) -> list[str]:
    if not isinstance(post, dict):
        return []
    entities = post.get("entities") or {}
    if not isinstance(entities, dict):
        return []
    raw_urls = entities.get("urls") or []
    urls: list[str] = []
    for entry in raw_urls:
        if not isinstance(entry, dict):
            continue
        expanded = entry.get("expanded_url")
        raw = entry.get("url")
        candidate = expanded if isinstance(expanded, str) and expanded.strip() else raw
        if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
            urls.append(candidate)
    return urls


def _normalize_url(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().rstrip("/") or None
