from __future__ import annotations

import re
from uuid import UUID

from .models import DiscoveredUrlObservationDraft, GitHubRepoProjection


_URL_RE = re.compile(r"https?://[^\s<>()\[\]{}\"']+")


class GitHubUrlDiscovery:
    def discover(
        self,
        *,
        candidate_group_id: UUID,
        parent_artifact_id: UUID,
        repo_projection: GitHubRepoProjection | None,
    ) -> list[DiscoveredUrlObservationDraft]:
        if repo_projection is None:
            return []

        results: list[DiscoveredUrlObservationDraft] = []

        def add(url: str, context_path: str, reason: str) -> None:
            results.append(
                DiscoveredUrlObservationDraft(
                    parent_candidate_group_id=candidate_group_id,
                    parent_artifact_id=parent_artifact_id,
                    observed_url=url.rstrip(".,;:"),
                    context_path=context_path,
                    discovery_reason=reason,
                    depth_remaining=0,
                )
            )

        if repo_projection.readme_excerpt:
            for idx, url in enumerate(_URL_RE.findall(repo_projection.readme_excerpt)):
                add(url, f"readme_excerpt.url[{idx}]", "github_readme_embedded_link")

        for sample in repo_projection.sampled_files:
            if not sample.excerpt:
                continue
            for idx, url in enumerate(_URL_RE.findall(sample.excerpt)):
                add(url, f"sampled_files[{sample.path}].url[{idx}]", f"github_sample_{sample.role}_embedded_link")

        return results
