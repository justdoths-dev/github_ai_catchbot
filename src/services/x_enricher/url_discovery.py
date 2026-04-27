from __future__ import annotations

from typing import Any
from uuid import UUID

from services.router_normalizer.canonicalizer import canonicalize_url
from services.router_normalizer.models import ResolvedUrl

from .models import DiscoveredUrlObservationDraft


class XUrlDiscovery:
    def discover(
        self,
        *,
        candidate_group_id: UUID,
        parent_artifact_id: UUID,
        projection: dict[str, Any] | None,
        depth_remaining: int,
    ) -> tuple[list[dict[str, Any]], list[DiscoveredUrlObservationDraft]]:
        if not projection:
            return [], []

        raw_observations: list[ResolvedUrl] = []
        root_post = projection.get("root_post")
        for idx, url in enumerate(_extract_urls_from_post(root_post)):
            raw_observations.append(
                ResolvedUrl(
                    observed_url=url,
                    normalized_url=url,
                    resolved_url=url,
                    source_kind="x_entities",
                    context_path=f"root_post.entities.urls[{idx}]",
                )
            )

        for ref_idx, ref in enumerate(projection.get("referenced_posts") or []):
            if not isinstance(ref, dict):
                continue
            raw_post = ref.get("raw_post")
            for url_idx, url in enumerate(_extract_urls_from_post(raw_post)):
                raw_observations.append(
                    ResolvedUrl(
                        observed_url=url,
                        normalized_url=url,
                        resolved_url=url,
                        source_kind="x_referenced_entities",
                        context_path=f"referenced_posts[{ref_idx}].entities.urls[{url_idx}]",
                    )
                )

        links_json: list[dict[str, Any]] = []
        drafts: list[DiscoveredUrlObservationDraft] = []
        seen: set[tuple[str, str | None]] = set()
        for observation in raw_observations:
            key = (observation.observed_url, observation.context_path)
            if key in seen:
                continue
            seen.add(key)
            artifact = canonicalize_url(observation.resolved_url or observation.normalized_url, observed=observation)
            links_json.append(
                {
                    "observed_url": artifact.observed_url,
                    "normalized_url": artifact.normalized_url,
                    "resolved_url": artifact.resolved_url,
                    "canonical_url": artifact.canonical_url,
                    "classification": artifact.classification,
                    "canonical_id": artifact.canonical_id,
                    "provider_route": artifact.provider_route,
                    "context_path": artifact.context_path,
                    "source_kind": artifact.source_kind,
                }
            )
            drafts.append(
                DiscoveredUrlObservationDraft(
                    parent_candidate_group_id=candidate_group_id,
                    parent_artifact_id=parent_artifact_id,
                    observed_url=observation.observed_url,
                    context_path=observation.context_path or "x_post.entities.urls",
                    discovery_reason="x_post_embedded_link",
                    depth_remaining=depth_remaining,
                )
            )
        return links_json, drafts


def _extract_urls_from_post(post: Any) -> list[str]:
    if not isinstance(post, dict):
        return []
    entities = post.get("entities") or {}
    if not isinstance(entities, dict):
        return []
    urls = entities.get("urls") or []
    result: list[str] = []
    for entry in urls:
        if not isinstance(entry, dict):
            continue
        expanded = entry.get("expanded_url")
        raw = entry.get("url")
        candidate = expanded if isinstance(expanded, str) and expanded.strip() else raw
        if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
            result.append(candidate.strip())
    return result
