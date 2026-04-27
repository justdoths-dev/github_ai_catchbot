from __future__ import annotations

from typing import Any
from uuid import UUID

from services.router_normalizer.canonicalizer import canonicalize_url
from services.router_normalizer.models import ResolvedUrl

from .models import DiscoveredUrlObservationDraft


class WebUrlDiscovery:
    def discover(
        self,
        *,
        candidate_group_id: UUID,
        parent_artifact_id: UUID,
        outbound_links: list[str],
        depth_remaining: int,
    ) -> tuple[list[dict[str, Any]], list[DiscoveredUrlObservationDraft]]:
        links_json: list[dict[str, Any]] = []
        drafts: list[DiscoveredUrlObservationDraft] = []
        seen: set[tuple[str, str]] = set()
        for idx, url in enumerate(outbound_links):
            context_path = f"web_article.outbound_links[{idx}]"
            key = (url, context_path)
            if key in seen:
                continue
            seen.add(key)
            observed = ResolvedUrl(
                observed_url=url,
                normalized_url=url,
                resolved_url=url,
                source_kind="web_article_anchor",
                context_path=context_path,
            )
            artifact = canonicalize_url(url, observed=observed)
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
                    observed_url=url,
                    context_path=context_path,
                    discovery_reason="web_article_embedded_link",
                    depth_remaining=depth_remaining,
                )
            )
        return links_json, drafts
