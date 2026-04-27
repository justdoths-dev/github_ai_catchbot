from __future__ import annotations

import hashlib
import logging
from dataclasses import replace
from uuid import UUID

from .article_parser import ArticleParser
from .models import ArtifactEnrichmentJob, ArtifactRecord, CurrentSnapshotRef, EnrichmentResult, WebArticleSnapshotDraft
from .repositories import WebEnricherRepository
from .url_discovery import WebUrlDiscovery
from .web_fetch_client import (
    UnsupportedContentTypeError,
    WebAccessDeniedError,
    WebFetchClient,
    WebFetchClientError,
    WebNotFoundError,
    WebPermanentFetchError,
    WebRateLimitedError,
    WebTransientFetchError,
)


class WebEnricherService:
    def __init__(
        self,
        config,
        *,
        repository: WebEnricherRepository,
        fetch_client: WebFetchClient,
        article_parser: ArticleParser,
        url_discovery: WebUrlDiscovery,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._fetch_client = fetch_client
        self._article_parser = article_parser
        self._url_discovery = url_discovery
        self._logger = logger or logging.getLogger(__name__)

    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        return await self._repository.load_job_by_trigger_event_id(UUID(trigger_event_id))

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        if job.provider_route != "web" or job.artifact_type != "web_article":
            return _result(job, "unsupported")

        artifact = await self._repository.load_artifact(job.artifact_id)
        if artifact is None:
            return _result(job, "failed_permanent")
        if artifact.artifact_type != "web_article":
            return _result(job, "unsupported")
        if not artifact.canonical_url or not artifact.canonical_url.startswith(("http://", "https://")):
            return _result(job, "unsupported")

        current_snapshot = await self._repository.load_current_snapshot(artifact.current_snapshot_id)
        input_hash = self._build_run_input_hash(job=job, artifact=artifact, current_snapshot=current_snapshot)
        async with self._repository.transaction():
            run_id = await self._repository.insert_enrichment_run_if_absent(
                artifact_id=artifact.artifact_id,
                refresh_mode=job.refresh_mode,
                depth_budget=job.depth_budget,
                status="pending",
                job_idempotency_key=f"enrich:web:{artifact.artifact_id}:{input_hash}",
                content_anchor=None,
            )
            if run_id is not None:
                await self._repository.mark_enrichment_run_started(run_id)

        if run_id is None:
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=current_snapshot.snapshot_id if current_snapshot else None,
                status=current_snapshot.status if current_snapshot else "pending",  # type: ignore[arg-type]
                content_anchor=current_snapshot.content_anchor if current_snapshot else None,
                emitted_snapshot_updated=False,
            )

        try:
            fetched = await self._fetch_client.fetch(artifact.canonical_url)
        except WebRateLimitedError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="rate_limited")
        except WebAccessDeniedError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="access_denied")
        except WebNotFoundError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="failed_permanent")
        except UnsupportedContentTypeError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="unsupported")
        except WebPermanentFetchError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="failed_permanent")
        except (WebTransientFetchError, WebFetchClientError):
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="failed_transient")

        parsed = self._article_parser.parse(
            final_url=fetched.final_url,
            content_type=fetched.content_type,
            body_text=fetched.body_text,
        )
        content_anchor = compute_content_anchor(final_url=fetched.final_url, content_hash=fetched.content_hash)
        status, evidence_limitations = classify_snapshot_status(
            title=parsed.title,
            description=parsed.description,
            excerpt=parsed.main_text_excerpt,
            outbound_links=parsed.outbound_links,
            content_type=fetched.content_type,
        )
        links_json, discovered_urls = self._url_discovery.discover(
            candidate_group_id=job.candidate_group_id,
            parent_artifact_id=job.artifact_id,
            outbound_links=parsed.outbound_links,
            depth_remaining=max(0, job.depth_budget - 1),
        )
        draft = WebArticleSnapshotDraft(
            snapshot_type="web_article",
            status=status,
            content_anchor=content_anchor,
            auth_mode="anonymous_public",
            normalized_projection={
                "start_url": artifact.canonical_url,
                "final_url": fetched.final_url,
                "canonical_url_candidate": parsed.canonical_url_candidate,
                "response_headers_subset": fetched.response_headers_subset,
                "content_type": fetched.content_type,
                "content_length_observed": len(fetched.body_bytes),
                "content_anchor": content_anchor,
                "parser_projection": parsed.normalized_projection,
            },
            raw_payload_ref=None,
            evidence_limitations=evidence_limitations,
            fetch_anomalies=fetched.fetch_anomalies,
            final_url=fetched.final_url,
            canonical_url_candidate=parsed.canonical_url_candidate,
            site_name=parsed.site_name,
            title=parsed.title,
            description=parsed.description,
            author=parsed.author,
            published_at=parsed.published_at,
            content_hash=fetched.content_hash,
            main_text_excerpt=parsed.main_text_excerpt,
            outbound_links_json=links_json,
            discovered_urls=discovered_urls,
        )

        if _is_current_same_content(current_snapshot, draft.content_anchor):
            async with self._repository.transaction():
                await self._repository.mark_enrichment_run_finished(
                    run_id=run_id,
                    status=draft.status,
                    content_anchor=draft.content_anchor,
                )
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=current_snapshot.snapshot_id if current_snapshot else None,
                status=draft.status,
                content_anchor=draft.content_anchor,
                emitted_snapshot_updated=False,
            )

        async with self._repository.transaction():
            snapshot_id = await self._repository.insert_snapshot(artifact_id=artifact.artifact_id, draft=draft)
            await self._repository.upsert_web_article_child(snapshot_id=snapshot_id, draft=draft)
            for discovered in draft.discovered_urls:
                await self._repository.insert_discovered_url(snapshot_id=snapshot_id, draft=discovered)
            await self._repository.update_artifact_current_snapshot(
                artifact_id=artifact.artifact_id,
                snapshot_id=snapshot_id,
                status=draft.status,
            )
            await self._repository.insert_snapshot_updated_outbox(
                artifact_id=artifact.artifact_id,
                candidate_group_id=job.candidate_group_id,
                snapshot_id=snapshot_id,
                status=draft.status,
                content_anchor=draft.content_anchor,
            )
            await self._repository.mark_enrichment_run_finished(
                run_id=run_id,
                status=draft.status,
                content_anchor=draft.content_anchor,
            )

        return EnrichmentResult(
            artifact_id=artifact.artifact_id,
            snapshot_id=snapshot_id,
            status=draft.status,
            content_anchor=draft.content_anchor,
            emitted_snapshot_updated=True,
        )

    async def _finish_failed_run(self, *, artifact: ArtifactRecord, run_id: UUID, status: str) -> EnrichmentResult:
        async with self._repository.transaction():
            await self._repository.mark_enrichment_run_finished(run_id=run_id, status=status, content_anchor=None)
        return EnrichmentResult(
            artifact_id=artifact.artifact_id,
            snapshot_id=None,
            status=status,  # type: ignore[arg-type]
            content_anchor=None,
            emitted_snapshot_updated=False,
        )

    def _build_run_input_hash(
        self,
        *,
        job: ArtifactEnrichmentJob,
        artifact: ArtifactRecord,
        current_snapshot: CurrentSnapshotRef | None,
    ) -> str:
        raw = "|".join(
            [
                str(artifact.artifact_id),
                artifact.artifact_type,
                artifact.canonical_url or "",
                job.refresh_mode,
                str(job.depth_budget),
                str(current_snapshot.snapshot_id) if current_snapshot else "none",
                current_snapshot.status if current_snapshot else "none",
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def compute_content_anchor(*, final_url: str, content_hash: str) -> str:
    return "web:" + hashlib.sha256(f"{final_url}|{content_hash}".encode("utf-8")).hexdigest()


def classify_snapshot_status(
    *,
    title: str | None,
    description: str | None,
    excerpt: str | None,
    outbound_links: list[str],
    content_type: str | None,
) -> tuple[str, list[str]]:
    limitations: list[str] = []
    if not excerpt and not outbound_links:
        return "low_evidence", ["web_text_and_links_missing"]
    if title and excerpt and outbound_links:
        status = "ready"
    else:
        status = "partial_ready"
        limitations.append("web_metadata_sparse")
    if not description:
        limitations.append("web_description_missing")
    if not outbound_links:
        limitations.append("web_outbound_links_missing")
    if content_type in {"text/plain", "text/markdown"}:
        status = "partial_ready" if status == "ready" else status
        limitations.append("web_plain_text_mode")
    return status, limitations


def _is_current_same_content(current_snapshot: CurrentSnapshotRef | None, content_anchor: str) -> bool:
    if current_snapshot is None:
        return False
    return (
        current_snapshot.provider == "web"
        and current_snapshot.snapshot_type == "web_article"
        and current_snapshot.content_anchor == content_anchor
        and current_snapshot.status in {"ready", "partial_ready", "low_evidence"}
    )


def _result(job: ArtifactEnrichmentJob, status: str) -> EnrichmentResult:
    return EnrichmentResult(
        artifact_id=job.artifact_id,
        snapshot_id=None,
        status=status,  # type: ignore[arg-type]
        content_anchor=None,
        emitted_snapshot_updated=False,
    )
