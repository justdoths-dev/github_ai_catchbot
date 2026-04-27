from __future__ import annotations

import hashlib
import logging
from dataclasses import replace
from uuid import UUID

from .models import ArtifactEnrichmentJob, ArtifactRecord, CurrentSnapshotRef, EnrichmentResult
from .repositories import XEnricherRepository
from .response_mapper import XResponseMapper
from .url_discovery import XUrlDiscovery
from .x_api_client import XAccessDeniedError, XApiClient, XApiClientError, XNotFoundError, XRateLimitedError


class XEnricherService:
    def __init__(
        self,
        config,
        *,
        repository: XEnricherRepository,
        x_api_client: XApiClient,
        response_mapper: XResponseMapper,
        url_discovery: XUrlDiscovery,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._x_api_client = x_api_client
        self._response_mapper = response_mapper
        self._url_discovery = url_discovery
        self._logger = logger or logging.getLogger(__name__)

    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        return await self._repository.load_job_by_trigger_event_id(UUID(trigger_event_id))

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        if job.provider_route != "x" or job.artifact_type != "x_post":
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="unsupported",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )

        artifact = await self._repository.load_artifact(job.artifact_id)
        if artifact is None:
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="failed_permanent",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )
        if artifact.artifact_type != "x_post":
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="unsupported",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )

        post_id = extract_post_id_from_canonical_id(artifact.canonical_id)
        if post_id is None:
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="unsupported",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )

        current_snapshot = await self._repository.load_current_snapshot(artifact.current_snapshot_id)
        snapshot_input_hash = self._build_snapshot_input_hash(job=job, artifact=artifact, current_snapshot=current_snapshot)
        async with self._repository.transaction():
            run_id = await self._repository.insert_enrichment_run_if_absent(
                artifact_id=artifact.artifact_id,
                refresh_mode=job.refresh_mode,
                depth_budget=job.depth_budget,
                status="pending",
                job_idempotency_key=f"enrich:x:{artifact.artifact_id}:{snapshot_input_hash}",
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
            profile = self._x_api_client.default_request_profile()
            payload = await self._x_api_client.get_posts_by_ids(post_ids=[post_id], profile=profile)
        except XRateLimitedError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="rate_limited")
        except XAccessDeniedError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="access_denied")
        except XNotFoundError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="failed_permanent")
        except XApiClientError:
            async with self._repository.transaction():
                await self._repository.mark_enrichment_run_finished(
                    run_id=run_id,
                    status="failed_transient",
                    content_anchor=None,
                )
            raise

        draft = self._response_mapper.map_post_lookup_response(requested_post_id=post_id, payload=payload)
        depth_remaining = max(0, job.depth_budget - 1)
        discovered_links_json, discovered_urls = self._url_discovery.discover(
            candidate_group_id=job.candidate_group_id,
            parent_artifact_id=job.artifact_id,
            projection=draft.normalized_projection,
            depth_remaining=depth_remaining,
        )
        draft = replace(draft, discovered_links_json=discovered_links_json, discovered_urls=discovered_urls)

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
            await self._repository.upsert_x_post_child(snapshot_id=snapshot_id, draft=draft)
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

    def _build_snapshot_input_hash(
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
                artifact.canonical_id,
                job.refresh_mode,
                str(job.depth_budget),
                str(current_snapshot.snapshot_id) if current_snapshot else "none",
                current_snapshot.status if current_snapshot else "none",
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def extract_post_id_from_canonical_id(canonical_id: str) -> str | None:
    if canonical_id.startswith("x:post:"):
        return canonical_id.split("x:post:", 1)[1] or None
    if canonical_id.startswith("x_post:"):
        return canonical_id.split("x_post:", 1)[1] or None
    return None


def _is_current_same_content(current_snapshot: CurrentSnapshotRef | None, content_anchor: str) -> bool:
    if current_snapshot is None:
        return False
    return current_snapshot.content_anchor == content_anchor and current_snapshot.status in {"ready", "partial_ready", "low_evidence"}
