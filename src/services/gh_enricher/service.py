from __future__ import annotations

import hashlib
import logging
from typing import Any

from .fetch_planner import GitHubFetchPlanner
from .file_sampler import GitHubFileSampler
from .github_client import GitHubAccessDeniedError, GitHubClient, GitHubNotFoundError, GitHubRateLimitedError
from .models import (
    ArtifactEnrichmentJob,
    ArtifactRecord,
    CurrentSnapshotRef,
    EnrichmentResult,
    GitHubRepoProjection,
    SnapshotWritePlan,
)
from .repositories import GhEnricherRepository
from .url_discovery import GitHubUrlDiscovery


SUPPORTED_GITHUB_ARTIFACT_TYPES = {"github_repo", "github_subpath", "github_repo_page", "github_gist"}


class GhEnricherService:
    def __init__(
        self,
        config: Any,
        *,
        repository: GhEnricherRepository,
        github_client: GitHubClient,
        fetch_planner: GitHubFetchPlanner,
        file_sampler: GitHubFileSampler,
        url_discovery: GitHubUrlDiscovery,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._github_client = github_client
        self._fetch_planner = fetch_planner
        self._file_sampler = file_sampler
        self._url_discovery = url_discovery
        self._logger = logger or logging.getLogger(__name__)

    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        from uuid import UUID

        return await self._repository.load_job_by_trigger_event_id(UUID(trigger_event_id))

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        if job.provider_route != "github" or job.artifact_type not in SUPPORTED_GITHUB_ARTIFACT_TYPES:
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
        if artifact.artifact_type != job.artifact_type:
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="unsupported",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )

        current_snapshot = await self._repository.load_current_snapshot(artifact.current_snapshot_id)
        if self._should_short_circuit(job=job, artifact=artifact, current_snapshot=current_snapshot):
            async with self._repository.transaction():
                await self._repository.insert_snapshot_updated_outbox(
                    artifact_id=artifact.artifact_id,
                    snapshot_id=current_snapshot.snapshot_id,
                    status=current_snapshot.status,
                    content_anchor=current_snapshot.content_anchor,
                )
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=current_snapshot.snapshot_id,
                status=current_snapshot.status,  # type: ignore[arg-type]
                content_anchor=current_snapshot.content_anchor,
                emitted_snapshot_updated=True,
            )

        snapshot_input_hash = self._build_snapshot_input_hash(job=job, artifact=artifact, current_snapshot=current_snapshot)
        job_idempotency_key = f"enrich:github:{artifact.artifact_id}:{snapshot_input_hash}"
        async with self._repository.transaction():
            run_id = await self._repository.insert_enrichment_run_if_absent(
                artifact_id=artifact.artifact_id,
                provider="github",
                refresh_mode=job.refresh_mode,
                depth_budget=job.depth_budget,
                status="pending",
                job_idempotency_key=job_idempotency_key,
                content_anchor=None,
            )
            if run_id is not None:
                await self._repository.mark_enrichment_run_started(run_id)
            elif current_snapshot is None:
                run_id = await self._repository.claim_failed_transient_enrichment_run_for_retry(
                    job_idempotency_key=job_idempotency_key,
                )

        if run_id is None:
            existing_status = None
            if current_snapshot is None:
                existing_run = await self._repository.load_enrichment_run_by_job_idempotency_key(
                    job_idempotency_key=job_idempotency_key,
                )
                existing_status = None if existing_run is None else existing_run.status
                if existing_run is not None and existing_run.status in {"pending", "fetching"}:
                    recovered = await self._recover_orphan_snapshot(
                        artifact=artifact,
                        run_id=existing_run.run_id,
                    )
                    if recovered is not None:
                        return recovered
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=current_snapshot.snapshot_id if current_snapshot else None,
                status=(
                    current_snapshot.status
                    if current_snapshot
                    else existing_status or "pending"
                ),  # type: ignore[arg-type]
                content_anchor=current_snapshot.content_anchor if current_snapshot else None,
                emitted_snapshot_updated=False,
            )

        try:
            plan = await self._build_snapshot_plan(job=job, artifact=artifact)
        except GitHubRateLimitedError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="rate_limited")
        except GitHubAccessDeniedError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="access_denied")
        except GitHubNotFoundError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="failed_permanent")
        except ValueError:
            return await self._finish_failed_run(artifact=artifact, run_id=run_id, status="unsupported")
        except Exception:
            async with self._repository.transaction():
                await self._repository.mark_enrichment_run_finished(
                    run_id=run_id,
                    status="failed_transient",
                    content_anchor=None,
                )
            raise

        async with self._repository.transaction():
            snapshot_id = await self._repository.insert_snapshot(
                artifact_id=artifact.artifact_id,
                provider="github",
                plan=plan,
            )
            if plan.repo_child is not None:
                await self._repository.insert_github_repo_child(snapshot_id=snapshot_id, repo=plan.repo_child)
                for sample in plan.repo_child.sampled_files:
                    await self._repository.insert_github_file_sample(snapshot_id=snapshot_id, sample=sample)
            for draft in plan.discovered_urls:
                await self._repository.insert_discovered_url(snapshot_id=snapshot_id, draft=draft)
            await self._repository.update_artifact_current_snapshot(
                artifact_id=artifact.artifact_id,
                snapshot_id=snapshot_id,
                status=plan.status,
            )
            await self._repository.insert_snapshot_updated_outbox(
                artifact_id=artifact.artifact_id,
                snapshot_id=snapshot_id,
                status=plan.status,
                content_anchor=plan.content_anchor,
            )
            await self._repository.mark_enrichment_run_finished(
                run_id=run_id,
                status=plan.status,
                content_anchor=plan.content_anchor,
            )

        return EnrichmentResult(
            artifact_id=artifact.artifact_id,
            snapshot_id=snapshot_id,
            status=plan.status,
            content_anchor=plan.content_anchor,
            emitted_snapshot_updated=True,
        )

    async def _finish_failed_run(self, *, artifact: ArtifactRecord, run_id, status: str) -> EnrichmentResult:
        async with self._repository.transaction():
            await self._repository.mark_enrichment_run_finished(run_id=run_id, status=status, content_anchor=None)
        return EnrichmentResult(
            artifact_id=artifact.artifact_id,
            snapshot_id=None,
            status=status,  # type: ignore[arg-type]
            content_anchor=None,
            emitted_snapshot_updated=False,
        )

    async def _recover_orphan_snapshot(
        self,
        *,
        artifact: ArtifactRecord,
        run_id,
    ) -> EnrichmentResult | None:
        snapshots = await self._repository.load_valid_orphan_provider_snapshots(
            artifact_id=artifact.artifact_id,
            provider="github",
            limit=2,
        )
        if not snapshots:
            return None
        if len(snapshots) > 1:
            return EnrichmentResult(
                artifact_id=artifact.artifact_id,
                snapshot_id=None,
                status="failed_permanent",
                content_anchor=None,
                emitted_snapshot_updated=False,
                error_code="multiple_orphan_github_provider_snapshots",
            )

        snapshot = snapshots[0]
        async with self._repository.transaction():
            await self._repository.update_artifact_current_snapshot(
                artifact_id=artifact.artifact_id,
                snapshot_id=snapshot.snapshot_id,
                status=snapshot.status,
            )
            await self._repository.insert_snapshot_updated_outbox(
                artifact_id=artifact.artifact_id,
                snapshot_id=snapshot.snapshot_id,
                status=snapshot.status,
                content_anchor=snapshot.content_anchor,
            )
            await self._repository.mark_enrichment_run_finished(
                run_id=run_id,
                status=snapshot.status,
                content_anchor=snapshot.content_anchor,
            )

        return EnrichmentResult(
            artifact_id=artifact.artifact_id,
            snapshot_id=snapshot.snapshot_id,
            status=snapshot.status,  # type: ignore[arg-type]
            content_anchor=snapshot.content_anchor,
            emitted_snapshot_updated=True,
        )

    def _should_short_circuit(
        self,
        *,
        job: ArtifactEnrichmentJob,
        artifact: ArtifactRecord,
        current_snapshot: CurrentSnapshotRef | None,
    ) -> bool:
        if current_snapshot is None:
            return False
        if job.refresh_mode != "standard":
            return False
        return artifact.current_status in {"ready", "partial_ready"} and current_snapshot.status in {"ready", "partial_ready"}

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
                job.refresh_mode,
                str(job.depth_budget),
                str(current_snapshot.snapshot_id) if current_snapshot else "none",
                current_snapshot.status if current_snapshot else "none",
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    async def _build_snapshot_plan(self, *, job: ArtifactEnrichmentJob, artifact: ArtifactRecord) -> SnapshotWritePlan:
        locator = self._fetch_planner.build_locator(artifact)
        auth_mode = self._initial_auth_mode()

        if locator.artifact_type == "github_gist":
            if locator.gist_id is None:
                raise ValueError("github_gist locator missing gist_id")
            gist_payload = await self._github_client.get_gist(locator.gist_id, auth_mode=auth_mode)
            files = gist_payload.get("files") or {}
            file_values = [value for value in files.values() if isinstance(value, dict)] if isinstance(files, dict) else []
            normalized_projection = {
                "gist_id": locator.gist_id,
                "description": gist_payload.get("description"),
                "public": gist_payload.get("public"),
                "truncated": any(bool(file_obj.get("truncated")) for file_obj in file_values),
                "file_names": sorted(files.keys()) if isinstance(files, dict) else [],
                "language_set": sorted({str(file_obj.get("language")) for file_obj in file_values if file_obj.get("language")}),
                "owner_login": ((gist_payload.get("owner") or {}).get("login") if isinstance(gist_payload.get("owner"), dict) else None),
            }
            return SnapshotWritePlan(
                snapshot_type="github_gist",
                status="partial_ready",
                content_anchor=f"gist:{locator.gist_id}",
                auth_mode=auth_mode,
                normalized_projection=normalized_projection,
                raw_payload_ref=None,
                evidence_limitations=["gist child snapshot schema is deferred; using parent normalized_projection only"],
                fetch_anomalies=[],
                repo_child=None,
                discovered_urls=[],
            )

        if locator.owner is None or locator.repo is None:
            raise ValueError("github repo locator missing owner/repo")

        repo_payload = await self._github_client.get_repo(locator.owner, locator.repo, auth_mode=auth_mode)
        default_branch = str(repo_payload.get("default_branch") or "HEAD")
        head_payload = await self._github_client.get_default_branch_head(locator.owner, locator.repo, default_branch, auth_mode=auth_mode)
        commit_sha = str(head_payload.get("sha") or "") if isinstance(head_payload, dict) else ""
        tree_ref = commit_sha or default_branch
        tree_payload = await self._github_client.get_tree(locator.owner, locator.repo, tree_ref, recursive=True, auth_mode=auth_mode)
        tree_entries = tree_payload.get("tree") if isinstance(tree_payload, dict) else []
        if not isinstance(tree_entries, list):
            tree_entries = []

        sampled_files = []
        readme_excerpt = None
        for candidate in self._file_sampler.select_paths(tree_entries, max_files=self._config.sample_max_files):
            try:
                contents_payload = await self._github_client.get_contents(
                    locator.owner,
                    locator.repo,
                    candidate.path,
                    ref=tree_ref,
                    auth_mode=auth_mode,
                )
            except Exception:
                continue
            if not isinstance(contents_payload, dict):
                continue
            raw_text = self._file_sampler.decode_contents_response(contents_payload)
            if len(raw_text.encode("utf-8")) > self._config.max_file_bytes:
                raw_text = raw_text[: self._config.sample_excerpt_chars]
            sample = self._file_sampler.build_sample(
                path=candidate.path,
                role=candidate.role,
                raw_text=raw_text,
                size_bytes=contents_payload.get("size"),
                excerpt_chars=self._config.sample_excerpt_chars,
            )
            sampled_files.append(sample)
            if candidate.role == "README" and readme_excerpt is None:
                readme_excerpt = sample.excerpt

        releases = await self._github_client.get_releases(locator.owner, locator.repo, auth_mode=auth_mode)
        repo_projection = GitHubRepoProjection(
            repo_full_name=str(repo_payload.get("full_name") or f"{locator.owner}/{locator.repo}"),
            default_branch=default_branch,
            resolved_ref=tree_ref,
            content_anchor_commit_sha=commit_sha or None,
            repo_flags_json={
                "archived": bool(repo_payload.get("archived", False)),
                "fork": bool(repo_payload.get("fork", False)),
                "template": bool(repo_payload.get("is_template", False)),
            },
            license_spdx=((repo_payload.get("license") or {}).get("spdx_id") if isinstance(repo_payload.get("license"), dict) else None),
            topics_json=repo_payload.get("topics") or None,
            readme_excerpt=readme_excerpt,
            detected_build_systems_json=self._detect_build_systems(sampled_files),
            detected_languages_json=[str(repo_payload["language"])] if repo_payload.get("language") else None,
            key_paths_json=self._paths_by_role(sampled_files, {"README", "manifest", "entrypoint", "config"}),
            test_paths_json=self._paths_by_role(sampled_files, {"tests"}),
            ci_paths_json=self._paths_by_role(sampled_files, {"ci"}),
            examples_paths_json=self._paths_by_role(sampled_files, {"examples"}),
            docs_paths_json=self._paths_by_role(sampled_files, {"docs"}),
            release_summary_json=self._release_summary(releases),
            normalized_projection={
                "artifact_type": locator.artifact_type,
                "focus_kind": self._focus_kind(locator.artifact_type),
                "focus_path": locator.path,
                "page_path": locator.page_path,
                "repo_homepage": repo_payload.get("homepage"),
                "description": repo_payload.get("description"),
                "pushed_at": repo_payload.get("pushed_at"),
                "stars": repo_payload.get("stargazers_count"),
                "watchers": repo_payload.get("subscribers_count"),
                "forks": repo_payload.get("forks_count"),
                "open_issues": repo_payload.get("open_issues_count"),
                "tree_truncated": bool(tree_payload.get("truncated", False)),
            },
            sampled_files=sampled_files,
        )
        discovered = self._url_discovery.discover(
            candidate_group_id=job.candidate_group_id,
            parent_artifact_id=artifact.artifact_id,
            repo_projection=repo_projection,
        )
        anomalies = ["git_tree_truncated"] if bool(tree_payload.get("truncated", False)) else []
        limitations = []
        if auth_mode == "anonymous_degraded":
            limitations.append("github_app_auth_unavailable; using anonymous public API mode")
        if readme_excerpt is None:
            limitations.append("readme_excerpt_missing")
        if not sampled_files:
            limitations.append("sampled_files_missing")

        status = "ready" if readme_excerpt and sampled_files else "partial_ready"
        return SnapshotWritePlan(
            snapshot_type="github_repo",
            status=status,
            content_anchor=f"commit:{commit_sha}" if commit_sha else f"branch:{default_branch}",
            auth_mode=auth_mode,
            normalized_projection=repo_projection.normalized_projection,
            raw_payload_ref=None,
            evidence_limitations=limitations,
            fetch_anomalies=anomalies,
            repo_child=repo_projection,
            discovered_urls=discovered,
        )

    def _initial_auth_mode(self) -> str:
        if getattr(self._config, "github_app_id", None) and getattr(self._config, "github_installation_id", None):
            return "app_installation"
        return "anonymous_degraded"

    @staticmethod
    def _focus_kind(artifact_type: str) -> str:
        if artifact_type == "github_subpath":
            return "subpath"
        if artifact_type == "github_repo_page":
            return "repo_page"
        return "repo"

    @staticmethod
    def _release_summary(releases: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "release_count_recent": len(releases[:10]),
            "latest_release_published_at": releases[0].get("published_at") if releases else None,
            "has_release_assets": bool(releases and releases[0].get("assets")),
            "release_asset_download_count_topk": sorted(
                [
                    asset.get("download_count", 0)
                    for release in releases[:3]
                    for asset in (release.get("assets") or [])
                    if isinstance(asset, dict)
                ],
                reverse=True,
            )[:5],
            "has_prerelease_pattern": any(bool(release.get("prerelease")) for release in releases[:5]),
        }

    @staticmethod
    def _detect_build_systems(samples) -> list[str] | None:
        mapping = {
            "package.json": "node",
            "pyproject.toml": "python",
            "requirements.txt": "python",
            "Cargo.toml": "rust",
            "go.mod": "go",
            "pom.xml": "java",
        }
        results = []
        for sample in samples:
            name = sample.path.split("/")[-1]
            if name in mapping and mapping[name] not in results:
                results.append(mapping[name])
        return results or None

    @staticmethod
    def _paths_by_role(samples, roles: set[str]) -> list[str] | None:
        values = [sample.path for sample in samples if sample.role in roles]
        return values or None
