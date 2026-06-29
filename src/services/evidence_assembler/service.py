from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any
from uuid import UUID

from services.router_normalizer.canonicalizer import canonicalize_url
from services.router_normalizer.models import ResolvedUrl

from .config import EvidenceAssemblerConfig
from .models import (
    AssemblyResult,
    BundleMemberDraft,
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    DiscoveredLinkSummary,
    EvidenceBundlePreview,
    EvidenceBundleDraft,
    SnapshotRecord,
)
from .readiness import ReadinessEvaluator
from .repositories import EvidenceAssemblerRepository
from .reroot_rules import RerootRules
from .text_idea_builder import TextIdeaBuilder
from .token_budget import TokenBudgetProfiler


_SOURCE_URL_RE = re.compile(r"https?://[^\s<>'\")]+", re.IGNORECASE)
_MCP_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9_])mcp(?![A-Za-z0-9_])", re.IGNORECASE)
_SETUP_SIGNAL_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:setup|set\s+up|install|configure|configuration|quickstart)(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_CONNECT_SIGNAL_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:connect|connected|connection|integrate|integration)(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_USE_SIGNAL_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:use|using|usage|run|try)(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_WHITESPACE_RE = re.compile(r"\s+")
_DATE_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_SOURCE_URL_COUNT_CAP = 20
_GITHUB_REPO_LIKE_SNAPSHOT_TYPES = frozenset(
    {"github_repo", "github_subpath", "github_repo_page", "github_gist"}
)
_GITHUB_CONTEXT_TEXT_CAP = 240
_GITHUB_CONTEXT_IDENTIFIER_CAP = 120
_GITHUB_CONTEXT_LIST_CAP = 8
_GITHUB_CONTEXT_FILE_LIST_CAP = 8


class EvidenceAssemblerService:
    def __init__(
        self,
        config: EvidenceAssemblerConfig,
        *,
        repository: EvidenceAssemblerRepository,
        text_idea_builder: TextIdeaBuilder | None = None,
        reroot_rules: RerootRules | None = None,
        readiness: ReadinessEvaluator | None = None,
        token_budget: TokenBudgetProfiler | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._text_idea_builder = text_idea_builder or TextIdeaBuilder()
        self._reroot_rules = reroot_rules or RerootRules()
        self._readiness = readiness or ReadinessEvaluator()
        self._token_budget = token_budget or TokenBudgetProfiler()
        self._logger = logger or logging.getLogger(__name__)

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> list[AssemblyResult]:
        targets = await self._repository.resolve_refresh_targets(UUID(str(trigger_event_id)))
        results: list[AssemblyResult] = []
        for target in targets:
            result = await self._refresh_one(target)
            if result is not None:
                results.append(result)
        return results

    async def preview_trigger_event(self, trigger_event_id: str | UUID) -> list[EvidenceBundlePreview]:
        targets = await self._repository.resolve_refresh_targets(UUID(str(trigger_event_id)))
        previews: list[EvidenceBundlePreview] = []
        for target in targets:
            preview = await self._preview_one(target)
            if preview is not None:
                previews.append(preview)
        return previews

    async def _preview_one(self, target: BundleRefreshTarget) -> EvidenceBundlePreview | None:
        candidate = await self._repository.load_candidate_group(target.candidate_group_id)
        if candidate is None:
            return None

        members = await self._repository.load_candidate_members(candidate.candidate_group_id)
        if not members:
            return None

        member_artifact_ids = [member.artifact_id for member in members]
        snapshots = await self._repository.load_current_snapshots(member_artifact_ids)
        artifact_types = {member.artifact_id: member.artifact_type for member in members}
        current_primary_artifact_id = candidate.current_primary_artifact_id
        primary_snapshot = snapshots.get(current_primary_artifact_id)
        discovered_links = await self._repository.load_discovered_links(
            candidate_group_id=candidate.candidate_group_id,
            parent_artifact_ids=member_artifact_ids,
        )
        bundle_members = self._bundle_members(
            current_primary_artifact_id=current_primary_artifact_id,
            members=members,
            snapshots=snapshots,
        )
        supporting_snapshots = [
            snapshots[member.artifact_id]
            for member in members
            if member.artifact_id != current_primary_artifact_id and member.artifact_id in snapshots
        ]
        token_budget_profile = self._token_budget.choose(
            primary_snapshot=primary_snapshot,
            supporting_snapshot_count=len(supporting_snapshots),
            discovered_links_count=len(discovered_links),
        )
        ready_for_analysis = self._readiness.is_ready_for_analysis(
            primary_snapshot=primary_snapshot,
            bundle_members=bundle_members,
            token_budget_profile=token_budget_profile,
        )
        reroot_count = await self._repository.count_reroot_events(candidate.candidate_group_id)
        source_context_signals = await self._source_context_signals_for_primary(
            candidate=candidate,
            artifact_type=artifact_types.get(current_primary_artifact_id),
        )
        primary_summary = self._snapshot_summary(
            primary_snapshot,
            artifact_id=current_primary_artifact_id,
            source_context_signals=source_context_signals,
        )
        bundle_input_hash = self._bundle_input_hash(
            candidate_group_id=candidate.candidate_group_id,
            current_primary_artifact_id=current_primary_artifact_id,
            members=bundle_members,
            reroot_count=reroot_count,
            discovered_links=discovered_links,
            bundle_profile_version=self._config.bundle_profile_version,
            source_context_signals=source_context_signals,
            github_context=primary_summary.get("github_context"),
        )
        existing_bundle = await self._repository.load_existing_bundle(
            candidate_group_id=candidate.candidate_group_id,
            bundle_profile_version=self._config.bundle_profile_version,
            bundle_input_hash=bundle_input_hash,
        )
        analysis_requested_existing = False
        if existing_bundle is not None:
            existing_analysis = await self._repository.load_analysis_requested_outbox(
                candidate_group_id=candidate.candidate_group_id,
                bundle_id=existing_bundle.bundle_id,
            )
            analysis_requested_existing = existing_analysis is not None
        judge_profile = self._judge_profile_for_primary(artifact_types.get(current_primary_artifact_id))
        analysis_requested_would_emit = (
            ready_for_analysis
            and judge_profile is not None
            and (existing_bundle is None or not analysis_requested_existing)
        )
        return EvidenceBundlePreview(
            candidate_group_id=candidate.candidate_group_id,
            current_bundle_present_before=candidate.current_bundle_id is not None,
            bundle_input_existing=existing_bundle is not None,
            ready_for_analysis=ready_for_analysis,
            analysis_requested_existing=analysis_requested_existing,
            analysis_requested_would_emit=analysis_requested_would_emit,
        )

    async def _refresh_one(self, target: BundleRefreshTarget) -> AssemblyResult | None:
        candidate = await self._repository.load_candidate_group(target.candidate_group_id)
        if candidate is None:
            return None

        members = await self._repository.load_candidate_members(candidate.candidate_group_id)
        if not members:
            return None

        member_artifact_ids = [member.artifact_id for member in members]
        snapshots = await self._repository.load_current_snapshots(member_artifact_ids)
        artifact_types = {member.artifact_id: member.artifact_type for member in members}

        promoted_github_artifact_ids = await self._promote_discovered_github_repos(
            candidate=candidate,
            members=members,
        )
        if promoted_github_artifact_ids:
            members = await self._repository.load_candidate_members(candidate.candidate_group_id)
            member_artifact_ids = [member.artifact_id for member in members]
            snapshots = await self._repository.load_current_snapshots(member_artifact_ids)
            artifact_types = {member.artifact_id: member.artifact_type for member in members}
            if self._has_unready_promoted_github_repo(
                promoted_artifact_ids=promoted_github_artifact_ids,
                snapshots=snapshots,
            ):
                return AssemblyResult(
                    candidate_group_id=candidate.candidate_group_id,
                    bundle_id=None,
                    reused_existing_bundle=False,
                    ready_for_analysis=False,
                    emitted_analysis_requested=False,
                )

        if self._config.enable_text_idea:
            await self._materialize_existing_text_idea(candidate=candidate, members=members, snapshots=snapshots)

        current_primary_artifact_id = candidate.current_primary_artifact_id
        if self._config.enable_reroot:
            decision = self._reroot_rules.decide(
                current_primary_artifact_id=current_primary_artifact_id,
                members=members,
                current_snapshots=snapshots,
            )
            if decision.changed:
                async with self._repository.transaction():
                    await self._repository.append_reroot_event(
                        candidate_group_id=candidate.candidate_group_id,
                        from_artifact_id=decision.from_artifact_id,
                        to_artifact_id=decision.to_artifact_id,
                        reason_code=decision.reason_code or "reroot",
                        trigger_snapshot_id=target.trigger_snapshot_id,
                    )
                    await self._repository.update_current_primary(
                        candidate_group_id=candidate.candidate_group_id,
                        artifact_id=decision.to_artifact_id,
                    )
                current_primary_artifact_id = decision.to_artifact_id

        primary_snapshot = snapshots.get(current_primary_artifact_id)
        discovered_links = await self._repository.load_discovered_links(
            candidate_group_id=candidate.candidate_group_id,
            parent_artifact_ids=member_artifact_ids,
        )
        bundle_members = self._bundle_members(
            current_primary_artifact_id=current_primary_artifact_id,
            members=members,
            snapshots=snapshots,
        )
        supporting_snapshots = [
            snapshots[member.artifact_id]
            for member in members
            if member.artifact_id != current_primary_artifact_id and member.artifact_id in snapshots
        ]
        token_budget_profile = self._token_budget.choose(
            primary_snapshot=primary_snapshot,
            supporting_snapshot_count=len(supporting_snapshots),
            discovered_links_count=len(discovered_links),
        )
        ready_for_analysis = self._readiness.is_ready_for_analysis(
            primary_snapshot=primary_snapshot,
            bundle_members=bundle_members,
            token_budget_profile=token_budget_profile,
        )
        evidence_limitations = self._collect_limitations(primary_snapshot, supporting_snapshots)
        reroot_count = await self._repository.count_reroot_events(candidate.candidate_group_id)
        source_context_signals = await self._source_context_signals_for_primary(
            candidate=candidate,
            artifact_type=artifact_types.get(current_primary_artifact_id),
        )
        primary_summary = self._snapshot_summary(
            primary_snapshot,
            artifact_id=current_primary_artifact_id,
            source_context_signals=source_context_signals,
        )
        bundle_input_hash = self._bundle_input_hash(
            candidate_group_id=candidate.candidate_group_id,
            current_primary_artifact_id=current_primary_artifact_id,
            members=bundle_members,
            reroot_count=reroot_count,
            discovered_links=discovered_links,
            bundle_profile_version=self._config.bundle_profile_version,
            source_context_signals=source_context_signals,
            github_context=primary_summary.get("github_context"),
        )

        existing_bundle = await self._repository.load_existing_bundle(
            candidate_group_id=candidate.candidate_group_id,
            bundle_profile_version=self._config.bundle_profile_version,
            bundle_input_hash=bundle_input_hash,
        )
        if existing_bundle is not None:
            analysis_requested_event_id: UUID | None = None
            emitted_analysis_requested = False
            judge_profile = (
                self._judge_profile_for_primary(artifact_types.get(current_primary_artifact_id))
                if existing_bundle.ready_for_analysis
                else None
            )
            should_update_current_bundle = candidate.current_bundle_id != existing_bundle.bundle_id
            if judge_profile or should_update_current_bundle:
                async with self._repository.transaction():
                    if judge_profile:
                        record = await self._repository.insert_analysis_requested_outbox(
                            candidate_group_id=candidate.candidate_group_id,
                            bundle_id=existing_bundle.bundle_id,
                            judge_profile=judge_profile,
                            escalation_allowed=True,
                        )
                        analysis_requested_event_id = record.event_id
                        emitted_analysis_requested = record.created
                    if should_update_current_bundle:
                        await self._repository.update_current_bundle(
                            candidate_group_id=candidate.candidate_group_id,
                            bundle_id=existing_bundle.bundle_id,
                        )
            return AssemblyResult(
                candidate_group_id=candidate.candidate_group_id,
                bundle_id=existing_bundle.bundle_id,
                reused_existing_bundle=True,
                ready_for_analysis=existing_bundle.ready_for_analysis,
                emitted_analysis_requested=emitted_analysis_requested,
                analysis_requested_event_id=analysis_requested_event_id,
            )

        judge_profile = self._judge_profile_for_primary(artifact_types.get(current_primary_artifact_id))
        bundle_draft = EvidenceBundleDraft(
            candidate_group_id=candidate.candidate_group_id,
            initial_primary_artifact_id=candidate.initial_primary_artifact_id,
            current_primary_artifact_id=current_primary_artifact_id,
            bundle_profile_version=self._config.bundle_profile_version,
            bundle_input_hash=bundle_input_hash,
            reroot_count=reroot_count,
            primary_summary=primary_summary,
            supporting_summaries_json=[
                self._snapshot_summary(snapshot, artifact_id=snapshot.artifact_id) for snapshot in supporting_snapshots
            ],
            discovered_links_summary_json=[self._discovered_link_summary(item) for item in discovered_links],
            evidence_limitations=evidence_limitations,
            ready_for_analysis=ready_for_analysis,
            token_budget_profile=token_budget_profile,
            members=bundle_members,
            judge_profile=judge_profile,
        )

        emitted_analysis_requested = False
        analysis_requested_event_id: UUID | None = None
        async with self._repository.transaction():
            bundle_version = await self._repository.next_bundle_version(candidate.candidate_group_id)
            bundle_id = await self._repository.append_bundle(draft=bundle_draft, bundle_version=bundle_version)
            await self._repository.update_current_bundle(
                candidate_group_id=candidate.candidate_group_id,
                bundle_id=bundle_id,
            )
            if bundle_draft.ready_for_analysis and bundle_draft.judge_profile:
                record = await self._repository.insert_analysis_requested_outbox(
                    candidate_group_id=candidate.candidate_group_id,
                    bundle_id=bundle_id,
                    judge_profile=bundle_draft.judge_profile,
                    escalation_allowed=True,
                )
                emitted_analysis_requested = record.created
                analysis_requested_event_id = record.event_id

        return AssemblyResult(
            candidate_group_id=candidate.candidate_group_id,
            bundle_id=bundle_id,
            reused_existing_bundle=False,
            ready_for_analysis=bundle_draft.ready_for_analysis,
            emitted_analysis_requested=emitted_analysis_requested,
            analysis_requested_event_id=analysis_requested_event_id,
        )

    async def _promote_discovered_github_repos(
        self,
        *,
        candidate: CandidateGroupRecord,
        members: list[CandidateMemberRecord],
    ) -> set[UUID]:
        if candidate.proposal_status != "ready_for_enrich":
            return set()

        parent_artifact_types = {member.artifact_id: member.artifact_type for member in members}
        x_parent_artifact_ids = [
            member.artifact_id
            for member in members
            if member.artifact_type == "x_post"
        ]
        if not x_parent_artifact_ids:
            return set()

        discovered_links = await self._repository.load_discovered_links(
            candidate_group_id=candidate.candidate_group_id,
            parent_artifact_ids=x_parent_artifact_ids,
        )
        promoted_artifact_ids: set[UUID] = set()
        for link in discovered_links:
            if parent_artifact_types.get(link.parent_artifact_id) != "x_post":
                continue
            artifact = canonicalize_url(
                link.observed_url,
                observed=ResolvedUrl(
                    observed_url=link.observed_url,
                    normalized_url=link.observed_url,
                    resolved_url=link.observed_url,
                    source_kind="discovered_url_observation",
                    context_path=link.context_path,
                ),
            )
            if artifact.artifact_type != "github_repo":
                continue

            depth_budget = max(0, min(int(link.depth_remaining or 0), 1))
            async with self._repository.transaction():
                promoted = await self._repository.upsert_artifact_registry(artifact)
                await self._repository.insert_supporting_member_if_absent(
                    candidate_group_id=candidate.candidate_group_id,
                    artifact_id=promoted.artifact_id,
                )
                if promoted.current_status not in RerootRules.READY_REPO_STATES:
                    await self._repository.insert_github_enrich_requested_outbox(
                        candidate=candidate,
                        artifact=promoted,
                        depth_budget=depth_budget,
                    )
            promoted_artifact_ids.add(promoted.artifact_id)
        return promoted_artifact_ids

    @staticmethod
    def _has_unready_promoted_github_repo(
        *,
        promoted_artifact_ids: set[UUID],
        snapshots: dict[UUID, SnapshotRecord],
    ) -> bool:
        for artifact_id in promoted_artifact_ids:
            snapshot = snapshots.get(artifact_id)
            if snapshot is None or snapshot.status not in RerootRules.READY_REPO_STATES:
                return True
        return False

    async def _materialize_existing_text_idea(
        self,
        *,
        candidate: CandidateGroupRecord,
        members: list[CandidateMemberRecord],
        snapshots: dict[UUID, SnapshotRecord],
    ) -> None:
        text_idea_members = [member for member in members if member.artifact_type == "text_idea"]
        if not text_idea_members:
            return
        usable_external = any(
            member.artifact_type != "text_idea"
            and member.artifact_id in snapshots
            and snapshots[member.artifact_id].status in {"ready", "partial_ready", "low_evidence"}
            for member in members
        )
        needs_text_idea = (
            any(member.artifact_id == candidate.current_primary_artifact_id for member in text_idea_members)
            or not usable_external
        )
        if not needs_text_idea:
            return
        text_surface = await self._repository.load_source_message_text_surface(
            source_message_id=candidate.source_message_id,
            source_version_no=candidate.source_version_no,
        )
        for member in text_idea_members:
            if member.artifact_id in snapshots and snapshots[member.artifact_id].provider == "local_text_idea":
                continue
            draft = self._text_idea_builder.build(
                artifact_id=member.artifact_id,
                source_message_id=candidate.source_message_id,
                source_version_no=candidate.source_version_no,
                text_surface=text_surface,
            )
            if draft is None:
                continue
            async with self._repository.transaction():
                snapshots[member.artifact_id] = await self._repository.ensure_text_idea_snapshot(draft)

    async def _source_context_signals_for_primary(
        self,
        *,
        candidate: CandidateGroupRecord,
        artifact_type: str | None,
    ) -> dict[str, Any] | None:
        if artifact_type not in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
            return None
        text_surface = await self._repository.load_source_message_text_surface(
            source_message_id=candidate.source_message_id,
            source_version_no=candidate.source_version_no,
        )
        return self._source_context_signals(text_surface)

    @staticmethod
    def _source_context_signals(text_surface: str | None) -> dict[str, Any]:
        if not text_surface:
            return {
                "source_text_present": False,
                "source_text_chars_bucket": "0",
                "regex_url_count": 0,
                "regex_url_count_capped": False,
                "contains_mcp_token": False,
                "contains_setup_signal": False,
                "contains_connect_signal": False,
                "contains_use_signal": False,
                "signal_count": 0,
            }

        url_count = len(_SOURCE_URL_RE.findall(text_surface))
        contains_mcp_token = bool(_MCP_TOKEN_RE.search(text_surface))
        contains_setup_signal = bool(_SETUP_SIGNAL_RE.search(text_surface))
        contains_connect_signal = bool(_CONNECT_SIGNAL_RE.search(text_surface))
        contains_use_signal = bool(_USE_SIGNAL_RE.search(text_surface))
        signal_count = sum(
            [
                contains_mcp_token,
                contains_setup_signal,
                contains_connect_signal,
                contains_use_signal,
            ]
        )
        return {
            "source_text_present": True,
            "source_text_chars_bucket": _source_text_chars_bucket(len(text_surface)),
            "regex_url_count": min(url_count, _SOURCE_URL_COUNT_CAP),
            "regex_url_count_capped": url_count > _SOURCE_URL_COUNT_CAP,
            "contains_mcp_token": contains_mcp_token,
            "contains_setup_signal": contains_setup_signal,
            "contains_connect_signal": contains_connect_signal,
            "contains_use_signal": contains_use_signal,
            "signal_count": signal_count,
        }

    def _bundle_members(
        self,
        *,
        current_primary_artifact_id: UUID,
        members: list[CandidateMemberRecord],
        snapshots: dict[UUID, SnapshotRecord],
    ) -> list[BundleMemberDraft]:
        drafts = [
            BundleMemberDraft(
                artifact_id=member.artifact_id,
                snapshot_id=snapshots[member.artifact_id].snapshot_id,
                member_role="primary" if member.artifact_id == current_primary_artifact_id else "supporting",
                member_order=0 if member.artifact_id == current_primary_artifact_id else member.member_order,
            )
            for member in members
            if member.artifact_id in snapshots
        ]
        drafts.sort(
            key=lambda item: (
                0 if item.member_role == "primary" else 1,
                item.member_order if item.member_order is not None else 999999,
                str(item.artifact_id),
            )
        )
        return drafts

    def _collect_limitations(
        self,
        primary_snapshot: SnapshotRecord | None,
        supporting_snapshots: list[SnapshotRecord],
    ) -> list[str]:
        values: list[str] = []
        for snapshot in [primary_snapshot, *supporting_snapshots]:
            if snapshot is None:
                continue
            for item in snapshot.evidence_limitations:
                if item and item not in values:
                    values.append(item)
            if snapshot.status in {"partial_ready", "low_evidence"} and snapshot.status not in values:
                values.append(snapshot.status)
            if snapshot.status in {"failed_transient", "failed_permanent", "rate_limited", "access_denied", "unsupported"}:
                limitation = f"snapshot_{snapshot.status}"
                if limitation not in values:
                    values.append(limitation)
        return values

    def _snapshot_summary(
        self,
        snapshot: SnapshotRecord | None,
        *,
        artifact_id: UUID,
        source_context_signals: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if snapshot is None:
            summary = {"artifact_id": str(artifact_id), "status": "missing"}
            if source_context_signals is not None:
                summary["source_context_signals"] = source_context_signals
            return summary
        projection = snapshot.normalized_projection or {}
        summary: dict[str, Any] = {
            "artifact_id": str(snapshot.artifact_id),
            "snapshot_id": str(snapshot.snapshot_id),
            "provider": snapshot.provider,
            "snapshot_type": snapshot.snapshot_type,
            "status": snapshot.status,
            "content_anchor": snapshot.content_anchor,
            "headline": projection.get("title") or projection.get("description") or projection.get("display_surface"),
        }
        github_context = _github_projection_summary(snapshot=snapshot, projection=projection)
        if github_context:
            summary["github_context"] = github_context
        if source_context_signals is not None:
            summary["source_context_signals"] = source_context_signals
        return summary

    @staticmethod
    def _discovered_link_summary(link: DiscoveredLinkSummary) -> dict[str, Any]:
        return {
            "parent_artifact_id": str(link.parent_artifact_id),
            "parent_snapshot_id": str(link.parent_snapshot_id),
            "context_path": link.context_path,
            "observed_url": link.observed_url,
            "discovery_reason": link.discovery_reason,
        }

    def _bundle_input_hash(
        self,
        *,
        candidate_group_id: UUID,
        current_primary_artifact_id: UUID,
        members: list[BundleMemberDraft],
        reroot_count: int,
        discovered_links: list[DiscoveredLinkSummary],
        bundle_profile_version: str,
        source_context_signals: dict[str, Any] | None = None,
        github_context: Any | None = None,
    ) -> str:
        payload = {
            "candidate_group_id": str(candidate_group_id),
            "current_primary_artifact_id": str(current_primary_artifact_id),
            "members": [
                {
                    "artifact_id": str(member.artifact_id),
                    "snapshot_id": str(member.snapshot_id),
                    "member_role": member.member_role,
                    "member_order": member.member_order,
                }
                for member in members
            ],
            "reroot_count": reroot_count,
            "discovered_links": [
                {
                    "parent_artifact_id": str(link.parent_artifact_id),
                    "context_path": link.context_path,
                    "observed_url": link.observed_url,
                }
                for link in discovered_links
            ],
            "bundle_profile_version": bundle_profile_version,
        }
        if source_context_signals is not None:
            payload["source_context_signals"] = source_context_signals
        if isinstance(github_context, dict) and github_context:
            payload["primary_summary_github_context"] = github_context
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _judge_profile_for_primary(self, artifact_type: str | None) -> str | None:
        if artifact_type in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
            return "github_primary"
        if artifact_type == "x_post":
            return "x_primary"
        if artifact_type in {"web_article", "text_idea"}:
            return "text_idea_primary"
        return None


def _github_projection_summary(*, snapshot: SnapshotRecord, projection: dict[str, Any]) -> dict[str, Any] | None:
    if not _is_github_projection(snapshot=snapshot, projection=projection):
        return None

    context: dict[str, Any] = {}
    _put_text(
        context,
        "repository",
        projection,
        ("repo_full_name", "repository_full_name", "full_name", ("repository", "full_name"), ("repo", "repo_full_name")),
    )
    _put_text(context, "name", projection, ("repo_name", "name", ("repository", "name")))
    _put_text(context, "display_name", projection, ("display_name", "title"))
    _put_text(
        context,
        "description",
        projection,
        ("description", "display_surface"),
        cap=_GITHUB_CONTEXT_TEXT_CAP,
        redact_urls=True,
    )
    _put_text(context, "language", projection, ("language", "primary_language"))
    languages = _projection_list(projection, ("detected_languages_json", "languages"))
    if languages:
        if "language" not in context:
            context["language"] = languages[0]
        _put_list(context, "languages", values=languages, cap=_GITHUB_CONTEXT_LIST_CAP)
    _put_text(context, "license", projection, ("license_spdx", "license_name", ("license", "spdx_id"), ("license", "key"), "license"))
    _put_text(context, "default_branch", projection, ("default_branch", "observed_default_branch"))
    _put_text(context, "auth_mode", projection, ("auth_mode",))

    for output_key, keys in {
        "stars": ("stars", "stargazers_count", "star_count"),
        "forks": ("forks", "forks_count", "fork_count"),
        "watchers": ("watchers", "watchers_count", "subscribers_count"),
        "open_issues": ("open_issues", "open_issues_count"),
    }.items():
        _put_count(context, output_key, projection, keys)

    for output_key in ("pushed_at", "updated_at", "created_at"):
        _put_date(context, output_key, projection, (output_key,))
    _put_date(context, "latest_release_published_at", projection, (("release_summary_json", "latest_release_published_at"),))
    _put_count(context, "release_count_recent", projection, (("release_summary_json", "release_count_recent"),))

    _put_list(
        context,
        "topics",
        values=_projection_list(projection, ("topics", "topics_json")),
        cap=_GITHUB_CONTEXT_LIST_CAP,
        include_count=True,
    )

    notable_files = _projection_list(
        projection,
        (
            "notable_files",
            "file_names",
            "file_samples",
            "key_paths_json",
            "test_paths_json",
            "ci_paths_json",
            "examples_paths_json",
            "docs_paths_json",
        ),
    )
    _put_list(context, "notable_files", values=notable_files, cap=_GITHUB_CONTEXT_FILE_LIST_CAP, include_count=True)
    sample_count = _first_count(projection, ("file_sample_count", "sampled_file_count"))
    if sample_count is None and isinstance(projection.get("file_samples"), list):
        sample_count = len(projection["file_samples"])
    if sample_count is not None:
        context["file_sample_count"] = sample_count
    _put_list(
        context,
        "file_sample_roles",
        values=_projection_list(projection, ("file_sample_roles", "sampled_file_roles")),
        cap=_GITHUB_CONTEXT_LIST_CAP,
    )

    tooling = _projection_list(
        projection,
        (
            "package_tooling",
            "tooling_indicators",
            "package_managers",
            "detected_build_systems_json",
            "build_systems",
        ),
    )
    _put_list(context, "package_tooling", values=tooling, cap=_GITHUB_CONTEXT_LIST_CAP)

    setup_indicators = _projection_list(projection, ("setup_indicators", "setup_paths", "setup_files"))
    install_indicators = _projection_list(projection, ("install_indicators", "install_paths", "install_files"))
    _put_list(context, "setup_indicators", values=setup_indicators, cap=_GITHUB_CONTEXT_LIST_CAP)
    _put_list(context, "install_indicators", values=install_indicators, cap=_GITHUB_CONTEXT_LIST_CAP)
    _put_readiness_indicators(context, projection=projection, notable_files=notable_files)

    _put_text(context, "focus_kind", projection, ("focus_kind",))
    _put_text(context, "focus_path", projection, ("focus_path",))
    _put_text(context, "page_path", projection, ("page_path",))
    for output_key, keys in {
        "tree_truncated": ("tree_truncated",),
        "truncated": ("truncated",),
        "archived": ("archived", ("repo_flags_json", "archived")),
        "fork": ("fork", ("repo_flags_json", "fork")),
        "template": ("template", "is_template", ("repo_flags_json", "template")),
        "has_release_assets": ("has_release_assets", ("release_summary_json", "has_release_assets")),
    }.items():
        _put_bool(context, output_key, projection, keys)

    limitations = _projection_list(projection, ("evidence_limitations", "limitations", "fetch_anomalies"))
    _put_list(context, "evidence_limitations", values=limitations, cap=_GITHUB_CONTEXT_LIST_CAP)
    return context or None


def _is_github_projection(*, snapshot: SnapshotRecord, projection: dict[str, Any]) -> bool:
    snapshot_type = str(snapshot.snapshot_type or "").lower()
    provider = str(snapshot.provider or "").lower()
    artifact_type = str(projection.get("artifact_type") or "").lower()
    return (
        provider == "github"
        or snapshot_type in _GITHUB_REPO_LIKE_SNAPSHOT_TYPES
        or snapshot_type.startswith("github_")
        or artifact_type in _GITHUB_REPO_LIKE_SNAPSHOT_TYPES
    )


def _put_text(
    context: dict[str, Any],
    output_key: str,
    projection: dict[str, Any],
    keys: tuple[Any, ...],
    *,
    cap: int = _GITHUB_CONTEXT_IDENTIFIER_CAP,
    redact_urls: bool = False,
) -> None:
    value = _first_text(projection, keys, cap=cap, redact_urls=redact_urls)
    if value is not None:
        context[output_key] = value


def _first_text(
    projection: dict[str, Any],
    keys: tuple[Any, ...],
    *,
    cap: int,
    redact_urls: bool = False,
) -> str | None:
    for key in keys:
        text = _safe_text(_projection_lookup(projection, key), cap=cap, redact_urls=redact_urls)
        if text is not None:
            return text
    return None


def _safe_text(value: Any, *, cap: int, redact_urls: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if redact_urls:
        text = _SOURCE_URL_RE.sub("[redacted_url]", text)
    elif _contains_url(text):
        return None
    text = _WHITESPACE_RE.sub(" ", text).strip()
    if not text:
        return None
    if len(text) > cap:
        text = f"{text[: max(0, cap - 3)].rstrip()}..."
    return text


def _put_count(context: dict[str, Any], output_key: str, projection: dict[str, Any], keys: tuple[Any, ...]) -> None:
    value = _first_count(projection, keys)
    if value is not None:
        context[output_key] = value


def _first_count(projection: dict[str, Any], keys: tuple[Any, ...]) -> int | None:
    for key in keys:
        value = _safe_count(_projection_lookup(projection, key))
        if value is not None:
            return value
    return None


def _safe_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _put_bool(context: dict[str, Any], output_key: str, projection: dict[str, Any], keys: tuple[Any, ...]) -> None:
    value = _first_bool(projection, keys)
    if value is not None:
        context[output_key] = value


def _first_bool(projection: dict[str, Any], keys: tuple[Any, ...]) -> bool | None:
    for key in keys:
        value = _safe_bool(_projection_lookup(projection, key))
        if value is not None:
            return value
    return None


def _safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    return None


def _put_date(context: dict[str, Any], output_key: str, projection: dict[str, Any], keys: tuple[Any, ...]) -> None:
    for key in keys:
        value = _date_prefix(_projection_lookup(projection, key))
        if value is not None:
            context[output_key] = value
            return


def _date_prefix(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if _contains_url(text):
        return None
    match = _DATE_PREFIX_RE.match(text)
    return match.group(0) if match else None


def _put_list(
    context: dict[str, Any],
    output_key: str,
    *,
    values: list[str],
    cap: int,
    include_count: bool = False,
) -> None:
    if not values:
        return
    context[output_key] = values[:cap]
    if include_count:
        context[f"{output_key}_count"] = len(values)
    if len(values) > cap:
        context[f"{output_key}_capped"] = True


def _projection_list(projection: dict[str, Any], keys: tuple[Any, ...]) -> list[str]:
    values: list[str] = []
    for key in keys:
        values.extend(_coerce_list_values(_projection_lookup(projection, key)))

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _coerce_list_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values: list[str] = []
        for item in value:
            if isinstance(item, dict):
                values.extend(_coerce_dict_list_item(item))
                continue
            text = _safe_text(item, cap=_GITHUB_CONTEXT_IDENTIFIER_CAP)
            if text is not None:
                values.append(text)
        return values
    text = _safe_text(value, cap=_GITHUB_CONTEXT_IDENTIFIER_CAP)
    return [text] if text is not None else []


def _coerce_dict_list_item(item: dict[str, Any]) -> list[str]:
    for key in ("path", "name", "role"):
        text = _safe_text(item.get(key), cap=_GITHUB_CONTEXT_IDENTIFIER_CAP)
        if text is not None:
            return [text]
    return []


def _put_readiness_indicators(
    context: dict[str, Any],
    *,
    projection: dict[str, Any],
    notable_files: list[str],
) -> None:
    for output_key, bool_keys, list_keys, contains_tokens in (
        ("readme_present", ("readme_present", "has_readme"), ("readme_excerpt",), ("readme",)),
        ("docs_present", ("docs_present", "has_docs"), ("docs_paths_json", "docs_indicators"), ("docs/", "documentation")),
        ("config_present", ("config_present", "has_config"), ("config_paths_json", "config_indicators"), ("config", ".env")),
    ):
        value = _first_bool(projection, bool_keys)
        if value is None and _projection_list(projection, list_keys):
            value = True
        if value is None and contains_tokens:
            value = any(_contains_any(item, contains_tokens) for item in notable_files)
        if value is not None:
            context[output_key] = value

    if "setup_indicators" in context:
        context["setup_present"] = True
    else:
        _put_bool(context, "setup_present", projection, ("setup_present", "has_setup"))
    if "install_indicators" in context:
        context["install_present"] = True
    else:
        _put_bool(context, "install_present", projection, ("install_present", "has_install"))


def _contains_any(value: str, tokens: tuple[str, ...]) -> bool:
    lowered = value.lower()
    return any(token in lowered for token in tokens)


def _projection_lookup(projection: dict[str, Any], key: Any) -> Any:
    if isinstance(key, tuple):
        current: Any = projection
        for part in key:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current
    return projection.get(key)


def _contains_url(value: str) -> bool:
    lowered = value.lower()
    return "http://" in lowered or "https://" in lowered


def _source_text_chars_bucket(length: int) -> str:
    if length <= 0:
        return "0"
    if length <= 120:
        return "1-120"
    if length <= 500:
        return "121-500"
    if length <= 2000:
        return "501-2000"
    return "2001-plus"
