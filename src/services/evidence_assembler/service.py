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
_SOURCE_URL_COUNT_CAP = 20


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
        bundle_input_hash = self._bundle_input_hash(
            candidate_group_id=candidate.candidate_group_id,
            current_primary_artifact_id=current_primary_artifact_id,
            members=bundle_members,
            reroot_count=reroot_count,
            discovered_links=discovered_links,
            bundle_profile_version=self._config.bundle_profile_version,
            source_context_signals=source_context_signals,
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
        bundle_input_hash = self._bundle_input_hash(
            candidate_group_id=candidate.candidate_group_id,
            current_primary_artifact_id=current_primary_artifact_id,
            members=bundle_members,
            reroot_count=reroot_count,
            discovered_links=discovered_links,
            bundle_profile_version=self._config.bundle_profile_version,
            source_context_signals=source_context_signals,
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
            primary_summary=self._snapshot_summary(
                primary_snapshot,
                artifact_id=current_primary_artifact_id,
                source_context_signals=source_context_signals,
            ),
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
