from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from src.services.collector_telegram.operator_supplied_source import (
    OperatorSuppliedSourceAdapter,
    parse_operator_source_packet,
)
from src.services.evidence_assembler.config import EvidenceAssemblerConfig
from src.services.evidence_assembler.models import (
    AnalysisRequestedOutboxRecord,
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    DiscoveredLinkSummary,
    EvidenceBundleDraft,
    ExistingBundleRecord,
    SnapshotRecord,
)
from src.services.evidence_assembler.service import EvidenceAssemblerService
from src.services.evidence_assembler.text_idea_builder import TextIdeaBuilder
from src.services.gh_enricher.fetch_planner import GitHubFetchPlanner
from src.services.gh_enricher.file_sampler import GitHubFileSampler
from src.services.gh_enricher.models import (
    ArtifactEnrichmentJob,
    ArtifactRecord as GhArtifactRecord,
    CurrentSnapshotRef as GhCurrentSnapshotRef,
)
from src.services.gh_enricher.service import GhEnricherService
from src.services.gh_enricher.url_discovery import GitHubUrlDiscovery
from src.services.maintenance.exact_target_source_to_analysis_materializer import (
    ExactTargetSourceToAnalysisRequest,
    FinalReadback,
    NormalizationReadback,
    ProviderEnrichmentRequest,
    ProviderEnrichmentResult,
    RefreshEventRecord,
    run_exact_target_source_to_analysis_materializer,
)
from src.services.router_normalizer.config import RouterNormalizerConfig
from src.services.router_normalizer.models import (
    CanonicalArtifact,
    OutboxEventRow,
    SourceMessageSnapshot,
)
from src.services.router_normalizer.service import RouterNormalizerService


KOREAN_LLM_WORKFLOW_TEXT = (
    "회사에서 llm 사용 권한 받은김에 이것저것 작업 중인데.. "
    "머 보안때문에 되는게 없네요. cli는 쓸수도 없고.. 자동화를 할수가 없네"
)


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class Ledger:
    def __init__(self) -> None:
        self.registry_rows = [{"registry_id": str(uuid4()), "chat_id": 9001}]
        self.current: dict[tuple[str, int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[dict[str, Any]] = []
        self.normalization_runs: list[dict[str, Any]] = []
        self.artifacts_by_canonical: dict[str, UUID] = {}
        self.artifact_records: dict[UUID, CanonicalArtifact] = {}
        self.observations: list[dict[str, Any]] = []
        self.candidates: dict[UUID, CandidateGroupRecord] = {}
        self.members: dict[UUID, list[CandidateMemberRecord]] = {}
        self.snapshots: dict[UUID, SnapshotRecord] = {}
        self.enrichment_run_keys: set[str] = set()
        self.provider_snapshot_events: list[UUID] = []
        self.text_idea_snapshots_created = 0
        self.bundles: list[tuple[UUID, EvidenceBundleDraft]] = []
        self.analysis_outbox: list[dict[str, Any]] = []
        self.current_bundle_updates: list[dict[str, Any]] = []
        self.commits: list[str] = []
        self.allow_enrichment_requests = False
        self.github_client_calls: list[str] = []


class CollectorRepo:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def find_public_username_registry_targets(self, normalized_source_value: str):
        del normalized_source_value
        return self.ledger.registry_rows

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int):
        return self.ledger.current.get((platform, chat_id, message_id))

    async def get_latest_version(self, source_message_id: str):
        rows = self.ledger.versions.get(source_message_id, [])
        return rows[-1] if rows else None

    async def upsert_source_message(self, projection, *, platform: str = "telegram"):
        key = (platform, projection.chat_id, projection.message_id)
        row = self.ledger.current.get(key)
        if row is None:
            row = {
                "source_message_id": str(uuid4()),
                "current_version_no": 0,
            }
            self.ledger.current[key] = row
        row.update(
            {
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "text_body": projection.text_body,
                "caption_text": projection.caption_text,
                "text_surface": projection.text_surface,
                "entities_json": projection.entities_json,
                "url_surface_json": projection.url_surface_json,
                "raw_message_json": projection.raw_message_json,
                "deleted_at": None,
            }
        )
        return row

    async def append_source_message_version(
        self,
        *,
        source_message_id: str,
        projection,
        version_reason: str,
        observed_at=None,
        telegram_edit_date=None,
    ):
        row = {
            "version_no": len(self.ledger.versions.get(source_message_id, [])) + 1,
            "content_hash": projection.content_hash,
            "text_surface": projection.text_surface,
            "entities_json": projection.entities_json,
            "raw_message_json": projection.raw_message_json,
            "version_reason": version_reason,
        }
        self.ledger.versions.setdefault(source_message_id, []).append(row)
        for current in self.ledger.current.values():
            if current["source_message_id"] == source_message_id:
                current["current_version_no"] = row["version_no"]
        return row

    async def insert_outbox_event(self, event):
        self.ledger.outbox.append(
            {
                "event_id": uuid4(),
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": UUID(str(event.aggregate_id)),
                "dedupe_key": event.dedupe_key,
                "payload_json": event.payload_json,
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
            }
        )
        return True

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str):
        return next((row for row in self.ledger.outbox if row["dedupe_key"] == dedupe_key), None)


class RouterRepo:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def get_outbox_event(self, event_id: UUID):
        row = next((item for item in self.ledger.outbox if item["event_id"] == event_id), None)
        if row is None:
            return None
        return OutboxEventRow(
            event_id=row["event_id"],
            event_type=row["event_type"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            dedupe_key=row["dedupe_key"],
            payload_json=row["payload_json"],
            status=row["status"],
            created_at=row["created_at"],
        )

    async def get_current_source_message(self, source_message_id: UUID):
        for row in self.ledger.current.values():
            if row["source_message_id"] == str(source_message_id):
                return SourceMessageSnapshot(
                    source_message_id=source_message_id,
                    source_version_no=int(row["current_version_no"]),
                    text_body=row["text_body"],
                    caption_text=row["caption_text"],
                    text_surface=row["text_surface"],
                    entities_json=row["entities_json"],
                    url_surface_json=row["url_surface_json"],
                    raw_message_json=row["raw_message_json"],
                    deleted_at=row["deleted_at"],
                )
        return None

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int):
        raise AssertionError("current version should be used in this component test")

    async def upsert_normalization_run(self, **kwargs):
        if not self.ledger.normalization_runs:
            self.ledger.normalization_runs.append(kwargs)
        return uuid4()

    async def insert_suppression_trace(self, **kwargs):
        raise AssertionError("local text idea candidate must not be suppressed")

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact):
        artifact_id = self.ledger.artifacts_by_canonical.get(artifact.canonical_id)
        if artifact_id is None:
            artifact_id = uuid4()
            self.ledger.artifacts_by_canonical[artifact.canonical_id] = artifact_id
            self.ledger.artifact_records[artifact_id] = artifact
        return artifact_id

    async def insert_artifact_observation_if_absent(self, **kwargs):
        self.ledger.observations.append(kwargs)

    async def upsert_candidate_group(self, **kwargs):
        existing = next(
            (
                candidate
                for candidate in self.ledger.candidates.values()
                if candidate.source_message_id == kwargs["source_message_id"]
                and candidate.source_version_no == kwargs["source_version_no"]
            ),
            None,
        )
        if existing is not None:
            return existing.candidate_group_id
        candidate_group_id = uuid4()
        self.ledger.candidates[candidate_group_id] = CandidateGroupRecord(
            candidate_group_id=candidate_group_id,
            source_message_id=kwargs["source_message_id"],
            source_version_no=kwargs["source_version_no"],
            initial_primary_artifact_id=kwargs["primary_artifact_id"],
            current_primary_artifact_id=kwargs["primary_artifact_id"],
            proposal_status="proposed",
            current_bundle_id=None,
        )
        self.ledger.members[candidate_group_id] = []
        return candidate_group_id

    async def upsert_candidate_member(self, **kwargs):
        artifact = self.ledger.artifact_records[kwargs["artifact_id"]]
        member = CandidateMemberRecord(
            artifact_id=kwargs["artifact_id"],
            artifact_type=artifact.artifact_type,
            member_role=kwargs["member_role"],
            member_order=kwargs["member_order"],
            canonical_id=artifact.canonical_id,
            canonical_url=artifact.canonical_url,
        )
        members = self.ledger.members.setdefault(kwargs["candidate_group_id"], [])
        if not any(
            item.artifact_id == member.artifact_id and item.member_role == member.member_role
            for item in members
        ):
            members.append(member)

    async def insert_enrichment_requested_outbox(self, **kwargs):
        artifact = kwargs["artifact"]
        if artifact.provider_route is not None and not self.ledger.allow_enrichment_requests:
            raise AssertionError("text_idea path must not request enrichment")
        if artifact.provider_route is None:
            return
        self.ledger.outbox.append(
            {
                "event_id": uuid4(),
                "event_type": "artifact.enrich.requested.v1",
                "aggregate_type": "artifact",
                "aggregate_id": kwargs["artifact_id"],
                "payload_json": {
                    "candidate_group_id": str(kwargs["candidate_group_id"]),
                    "artifact_id": str(kwargs["artifact_id"]),
                    "artifact_type": artifact.artifact_type,
                    "provider_route": artifact.provider_route,
                    "refresh_mode": "standard",
                    "depth_budget": 1,
                    "source_message_id": str(kwargs["source_message_id"]),
                    "source_version_no": kwargs["source_version_no"],
                },
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
            }
        )


class EvidenceRepo:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    def transaction(self):
        return Tx()

    async def resolve_refresh_targets(self, trigger_event_id):
        row = next((item for item in self.ledger.outbox if item["event_id"] == trigger_event_id), None)
        if row is None:
            return []
        if row["event_type"] == "artifact.snapshot.updated.v1":
            artifact_id = row["aggregate_id"]
            return [
                BundleRefreshTarget(
                    candidate_group_id=candidate_group_id,
                    trigger_event_id=row["event_id"],
                    trigger_event_type=row["event_type"],
                    trigger_artifact_id=artifact_id,
                    trigger_snapshot_id=UUID(str(row["payload_json"]["snapshot_id"])),
                )
                for candidate_group_id, members in self.ledger.members.items()
                if any(member.artifact_id == artifact_id for member in members)
            ]
        return [
            BundleRefreshTarget(
                candidate_group_id=row["aggregate_id"],
                trigger_event_id=row["event_id"],
                trigger_event_type=row["event_type"],
            )
        ]

    async def load_candidate_group(self, candidate_group_id):
        return self.ledger.candidates.get(candidate_group_id)

    async def load_candidate_members(self, candidate_group_id):
        return self.ledger.members.get(candidate_group_id, [])

    async def load_current_snapshots(self, artifact_ids):
        wanted = set(artifact_ids)
        return {
            artifact_id: snapshot
            for artifact_id, snapshot in self.ledger.snapshots.items()
            if artifact_id in wanted
        }

    async def load_source_message_text_surface(self, *, source_message_id, source_version_no):
        del source_version_no
        for row in self.ledger.current.values():
            if row["source_message_id"] == str(source_message_id):
                return row["text_surface"]
        return None

    async def ensure_text_idea_snapshot(self, draft):
        content_anchor = TextIdeaBuilder.input_hash(draft)
        existing = self.ledger.snapshots.get(draft.artifact_id)
        if existing is not None and existing.content_anchor == content_anchor:
            return existing
        self.ledger.text_idea_snapshots_created += 1
        snapshot = SnapshotRecord(
            snapshot_id=uuid4(),
            artifact_id=draft.artifact_id,
            provider="local_text_idea",
            snapshot_type="text_idea",
            status=draft.status,
            fetched_at=None,
            content_anchor=content_anchor,
            normalized_projection={"display_surface": draft.display_surface},
            evidence_limitations=draft.evidence_limitations,
        )
        self.ledger.snapshots[draft.artifact_id] = snapshot
        return snapshot

    async def load_discovered_links(self, **kwargs) -> list[DiscoveredLinkSummary]:
        return []

    async def count_reroot_events(self, candidate_group_id):
        return 0

    async def load_existing_bundle(self, **kwargs):
        for bundle_id, draft in self.ledger.bundles:
            if (
                draft.candidate_group_id == kwargs["candidate_group_id"]
                and draft.bundle_profile_version == kwargs["bundle_profile_version"]
                and draft.bundle_input_hash == kwargs["bundle_input_hash"]
            ):
                return ExistingBundleRecord(
                    bundle_id=bundle_id,
                    candidate_group_id=draft.candidate_group_id,
                    bundle_version=1,
                    bundle_profile_version=draft.bundle_profile_version,
                    bundle_input_hash=draft.bundle_input_hash,
                    ready_for_analysis=draft.ready_for_analysis,
                )
        return None

    async def load_analysis_requested_outbox(self, **kwargs):
        for row in self.ledger.analysis_outbox:
            if row["candidate_group_id"] == kwargs["candidate_group_id"] and row["bundle_id"] == kwargs["bundle_id"]:
                return AnalysisRequestedOutboxRecord(event_id=row["event_id"], created=False)
        return None

    async def next_bundle_version(self, candidate_group_id):
        return 1 + sum(1 for _, draft in self.ledger.bundles if draft.candidate_group_id == candidate_group_id)

    async def append_bundle(self, **kwargs):
        bundle_id = uuid4()
        self.ledger.bundles.append((bundle_id, kwargs["draft"]))
        return bundle_id

    async def update_current_bundle(self, **kwargs):
        self.ledger.current_bundle_updates.append(kwargs)
        candidate = self.ledger.candidates[kwargs["candidate_group_id"]]
        self.ledger.candidates[kwargs["candidate_group_id"]] = CandidateGroupRecord(
            candidate_group_id=candidate.candidate_group_id,
            source_message_id=candidate.source_message_id,
            source_version_no=candidate.source_version_no,
            initial_primary_artifact_id=candidate.initial_primary_artifact_id,
            current_primary_artifact_id=candidate.current_primary_artifact_id,
            proposal_status=candidate.proposal_status,
            current_bundle_id=kwargs["bundle_id"],
        )

    async def insert_analysis_requested_outbox(self, **kwargs):
        existing = await self.load_analysis_requested_outbox(**kwargs)
        if existing is not None:
            return existing
        event_id = uuid4()
        self.ledger.analysis_outbox.append({**kwargs, "event_id": event_id})
        return AnalysisRequestedOutboxRecord(event_id=event_id, created=True)


class MaterializerRepo:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def load_normalization_readback(self, *, source_message_id: UUID, source_version_no: int):
        candidates = [
            candidate
            for candidate in self.ledger.candidates.values()
            if candidate.source_message_id == source_message_id
            and candidate.source_version_no == source_version_no
        ]
        members = self.ledger.members.get(candidates[0].candidate_group_id, []) if candidates else []
        primary = next((member for member in members if member.member_role == "primary"), None)
        enrichment_rows = [
            row
            for row in self.ledger.outbox
            if row["event_type"] == "artifact.enrich.requested.v1"
            and row["payload_json"].get("source_message_id") == str(source_message_id)
            and row["payload_json"].get("source_version_no") == source_version_no
        ]
        provider_route_counts: dict[str, int] = {}
        for row in enrichment_rows:
            route = row["payload_json"].get("provider_route")
            if route in {"github", "x", "web"}:
                provider_route_counts[route] = provider_route_counts.get(route, 0) + 1
        return NormalizationReadback(
            normalization_runs=len(self.ledger.normalization_runs),
            candidate_groups=len(candidates),
            primary_members=1 if primary else 0,
            primary_artifact_type=None if primary is None else primary.artifact_type,
            primary_artifact_id=None if primary is None else primary.artifact_id,
            candidate_group_id=None if not candidates else candidates[0].candidate_group_id,
            enrichment_requests=len(enrichment_rows),
            enrichment_request_event_id=(
                None if not enrichment_rows else enrichment_rows[0]["event_id"]
            ),
            provider_route=None if not enrichment_rows else enrichment_rows[0]["payload_json"].get("provider_route"),
            refresh_mode=None if not enrichment_rows else enrichment_rows[0]["payload_json"].get("refresh_mode"),
            depth_budget=None if not enrichment_rows else int(enrichment_rows[0]["payload_json"].get("depth_budget")),
            provider_route_counts=provider_route_counts,
        )

    async def insert_candidate_bundle_refresh_event(
        self,
        *,
        candidate_group_id: UUID,
        source_message_id: UUID,
        source_version_no: int,
        packet_fingerprint: str,
    ):
        del source_message_id, source_version_no, packet_fingerprint
        event_id = uuid4()
        self.ledger.outbox.append(
            {
                "event_id": event_id,
                "event_type": "candidate.bundle.refresh.v1",
                "aggregate_type": "candidate_group",
                "aggregate_id": candidate_group_id,
                "payload_json": {"candidate_group_id": str(candidate_group_id)},
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
            }
        )
        return RefreshEventRecord(event_id=event_id, created=True)

    async def load_final_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        source_content_hash: str,
        chat_id: int,
        message_id: int,
        candidate_group_id: UUID,
    ):
        del source_content_hash, chat_id, message_id
        source_id = str(source_message_id)
        candidate = self.ledger.candidates[candidate_group_id]
        bundle_id = candidate.current_bundle_id
        return FinalReadback(
            source_messages=sum(1 for row in self.ledger.current.values() if row["source_message_id"] == source_id),
            source_message_versions=len(self.ledger.versions.get(source_id, [])),
            source_created_events=sum(1 for row in self.ledger.outbox if row["event_type"] == "source_message.created.v1"),
            normalization_runs=len(self.ledger.normalization_runs),
            candidate_groups=1,
            primary_text_idea_members=sum(
                1
                for member in self.ledger.members[candidate_group_id]
                if member.member_role == "primary" and member.artifact_type == "text_idea"
            ),
            external_enrichment_requests=sum(
                1 for row in self.ledger.outbox if row["event_type"] == "artifact.enrich.requested.v1"
            ),
            provider_snapshots=sum(
                1
                for member in self.ledger.members[candidate_group_id]
                if member.artifact_id in self.ledger.snapshots
                and self.ledger.snapshots[member.artifact_id].provider in {"github", "x", "web"}
            ),
            artifact_snapshot_updated_events=sum(
                1 for row in self.ledger.outbox if row["event_type"] == "artifact.snapshot.updated.v1"
            ),
            text_idea_snapshots=self.ledger.text_idea_snapshots_created,
            ready_current_bundles=1 if bundle_id else 0,
            candidate_evidence_members=1 if bundle_id else 0,
            analysis_requested_events=len(self.ledger.analysis_outbox),
            bundle_id=bundle_id,
            analysis_request_event_id=(
                self.ledger.analysis_outbox[0]["event_id"] if self.ledger.analysis_outbox else None
            ),
        )


class GithubProviderRepo:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger
        self.snapshot_updated_event_id: UUID | None = None

    def transaction(self):
        return Tx()

    async def load_artifact(self, artifact_id: UUID):
        artifact = self.ledger.artifact_records.get(artifact_id)
        if artifact is None:
            return None
        snapshot = self.ledger.snapshots.get(artifact_id)
        return GhArtifactRecord(
            artifact_id=artifact_id,
            artifact_type=artifact.artifact_type,
            canonical_id=artifact.canonical_id,
            canonical_url=artifact.canonical_url,
            normalized_host=artifact.normalized_host,
            artifact_key_json=artifact.artifact_key_json,
            current_snapshot_id=None if snapshot is None else snapshot.snapshot_id,
            current_status=None if snapshot is None else snapshot.status,
        )

    async def load_current_snapshot(self, snapshot_id: UUID | None):
        if snapshot_id is None:
            return None
        for snapshot in self.ledger.snapshots.values():
            if snapshot.snapshot_id == snapshot_id:
                return GhCurrentSnapshotRef(
                    snapshot_id=snapshot.snapshot_id,
                    status=snapshot.status,
                    fetched_at=snapshot.fetched_at,
                    content_anchor=snapshot.content_anchor,
                    normalized_projection=snapshot.normalized_projection,
                )
        return None

    async def insert_enrichment_run_if_absent(self, **kwargs):
        key = kwargs["job_idempotency_key"]
        if key in self.ledger.enrichment_run_keys:
            return None
        self.ledger.enrichment_run_keys.add(key)
        return uuid4()

    async def mark_enrichment_run_started(self, run_id) -> None:
        del run_id

    async def mark_enrichment_run_finished(self, **kwargs) -> None:
        del kwargs

    async def insert_snapshot(self, *, artifact_id: UUID, provider: str, plan) -> UUID:
        snapshot = SnapshotRecord(
            snapshot_id=uuid4(),
            artifact_id=artifact_id,
            provider=provider,
            snapshot_type=plan.snapshot_type,
            status=plan.status,
            fetched_at=datetime.now(timezone.utc),
            content_anchor=plan.content_anchor,
            normalized_projection=plan.normalized_projection,
            evidence_limitations=plan.evidence_limitations,
        )
        self.ledger.snapshots[artifact_id] = snapshot
        return snapshot.snapshot_id

    async def insert_github_repo_child(self, **kwargs) -> None:
        del kwargs

    async def insert_github_file_sample(self, **kwargs) -> None:
        del kwargs

    async def insert_discovered_url(self, *, snapshot_id: UUID, draft) -> None:
        del snapshot_id, draft

    async def update_artifact_current_snapshot(self, **kwargs) -> None:
        del kwargs

    async def insert_snapshot_updated_outbox(self, **kwargs):
        event_id = uuid4()
        self.snapshot_updated_event_id = event_id
        self.ledger.provider_snapshot_events.append(event_id)
        self.ledger.outbox.append(
            {
                "event_id": event_id,
                "event_type": "artifact.snapshot.updated.v1",
                "aggregate_type": "artifact",
                "aggregate_id": kwargs["artifact_id"],
                "payload_json": {
                    "artifact_id": str(kwargs["artifact_id"]),
                    "snapshot_id": str(kwargs["snapshot_id"]),
                    "provider": "github",
                    "status": kwargs["status"],
                    "content_anchor": kwargs["content_anchor"],
                },
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
            }
        )
        return event_id


class FakeGitHubClient:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def get_repo(self, owner, repo, *, auth_mode):
        del auth_mode
        self.ledger.github_client_calls.append("repo")
        return {
            "full_name": f"{owner}/{repo}",
            "default_branch": "main",
            "description": "In-memory fake GitHub provider fixture",
            "homepage": None,
            "license": {"spdx_id": "MIT"},
            "topics": ["ai"],
            "language": "Python",
            "stargazers_count": 3,
            "subscribers_count": 1,
            "forks_count": 0,
            "open_issues_count": 0,
            "archived": False,
            "fork": False,
            "is_template": False,
            "pushed_at": "2026-06-01T00:00:00Z",
        }

    async def get_default_branch_head(self, owner, repo, default_branch, *, auth_mode):
        del owner, repo, default_branch, auth_mode
        self.ledger.github_client_calls.append("head")
        return {"sha": "abc123def456"}

    async def get_tree(self, owner, repo, ref, *, recursive, auth_mode):
        del owner, repo, ref, auth_mode
        self.ledger.github_client_calls.append("tree")
        return {
            "truncated": False,
            "tree": [
                {"type": "blob", "path": "README.md"},
                {"type": "blob", "path": "pyproject.toml"},
                {"type": "blob", "path": "tests/test_feature.py"},
            ],
        } if recursive else {"truncated": False, "tree": []}

    async def get_contents(self, owner, repo, path, *, ref, auth_mode):
        del owner, repo, ref, auth_mode
        self.ledger.github_client_calls.append(f"contents:{path}")
        return {
            "encoding": "base64",
            "content": (
                "IyBGYWtlIHByb3ZpZGVyIGV2aWRlbmNlCk9mZmxpbmUgb25seS4K"
            ),
            "size": 46,
        }

    async def get_releases(self, owner, repo, *, auth_mode):
        del owner, repo, auth_mode
        self.ledger.github_client_calls.append("releases")
        return []


class ProviderEnrichmentService:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def materialize_provider_request(
        self,
        request: ProviderEnrichmentRequest,
    ) -> ProviderEnrichmentResult:
        assert request.provider_route == "github"
        repository = GithubProviderRepo(self.ledger)
        service = GhEnricherService(
            github_config(),
            repository=repository,
            github_client=FakeGitHubClient(self.ledger),
            fetch_planner=GitHubFetchPlanner(),
            file_sampler=GitHubFileSampler(),
            url_discovery=GitHubUrlDiscovery(),
        )
        result = await service.handle_job(
            ArtifactEnrichmentJob(
                trigger_event_id=request.trigger_event_id,
                event_type="artifact.enrich.requested.v1",
                candidate_group_id=request.candidate_group_id,
                artifact_id=request.artifact_id,
                artifact_type=request.artifact_type,
                provider_route=request.provider_route,
                refresh_mode=request.refresh_mode,
                depth_budget=request.depth_budget,
            )
        )
        return ProviderEnrichmentResult(
            provider_route=request.provider_route,
            status=result.status,
            emitted_snapshot_updated=result.emitted_snapshot_updated,
            snapshot_id=result.snapshot_id,
            snapshot_updated_event_id=repository.snapshot_updated_event_id,
        )


class StageComponents:
    def __init__(self, ledger: Ledger, stage_name: str) -> None:
        self.ledger = ledger
        self.stage_name = stage_name
        self.collector_repository = CollectorRepo(ledger)
        self.source_adapter = OperatorSuppliedSourceAdapter()
        self.materializer_repository = MaterializerRepo(ledger)
        self.normalizer_service = RouterNormalizerService(router_config(), repository=RouterRepo(ledger))
        self.provider_enrichment_service = ProviderEnrichmentService(ledger)
        self.assembler_service = EvidenceAssemblerService(assembler_config(), repository=EvidenceRepo(ledger))

    async def commit(self) -> None:
        self.ledger.commits.append(self.stage_name)


class StageFactory:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    @asynccontextmanager
    async def stage(self, stage_name: str):
        yield StageComponents(self.ledger, stage_name)


def router_config() -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="test",
        database_url="db_locator_not_used",
        redis_url="redis_locator_not_used",
        queue_name="q.source.normalize",
        consumer_group="router-normalizer",
        consumer_name="component",
        block_ms=1,
        batch_size=1,
        normalizer_version="component-normalizer",
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


def assembler_config() -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig(
        app_env="test",
        database_url="db_locator_not_used",
        redis_url="redis_locator_not_used",
        queue_name="q.candidate.bundle",
        consumer_group="evidence-assembler",
        consumer_name="component",
        batch_size=1,
        block_ms=1,
        bundle_profile_version="bundle_profile_v1",
        enable_text_idea=True,
        enable_reroot=True,
        log_level="INFO",
    )


def github_config():
    class Config:
        sample_max_files = 5
        sample_excerpt_chars = 600
        max_file_bytes = 16384
        github_app_id = None
        github_installation_id = None

    return Config()


def packet(message_text: str = KOREAN_LLM_WORKFLOW_TEXT):
    return parse_operator_source_packet(
        {
            "schema_version": "operator_supplied_telegram_source_v1",
            "source_ref": "https://t.me/SynthChannel/12345",
            "posted_at": "2026-06-23T01:02:03Z",
            "message_text": message_text,
        }
    )


@pytest.mark.asyncio
async def test_operator_source_to_analysis_request_flow_uses_real_normalizer_and_assembler() -> None:
    ledger = Ledger()

    report = await run_exact_target_source_to_analysis_materializer(
        ExactTargetSourceToAnalysisRequest(mode="execute", packet=packet()),
        stage_factory=StageFactory(ledger),
    )

    assert report.status == "pass"
    assert report.reason_code == "analysis_request_materialized"
    assert len(ledger.current) == 1
    assert sum(len(rows) for rows in ledger.versions.values()) == 1
    assert [row["event_type"] for row in ledger.outbox] == [
        "source_message.created.v1",
        "candidate.bundle.refresh.v1",
    ]
    assert len(ledger.normalization_runs) == 1
    assert len(ledger.candidates) == 1
    candidate_group_id = next(iter(ledger.candidates))
    members = ledger.members[candidate_group_id]
    assert len(members) == 1
    assert members[0].artifact_type == "text_idea"
    assert ledger.text_idea_snapshots_created == 1
    assert len(ledger.bundles) == 1
    assert ledger.bundles[0][1].ready_for_analysis is True
    assert ledger.bundles[0][1].judge_profile == "text_idea_primary"
    assert len(ledger.analysis_outbox) == 1
    assert ledger.commits == ["source_ingest", "normalization", "refresh_event", "assembler"]


@pytest.mark.asyncio
async def test_operator_source_url_flow_materializes_provider_evidence_analysis_request() -> None:
    ledger = Ledger()
    ledger.allow_enrichment_requests = True

    report = await run_exact_target_source_to_analysis_materializer(
        ExactTargetSourceToAnalysisRequest(
            mode="execute",
            packet=packet(
                "https://github.com/DietrichGebert/ponytail\n\n"
                "AI가 코드를 작성하기 전에 다음 6단계의 사다리를 거치도록 통제합니다..."
            ),
        ),
        stage_factory=StageFactory(ledger),
    )

    assert report.status == "pass"
    assert report.reason_code == "source_url_provider_evidence_analysis_requested"
    assert report.bundle_refresh_attempted is False
    assert report.provider_enrichment_attempted is True
    assert report.assembler_attempted is True
    assert report.artifact_enrichment_request_created is True
    assert report.provider_snapshot_created is True
    assert report.analysis_request_created is True
    assert report.openai_attempted is False
    assert report.redis_attempted is False
    assert report.telegram_live_read_attempted is False
    assert report.telegram_send_attempted is False
    assert report.external_network_attempted is False
    assert len(ledger.current) == 1
    assert sum(len(rows) for rows in ledger.versions.values()) == 1
    assert [row["event_type"] for row in ledger.outbox] == [
        "source_message.created.v1",
        "artifact.enrich.requested.v1",
        "artifact.snapshot.updated.v1",
    ]
    assert len(ledger.normalization_runs) == 1
    assert len(ledger.candidates) == 1
    candidate_group_id = next(iter(ledger.candidates))
    members = ledger.members[candidate_group_id]
    assert len(members) == 1
    assert members[0].artifact_type == "github_repo"
    assert len(ledger.github_client_calls) >= 5
    assert len(ledger.provider_snapshot_events) == 1
    assert len(ledger.snapshots) == 1
    assert ledger.text_idea_snapshots_created == 0
    assert len(ledger.bundles) == 1
    assert ledger.bundles[0][1].ready_for_analysis is True
    assert ledger.bundles[0][1].judge_profile == "github_primary"
    assert len(ledger.analysis_outbox) == 1
    assert ledger.commits == ["source_ingest", "normalization", "provider_enrichment", "assembler"]
