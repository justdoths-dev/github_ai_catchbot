from __future__ import annotations

from uuid import uuid4

import pytest

from services.evidence_assembler.config import EvidenceAssemblerConfig
from services.evidence_assembler.models import (
    AnalysisRequestedOutboxRecord,
    ArtifactRecord,
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    DiscoveredLinkSummary,
    SnapshotRecord,
)
from services.evidence_assembler.service import EvidenceAssemblerService


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _PromotionRepository:
    def __init__(self) -> None:
        self.candidate_group_id = uuid4()
        self.source_message_id = uuid4()
        self.x_artifact_id = uuid4()
        self.x_snapshot_id = uuid4()
        self.github_artifact_id = uuid4()
        self.trigger_event_id = uuid4()
        self.github_url = "https://github.com/example/discovered-promotion"
        self.github_canonical_id = "github:repo:example/discovered-promotion"
        self.members = [
            CandidateMemberRecord(self.x_artifact_id, "x_post", "primary", 0),
        ]
        self.snapshots = {
            self.x_artifact_id: SnapshotRecord(
                snapshot_id=self.x_snapshot_id,
                artifact_id=self.x_artifact_id,
                provider="x",
                snapshot_type="x_post",
                status="ready",
                fetched_at=None,
                content_anchor="xpost:1:1",
            )
        }
        self.upserted_canonical_ids: list[str] = []
        self.supporting_members: list[dict] = []
        self.github_enrich_requests: list[dict] = []
        self.appended_bundles: list[dict] = []
        self.analysis_requests: list[dict] = []
        self.proposal_status = "ready_for_enrich"

    def transaction(self):
        return _Tx()

    async def resolve_refresh_targets(self, trigger_event_id):
        return [
            BundleRefreshTarget(
                self.candidate_group_id,
                trigger_event_id,
                "artifact.snapshot.updated.v1",
                self.x_artifact_id,
                self.x_snapshot_id,
            )
        ]

    async def load_candidate_group(self, candidate_group_id):
        return CandidateGroupRecord(
            candidate_group_id=self.candidate_group_id,
            source_message_id=self.source_message_id,
            source_version_no=1,
            initial_primary_artifact_id=self.x_artifact_id,
            current_primary_artifact_id=self.x_artifact_id,
            proposal_status=self.proposal_status,
            current_bundle_id=None,
        )

    async def load_candidate_members(self, candidate_group_id):
        return list(self.members)

    async def load_current_snapshots(self, artifact_ids):
        wanted = set(artifact_ids)
        return {artifact_id: snapshot for artifact_id, snapshot in self.snapshots.items() if artifact_id in wanted}

    async def load_discovered_links(self, **kwargs):
        return [
            DiscoveredLinkSummary(
                observed_url=self.github_url,
                context_path="root_post.entities.urls[0]",
                discovery_reason="x_post_embedded_link",
                parent_artifact_id=self.x_artifact_id,
                parent_snapshot_id=self.x_snapshot_id,
                depth_remaining=0,
            )
        ]

    async def upsert_artifact_registry(self, artifact):
        self.upserted_canonical_ids.append(artifact.canonical_id)
        return ArtifactRecord(
            artifact_id=self.github_artifact_id,
            artifact_type=artifact.artifact_type,
            canonical_id=artifact.canonical_id,
            canonical_url=artifact.canonical_url,
            normalized_host=artifact.normalized_host,
            artifact_key_json=artifact.artifact_key_json,
            current_snapshot_id=None,
            current_status=None,
        )

    async def insert_supporting_member_if_absent(self, **kwargs):
        self.supporting_members.append(kwargs)
        if not any(member.artifact_id == kwargs["artifact_id"] for member in self.members):
            self.members.append(CandidateMemberRecord(kwargs["artifact_id"], "github_repo", "supporting", 1))

    async def insert_github_enrich_requested_outbox(self, **kwargs):
        self.github_enrich_requests.append(kwargs)

    async def count_reroot_events(self, candidate_group_id):
        return 0

    async def load_existing_bundle(self, **kwargs):
        return None

    async def load_analysis_requested_outbox(self, **kwargs):
        return None

    async def next_bundle_version(self, candidate_group_id):
        return 1

    async def append_bundle(self, **kwargs):
        self.appended_bundles.append(kwargs)
        return uuid4()

    async def update_current_bundle(self, **kwargs):
        pass

    async def insert_analysis_requested_outbox(self, **kwargs):
        self.analysis_requests.append(kwargs)
        return AnalysisRequestedOutboxRecord(event_id=uuid4(), created=True)


def _config() -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig(
        "test",
        "db",
        "redis",
        "q.candidate.bundle",
        "group",
        "consumer",
        1,
        1,
        "bundle_profile_v1",
        True,
        True,
        "INFO",
    )


@pytest.mark.asyncio
async def test_promotes_x_discovered_github_repo_and_defers_analysis_until_github_snapshot_ready() -> None:
    repository = _PromotionRepository()
    service = EvidenceAssemblerService(_config(), repository=repository)  # type: ignore[arg-type]

    result = (await service.handle_trigger_event(repository.trigger_event_id))[0]

    assert result.bundle_id is None
    assert result.ready_for_analysis is False
    assert result.emitted_analysis_requested is False
    assert repository.upserted_canonical_ids == [repository.github_canonical_id]
    assert repository.supporting_members == [
        {
            "candidate_group_id": repository.candidate_group_id,
            "artifact_id": repository.github_artifact_id,
        }
    ]
    assert len(repository.github_enrich_requests) == 1
    assert repository.github_enrich_requests[0]["candidate"].candidate_group_id == repository.candidate_group_id
    assert repository.github_enrich_requests[0]["artifact"].canonical_id == repository.github_canonical_id
    assert repository.github_enrich_requests[0]["depth_budget"] == 0
    assert repository.appended_bundles == []
    assert repository.analysis_requests == []


@pytest.mark.asyncio
async def test_does_not_promote_router_proposed_candidate_status() -> None:
    repository = _PromotionRepository()
    repository.proposal_status = "proposed"
    service = EvidenceAssemblerService(_config(), repository=repository)  # type: ignore[arg-type]

    await service.handle_trigger_event(repository.trigger_event_id)

    assert repository.upserted_canonical_ids == []
    assert repository.supporting_members == []
    assert repository.github_enrich_requests == []
    assert len(repository.appended_bundles) == 1
