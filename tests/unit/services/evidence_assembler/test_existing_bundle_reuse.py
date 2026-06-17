from __future__ import annotations

from uuid import uuid4

import pytest

from services.evidence_assembler.config import EvidenceAssemblerConfig
from services.evidence_assembler.models import (
    AnalysisRequestedOutboxRecord,
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    ExistingBundleRecord,
    SnapshotRecord,
)
from services.evidence_assembler.service import EvidenceAssemblerService


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.candidate_group_id = uuid4()
        self.artifact_id = uuid4()
        self.snapshot_id = uuid4()
        self.existing_bundle_id = uuid4()
        self.existing_analysis_event_id = uuid4()
        self.current_bundle_updates = []
        self.appended_bundles = []
        self.outbox = []

    def transaction(self):
        return _Tx()

    async def resolve_refresh_targets(self, trigger_event_id):
        return [BundleRefreshTarget(self.candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    async def load_candidate_group(self, candidate_group_id):
        return CandidateGroupRecord(
            candidate_group_id=self.candidate_group_id,
            source_message_id=uuid4(),
            source_version_no=1,
            initial_primary_artifact_id=self.artifact_id,
            current_primary_artifact_id=self.artifact_id,
            proposal_status="ready_for_enrich",
            current_bundle_id=None,
        )

    async def load_candidate_members(self, candidate_group_id):
        return [CandidateMemberRecord(self.artifact_id, "github_repo", "primary", 0)]

    async def load_current_snapshots(self, artifact_ids):
        return {
            self.artifact_id: SnapshotRecord(
                self.snapshot_id,
                self.artifact_id,
                "github",
                "github_repo",
                "ready",
                None,
                "anchor",
            )
        }

    async def load_discovered_links(self, **kwargs):
        return []

    async def count_reroot_events(self, candidate_group_id):
        return 0

    async def load_existing_bundle(self, **kwargs):
        return ExistingBundleRecord(
            self.existing_bundle_id,
            self.candidate_group_id,
            1,
            kwargs["bundle_profile_version"],
            kwargs["bundle_input_hash"],
            True,
        )

    async def load_analysis_requested_outbox(self, **kwargs):
        return AnalysisRequestedOutboxRecord(event_id=self.existing_analysis_event_id, created=False)

    async def insert_analysis_requested_outbox(self, **kwargs):
        self.outbox.append(kwargs)
        return AnalysisRequestedOutboxRecord(event_id=self.existing_analysis_event_id, created=False)

    async def update_current_bundle(self, **kwargs):
        self.current_bundle_updates.append(kwargs)


def _config() -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig("test", "db", "redis", "q.candidate.bundle", "group", "consumer", 1, 1, "profile", True, True, "INFO")


@pytest.mark.asyncio
async def test_existing_bundle_is_reused_without_analysis_reemit() -> None:
    repository = FakeRepository()
    service = EvidenceAssemblerService(_config(), repository=repository)  # type: ignore[arg-type]

    result = (await service.handle_trigger_event(uuid4()))[0]

    assert result.bundle_id == repository.existing_bundle_id
    assert result.reused_existing_bundle is True
    assert result.emitted_analysis_requested is False
    assert result.analysis_requested_event_id == repository.existing_analysis_event_id
    assert repository.current_bundle_updates[0]["bundle_id"] == repository.existing_bundle_id
    assert repository.appended_bundles == []
    assert len(repository.outbox) == 1
