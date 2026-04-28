from __future__ import annotations

from uuid import uuid4

import pytest

from services.evidence_assembler.models import BundleRefreshTarget
from services.evidence_assembler.service import EvidenceAssemblerService

from ._fakes import FakeRepository, add_candidate, config, snapshot


@pytest.mark.asyncio
async def test_ready_bundle_emits_analysis_requested() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(repository, candidate_group_id=candidate_group_id, primary_artifact_id=artifact_id, artifact_type="github_repo")
    repository.snapshots[artifact_id] = snapshot(artifact_id, snapshot_type="github_repo", status="ready")
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    await EvidenceAssemblerService(config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    assert len(repository.outbox) == 1
    assert repository.outbox[0]["candidate_group_id"] == candidate_group_id
    assert repository.outbox[0]["judge_profile"] == "github_primary"
    assert repository.outbox[0]["escalation_allowed"] is True
