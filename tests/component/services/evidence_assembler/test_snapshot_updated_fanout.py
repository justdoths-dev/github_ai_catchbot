from __future__ import annotations

from uuid import uuid4

import pytest

from services.evidence_assembler.models import BundleRefreshTarget
from services.evidence_assembler.service import EvidenceAssemblerService

from ._fakes import FakeRepository, add_candidate, config, snapshot


@pytest.mark.asyncio
async def test_snapshot_updated_fans_out_to_all_candidate_members() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    first_candidate = uuid4()
    second_candidate = uuid4()
    add_candidate(repository, candidate_group_id=first_candidate, primary_artifact_id=artifact_id, artifact_type="github_repo")
    add_candidate(repository, candidate_group_id=second_candidate, primary_artifact_id=artifact_id, artifact_type="github_repo")
    repository.snapshots[artifact_id] = snapshot(artifact_id)
    repository.targets = [
        BundleRefreshTarget(first_candidate, trigger_event_id, "artifact.snapshot.updated.v1", artifact_id, uuid4()),
        BundleRefreshTarget(second_candidate, trigger_event_id, "artifact.snapshot.updated.v1", artifact_id, uuid4()),
    ]

    await EvidenceAssemblerService(config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    assert [draft.candidate_group_id for _, draft in repository.bundles] == [first_candidate, second_candidate]
