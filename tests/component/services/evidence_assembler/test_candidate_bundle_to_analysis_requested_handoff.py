from __future__ import annotations

from uuid import uuid4

import pytest

from services.evidence_assembler.models import BundleRefreshTarget
from services.evidence_assembler.service import EvidenceAssemblerService

from ._fakes import FakeRepository, add_candidate, config, snapshot


ALLOWED_JUDGE_PROFILES = {"github_primary", "x_primary", "text_idea_primary"}


@pytest.mark.asyncio
async def test_ready_snapshot_produces_ready_bundle_and_analysis_requested_handoff() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(repository, candidate_group_id=candidate_group_id, primary_artifact_id=artifact_id, artifact_type="github_repo")
    repository.snapshots[artifact_id] = snapshot(artifact_id, snapshot_type="github_repo", status="ready")
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    results = await EvidenceAssemblerService(config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    assert len(results) == 1
    assert results[0].ready_for_analysis is True
    assert results[0].emitted_analysis_requested is True
    assert len(repository.bundles) == 1
    bundle_id, bundle = repository.bundles[0]
    assert bundle.ready_for_analysis is True
    assert bundle.judge_profile == "github_primary"
    assert bundle.judge_profile in ALLOWED_JUDGE_PROFILES
    assert len(bundle.members) == 1
    assert repository.outbox == [
        {
            "candidate_group_id": candidate_group_id,
            "bundle_id": bundle_id,
            "judge_profile": "github_primary",
            "escalation_allowed": True,
        }
    ]


@pytest.mark.asyncio
async def test_duplicate_bundle_input_hash_reuses_bundle_without_reemitting_analysis_requested() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(repository, candidate_group_id=candidate_group_id, primary_artifact_id=artifact_id, artifact_type="github_repo")
    repository.snapshots[artifact_id] = snapshot(artifact_id, snapshot_type="github_repo", status="ready")
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]
    service = EvidenceAssemblerService(config(), repository=repository)  # type: ignore[arg-type]

    first = await service.handle_trigger_event(trigger_event_id)
    second = await service.handle_trigger_event(trigger_event_id)

    assert first[0].emitted_analysis_requested is True
    assert second[0].reused_existing_bundle is True
    assert second[0].emitted_analysis_requested is False
    assert len(repository.bundles) == 1
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
async def test_low_evidence_non_text_primary_keeps_limitation_and_can_handoff() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(repository, candidate_group_id=candidate_group_id, primary_artifact_id=artifact_id, artifact_type="x_post")
    repository.snapshots[artifact_id] = snapshot(artifact_id, snapshot_type="x_post", status="low_evidence")
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    results = await EvidenceAssemblerService(config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    assert len(results) == 1
    assert results[0].ready_for_analysis is True
    assert results[0].emitted_analysis_requested is True
    assert repository.bundles[0][1].ready_for_analysis is True
    assert repository.bundles[0][1].judge_profile == "x_primary"
    assert "low_evidence" in repository.bundles[0][1].evidence_limitations
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
async def test_judge_profile_mapping_is_contract_valid_for_text_idea_handoff() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(repository, candidate_group_id=candidate_group_id, primary_artifact_id=artifact_id, artifact_type="text_idea")
    repository.source_text = "AI coding agent workflow with repository tests and examples."
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    results = await EvidenceAssemblerService(config(enable_reroot=False), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    assert len(results) == 1
    assert results[0].ready_for_analysis is True
    assert results[0].emitted_analysis_requested is True
    assert repository.bundles[0][1].judge_profile == "text_idea_primary"
    assert repository.bundles[0][1].judge_profile in ALLOWED_JUDGE_PROFILES
