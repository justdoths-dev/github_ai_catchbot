from __future__ import annotations

from uuid import uuid4

import pytest

from services.evidence_assembler.models import BundleRefreshTarget
from services.evidence_assembler.service import EvidenceAssemblerService
from services.evidence_assembler.text_idea_builder import TextIdeaBuilder

from ._fakes import FakeRepository, add_candidate, config


@pytest.mark.asyncio
async def test_text_idea_snapshot_reused_for_same_text_surface() -> None:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    text_idea_artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(
        repository,
        candidate_group_id=candidate_group_id,
        primary_artifact_id=text_idea_artifact_id,
        artifact_type="text_idea",
    )
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]
    service = EvidenceAssemblerService(config(), repository=repository)  # type: ignore[arg-type]

    await service.handle_trigger_event(trigger_event_id)
    await service.handle_trigger_event(trigger_event_id)

    assert repository.text_idea_snapshots_created == 1
    assert len(repository.bundles) == 1
    snapshot = next(iter(repository._text_idea_by_anchor.values()))
    draft = TextIdeaBuilder().build(
        artifact_id=text_idea_artifact_id,
        source_message_id=repository.candidates[candidate_group_id].source_message_id,
        source_version_no=repository.candidates[candidate_group_id].source_version_no,
        text_surface=repository.source_text,
    )
    assert draft is not None
    assert snapshot.content_anchor == TextIdeaBuilder.input_hash(draft)
