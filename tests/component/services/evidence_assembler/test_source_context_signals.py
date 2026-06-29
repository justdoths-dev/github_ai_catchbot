from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.evidence_assembler.models import BundleRefreshTarget, SnapshotRecord
from services.evidence_assembler.service import EvidenceAssemblerService

from ._fakes import FakeRepository, add_candidate, config


URL_SCHEME = "http" + "s://"
RAW_URL = URL_SCHEME + "example.invalid/mcp-tool"
RAW_SOURCE_TEXT = (
    "MCP server setup guide: connect the GitHub repo and use it from your agent. "
    f"See {RAW_URL} for details."
)


@pytest.mark.asyncio
async def test_github_primary_summary_includes_sanitized_source_context_signals() -> None:
    repository = FakeRepository()
    repository.source_text = RAW_SOURCE_TEXT
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(
        repository,
        candidate_group_id=candidate_group_id,
        primary_artifact_id=artifact_id,
        artifact_type="github_repo",
    )
    repository.snapshots[artifact_id] = SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="github",
        snapshot_type="github_repo",
        status="ready",
        fetched_at=None,
        content_anchor="github:repo:example/mcp-tool:ready",
        normalized_projection={"title": "MCP helper"},
    )
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]

    await EvidenceAssemblerService(config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]

    bundle = repository.bundles[0][1]
    primary_summary = bundle.primary_summary
    assert primary_summary["headline"] == "MCP helper"
    assert primary_summary["provider"] == "github"
    assert primary_summary["snapshot_type"] == "github_repo"
    assert primary_summary["status"] == "ready"
    assert primary_summary["source_context_signals"] == {
        "source_text_present": True,
        "source_text_chars_bucket": "121-500",
        "regex_url_count": 1,
        "regex_url_count_capped": False,
        "contains_mcp_token": True,
        "contains_setup_signal": True,
        "contains_connect_signal": True,
        "contains_use_signal": True,
        "signal_count": 4,
    }
    serialized_signals = json.dumps(primary_summary["source_context_signals"], sort_keys=True)
    assert RAW_URL not in serialized_signals
    assert "MCP server setup guide" not in serialized_signals
    assert repository.source_text not in json.dumps(primary_summary, sort_keys=True)


@pytest.mark.asyncio
async def test_source_context_signals_participate_in_bundle_hash_and_reuse() -> None:
    repository = FakeRepository()
    repository.source_text = RAW_SOURCE_TEXT
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    add_candidate(
        repository,
        candidate_group_id=candidate_group_id,
        primary_artifact_id=artifact_id,
        artifact_type="github_repo",
    )
    repository.snapshots[artifact_id] = SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="github",
        snapshot_type="github_repo",
        status="ready",
        fetched_at=None,
        content_anchor="github:repo:example/mcp-tool:ready",
        normalized_projection={"title": "MCP helper"},
    )
    repository.targets = [BundleRefreshTarget(candidate_group_id, trigger_event_id, "candidate.bundle.refresh.v1")]
    service = EvidenceAssemblerService(config(), repository=repository)  # type: ignore[arg-type]

    first = await service.handle_trigger_event(trigger_event_id)
    first_hash = repository.bundles[0][1].bundle_input_hash
    first_snapshot_ids = [member.snapshot_id for member in repository.bundles[0][1].members]
    second = await service.handle_trigger_event(trigger_event_id)

    assert first[0].reused_existing_bundle is False
    assert second[0].reused_existing_bundle is True
    assert len(repository.bundles) == 1
    assert repository.bundles[0][1].bundle_input_hash == first_hash

    repository.source_text = "Repository mention with general notes only."
    third = await service.handle_trigger_event(trigger_event_id)

    assert third[0].reused_existing_bundle is False
    assert len(repository.bundles) == 2
    second_bundle = repository.bundles[1][1]
    assert second_bundle.bundle_input_hash != first_hash
    assert [member.snapshot_id for member in second_bundle.members] == first_snapshot_ids
    assert second_bundle.discovered_links_summary_json == []
    assert second_bundle.primary_summary["source_context_signals"]["contains_mcp_token"] is False
    assert second_bundle.primary_summary["source_context_signals"]["signal_count"] == 0
