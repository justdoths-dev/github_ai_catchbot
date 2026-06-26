from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import CanonicalArtifact, OutboxEventRow, RedisNormalizeMessage, SourceMessageSnapshot
from services.router_normalizer.service import RouterNormalizerService


FIXTURE_ROOT = Path(__file__).resolve().parents[3] / "fixtures" / "upstream"


class FakeRepository:
    def __init__(self, *, snapshot: SourceMessageSnapshot) -> None:
        self.trigger_event_id = uuid4()
        self.event = OutboxEventRow(
            event_id=self.trigger_event_id,
            event_type="source_message.created.v1",
            aggregate_type="source_message",
            aggregate_id=snapshot.source_message_id,
            dedupe_key=f"source-message:{snapshot.source_message_id}:{snapshot.source_version_no}",
            payload_json={
                "source_message_id": str(snapshot.source_message_id),
                "current_version_no": snapshot.source_version_no,
            },
            status="published",
            created_at=datetime.now(timezone.utc),
        )
        self.snapshot = snapshot
        self.normalization_runs: list[dict] = []
        self.suppression_traces: list[dict] = []
        self.artifacts_by_id: dict[str, UUID] = {}
        self.artifacts: dict[str, CanonicalArtifact] = {}
        self.observations: list[dict] = []
        self.candidate_groups: list[dict] = []
        self.members: list[dict] = []
        self.enrich_events: list[dict] = []

    async def get_outbox_event(self, event_id: UUID):
        return self.event if event_id == self.event.event_id else None

    async def get_current_source_message(self, source_message_id: UUID):
        return self.snapshot if source_message_id == self.snapshot.source_message_id else None

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int):
        raise AssertionError("candidate generation acceptance uses the current fixture version")

    async def upsert_normalization_run(self, **kwargs):
        self.normalization_runs.append(kwargs)
        return uuid4()

    async def insert_suppression_trace(self, **kwargs):
        self.suppression_traces.append(kwargs)

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact):
        artifact_id = self.artifacts_by_id.get(artifact.canonical_id)
        if artifact_id is None:
            artifact_id = uuid4()
            self.artifacts_by_id[artifact.canonical_id] = artifact_id
        self.artifacts[artifact.canonical_id] = artifact
        return artifact_id

    async def insert_artifact_observation_if_absent(self, **kwargs):
        if kwargs not in self.observations:
            self.observations.append(kwargs)

    async def upsert_candidate_group(self, **kwargs):
        group_id = uuid4()
        self.candidate_groups.append({"candidate_group_id": group_id, **kwargs})
        return group_id

    async def upsert_candidate_member(self, **kwargs):
        self.members.append(kwargs)

    async def insert_enrichment_requested_outbox(self, **kwargs):
        if kwargs["artifact"].provider_route is not None:
            self.enrich_events.append(kwargs)


def _config() -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.source.normalize",
        consumer_group="router-normalizer",
        consumer_name="test",
        block_ms=100,
        batch_size=10,
        normalizer_version="test-normalizer",
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


def _load_fixture(name: str) -> SourceMessageSnapshot:
    payload = json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))
    return SourceMessageSnapshot(
        source_message_id=UUID(payload["source_message_id"]),
        source_version_no=int(payload["source_version_no"]),
        text_body=payload.get("text_body"),
        caption_text=payload.get("caption_text"),
        text_surface=payload.get("text_surface"),
        entities_json=payload.get("entities_json"),
        url_surface_json=payload.get("url_surface_json"),
        raw_message_json=payload.get("raw_message_json") or {},
    )


async def _run(snapshot: SourceMessageSnapshot) -> tuple[FakeRepository, object]:
    repository = FakeRepository(snapshot=snapshot)
    service = RouterNormalizerService(_config(), repository=repository)
    result = await service.process_stream_message(
        RedisNormalizeMessage(
            job_id=str(repository.trigger_event_id),
            stage_name="normalize",
            root_object_type="source_message",
            root_object_id=str(snapshot.source_message_id),
            idempotency_key="fixture",
            trigger_event_id=str(repository.trigger_event_id),
        )
    )
    return repository, result


@pytest.mark.asyncio
async def test_github_repo_url_becomes_github_repo_artifact_and_candidate() -> None:
    repository, result = await _run(_load_fixture("source_message_github_repo_signal.json"))

    assert result.candidate_eligible is True
    assert result.trigger_strength == "strong"
    assert set(repository.artifacts_by_id) == {"github:repo:example/example-tool"}
    artifact = repository.artifacts["github:repo:example/example-tool"]
    assert artifact.artifact_type == "github_repo"
    assert artifact.provider_route == "github"
    assert len(repository.candidate_groups) == 1
    assert repository.candidate_groups[0]["dedupe_subject_key"] == "github:repo:example/example-tool"
    assert [member["member_role"] for member in repository.members] == ["primary"]
    assert len(repository.enrich_events) == 1


@pytest.mark.asyncio
async def test_x_link_with_supporting_github_url_keeps_github_primary_and_x_supporting() -> None:
    repository, result = await _run(_load_fixture("source_message_x_with_github_supporting_signal.json"))

    assert result.candidate_eligible is True
    assert set(repository.artifacts_by_id) == {
        "github:repo:example/example-tool",
        "x:post:1881234567890123456",
    }
    assert len(repository.candidate_groups) == 1
    assert repository.candidate_groups[0]["dedupe_subject_key"] == "github:repo:example/example-tool"
    role_by_artifact_id = {member["artifact_id"]: member["member_role"] for member in repository.members}
    assert role_by_artifact_id[repository.artifacts_by_id["github:repo:example/example-tool"]] == "primary"
    assert role_by_artifact_id[repository.artifacts_by_id["x:post:1881234567890123456"]] == "supporting"
    assert {event["artifact"].provider_route for event in repository.enrich_events} == {"github", "x"}


@pytest.mark.asyncio
async def test_ai_alone_signals_but_does_not_create_hard_candidate() -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body="interesting AI",
        caption_text=None,
        text_surface="interesting AI",
        entities_json=[],
        url_surface_json=[],
        raw_message_json={},
    )

    repository, result = await _run(snapshot)

    assert result.signal_detected is True
    assert result.candidate_eligible is False
    assert result.trigger_strength == "weak"
    assert result.suppression_reason_codes == ["ai_without_dev_context"]
    assert repository.artifacts_by_id == {}
    assert repository.candidate_groups == []
    assert repository.enrich_events == []


@pytest.mark.asyncio
async def test_text_only_developer_tool_signal_creates_text_idea_candidate() -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body="new agent CLI for coding workflows",
        caption_text=None,
        text_surface="new agent CLI for coding workflows",
        entities_json=[],
        url_surface_json=[],
        raw_message_json={},
    )

    repository, result = await _run(snapshot)

    assert result.signal_detected is True
    assert result.candidate_eligible is True
    assert result.trigger_strength == "medium"
    assert result.suppression_reason_codes == []
    assert len(repository.artifacts_by_id) == 1
    canonical_id = next(iter(repository.artifacts_by_id))
    artifact = repository.artifacts[canonical_id]
    assert artifact.artifact_type == "text_idea"
    assert artifact.provider_route is None
    assert len(repository.observations) == 1
    assert len(repository.candidate_groups) == 1
    assert repository.candidate_groups[0]["dedupe_subject_key"] == canonical_id
    assert [member["member_role"] for member in repository.members] == ["primary"]
    assert repository.enrich_events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("text", ["cli", "sdk", "api", "mcp", "codegen", "coding assistant", "terminal"])
async def test_low_level_developer_tool_terms_do_not_create_text_idea_candidate(text: str) -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body=text,
        caption_text=None,
        text_surface=text,
        entities_json=[],
        url_surface_json=[],
        raw_message_json={},
    )

    repository, result = await _run(snapshot)

    assert result.signal_detected is True
    assert result.candidate_eligible is False
    assert result.trigger_strength == "weak"
    assert result.suppression_reason_codes != ["developer_tool_signal"]
    assert repository.artifacts_by_id == {}
    assert repository.candidate_groups == []
    assert repository.enrich_events == []


@pytest.mark.asyncio
async def test_generic_agent_hype_stays_suppressed_with_trace() -> None:
    snapshot = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body="agent hype thread",
        caption_text=None,
        text_surface="agent hype thread",
        entities_json=[],
        url_surface_json=[],
        raw_message_json={},
    )

    repository, result = await _run(snapshot)

    assert result.signal_detected is True
    assert result.candidate_eligible is False
    assert result.trigger_strength == "weak"
    assert result.suppression_reason_codes == ["domain_signal_without_candidate_context"]
    assert repository.suppression_traces[0]["reason_code"] == "domain_signal_without_candidate_context"
    assert repository.artifacts_by_id == {}
    assert repository.candidate_groups == []
    assert repository.enrich_events == []


@pytest.mark.asyncio
async def test_same_repo_repost_uses_stable_candidate_subject() -> None:
    first = _load_fixture("source_message_github_repo_signal.json")
    second = SourceMessageSnapshot(
        source_message_id=uuid4(),
        source_version_no=1,
        text_body=first.text_body,
        caption_text=first.caption_text,
        text_surface=first.text_surface,
        entities_json=first.entities_json,
        url_surface_json=first.url_surface_json,
        raw_message_json=first.raw_message_json,
    )

    first_repository, first_result = await _run(first)
    second_repository, second_result = await _run(second)

    assert first_result.candidate_eligible is True
    assert second_result.candidate_eligible is True
    assert first_repository.candidate_groups[0]["dedupe_subject_key"] == "github:repo:example/example-tool"
    assert second_repository.candidate_groups[0]["dedupe_subject_key"] == "github:repo:example/example-tool"
