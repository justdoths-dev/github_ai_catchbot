from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import CanonicalArtifact, OutboxEventRow, RedisNormalizeMessage, SourceMessageSnapshot
from services.router_normalizer.service import RouterNormalizerService


class FakeRepository:
    def __init__(self, *, event: OutboxEventRow, current_snapshot: SourceMessageSnapshot) -> None:
        self.event = event
        self.current_snapshot = current_snapshot
        self.artifacts_by_id = {}
        self.observations = []
        self.candidate_groups = []
        self.members = []
        self.enrich_events = []

    async def get_outbox_event(self, event_id):
        return self.event if event_id == self.event.event_id else None

    async def get_current_source_message(self, source_message_id):
        return self.current_snapshot if source_message_id == self.current_snapshot.source_message_id else None

    async def get_source_message_version(self, *, source_message_id, version_no):
        raise AssertionError("current version should be used")

    async def upsert_normalization_run(self, **kwargs):
        return uuid4()

    async def insert_suppression_trace(self, **kwargs):
        raise AssertionError("strong candidates should not be suppressed")

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact):
        artifact_id = self.artifacts_by_id.get(artifact.canonical_id)
        if artifact_id is None:
            artifact_id = uuid4()
            self.artifacts_by_id[artifact.canonical_id] = artifact_id
        return artifact_id

    async def insert_artifact_observation_if_absent(self, **kwargs):
        self.observations.append(kwargs)

    async def upsert_candidate_group(self, **kwargs):
        group_id = uuid4()
        self.candidate_groups.append({"group_id": group_id, **kwargs})
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


@pytest.mark.asyncio
async def test_candidate_proposal_flow_writes_artifacts_group_members_and_enrich_outbox() -> None:
    trigger_event_id = uuid4()
    source_message_id = uuid4()
    event = OutboxEventRow(
        event_id=trigger_event_id,
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=source_message_id,
        dedupe_key="srcmsg:create:real:1",
        payload_json={"source_message_id": str(source_message_id), "current_version_no": 1},
        status="published",
        created_at=datetime.now(timezone.utc),
    )
    snapshot = SourceMessageSnapshot(
        source_message_id=source_message_id,
        source_version_no=1,
        text_body="Check this repo file",
        caption_text=None,
        text_surface="Check this repo file",
        entities_json=None,
        url_surface_json=[
            {
                "observed_url": "https://github.com/OpenAI/openai-python/blob/main/src/openai/__init__.py",
                "source_kind": "entity",
            }
        ],
        raw_message_json={},
    )
    repository = FakeRepository(event=event, current_snapshot=snapshot)
    service = RouterNormalizerService(_config(), repository=repository)

    result = await service.process_stream_message(
        RedisNormalizeMessage(
            job_id=str(trigger_event_id),
            stage_name="normalize",
            root_object_type="source_message",
            root_object_id=str(source_message_id),
            idempotency_key="thin",
            trigger_event_id=str(trigger_event_id),
        )
    )

    assert result.candidate_eligible is True
    assert result.trigger_strength == "strong"
    assert set(repository.artifacts_by_id) == {
        "github:repo:openai/openai-python",
        "github:subpath:openai/openai-python:main:src/openai/__init__.py",
    }
    assert len(repository.observations) == 2
    assert len(repository.candidate_groups) == 1
    assert repository.candidate_groups[0]["dedupe_subject_key"] == "github:repo:openai/openai-python"
    assert {member["member_role"] for member in repository.members} == {"primary", "supporting"}
    assert len(repository.enrich_events) == 2
    assert {event["candidate_group_id"] for event in repository.enrich_events} == {
        repository.candidate_groups[0]["group_id"]
    }
    assert {event["artifact"].provider_route for event in repository.enrich_events} == {"github"}


@pytest.mark.asyncio
async def test_candidate_proposal_flow_emits_x_post_contract_to_enrichment_outbox() -> None:
    trigger_event_id = uuid4()
    source_message_id = uuid4()
    event = OutboxEventRow(
        event_id=trigger_event_id,
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=source_message_id,
        dedupe_key="srcmsg:create:x",
        payload_json={"source_message_id": str(source_message_id), "current_version_no": 1},
        status="published",
        created_at=datetime.now(timezone.utc),
    )
    snapshot = SourceMessageSnapshot(
        source_message_id=source_message_id,
        source_version_no=1,
        text_body="Watch this post",
        caption_text=None,
        text_surface="Watch this post",
        entities_json=None,
        url_surface_json=[
            {
                "observed_url": "https://x.com/someone/status/1881234567890123456?s=20",
                "source_kind": "entity",
            }
        ],
        raw_message_json={},
    )
    repository = FakeRepository(event=event, current_snapshot=snapshot)
    service = RouterNormalizerService(_config(), repository=repository)

    result = await service.process_stream_message(
        RedisNormalizeMessage(
            job_id=str(trigger_event_id),
            stage_name="normalize",
            root_object_type="source_message",
            root_object_id=str(source_message_id),
            idempotency_key="thin",
            trigger_event_id=str(trigger_event_id),
        )
    )

    assert result.candidate_eligible is True
    assert set(repository.artifacts_by_id) == {"x:post:1881234567890123456"}
    assert repository.candidate_groups[0]["dedupe_subject_key"] == "x:post:1881234567890123456"
    assert len(repository.enrich_events) == 1
    artifact = repository.enrich_events[0]["artifact"]
    assert artifact.artifact_type == "x_post"
    assert artifact.canonical_id == "x:post:1881234567890123456"
    assert artifact.provider_route == "x"
