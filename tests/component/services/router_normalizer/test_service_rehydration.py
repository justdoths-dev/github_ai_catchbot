from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import (
    CanonicalArtifact,
    ExtractedUrl,
    OutboxEventRow,
    RedisNormalizeMessage,
    ResolvedUrl,
    SourceMessageSnapshot,
)
from services.router_normalizer.service import RouterNormalizerService


class FakeRepository:
    def __init__(self, *, event: OutboxEventRow, current_snapshot: SourceMessageSnapshot) -> None:
        self.event = event
        self.current_snapshot = current_snapshot
        self.requested_event_ids: list[UUID] = []
        self.requested_source_ids: list[UUID] = []
        self.normalization_runs: list[dict] = []
        self.suppression_traces: list[dict] = []
        self.artifacts_by_id: dict[str, UUID] = {}
        self.observations: list[dict] = []
        self.candidate_groups: list[dict] = []
        self.members: list[dict] = []
        self.enrich_events: list[dict] = []

    async def get_outbox_event(self, event_id: UUID) -> OutboxEventRow | None:
        self.requested_event_ids.append(event_id)
        return self.event if event_id == self.event.event_id else None

    async def get_current_source_message(self, source_message_id: UUID) -> SourceMessageSnapshot | None:
        self.requested_source_ids.append(source_message_id)
        return self.current_snapshot if source_message_id == self.current_snapshot.source_message_id else None

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int):
        raise AssertionError("current version should be used")

    async def upsert_normalization_run(self, **kwargs) -> UUID:
        self.normalization_runs.append(kwargs)
        return uuid4()

    async def insert_suppression_trace(self, **kwargs) -> None:
        self.suppression_traces.append(kwargs)

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact) -> UUID:
        artifact_id = self.artifacts_by_id.get(artifact.canonical_id)
        if artifact_id is None:
            artifact_id = uuid4()
            self.artifacts_by_id[artifact.canonical_id] = artifact_id
        return artifact_id

    async def insert_artifact_observation_if_absent(self, **kwargs) -> None:
        self.observations.append(kwargs)

    async def upsert_candidate_group(self, **kwargs) -> UUID:
        group_id = uuid4()
        self.candidate_groups.append({"group_id": group_id, **kwargs})
        return group_id

    async def upsert_candidate_member(self, **kwargs) -> None:
        self.members.append(kwargs)

    async def insert_enrichment_requested_outbox(self, **kwargs) -> None:
        if kwargs["artifact"].provider_route is not None:
            self.enrich_events.append(kwargs)


class UnresolvedShortUrlResolver:
    async def resolve(self, url: ExtractedUrl) -> ResolvedUrl:
        return ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=url.observed_url,
            resolved_url=None,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status="short_url_unresolved",
        )


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
async def test_service_rehydrates_from_outbox_and_source_message_not_redis_payload() -> None:
    trigger_event_id = uuid4()
    source_message_id = uuid4()
    redis_root_object_id = uuid4()
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
        text_body="AI",
        caption_text=None,
        text_surface="AI",
        entities_json=None,
        url_surface_json=None,
        raw_message_json={},
    )
    repository = FakeRepository(event=event, current_snapshot=snapshot)
    service = RouterNormalizerService(_config(), repository=repository)

    result = await service.process_stream_message(
        RedisNormalizeMessage(
            job_id=str(trigger_event_id),
            stage_name="normalize",
            root_object_type="source_message",
            root_object_id=str(redis_root_object_id),
            idempotency_key="untrusted",
            trigger_event_id=str(trigger_event_id),
        )
    )

    assert repository.requested_event_ids == [trigger_event_id]
    assert repository.requested_source_ids == [source_message_id]
    assert result.signal_detected is True
    assert result.candidate_eligible is False
    assert repository.suppression_traces[0]["reason_code"] == "ai_without_dev_context"
    assert repository.artifacts_by_id == {}


@pytest.mark.asyncio
async def test_deleted_current_source_message_is_suppressed_without_candidate_creation() -> None:
    trigger_event_id = uuid4()
    source_message_id = uuid4()
    event = OutboxEventRow(
        event_id=trigger_event_id,
        event_type="source_message.deleted.v1",
        aggregate_type="source_message",
        aggregate_id=source_message_id,
        dedupe_key="srcmsg:delete:real:2",
        payload_json={"source_message_id": str(source_message_id), "current_version_no": 2},
        status="published",
        created_at=datetime.now(timezone.utc),
    )
    snapshot = SourceMessageSnapshot(
        source_message_id=source_message_id,
        source_version_no=2,
        text_body="https://github.com/openai/openai-python",
        caption_text=None,
        text_surface="https://github.com/openai/openai-python",
        entities_json=None,
        url_surface_json=[
            {
                "observed_url": "https://github.com/openai/openai-python",
                "source_kind": "entity",
            }
        ],
        raw_message_json={},
        deleted_at=datetime.now(timezone.utc),
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

    assert result.candidate_eligible is False
    assert result.suppression_reason_codes == ["source_message_deleted_current"]
    assert repository.suppression_traces[0]["reason_code"] == "source_message_deleted_current"
    assert repository.artifacts_by_id == {}
    assert repository.candidate_groups == []
    assert repository.enrich_events == []


@pytest.mark.asyncio
async def test_suppressed_link_observations_are_still_persisted() -> None:
    trigger_event_id = uuid4()
    source_message_id = uuid4()
    event = OutboxEventRow(
        event_id=trigger_event_id,
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=source_message_id,
        dedupe_key="srcmsg:create:short:1",
        payload_json={"source_message_id": str(source_message_id), "current_version_no": 1},
        status="published",
        created_at=datetime.now(timezone.utc),
    )
    snapshot = SourceMessageSnapshot(
        source_message_id=source_message_id,
        source_version_no=1,
        text_body="AI https://t.co/example",
        caption_text=None,
        text_surface="AI https://t.co/example",
        entities_json=None,
        url_surface_json=[
            {
                "observed_url": "https://t.co/example",
                "source_kind": "entity",
            }
        ],
        raw_message_json={},
    )
    repository = FakeRepository(event=event, current_snapshot=snapshot)
    service = RouterNormalizerService(
        _config(),
        repository=repository,
        short_url_resolver=UnresolvedShortUrlResolver(),
    )

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

    assert result.signal_detected is True
    assert result.candidate_eligible is False
    assert len(repository.artifacts_by_id) == 1
    assert next(iter(repository.artifacts_by_id)).startswith("short_url_unresolved:")
    assert len(repository.observations) == 1
    assert repository.candidate_groups == []
    assert repository.enrich_events == []
