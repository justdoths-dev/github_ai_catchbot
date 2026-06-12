from __future__ import annotations

from uuid import uuid4

import pytest

from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import NormalizationResult, RedisNormalizeMessage
from services.router_normalizer.worker import RouterNormalizerWorker


class RecordingConsumer:
    def __init__(self, messages: list[tuple[str, RedisNormalizeMessage]]) -> None:
        self._messages = messages
        self.acked: list[str] = []
        self.ensure_group_calls = 0
        self.read_calls = 0

    async def ensure_group(self) -> None:
        self.ensure_group_calls += 1

    async def read_batch(self) -> list[tuple[str, RedisNormalizeMessage]]:
        self.read_calls += 1
        messages = self._messages
        self._messages = []
        return messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class RecordingService:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[RedisNormalizeMessage] = []

    async def process_stream_message(self, message: RedisNormalizeMessage) -> NormalizationResult:
        self.calls.append(message)
        if self.fail:
            raise ValueError("redacted test failure")
        return NormalizationResult(
            normalization_run_id=uuid4(),
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="strong",
            artifact_count=1,
            candidate_group_count=1,
            suppression_reason_codes=[],
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
        normalizer_version="worker-once-test-v1",
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


def _message(*, trigger_event_id: str | None = None) -> RedisNormalizeMessage:
    event_id = trigger_event_id if trigger_event_id is not None else str(uuid4())
    return RedisNormalizeMessage(
        job_id=event_id,
        stage_name="normalize",
        root_object_type="source_message",
        root_object_id=str(uuid4()),
        idempotency_key=f"srcmsg:create:{uuid4()}",
        trigger_event_id=event_id,
    )


@pytest.mark.asyncio
async def test_run_once_empty_batch_returns_zero_counts_without_group_setup() -> None:
    consumer = RecordingConsumer([])
    service = RecordingService()
    worker = RouterNormalizerWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 0
    assert result.acked == 0
    assert result.failed == 0
    assert result.skipped == 0
    assert consumer.read_calls == 1
    assert consumer.ensure_group_calls == 0
    assert consumer.acked == []
    assert service.calls == []


@pytest.mark.asyncio
async def test_run_once_valid_thin_message_calls_service_and_acks_once() -> None:
    message = _message()
    consumer = RecordingConsumer([("1-0", message)])
    service = RecordingService()
    worker = RouterNormalizerWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert result.failed == 0
    assert result.skipped == 0
    assert service.calls == [message]
    assert consumer.acked == ["1-0"]


@pytest.mark.asyncio
async def test_run_once_missing_trigger_event_id_is_skipped_and_acked_as_malformed() -> None:
    message = _message(trigger_event_id="")
    consumer = RecordingConsumer([("2-0", message)])
    service = RecordingService()
    worker = RouterNormalizerWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert result.failed == 0
    assert result.skipped == 1
    assert service.calls == []
    assert consumer.acked == ["2-0"]


@pytest.mark.asyncio
async def test_run_once_service_failure_is_failed_without_ack() -> None:
    message = _message(trigger_event_id="not-a-uuid")
    consumer = RecordingConsumer([("3-0", message)])
    service = RecordingService(fail=True)
    worker = RouterNormalizerWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 0
    assert result.failed == 1
    assert result.skipped == 0
    assert service.calls == [message]
    assert consumer.acked == []
