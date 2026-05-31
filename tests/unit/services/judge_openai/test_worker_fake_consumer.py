from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from services.judge_openai.config import JudgeOpenAIConfig
from services.judge_openai.models import JudgeCallJob, StreamMessage
from services.judge_openai.worker import JudgeOpenAIWorker


def _config() -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url="unused-database",
        redis_url="unused-redis",
        queue_name="q.analysis.judge",
        consumer_group="judge-openai",
        consumer_name="judge-openai-test",
        batch_size=1,
        block_ms=1,
        openai_api_key="unused",
        openai_project=None,
        request_timeout_sec=1.0,
        max_output_tokens=500,
        enable_prompt_guard_preflight=False,
        log_level="INFO",
    )


class FakeConsumer:
    def __init__(self, message: StreamMessage, events: list[str]) -> None:
        self._message = message
        self._events = events
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        self._events.append("ensure_group")

    async def read_batch(self) -> list[StreamMessage]:
        self._events.append("read_batch")
        return [self._message]

    async def ack(self, message_id: str) -> None:
        self._events.append(f"ack:{message_id}")
        self.acked.append(message_id)


class FakeService:
    def __init__(self, job: JudgeCallJob, events: list[str]) -> None:
        self._job = job
        self._events = events
        self.rehydrated: list[str] = []
        self.handled: list[JudgeCallJob] = []

    async def rehydrate_job(self, trigger_event_id: str):
        self._events.append("rehydrate")
        self.rehydrated.append(trigger_event_id)
        return self._job

    async def handle_job(self, job: JudgeCallJob) -> None:
        self._events.append("handle_start")
        await asyncio.sleep(0)
        self.handled.append(job)
        self._events.append("handle_done")


@pytest.mark.asyncio
async def test_worker_rehydrates_thin_message_handles_job_and_acks_after_service_returns() -> None:
    trigger_event_id = uuid4()
    job = JudgeCallJob(
        trigger_event_id=trigger_event_id,
        event_type="judge.call.requested.v1",
        judge_run_id=uuid4(),
        bundle_id=uuid4(),
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_prompt_v1",
        prompt_cache_key="judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1",
    )
    message = StreamMessage(
        stream="q.analysis.judge",
        message_id="1-0",
        fields={"trigger_event_id": str(trigger_event_id)},
    )
    events: list[str] = []
    consumer = FakeConsumer(message, events)
    service = FakeService(job, events)
    worker = JudgeOpenAIWorker(
        _config(),
        consumer=consumer,
        service=service,  # type: ignore[arg-type]
    )

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert service.rehydrated == [str(trigger_event_id)]
    assert service.handled == [job]
    assert consumer.acked == ["1-0"]
    assert events == ["read_batch", "rehydrate", "handle_start", "handle_done", "ack:1-0"]
