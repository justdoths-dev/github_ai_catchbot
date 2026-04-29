from __future__ import annotations

import asyncio
import logging
from typing import Protocol

from .config import JudgeOpenAIConfig
from .models import StreamMessage, WorkerBatchResult
from .service import JudgeOpenAIService


class RedisStreamConsumerProtocol(Protocol):
    async def ensure_group(self) -> None: ...
    async def read_batch(self) -> list[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...


class JudgeOpenAIWorker:
    def __init__(
        self,
        config: JudgeOpenAIConfig,
        *,
        consumer: RedisStreamConsumerProtocol,
        service: JudgeOpenAIService,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._service = service
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        await self._consumer.ensure_group()
        self._logger.info(
            "judge_openai_worker_started",
            extra={
                "service": "judge-openai",
                "event": "judge_openai_worker_started",
                "queue_name": self._config.queue_name,
                "consumer_group": self._config.consumer_group,
                "consumer_name": self._config.consumer_name,
            },
        )
        while not self._stop_event.is_set():
            result = await self.run_once()
            if result.processed == 0:
                await asyncio.sleep(0)

    async def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> WorkerBatchResult:
        messages = await self._consumer.read_batch()
        if not messages:
            return WorkerBatchResult()

        processed = 0
        acked = 0
        for message in messages:
            processed += 1
            await self._process_message(message)
            await self._consumer.ack(message.message_id)
            acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)

    async def _process_message(self, message: StreamMessage) -> None:
        trigger_event_id = message.fields.get("trigger_event_id")
        if not trigger_event_id:
            self._logger.error(
                "judge_openai_stream_missing_trigger_event_id",
                extra={
                    "service": "judge-openai",
                    "event": "judge_openai_stream_missing_trigger_event_id",
                    "stream_message_id": message.message_id,
                },
            )
            return
        await self._service.handle_trigger_event(trigger_event_id)
