from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .config import WebEnricherConfig
from .redis_streams import RedisStreamConsumer, StreamMessage
from .service import WebEnricherService


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0


class WebEnricherWorker:
    def __init__(
        self,
        config: WebEnricherConfig,
        *,
        consumer: RedisStreamConsumer,
        service: WebEnricherService,
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
            "web_enricher_worker_started",
            extra={
                "service": "web-enricher",
                "event": "web_enricher_worker_started",
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
                "web_enricher_stream_missing_trigger_event_id",
                extra={"service": "web-enricher", "event": "web_enricher_stream_missing_trigger_event_id"},
            )
            return
        job = await self._service.rehydrate_job(trigger_event_id)
        if job is None:
            self._logger.warning(
                "web_enricher_missing_outbox_job",
                extra={
                    "service": "web-enricher",
                    "event": "web_enricher_missing_outbox_job",
                    "trigger_event_id": trigger_event_id,
                },
            )
            return
        await self._service.handle_job(job)
