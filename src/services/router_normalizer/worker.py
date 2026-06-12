from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

from .config import RouterNormalizerConfig
from .models import RedisNormalizeMessage


class RedisStreamsConsumerProtocol(Protocol):
    async def ensure_group(self) -> None: ...
    async def read_batch(self) -> list[tuple[str, RedisNormalizeMessage]]: ...
    async def ack(self, message_id: str) -> None: ...


class RouterNormalizerServiceProtocol(Protocol):
    async def process_stream_message(self, message: RedisNormalizeMessage) -> object: ...


@dataclass(slots=True, frozen=True)
class WorkerBatchResult:
    processed: int = 0
    acked: int = 0
    failed: int = 0
    skipped: int = 0


class RouterNormalizerWorker:
    def __init__(
        self,
        config: RouterNormalizerConfig,
        *,
        consumer: RedisStreamsConsumerProtocol,
        service: RouterNormalizerServiceProtocol,
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
            "router_normalizer_worker_starting",
            extra={
                "service": "router-normalizer",
                "event": "router_normalizer_worker_starting",
                "queue_name": self._config.queue_name,
                "consumer_group": self._config.consumer_group,
                "consumer_name": self._config.consumer_name,
            },
        )
        try:
            while not self._stop_event.is_set():
                result = await self.run_once()
                if result.processed == 0:
                    await asyncio.sleep(0)
        finally:
            self._logger.info(
                "router_normalizer_worker_stopped",
                extra={"service": "router-normalizer", "event": "router_normalizer_worker_stopped"},
            )

    async def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> WorkerBatchResult:
        messages = await self._consumer.read_batch()
        if not messages:
            return WorkerBatchResult()

        processed = 0
        acked = 0
        failed = 0
        skipped = 0
        for message_id, message in messages:
            processed += 1
            if not message.trigger_event_id:
                skipped += 1
                await self._consumer.ack(message_id)
                acked += 1
                self._logger.error(
                    "router_normalizer_stream_missing_trigger_event_id",
                    extra={
                        "service": "router-normalizer",
                        "event": "router_normalizer_stream_missing_trigger_event_id",
                        "stream_message_id": message_id,
                    },
                )
                continue
            try:
                await self._process_message(message)
            except Exception as exc:
                failed += 1
                self._logger.error(
                    "router_normalizer_stream_message_failed",
                    extra={
                        "service": "router-normalizer",
                        "event": "router_normalizer_stream_message_failed",
                        "stream_message_id": message_id,
                        "failure_class": type(exc).__name__,
                    },
                )
                continue
            await self._consumer.ack(message_id)
            acked += 1

        return WorkerBatchResult(processed=processed, acked=acked, failed=failed, skipped=skipped)

    async def _process_message(self, message: RedisNormalizeMessage) -> None:
        await self._service.process_stream_message(message)
