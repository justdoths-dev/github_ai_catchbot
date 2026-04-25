from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Awaitable

from .models import RedisNormalizeMessage
from .redis_streams import RedisStreamsConsumer


class RouterNormalizerWorker:
    def __init__(
        self,
        *,
        consumer: RedisStreamsConsumer,
        process_message: Callable[[RedisNormalizeMessage], Awaitable[None]],
        logger: logging.Logger | None = None,
    ) -> None:
        self._consumer = consumer
        self._process_message = process_message
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        await self._consumer.ensure_group()
        self._logger.info(
            "router_normalizer_worker_starting",
            extra={"service": "router-normalizer", "event": "router_normalizer_worker_starting"},
        )
        try:
            while not self._stop_event.is_set():
                messages = await self._consumer.read_batch()
                for message_id, message in messages:
                    await self._process_message(message)
                    await self._consumer.ack(message_id)
        finally:
            self._logger.info(
                "router_normalizer_worker_stopped",
                extra={"service": "router-normalizer", "event": "router_normalizer_worker_stopped"},
            )

    async def stop(self) -> None:
        self._stop_event.set()

