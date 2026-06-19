from __future__ import annotations

import asyncio
import logging
from typing import Protocol
from uuid import UUID

from .ack_decision import maintenance_result_allows_ack, replay_result_allows_ack
from .config import MaintenanceConfig
from .models import DeliveryReplayDecision, DeliveryResultWorkerResult, StreamMessage, WorkerBatchResult


class RedisStreamConsumerProtocol(Protocol):
    async def ensure_group(self) -> None: ...
    async def read_batch(self) -> list[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...


class MaintenanceServiceProtocol(Protocol):
    async def handle_maintenance_trigger_event(self, trigger_event_id: str | UUID) -> DeliveryResultWorkerResult | None: ...
    async def handle_replay_trigger_event(self, trigger_event_id: str | UUID) -> DeliveryReplayDecision | None: ...
    async def promote_due_retries_once(self, limit: int | None = None) -> int: ...


class MaintenanceQueueWorker:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        consumer: RedisStreamConsumerProtocol,
        service: MaintenanceServiceProtocol,
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
            "maintenance_queue_worker_started",
            extra={
                "service": "maintenance",
                "event": "maintenance_queue_worker_started",
                "queue_name": self._config.maintenance_queue_name,
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
            if await self._process_message(message):
                if await self._ack_message(message):
                    acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)

    async def _process_message(self, message: StreamMessage) -> bool:
        if message.stream != self._config.maintenance_queue_name:
            self._logger.error("maintenance_stream_queue_mismatch")
            return False
        trigger_event_id = _parse_uuid(message.fields.get("trigger_event_id"))
        if trigger_event_id is None:
            self._logger.error("maintenance_stream_missing_trigger_event_id")
            return False
        try:
            result = await self._service.handle_maintenance_trigger_event(trigger_event_id)
        except Exception:
            self._logger.error("maintenance_handler_failed")
            return False
        return maintenance_result_allows_ack(result)

    async def _ack_message(self, message: StreamMessage) -> bool:
        try:
            await self._consumer.ack(message.message_id)
        except Exception:
            self._logger.error("maintenance_stream_ack_failed")
            return False
        return True


class ReplayQueueWorker:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        consumer: RedisStreamConsumerProtocol,
        service: MaintenanceServiceProtocol,
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
            "maintenance_replay_worker_started",
            extra={
                "service": "maintenance",
                "event": "maintenance_replay_worker_started",
                "queue_name": self._config.replay_queue_name,
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
            if await self._process_message(message):
                if await self._ack_message(message):
                    acked += 1
        return WorkerBatchResult(processed=processed, acked=acked)

    async def _process_message(self, message: StreamMessage) -> bool:
        if message.stream != self._config.replay_queue_name:
            self._logger.error("maintenance_replay_stream_queue_mismatch")
            return False
        trigger_event_id = _parse_uuid(message.fields.get("trigger_event_id"))
        if trigger_event_id is None:
            self._logger.error("maintenance_replay_stream_missing_trigger_event_id")
            return False
        try:
            result = await self._service.handle_replay_trigger_event(trigger_event_id)
        except Exception:
            self._logger.error("maintenance_replay_handler_failed")
            return False
        return replay_result_allows_ack(result)

    async def _ack_message(self, message: StreamMessage) -> bool:
        try:
            await self._consumer.ack(message.message_id)
        except Exception:
            self._logger.error("maintenance_replay_stream_ack_failed")
            return False
        return True


class DueRetryPromotionWorker:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        service: MaintenanceServiceProtocol,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._service = service
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        self._logger.info(
            "maintenance_due_retry_worker_started",
            extra={
                "service": "maintenance",
                "event": "maintenance_due_retry_worker_started",
                "poll_sec": self._config.retry_scan_poll_sec,
            },
        )
        while not self._stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._config.retry_scan_poll_sec)
            except asyncio.TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> WorkerBatchResult:
        processed = await self._service.promote_due_retries_once()
        return WorkerBatchResult(processed=processed, acked=0)


def _parse_uuid(value: object) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None
