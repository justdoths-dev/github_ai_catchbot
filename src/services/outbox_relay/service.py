from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from .config import OutboxRelayConfig
from .models import OutboxEventRow, QueueRoute, RedisQueuedMessage, redis_queued_message_from_outbox_row
from .redis_streams import RedisStreamsPublisher
from .repositories import OutboxRelayRepository
from .routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError


class OutboxRelayService:
    """Single-worker outbox relay.

    v0.1 assumptions:
    - one relay instance on the single VPS,
    - no distributed claim state in `event_outbox` yet,
    - correctness and thin-message routing first, scale-out later.
    """

    def __init__(
        self,
        config: OutboxRelayConfig,
        *,
        repository: OutboxRelayRepository,
        publisher: RedisStreamsPublisher,
        route_resolver: OutboxRouteResolver,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._publisher = publisher
        self._route_resolver = route_resolver
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        self._logger.info(
            "outbox_relay_starting",
            extra={
                "service": "outbox-relay",
                "event": "outbox_relay_starting",
                "batch_size": self._config.batch_size,
                "poll_interval_ms": self._config.poll_interval_ms,
            },
        )
        try:
            while not self._stop_event.is_set():
                processed = await self.run_once()
                if processed == 0:
                    await asyncio.sleep(self._config.poll_interval_ms / 1000.0)
        finally:
            self._logger.info(
                "outbox_relay_stopped",
                extra={"service": "outbox-relay", "event": "outbox_relay_stopped"},
            )

    async def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> int:
        rows = await self._repository.fetch_pending_batch(limit=self._config.batch_size)
        processed = 0
        for row in rows:
            processed += 1
            await self._process_row(row)
        return processed

    async def _process_row(self, row: OutboxEventRow) -> None:
        try:
            route = self._route_resolver.resolve(row)
            message = self._build_stream_message(row, route)
            redis_message_id = await self._publisher.publish(route, message)
            await self._repository.mark_published(
                event_id=row.event_id,
                published_at=datetime.now(timezone.utc),
            )
            await self._repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
            self._logger.info(
                "outbox_event_published",
                extra={
                    "service": "outbox-relay",
                    "event": "outbox_event_published",
                    "event_id": str(row.event_id),
                    "event_type": row.event_type,
                    "queue_name": route.queue_name,
                    "redis_message_id": redis_message_id,
                },
            )
        except UnsupportedOutboxEventTypeError as exc:
            await self._repository.mark_failed(event_id=row.event_id, error_text=str(exc))
            await self._repository.insert_job_attempt(
                stage_name="outbox_route",
                queue_name="unsupported",
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="failed_terminal",
                error_code="unsupported_event_type",
            )
            self._logger.exception(
                "outbox_event_unsupported",
                extra={
                    "service": "outbox-relay",
                    "event": "outbox_event_unsupported",
                    "event_id": str(row.event_id),
                    "event_type": row.event_type,
                },
            )
        except Exception as exc:  # pragma: no cover - exercised via component test
            await self._repository.mark_failed(event_id=row.event_id, error_text=str(exc))
            route = self._safe_route(row)
            await self._repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="failed_retryable",
                error_code=type(exc).__name__,
            )
            self._logger.exception(
                "outbox_event_publish_failed",
                extra={
                    "service": "outbox-relay",
                    "event": "outbox_event_publish_failed",
                    "event_id": str(row.event_id),
                    "event_type": row.event_type,
                },
            )

    def _build_stream_message(self, row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
        return redis_queued_message_from_outbox_row(row, route)

    def _safe_route(self, row: OutboxEventRow) -> QueueRoute:
        try:
            return self._route_resolver.resolve(row)
        except Exception:
            return QueueRoute(queue_name="unknown", stage_name="outbox_route")
