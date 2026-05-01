from __future__ import annotations

import asyncio
import logging

from .config import MaintenanceConfig
from .redis_streams import RedisStreamConsumer
from .repositories import MaintenanceRepository
from .service import MaintenanceService
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("maintenance")


async def _run() -> int:
    config = MaintenanceConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    maintenance_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.maintenance_queue_name,
        consumer_group=config.maintenance_consumer_group,
        consumer_name=config.maintenance_consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )
    replay_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.replay_queue_name,
        consumer_group=config.replay_consumer_group,
        consumer_name=config.replay_consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )

    class SessionBackedService:
        async def handle_maintenance_trigger_event(self, trigger_event_id: str) -> None:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                await service.handle_maintenance_trigger_event(trigger_event_id)

        async def handle_replay_trigger_event(self, trigger_event_id: str) -> None:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                await service.handle_replay_trigger_event(trigger_event_id)

        async def promote_due_retries_once(self, limit: int | None = None) -> int:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.promote_due_retries_once(limit=limit)

    service = SessionBackedService()
    maintenance_worker = MaintenanceQueueWorker(config, consumer=maintenance_consumer, service=service, logger=logger)
    replay_worker = ReplayQueueWorker(config, consumer=replay_consumer, service=service, logger=logger)
    due_retry_worker = DueRetryPromotionWorker(config, service=service, logger=logger)
    try:
        await asyncio.gather(
            maintenance_worker.run_forever(),
            replay_worker.run_forever(),
            due_retry_worker.run_forever(),
        )
    except asyncio.CancelledError:
        logger.info("maintenance_cancelled", extra={"service": "maintenance", "event": "cancelled"})
        return 0
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
