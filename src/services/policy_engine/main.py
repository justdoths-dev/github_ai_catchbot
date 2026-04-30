from __future__ import annotations

import asyncio
import logging

from .config import PolicyEngineConfig
from .delivery_policy import DeliveryPolicy
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository
from .service import PolicyEngineService
from .verdict_policy import VerdictPolicy
from .worker import PolicyEngineWorker


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("policy-engine")


async def _run() -> int:
    config = PolicyEngineConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    from services.analysis_router.redis_streams import RedisStreamConsumer

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.queue_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )

    class SessionBackedService:
        async def handle_trigger_event(self, trigger_event_id: str):
            async with session_factory.begin() as session:
                repository = PolicyEngineRepository(session)
                service = PolicyEngineService(
                    config,
                    repository=repository,
                    verdict_policy=VerdictPolicy(),
                    delivery_policy=DeliveryPolicy(
                        enable_later_delivery=config.enable_later_delivery,
                        enable_silent_later=config.enable_silent_later,
                    ),
                    notification_intent_builder=NotificationIntentBuilder(config=config),
                    logger=logger,
                )
                return await service.handle_trigger_event(trigger_event_id)

    worker = PolicyEngineWorker(config, consumer=consumer, service=SessionBackedService(), logger=logger)  # type: ignore[arg-type]
    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info("policy_engine_cancelled", extra={"service": "policy-engine", "event": "cancelled"})
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
