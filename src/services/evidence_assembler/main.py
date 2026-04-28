from __future__ import annotations

import asyncio
import logging

from .config import EvidenceAssemblerConfig
from .redis_streams import RedisStreamConsumer
from .repositories import EvidenceAssemblerRepository
from .service import EvidenceAssemblerService
from .worker import EvidenceAssemblerWorker


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("evidence-assembler")


async def _run() -> int:
    config = EvidenceAssemblerConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

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
            async with session_factory() as session:
                repository = EvidenceAssemblerRepository(session)
                service = EvidenceAssemblerService(config, repository=repository, logger=logger)
                return await service.handle_trigger_event(trigger_event_id)

    worker = EvidenceAssemblerWorker(config, consumer=consumer, service=SessionBackedService(), logger=logger)
    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info("evidence_assembler_cancelled", extra={"service": "evidence-assembler", "event": "cancelled"})
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
