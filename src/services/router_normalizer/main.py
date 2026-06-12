from __future__ import annotations

import asyncio
import logging

from .config import RouterNormalizerConfig
from .redis_streams import RedisStreamsConsumer
from .repositories import RouterNormalizerRepository
from .service import RouterNormalizerService
from .worker import RouterNormalizerWorker


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("router-normalizer")


async def _run() -> int:
    config = RouterNormalizerConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    consumer = RedisStreamsConsumer(
        redis_client,
        queue_name=config.queue_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )

    class SessionBackedService:
        async def process_stream_message(self, message):
            async with session_factory() as session:
                async with session.begin():
                    repository = RouterNormalizerRepository(session)
                    service = RouterNormalizerService(config, repository=repository, logger=logger)
                    result = await service.process_stream_message(message)
                    logger.info(
                        "router_normalizer_message_processed",
                        extra={
                            "service": "router-normalizer",
                            "event": "router_normalizer_message_processed",
                            "trigger_event_id": message.trigger_event_id,
                            "candidate_eligible": result.candidate_eligible,
                            "artifact_count": result.artifact_count,
                            "candidate_group_count": result.candidate_group_count,
                        },
                    )
                    return result

    worker = RouterNormalizerWorker(
        config,
        consumer=consumer,
        service=SessionBackedService(),
        logger=logger,
    )

    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info(
            "router_normalizer_cancelled",
            extra={"service": "router-normalizer", "event": "cancelled"},
        )
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
