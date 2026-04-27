from __future__ import annotations

import asyncio
import logging

from .config import XEnricherConfig
from .redis_streams import RedisStreamConsumer
from .repositories import XEnricherRepository
from .response_mapper import XResponseMapper
from .service import XEnricherService
from .url_discovery import XUrlDiscovery
from .worker import XEnricherWorker
from .x_api_client import XApiClient


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("x-enricher")


async def _run() -> int:
    config = XEnricherConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    x_api_client = XApiClient(
        api_base_url=config.x_api_base_url,
        bearer_token=config.x_bearer_token,
        timeout_sec=config.request_timeout_sec,
        request_max_ids=config.request_max_ids,
    )
    consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.queue_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )

    class SessionBackedService:
        async def rehydrate_job(self, trigger_event_id: str):
            async with session_factory() as session:
                repository = XEnricherRepository(session)
                service = XEnricherService(
                    config,
                    repository=repository,
                    x_api_client=x_api_client,
                    response_mapper=XResponseMapper(),
                    url_discovery=XUrlDiscovery(),
                    logger=logger,
                )
                return await service.rehydrate_job(trigger_event_id)

        async def handle_job(self, job):
            async with session_factory() as session:
                async with session.begin():
                    repository = XEnricherRepository(session)
                    service = XEnricherService(
                        config,
                        repository=repository,
                        x_api_client=x_api_client,
                        response_mapper=XResponseMapper(),
                        url_discovery=XUrlDiscovery(),
                        logger=logger,
                    )
                    return await service.handle_job(job)

    worker = XEnricherWorker(config, consumer=consumer, service=SessionBackedService(), logger=logger)
    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info("x_enricher_cancelled", extra={"service": "x-enricher", "event": "cancelled"})
        return 0
    finally:
        await x_api_client.close()
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
