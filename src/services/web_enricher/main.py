from __future__ import annotations

import asyncio
import logging

from .article_parser import ArticleParser
from .config import WebEnricherConfig
from .redis_streams import RedisStreamConsumer
from .repositories import WebEnricherRepository
from .service import WebEnricherService
from .url_discovery import WebUrlDiscovery
from .web_fetch_client import WebFetchClient
from .worker import WebEnricherWorker


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("web-enricher")


async def _run() -> int:
    config = WebEnricherConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    fetch_client = WebFetchClient(
        timeout_sec=config.request_timeout_sec,
        max_redirects=config.max_redirects,
        max_bytes=config.max_bytes,
        user_agent=config.user_agent,
        content_type_allowlist=config.content_type_allowlist,
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
                repository = WebEnricherRepository(session)
                service = WebEnricherService(
                    config,
                    repository=repository,
                    fetch_client=fetch_client,
                    article_parser=ArticleParser(
                        excerpt_chars=config.excerpt_chars,
                        max_outbound_links=config.max_outbound_links,
                    ),
                    url_discovery=WebUrlDiscovery(),
                    logger=logger,
                )
                return await service.rehydrate_job(trigger_event_id)

        async def handle_job(self, job):
            async with session_factory() as session:
                repository = WebEnricherRepository(session)
                service = WebEnricherService(
                    config,
                    repository=repository,
                    fetch_client=fetch_client,
                    article_parser=ArticleParser(
                        excerpt_chars=config.excerpt_chars,
                        max_outbound_links=config.max_outbound_links,
                    ),
                    url_discovery=WebUrlDiscovery(),
                    logger=logger,
                )
                return await service.handle_job(job)

    worker = WebEnricherWorker(config, consumer=consumer, service=SessionBackedService(), logger=logger)
    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info("web_enricher_cancelled", extra={"service": "web-enricher", "event": "cancelled"})
        return 0
    finally:
        await fetch_client.close()
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
