from __future__ import annotations

import asyncio
import logging

from .config import GhEnricherConfig
from .fetch_planner import GitHubFetchPlanner
from .file_sampler import GitHubFileSampler
from .github_app_auth import GitHubAppTokenProvider
from .github_client import GitHubClient
from .redis_streams import RedisStreamConsumer
from .repositories import GhEnricherRepository
from .service import GhEnricherService
from .url_discovery import GitHubUrlDiscovery
from .worker import GhEnricherWorker


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("gh-enricher")


async def _run() -> int:
    config = GhEnricherConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    token_provider = None
    if config.github_app_id and config.github_installation_id and config.github_private_key:
        token_provider = GitHubAppTokenProvider(
            app_id=config.github_app_id,
            installation_id=config.github_installation_id,
            private_key_pem=config.github_private_key,
            api_base_url=config.github_api_base_url,
            timeout_sec=config.request_timeout_sec,
        )

    github_client = GitHubClient(
        api_base_url=config.github_api_base_url,
        timeout_sec=config.request_timeout_sec,
        token_provider=token_provider,
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
                repository = GhEnricherRepository(session)
                service = GhEnricherService(
                    config,
                    repository=repository,
                    github_client=github_client,
                    fetch_planner=GitHubFetchPlanner(),
                    file_sampler=GitHubFileSampler(),
                    url_discovery=GitHubUrlDiscovery(),
                    logger=logger,
                )
                return await service.rehydrate_job(trigger_event_id)

        async def handle_job(self, job):
            async with session_factory() as session:
                async with session.begin():
                    repository = GhEnricherRepository(session)
                    service = GhEnricherService(
                        config,
                        repository=repository,
                        github_client=github_client,
                        fetch_planner=GitHubFetchPlanner(),
                        file_sampler=GitHubFileSampler(),
                        url_discovery=GitHubUrlDiscovery(),
                        logger=logger,
                    )
                    return await service.handle_job(job)

    service = SessionBackedService()
    worker = GhEnricherWorker(config, consumer=consumer, service=service, logger=logger)
    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info("gh_enricher_cancelled", extra={"service": "gh-enricher", "event": "cancelled"})
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
