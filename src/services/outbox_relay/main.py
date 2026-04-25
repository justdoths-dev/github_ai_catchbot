from __future__ import annotations

import asyncio
import logging

from .config import OutboxRelayConfig
from .redis_streams import RedisStreamsPublisher
from .repositories import OutboxRelayRepository
from .routing import OutboxRouteResolver
from .service import OutboxRelayService


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("outbox-relay")


async def _run() -> int:
    config = OutboxRelayConfig.from_env()
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    try:
        async with session_factory() as session:
            repository = OutboxRelayRepository(session)
            publisher = RedisStreamsPublisher(redis_client, maxlen=config.xadd_maxlen)
            service = OutboxRelayService(
                config,
                repository=repository,
                publisher=publisher,
                route_resolver=OutboxRouteResolver(),
                logger=logger,
            )
            await service.run_forever()
    except asyncio.CancelledError:
        logger.info(
            "outbox_relay_cancelled",
            extra={"service": "outbox-relay", "event": "cancelled"},
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
