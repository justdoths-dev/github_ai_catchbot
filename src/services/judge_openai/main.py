from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from .config import JudgeOpenAIConfig
from .openai_client import OpenAIJudgeClient
from .repositories import JudgeOpenAIRepository
from .service import JudgeOpenAIService
from .worker import JudgeOpenAIWorker


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("judge-openai")


class RuntimeScopedJudgeRepository:
    def __init__(self, session_factory: Any) -> None:
        self._session_factory = session_factory
        self._active_repository: JudgeOpenAIRepository | None = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[JudgeOpenAIRepository]:
        if self._active_repository is not None:
            yield self._active_repository
            return

        previous_repository = self._active_repository
        async with self._session_factory.begin() as session:
            self._active_repository = JudgeOpenAIRepository(session)
            try:
                yield self._active_repository
            finally:
                self._active_repository = previous_repository

    async def _call_repository(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        if self._active_repository is not None:
            method = getattr(self._active_repository, method_name)
            return await method(*args, **kwargs)

        async with self._session_factory.begin() as session:
            repository = JudgeOpenAIRepository(session)
            method = getattr(repository, method_name)
            return await method(*args, **kwargs)

    async def load_job_by_trigger_event_id(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("load_job_by_trigger_event_id", *args, **kwargs)

    async def load_judge_run(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("load_judge_run", *args, **kwargs)

    async def load_bundle_context(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("load_bundle_context", *args, **kwargs)

    async def mark_judge_run_running(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("mark_judge_run_running", *args, **kwargs)

    async def increment_schema_retry_count(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("increment_schema_retry_count", *args, **kwargs)

    async def finish_judge_run(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("finish_judge_run", *args, **kwargs)

    async def insert_judge_output(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("insert_judge_output", *args, **kwargs)

    async def insert_judge_output_ready_outbox(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call_repository("insert_judge_output_ready_outbox", *args, **kwargs)


class SessionBackedJudgeOpenAIService:
    def __init__(
        self,
        config: JudgeOpenAIConfig,
        *,
        session_factory: Any,
        openai_client: OpenAIJudgeClient,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._session_factory = session_factory
        self._openai_client = openai_client
        self._logger = logger

    async def handle_trigger_event(self, trigger_event_id: str) -> None:
        repository = RuntimeScopedJudgeRepository(self._session_factory)
        service = JudgeOpenAIService(
            self._config,
            repository=repository,  # type: ignore[arg-type]
            openai_client=self._openai_client,
            logger=self._logger,
        )
        await service.handle_trigger_event(trigger_event_id)


async def _run() -> int:
    config = JudgeOpenAIConfig.from_env()
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
    openai_client = OpenAIJudgeClient(
        api_key=config.openai_api_key,
        project=config.openai_project,
        timeout_sec=config.request_timeout_sec,
    )

    service = SessionBackedJudgeOpenAIService(
        config,
        session_factory=session_factory,
        openai_client=openai_client,
        logger=logger,
    )
    worker = JudgeOpenAIWorker(config, consumer=consumer, service=service, logger=logger)  # type: ignore[arg-type]
    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info("judge_openai_cancelled", extra={"service": "judge-openai", "event": "cancelled"})
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
