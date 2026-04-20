"""Lifecycle wrapper around the collector bootstrap runtime."""

from __future__ import annotations

import asyncio
from contextlib import suppress

from .config import CollectorTelegramConfig
from .exceptions import CollectorTelegramLifecycleError, CollectorTelegramRuntimeError
from .models import CollectorLifecycleState, RuntimeSnapshot
from .runtime import CollectorTelegramRuntime

_DEFAULT_SHUTDOWN_GRACE_SECONDS = 15.0


class CollectorTelegramService:
    """Minimal service wrapper used by the collector bootstrap entrypoint."""

    def __init__(
        self,
        config: CollectorTelegramConfig,
        runtime: CollectorTelegramRuntime | None = None,
    ) -> None:
        self._config = config
        self._runtime = runtime or CollectorTelegramRuntime(config)
        self._task: asyncio.Task[RuntimeSnapshot] | None = None

    @property
    def state(self) -> CollectorLifecycleState:
        return self._runtime.state

    @property
    def config(self) -> CollectorTelegramConfig:
        return self._config

    def snapshot(self) -> RuntimeSnapshot:
        return self._runtime.snapshot()

    async def start(self) -> RuntimeSnapshot:
        if self._task is not None and not self._task.done():
            raise CollectorTelegramLifecycleError("service is already running")

        await self._runtime.start()
        self._task = asyncio.create_task(
            self._runtime.serve(),
            name="collector-telegram-runtime",
        )
        await asyncio.sleep(0)
        return self.snapshot()

    async def wait_closed(self) -> RuntimeSnapshot:
        if self._task is None:
            raise CollectorTelegramLifecycleError("service has not been started")
        return await self._task

    async def run(self) -> RuntimeSnapshot:
        await self.start()
        return await self.wait_closed()

    def request_stop(self, reason: str = "stop-requested") -> None:
        self._runtime.request_stop(reason)

    async def stop(self, reason: str = "service-stop") -> RuntimeSnapshot:
        if self._task is None:
            self._runtime.request_stop(reason)
            return self.snapshot()

        self._runtime.request_stop(reason)
        try:
            return await asyncio.wait_for(
                asyncio.shield(self._task),
                timeout=_DEFAULT_SHUTDOWN_GRACE_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            raise CollectorTelegramRuntimeError(
                "collector telegram bootstrap did not stop within the grace window"
            ) from exc
