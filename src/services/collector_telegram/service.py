from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone

from .config import CollectorTelegramConfig
from .health import CollectorHealthService
from .models import CollectorLifecycleState, CollectorServiceSnapshot
from .runtime import CollectorRuntime


class CollectorTelegramService:
    def __init__(
        self,
        config: CollectorTelegramConfig,
        runtime: CollectorRuntime | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._runtime = runtime or CollectorRuntime(
            config,
            health=CollectorHealthService(),
            logger=self._logger.getChild("runtime"),
        )

        self._state = CollectorLifecycleState.CREATED
        self._started_at: datetime | None = None
        self._stop_reason: str | None = None
        self._heartbeat_count = 0

        self._runtime_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

    @property
    def state(self) -> CollectorLifecycleState:
        return self._state

    def snapshot(self) -> CollectorServiceSnapshot:
        return CollectorServiceSnapshot(
            lifecycle_state=self._state,
            app_env=self._config.app_env,
            collector_mode=self._config.collector_mode,
            stop_reason=self._stop_reason,
            heartbeat_count=self._heartbeat_count,
            started_at=self._started_at,
        )

    async def start(self) -> None:
        if self._state in {
            CollectorLifecycleState.STARTING,
            CollectorLifecycleState.RUNNING,
            CollectorLifecycleState.STOPPING,
        }:
            return

        self._config.validate()
        self._config.ensure_runtime_dirs()

        self._logger.info(
            "collector_service_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_starting",
                "collector_mode": str(self._config.collector_mode),
                "app_env": str(self._config.app_env),
            },
        )

        self._started_at = self._started_at or datetime.now(timezone.utc)
        self._state = CollectorLifecycleState.STARTING

        self._runtime_task = asyncio.create_task(
            self._runtime.run_forever(),
            name="collector-telegram-runtime",
        )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name="collector-telegram-heartbeat",
        )

        self._state = CollectorLifecycleState.RUNNING

    async def run(self) -> None:
        await self.start()
        await self.wait_closed()

    def request_stop(self, reason: str = "requested") -> None:
        if self._stop_reason is None:
            self._stop_reason = reason

        if self._state == CollectorLifecycleState.CREATED:
            self._state = CollectorLifecycleState.STOPPED
            return

        if self._state in {
            CollectorLifecycleState.STARTING,
            CollectorLifecycleState.RUNNING,
        }:
            self._state = CollectorLifecycleState.STOPPING

        if self._runtime_task is not None and not self._runtime_task.done():
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._runtime.shutdown())
            except RuntimeError:
                pass

    async def stop(self) -> None:
        self.request_stop(self._stop_reason or "service_stop")
        await self.wait_closed()

    async def wait_closed(self) -> CollectorServiceSnapshot:
        if self._runtime_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._runtime_task

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task

        self._state = CollectorLifecycleState.STOPPED

        self._logger.info(
            "collector_service_stopped",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_stopped",
                "stop_reason": self._stop_reason,
                "heartbeat_count": self._heartbeat_count,
            },
        )
        return self.snapshot()

    async def _heartbeat_loop(self) -> None:
        try:
            while self._state in {
                CollectorLifecycleState.STARTING,
                CollectorLifecycleState.RUNNING,
                CollectorLifecycleState.STOPPING,
            }:
                self._heartbeat_count += 1
                await asyncio.sleep(0.02)
        except asyncio.CancelledError:
            raise