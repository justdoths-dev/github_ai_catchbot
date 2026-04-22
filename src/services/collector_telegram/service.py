from __future__ import annotations

import asyncio
import contextlib
import logging

from .config import CollectorTelegramConfig
from .health import CollectorHealthService
from .models import CollectorLifecycleState, CollectorServiceSnapshot
from .runtime import CollectorRuntime
from .singleton_guard import CollectorSingletonGuard


class CollectorTelegramService:
    def __init__(
        self,
        config: CollectorTelegramConfig,
        runtime: CollectorRuntime | None = None,
        *,
        singleton_guard: CollectorSingletonGuard | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._runtime = runtime or CollectorRuntime(
            config,
            health=CollectorHealthService(logger=self._logger.getChild("health")),
            logger=self._logger.getChild("runtime"),
        )
        self._singleton_guard = singleton_guard or CollectorSingletonGuard(
            lock_path=config.singleton_lock_path,
        )
        self._started = False
        self._runtime_task: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Task[None] | None = None
        self._state = CollectorLifecycleState.CREATED
        self._stop_reason: str | None = None
        self._heartbeat_count = 0

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
            started_at=self._runtime.snapshot.started_at,
        )

    async def start(self) -> None:
        if self._started:
            return

        self._state = CollectorLifecycleState.STARTING
        self._config.validate()
        self._config.ensure_runtime_dirs()

        if self._config.collector_mode == "live":
            self._singleton_guard.acquire()

        self._logger.info(
            "collector_service_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_starting",
                "collector_mode": self._config.collector_mode,
                "app_env": self._config.app_env,
                "stage": "collector_acceptance_hardening",
            },
        )

        try:
            await self._runtime.startup_acceptance_check()
        except Exception:
            self._state = CollectorLifecycleState.FAILING
            if self._config.collector_mode == "live":
                self._singleton_guard.release()
            raise

        self._started = True
        self._state = CollectorLifecycleState.RUNNING
        self._runtime_task = asyncio.create_task(self._run_runtime(), name="collector.service.runtime")

    async def run(self) -> None:
        if not self._started:
            await self.start()

        if self._runtime_task is not None:
            await self._runtime_task

    async def stop(self) -> None:
        if not self._started and self._runtime_task is None:
            return

        self._state = CollectorLifecycleState.STOPPING
        self._logger.info(
            "collector_service_stopping",
            extra={
                "service": "collector-telegram",
                "event": "collector_service_stopping",
                "stage": "collector_acceptance_hardening",
            },
        )
        try:
            await self._runtime.shutdown()
            runtime_task = self._runtime_task
            if runtime_task is not None and runtime_task is not asyncio.current_task():
                with contextlib.suppress(asyncio.CancelledError):
                    await runtime_task
        finally:
            if self._config.collector_mode == "live":
                self._singleton_guard.release()
            self._started = False
            self._runtime_task = None
            self._stop_task = None
            self._state = CollectorLifecycleState.STOPPED

    def request_stop(self, reason: str) -> None:
        self._stop_reason = reason
        self._stop_task = asyncio.create_task(self.stop(), name="collector.service.stop")

    async def wait_closed(self) -> CollectorServiceSnapshot:
        stop_task = self._stop_task
        if stop_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await stop_task
        runtime_task = self._runtime_task
        if runtime_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await runtime_task
        return self.snapshot()

    async def _run_runtime(self) -> None:
        self._heartbeat_count += 1
        try:
            await self._runtime.run_forever()
        except Exception:
            self._state = CollectorLifecycleState.FAILING
            raise
