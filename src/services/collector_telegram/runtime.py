from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone

from .config import CollectorTelegramConfig
from .health import CollectorHealthService
from .models import RuntimeSnapshot


class CollectorRuntime:
    """Runtime orchestration skeleton with health/observability wiring.

    This stage still does not wire concrete TDLib/DB/update flows directly.
    Its responsibility here is to make loop lifecycle and collector-local
    observability explicit and stable.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        health: CollectorHealthService,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._health = health
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._snapshot = RuntimeSnapshot()

    @property
    def snapshot(self) -> RuntimeSnapshot:
        return self._snapshot

    async def run_forever(self) -> None:
        self._logger.info(
            "collector_runtime_starting",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_starting",
                "collector_mode": self._config.collector_mode,
                "app_env": self._config.app_env,
                "stage": "collector_runtime",
            },
        )
        self._snapshot.started_at = datetime.now(timezone.utc)
        self._snapshot.health_state = "starting"

        self._health.mark_starting(note="collector runtime booting")
        self._health.mark_tracked_channels_active(0)
        self._health.mark_authorization_state(None)

        self._tasks = [
            asyncio.create_task(self._authorization_loop(), name="collector.authorization"),
            asyncio.create_task(self._update_ingest_loop(), name="collector.update_ingest"),
            asyncio.create_task(self._reconcile_scheduler_loop(), name="collector.reconcile_scheduler"),
            asyncio.create_task(self._registry_refresh_loop(), name="collector.registry_refresh"),
            asyncio.create_task(self._health_publisher_loop(), name="collector.health"),
        ]

        self._snapshot.health_state = "ready"
        self._health.mark_ready(note="collector runtime loops started")

        try:
            await self._stop_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        already_stopping = self._stop_event.is_set()
        if not already_stopping:
            self._stop_event.set()

        self._snapshot.health_state = "stopped"
        self._health.mark_stopped(note="collector runtime stopping")

        current_task = asyncio.current_task()
        for task in self._tasks:
            if task is current_task:
                continue
            task.cancel()

        for task in self._tasks:
            if task is current_task:
                continue
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._logger.info(
            "collector_runtime_stopped",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_stopped",
                "stage": "collector_runtime",
            },
        )

    async def _authorization_loop(self) -> None:
        await self._idle_loop(
            loop_name="authorization_loop",
            interval_sec=5,
            on_tick=self._authorization_tick,
        )

    async def _update_ingest_loop(self) -> None:
        await self._idle_loop(
            loop_name="update_ingest_loop",
            interval_sec=2,
            on_tick=self._update_ingest_tick,
        )

    async def _reconcile_scheduler_loop(self) -> None:
        await self._idle_loop(
            loop_name="reconcile_scheduler_loop",
            interval_sec=float(self._config.reconcile_interval_sec),
            on_tick=self._reconcile_scheduler_tick,
        )

    async def _registry_refresh_loop(self) -> None:
        await self._idle_loop(
            loop_name="registry_refresh_loop",
            interval_sec=60,
            on_tick=self._registry_refresh_tick,
        )

    async def _health_publisher_loop(self) -> None:
        await self._idle_loop(
            loop_name="health_publisher_loop",
            interval_sec=30,
            on_tick=self._health_publisher_tick,
        )

    async def _idle_loop(self, loop_name: str, *, interval_sec: float, on_tick) -> None:
        self._health.mark_runtime_loop_started(loop_name)
        self._logger.info(
            "collector_loop_started",
            extra={
                "service": "collector-telegram",
                "event": "collector_loop_started",
                "stage": "collector_runtime",
                "loop_name": loop_name,
                "interval_sec": interval_sec,
            },
        )

        try:
            while not self._stop_event.is_set():
                self._snapshot.last_tick_at = datetime.now(timezone.utc)
                self._health.heartbeat()
                await on_tick()
                await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            self._logger.info(
                "collector_loop_cancelled",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_loop_cancelled",
                    "stage": "collector_runtime",
                    "loop_name": loop_name,
                },
            )
            raise
        except Exception:
            self._snapshot.health_state = "degraded"
            self._health.mark_degraded(note=f"loop failure: {loop_name}")
            self._logger.exception(
                "collector_loop_failed",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_loop_failed",
                    "stage": "collector_runtime",
                    "loop_name": loop_name,
                    "status": "failed",
                },
            )
            raise
        finally:
            self._health.mark_runtime_loop_stopped(loop_name)

    async def _authorization_tick(self) -> None:
        # Placeholder until TDLib/auth FSM wiring is connected in the next integration pass.
        if self._health.snapshot().tdlib_authorization_state is None:
            self._health.mark_authorization_state("authorizationStateReady")

    async def _update_ingest_tick(self) -> None:
        return None

    async def _reconcile_scheduler_tick(self) -> None:
        return None

    async def _registry_refresh_tick(self) -> None:
        return None

    async def _health_publisher_tick(self) -> None:
        snapshot = self._health.snapshot_dict()
        self._logger.info(
            "collector_health_snapshot",
            extra={
                "service": "collector-telegram",
                "event": "collector_health_snapshot",
                "stage": "collector_observability",
                **snapshot,
            },
        )
