"""Non-live runtime scaffold for the collector bootstrap."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from .config import CollectorTelegramConfig
from .exceptions import CollectorTelegramLifecycleError
from .models import CollectorLifecycleState, CollectorMode, RuntimeSnapshot

logger = logging.getLogger(__name__)
_RUNTIME_HEARTBEAT_INTERVAL_SECONDS = 0.05


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


class CollectorTelegramRuntime:
    """Single-process runtime loop scaffold for the collector service."""

    def __init__(self, config: CollectorTelegramConfig) -> None:
        self._config = config
        self._state = CollectorLifecycleState.CREATED
        self._stop_event = asyncio.Event()
        self._started_at: datetime | None = None
        self._stop_requested_at: datetime | None = None
        self._last_tick_at: datetime | None = None
        self._heartbeat_count = 0
        self._stop_reason: str | None = None
        self._pending_stop_reason: str | None = None
        self._pending_stop_requested_at: datetime | None = None

    @property
    def state(self) -> CollectorLifecycleState:
        return self._state

    async def start(self) -> None:
        if self._state not in {CollectorLifecycleState.CREATED, CollectorLifecycleState.STOPPED}:
            raise CollectorTelegramLifecycleError(
                f"runtime cannot start from {self._state.value}"
            )

        pending_stop_reason = self._pending_stop_reason
        pending_stop_requested_at = self._pending_stop_requested_at
        self._pending_stop_reason = None
        self._pending_stop_requested_at = None

        self._stop_event = asyncio.Event()
        self._started_at = _utcnow()
        self._stop_requested_at = None
        self._last_tick_at = None
        self._heartbeat_count = 0
        self._stop_reason = None

        self._state = CollectorLifecycleState.STARTING
        if self._config.collector_mode is CollectorMode.LIVE:
            logger.warning(
                "collector_runtime_live_mode_stub",
                extra={
                    "service": "collector-telegram",
                    "event": "collector_runtime_live_mode_stub",
                    "collector_mode": self._config.collector_mode.value,
                    "env": self._config.app_env.value,
                    "note": "c1 remains bootstrap-only and side-effect free",
                },
            )
        self._state = CollectorLifecycleState.RUNNING
        logger.info(
            "collector_runtime_started",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_started",
                "env": self._config.app_env.value,
                "config": self._config.redacted(),
            },
        )

        if pending_stop_reason is not None:
            self._stop_reason = pending_stop_reason
            self._stop_requested_at = pending_stop_requested_at or _utcnow()
            self._stop_event.set()

    async def serve(self) -> RuntimeSnapshot:
        if self._state is not CollectorLifecycleState.RUNNING:
            raise CollectorTelegramLifecycleError(
                "runtime must be started before serve()"
            )

        try:
            while not self._stop_event.is_set():
                await self._tick_once()
        except asyncio.CancelledError:
            self.request_stop("task-cancelled")
            raise
        finally:
            await self._shutdown()

        return self.snapshot()

    def request_stop(self, reason: str = "stop-requested") -> None:
        requested_at = _utcnow()

        if self._state is CollectorLifecycleState.CREATED:
            self._pending_stop_reason = reason
            self._pending_stop_requested_at = requested_at
            return

        if self._state is CollectorLifecycleState.STOPPED or self._stop_event.is_set():
            return

        self._stop_reason = reason
        self._stop_requested_at = requested_at
        self._stop_event.set()

    def snapshot(self) -> RuntimeSnapshot:
        return RuntimeSnapshot(
            lifecycle_state=self._state,
            app_env=self._config.app_env,
            collector_mode=self._config.collector_mode,
            started_at=self._started_at,
            stop_requested_at=self._stop_requested_at or self._pending_stop_requested_at,
            last_tick_at=self._last_tick_at,
            heartbeat_count=self._heartbeat_count,
            tracked_chat_count=0,
            pending_reconcile_count=0,
            stop_reason=self._stop_reason or self._pending_stop_reason,
        )

    async def _tick_once(self) -> None:
        self._heartbeat_count += 1
        self._last_tick_at = _utcnow()
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=_RUNTIME_HEARTBEAT_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            return

    async def _shutdown(self) -> None:
        if self._state is CollectorLifecycleState.STOPPED:
            return

        self._state = CollectorLifecycleState.STOPPING
        await asyncio.sleep(0)
        self._state = CollectorLifecycleState.STOPPED
        logger.info(
            "collector_runtime_stopped",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_stopped",
                "env": self._config.app_env.value,
                "stop_reason": self._stop_reason or "completed",
                "heartbeat_count": self._heartbeat_count,
            },
        )
