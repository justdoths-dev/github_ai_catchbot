from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from .config import CollectorTelegramConfig
from .exceptions import AuthorizationManualInterventionRequired
from .health import CollectorHealthService
from .models import ReconcileSummary, RuntimeSnapshot, TrackedChat


@dataclass(slots=True, frozen=True)
class AuthorizationPumpResult:
    state_name: str | None = None
    requires_manual_intervention: bool = False
    note: str | None = None


@dataclass(slots=True, frozen=True)
class UpdateIngestBatchResult:
    update_counts: dict[str, int] = field(default_factory=dict)


class AuthorizationPumpProtocol(Protocol):
    async def pump_once(self) -> AuthorizationPumpResult | None: ...


class UpdateIngestRunnerProtocol(Protocol):
    async def pump_once(self) -> UpdateIngestBatchResult | None: ...


class RegistrySyncProtocol(Protocol):
    async def load_active_channels(self) -> list[TrackedChat]: ...
    async def sync_unresolved_channels(self): ...
    async def sync_join_requested_channels(self): ...
    async def sync_access_lost_channels(self): ...


class ReconcileProtocol(Protocol):
    async def run_startup_warm_backfill(self, chat_id: int) -> ReconcileSummary: ...
    async def run_scheduled_targets(self, *, limit: int = 20) -> list[ReconcileSummary]: ...


class CollectorRuntime:
    """Runtime orchestration skeleton with acceptance hardening hooks.

    This step closes the collector-local acceptance gap by wiring:
    - startup warm backfill,
    - active channel loading,
    - manual intervention degraded state,
    - per-loop health transitions.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        health: CollectorHealthService,
        authorization_pump: AuthorizationPumpProtocol | None = None,
        update_ingest_runner: UpdateIngestRunnerProtocol | None = None,
        registry_sync: RegistrySyncProtocol | None = None,
        reconcile: ReconcileProtocol | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._health = health
        self._authorization_pump = authorization_pump
        self._update_ingest_runner = update_ingest_runner
        self._registry_sync = registry_sync
        self._reconcile = reconcile
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._snapshot = RuntimeSnapshot()

    @property
    def snapshot(self) -> RuntimeSnapshot:
        return self._snapshot

    async def startup_acceptance_check(self) -> None:
        """Startup validation before entering the forever-loop path.

        Acceptance goals covered here:
        - active channel set can be loaded,
        - startup warm backfill exists,
        - health/readiness state is updated,
        - restart recovery has an explicit entrypoint.
        """
        self._health.mark_starting(note="collector startup acceptance check begin")

        active_channels: list[TrackedChat] = []
        if self._registry_sync is not None:
            active_channels = await self._registry_sync.load_active_channels()
            self._health.mark_tracked_channels_active(len(active_channels))

        if self._config.startup_warm_backfill_enabled and self._reconcile is not None:
            for chat in active_channels:
                if chat.chat_id is None:
                    continue
                summary = await self._reconcile.run_startup_warm_backfill(int(chat.chat_id))
                self._health.mark_reconcile_result(summary)

        self._snapshot.health_state = "ready"
        self._health.mark_ready(note="collector startup acceptance check complete")

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
        if self._authorization_pump is None:
            self._health.mark_authorization_state("authorizationStateReady")
            return

        result = await self._authorization_pump.pump_once()
        if result is None:
            return

        self._health.mark_authorization_state(result.state_name)
        if result.requires_manual_intervention:
            self._snapshot.health_state = "degraded"
            self._health.mark_degraded(note=result.note or "authorization manual intervention required")
            raise AuthorizationManualInterventionRequired(
                result.note or "authorization manual intervention required"
            )

    async def _update_ingest_tick(self) -> None:
        if self._update_ingest_runner is None:
            return

        result = await self._update_ingest_runner.pump_once()
        if result is None:
            return

        for update_type, count in result.update_counts.items():
            for _ in range(max(0, count)):
                self._health.mark_update_received(update_type)

    async def _reconcile_scheduler_tick(self) -> None:
        if self._reconcile is None:
            return
        summaries = await self._reconcile.run_scheduled_targets(limit=20)
        for summary in summaries:
            self._health.mark_reconcile_result(summary)

    async def _registry_refresh_tick(self) -> None:
        if self._registry_sync is None:
            return

        await self._registry_sync.sync_unresolved_channels()
        await self._registry_sync.sync_join_requested_channels()
        await self._registry_sync.sync_access_lost_channels()
        active_channels = await self._registry_sync.load_active_channels()
        self._health.mark_tracked_channels_active(len(active_channels))

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
