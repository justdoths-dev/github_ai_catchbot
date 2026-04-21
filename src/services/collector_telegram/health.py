from __future__ import annotations

import logging
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .models import ReconcileSummary


CollectorHealthState = str


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


@dataclass(slots=True, frozen=True)
class CollectorHealthSnapshot:
    health_state: CollectorHealthState
    readiness: str
    tdlib_authorization_state: str | None
    started_at: str | None
    last_heartbeat_at: str | None
    last_update_received_at: str | None
    tracked_channels_active: int
    outbox_pending_count: int | None
    update_counters: dict[str, int]
    reconcile_runs_total: int
    reconcile_gap_fills_total: int
    loop_states: dict[str, str]
    last_successful_history_sync_at: dict[str, str]
    notes: list[str]


class CollectorHealthService:
    """In-memory collector observability state.

    This service owns collector-local heartbeat, readiness, and lightweight
    counters. It does not perform database probing or export to Prometheus.
    """

    _REQUIRED_RUNTIME_LOOPS = {
        "authorization_loop",
        "update_ingest_loop",
        "reconcile_scheduler_loop",
        "registry_refresh_loop",
        "health_publisher_loop",
    }

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._lock = threading.RLock()

        self._health_state: CollectorHealthState = "starting"
        self._tdlib_authorization_state: str | None = None
        self._started_at: datetime | None = None
        self._last_heartbeat_at: datetime | None = None
        self._last_update_received_at: datetime | None = None
        self._tracked_channels_active: int = 0
        self._outbox_pending_count: int | None = None

        self._update_counters: dict[str, int] = defaultdict(int)
        self._reconcile_runs_total: int = 0
        self._reconcile_gap_fills_total: int = 0
        self._last_successful_history_sync_at: dict[str, datetime] = {}
        self._loop_states: dict[str, str] = {}
        self._notes: deque[str] = deque(maxlen=25)

    def mark_starting(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "starting"
            self._started_at = self._started_at or _utcnow()
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_ready(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "ready"
            self._started_at = self._started_at or _utcnow()
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_degraded(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "degraded"
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_failing(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "failing"
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_stopped(self, *, note: str | None = None) -> None:
        with self._lock:
            self._health_state = "stopped"
            self._last_heartbeat_at = _utcnow()
            if note:
                self._notes.append(note)

    def mark_authorization_state(self, state_name: str | None) -> None:
        with self._lock:
            self._tdlib_authorization_state = state_name
            self._last_heartbeat_at = _utcnow()

    def mark_runtime_loop_started(self, loop_name: str) -> None:
        with self._lock:
            self._loop_states[loop_name] = "running"
            self._last_heartbeat_at = _utcnow()

    def mark_runtime_loop_stopped(self, loop_name: str) -> None:
        with self._lock:
            self._loop_states[loop_name] = "stopped"
            self._last_heartbeat_at = _utcnow()

    def heartbeat(self) -> None:
        with self._lock:
            self._last_heartbeat_at = _utcnow()

    def mark_update_received(self, update_type: str) -> None:
        with self._lock:
            self._update_counters[update_type] += 1
            self._last_update_received_at = _utcnow()
            self._last_heartbeat_at = self._last_update_received_at

    def mark_reconcile_result(self, summary: ReconcileSummary) -> None:
        with self._lock:
            self._reconcile_runs_total += 1
            self._reconcile_gap_fills_total += int(summary.gap_filled_count)
            self._last_heartbeat_at = _utcnow()
            if summary.error_code is None:
                self._last_successful_history_sync_at[str(summary.chat_id)] = _utcnow()
            else:
                self._notes.append(
                    f"reconcile chat={summary.chat_id} result={summary.result_type} error={summary.error_code}"
                )

    def mark_tracked_channels_active(self, count: int) -> None:
        with self._lock:
            self._tracked_channels_active = max(0, count)
            self._last_heartbeat_at = _utcnow()

    def set_outbox_pending_count(self, count: int | None) -> None:
        with self._lock:
            self._outbox_pending_count = None if count is None else max(0, count)
            self._last_heartbeat_at = _utcnow()

    def readiness(self) -> str:
        with self._lock:
            if self._health_state in {"failing", "stopped"}:
                return self._health_state

            missing_loops = self._REQUIRED_RUNTIME_LOOPS - {
                name for name, state in self._loop_states.items() if state == "running"
            }
            if missing_loops:
                return "starting"

            auth_state = self._tdlib_authorization_state
            if auth_state and auth_state != "authorizationStateReady":
                return "degraded"

            return self._health_state

    def heartbeat_age_seconds(self) -> float | None:
        with self._lock:
            if self._last_heartbeat_at is None:
                return None
            return (_utcnow() - self._last_heartbeat_at).total_seconds()

    def snapshot(self) -> CollectorHealthSnapshot:
        with self._lock:
            return CollectorHealthSnapshot(
                health_state=self._health_state,
                readiness=self.readiness(),
                tdlib_authorization_state=self._tdlib_authorization_state,
                started_at=_isoformat(self._started_at),
                last_heartbeat_at=_isoformat(self._last_heartbeat_at),
                last_update_received_at=_isoformat(self._last_update_received_at),
                tracked_channels_active=self._tracked_channels_active,
                outbox_pending_count=self._outbox_pending_count,
                update_counters=dict(sorted(self._update_counters.items())),
                reconcile_runs_total=self._reconcile_runs_total,
                reconcile_gap_fills_total=self._reconcile_gap_fills_total,
                loop_states=dict(sorted(self._loop_states.items())),
                last_successful_history_sync_at={
                    key: _isoformat(value) or ""
                    for key, value in sorted(self._last_successful_history_sync_at.items())
                },
                notes=list(self._notes),
            )

    def snapshot_dict(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        return {
            "health_state": snapshot.health_state,
            "readiness": snapshot.readiness,
            "tdlib_authorization_state": snapshot.tdlib_authorization_state,
            "started_at": snapshot.started_at,
            "last_heartbeat_at": snapshot.last_heartbeat_at,
            "last_update_received_at": snapshot.last_update_received_at,
            "tracked_channels_active": snapshot.tracked_channels_active,
            "outbox_pending_count": snapshot.outbox_pending_count,
            "update_counters": snapshot.update_counters,
            "reconcile_runs_total": snapshot.reconcile_runs_total,
            "reconcile_gap_fills_total": snapshot.reconcile_gap_fills_total,
            "loop_states": snapshot.loop_states,
            "last_successful_history_sync_at": snapshot.last_successful_history_sync_at,
            "notes": snapshot.notes,
            "heartbeat_age_seconds": self.heartbeat_age_seconds(),
        }
