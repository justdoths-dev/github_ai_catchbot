from __future__ import annotations

from services.collector_telegram.health import CollectorHealthService
from services.collector_telegram.models import ReconcileSummary


def test_health_snapshot_tracks_readiness_counters_and_sync_state() -> None:
    health = CollectorHealthService()

    health.mark_starting(note="boot")
    assert health.readiness() == "starting"

    for loop_name in [
        "authorization_loop",
        "update_ingest_loop",
        "reconcile_scheduler_loop",
        "registry_refresh_loop",
        "health_publisher_loop",
    ]:
        health.mark_runtime_loop_started(loop_name)

    health.mark_authorization_state("authorizationStateReady")
    health.mark_ready(note="loops up")
    health.mark_tracked_channels_active(3)
    health.set_outbox_pending_count(7)
    health.mark_update_received("updateNewMessage")
    health.mark_update_received("updateNewMessage")
    health.mark_reconcile_result(
        ReconcileSummary(
            chat_id=1234,
            result_type="gap_filled",
            processed_count=5,
            inserted_count=1,
            updated_count=2,
            gap_filled_count=3,
        )
    )

    snapshot = health.snapshot()

    assert snapshot.health_state == "ready"
    assert snapshot.readiness == "ready"
    assert snapshot.tdlib_authorization_state == "authorizationStateReady"
    assert snapshot.tracked_channels_active == 3
    assert snapshot.outbox_pending_count == 7
    assert snapshot.update_counters == {"updateNewMessage": 2}
    assert snapshot.reconcile_runs_total == 1
    assert snapshot.reconcile_gap_fills_total == 3
    assert snapshot.loop_states["authorization_loop"] == "running"
    assert snapshot.last_successful_history_sync_at["1234"]
    assert "boot" in snapshot.notes
    assert "loops up" in snapshot.notes
