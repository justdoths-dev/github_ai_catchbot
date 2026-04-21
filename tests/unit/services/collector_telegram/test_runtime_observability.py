from __future__ import annotations

import asyncio

import pytest

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.health import CollectorHealthService
from services.collector_telegram.runtime import CollectorRuntime


def _config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env="test",
        database_url="postgresql+asyncpg://test:test@localhost/test",
        redis_url=None,
        collector_mode="replay",
        telegram_api_id=1,
        telegram_api_hash="hash",
        telegram_phone_number="+10000000000",
        telegram_2fa_password=None,
        tdlib_state_dir="/tmp/collector-state",
        tdlib_files_dir="/tmp/collector-files",
        tdlib_db_encryption_key="enc-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=50,
        warm_backfill_limit=30,
        history_page_limit=50,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_authorization_tick_sets_placeholder_ready_state() -> None:
    health = CollectorHealthService()
    runtime = CollectorRuntime(_config(), health=health)

    await runtime._authorization_tick()

    assert health.snapshot().tdlib_authorization_state == "authorizationStateReady"


@pytest.mark.asyncio
async def test_runtime_run_forever_marks_loop_states_and_stops_cleanly() -> None:
    health = CollectorHealthService()
    runtime = CollectorRuntime(_config(), health=health)

    task = asyncio.create_task(runtime.run_forever())
    await asyncio.sleep(0.05)
    await runtime.shutdown()
    await task

    snapshot = health.snapshot()
    assert snapshot.health_state == "stopped"
    assert set(snapshot.loop_states.keys()) == {
        "authorization_loop",
        "update_ingest_loop",
        "reconcile_scheduler_loop",
        "registry_refresh_loop",
        "health_publisher_loop",
    }
    assert all(state == "stopped" for state in snapshot.loop_states.values())
    assert snapshot.last_heartbeat_at is not None
