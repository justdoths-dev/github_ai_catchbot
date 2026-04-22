from __future__ import annotations

import pytest

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.health import CollectorHealthService
from services.collector_telegram.models import ReconcileSummary, TrackedChat
from services.collector_telegram.runtime import CollectorRuntime


class StubRegistrySync:
    async def load_active_channels(self):
        return [
            TrackedChat(
                registry_id="r1",
                chat_id=1001,
                desired_state="active",
                access_state="joined",
                source_kind="public_username",
                source_value="channel_a",
            ),
            TrackedChat(
                registry_id="r2",
                chat_id=1002,
                desired_state="active",
                access_state="joined",
                source_kind="public_username",
                source_value="channel_b",
            ),
        ]

    async def sync_unresolved_channels(self):
        return None

    async def sync_join_requested_channels(self):
        return None

    async def sync_access_lost_channels(self):
        return None


class StubReconcile:
    def __init__(self) -> None:
        self.calls: list[int] = []

    async def run_startup_warm_backfill(self, chat_id: int) -> ReconcileSummary:
        self.calls.append(chat_id)
        return ReconcileSummary(
            chat_id=chat_id,
            result_type="no_changes",
            processed_count=5,
            inserted_count=0,
            updated_count=0,
            gap_filled_count=0,
        )

    async def run_scheduled_targets(self, *, limit: int = 20):
        return []


def _config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env="dev",
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
        singleton_lock_path="/tmp/collector.lock",
        startup_probe_timeout_sec=30,
        startup_warm_backfill_enabled=True,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_startup_acceptance_runs_warm_backfill_for_active_channels() -> None:
    health = CollectorHealthService()
    reconcile = StubReconcile()
    runtime = CollectorRuntime(
        _config(),
        health=health,
        registry_sync=StubRegistrySync(),
        reconcile=reconcile,
    )

    await runtime.startup_acceptance_check()

    assert reconcile.calls == [1001, 1002]
    snapshot = health.snapshot()
    assert snapshot.health_state == "ready"
    assert snapshot.tracked_channels_active == 2
    assert snapshot.reconcile_runs_total == 2