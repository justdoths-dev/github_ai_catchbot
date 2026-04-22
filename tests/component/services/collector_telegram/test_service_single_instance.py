from __future__ import annotations

from pathlib import Path

import pytest

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.exceptions import SingletonViolationError
from services.collector_telegram.health import CollectorHealthService
from services.collector_telegram.runtime import CollectorRuntime
from services.collector_telegram.service import CollectorTelegramService
from services.collector_telegram.singleton_guard import CollectorSingletonGuard


class StubRuntime(CollectorRuntime):
    async def startup_acceptance_check(self) -> None:  # type: ignore[override]
        return None


def _config(lock_path: str) -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env="prod",
        database_url="postgresql+asyncpg://test:test@localhost/test",
        redis_url=None,
        collector_mode="live",
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
        singleton_lock_path=lock_path,
        startup_probe_timeout_sec=30,
        startup_warm_backfill_enabled=True,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_live_service_enforces_single_instance(tmp_path: Path) -> None:
    lock_path = str(tmp_path / "collector.lock")
    config = _config(lock_path)

    service_a = CollectorTelegramService(
        config,
        StubRuntime(config, health=CollectorHealthService()),
        singleton_guard=CollectorSingletonGuard(lock_path),
    )
    service_b = CollectorTelegramService(
        config,
        StubRuntime(config, health=CollectorHealthService()),
        singleton_guard=CollectorSingletonGuard(lock_path),
    )

    await service_a.start()
    try:
        with pytest.raises(SingletonViolationError):
            await service_b.start()
    finally:
        await service_a.stop()