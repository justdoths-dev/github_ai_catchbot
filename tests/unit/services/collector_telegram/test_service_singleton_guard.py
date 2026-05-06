from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

import pytest

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.models import RuntimeSnapshot
from services.collector_telegram.service import CollectorTelegramService


@dataclass(slots=True)
class FakeSingletonGuard:
    acquired: bool = False
    acquire_count: int = 0
    release_count: int = 0

    def acquire(self) -> None:
        self.acquired = True
        self.acquire_count += 1

    def release(self) -> None:
        self.acquired = False
        self.release_count += 1

    def is_acquired(self) -> bool:
        return self.acquired


class FakeRuntime:
    def __init__(self, *, fail_startup: bool = False) -> None:
        self.snapshot = RuntimeSnapshot()
        self.fail_startup = fail_startup
        self.shutdown_count = 0
        self._stop_event = asyncio.Event()

    async def startup_acceptance_check(self) -> None:
        if self.fail_startup:
            raise RuntimeError("startup failed")

    async def run_forever(self) -> None:
        await self._stop_event.wait()

    async def shutdown(self) -> None:
        self.shutdown_count += 1
        self._stop_event.set()


def _config(tmp_path: Path, *, app_env: str, collector_mode: str) -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env=app_env,  # type: ignore[arg-type]
        database_url="postgresql+asyncpg://collector:secret@localhost/test",
        redis_url=None,
        collector_mode=collector_mode,  # type: ignore[arg-type]
        telegram_api_id=12345,
        telegram_api_hash="hash-value",
        telegram_phone_number="+10000000000",
        telegram_2fa_password=None,
        tdlib_state_dir=str(tmp_path / "tdlib-state"),
        tdlib_files_dir=str(tmp_path / "tdlib-files"),
        tdlib_db_encryption_key="enc-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=50,
        warm_backfill_limit=30,
        history_page_limit=50,
        singleton_lock_path=str(tmp_path / "collector.lock"),
        startup_probe_timeout_sec=30,
        startup_warm_backfill_enabled=True,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_live_service_start_acquires_singleton_guard(tmp_path: Path) -> None:
    config = _config(tmp_path, app_env="prod", collector_mode="live")
    guard = FakeSingletonGuard()
    runtime = FakeRuntime()
    service = CollectorTelegramService(config, runtime, singleton_guard=guard)  # type: ignore[arg-type]

    await service.start()
    try:
        assert guard.is_acquired()
        assert guard.acquire_count == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_live_service_stop_releases_singleton_guard(tmp_path: Path) -> None:
    config = _config(tmp_path, app_env="prod", collector_mode="live")
    guard = FakeSingletonGuard()
    runtime = FakeRuntime()
    service = CollectorTelegramService(config, runtime, singleton_guard=guard)  # type: ignore[arg-type]

    await service.start()
    await service.stop()

    assert not guard.is_acquired()
    assert guard.release_count == 1
    assert runtime.shutdown_count == 1


@pytest.mark.asyncio
async def test_replay_service_does_not_acquire_live_singleton_guard(tmp_path: Path) -> None:
    config = _config(tmp_path, app_env="dev", collector_mode="replay")
    guard = FakeSingletonGuard()
    runtime = FakeRuntime()
    service = CollectorTelegramService(config, runtime, singleton_guard=guard)  # type: ignore[arg-type]

    await service.start()
    try:
        assert not guard.is_acquired()
        assert guard.acquire_count == 0
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_live_service_releases_singleton_guard_when_startup_fails(tmp_path: Path) -> None:
    config = _config(tmp_path, app_env="prod", collector_mode="live")
    guard = FakeSingletonGuard()
    runtime = FakeRuntime(fail_startup=True)
    service = CollectorTelegramService(config, runtime, singleton_guard=guard)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="startup failed"):
        await service.start()

    assert not guard.is_acquired()
    assert guard.acquire_count == 1
    assert guard.release_count == 1
