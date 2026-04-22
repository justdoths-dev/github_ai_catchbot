from __future__ import annotations

import pytest

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.exceptions import AuthorizationManualInterventionRequired
from services.collector_telegram.health import CollectorHealthService
from services.collector_telegram.runtime import (
    AuthorizationPumpResult,
    CollectorRuntime,
)


class StubAuthorizationPump:
    async def pump_once(self) -> AuthorizationPumpResult:
        return AuthorizationPumpResult(
            state_name="authorizationStateWaitCode",
            requires_manual_intervention=True,
            note="operator code required",
        )


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
async def test_authorization_manual_intervention_marks_runtime_degraded() -> None:
    health = CollectorHealthService()
    runtime = CollectorRuntime(
        _config(),
        health=health,
        authorization_pump=StubAuthorizationPump(),
    )

    with pytest.raises(AuthorizationManualInterventionRequired):
        await runtime._authorization_tick()

    snapshot = health.snapshot()
    assert snapshot.health_state == "degraded"
    assert snapshot.tdlib_authorization_state == "authorizationStateWaitCode"