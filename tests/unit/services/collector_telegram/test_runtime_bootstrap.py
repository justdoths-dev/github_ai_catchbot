from __future__ import annotations

import asyncio
import io
import json
import logging
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.main import configure_logging
from services.collector_telegram.models import (
    CollectorEnvironment,
    CollectorLifecycleState,
    CollectorMode,
)
from services.collector_telegram.service import CollectorTelegramService


class CollectorTelegramLoggingTests(unittest.TestCase):
    def test_configure_logging_emits_structured_json(self) -> None:
        stream = io.StringIO()
        configure_logging("DEBUG", stream=stream)

        logging.getLogger("services.collector_telegram.test").info(
            "collector_runtime_started",
            extra={
                "service": "collector-telegram",
                "event": "collector_runtime_started",
                "collector_mode": "replay",
            },
        )

        payload = json.loads(stream.getvalue().strip())
        self.assertEqual(payload["message"], "collector_runtime_started")
        self.assertEqual(payload["event"], "collector_runtime_started")
        self.assertEqual(payload["collector_mode"], "replay")
        self.assertEqual(payload["level"], "INFO")
        self.assertIn("ts", payload)
        self.assertEqual(payload["service"], "collector-telegram")


class CollectorTelegramRuntimeBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_starts_and_stops_without_live_side_effects(self) -> None:
        config = CollectorTelegramConfig(
            app_env=CollectorEnvironment.DEV,
            database_url="postgresql://collector:secret@localhost:5432/catchbot",
            redis_url="redis://localhost:6379/0",
            collector_mode=CollectorMode.REPLAY,
            telegram_api_id=12345,
            telegram_api_hash="hash-value",
            telegram_phone_number="+10000000000",
            telegram_2fa_password=None,
            tdlib_state_dir="/tmp/catchbot-tdlib-state",
            tdlib_files_dir="/tmp/catchbot-tdlib-files",
            tdlib_db_encryption_key="enc-key",
            reconcile_interval_sec=60,
            reconcile_backfill_limit=10,
            warm_backfill_limit=5,
            history_page_limit=25,
            log_level="INFO",
        )
        service = CollectorTelegramService(config)

        initial_snapshot = service.snapshot()
        self.assertEqual(initial_snapshot.lifecycle_state, CollectorLifecycleState.CREATED)
        self.assertEqual(initial_snapshot.app_env, CollectorEnvironment.DEV)
        self.assertEqual(initial_snapshot.collector_mode, CollectorMode.REPLAY)

        await service.start()
        await asyncio.sleep(0.06)
        self.assertEqual(service.state, CollectorLifecycleState.RUNNING)

        service.request_stop("unit-test-stop")
        stopped_snapshot = await service.wait_closed()

        self.assertEqual(stopped_snapshot.lifecycle_state, CollectorLifecycleState.STOPPED)
        self.assertEqual(stopped_snapshot.stop_reason, "unit-test-stop")
        self.assertGreater(stopped_snapshot.heartbeat_count, 0)


if __name__ == "__main__":
    unittest.main()
