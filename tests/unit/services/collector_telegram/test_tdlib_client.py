from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.models import CollectorEnvironment, CollectorMode
from services.collector_telegram.tdlib_client import TDLibClient


class StubTransport:
    def __init__(self) -> None:
        self.initialized = False
        self.closed = False
        self.sent_requests: list[dict] = []
        self.received_payloads: list[dict | None] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def send(self, request: dict) -> None:
        self.sent_requests.append(request)

    async def receive(self, timeout: float) -> dict | None:
        if self.received_payloads:
            return self.received_payloads.pop(0)
        return None

    async def close(self) -> None:
        self.closed = True


class TDLibClientTests(unittest.IsolatedAsyncioTestCase):
    def _config(self) -> CollectorTelegramConfig:
        return CollectorTelegramConfig(
            app_env=CollectorEnvironment.PROD,
            database_url="postgresql://collector:secret@localhost:5432/catchbot",
            redis_url=None,
            collector_mode=CollectorMode.LIVE,
            telegram_api_id=12345,
            telegram_api_hash="hash-value",
            telegram_phone_number="+10000000000",
            telegram_2fa_password="2fa-pass",
            tdlib_state_dir="/tmp/catchbot-tdlib-state",
            tdlib_files_dir="/tmp/catchbot-tdlib-files",
            tdlib_db_encryption_key="enc-key",
            reconcile_interval_sec=300,
            reconcile_backfill_limit=50,
            warm_backfill_limit=30,
            history_page_limit=50,
            log_level="INFO",
        )

    async def test_initialize_send_receive_and_close(self) -> None:
        transport = StubTransport()
        client = TDLibClient(self._config(), transport=transport)

        await client.initialize()
        self.assertTrue(transport.initialized)

        await client.send({"@type": "testRequest"})
        self.assertEqual(transport.sent_requests, [{"@type": "testRequest"}])

        transport.received_payloads.append(
            {
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": "authorizationStateReady"},
            }
        )
        payload = await client.receive(1.0)
        self.assertIsNotNone(payload)
        self.assertTrue(client.is_ready())
        self.assertEqual(client.current_authorization_state_type(), "authorizationStateReady")

        await client.close()
        self.assertTrue(transport.closed)

    def test_request_builders_follow_expected_shape(self) -> None:
        transport = StubTransport()
        client = TDLibClient(self._config(), transport=transport)

        tdlib_params = client.build_set_tdlib_parameters_request().payload
        self.assertEqual(tdlib_params["@type"], "setTdlibParameters")
        self.assertEqual(tdlib_params["parameters"]["api_id"], 12345)
        self.assertEqual(tdlib_params["parameters"]["api_hash"], "hash-value")

        phone_req = client.build_set_authentication_phone_number_request().payload
        self.assertEqual(phone_req["@type"], "setAuthenticationPhoneNumber")
        self.assertEqual(phone_req["phone_number"], "+10000000000")

        history_req = client.build_get_chat_history_request(chat_id=10, limit=20).payload
        self.assertEqual(history_req["@type"], "getChatHistory")
        self.assertEqual(history_req["chat_id"], 10)
        self.assertEqual(history_req["limit"], 20)


if __name__ == "__main__":
    unittest.main()
