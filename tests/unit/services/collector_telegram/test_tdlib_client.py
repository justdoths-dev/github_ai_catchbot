from __future__ import annotations

import sys
import unittest
from unittest.mock import patch
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.exceptions import TDLibTransportError
from services.collector_telegram.models import CollectorEnvironment, CollectorMode
from services.collector_telegram.tdlib_client import (
    TDJsonTransport,
    TDLibClient,
    tdlib_parameters_shape_errors,
)


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
        self.assertNotIn("parameters", tdlib_params)
        self.assertEqual(tdlib_params["api_id"], 12345)
        self.assertEqual(tdlib_params["api_hash"], "hash-value")
        self.assertEqual(tdlib_params["database_directory"], "/tmp/catchbot-tdlib-state")
        self.assertEqual(tdlib_params["files_directory"], "/tmp/catchbot-tdlib-files")
        self.assertEqual(tdlib_params["database_encryption_key"], "enc-key")
        self.assertIs(tdlib_params["use_file_database"], True)
        self.assertIs(tdlib_params["use_chat_info_database"], True)
        self.assertIs(tdlib_params["use_message_database"], True)
        self.assertIs(tdlib_params["use_secret_chats"], False)
        self.assertEqual(tdlib_parameters_shape_errors(tdlib_params), ())

        phone_req = client.build_set_authentication_phone_number_request().payload
        self.assertEqual(phone_req["@type"], "setAuthenticationPhoneNumber")
        self.assertEqual(phone_req["phone_number"], "+10000000000")

        history_req = client.build_get_chat_history_request(chat_id=10, limit=20).payload
        self.assertEqual(history_req["@type"], "getChatHistory")
        self.assertEqual(history_req["chat_id"], 10)
        self.assertEqual(history_req["limit"], 20)

    def test_real_tdjson_transport_reports_missing_library(self) -> None:
        with patch("services.collector_telegram.tdlib_client.ctypes.util.find_library", return_value=None):
            with self.assertRaises(TDLibTransportError):
                TDJsonTransport().assert_available()

    def test_tdlib_parameter_shape_guard_rejects_default_like_payload(self) -> None:
        invalid_payload = {
            "@type": "setTdlibParameters",
            "database_directory": "",
            "files_directory": "",
            "use_file_database": False,
            "use_chat_info_database": False,
            "use_message_database": False,
            "use_secret_chats": False,
            "api_id": 0,
            "api_hash": "",
            "database_encryption_key": "",
        }

        errors = tdlib_parameters_shape_errors(invalid_payload)

        self.assertIn("api_id.invalid", errors)
        self.assertIn("api_hash.empty", errors)
        self.assertIn("database_directory.empty", errors)
        self.assertIn("files_directory.empty", errors)
        self.assertIn("database_encryption_key.empty", errors)
        self.assertIn("use_file_database.invalid", errors)
        self.assertIn("use_chat_info_database.invalid", errors)
        self.assertIn("use_message_database.invalid", errors)


if __name__ == "__main__":
    unittest.main()
