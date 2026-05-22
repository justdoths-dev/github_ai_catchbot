from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.auth_fsm import AuthorizationFSM
from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.models import CollectorEnvironment, CollectorMode


class AuthorizationFSMTests(unittest.TestCase):
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

    def test_wait_tdlib_parameters_builds_request(self) -> None:
        fsm = AuthorizationFSM(self._config())
        result = fsm.handle_state({"@type": "authorizationStateWaitTdlibParameters"})
        self.assertEqual(result.new_state, "waiting_tdlib_parameters")
        self.assertFalse(result.requires_manual_intervention)
        request = result.requests[0]
        self.assertEqual(request["@type"], "setTdlibParameters")
        self.assertNotIn("parameters", request)
        self.assertEqual(request["api_id"], 12345)
        self.assertEqual(request["api_hash"], "hash-value")
        self.assertEqual(request["database_directory"], "/tmp/catchbot-tdlib-state")
        self.assertEqual(request["files_directory"], "/tmp/catchbot-tdlib-files")
        self.assertEqual(request["database_encryption_key"], "ZW5jLWtleQ==")
        self.assertIs(request["use_file_database"], True)
        self.assertIs(request["use_chat_info_database"], True)
        self.assertIs(request["use_message_database"], True)
        self.assertIs(request["use_secret_chats"], False)

    def test_wait_code_requires_manual_intervention(self) -> None:
        fsm = AuthorizationFSM(self._config())
        result = fsm.handle_state({"@type": "authorizationStateWaitCode"})
        self.assertEqual(result.new_state, "waiting_code")
        self.assertTrue(result.requires_manual_intervention)
        self.assertEqual(result.requests, [])
        self.assertTrue(fsm.requires_manual_intervention())

    def test_check_authentication_code_request_builder_is_not_used_by_default(self) -> None:
        fsm = AuthorizationFSM(self._config())
        request = fsm.build_check_authentication_code_request("test-code-value")

        self.assertEqual(
            request,
            {
                "@type": "checkAuthenticationCode",
                "code": "test-code-value",
            },
        )

    def test_wait_encryption_key_builds_tdlib_json_bytes_request(self) -> None:
        fsm = AuthorizationFSM(self._config())
        result = fsm.handle_state({"@type": "authorizationStateWaitEncryptionKey"})
        self.assertEqual(result.new_state, "waiting_encryption_key")
        request = result.requests[0]
        self.assertEqual(request["@type"], "checkDatabaseEncryptionKey")
        self.assertEqual(request["encryption_key"], "ZW5jLWtleQ==")

    def test_ready_then_regression_marks_degraded(self) -> None:
        fsm = AuthorizationFSM(self._config())
        ready = fsm.handle_state({"@type": "authorizationStateReady"})
        self.assertEqual(ready.new_state, "ready")
        self.assertTrue(fsm.is_ready())

        degraded = fsm.handle_state({"@type": "authorizationStateWaitPassword"})
        self.assertEqual(degraded.new_state, "degraded")
        self.assertTrue(degraded.requires_manual_intervention)
        self.assertTrue(fsm.is_degraded())


if __name__ == "__main__":
    unittest.main()
