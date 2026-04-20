from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.exceptions import CollectorTelegramConfigError
from services.collector_telegram.models import CollectorEnvironment, CollectorMode


class CollectorTelegramConfigTests(unittest.TestCase):
    def test_from_env_builds_non_live_dev_config(self) -> None:
        config = CollectorTelegramConfig.from_env(
            {
                "APP_ENV": "dev",
                "DATABASE_URL": "postgresql://collector:secret@localhost:5432/catchbot",
                "REDIS_URL": "redis://localhost:6379/0",
                "COLLECTOR_MODE": "replay",
                "TELEGRAM_API_ID": "12345",
                "TELEGRAM_API_HASH": "hash-value",
                "TELEGRAM_PHONE_NUMBER": "+10000000000",
                "TDLIB_STATE_DIR": "/tmp/catchbot-tdlib-state",
                "TDLIB_FILES_DIR": "/tmp/catchbot-tdlib-files",
                "TDLIB_DB_ENCRYPTION_KEY": "enc-key",
                "RECONCILE_INTERVAL_SEC": "60",
                "RECONCILE_BACKFILL_LIMIT": "25",
                "WARM_BACKFILL_LIMIT": "10",
                "HISTORY_PAGE_LIMIT": "100",
                "LOG_LEVEL": "debug",
            }
        )

        self.assertEqual(config.app_env, CollectorEnvironment.DEV)
        self.assertEqual(config.collector_mode, CollectorMode.REPLAY)
        self.assertEqual(config.reconcile_interval_sec, 60)
        self.assertEqual(config.reconcile_backfill_limit, 25)
        self.assertEqual(config.warm_backfill_limit, 10)
        self.assertEqual(config.history_page_limit, 100)
        self.assertEqual(config.log_level, "DEBUG")

        redacted = config.redacted()
        self.assertEqual(redacted["app_env"], "dev")
        self.assertTrue(redacted["has_database_url"])
        self.assertTrue(redacted["has_redis_url"])
        self.assertNotIn("database_url", redacted)
        self.assertNotIn("redis_url", redacted)

    def test_prod_requires_live_mode(self) -> None:
        with self.assertRaises(CollectorTelegramConfigError):
            CollectorTelegramConfig.from_env(
                {
                    "APP_ENV": "prod",
                    "DATABASE_URL": "postgresql://collector:secret@localhost:5432/catchbot",
                    "REDIS_URL": "redis://localhost:6379/0",
                    "COLLECTOR_MODE": "replay",
                    "TELEGRAM_API_ID": "12345",
                    "TELEGRAM_API_HASH": "hash-value",
                    "TELEGRAM_PHONE_NUMBER": "+10000000000",
                    "TDLIB_STATE_DIR": "/tmp/catchbot-tdlib-state",
                    "TDLIB_FILES_DIR": "/tmp/catchbot-tdlib-files",
                    "TDLIB_DB_ENCRYPTION_KEY": "enc-key",
                }
            )

    def test_dev_env_rejects_live_mode(self) -> None:
        with self.assertRaises(CollectorTelegramConfigError):
            CollectorTelegramConfig.from_env(
                {
                    "APP_ENV": "dev",
                    "DATABASE_URL": "postgresql://collector:secret@localhost:5432/catchbot",
                    "REDIS_URL": "redis://localhost:6379/0",
                    "COLLECTOR_MODE": "live",
                    "TELEGRAM_API_ID": "12345",
                    "TELEGRAM_API_HASH": "hash-value",
                    "TELEGRAM_PHONE_NUMBER": "+10000000000",
                    "TDLIB_STATE_DIR": "/tmp/catchbot-tdlib-state",
                    "TDLIB_FILES_DIR": "/tmp/catchbot-tdlib-files",
                    "TDLIB_DB_ENCRYPTION_KEY": "enc-key",
                }
            )

    def test_file_backed_secrets_are_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_path = Path(tempdir)
            api_hash_file = temp_path / "api_hash.txt"
            password_file = temp_path / "password.txt"
            encryption_file = temp_path / "encryption.txt"
            api_hash_file.write_text("hash-value\n", encoding="utf-8")
            password_file.write_text("2fa-password\n", encoding="utf-8")
            encryption_file.write_text("enc-key\n", encoding="utf-8")

            config = CollectorTelegramConfig.from_env(
                {
                    "APP_ENV": "prod",
                    "DATABASE_URL": "postgresql://collector:secret@localhost:5432/catchbot",
                    "COLLECTOR_MODE": "live",
                    "TELEGRAM_API_ID": "12345",
                    "TELEGRAM_API_HASH_FILE": str(api_hash_file),
                    "TELEGRAM_PHONE_NUMBER": "+10000000000",
                    "TELEGRAM_2FA_PASSWORD_FILE": str(password_file),
                    "TDLIB_STATE_DIR": "/tmp/catchbot-tdlib-state",
                    "TDLIB_FILES_DIR": "/tmp/catchbot-tdlib-files",
                    "TDLIB_DB_ENCRYPTION_KEY_FILE": str(encryption_file),
                }
            )

        self.assertEqual(config.telegram_api_hash, "hash-value")
        self.assertEqual(config.telegram_2fa_password, "2fa-password")
        self.assertEqual(config.tdlib_db_encryption_key, "enc-key")
        self.assertIsNone(config.redis_url)

    def test_invalid_history_limit_is_rejected(self) -> None:
        with self.assertRaises(CollectorTelegramConfigError):
            CollectorTelegramConfig.from_env(
                {
                    "APP_ENV": "dev",
                    "DATABASE_URL": "postgresql://collector:secret@localhost:5432/catchbot",
                    "REDIS_URL": "redis://localhost:6379/0",
                    "COLLECTOR_MODE": "replay",
                    "TELEGRAM_API_ID": "12345",
                    "TELEGRAM_API_HASH": "hash-value",
                    "TELEGRAM_PHONE_NUMBER": "+10000000000",
                    "TDLIB_STATE_DIR": "/tmp/catchbot-tdlib-state",
                    "TDLIB_FILES_DIR": "/tmp/catchbot-tdlib-files",
                    "TDLIB_DB_ENCRYPTION_KEY": "enc-key",
                    "HISTORY_PAGE_LIMIT": "101",
                }
            )


if __name__ == "__main__":
    unittest.main()
