from __future__ import annotations

import os
from dataclasses import dataclass


class NotifierTelegramConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class NotifierTelegramConfig:
    app_env: str
    database_url: str
    redis_url: str
    telegram_bot_token: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    dry_run: bool
    allow_edits: bool
    enable_notification_send: bool
    max_message_chars: int
    telegram_api_base_url: str
    request_timeout_sec: float
    log_level: str

    @classmethod
    def from_env(cls) -> "NotifierTelegramConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        app_env = _read("APP_ENV", "dev").lower()
        is_prod = app_env in {"prod", "production"}
        try:
            cfg = cls(
                app_env=app_env,
                database_url=_read("DATABASE_URL"),
                redis_url=_read("REDIS_URL"),
                telegram_bot_token=_read("TELEGRAM_BOT_TOKEN"),
                queue_name=_read("NOTIFIER_TELEGRAM_QUEUE_NAME", "q.notification.send"),
                consumer_group=_read("NOTIFIER_TELEGRAM_CONSUMER_GROUP", "notifier-telegram"),
                consumer_name=_read("NOTIFIER_TELEGRAM_CONSUMER_NAME", "notifier-telegram-1"),
                batch_size=int(_read("NOTIFIER_TELEGRAM_BATCH_SIZE", "20")),
                block_ms=int(_read("NOTIFIER_TELEGRAM_BLOCK_MS", "5000")),
                dry_run=_bool_env(_read("NOTIFIER_TELEGRAM_DRY_RUN", "false" if is_prod else "true")),
                allow_edits=_bool_env(_read("NOTIFIER_TELEGRAM_ALLOW_EDITS", "true" if is_prod else "false")),
                enable_notification_send=_bool_env(_read("ENABLE_NOTIFICATION_SEND", "false")),
                max_message_chars=int(_read("NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", "3800")),
                telegram_api_base_url=_read("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
                request_timeout_sec=float(_read("NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", "10")),
                log_level=_read("LOG_LEVEL", "INFO").upper(),
            )
        except ValueError as exc:
            raise NotifierTelegramConfigurationError(str(exc)) from exc
        cfg.validate()
        return cfg

    @property
    def transport_enabled(self) -> bool:
        return self.enable_notification_send and not self.dry_run

    def validate(self) -> None:
        if not self.database_url:
            raise NotifierTelegramConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise NotifierTelegramConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_CONSUMER_NAME must not be empty")
        if self.batch_size < 1 or self.batch_size > 100:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_BLOCK_MS must be > 0")
        if self.max_message_chars < 500 or self.max_message_chars > 4096:
            raise NotifierTelegramConfigurationError("NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS must be between 500 and 4096")
        if self.transport_enabled and not self.telegram_bot_token:
            raise NotifierTelegramConfigurationError("TELEGRAM_BOT_TOKEN is required when Telegram transport is enabled")


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
