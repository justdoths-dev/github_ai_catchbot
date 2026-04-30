from __future__ import annotations

import os
from dataclasses import dataclass


class PolicyEngineConfigurationError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class PolicyEngineConfig:
    app_env: str
    database_url: str
    redis_url: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    policy_version: str
    delivery_policy_version: str
    operator_chat_id: int
    enable_later_delivery: bool
    enable_silent_later: bool
    enable_notification_send: bool
    render_profile_high: str
    render_profile_normal: str
    log_level: str

    @classmethod
    def from_env(cls) -> "PolicyEngineConfig":
        def _read(name: str, default: str = "") -> str:
            return os.getenv(name, default).strip()

        try:
            cfg = cls(
                app_env=_read("APP_ENV", "dev").lower(),
                database_url=_read("DATABASE_URL"),
                redis_url=_read("REDIS_URL"),
                queue_name=_read("POLICY_ENGINE_QUEUE_NAME", "q.analysis.policy"),
                consumer_group=_read("POLICY_ENGINE_CONSUMER_GROUP", "policy-engine"),
                consumer_name=_read("POLICY_ENGINE_CONSUMER_NAME", "policy-engine-1"),
                batch_size=int(_read("POLICY_ENGINE_BATCH_SIZE", "20")),
                block_ms=int(_read("POLICY_ENGINE_BLOCK_MS", "5000")),
                policy_version=_read("VERDICT_POLICY_VERSION", "verdict_policy_v1"),
                delivery_policy_version=_read("DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
                operator_chat_id=int(_read("TELEGRAM_OPERATOR_CHAT_ID", "0")),
                enable_later_delivery=_bool_env(_read("ENABLE_LATER_DELIVERY", "true")),
                enable_silent_later=_bool_env(_read("ENABLE_SILENT_LATER", "true")),
                enable_notification_send=_bool_env(_read("ENABLE_NOTIFICATION_SEND", "true")),
                render_profile_high=_read("NOTIFY_RENDER_PROFILE_HIGH", "telegram_single_alert_high_v1"),
                render_profile_normal=_read("NOTIFY_RENDER_PROFILE_NORMAL", "telegram_single_alert_normal_v1"),
                log_level=_read("LOG_LEVEL", "INFO").upper(),
            )
        except ValueError as exc:
            raise PolicyEngineConfigurationError(str(exc)) from exc
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.database_url:
            raise PolicyEngineConfigurationError("DATABASE_URL is required")
        if not self.redis_url:
            raise PolicyEngineConfigurationError("REDIS_URL is required")
        if not self.queue_name:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_QUEUE_NAME must not be empty")
        if not self.consumer_group:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_CONSUMER_GROUP must not be empty")
        if not self.consumer_name:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_CONSUMER_NAME must not be empty")
        if self.batch_size < 1 or self.batch_size > 100:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_BATCH_SIZE must be between 1 and 100")
        if self.block_ms <= 0:
            raise PolicyEngineConfigurationError("POLICY_ENGINE_BLOCK_MS must be > 0")
        if not self.policy_version:
            raise PolicyEngineConfigurationError("VERDICT_POLICY_VERSION must not be empty")
        if not self.delivery_policy_version:
            raise PolicyEngineConfigurationError("DELIVERY_POLICY_VERSION must not be empty")
        if self.enable_notification_send and self.operator_chat_id == 0:
            raise PolicyEngineConfigurationError(
                "TELEGRAM_OPERATOR_CHAT_ID is required when ENABLE_NOTIFICATION_SEND=true"
            )
        if not self.render_profile_high:
            raise PolicyEngineConfigurationError("NOTIFY_RENDER_PROFILE_HIGH must not be empty")
        if not self.render_profile_normal:
            raise PolicyEngineConfigurationError("NOTIFY_RENDER_PROFILE_NORMAL must not be empty")


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
