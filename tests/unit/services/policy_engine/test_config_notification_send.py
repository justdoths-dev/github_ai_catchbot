from __future__ import annotations

import pytest

from services.policy_engine.config import PolicyEngineConfig, PolicyEngineConfigurationError


def _config(*, enable_notification_send: bool, operator_chat_id: int) -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=operator_chat_id,
        enable_later_delivery=True,
        enable_silent_later=True,
        enable_notification_send=enable_notification_send,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )


def test_operator_chat_id_not_required_when_notification_send_disabled() -> None:
    _config(enable_notification_send=False, operator_chat_id=0).validate()


def test_operator_chat_id_required_when_notification_send_enabled() -> None:
    with pytest.raises(
        PolicyEngineConfigurationError,
        match="TELEGRAM_OPERATOR_CHAT_ID is required when ENABLE_NOTIFICATION_SEND=true",
    ):
        _config(enable_notification_send=True, operator_chat_id=0).validate()
