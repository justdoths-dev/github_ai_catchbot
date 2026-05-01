from __future__ import annotations

from uuid import uuid4

import pytest

from services.notifier_telegram.config import NotifierTelegramConfig
from services.notifier_telegram.models import NotificationIntentJob
from services.notifier_telegram.service import NotifierTelegramService


class Repo:
    async def load_successful_delivery_for_material(
        self, *, dedupe_subject_key: str, target_chat_id: int, material_change_hash: str
    ):
        return None

    async def load_recent_successful_delivery(self, *, dedupe_subject_key: str, target_chat_id: int):
        return None

    async def has_previous_edit_restriction(self, *, notification_plan_id):
        return False


def _config() -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        telegram_bot_token="",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        dry_run=True,
        allow_edits=False,
        enable_notification_send=False,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10,
        log_level="INFO",
    )


def _intent(delivery_decision: str = "send_now") -> NotificationIntentJob:
    return NotificationIntentJob(
        trigger_event_id=uuid4(),
        event_type="notification.plan.created.v1",
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision=delivery_decision,  # type: ignore[arg-type]
        urgency_profile="high",
        target_chat_id=123,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject",
        material_change_hash="hash",
        send_after=None,
        suppress_reason_code=None,
    )


@pytest.mark.asyncio
async def test_delivery_action_send_and_noop_paths() -> None:
    service = NotifierTelegramService(_config(), repository=Repo())  # type: ignore[arg-type]

    assert (await service.decide_delivery_action(_intent())).mode == "send"
    digest = await service.decide_delivery_action(_intent("send_digest"))

    assert digest.mode == "noop"
    assert digest.reason_code == "notification_not_immediate_send"
