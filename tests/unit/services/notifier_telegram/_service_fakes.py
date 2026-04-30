from __future__ import annotations

from uuid import uuid4

from services.notifier_telegram.config import NotifierTelegramConfig
from services.notifier_telegram.models import NotificationIntentJob, NotificationRenderDraft


def config(*, dry_run: bool = True, enable_notification_send: bool = False) -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        telegram_bot_token="token" if enable_notification_send else "",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        dry_run=dry_run,
        allow_edits=False,
        enable_notification_send=enable_notification_send,
        max_message_chars=3800,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10,
        log_level="INFO",
    )


def intent() -> NotificationIntentJob:
    return NotificationIntentJob(
        trigger_event_id=uuid4(),
        event_type="notification.plan.created.v1",
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=123,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject",
        material_change_hash="hash",
        send_after=None,
        suppress_reason_code=None,
    )


def render() -> NotificationRenderDraft:
    return NotificationRenderDraft(
        notification_plan_id=uuid4(),
        message_text="Verdict: inspect_now",
        entities_json=[],
        link_preview_options_json={"is_disabled": True},
        reply_markup_json=None,
        disable_notification=False,
        protect_content=False,
        parse_strategy="entities",
        render_hash="abc",
    )
