from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.notifier_telegram.models import NotificationPlanDraft

from ._fakes import RaisingTelegramClient, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_dry_run_edit_candidate_never_calls_send_or_edit() -> None:
    repository, intent = repo_with_valid_case()
    old_plan_id = uuid4()
    repository.plans[old_plan_id] = NotificationPlanDraft(
        notification_plan_id=old_plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=intent.target_chat_id,
        target_thread_id=None,
        render_profile=intent.render_profile,
        dedupe_subject_key=intent.dedupe_subject_key,
        material_change_hash="old-material",
        send_after=None,
        suppress_reason_code=None,
    )
    repository.delivery_records.append(
        {
            "notification_delivery_record_id": uuid4(),
            "notification_plan_id": old_plan_id,
            "result_status": "sent",
            "telegram_chat_id": intent.target_chat_id,
            "telegram_message_id": 1234,
            "created_at": datetime.now(timezone.utc),
        }
    )
    client = RaisingTelegramClient()

    await service(
        repository,
        cfg=config(dry_run=True, enable_notification_send=True, allow_edits=True),
        client=client,
    ).handle_intent(replace(intent, material_change_hash="new-material"))

    assert client.calls == 0
    assert repository.delivery_records[-1]["result_status"] == "suppressed"
    assert repository.delivery_records[-1]["telegram_response_json"]["delivery_action"] == "edit"
