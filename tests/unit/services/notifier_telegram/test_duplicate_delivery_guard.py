from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from services.notifier_telegram.models import NotificationPlanDraft
from tests.component.services.notifier_telegram._fakes import RaisingTelegramClient, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_same_material_already_delivered_is_noop_without_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    await service(repository, cfg=config(dry_run=True, enable_notification_send=True), client=client).handle_intent(intent)
    repository.delivery_records[0]["result_status"] = "sent"
    repository.delivery_records[0]["telegram_message_id"] = 777

    await service(repository, cfg=config(dry_run=False, enable_notification_send=True), client=client).handle_intent(intent)

    assert client.calls == 0
    assert len(repository.delivery_records) == 1
    assert repository.state_transitions[-1]["reason_code"] == "notification_duplicate_noop"


@pytest.mark.asyncio
async def test_older_same_material_delivery_is_noop_when_newer_different_material_exists() -> None:
    repository, intent = repo_with_valid_case()
    older = datetime.now(timezone.utc) - timedelta(minutes=2)
    newer = datetime.now(timezone.utc) - timedelta(minutes=1)

    _add_successful_delivery(
        repository,
        intent,
        material_change_hash="material-a",
        telegram_message_id=101,
        created_at=older,
    )
    _add_successful_delivery(
        repository,
        intent,
        material_change_hash="material-b",
        telegram_message_id=202,
        created_at=newer,
    )
    incoming = replace(intent, material_change_hash="material-a")

    action = await service(repository, cfg=config(dry_run=False, enable_notification_send=True)).decide_delivery_action(
        incoming,
        candidate=repository.candidates[incoming.candidate_group_id],
    )

    assert action.mode == "noop"
    assert action.reason_code == "notification_duplicate_noop"


def _add_successful_delivery(
    repository,
    intent,
    *,
    material_change_hash: str,
    telegram_message_id: int,
    created_at: datetime,
) -> None:
    plan_id = uuid4()
    repository.plans[plan_id] = NotificationPlanDraft(
        notification_plan_id=plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=intent.target_chat_id,
        target_thread_id=None,
        render_profile=intent.render_profile,
        dedupe_subject_key=intent.dedupe_subject_key,
        material_change_hash=material_change_hash,
        send_after=None,
        suppress_reason_code=None,
    )
    repository.delivery_records.append(
        {
            "notification_delivery_record_id": uuid4(),
            "notification_plan_id": plan_id,
            "result_status": "sent",
            "telegram_chat_id": intent.target_chat_id,
            "telegram_message_id": telegram_message_id,
            "created_at": created_at,
        }
    )
