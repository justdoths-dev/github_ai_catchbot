from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from services.notifier_telegram.models import NotificationPlanDraft
from services.notifier_telegram.service import NotifierIdempotencyGuardError
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
async def test_existing_duplicate_sent_plans_noop_without_new_plan_render_delivery_or_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    _add_successful_delivery(
        repository,
        intent,
        material_change_hash=intent.material_change_hash,
        telegram_message_id=101,
        created_at=datetime.now(timezone.utc) - timedelta(minutes=2),
        status="sent",
    )
    _add_successful_delivery(
        repository,
        intent,
        material_change_hash=intent.material_change_hash,
        telegram_message_id=202,
        created_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        status="sent",
    )
    incoming = replace(intent, notification_plan_id=uuid4())

    result = await service(repository, cfg=config(dry_run=False, enable_notification_send=True), client=client).handle_intent(
        incoming
    )

    assert result is not None
    assert result.delivery_status == "suppressed"
    assert result.attempt_count == 0
    assert result.transport_error_code == "duplicate_existing_state"
    assert client.calls == 0
    assert len(repository.plans) == 2
    assert len(repository.renders) == 0
    assert len(repository.delivery_records) == 2
    assert repository.state_transitions[-1]["reason_code"] == "duplicate_existing_state"


@pytest.mark.asyncio
async def test_existing_duplicate_pending_plans_fail_closed_without_adding_rows() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    _add_plan(repository, intent, status="planned")
    _add_plan(repository, intent, status="planned")
    incoming = replace(intent, notification_plan_id=uuid4())

    with pytest.raises(NotifierIdempotencyGuardError) as exc_info:
        await service(repository, cfg=config(dry_run=False, enable_notification_send=True), client=client).handle_intent(
            incoming
        )

    assert exc_info.value.reason_code == "duplicate_existing_state"
    assert client.calls == 0
    assert len(repository.plans) == 2
    assert len(repository.renders) == 0
    assert len(repository.delivery_records) == 0


@pytest.mark.asyncio
async def test_existing_suppressed_plan_noops_without_transport_or_recovery_send() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()
    plan_id = _add_plan(repository, intent, status="suppressed")
    repository.delivery_records.append(
        {
            "notification_delivery_record_id": uuid4(),
            "notification_plan_id": plan_id,
            "result_status": "suppressed",
            "telegram_chat_id": None,
            "telegram_message_id": None,
            "created_at": datetime.now(timezone.utc),
        }
    )

    result = await service(repository, cfg=config(dry_run=False, enable_notification_send=True), client=client).handle_intent(
        intent
    )

    assert result is not None
    assert result.delivery_status == "suppressed"
    assert result.transport_error_code == "notification_existing_suppressed_noop"
    assert client.calls == 0
    assert len(repository.plans) == 1
    assert len(repository.renders) == 0
    assert len(repository.delivery_records) == 1


@pytest.mark.asyncio
async def test_existing_pending_plan_is_reused_without_duplicate_plan_insert() -> None:
    repository, intent = repo_with_valid_case()
    existing_plan_id = _add_plan(repository, intent, status="planned")
    incoming = replace(intent, notification_plan_id=uuid4())

    result = await service(repository, cfg=config(dry_run=True, enable_notification_send=False)).handle_intent(incoming)

    assert result is not None
    assert result.delivery_status == "suppressed"
    assert len(repository.plans) == 1
    assert existing_plan_id in repository.plans
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1


@pytest.mark.asyncio
async def test_existing_render_without_delivery_is_reused_without_duplicate_render() -> None:
    repository, intent = repo_with_valid_case()
    notifier = service(repository, cfg=config(dry_run=True, enable_notification_send=False))

    first = await notifier.handle_intent(intent)
    assert first is not None
    assert len(repository.renders) == 1
    repository.delivery_records.clear()
    repository.plans[intent.notification_plan_id] = replace(repository.plans[intent.notification_plan_id], status="rendered")

    second = await notifier.handle_intent(intent)

    assert second is not None
    assert len(repository.plans) == 1
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1


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
    status: str = "planned",
) -> None:
    plan_id = _add_plan(
        repository,
        replace(intent, material_change_hash=material_change_hash),
        status=status,
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


def _add_plan(repository, intent, *, status: str):
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
        material_change_hash=intent.material_change_hash,
        send_after=None,
        suppress_reason_code=None,
        status=status,
    )
    return plan_id
