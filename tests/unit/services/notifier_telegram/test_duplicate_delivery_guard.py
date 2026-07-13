from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from services.notifier_telegram.models import NotificationPlanDraft
from services.notifier_telegram.service import NotifierIdempotencyGuardError
from tests.component.services.notifier_telegram._fakes import (
    FakeRepository,
    RaisingTelegramClient,
    config,
    repo_with_valid_case,
    service,
)


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
async def test_live_send_enabled_treats_existing_suppressed_plan_as_history_and_sends() -> None:
    repository, intent = repo_with_valid_case()
    client = RecordingTelegramClient(target_chat_id=intent.target_chat_id)
    plan_id = _add_plan(repository, intent, status="suppressed")
    repository.delivery_records.append(
        {
            "notification_delivery_record_id": uuid4(),
            "notification_plan_id": plan_id,
            "result_status": "suppressed",
            "telegram_chat_id": None,
            "telegram_message_id": None,
            "transport_error_code": "notification_send_flag_disabled",
            "created_at": datetime.now(timezone.utc),
        }
    )

    result = await service(repository, cfg=config(dry_run=False, enable_notification_send=True), client=client).handle_intent(
        intent
    )

    assert result is not None
    assert result.delivery_status == "sent"
    assert result.transport_error_code is None
    assert result.telegram_message_id == 909
    assert client.send_message_calls == 1
    assert len(repository.plans) == 1
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 2
    assert repository.delivery_records[-1]["result_status"] == "sent"
    assert len(repository.delivery_outbox) == 1


@pytest.mark.asyncio
async def test_send_disabled_existing_suppressed_plan_still_noops_without_transport() -> None:
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
            "transport_error_code": "notification_send_flag_disabled",
            "created_at": datetime.now(timezone.utc),
        }
    )

    result = await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=False),
        client=client,
    ).handle_intent(intent)

    assert result is not None
    assert result.delivery_status == "suppressed"
    assert result.transport_error_code == "notification_existing_suppressed_noop"
    assert client.calls == 0
    assert len(repository.plans) == 1
    assert len(repository.renders) == 0
    assert len(repository.delivery_records) == 1


@pytest.mark.asyncio
async def test_suppressed_delivery_record_does_not_count_as_successful_material_duplicate() -> None:
    repository, intent = repo_with_valid_case()
    plan_id = _add_plan(repository, intent, status="suppressed")
    repository.delivery_records.append(
        {
            "notification_delivery_record_id": uuid4(),
            "notification_plan_id": plan_id,
            "result_status": "suppressed",
            "telegram_chat_id": None,
            "telegram_message_id": None,
            "transport_error_code": "dry_run_skip_transport",
            "created_at": datetime.now(timezone.utc),
        }
    )

    action = await service(repository, cfg=config(dry_run=False, enable_notification_send=True)).decide_delivery_action(
        intent,
        candidate=repository.candidates[intent.candidate_group_id],
    )

    assert action.mode == "send"
    assert action.reason_code == "notification_no_recent_delivery"


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
async def test_cross_candidate_pending_material_claim_noops_before_render_or_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()
    existing_plan_id = _add_plan(repository, intent, status="planned")
    candidate_group_id = uuid4()
    analysis_id = uuid4()
    repository.candidates[candidate_group_id] = replace(
        repository.candidates[intent.candidate_group_id],
        candidate_group_id=candidate_group_id,
    )
    repository.analyses[analysis_id] = replace(
        repository.analyses[intent.analysis_id],
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
    )
    incoming = replace(
        intent,
        notification_plan_id=uuid4(),
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
    )

    result = await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=client,
    ).handle_intent(incoming)

    assert result is not None
    assert result.delivery_status == "suppressed"
    assert result.transport_error_code == "notification_duplicate_repost_noop"
    assert client.calls == 0
    assert set(repository.plans) == {existing_plan_id}
    assert repository.renders == []
    assert repository.delivery_records == []


@pytest.mark.asyncio
async def test_same_candidate_cross_analysis_material_claim_noops_before_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()
    existing_plan_id = _add_plan(repository, intent, status="planned")
    analysis_id = uuid4()
    repository.analyses[analysis_id] = replace(
        repository.analyses[intent.analysis_id],
        analysis_id=analysis_id,
    )
    incoming = replace(
        intent,
        notification_plan_id=uuid4(),
        analysis_id=analysis_id,
    )

    result = await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=client,
    ).handle_intent(incoming)

    assert result is not None
    assert result.delivery_status == "suppressed"
    assert result.transport_error_code == "notification_duplicate_repost_noop"
    assert client.calls == 0
    assert set(repository.plans) == {existing_plan_id}
    assert repository.renders == []
    assert repository.delivery_records == []


@pytest.mark.asyncio
async def test_same_plan_id_with_mismatched_identity_fails_closed() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()
    repository.plans[intent.notification_plan_id] = NotificationPlanDraft(
        notification_plan_id=intent.notification_plan_id,
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision=intent.delivery_decision,
        urgency_profile=intent.urgency_profile,
        target_chat_id=intent.target_chat_id,
        target_thread_id=intent.target_thread_id,
        render_profile=intent.render_profile,
        dedupe_subject_key="different-subject",
        material_change_hash=intent.material_change_hash,
        send_after=intent.send_after,
        suppress_reason_code=intent.suppress_reason_code,
    )

    with pytest.raises(NotifierIdempotencyGuardError) as exc_info:
        await service(
            repository,
            cfg=config(dry_run=False, enable_notification_send=True),
            client=client,
        ).handle_intent(intent)

    assert exc_info.value.reason_code == "notification_plan_identity_mismatch"
    assert client.calls == 0
    assert repository.renders == []
    assert repository.delivery_records == []


@pytest.mark.asyncio
async def test_different_plan_id_same_analysis_identity_mismatch_fails_closed() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()
    existing_plan_id = _add_plan(repository, intent, status="planned")
    repository.plans[existing_plan_id] = replace(
        repository.plans[existing_plan_id],
        render_profile="different-render-profile",
    )
    incoming = replace(intent, notification_plan_id=uuid4())

    with pytest.raises(NotifierIdempotencyGuardError) as exc_info:
        await service(
            repository,
            cfg=config(dry_run=False, enable_notification_send=True),
            client=client,
        ).handle_intent(incoming)

    assert exc_info.value.reason_code == "notification_plan_identity_mismatch"
    assert client.calls == 0
    assert repository.renders == []
    assert repository.delivery_records == []


@pytest.mark.asyncio
async def test_legacy_candidate_scoped_material_is_noop_for_canonical_repost() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()
    legacy_plan_id = _add_plan(
        repository,
        replace(
            intent,
            dedupe_subject_key=str(intent.candidate_group_id),
            material_change_hash="legacy-candidate-material",
        ),
        status="planned",
    )
    candidate_group_id = uuid4()
    analysis_id = uuid4()
    repository.candidates[candidate_group_id] = replace(
        repository.candidates[intent.candidate_group_id],
        candidate_group_id=candidate_group_id,
    )
    repository.analyses[analysis_id] = replace(
        repository.analyses[intent.analysis_id],
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
    )
    incoming = replace(
        intent,
        notification_plan_id=uuid4(),
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        dedupe_subject_key=repository.candidates[candidate_group_id].primary_canonical_id or "missing",
        material_change_hash="canonical-subject-material",
    )

    result = await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=client,
    ).handle_intent(incoming)

    assert result is not None
    assert result.transport_error_code == "notification_duplicate_repost_noop"
    assert client.calls == 0
    assert set(repository.plans) == {legacy_plan_id}
    assert repository.renders == []
    assert repository.delivery_records == []


@pytest.mark.asyncio
async def test_concurrent_cross_candidate_reposts_share_one_material_claim() -> None:
    base_repository, first_intent = repo_with_valid_case()
    repository = ConcurrentClaimRepository()
    repository.__dict__.update(base_repository.__dict__)
    candidate_group_id = uuid4()
    analysis_id = uuid4()
    repository.candidates[candidate_group_id] = replace(
        repository.candidates[first_intent.candidate_group_id],
        candidate_group_id=candidate_group_id,
    )
    repository.analyses[analysis_id] = replace(
        repository.analyses[first_intent.analysis_id],
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
    )
    second_intent = replace(
        first_intent,
        notification_plan_id=uuid4(),
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
    )
    notifier = service(
        repository,
        cfg=config(dry_run=True, enable_notification_send=False),
    )

    results = await asyncio.gather(
        notifier.handle_intent(first_intent),
        notifier.handle_intent(second_intent),
    )

    assert all(result is not None for result in results)
    assert {result.transport_error_code for result in results if result is not None} == {
        "dry_run_skip_transport",
        "notification_duplicate_repost_noop",
    }
    assert len(repository.plans) == 1
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1


@pytest.mark.asyncio
async def test_concurrent_cross_analysis_digest_claim_uses_only_winner_plan_id() -> None:
    base_repository, base_intent = repo_with_valid_case()
    repository = ConcurrentClaimRepository()
    repository.__dict__.update(base_repository.__dict__)
    repository.analyses[base_intent.analysis_id] = replace(
        repository.analyses[base_intent.analysis_id],
        delivery_decision="send_digest",
    )
    first_intent = replace(
        base_intent,
        delivery_decision="send_digest",
        urgency_profile="digest",
        render_profile="telegram_digest_v1",
    )
    analysis_id = uuid4()
    repository.analyses[analysis_id] = replace(
        repository.analyses[first_intent.analysis_id],
        analysis_id=analysis_id,
    )
    second_intent = replace(
        first_intent,
        notification_plan_id=uuid4(),
        analysis_id=analysis_id,
    )
    notifier = service(repository, cfg=config(dry_run=True, enable_notification_send=False))

    results = await asyncio.gather(
        notifier.handle_intent(first_intent),
        notifier.handle_intent(second_intent),
    )

    assert all(result is not None for result in results)
    assert {result.transport_error_code for result in results if result is not None} == {
        "notification_digest_deferred",
        "notification_duplicate_repost_noop",
    }
    assert len(repository.plans) == 1
    winner_plan_id = next(iter(repository.plans))
    assert {transition["object_id"] for transition in repository.state_transitions} == {winner_plan_id}
    assert repository.renders == []
    assert repository.delivery_records == []


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


class RecordingTelegramClient:
    def __init__(self, *, target_chat_id: int) -> None:
        self.target_chat_id = target_chat_id
        self.send_message_calls = 0
        self.edit_message_text_calls = 0

    async def send_message(self, **kwargs):
        self.send_message_calls += 1
        return {"ok": True, "result": {"message_id": 909, "chat": {"id": self.target_chat_id}}}

    async def edit_message_text(self, **kwargs):
        self.edit_message_text_calls += 1
        raise AssertionError("edit_message_text must not be called")


class ConcurrentClaimRepository(FakeRepository):
    def __init__(self) -> None:
        super().__init__()
        self._preclaim_reads = 0
        self._preclaim_barrier = asyncio.Event()

    async def load_existing_plan_by_subject_material(self, **kwargs):
        if not self.plans and self._preclaim_reads < 2:
            self._preclaim_reads += 1
            if self._preclaim_reads == 2:
                self._preclaim_barrier.set()
            await self._preclaim_barrier.wait()
            return None
        return await super().load_existing_plan_by_subject_material(**kwargs)
