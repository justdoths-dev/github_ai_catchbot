from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest

from services.notifier_telegram.models import StreamMessage
from services.notifier_telegram.worker import NotifierTelegramWorker

from ._fakes import FakeConsumer, RaisingTelegramClient, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_worker_rehydrates_by_trigger_event_id_and_ignores_misleading_redis_fields() -> None:
    repository, intent = repo_with_valid_case()
    misleading_analysis_id = uuid4()
    consumer = FakeConsumer(
        [
            StreamMessage(
                stream="q.notification.send",
                message_id="1-0",
                fields={
                    "trigger_event_id": str(intent.trigger_event_id),
                    "root_object_id": str(misleading_analysis_id),
                    "delivery_decision": "suppress",
                },
            )
        ]
    )
    worker = NotifierTelegramWorker(config(), consumer=consumer, service=service(repository))  # type: ignore[arg-type]

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert repository.loaded_trigger_ids == [intent.trigger_event_id]
    assert repository.renders[0].notification_plan_id == intent.notification_plan_id


@pytest.mark.asyncio
async def test_plan_intent_concretizes_notification_plan() -> None:
    repository, intent = repo_with_valid_case()

    await service(repository).handle_intent(intent)

    assert list(repository.plans) == [intent.notification_plan_id]
    plan = repository.plans[intent.notification_plan_id]
    assert plan.analysis_id == intent.analysis_id
    assert plan.delivery_decision == "send_now"
    assert plan.target_chat_id == intent.target_chat_id


@pytest.mark.asyncio
async def test_dry_run_creates_render_delivery_record_and_outbox_without_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    await service(repository, cfg=config(dry_run=True, enable_notification_send=True), client=client).handle_intent(intent)

    assert client.calls == 0
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1
    assert repository.delivery_records[0]["result_status"] == "suppressed"
    assert repository.delivery_records[0]["attempt_count"] == 0
    assert repository.delivery_records[0]["telegram_response_json"]["reason_code"] == "telegram_dry_run"
    assert repository.delivery_records[0]["telegram_response_json"]["transport_skipped"] is True
    assert len(repository.delivery_outbox) == 1
    assert repository.delivery_outbox[0]["delivery_status"] == "suppressed"
    assert repository.delivery_outbox[0]["attempt_count"] == 0


@pytest.mark.asyncio
async def test_disabled_send_path_does_not_call_telegram() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    await service(repository, cfg=config(dry_run=False, enable_notification_send=False), client=client).handle_intent(intent)

    assert client.calls == 0
    assert repository.delivery_records[0]["result_status"] == "suppressed"
    assert repository.delivery_records[0]["transport_error_code"] == "telegram_send_disabled"
    assert repository.delivery_records[0]["telegram_response_json"]["reason_code"] == "telegram_send_disabled"
    assert repository.delivery_records[0]["telegram_response_json"]["transport_skipped"] is True


@pytest.mark.asyncio
async def test_delivery_result_outbox_emitted_after_delivery_record() -> None:
    repository, intent = repo_with_valid_case()

    await service(repository).handle_intent(intent)

    assert "delivery_record" in repository.operations
    assert "delivery_outbox" in repository.operations
    assert repository.operations.index("delivery_record") < repository.operations.index("delivery_outbox")


@pytest.mark.asyncio
async def test_invalid_mismatched_intent_noops_without_render_or_delivery() -> None:
    repository, intent = repo_with_valid_case()
    bad_intent = replace(intent, candidate_group_id=uuid4())

    await service(repository).handle_intent(bad_intent)

    assert repository.renders == []
    assert repository.delivery_records == []
    assert repository.delivery_outbox == []
    assert repository.state_transitions[0]["to_state"] == "failed_terminal"


@pytest.mark.asyncio
async def test_suppress_path_does_not_call_telegram() -> None:
    repository, intent = repo_with_valid_case()
    suppress_intent = replace(intent, delivery_decision="suppress", urgency_profile="suppressed", suppress_reason_code="policy_skip")
    repository.analyses[intent.analysis_id] = replace(repository.analyses[intent.analysis_id], delivery_decision="suppress")
    client = RaisingTelegramClient()

    await service(repository, client=client).handle_intent(suppress_intent)

    assert client.calls == 0
    assert repository.renders == []
    assert repository.delivery_records == []
    assert repository.plans[suppress_intent.notification_plan_id].status == "suppressed"


@pytest.mark.asyncio
async def test_same_material_idempotency_prevents_duplicate_plan_row() -> None:
    repository, intent = repo_with_valid_case()

    await service(repository).handle_intent(intent)
    await service(repository).handle_intent(replace(intent, notification_plan_id=uuid4()))

    assert len(repository.plans) == 1
