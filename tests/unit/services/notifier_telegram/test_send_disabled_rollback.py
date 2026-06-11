from __future__ import annotations

import json
from typing import Any

import pytest

from services.notifier_telegram.models import NotificationIntentJob, NotificationPlanDraft, StreamMessage
from services.notifier_telegram.worker_once import (
    EXPECTED_QUEUE_NAME,
    EXPECTED_ROOT_OBJECT_TYPE,
    EXPECTED_STAGE_NAME,
    WorkerOnceRuntime,
    run_worker_once_invocation,
)
from tests.component.services.notifier_telegram._fakes import (
    FakeRepository,
    RaisingTelegramClient,
    config,
    repo_with_valid_case,
    service,
)


@pytest.mark.asyncio
async def test_send_disabled_rollback_records_suppressed_delivery_result() -> None:
    repository, intent = repo_with_valid_case()
    _seed_suppressed_plan_without_delivery_record(repository, intent)
    client = RaisingTelegramClient()

    result = await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=False),
        client=client,
    ).handle_intent(intent)

    record = repository.delivery_records[0]
    response_json = record["telegram_response_json"]

    assert result is not None
    assert result.delivery_status == "suppressed"
    assert client.calls == 0
    assert repository.plans[intent.notification_plan_id].status == "suppressed"
    assert len(repository.renders) == 1
    assert record["result_status"] == "suppressed"
    assert record["attempt_count"] == 0
    assert response_json["send_disabled"] is True
    assert response_json["dry_run"] is False
    assert response_json["reason_code"] == "notification_send_flag_disabled"
    assert response_json["transport_skipped"] is True
    assert repository.state_transitions[-1]["reason_code"] == "notification_send_flag_disabled"
    assert repository.delivery_outbox[0]["delivery_status"] == "suppressed"
    assert repository.delivery_outbox[0]["attempt_count"] == 0


@pytest.mark.asyncio
async def test_send_disabled_rollback_is_handler_successful_for_worker_once_ack_semantics() -> None:
    repository, intent = repo_with_valid_case()
    _seed_suppressed_plan_without_delivery_record(repository, intent)
    client = RaisingTelegramClient()
    consumer = _OneMessageConsumer(_worker_once_message(intent))

    async def runtime_builder(cfg, state, logger) -> WorkerOnceRuntime:
        del state, logger

        async def dispose() -> None:
            return None

        return WorkerOnceRuntime(
            consumer=consumer,
            service=service(repository, cfg=cfg, client=client),
            dispose=dispose,
        )

    emitted: list[str] = []
    code = await run_worker_once_invocation(
        queue=EXPECTED_QUEUE_NAME,
        confirm_worker_once=True,
        output_format="json",
        emit_json=emitted.append,
        config_loader=lambda: config(dry_run=False, enable_notification_send=False),
        runtime_builder=runtime_builder,
    )
    payload = json.loads(emitted[0])

    assert code == 0
    assert payload["status"] == "processed"
    assert payload["handler_called"] is True
    assert payload["acked"] is True
    assert payload["authority"]["telegram_transport_possible"] is False
    assert consumer.acked == ["1-0"]
    assert client.calls == 0
    assert len(repository.delivery_records) == 1
    assert repository.delivery_records[0]["transport_error_code"] == "notification_send_flag_disabled"


@pytest.mark.asyncio
async def test_send_enabled_transport_path_still_sends() -> None:
    repository, intent = repo_with_valid_case()
    client = _SuccessfulTelegramClient()

    result = await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=client,
    ).handle_intent(intent)

    assert result is not None
    assert result.delivery_status == "sent"
    assert client.send_calls == 1
    assert client.edit_calls == 0
    assert repository.delivery_records[0]["result_status"] == "sent"
    assert repository.delivery_records[0]["transport_error_code"] is None


@pytest.mark.asyncio
async def test_send_disabled_rollback_record_metadata_is_sanitized() -> None:
    repository, intent = repo_with_valid_case()
    _seed_suppressed_plan_without_delivery_record(repository, intent)

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=False),
        client=RaisingTelegramClient(),
    ).handle_intent(intent)

    rendered = json.dumps(
        {
            "delivery_record": repository.delivery_records[0],
            "delivery_outbox": repository.delivery_outbox[0],
        },
        default=str,
        sort_keys=True,
    )

    for forbidden in (
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "OPENAI_API_KEY",
        "GITHUB_TOKEN",
        "Traceback",
        "RAW_EXCEPTION_SENTINEL",
        "prod.env",
        "postgresql" + "://",
        "redis" + "://",
    ):
        assert forbidden not in rendered


def _seed_suppressed_plan_without_delivery_record(repository: FakeRepository, intent: NotificationIntentJob) -> None:
    repository.plans[intent.notification_plan_id] = NotificationPlanDraft(
        notification_plan_id=intent.notification_plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision=intent.delivery_decision,
        urgency_profile=intent.urgency_profile,
        target_chat_id=intent.target_chat_id,
        target_thread_id=intent.target_thread_id,
        render_profile=intent.render_profile,
        dedupe_subject_key=intent.dedupe_subject_key,
        material_change_hash=intent.material_change_hash,
        send_after=intent.send_after,
        suppress_reason_code=intent.suppress_reason_code,
        status="suppressed",
    )


def _worker_once_message(intent: NotificationIntentJob) -> StreamMessage:
    event_id = intent.trigger_event_id
    return StreamMessage(
        stream=EXPECTED_QUEUE_NAME,
        message_id="1-0",
        fields={
            "job_id": f"notify:{event_id}",
            "stage_name": EXPECTED_STAGE_NAME,
            "root_object_type": EXPECTED_ROOT_OBJECT_TYPE,
            "root_object_id": str(intent.analysis_id),
            "idempotency_key": f"q-notification-send:{event_id}",
            "pipeline_run_id": "",
            "not_before": "",
            "trigger_event_id": str(event_id),
        },
    )


class _OneMessageConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self._message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        return None

    async def read_batch(self) -> list[StreamMessage]:
        return [self._message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class _SuccessfulTelegramClient:
    def __init__(self) -> None:
        self.send_calls = 0
        self.edit_calls = 0

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.send_calls += 1
        return {"ok": True, "result": {"message_id": 456, "chat": {"id": kwargs["chat_id"]}}}

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self.edit_calls += 1
        return {"ok": True, "result": {"message_id": 456, "chat": {"id": 12345}}}
