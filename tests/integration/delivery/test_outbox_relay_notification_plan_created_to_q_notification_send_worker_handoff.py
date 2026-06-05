from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from services.notifier_telegram.models import NotificationPlanDraft, StreamMessage
from services.notifier_telegram.worker import NotifierTelegramWorker
from services.outbox_relay.config import OutboxRelayConfig
from services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from services.outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError
from services.outbox_relay.service import OutboxRelayService
from tests.component.services.notifier_telegram._fakes import (
    FakeConsumer,
    RaisingTelegramClient,
    config,
    repo_with_valid_case,
    service,
)


FORBIDDEN_STREAM_FIELDS = {
    "payload_json",
    "notification_plan_id",
    "notification_delivery_record_id",
    "delivery_status",
    "telegram_bot_token",
    "telegram_token",
    "openai_api_key",
    "secret",
    "token",
}


@dataclass(frozen=True)
class _RelayJobAttempt:
    stage_name: str
    queue_name: str
    root_object_type: str
    root_object_id: UUID
    attempt_status: str
    error_code: str | None


@dataclass(frozen=True)
class _PublishedMessage:
    route: QueueRoute
    message: RedisQueuedMessage
    message_id: str

    def as_notifier_stream_message(self, *, poisoned_fields: dict[str, str] | None = None) -> StreamMessage:
        fields = self.message.as_stream_fields()
        if poisoned_fields:
            fields.update(poisoned_fields)
        return StreamMessage(
            stream=self.route.queue_name,
            message_id=self.message_id,
            fields=fields,
        )


class _FakeOutboxRelayRepository:
    def __init__(self, rows: list[OutboxEventRow]) -> None:
        self._rows = rows
        self.status_by_event_id = {row.event_id: row.status for row in rows}
        self.published_event_ids: list[UUID] = []
        self.failed_event_ids: list[UUID] = []
        self.job_attempts: list[_RelayJobAttempt] = []

    async def fetch_pending_batch(self, *, limit: int) -> list[OutboxEventRow]:
        return [row for row in self._rows if self.status_by_event_id[row.event_id] == "pending"][:limit]

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        self.status_by_event_id[event_id] = "published"
        self.published_event_ids.append(event_id)

    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None:
        self.status_by_event_id[event_id] = "failed"
        self.failed_event_ids.append(event_id)

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        self.job_attempts.append(
            _RelayJobAttempt(
                stage_name=stage_name,
                queue_name=queue_name,
                root_object_type=root_object_type,
                root_object_id=root_object_id,
                attempt_status=attempt_status,
                error_code=error_code,
            )
        )


class _FakeRedisStreamPublisher:
    def __init__(self) -> None:
        self.published: list[_PublishedMessage] = []

    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        message_id = f"{len(self.published) + 1}-0"
        self.published.append(_PublishedMessage(route=route, message=message, message_id=message_id))
        return message_id


class _SuccessfulTelegramClient:
    def __init__(self) -> None:
        self.calls = 0
        self.sent_payloads: list[dict] = []

    async def send_message(self, **kwargs):
        self.calls += 1
        self.sent_payloads.append(kwargs)
        return {
            "ok": True,
            "result": {
                "message_id": 456,
                "chat": {"id": kwargs["chat_id"]},
            },
        }

    async def edit_message_text(self, **kwargs):
        raise AssertionError("replay recovery must perform a new send")


def _relay_config() -> OutboxRelayConfig:
    return OutboxRelayConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        poll_interval_ms=1000,
        batch_size=10,
        xadd_maxlen=10000,
        log_level="INFO",
    )


def _notification_plan_created_row(*, intent, event_id: UUID | None = None) -> OutboxEventRow:
    event_id = event_id or intent.trigger_event_id
    return OutboxEventRow(
        event_id=event_id,
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=intent.analysis_id,
        dedupe_key="notification-plan-created:handoff-acceptance",
        payload_json={
            "notification_plan_id": str(intent.notification_plan_id),
            "analysis_id": str(intent.analysis_id),
            "candidate_group_id": str(intent.candidate_group_id),
            "delivery_decision": intent.delivery_decision,
            "urgency_profile": intent.urgency_profile,
            "target_chat_id": intent.target_chat_id,
            "target_thread_id": intent.target_thread_id,
            "render_profile": intent.render_profile,
            "dedupe_subject_key": intent.dedupe_subject_key,
            "material_change_hash": intent.material_change_hash,
            "send_after": intent.send_after,
            "suppress_reason_code": intent.suppress_reason_code,
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _event_row(event_type: str) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=uuid4(),
        event_type=event_type,
        aggregate_type="notification_plan",
        aggregate_id=uuid4(),
        dedupe_key=f"dedupe:{event_type}",
        payload_json={},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


async def _publish_once(row: OutboxEventRow) -> tuple[_FakeOutboxRelayRepository, _FakeRedisStreamPublisher]:
    repository = _FakeOutboxRelayRepository([row])
    publisher = _FakeRedisStreamPublisher()
    relay = OutboxRelayService(
        _relay_config(),
        repository=repository,
        publisher=publisher,
        route_resolver=OutboxRouteResolver(),
    )

    processed = await relay.run_once()

    assert processed == 1
    return repository, publisher


def _assert_notification_send_stream_fields(fields: dict[str, str], row: OutboxEventRow) -> None:
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": str(row.aggregate_id),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    assert FORBIDDEN_STREAM_FIELDS.isdisjoint(fields)
    assert all("telegram" not in key.lower() for key in fields)
    assert all("openai" not in key.lower() for key in fields)
    assert all("secret" not in key.lower() for key in fields)
    assert all("token" not in key.lower() for key in fields)


@pytest.mark.asyncio
async def test_notification_plan_created_relay_publishes_id_only_q_notification_send_and_worker_rehydrates() -> None:
    notifier_repository, intent = repo_with_valid_case()
    row = _notification_plan_created_row(intent=intent)
    route = OutboxRouteResolver().resolve(row)

    assert route.queue_name == "q.notification.send"
    assert route.stage_name == "notify"

    relay_repository, publisher = await _publish_once(row)

    assert relay_repository.status_by_event_id[row.event_id] == "published"
    assert relay_repository.published_event_ids == [row.event_id]
    assert relay_repository.failed_event_ids == []
    assert relay_repository.job_attempts == [
        _RelayJobAttempt(
            stage_name="notify",
            queue_name="q.notification.send",
            root_object_type="analysis",
            root_object_id=intent.analysis_id,
            attempt_status="succeeded",
            error_code=None,
        )
    ]
    assert len(publisher.published) == 1
    published = publisher.published[0]
    assert published.route == route
    _assert_notification_send_stream_fields(published.message.as_stream_fields(), row)

    original_analysis = notifier_repository.analyses[intent.analysis_id]
    original_judge_output = notifier_repository.judge_outputs[original_analysis.judge_output_id]
    original_candidate = notifier_repository.candidates[intent.candidate_group_id]
    client = RaisingTelegramClient()
    poisoned_business_fields = {
        "payload_json": "do-not-trust-stream-payload",
        "notification_plan_id": str(uuid4()),
        "delivery_status": "sent",
        "telegram_bot_token": "poisoned",
        "openai_api_key": "poisoned",
    }
    worker = NotifierTelegramWorker(
        config(dry_run=False, enable_notification_send=False),
        consumer=FakeConsumer([published.as_notifier_stream_message(poisoned_fields=poisoned_business_fields)]),
        service=service(
            notifier_repository,
            cfg=config(dry_run=False, enable_notification_send=False),
            client=client,
        ),
    )

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert client.calls == 0
    assert notifier_repository.loaded_trigger_ids == [row.event_id]
    assert len(notifier_repository.plans) == 1
    assert notifier_repository.plans[intent.notification_plan_id].status == "suppressed"
    assert len(notifier_repository.renders) == 1
    assert len(notifier_repository.delivery_records) == 1
    assert notifier_repository.delivery_records[0]["result_status"] == "suppressed"
    assert notifier_repository.delivery_records[0]["transport_error_code"] == "notification_send_flag_disabled"
    assert notifier_repository.delivery_records[0]["telegram_response_json"]["send_disabled"] is True
    assert [transition["to_state"] for transition in notifier_repository.state_transitions] == [
        "rendered",
        "suppressed",
    ]
    assert len(notifier_repository.delivery_outbox) == 1
    assert notifier_repository.delivery_outbox[0]["delivery_status"] == "suppressed"
    assert notifier_repository.analyses[intent.analysis_id] == original_analysis
    assert notifier_repository.judge_outputs[original_analysis.judge_output_id] == original_judge_output
    assert notifier_repository.candidates[intent.candidate_group_id] == original_candidate

    duplicate_worker = NotifierTelegramWorker(
        config(dry_run=True, enable_notification_send=True),
        consumer=FakeConsumer([published.as_notifier_stream_message(poisoned_fields=poisoned_business_fields)]),
        service=service(
            notifier_repository,
            cfg=config(dry_run=True, enable_notification_send=True),
            client=client,
        ),
    )

    duplicate_result = await duplicate_worker.run_once()

    assert duplicate_result.processed == 1
    assert duplicate_result.acked == 1
    assert client.calls == 0
    assert notifier_repository.loaded_trigger_ids == [row.event_id, row.event_id]
    assert len(notifier_repository.plans) == 1
    assert len(notifier_repository.renders) == 1
    assert len(notifier_repository.delivery_records) == 1
    assert len(notifier_repository.delivery_outbox) == 1
    assert notifier_repository.state_transitions[-1]["reason_code"] == "notification_duplicate_terminal_noop"

    replay_intent = replace(intent, trigger_event_id=uuid4())
    notifier_repository.jobs[replay_intent.trigger_event_id] = replay_intent
    replay_row = _notification_plan_created_row(intent=replay_intent)
    _, replay_publisher = await _publish_once(replay_row)
    success_client = _SuccessfulTelegramClient()
    replay_worker = NotifierTelegramWorker(
        config(dry_run=False, enable_notification_send=True),
        consumer=FakeConsumer([replay_publisher.published[0].as_notifier_stream_message(poisoned_fields=poisoned_business_fields)]),
        service=service(
            notifier_repository,
            cfg=config(dry_run=False, enable_notification_send=True),
            client=success_client,
        ),
    )

    replay_result = await replay_worker.run_once()

    assert replay_result.processed == 1
    assert replay_result.acked == 1
    assert success_client.calls == 1
    assert notifier_repository.loaded_trigger_ids == [row.event_id, row.event_id, replay_row.event_id]
    assert len(notifier_repository.plans) == 1
    assert notifier_repository.plans[intent.notification_plan_id].status == "sent"
    assert len(notifier_repository.delivery_records) == 2
    assert notifier_repository.delivery_records[-1]["result_status"] == "sent"
    assert notifier_repository.delivery_records[-1]["telegram_message_id"] == 456
    assert len(notifier_repository.delivery_outbox) == 2
    assert notifier_repository.delivery_outbox[-1]["delivery_status"] == "sent"
    assert notifier_repository.state_transitions[-1]["to_state"] == "sent"
    assert notifier_repository.state_transitions[-1]["reason_code"] != "notification_duplicate_terminal_noop"
    assert notifier_repository.analyses[intent.analysis_id] == original_analysis
    assert notifier_repository.judge_outputs[original_analysis.judge_output_id] == original_judge_output
    assert notifier_repository.candidates[intent.candidate_group_id] == original_candidate


@pytest.mark.asyncio
async def test_same_material_successful_delivery_is_deterministic_noop_without_transport() -> None:
    notifier_repository, intent = repo_with_valid_case()
    existing_plan_id = uuid4()
    notifier_repository.plans[existing_plan_id] = NotificationPlanDraft(
        notification_plan_id=existing_plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision=intent.delivery_decision,
        urgency_profile=intent.urgency_profile,
        target_chat_id=intent.target_chat_id,
        target_thread_id=intent.target_thread_id,
        render_profile=intent.render_profile,
        dedupe_subject_key=intent.dedupe_subject_key,
        material_change_hash=intent.material_change_hash,
        send_after=None,
        suppress_reason_code=None,
        status="sent",
    )
    notifier_repository.delivery_records.append(
        {
            "notification_delivery_record_id": uuid4(),
            "notification_plan_id": existing_plan_id,
            "result_status": "sent",
            "telegram_chat_id": intent.target_chat_id,
            "telegram_message_id": 123,
            "created_at": datetime.now(timezone.utc),
        }
    )
    row = _notification_plan_created_row(intent=intent)
    _, publisher = await _publish_once(row)
    client = RaisingTelegramClient()
    worker = NotifierTelegramWorker(
        config(dry_run=False, enable_notification_send=True),
        consumer=FakeConsumer([publisher.published[0].as_notifier_stream_message()]),
        service=service(
            notifier_repository,
            cfg=config(dry_run=False, enable_notification_send=True),
            client=client,
        ),
    )

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert client.calls == 0
    assert len(notifier_repository.plans) == 1
    assert len(notifier_repository.renders) == 0
    assert len(notifier_repository.delivery_records) == 1
    assert notifier_repository.state_transitions[-1]["reason_code"] == "notification_duplicate_noop"


@pytest.mark.asyncio
async def test_future_send_after_defers_worker_without_transport_or_sent_mark() -> None:
    notifier_repository, intent = repo_with_valid_case()
    future_intent = replace(intent, send_after=datetime.now(timezone.utc) + timedelta(minutes=10))
    notifier_repository.jobs[intent.trigger_event_id] = future_intent
    row = _notification_plan_created_row(intent=future_intent, event_id=intent.trigger_event_id)
    _, publisher = await _publish_once(row)
    client = RaisingTelegramClient()
    worker = NotifierTelegramWorker(
        config(dry_run=False, enable_notification_send=True),
        consumer=FakeConsumer([publisher.published[0].as_notifier_stream_message()]),
        service=service(
            notifier_repository,
            cfg=config(dry_run=False, enable_notification_send=True),
            client=client,
        ),
    )

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert client.calls == 0
    assert notifier_repository.renders == []
    assert notifier_repository.delivery_records == []
    assert notifier_repository.plans[future_intent.notification_plan_id].status == "planned"
    assert notifier_repository.state_transitions[-1]["reason_code"] == "notification_send_after_deferred"


def test_delivery_result_routes_to_maintenance_and_unsupported_event_is_rejected() -> None:
    resolver = OutboxRouteResolver()

    maintenance_route = resolver.resolve(_event_row("notification.delivery.result.v1"))

    assert maintenance_route.queue_name == "q.maintenance"
    assert maintenance_route.stage_name == "maintenance"
    with pytest.raises(UnsupportedOutboxEventTypeError):
        resolver.resolve(_event_row("unsupported.event.v1"))
