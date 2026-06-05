from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.maintenance.models import DeliveryResultWorkerResult, OutboxEvent, StreamMessage
from services.maintenance.retry_policy import (
    DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
    DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
)
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import MaintenanceQueueWorker
from services.outbox_relay.config import OutboxRelayConfig
from services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from services.outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError
from services.outbox_relay.service import OutboxRelayService
from tests.component.services.maintenance._fakes import FakeConsumer, FakeRepository, config, latest_delivery_record, plan


EVENT_ID = UUID("11111111-1111-4111-8111-111111111111")
NOTIFICATION_PLAN_ID = UUID("22222222-2222-4222-8222-222222222222")
NOTIFICATION_DELIVERY_RECORD_ID = UUID("33333333-3333-4333-8333-333333333333")
ANALYSIS_ID = UUID("44444444-4444-4444-8444-444444444444")
CANDIDATE_GROUP_ID = UUID("55555555-5555-4555-8555-555555555555")


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

    def as_stream_message(self, *, poisoned_fields: dict[str, str] | None = None) -> StreamMessage:
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
        return [
            row
            for row in self._rows
            if self.status_by_event_id[row.event_id] == "pending"
        ][:limit]

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


class _RecordingMaintenanceService:
    def __init__(self, service: MaintenanceService) -> None:
        self._service = service
        self.trigger_event_ids: list[str] = []
        self.results: list[DeliveryResultWorkerResult | None] = []

    async def handle_maintenance_trigger_event(self, trigger_event_id: str):
        self.trigger_event_ids.append(str(trigger_event_id))
        result = await self._service.handle_maintenance_trigger_event(trigger_event_id)
        self.results.append(result)
        return result

    async def handle_replay_trigger_event(self, trigger_event_id: str) -> None:
        raise AssertionError("q.maintenance delivery-result handoff must not dispatch replay")

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        raise AssertionError("q.maintenance result consumption must not run due retry promotion")


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


def _delivery_result_row(*, delivery_status: str, event_id: UUID = EVENT_ID) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id,
        event_type="notification.delivery.result.v1",
        aggregate_type="notification_plan",
        aggregate_id=NOTIFICATION_PLAN_ID,
        dedupe_key="notification-delivery-result:handoff-acceptance",
        payload_json={
            "notification_plan_id": str(NOTIFICATION_PLAN_ID),
            "notification_delivery_record_id": str(NOTIFICATION_DELIVERY_RECORD_ID),
            "delivery_status": delivery_status,
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _maintenance_repository_for(row: OutboxEventRow, *, latest_status: str) -> FakeRepository:
    repository = FakeRepository()
    notification_plan = replace(
        plan(status=latest_status, send_after=None),
        notification_plan_id=NOTIFICATION_PLAN_ID,
        analysis_id=ANALYSIS_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
    )
    latest = replace(
        latest_delivery_record(
            notification_plan_id=NOTIFICATION_PLAN_ID,
            delivery_status=latest_status,
            attempt_count=1,
        ),
        notification_delivery_record_id=NOTIFICATION_DELIVERY_RECORD_ID,
    )
    repository.plans[NOTIFICATION_PLAN_ID] = notification_plan
    repository.latest_delivery_records[NOTIFICATION_PLAN_ID] = latest
    repository.events[row.event_id] = OutboxEvent(
        event_id=row.event_id,
        event_type=row.event_type,
        aggregate_type=row.aggregate_type,
        aggregate_id=row.aggregate_id,
        payload_json=dict(row.payload_json),
    )
    return repository


async def _publish_once(row: OutboxEventRow) -> tuple[_FakeOutboxRelayRepository, _FakeRedisStreamPublisher]:
    repository = _FakeOutboxRelayRepository([row])
    publisher = _FakeRedisStreamPublisher()
    service = OutboxRelayService(
        _relay_config(),
        repository=repository,
        publisher=publisher,
        route_resolver=OutboxRouteResolver(),
    )

    processed = await service.run_once()

    assert processed == 1
    return repository, publisher


def _assert_id_only_maintenance_fields(fields: dict[str, str], row: OutboxEventRow) -> None:
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "maintenance",
        "root_object_type": "notification_plan",
        "root_object_id": str(NOTIFICATION_PLAN_ID),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    forbidden_field_names = {
        "payload_json",
        "notification_delivery_record_id",
        "delivery_status",
        "telegram_bot_token",
        "openai_api_key",
        "secret",
        "token",
    }
    assert forbidden_field_names.isdisjoint(fields)
    assert all("telegram" not in key.lower() for key in fields)
    assert all("openai" not in key.lower() for key in fields)
    assert all("secret" not in key.lower() for key in fields)
    assert all("token" not in key.lower() for key in fields)


@pytest.mark.asyncio
async def test_delivery_result_outbox_relay_publishes_id_only_q_maintenance_and_worker_marks_sent_success_once() -> None:
    row = _delivery_result_row(delivery_status="sent")
    route = OutboxRouteResolver().resolve(row)
    assert route.queue_name == "q.maintenance"
    assert route.stage_name == "maintenance"

    relay_repository, publisher = await _publish_once(row)

    assert relay_repository.status_by_event_id[row.event_id] == "published"
    assert relay_repository.published_event_ids == [row.event_id]
    assert relay_repository.failed_event_ids == []
    assert relay_repository.job_attempts == [
        _RelayJobAttempt(
            stage_name="maintenance",
            queue_name="q.maintenance",
            root_object_type="notification_plan",
            root_object_id=NOTIFICATION_PLAN_ID,
            attempt_status="succeeded",
            error_code=None,
        )
    ]
    assert len(publisher.published) == 1
    published = publisher.published[0]
    assert published.route == route
    _assert_id_only_maintenance_fields(published.message.as_stream_fields(), row)

    maintenance_repository = _maintenance_repository_for(row, latest_status="sent")
    original_plan = maintenance_repository.plans[NOTIFICATION_PLAN_ID]
    original_latest_delivery = maintenance_repository.latest_delivery_records[NOTIFICATION_PLAN_ID]
    recording_service = _RecordingMaintenanceService(
        MaintenanceService(config(), repository=maintenance_repository)
    )
    poisoned_business_fields = {
        "payload_json": "do-not-trust-stream-payload",
        "notification_plan_id": str(uuid4()),
        "notification_delivery_record_id": str(uuid4()),
        "delivery_status": "failed_retryable",
    }
    worker = MaintenanceQueueWorker(
        config(),
        consumer=FakeConsumer([published.as_stream_message(poisoned_fields=poisoned_business_fields)]),
        service=recording_service,
    )

    first_result = await worker.run_once()

    assert first_result.processed == 1
    assert first_result.acked == 1
    assert recording_service.trigger_event_ids == [str(row.event_id)]
    assert len(recording_service.results) == 1
    assert recording_service.results[0] is not None
    assert recording_service.results[0].classification == "terminal_success"
    assert recording_service.results[0].action == "mark_terminal_success"
    assert recording_service.results[0].marker_written is True
    assert recording_service.results[0].retry_intent_written is False
    assert recording_service.results[0].dead_letter_written is False
    assert recording_service.results[0].replay_request_written is False
    assert maintenance_repository.plan_created_outbox == []
    assert maintenance_repository.dead_letters == []
    assert maintenance_repository.replay_requests == {}
    assert maintenance_repository.replay_status_updates == []
    assert maintenance_repository.plans[NOTIFICATION_PLAN_ID] == original_plan
    assert maintenance_repository.latest_delivery_records[NOTIFICATION_PLAN_ID] == original_latest_delivery
    assert maintenance_repository.upstream_recompute_calls == 0
    assert maintenance_repository.job_attempts == [
        {
            "stage_name": DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
            "queue_name": "q.maintenance",
            "root_object_type": "notification_delivery_record",
            "root_object_id": NOTIFICATION_DELIVERY_RECORD_ID,
            "attempt_status": "succeeded",
            "error_code": DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
        }
    ]

    duplicate_worker = MaintenanceQueueWorker(
        config(),
        consumer=FakeConsumer([published.as_stream_message(poisoned_fields=poisoned_business_fields)]),
        service=recording_service,
    )

    duplicate_result = await duplicate_worker.run_once()

    assert duplicate_result.processed == 1
    assert duplicate_result.acked == 1
    assert recording_service.trigger_event_ids == [str(row.event_id), str(row.event_id)]
    assert len(recording_service.results) == 2
    assert recording_service.results[1] is not None
    assert recording_service.results[1].classification == "terminal_success"
    assert recording_service.results[1].action == "already_marked"
    assert recording_service.results[1].already_marked is True
    assert recording_service.results[1].marker_written is False
    assert maintenance_repository.job_attempts == [
        {
            "stage_name": DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
            "queue_name": "q.maintenance",
            "root_object_type": "notification_delivery_record",
            "root_object_id": NOTIFICATION_DELIVERY_RECORD_ID,
            "attempt_status": "succeeded",
            "error_code": DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
        }
    ]
    assert maintenance_repository.plan_created_outbox == []
    assert maintenance_repository.dead_letters == []
    assert maintenance_repository.replay_requests == {}
    assert maintenance_repository.plans[NOTIFICATION_PLAN_ID] == original_plan
    assert maintenance_repository.latest_delivery_records[NOTIFICATION_PLAN_ID] == original_latest_delivery


def test_delivery_result_routing_guard_keeps_notification_plan_created_on_notifier_queue() -> None:
    resolver = OutboxRouteResolver()
    plan_created = OutboxEventRow(
        event_id=uuid4(),
        event_type="notification.plan.created.v1",
        aggregate_type="notification_plan",
        aggregate_id=NOTIFICATION_PLAN_ID,
        dedupe_key="notification-plan-created:not-maintenance",
        payload_json={"notification_plan_id": str(NOTIFICATION_PLAN_ID)},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )

    route = resolver.resolve(plan_created)

    assert route.queue_name == "q.notification.send"
    assert route.stage_name == "notify"
    assert route.queue_name != "q.maintenance"

    unsupported = OutboxEventRow(
        event_id=uuid4(),
        event_type="notification.delivery.unknown.v1",
        aggregate_type="notification_plan",
        aggregate_id=NOTIFICATION_PLAN_ID,
        dedupe_key="unsupported:not-maintenance",
        payload_json={},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    with pytest.raises(UnsupportedOutboxEventTypeError):
        resolver.resolve(unsupported)


@pytest.mark.asyncio
async def test_failed_retryable_delivery_result_handoff_records_interpretation_without_retry_promotion() -> None:
    row = _delivery_result_row(
        delivery_status="failed_retryable",
        event_id=UUID("66666666-6666-4666-8666-666666666666"),
    )
    _, publisher = await _publish_once(row)
    published = publisher.published[0]
    _assert_id_only_maintenance_fields(published.message.as_stream_fields(), row)

    maintenance_repository = _maintenance_repository_for(row, latest_status="failed_retryable")
    original_plan = maintenance_repository.plans[NOTIFICATION_PLAN_ID]
    recording_service = _RecordingMaintenanceService(
        MaintenanceService(config(), repository=maintenance_repository)
    )
    worker = MaintenanceQueueWorker(
        config(),
        consumer=FakeConsumer([published.as_stream_message()]),
        service=recording_service,
    )

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert len(recording_service.results) == 1
    assert recording_service.results[0] is not None
    assert recording_service.results[0].classification == "retryable_candidate"
    assert recording_service.results[0].action == "record_retryable_interpretation"
    assert recording_service.results[0].reason_code == "failed_retryable_deferred_to_due_scan"
    assert recording_service.results[0].retry_intent_written is False
    assert recording_service.results[0].dead_letter_written is False
    assert recording_service.results[0].replay_request_written is False
    assert maintenance_repository.job_attempts == [
        {
            "stage_name": "maintenance_delivery_result",
            "queue_name": "q.maintenance",
            "root_object_type": "notification_plan",
            "root_object_id": NOTIFICATION_PLAN_ID,
            "attempt_status": "failed_retryable",
            "error_code": "delivery_result_failed_retryable_due_scan_candidate",
        }
    ]
    assert maintenance_repository.plan_created_outbox == []
    assert maintenance_repository.dead_letters == []
    assert maintenance_repository.replay_requests == {}
    assert maintenance_repository.plans[NOTIFICATION_PLAN_ID] == original_plan
