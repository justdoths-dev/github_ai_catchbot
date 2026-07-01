from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.maintenance.models import DeliveryResultWorkerResult, OutboxEvent, StreamMessage
from services.maintenance.bounded_runtime import (
    MAINTENANCE_RESULT_COMMAND,
    BoundedMaintenanceQueueOnceConfig,
    BoundedMaintenanceRuntimeConfig,
    RedisExactNextMaintenanceConsumer,
    run_bounded_maintenance_queue_once,
)
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import MaintenanceQueueWorker
from services.outbox_relay.eligibility import (
    DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
    DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
    EVENT_OUTBOX_ROOT_OBJECT_TYPE,
    MAINTENANCE_DELIVERY_RESULT_STAGE,
)
from services.outbox_relay.config import OutboxRelayConfig
from services.outbox_relay.bounded_delivery_result_outbox_publish import (
    BoundedDeliveryResultOutboxPublishConfig,
    BoundedDeliveryResultPublishRuntimeConfig,
    BoundedDeliveryResultRedisPublisherHandle,
    BoundedDeliveryResultRepositoryHandle,
    run_bounded_delivery_result_outbox_publish,
)
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

    async def fetch_event_by_id(self, *, event_id: UUID) -> OutboxEventRow | None:
        for row in self._rows:
            if row.event_id == event_id:
                return row
        return None

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
        message_id = f"1740000000000-{len(self.published) + 1}"
        self.published.append(_PublishedMessage(route=route, message=message, message_id=message_id))
        return message_id


class _FakeDeliveryResultRepositoryBuilder:
    def __init__(self, repository: _FakeOutboxRelayRepository) -> None:
        self.repository = repository
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)

        return BoundedDeliveryResultRepositoryHandle(repository=self.repository, close=close)


class _FakeDeliveryResultRedisPublisherBuilder:
    def __init__(self, publisher: _FakeRedisStreamPublisher) -> None:
        self.publisher = publisher
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.redis_publisher_created = True

        async def close() -> None:
            self.close_calls += 1

        return BoundedDeliveryResultRedisPublisherHandle(publisher=self.publisher, close=close)


class _ExactMaintenanceFakeRedis:
    def __init__(self, message: StreamMessage) -> None:
        self.message = message
        self.pending = 0
        self.lag = 1
        self.xreadgroup_calls: list[dict] = []
        self.acked: list[str] = []

    async def xinfo_groups(self, name):
        assert name == "q.maintenance"
        return [
            {
                "name": "maintenance",
                "pending": self.pending,
                "lag": self.lag,
                "last-delivered-id": "0-0",
            }
        ]

    async def xrange(self, name, min="-", max="+", count=None):
        del min, max
        assert name == "q.maintenance"
        return [(self.message.message_id, self.message.fields)][: count or 1]

    async def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        self.xreadgroup_calls.append(
            {
                "groupname": groupname,
                "consumername": consumername,
                "streams": streams,
                "count": count,
                "block": block,
            }
        )
        self.pending = 1
        return [("q.maintenance", [(self.message.message_id, self.message.fields)])]

    async def xack(self, name, groupname, *ids):
        assert name == "q.maintenance"
        assert groupname == "maintenance"
        self.acked.extend(ids)
        self.pending = 0
        self.lag = 0
        return len(ids)


class _ExactMaintenanceRuntime:
    def __init__(self, *, redis: _ExactMaintenanceFakeRedis, repository: FakeRepository, state) -> None:
        self.consumer = RedisExactNextMaintenanceConsumer(
            redis,
            queue_name="q.maintenance",
            consumer_group="maintenance",
            consumer_name="test",
            block_ms=1,
            state=state,
        )
        self.repository = repository
        self.service = MaintenanceService(config(), repository=repository)
        self.state = state
        self.order: list[str] = []

    async def inspect_target(self, config):
        return await self.consumer.inspect_target(config)

    async def consume_target(self, expected, config):
        return await self.consumer.consume_target(expected, config)

    async def load_outbox_event(self, trigger_event_id: UUID):
        self.state.database_read_attempted = True
        return self.repository.events.get(trigger_event_id)

    async def invoke_maintenance(self, trigger_event_id: UUID):
        self.order.append("invoke_maintenance")
        self.state.service_called = True
        self.state.database_write_attempted = True
        return await self.service.handle_maintenance_trigger_event(trigger_event_id)

    async def invoke_replay(self, trigger_event_id: UUID):
        raise AssertionError(f"unexpected replay invocation {trigger_event_id}")

    async def commit_database(self):
        self.order.append("commit")
        self.state.database_committed = True

    async def rollback_database(self):
        self.order.append("rollback")
        self.state.database_rolled_back = True

    async def ack(self, message_id: str):
        self.order.append("ack")
        return await self.consumer.ack(message_id)

    async def close(self):
        return None


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


def _delivery_result_publish_runtime_config() -> BoundedDeliveryResultPublishRuntimeConfig:
    return BoundedDeliveryResultPublishRuntimeConfig(
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
    )


def _maintenance_runtime_config() -> BoundedMaintenanceRuntimeConfig:
    return BoundedMaintenanceRuntimeConfig(maintenance_config=config())


def _bounded_queue_config(
    *,
    row: OutboxEventRow,
    redis_message_id: str,
    mode: str,
) -> BoundedMaintenanceQueueOnceConfig:
    return BoundedMaintenanceQueueOnceConfig(
        command=MAINTENANCE_RESULT_COMMAND,
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=mode == "execute",
        allow_database_write=mode == "execute",
        allow_redis_read=True,
        allow_redis_consume=mode == "execute",
        allow_redis_ack=mode == "execute",
        mode=mode,
        trigger_event_suffix=str(row.event_id)[-8:],
        root_object_id_suffix=str(row.aggregate_id)[-8:],
        redis_message_id_suffix=redis_message_id[-8:],
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
            "attempt_count": 1,
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
            "stage_name": MAINTENANCE_DELIVERY_RESULT_STAGE,
            "queue_name": "q.maintenance",
            "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
            "root_object_id": row.event_id,
            "attempt_status": "succeeded",
            "error_code": DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
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
            "stage_name": MAINTENANCE_DELIVERY_RESULT_STAGE,
            "queue_name": "q.maintenance",
            "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
            "root_object_id": row.event_id,
            "attempt_status": "succeeded",
            "error_code": DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
        }
    ]
    assert maintenance_repository.plan_created_outbox == []
    assert maintenance_repository.dead_letters == []
    assert maintenance_repository.replay_requests == {}
    assert maintenance_repository.plans[NOTIFICATION_PLAN_ID] == original_plan
    assert maintenance_repository.latest_delivery_records[NOTIFICATION_PLAN_ID] == original_latest_delivery


@pytest.mark.asyncio
async def test_exact_delivery_result_publish_then_bounded_q_maintenance_once_drains_target_and_ignores_stale_outbox() -> None:
    stale = _delivery_result_row(
        delivery_status="sent",
        event_id=UUID("77777777-7777-4777-8777-777777777777"),
    )
    target = _delivery_result_row(delivery_status="sent")
    relay_repository = _FakeOutboxRelayRepository([stale, target])
    redis_publisher = _FakeRedisStreamPublisher()
    repository_builder = _FakeDeliveryResultRepositoryBuilder(relay_repository)

    publish_result = await run_bounded_delivery_result_outbox_publish(
        BoundedDeliveryResultOutboxPublishConfig(
            operator_approved=True,
            target_event_id=target.event_id,
            allow_database_read=True,
            allow_redis_write=True,
            allow_outbox_status_update=True,
        ),
        runtime_config_loader=_delivery_result_publish_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=_FakeDeliveryResultRedisPublisherBuilder(redis_publisher),
    )

    assert publish_result.ok is True
    assert publish_result.queue_name == "q.maintenance"
    assert publish_result.stage_name == "maintenance"
    assert relay_repository.status_by_event_id[target.event_id] == "published"
    assert relay_repository.status_by_event_id[stale.event_id] == "pending"
    assert relay_repository.published_event_ids == [target.event_id]
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
    assert repository_builder.close_commits == [True]
    assert len(redis_publisher.published) == 1
    published = redis_publisher.published[0]
    _assert_id_only_maintenance_fields(published.message.as_stream_fields(), target)

    maintenance_repository = _maintenance_repository_for(target, latest_status="sent")
    redis = _ExactMaintenanceFakeRedis(published.as_stream_message())
    runtime_holder: dict[str, _ExactMaintenanceRuntime] = {}

    async def runtime_builder(runtime_config, state, logger):
        del runtime_config, logger
        runtime = _ExactMaintenanceRuntime(redis=redis, repository=maintenance_repository, state=state)
        runtime_holder["runtime"] = runtime
        return runtime

    preview = await run_bounded_maintenance_queue_once(
        _bounded_queue_config(row=target, redis_message_id=published.message_id, mode="preview"),
        runtime_config_loader=_maintenance_runtime_config,
        runtime_builder=runtime_builder,
    )

    assert preview.ok is True
    assert preview.redis_selection is not None
    assert preview.redis_selection.group_lag == 1
    assert preview.redis_selection.group_pending == 0
    assert preview.state.redis_consume_called is False
    assert preview.state.redis_ack_attempted is False
    assert maintenance_repository.job_attempts == []

    execute = await run_bounded_maintenance_queue_once(
        _bounded_queue_config(row=target, redis_message_id=published.message_id, mode="execute"),
        runtime_config_loader=_maintenance_runtime_config,
        runtime_builder=runtime_builder,
    )

    assert execute.ok is True
    assert execute.service_result is not None
    assert execute.service_result.classification == "terminal_success"
    assert execute.service_result.action == "mark_terminal_success"
    assert execute.service_result.marker_written is True
    assert execute.acked is True
    assert redis.xreadgroup_calls == [
        {
            "groupname": "maintenance",
            "consumername": "test",
            "streams": {"q.maintenance": ">"},
            "count": 1,
            "block": 1,
        }
    ]
    assert redis.acked == [published.message_id]
    assert redis.pending == 0
    assert redis.lag == 0
    assert runtime_holder["runtime"].order == ["invoke_maintenance", "commit", "ack"]
    assert maintenance_repository.job_attempts == [
        {
            "stage_name": MAINTENANCE_DELIVERY_RESULT_STAGE,
            "queue_name": "q.maintenance",
            "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
            "root_object_id": target.event_id,
            "attempt_status": "succeeded",
            "error_code": DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
        }
    ]
    assert maintenance_repository.plan_created_outbox == []
    assert maintenance_repository.dead_letters == []
    assert maintenance_repository.replay_requests == {}


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
            "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
            "root_object_id": row.event_id,
            "attempt_status": "succeeded",
            "error_code": DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
        }
    ]
    assert maintenance_repository.plan_created_outbox == []
    assert maintenance_repository.dead_letters == []
    assert maintenance_repository.replay_requests == {}
    assert maintenance_repository.plans[NOTIFICATION_PLAN_ID] == original_plan
