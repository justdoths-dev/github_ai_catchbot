from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.outbox_relay.bounded_delivery_result_outbox_publish import (
    BoundedDeliveryResultOutboxPublishConfig,
    BoundedDeliveryResultPublishRuntimeConfig,
    BoundedDeliveryResultRedisPublisherHandle,
    BoundedDeliveryResultRepositoryHandle,
    run_bounded_delivery_result_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_delivery_result_outbox_publish.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel_redis_url"
REDIS_MESSAGE_ID = "1740000000000-secret"
EXCEPTION_DETAIL = "sentinel private xadd failure detail"
PLAN_ID = UUID("22222222-2222-4222-8222-222222222222")
DELIVERY_RECORD_ID = UUID("33333333-3333-4333-8333-333333333333")


class FakeRepository:
    def __init__(
        self,
        *,
        rows: list[OutboxEventRow],
        operation_log: list[str] | None = None,
        fail_mark_published: bool = False,
        fail_insert_job_attempt: bool = False,
    ) -> None:
        self.rows = list(rows)
        self.operation_log = operation_log if operation_log is not None else []
        self.fail_mark_published = fail_mark_published
        self.fail_insert_job_attempt = fail_insert_job_attempt
        self.fetch_by_id_calls: list[UUID] = []
        self.mark_published_calls: list[UUID] = []
        self.job_attempt_calls: list[dict] = []

    async def fetch_event_by_id(self, *, event_id: UUID) -> OutboxEventRow | None:
        self.operation_log.append("fetch_by_id")
        self.fetch_by_id_calls.append(event_id)
        for row in self.rows:
            if row.event_id == event_id:
                return row
        return None

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        del published_at
        self.operation_log.append("mark_published")
        if self.fail_mark_published:
            raise RuntimeError("sentinel mark published detail")
        self.mark_published_calls.append(event_id)

    async def insert_job_attempt(self, **kwargs) -> None:
        self.operation_log.append("insert_job_attempt")
        if self.fail_insert_job_attempt:
            raise RuntimeError("sentinel job attempt detail")
        self.job_attempt_calls.append(kwargs)


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)

        return BoundedDeliveryResultRepositoryHandle(repository=self.repository, close=close)


class FakeRedisPublisher:
    def __init__(
        self,
        *,
        operation_log: list[str] | None = None,
        message_id: str = REDIS_MESSAGE_ID,
        failure: BaseException | None = None,
    ) -> None:
        self.operation_log = operation_log if operation_log is not None else []
        self.message_id = message_id
        self.failure = failure
        self.publish_calls: list[tuple[object, object]] = []

    async def publish(self, route, message) -> str:
        self.operation_log.append("publish")
        self.publish_calls.append((route, message))
        if self.failure is not None:
            raise self.failure
        return self.message_id


class FakeRedisPublisherBuilder:
    def __init__(self, publisher: FakeRedisPublisher) -> None:
        self.publisher = publisher
        self.calls = 0
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_publisher_created = True

        async def close() -> None:
            self.close_calls += 1

        return BoundedDeliveryResultRedisPublisherHandle(publisher=self.publisher, close=close)


class WrongRouteResolver:
    def resolve(self, row: OutboxEventRow) -> QueueRoute:
        del row
        return QueueRoute(queue_name="q.notification.send", stage_name="notify")


def _runtime_config() -> BoundedDeliveryResultPublishRuntimeConfig:
    return BoundedDeliveryResultPublishRuntimeConfig(database_url=DB_URL, redis_url=REDIS_URL)


def _raising_runtime_config() -> BoundedDeliveryResultPublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _payload(**overrides) -> dict[str, object]:
    values: dict[str, object] = {
        "notification_plan_id": str(PLAN_ID),
        "notification_delivery_record_id": str(DELIVERY_RECORD_ID),
        "delivery_status": "sent",
        "attempt_count": 1,
        "transport_error_code": None,
        "telegram_response_json": {"token": "sentinel_private_telegram_payload"},
        "telegram_chat_id": -100123456,
        "telegram_message_id": 98765,
    }
    values.update(overrides)
    return values


def _row(**overrides) -> OutboxEventRow:
    values = {
        "event_id": uuid4(),
        "event_type": "notification.delivery.result.v1",
        "aggregate_type": "notification_plan",
        "aggregate_id": PLAN_ID,
        "dedupe_key": "notification-delivery-result:sentinel-dedupe-key",
        "payload_json": _payload(),
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }
    values.update(overrides)
    return OutboxEventRow(**values)


def _approved_config(**overrides) -> BoundedDeliveryResultOutboxPublishConfig:
    values = {
        "operator_approved": True,
        "target_event_id": uuid4(),
        "allow_database_read": True,
        "allow_redis_write": True,
        "allow_outbox_status_update": True,
    }
    values.update(overrides)
    return BoundedDeliveryResultOutboxPublishConfig(**values)


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_db_or_redis() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository(rows=[_row()]))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_delivery_result_outbox_publish(
        BoundedDeliveryResultOutboxPublishConfig(),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )

    assert result.status == "blocked"
    assert result.error_code == "operator_approval_missing"
    assert result.state.database_read_attempted is False
    assert result.state.redis_xadd_attempted is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_success_publishes_exact_delivery_result_to_q_maintenance_then_marks_and_records_job() -> None:
    operation_log: list[str] = []
    stale = _row()
    target = _row(event_id=UUID("11111111-1111-4111-8111-111111111111"))
    repository = FakeRepository(rows=[stale, target], operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log)

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=target.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["queue_name"] == "q.maintenance"
    assert report["stage_name"] == "maintenance"
    assert report["selected_event_id_suffix"] == "11111111"
    assert report["selected_aggregate_id_suffix"] == str(PLAN_ID).replace("-", "")[-8:]
    assert report["payload_has_notification_plan_id"] is True
    assert report["payload_has_notification_delivery_record_id"] is True
    assert report["payload_has_delivery_status"] is True
    assert report["payload_has_attempt_count"] is True
    assert report["payload_notification_plan_id_matches_aggregate"] is True
    assert report["redis_xadd_count"] == 1
    assert report["event_outbox_marked_published"] is True
    assert report["job_attempt_inserted"] is True
    assert operation_log == ["fetch_by_id", "publish", "mark_published", "insert_job_attempt"]
    assert repository_builder.close_commits == [True]
    assert repository.fetch_by_id_calls == [target.event_id]
    assert repository.mark_published_calls == [target.event_id]
    assert stale.event_id not in repository.mark_published_calls
    assert repository.job_attempt_calls == [
        {
            "stage_name": "maintenance",
            "queue_name": "q.maintenance",
            "root_object_type": "notification_plan",
            "root_object_id": PLAN_ID,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]


@pytest.mark.asyncio
async def test_redis_fields_are_id_only_and_do_not_include_delivery_payload_or_secrets() -> None:
    row = _row()
    publisher = FakeRedisPublisher()

    await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )

    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.maintenance"
    assert route.stage_name == "maintenance"
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "maintenance",
        "root_object_type": "notification_plan",
        "root_object_id": str(PLAN_ID),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    forbidden = {
        "payload_json",
        "notification_plan_id",
        "notification_delivery_record_id",
        "delivery_status",
        "telegram_response_json",
        "telegram_chat_id",
        "telegram_message_id",
        "database_url",
        "redis_url",
        "openai_api_key",
        "github_token",
        "secret",
        "token",
    }
    assert forbidden.isdisjoint(fields)
    assert all("telegram" not in key.lower() for key in fields)
    assert all("openai" not in key.lower() for key in fields)
    assert all("github" not in key.lower() for key in fields)
    assert all("secret" not in key.lower() for key in fields)
    assert all("token" not in key.lower() for key in fields)


@pytest.mark.asyncio
async def test_redis_xadd_failure_does_not_mark_published_or_insert_job_attempt() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository(rows=[row], operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log, failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "redis_xadd_failed"
    assert result.redis_xadd_count == 0
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []
    assert repository_builder.close_commits == [False]
    assert operation_log == ["fetch_by_id", "publish"]
    assert EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_database_write_after_xadd_failure_rolls_back_without_claiming_success() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository(rows=[row], operation_log=operation_log, fail_insert_job_attempt=True)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log)

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )

    assert result.status == "failed"
    assert result.error_code == "database_write_failed_after_redis_publish"
    assert result.redis_xadd_count == 1
    assert result.event_outbox_marked_published is True
    assert result.job_attempt_inserted is False
    assert repository_builder.close_commits == [False]
    assert operation_log == ["fetch_by_id", "publish", "mark_published", "insert_job_attempt"]


@pytest.mark.asyncio
async def test_wrong_event_type_rejects_before_redis() -> None:
    row = _row(event_type="notification.plan.created.v1")
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_type_mismatch"
    assert result.state.redis_xadd_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_already_published_event_rejects_before_redis() -> None:
    row = _row(status="published")
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_not_pending"
    assert result.selected_event_status == "published"
    assert result.state.redis_xadd_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        _payload(notification_delivery_record_id=""),
        _payload(delivery_status=""),
        _payload(attempt_count=None),
        _payload(attempt_count="not-int"),
        _payload(attempt_count=0),
    ],
)
async def test_missing_or_malformed_payload_rejects_before_redis(payload: dict[str, object]) -> None:
    row = _row(payload_json=payload)
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "malformed_event_payload"
    assert result.state.redis_xadd_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_payload_plan_id_must_match_aggregate_id_before_redis() -> None:
    row = _row(payload_json=_payload(notification_plan_id=str(uuid4())))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "payload_notification_plan_id_mismatch"
    assert result.payload_notification_plan_id_matches_aggregate is False
    assert result.state.redis_xadd_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_route_must_resolve_to_q_maintenance_before_redis() -> None:
    row = _row()
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=publisher_builder,
        route_resolver=WrongRouteResolver(),
    )

    assert result.error_code == "route_not_allowed"
    assert result.queue_name == "q.notification.send"
    assert result.stage_name == "notify"
    assert result.state.redis_xadd_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_sanitized_output_omits_raw_ids_urls_payload_secrets_message_id_and_exception_detail() -> None:
    row = _row()
    publisher = FakeRedisPublisher()
    result = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        str(DELIVERY_RECORD_ID),
        row.dedupe_key,
        DB_URL,
        REDIS_URL,
        REDIS_MESSAGE_ID,
        "sentinel_private_telegram_payload",
        str(row.payload_json["telegram_chat_id"]),
        str(row.payload_json["telegram_message_id"]),
    ):
        assert raw not in rendered
    assert '"payload_json":' not in rendered
    assert str(row.event_id).replace("-", "")[-8:] in rendered
    assert str(PLAN_ID).replace("-", "")[-8:] in rendered
    assert str(DELIVERY_RECORD_ID).replace("-", "")[-8:] in rendered

    failing = await run_bounded_delivery_result_outbox_publish(
        _approved_config(target_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(rows=[row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(
            FakeRedisPublisher(failure=RuntimeError(EXCEPTION_DETAIL))
        ),
    )
    assert EXCEPTION_DETAIL not in json.dumps(failing.to_sanitized_dict(), sort_keys=True)


def test_source_ast_guard_has_no_broad_worker_consume_or_external_client_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = set()
    imported_roots = set()
    forbidden_call_attrs = {
        "xreadgroup",
        "xack",
        "xclaim",
        "xautoclaim",
        "xdel",
        "xgroup_create",
        "run_forever",
        "sleep",
        "system",
        "popen",
        "call",
        "check_call",
        "check_output",
    }

    assert not any(isinstance(node, ast.While) for node in ast.walk(tree))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in forbidden_call_attrs

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(imported_roots)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "OutboxRelayService" not in source
    assert "run_forever(" not in source
