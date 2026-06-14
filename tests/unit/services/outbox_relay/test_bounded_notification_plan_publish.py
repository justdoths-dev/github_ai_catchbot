from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.outbox_relay.bounded_notification_plan_publish import (
    BoundedNotificationPlanOutboxPublishConfig,
    BoundedNotificationPlanPublishRuntimeConfig,
    BoundedNotificationPlanRedisPublisherHandle,
    BoundedNotificationPlanRepositoryHandle,
    run_bounded_notification_plan_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_notification_plan_publish.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel_redis_url"
TELEGRAM_TOKEN = "123456:sentinel_telegram_token"
REDIS_MESSAGE_ID = "secret-redis-message-id"
EXCEPTION_DETAIL = "sentinel private xadd failure detail"


class FakeRepository:
    def __init__(
        self,
        *,
        pending_count: int = 1,
        row: OutboxEventRow | None = None,
        operation_log: list[str] | None = None,
        fail_mark_published: bool = False,
        fail_insert_job_attempt: bool = False,
    ) -> None:
        self.pending_count = pending_count
        self.row = row
        self.operation_log = operation_log if operation_log is not None else []
        self.fail_mark_published = fail_mark_published
        self.fail_insert_job_attempt = fail_insert_job_attempt
        self.count_calls = 0
        self.fetch_calls = 0
        self.mark_published_calls: list[UUID] = []
        self.job_attempt_calls: list[dict] = []

    async def count_pending_events(self, *, event_type: str) -> int:
        self.operation_log.append("count")
        self.count_calls += 1
        assert event_type == "notification.plan.created.v1"
        return self.pending_count

    async def fetch_oldest_pending_event(self, *, event_type: str) -> OutboxEventRow | None:
        self.operation_log.append("fetch")
        self.fetch_calls += 1
        assert event_type == "notification.plan.created.v1"
        return self.row

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

        return BoundedNotificationPlanRepositoryHandle(repository=self.repository, close=close)


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

        return BoundedNotificationPlanRedisPublisherHandle(publisher=self.publisher, close=close)


def _runtime_config() -> BoundedNotificationPlanPublishRuntimeConfig:
    return BoundedNotificationPlanPublishRuntimeConfig(database_url=DB_URL, redis_url=REDIS_URL)


def _raising_runtime_config() -> BoundedNotificationPlanPublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _payload() -> dict[str, object]:
    return {
        "notification_plan_id": str(uuid4()),
        "analysis_id": str(uuid4()),
        "candidate_group_id": str(uuid4()),
        "target_chat_id": -100123,
        "material_change_hash": "sentinel_material_change_hash",
        "telegram_bot_token": TELEGRAM_TOKEN,
    }


def _row(*, payload_json: dict[str, object] | None = None) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=uuid4(),
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=uuid4(),
        dedupe_key="notify:sentinel-dedupe-key",
        payload_json=payload_json if payload_json is not None else _payload(),
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _approved_config(**overrides) -> BoundedNotificationPlanOutboxPublishConfig:
    values = {
        "operator_approved": True,
        "allow_database_read": True,
        "allow_redis_write": True,
        "allow_outbox_status_update": True,
        "expected_pending_count": 1,
    }
    values.update(overrides)
    return BoundedNotificationPlanOutboxPublishConfig(**values)


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_db_or_redis() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository(row=_row()))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_notification_plan_outbox_publish(
        BoundedNotificationPlanOutboxPublishConfig(),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["database_read_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_xadd_attempted"] is False
    assert report["event_outbox_status_update_attempted"] is False
    assert report["side_effects"]["db_write"] is False
    assert report["side_effects"]["redis_mutation"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_database_read_allowance_blocks_before_db_session() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository(row=_row()))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_notification_plan_outbox_publish(
        BoundedNotificationPlanOutboxPublishConfig(operator_approved=True),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "database_read_not_allowed"
    assert result.state.database_session_opened is False
    assert result.state.database_read_attempted is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_pending_count_zero_blocks_before_redis() -> None:
    repository = FakeRepository(pending_count=0, row=None)
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "pending_count_mismatch"
    assert report["pending_count_observed"] == 0
    assert report["redis_xadd_attempted"] is False
    assert repository.count_calls == 1
    assert repository.fetch_calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_pending_count_greater_than_expected_blocks_before_redis() -> None:
    repository = FakeRepository(pending_count=2, row=_row())
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(expected_pending_count=1),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "pending_count_mismatch"
    assert result.pending_count_observed == 2
    assert result.state.redis_xadd_attempted is False
    assert repository.fetch_calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_malformed_payload_blocks_before_redis_without_printing_payload() -> None:
    payload = _payload()
    raw_material_hash = str(payload.pop("material_change_hash"))
    row = _row(payload_json=payload)
    repository = FakeRepository(row=row)
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "malformed_event_payload"
    assert result.payload_has_notification_plan_id is True
    assert result.payload_has_material_change_hash is False
    assert result.state.redis_xadd_attempted is False
    assert publisher_builder.calls == 0
    assert raw_material_hash not in rendered
    assert '"payload_json":' not in rendered


@pytest.mark.asyncio
async def test_missing_redis_write_allowance_blocks_before_xadd() -> None:
    repository = FakeRepository(row=_row())
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(allow_redis_write=False),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["error_code"] == "redis_write_not_allowed"
    assert report["redis_xadd_attempted"] is False
    assert report["event_outbox_status_update_attempted"] is False
    assert publisher_builder.calls == 0
    assert repository.mark_published_calls == []


@pytest.mark.asyncio
async def test_successful_fake_run_publishes_thin_message_then_marks_published_and_inserts_job_attempt() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository(row=row, operation_log=operation_log)
    publisher = FakeRedisPublisher(operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)

    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["pending_count_observed"] == 1
    assert report["selected_event_present"] is True
    assert report["selected_event_status"] == "pending"
    assert report["selected_aggregate_type"] == "analysis"
    assert report["payload_has_notification_plan_id"] is True
    assert report["payload_has_analysis_id"] is True
    assert report["payload_has_candidate_group_id"] is True
    assert report["payload_has_target_chat_id"] is True
    assert report["payload_has_material_change_hash"] is True
    assert report["redis_xadd_attempted"] is True
    assert report["redis_xadd_count"] == 1
    assert report["redis_message_id_present"] is True
    assert report["event_outbox_status_update_attempted"] is True
    assert report["event_outbox_marked_published"] is True
    assert report["job_attempt_inserted"] is True
    assert report["side_effects"]["redis_mutation"] is True
    assert report["side_effects"]["db_write"] is True
    assert operation_log == ["count", "fetch", "publish", "mark_published", "insert_job_attempt"]
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls == [
        {
            "stage_name": "notify",
            "queue_name": "q.notification.send",
            "root_object_type": "analysis",
            "root_object_id": row.aggregate_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]

    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.notification.send"
    assert route.stage_name == "notify"
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
    for forbidden in (
        "payload_json",
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "target_chat_id",
        "material_change_hash",
        "rendered_message_text",
    ):
        assert forbidden not in fields


@pytest.mark.asyncio
async def test_redis_xadd_failure_does_not_mark_published_or_insert_job_attempt() -> None:
    operation_log: list[str] = []
    repository = FakeRepository(row=_row(), operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log, failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "redis_xadd_failed"
    assert result.state.redis_xadd_attempted is True
    assert result.redis_xadd_count == 0
    assert result.event_outbox_marked_published is False
    assert result.job_attempt_inserted is False
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []
    assert repository_builder.close_commits == [False]
    assert operation_log == ["count", "fetch", "publish"]
    assert EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_sanitized_output_omits_full_ids_urls_tokens_raw_payload_message_id_and_exception_detail() -> None:
    row = _row()
    repository = FakeRepository(row=row)
    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        DB_URL,
        REDIS_URL,
        TELEGRAM_TOKEN,
        "sentinel_material_change_hash",
        "notify:sentinel-dedupe-key",
        REDIS_MESSAGE_ID,
    ):
        assert raw not in rendered
    assert rendered.count(str(row.event_id)[-8:]) == 1
    assert rendered.count(str(row.aggregate_id)[-8:]) == 1

    failing = await run_bounded_notification_plan_outbox_publish(
        _approved_config(),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository(row=_row())),
        redis_publisher_builder=FakeRedisPublisherBuilder(
            FakeRedisPublisher(failure=RuntimeError(EXCEPTION_DETAIL))
        ),
    )
    assert EXCEPTION_DETAIL not in json.dumps(failing.to_sanitized_dict(), sort_keys=True)


@pytest.mark.asyncio
async def test_missing_status_update_flag_blocks_after_fake_xadd_without_db_update() -> None:
    repository = FakeRepository(row=_row())
    publisher = FakeRedisPublisher()

    result = await run_bounded_notification_plan_outbox_publish(
        _approved_config(allow_outbox_status_update=False),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )

    assert result.error_code == "outbox_status_update_not_allowed"
    assert result.redis_xadd_count == 1
    assert result.state.event_outbox_status_update_attempted is False
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []
    assert len(publisher.publish_calls) == 1


def test_source_ast_guard_has_no_broad_worker_or_external_client_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = set()
    imported_roots = set()
    forbidden_call_attrs = {
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
