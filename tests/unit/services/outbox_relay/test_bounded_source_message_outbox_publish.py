from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.outbox_relay.bounded_source_message_outbox_publish_runner import (
    BoundedSourceMessageOutboxPublishConfig,
    BoundedSourceMessagePublishRuntimeConfig,
    BoundedSourceMessageRedisPublisherHandle,
    BoundedSourceMessageRepositoryHandle,
    run_bounded_source_message_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_source_message_outbox_publish_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel_redis_url"
RAW_PAYLOAD_VALUE = "sentinel source message text from payload"
RAW_DEDUPE_KEY = "srcmsg:create:sentinel-dedupe-key"
REDIS_MESSAGE_ID = "secret-source-redis-message-id"
EXCEPTION_DETAIL = "sentinel private xadd failure detail"
CLOSE_EXCEPTION_DETAIL = "sentinel private repository close detail"


class FakeRepository:
    def __init__(
        self,
        rows: list[OutboxEventRow] | None = None,
        *,
        operation_log: list[str] | None = None,
        fail_mark_published: bool = False,
        fail_insert_job_attempt: bool = False,
        fail_mark_failed: bool = False,
    ) -> None:
        self.rows = rows if rows is not None else []
        self.operation_log = operation_log if operation_log is not None else []
        self.fail_mark_published = fail_mark_published
        self.fail_insert_job_attempt = fail_insert_job_attempt
        self.fail_mark_failed = fail_mark_failed
        self.fetch_calls: list[dict] = []
        self.mark_published_calls: list[UUID] = []
        self.mark_failed_calls: list[tuple[UUID, str]] = []
        self.job_attempt_calls: list[dict] = []

    async def fetch_target_events(self, *, event_id, source_message_id, limit):
        self.operation_log.append("fetch")
        self.fetch_calls.append(
            {"event_id": event_id, "source_message_id": source_message_id, "limit": limit}
        )
        return self.rows[:limit]

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        del published_at
        self.operation_log.append("mark_published")
        if self.fail_mark_published:
            raise RuntimeError("sentinel mark published detail")
        self.mark_published_calls.append(event_id)

    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None:
        self.operation_log.append("mark_failed")
        if self.fail_mark_failed:
            raise RuntimeError("sentinel mark failed detail")
        self.mark_failed_calls.append((event_id, error_text))

    async def insert_job_attempt(self, **kwargs) -> None:
        self.operation_log.append("insert_job_attempt")
        if self.fail_insert_job_attempt:
            raise RuntimeError("sentinel job attempt detail")
        self.job_attempt_calls.append(kwargs)


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository, *, close_error: BaseException | None = None) -> None:
        self.repository = repository
        self.close_error = close_error
        self.calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.close_error is not None:
                raise self.close_error

        return BoundedSourceMessageRepositoryHandle(repository=self.repository, close=close)


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

        return BoundedSourceMessageRedisPublisherHandle(publisher=self.publisher, close=close)


def _runtime_config() -> BoundedSourceMessagePublishRuntimeConfig:
    return BoundedSourceMessagePublishRuntimeConfig(database_url=DB_URL, redis_url=REDIS_URL)


def _raising_runtime_config() -> BoundedSourceMessagePublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _row(
    *,
    event_id: UUID | None = None,
    source_message_id: UUID | None = None,
    event_type: str = "source_message.created.v1",
    aggregate_type: str = "source_message",
    status: str = "pending",
    payload_json: dict[str, object] | None = None,
) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id or uuid4(),
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=source_message_id or uuid4(),
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json=payload_json if payload_json is not None else {"source_text": RAW_PAYLOAD_VALUE},
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _approved_config(**overrides) -> BoundedSourceMessageOutboxPublishConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_publish": True,
        "allow_database_write": True,
        "event_id": uuid4(),
        "source_message_id": None,
        "max_events": 1,
    }
    values.update(overrides)
    return BoundedSourceMessageOutboxPublishConfig(**values)


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_redis_or_db_write() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository([_row()]))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_source_message_outbox_publish(
        BoundedSourceMessageOutboxPublishConfig(),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["redis_publish_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["side_effects"]["redis_mutation"] is False
    assert report["side_effects"]["db_write"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_target_fails_closed_before_runtime_config() -> None:
    result = await run_bounded_source_message_outbox_publish(
        BoundedSourceMessageOutboxPublishConfig(operator_approved=True),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.status == "blocked"
    assert result.error_code == "target_missing"
    assert result.state.runtime_config_loaded is False
    assert result.state.redis_publish_attempted is False


@pytest.mark.asyncio
async def test_conflicting_event_id_and_source_message_id_fails_closed() -> None:
    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=uuid4(), source_message_id=uuid4()),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.status == "blocked"
    assert result.error_code == "target_conflict"
    assert result.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_max_events_hard_max_one_fails_closed() -> None:
    result = await run_bounded_source_message_outbox_publish(
        _approved_config(max_events=2),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.error_code == "max_events_must_be_one"
    assert result.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_non_source_event_type_fails_closed_without_publish_or_db_write() -> None:
    row = _row(event_type="notification.plan.created.v1", aggregate_type="analysis")
    repository = FakeRepository([row])
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "source_message_event_contract_mismatch"
    assert report["events_seen"] == 1
    assert report["selected_event_type"] == "notification.plan.created.v1"
    assert report["selected_aggregate_type"] == "analysis"
    assert report["redis_publish_attempted"] is False
    assert report["database_write_attempted"] is False
    assert publisher_builder.calls == 0
    assert repository.mark_published_calls == []
    assert repository.mark_failed_calls == []
    assert repository.job_attempt_calls == []


@pytest.mark.asyncio
async def test_pending_source_message_created_publishes_thin_id_only_payload() -> None:
    operation_log: list[str] = []
    source_message_id = uuid4()
    row = _row(source_message_id=source_message_id)
    repository = FakeRepository([row], operation_log=operation_log)
    publisher = FakeRedisPublisher(operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "published"
    assert report["ok"] is True
    assert report["queue_name"] == "q.source.normalize"
    assert report["stage_name"] == "normalize"
    assert report["target_event_id_suffix"] == str(row.event_id)[-8:]
    assert report["target_source_message_id_suffix"] == str(source_message_id)[-8:]
    assert report["events_seen"] == 1
    assert report["events_published_count"] == 1
    assert report["job_attempts_inserted_count"] == 1
    assert report["redis_publish_attempted"] is True
    assert report["database_write_attempted"] is True
    assert report["event_outbox_marked_published"] is True
    assert report["side_effects"]["redis_mutation"] is True
    assert report["side_effects"]["db_write"] is True
    assert operation_log == ["fetch", "publish", "mark_published", "insert_job_attempt"]
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls == [
        {
            "stage_name": "normalize",
            "queue_name": "q.source.normalize",
            "root_object_type": "source_message",
            "root_object_id": source_message_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]

    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.source.normalize"
    assert route.stage_name == "normalize"
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": str(source_message_id),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    for forbidden in (
        "payload_json",
        "source_text",
        "message_text",
        "chat_id",
        "source_message_id",
        "event_type",
    ):
        assert forbidden not in fields


@pytest.mark.asyncio
async def test_successful_publish_by_source_message_id_uses_exact_target_and_limit_two_probe() -> None:
    source_message_id = uuid4()
    row = _row(source_message_id=source_message_id)
    repository = FakeRepository([row])

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=None, source_message_id=source_message_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.ok is True
    assert repository.fetch_calls == [
        {"event_id": None, "source_message_id": source_message_id, "limit": 2}
    ]


@pytest.mark.asyncio
async def test_successful_publish_marks_event_outbox_published_and_inserts_succeeded_job_attempt() -> None:
    row = _row()
    repository = FakeRepository([row])

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.ok is True
    assert repository.mark_published_calls == [row.event_id]
    assert repository.mark_failed_calls == []
    assert len(repository.job_attempt_calls) == 1
    assert repository.job_attempt_calls[0]["attempt_status"] == "succeeded"
    assert repository.job_attempt_calls[0]["error_code"] is None


@pytest.mark.asyncio
async def test_commit_close_failure_after_successful_publish_returns_sanitized_failure() -> None:
    row = _row()
    repository = FakeRepository([row])
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )
    publisher = FakeRedisPublisher()

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["ok"] is False
    assert report["error_code"] == "repository_commit_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["redis_publish_attempted"] is True
    assert report["events_published_count"] == 1
    assert report["event_outbox_marked_published"] is True
    assert report["job_attempts_inserted_count"] == 1
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls[0]["attempt_status"] == "succeeded"
    assert len(publisher.publish_calls) == 1
    assert CLOSE_EXCEPTION_DETAIL not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered
    assert RAW_PAYLOAD_VALUE not in rendered
    assert RAW_DEDUPE_KEY not in rendered
    assert REDIS_MESSAGE_ID not in rendered


@pytest.mark.asyncio
async def test_rollback_close_failure_after_blocked_path_returns_sanitized_failure() -> None:
    repository = FakeRepository([])
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=ValueError(CLOSE_EXCEPTION_DETAIL),
    )
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=uuid4()),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["ok"] is False
    assert report["error_code"] == "repository_rollback_failed"
    assert report["error_class"] == "ValueError"
    assert report["redis_publish_attempted"] is False
    assert report["events_published_count"] == 0
    assert repository_builder.close_commits == [False]
    assert publisher_builder.calls == 0
    assert CLOSE_EXCEPTION_DETAIL not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered


@pytest.mark.asyncio
async def test_redis_publish_failure_returns_sanitized_json_and_records_retryable_attempt() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository([row], operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log, failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["ok"] is False
    assert report["error_code"] == "redis_publish_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["redis_publish_attempted"] is True
    assert report["events_published_count"] == 0
    assert report["database_write_attempted"] is True
    assert report["event_outbox_marked_failed"] is True
    assert report["job_attempts_inserted_count"] == 1
    assert repository.mark_published_calls == []
    assert repository.mark_failed_calls == [(row.event_id, "redis_publish_failed")]
    assert repository.job_attempt_calls[0]["attempt_status"] == "failed_retryable"
    assert repository.job_attempt_calls[0]["error_code"] == "redis_publish_failed"
    assert repository_builder.close_commits == [True]
    assert operation_log == ["fetch", "publish", "mark_failed", "insert_job_attempt"]
    assert EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_sanitized_output_omits_full_ids_urls_payload_dedupe_redis_id_and_exception_detail() -> None:
    row = _row()
    repository = FakeRepository([row])
    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id),
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
        RAW_PAYLOAD_VALUE,
        RAW_DEDUPE_KEY,
        REDIS_MESSAGE_ID,
    ):
        assert raw not in rendered
    assert rendered.count(str(row.event_id)[-8:]) == 1
    assert rendered.count(str(row.aggregate_id)[-8:]) == 1

    failing = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(
            FakeRedisPublisher(failure=RuntimeError(EXCEPTION_DETAIL))
        ),
    )
    assert EXCEPTION_DETAIL not in json.dumps(failing.to_sanitized_dict(), sort_keys=True)


@pytest.mark.asyncio
async def test_missing_redis_publish_gate_blocks_before_xadd_and_db_write() -> None:
    row = _row()
    repository = FakeRepository([row])
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_source_message_outbox_publish(
        _approved_config(event_id=row.event_id, allow_redis_publish=False),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "redis_publish_not_allowed"
    assert result.state.redis_publish_attempted is False
    assert result.state.database_write_attempted is False
    assert publisher_builder.calls == 0
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []


def test_source_ast_guard_has_no_broad_worker_or_forbidden_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = set()
    imported_roots = set()
    forbidden_call_names = {
        "system",
        "popen",
        "call",
        "check_call",
        "check_output",
        "run_forever",
    }
    forbidden_call_attrs = forbidden_call_names | {
        "sleep",
    }

    assert not any(isinstance(node, ast.While) for node in ast.walk(tree))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_call_attrs
            elif isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_call_names

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(
        imported_roots
    )
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "OutboxRelayService" not in source
