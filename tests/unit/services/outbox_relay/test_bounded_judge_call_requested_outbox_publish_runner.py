from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.outbox_relay.bounded_judge_call_requested_outbox_publish_runner import (
    BoundedJudgeCallRequestedOutboxPublishConfig,
    BoundedJudgeCallRequestedOutboxPublishError,
    BoundedJudgeCallRequestedPublishRuntimeConfig,
    BoundedJudgeCallRequestedRedisPublisherHandle,
    BoundedJudgeCallRequestedRepositoryHandle,
    JudgeRunLocatorRecord,
    REQUIRED_PAYLOAD_FIELDS,
    run_bounded_judge_call_requested_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_judge_call_requested_outbox_publish_runner.py"
DB_LOCATOR = "db_locator_omitted_sentinel"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
RAW_DEDUPE_KEY = "judge-call:sentinel-dedupe-key"
RAW_PROMPT_CACHE_KEY = "judge:text_idea_primary:private-cache-key"
RAW_PROMPT = "sentinel private prompt material"
RAW_BUNDLE_DATA = "sentinel private bundle data"
RAW_TEXT = "sentinel raw source text"
RAW_MODEL_OUTPUT = "sentinel model output"
REDIS_MESSAGE_ID = "1700000000000-0-secret-suffix"
EXCEPTION_DETAIL = "sentinel private xadd failure detail"
CLOSE_EXCEPTION_DETAIL = "sentinel private repository close detail"


class FakeRepository:
    def __init__(
        self,
        rows: list[OutboxEventRow] | None = None,
        *,
        judge_run: JudgeRunLocatorRecord | None = None,
        operation_log: list[str] | None = None,
        fail_mark_published: bool = False,
        fail_insert_job_attempt: bool = False,
    ) -> None:
        self.rows = rows if rows is not None else []
        self.judge_run = judge_run
        self.operation_log = operation_log if operation_log is not None else []
        self.fail_mark_published = fail_mark_published
        self.fail_insert_job_attempt = fail_insert_job_attempt
        self.fetch_calls: list[dict] = []
        self.load_judge_run_calls: list[UUID] = []
        self.mark_published_calls: list[UUID] = []
        self.job_attempt_calls: list[dict] = []

    async def fetch_target_events(self, *, trigger_event_id, trigger_event_suffix, limit):
        self.operation_log.append("fetch")
        self.fetch_calls.append(
            {
                "trigger_event_id": trigger_event_id,
                "trigger_event_suffix": trigger_event_suffix,
                "limit": limit,
            }
        )
        return self.rows[:limit]

    async def load_judge_run(self, judge_run_id: UUID):
        self.operation_log.append("load_judge_run")
        self.load_judge_run_calls.append(judge_run_id)
        return self.judge_run

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

        return BoundedJudgeCallRequestedRepositoryHandle(repository=self.repository, close=close)


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

        return BoundedJudgeCallRequestedRedisPublisherHandle(publisher=self.publisher, close=close)


class RogueRouteResolver:
    def resolve(self, row):
        del row
        return QueueRoute("q.notification.send", "notify")


def _runtime_config() -> BoundedJudgeCallRequestedPublishRuntimeConfig:
    return BoundedJudgeCallRequestedPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _missing_runtime_config() -> BoundedJudgeCallRequestedPublishRuntimeConfig:
    raise BoundedJudgeCallRequestedOutboxPublishError("database_url_missing")


def _raising_runtime_config() -> BoundedJudgeCallRequestedPublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _payload(
    judge_run_id: UUID,
    *,
    bundle_id: UUID | None = None,
) -> dict[str, object]:
    return {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id or uuid4()),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_text_idea_primary_v1",
        "prompt_cache_key": RAW_PROMPT_CACHE_KEY,
        "prompt_material": RAW_PROMPT,
        "bundle_data": RAW_BUNDLE_DATA,
        "raw_text": RAW_TEXT,
        "model_output": RAW_MODEL_OUTPUT,
        "database_url": DB_LOCATOR,
        "redis_url": REDIS_LOCATOR,
    }


def _row(
    *,
    event_id: UUID | None = None,
    judge_run_id: UUID | None = None,
    bundle_id: UUID | None = None,
    event_type: str = "judge.call.requested.v1",
    aggregate_type: str = "judge_run",
    status: str = "pending",
    payload_json: dict[str, object] | None = None,
) -> OutboxEventRow:
    aggregate_id = judge_run_id or uuid4()
    return OutboxEventRow(
        event_id=event_id or uuid4(),
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json=payload_json if payload_json is not None else _payload(aggregate_id, bundle_id=bundle_id),
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _judge_run(judge_run_id: UUID, bundle_id: UUID, *, status: str = "pending") -> JudgeRunLocatorRecord:
    return JudgeRunLocatorRecord(judge_run_id=judge_run_id, bundle_id=bundle_id, status=status)


def _approved_config(**overrides) -> BoundedJudgeCallRequestedOutboxPublishConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "allow_redis_publish": True,
        "allow_database_write": True,
        "trigger_event_id": uuid4(),
        "trigger_event_suffix": None,
        "max_events": 1,
    }
    values.update(overrides)
    return BoundedJudgeCallRequestedOutboxPublishConfig(**values)


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_db_or_redis() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository([_row()]))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_judge_call_requested_outbox_publish(
        BoundedJudgeCallRequestedOutboxPublishConfig(),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "operator_approval_missing"
    assert report["database_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_authority_gate_failures_happen_before_runtime_config() -> None:
    configs = [
        BoundedJudgeCallRequestedOutboxPublishConfig(operator_approved=True),
        _approved_config(trigger_event_id=uuid4(), trigger_event_suffix="a1c22bcb"),
        _approved_config(trigger_event_suffix="not-hex", trigger_event_id=None),
        _approved_config(max_events=2),
        _approved_config(allow_runtime_config=False),
        _approved_config(allow_database_read=False),
        _approved_config(allow_redis_publish=False),
        _approved_config(allow_database_write=False),
    ]

    results = [
        await run_bounded_judge_call_requested_outbox_publish(
            config,
            runtime_config_loader=_raising_runtime_config,
            repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
            redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
        )
        for config in configs
    ]

    assert [result.error_code for result in results] == [
        "target_missing",
        "target_conflict",
        "invalid_trigger_event_suffix",
        "max_events_must_be_one",
        "runtime_config_not_allowed",
        "database_read_not_allowed",
        "redis_publish_not_allowed",
        "database_write_not_allowed",
    ]
    assert all(result.state.runtime_config_loaded is False for result in results)
    assert all(result.state.database_read_attempted is False for result in results)
    assert all(result.state.redis_publish_attempted is False for result in results)


@pytest.mark.asyncio
async def test_missing_runtime_config_blocks_before_database_or_redis() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository([_row()]))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(),
        runtime_config_loader=_missing_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )

    assert result.status == "blocked"
    assert result.error_code == "database_url_missing"
    assert result.state.database_read_attempted is False
    assert result.state.database_write_attempted is False
    assert result.state.redis_publish_attempted is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_target_missing_and_suffix_conflict_block_before_publish() -> None:
    missing_repository = FakeRepository([])
    conflict_repository = FakeRepository([_row(), _row()])
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    missing = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=None, trigger_event_suffix="a1c22bcb"),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(missing_repository),
        redis_publisher_builder=publisher_builder,
    )
    conflict = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=None, trigger_event_suffix="a1c22bcb"),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(conflict_repository),
        redis_publisher_builder=publisher_builder,
    )

    assert missing.error_code == "target_event_not_found"
    assert conflict.error_code == "target_event_count_exceeded"
    assert conflict.events_seen == 2
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_suffix_selector_uses_database_uniqueness_probe() -> None:
    bundle_id = uuid4()
    row = _row(bundle_id=bundle_id)
    repository = FakeRepository([row], judge_run=_judge_run(row.aggregate_id, bundle_id))

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=None, trigger_event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.ok is True
    assert result.selector_type == "trigger_event_suffix"
    assert repository.fetch_calls == [
        {"trigger_event_id": None, "trigger_event_suffix": str(row.event_id)[-8:], "limit": 2}
    ]


@pytest.mark.asyncio
async def test_wrong_event_status_or_aggregate_blocks_without_publish() -> None:
    wrong_event = _row(event_type="analysis.requested.v1")
    wrong_aggregate = _row(aggregate_type="candidate_group")
    blocked_status = _row(status="failed")
    already_published = _row(status="published")

    first = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=wrong_event.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([wrong_event])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    second = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=wrong_aggregate.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([wrong_aggregate])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    third = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=blocked_status.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([blocked_status])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    fourth = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=already_published.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([already_published])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert first.error_code == "wrong_event_type"
    assert second.error_code == "wrong_aggregate_type"
    assert third.error_code == "target_event_not_pending"
    assert fourth.status == "already_published"
    assert fourth.ok is True
    assert fourth.published_count == 0
    assert first.state.redis_publish_attempted is False
    assert second.state.redis_publish_attempted is False
    assert third.state.redis_publish_attempted is False
    assert fourth.state.redis_publish_attempted is False


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_field", REQUIRED_PAYLOAD_FIELDS)
async def test_required_payload_fields_missing_blocks_before_publish(missing_field: str) -> None:
    judge_run_id = uuid4()
    payload = _payload(judge_run_id)
    raw_value = str(payload.pop(missing_field))

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=uuid4()),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(
            FakeRepository([_row(judge_run_id=judge_run_id, payload_json=payload)])
        ),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "malformed_event_payload"
    assert getattr(result, f"payload_has_{missing_field}") is False
    assert result.state.redis_publish_attempted is False
    assert result.state.database_write_attempted is False
    if missing_field == "prompt_cache_key":
        assert raw_value not in rendered


@pytest.mark.asyncio
async def test_payload_judge_run_mismatch_blocks_before_publish() -> None:
    row = _row(payload_json=_payload(uuid4()))

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.error_code == "judge_run_id_mismatch"
    assert result.payload_judge_run_id_matches_aggregate is False
    assert result.state.redis_publish_attempted is False


@pytest.mark.asyncio
async def test_judge_run_missing_status_or_bundle_mismatch_blocks_before_publish() -> None:
    bundle_id = uuid4()
    row = _row(bundle_id=bundle_id)
    missing = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row], judge_run=None)),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    running = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(
            FakeRepository([row], judge_run=_judge_run(row.aggregate_id, bundle_id, status="running"))
        ),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    mismatch = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(
            FakeRepository([row], judge_run=_judge_run(row.aggregate_id, uuid4()))
        ),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert missing.error_code == "judge_run_missing"
    assert running.error_code == "judge_run_not_pending"
    assert mismatch.error_code == "judge_run_bundle_mismatch"
    assert missing.state.redis_publish_attempted is False
    assert running.state.redis_publish_attempted is False
    assert mismatch.state.redis_publish_attempted is False


@pytest.mark.asyncio
async def test_publish_uses_existing_route_and_thin_id_only_payload() -> None:
    operation_log: list[str] = []
    bundle_id = uuid4()
    row = _row(bundle_id=bundle_id)
    repository = FakeRepository(
        [row],
        judge_run=_judge_run(row.aggregate_id, bundle_id),
        operation_log=operation_log,
    )
    publisher = FakeRedisPublisher(operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "published"
    assert report["ok"] is True
    assert report["queue_name"] == "q.analysis.judge"
    assert report["stage_name"] == "judge"
    assert report["target_trigger_event_id_suffix"] == str(row.event_id)[-8:]
    assert report["target_judge_run_id_suffix"] == str(row.aggregate_id)[-8:]
    assert report["target_bundle_id_suffix"] == str(bundle_id)[-8:]
    assert report["redis_message_id_suffix"] == REDIS_MESSAGE_ID[-8:]
    assert report["published_count"] == 1
    assert report["event_outbox_status_updated"] is True
    assert report["job_attempts_written_count"] == 1
    assert operation_log == ["fetch", "load_judge_run", "publish", "mark_published", "insert_job_attempt"]
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls == [
        {
            "stage_name": "judge",
            "queue_name": "q.analysis.judge",
            "root_object_type": "judge_run",
            "root_object_id": row.aggregate_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]

    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.analysis.judge"
    assert route.stage_name == "judge"
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "judge",
        "root_object_type": "judge_run",
        "root_object_id": str(row.aggregate_id),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    for forbidden in (
        "payload_json",
        "bundle_id",
        "model",
        "reasoning_effort",
        "prompt_version",
        "prompt_cache_key",
        "prompt_material",
        "bundle_data",
        "raw_text",
        "model_output",
    ):
        assert forbidden not in fields


@pytest.mark.asyncio
async def test_custom_route_drift_blocks_before_publish() -> None:
    bundle_id = uuid4()
    row = _row(bundle_id=bundle_id)
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row], judge_run=_judge_run(row.aggregate_id, bundle_id))),
        redis_publisher_builder=publisher_builder,
        route_resolver=RogueRouteResolver(),  # type: ignore[arg-type]
    )

    assert result.error_code == "route_not_allowed"
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_redis_publish_failure_does_not_mark_event_published() -> None:
    operation_log: list[str] = []
    bundle_id = uuid4()
    row = _row(bundle_id=bundle_id)
    repository = FakeRepository(
        [row],
        judge_run=_judge_run(row.aggregate_id, bundle_id),
        operation_log=operation_log,
    )
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log, failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "redis_xadd_failed"
    assert result.error_class == "RuntimeError"
    assert result.published_count == 0
    assert result.event_outbox_status_updated is False
    assert result.state.database_write_attempted is False
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []
    assert repository_builder.close_commits == [False]
    assert operation_log == ["fetch", "load_judge_run", "publish"]
    assert EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_database_write_failure_after_xadd_reports_partial_failure() -> None:
    operation_log: list[str] = []
    bundle_id = uuid4()
    row = _row(bundle_id=bundle_id)
    repository = FakeRepository(
        [row],
        judge_run=_judge_run(row.aggregate_id, bundle_id),
        operation_log=operation_log,
        fail_insert_job_attempt=True,
    )
    publisher = FakeRedisPublisher(operation_log=operation_log)

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "database_write_failed_after_redis_publish"
    assert result.error_class == "RuntimeError"
    assert result.published_count == 1
    assert result.event_outbox_status_updated is True
    assert result.job_attempts_written_count == 0
    assert result.state.database_write_attempted is True
    assert operation_log == ["fetch", "load_judge_run", "publish", "mark_published", "insert_job_attempt"]
    assert "sentinel job attempt detail" not in rendered


@pytest.mark.asyncio
async def test_commit_close_failure_after_xadd_reports_sanitized_database_commit_failure() -> None:
    bundle_id = uuid4()
    row = _row(bundle_id=bundle_id)
    repository = FakeRepository([row], judge_run=_judge_run(row.aggregate_id, bundle_id))
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )

    result = await run_bounded_judge_call_requested_outbox_publish(
        _approved_config(trigger_event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["error_code"] == "database_commit_failed_after_redis_publish"
    assert report["error_class"] == "RuntimeError"
    assert report["published_count"] == 1
    assert report["event_outbox_status_updated"] is True
    assert repository_builder.close_commits == [True]
    assert CLOSE_EXCEPTION_DETAIL not in rendered


def test_source_ast_guard_has_no_forbidden_external_imports_or_worker_calls() -> None:
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
        "xreadgroup",
        "xread",
        "consume",
        "docker",
        "alembic",
        "systemctl",
    }

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
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".analysis_validator" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".router_normalizer" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever(" not in source
