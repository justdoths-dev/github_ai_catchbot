from __future__ import annotations

import ast
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

import tools.bounded_analysis_requested_outbox_publish_runner as cli
from src.services.outbox_relay.bounded_analysis_requested_outbox_publish_runner import (
    EVENT_TYPE,
    MODE_PREVIEW,
    MODE_PUBLISH,
    REQUIRED_PAYLOAD_FIELDS,
    BoundedAnalysisRequestedOutboxPublishConfig,
    BoundedAnalysisRequestedOutboxPublishError,
    BoundedAnalysisRequestedPublishRuntimeConfig,
    BoundedAnalysisRequestedRedisInspectorHandle,
    BoundedAnalysisRequestedRedisPublisherHandle,
    BoundedAnalysisRequestedRepositoryHandle,
    run_bounded_analysis_requested_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_analysis_requested_outbox_publish_runner.py"
TOOL_PATH = ROOT / "tools/bounded_analysis_requested_outbox_publish_runner.py"
DB_LOCATOR = "db_locator_omitted_sentinel"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
RAW_DEDUPE_KEY = "analysis:requested:sentinel-dedupe-key"
RAW_BUNDLE_DATA = "sentinel private bundle data"
RAW_TEXT = "sentinel raw source text"
RAW_EVIDENCE = "sentinel raw evidence body"
RAW_MESSAGE_TEXT = "sentinel message_text body"
RAW_PROFILE = "github_primary"
REDIS_MESSAGE_ID = "secret-analysis-route-redis-message-id"
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
    ) -> None:
        self.rows = rows if rows is not None else []
        self.operation_log = operation_log if operation_log is not None else []
        self.fail_mark_published = fail_mark_published
        self.fail_insert_job_attempt = fail_insert_job_attempt
        self.fetch_calls: list[dict] = []
        self.mark_published_calls: list[UUID] = []
        self.job_attempt_calls: list[dict] = []

    async def fetch_target_events(self, *, event_type, event_suffix, aggregate_suffix, limit):
        self.operation_log.append("fetch")
        self.fetch_calls.append(
            {
                "event_type": event_type,
                "event_suffix": event_suffix,
                "aggregate_suffix": aggregate_suffix,
                "limit": limit,
            }
        )
        return self.rows[:limit]

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

        return BoundedAnalysisRequestedRepositoryHandle(repository=self.repository, close=close)


class FakeRedisInspector:
    def __init__(
        self,
        *,
        operation_log: list[str] | None = None,
        stream_type: str = "stream",
        failure: BaseException | None = None,
    ) -> None:
        self.operation_log = operation_log if operation_log is not None else []
        self.stream_type = stream_type
        self.failure = failure
        self.calls: list[str] = []

    async def get_stream_type(self, queue_name: str) -> str:
        self.operation_log.append("redis_type")
        self.calls.append(queue_name)
        if self.failure is not None:
            raise self.failure
        return self.stream_type


class FakeRedisInspectorBuilder:
    def __init__(self, inspector: FakeRedisInspector) -> None:
        self.inspector = inspector
        self.calls = 0
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_reader_created = True

        async def close() -> None:
            self.close_calls += 1

        return BoundedAnalysisRequestedRedisInspectorHandle(inspector=self.inspector, close=close)


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

        return BoundedAnalysisRequestedRedisPublisherHandle(publisher=self.publisher, close=close)


class RogueRouteResolver:
    def resolve(self, row):
        del row
        return QueueRoute("q.notification.send", "notify")


def _runtime_config() -> BoundedAnalysisRequestedPublishRuntimeConfig:
    return BoundedAnalysisRequestedPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _missing_runtime_config() -> BoundedAnalysisRequestedPublishRuntimeConfig:
    raise BoundedAnalysisRequestedOutboxPublishError("database_url_missing")


def _raising_runtime_config() -> BoundedAnalysisRequestedPublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _payload(
    candidate_group_id: UUID,
    *,
    bundle_id: UUID | None = None,
    judge_profile: str = RAW_PROFILE,
    escalation_allowed: bool = False,
) -> dict[str, object]:
    return {
        "candidate_group_id": str(candidate_group_id),
        "bundle_id": str(bundle_id or uuid4()),
        "judge_profile": judge_profile,
        "escalation_allowed": escalation_allowed,
        "bundle_data": RAW_BUNDLE_DATA,
        "raw_text": RAW_TEXT,
        "raw_evidence": RAW_EVIDENCE,
        "message_text": RAW_MESSAGE_TEXT,
        "database_url": DB_LOCATOR,
        "redis_url": REDIS_LOCATOR,
    }


def _row(
    *,
    event_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
    bundle_id: UUID | None = None,
    event_type: str = EVENT_TYPE,
    aggregate_type: str = "candidate_group",
    status: str = "pending",
    payload_json: dict[str, object] | None = None,
) -> OutboxEventRow:
    aggregate_id = candidate_group_id or uuid4()
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


def _preview_config(**overrides) -> BoundedAnalysisRequestedOutboxPublishConfig:
    values = {
        "mode": MODE_PREVIEW,
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "allow_redis_read": True,
        "allow_redis_publish": False,
        "allow_outbox_status_update": False,
        "event_type": EVENT_TYPE,
        "event_suffix": "abcd1234",
        "aggregate_suffix": None,
        "max_events": 1,
    }
    values.update(overrides)
    return BoundedAnalysisRequestedOutboxPublishConfig(**values)


def _publish_config(**overrides) -> BoundedAnalysisRequestedOutboxPublishConfig:
    values = {
        **asdict(_preview_config()),
        "mode": MODE_PUBLISH,
        "allow_redis_publish": True,
        "allow_outbox_status_update": True,
    }
    values.update(overrides)
    return BoundedAnalysisRequestedOutboxPublishConfig(**values)


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_db_or_redis() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository([_row()]))
    inspector_builder = FakeRedisInspectorBuilder(FakeRedisInspector())
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_analysis_requested_outbox_publish(
        BoundedAnalysisRequestedOutboxPublishConfig(),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        redis_inspector_builder=inspector_builder,
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["database_read_attempted"] is False
    assert report["redis_read_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["database_write_attempted"] is False
    assert repository_builder.calls == 0
    assert inspector_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_gate_failures_happen_before_runtime_config() -> None:
    base = _preview_config()
    cases = [
        (dict(event_type=None), "event_type_required"),
        (dict(event_type="judge.call.requested.v1"), "event_type_not_allowed"),
        (dict(event_suffix=None), "target_event_suffix_missing"),
        (dict(event_suffix=str(uuid4())), "raw_event_id_not_allowed"),
        (dict(event_suffix="not-hex"), "invalid_event_suffix"),
        (dict(aggregate_suffix=str(uuid4())), "raw_aggregate_id_not_allowed"),
        (dict(max_events=2), "max_events_must_be_one"),
        (dict(allow_runtime_config=False), "runtime_config_not_allowed"),
        (dict(allow_database_read=False), "database_read_not_allowed"),
        (dict(allow_redis_read=False), "redis_read_not_allowed"),
        (dict(mode=MODE_PUBLISH, allow_redis_publish=False), "redis_publish_not_allowed"),
        (dict(mode=MODE_PUBLISH, allow_redis_publish=True, allow_outbox_status_update=False), "outbox_status_update_not_allowed"),
    ]

    for overrides, expected in cases:
        result = await run_bounded_analysis_requested_outbox_publish(
            BoundedAnalysisRequestedOutboxPublishConfig(**{**asdict(base), **overrides}),
            runtime_config_loader=_raising_runtime_config,
            repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
            redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
            redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
        )
        assert result.error_code == expected
        assert result.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_preview_exact_analysis_requested_target_has_no_db_write_or_redis_publish() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository([row], operation_log=operation_log)
    inspector = FakeRedisInspector(operation_log=operation_log, stream_type="stream")
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher(operation_log=operation_log))
    repository_builder = FakeRepositoryBuilder(repository)

    result = await run_bounded_analysis_requested_outbox_publish(
        _preview_config(event_suffix=str(row.event_id)[-8:], aggregate_suffix=str(row.aggregate_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_inspector_builder=FakeRedisInspectorBuilder(inspector),
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "preview"
    assert report["ok"] is True
    assert report["target_event_suffix"] == str(row.event_id)[-8:]
    assert report["aggregate_suffix"] == str(row.aggregate_id)[-8:]
    assert report["root_object_type"] == "candidate_group"
    assert report["root_object_id_suffix"] == str(row.aggregate_id)[-8:]
    assert report["queue_name"] == "q.analysis.route"
    assert report["stage_name"] == "analysis_route"
    assert report["event_outbox_status_before"] == "pending"
    assert report["redis_publish_would_occur"] is True
    assert report["redis_publish_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["side_effects"]["redis_mutation"] is False
    assert report["side_effects"]["db_write"] is False
    assert report["duplicate_handling_status"] == "pending_would_publish"
    assert repository.fetch_calls == [
        {
            "event_type": EVENT_TYPE,
            "event_suffix": str(row.event_id)[-8:],
            "aggregate_suffix": str(row.aggregate_id)[-8:],
            "limit": 2,
        }
    ]
    assert operation_log == ["fetch", "redis_type"]
    assert repository_builder.close_commits == [False]
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_publish_exact_target_emits_thin_q_analysis_route_message_through_relay_boundary() -> None:
    operation_log: list[str] = []
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    row = _row(candidate_group_id=candidate_group_id, bundle_id=bundle_id)
    repository = FakeRepository([row], operation_log=operation_log)
    publisher = FakeRedisPublisher(operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)

    result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector(operation_log=operation_log)),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "published"
    assert report["ok"] is True
    assert report["queue_name"] == "q.analysis.route"
    assert report["stage_name"] == "analysis_route"
    assert report["target_event_suffix"] == str(row.event_id)[-8:]
    assert report["aggregate_suffix"] == str(candidate_group_id)[-8:]
    assert report["bundle_id_suffix"] == str(bundle_id)[-8:]
    assert report["redis_published_count"] == 1
    assert report["event_outbox_status_updated_count"] == 1
    assert report["job_attempts_written_count"] == 1
    assert operation_log == ["fetch", "redis_type", "publish", "mark_published", "insert_job_attempt"]
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls == [
        {
            "stage_name": "analysis_route",
            "queue_name": "q.analysis.route",
            "root_object_type": "candidate_group",
            "root_object_id": candidate_group_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]

    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.analysis.route"
    assert route.stage_name == "analysis_route"
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "analysis_route",
        "root_object_type": "candidate_group",
        "root_object_id": str(candidate_group_id),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    for forbidden in (
        "payload_json",
        "candidate_group_id",
        "bundle_id",
        "judge_profile",
        "escalation_allowed",
        "bundle_data",
        "raw_text",
        "raw_evidence",
        "message_text",
        "database_url",
        "redis_url",
    ):
        assert forbidden not in fields


@pytest.mark.asyncio
async def test_already_published_target_is_deterministic_noop() -> None:
    row = _row(status="published")
    inspector_builder = FakeRedisInspectorBuilder(FakeRedisInspector())
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_inspector_builder=inspector_builder,
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "already_published"
    assert report["ok"] is True
    assert report["duplicate_handling_status"] == "already_published_noop"
    assert report["redis_publish_would_occur"] is False
    assert report["redis_publish_attempted"] is False
    assert report["redis_published_count"] == 0
    assert report["database_write_attempted"] is False
    assert inspector_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_wrong_event_type_aggregate_status_ambiguous_suffix_and_route_drift_block_before_publish() -> None:
    wrong_event = _row(event_type="judge.call.requested.v1")
    wrong_aggregate = _row(aggregate_type="artifact")
    failed_status = _row(status="failed")
    row_one = _row()
    row_two = _row()
    route_drift = _row()

    wrong_event_result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(wrong_event.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([wrong_event])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    wrong_aggregate_result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(wrong_aggregate.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([wrong_aggregate])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    failed_status_result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(failed_status.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([failed_status])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    ambiguous_result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(row_one.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row_one, row_two])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    drift_result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(route_drift.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([route_drift])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
        route_resolver=RogueRouteResolver(),  # type: ignore[arg-type]
    )

    assert wrong_event_result.error_code == "wrong_event_type"
    assert wrong_aggregate_result.error_code == "wrong_aggregate_type"
    assert failed_status_result.error_code == "target_event_not_pending"
    assert ambiguous_result.error_code == "target_event_count_exceeded"
    assert drift_result.error_code == "route_not_allowed"
    for result in (wrong_event_result, wrong_aggregate_result, failed_status_result, ambiguous_result, drift_result):
        assert result.state.redis_publish_attempted is False
        assert result.state.database_write_attempted is False


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_field", REQUIRED_PAYLOAD_FIELDS)
async def test_required_payload_fields_missing_blocks_before_publish_and_omits_raw_values(missing_field: str) -> None:
    candidate_group_id = uuid4()
    payload = _payload(candidate_group_id)
    raw_value = str(payload.pop(missing_field))

    result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(
            FakeRepository([_row(candidate_group_id=candidate_group_id, payload_json=payload)])
        ),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "malformed_event_payload"
    assert getattr(result, f"payload_has_{missing_field}") is False
    assert result.state.redis_publish_attempted is False
    assert result.state.database_write_attempted is False
    assert raw_value not in rendered


@pytest.mark.asyncio
async def test_payload_mismatch_invalid_redis_stream_and_redis_read_failure_block_before_publish() -> None:
    mismatched = _row(payload_json=_payload(uuid4()))
    stream_target = _row()
    read_failure_target = _row()

    mismatch = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(mismatched.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([mismatched])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    invalid_stream = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(stream_target.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([stream_target])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector(stream_type="list")),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    read_failure = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(read_failure_target.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([read_failure_target])),
        redis_inspector_builder=FakeRedisInspectorBuilder(
            FakeRedisInspector(failure=RuntimeError(EXCEPTION_DETAIL))
        ),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert mismatch.error_code == "candidate_group_id_mismatch"
    assert invalid_stream.error_code == "redis_stream_type_invalid"
    assert invalid_stream.redis_stream_type == "list"
    assert read_failure.error_code == "redis_type_read_failed"
    assert EXCEPTION_DETAIL not in json.dumps(read_failure.to_sanitized_dict(), sort_keys=True)
    for result in (mismatch, invalid_stream, read_failure):
        assert result.state.redis_publish_attempted is False
        assert result.state.database_write_attempted is False


@pytest.mark.asyncio
async def test_redis_publish_failure_does_not_mark_published_or_insert_job_attempt() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository([row], operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log, failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector(operation_log=operation_log)),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "redis_xadd_failed"
    assert result.error_class == "RuntimeError"
    assert result.state.redis_publish_attempted is True
    assert result.redis_published_count == 0
    assert result.state.database_write_attempted is False
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []
    assert repository_builder.close_commits == [False]
    assert operation_log == ["fetch", "redis_type", "publish"]
    assert EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_database_status_update_failure_after_xadd_does_not_claim_publish_success() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository([row], operation_log=operation_log, fail_mark_published=True)
    publisher = FakeRedisPublisher(operation_log=operation_log)

    result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector(operation_log=operation_log)),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.ok is False
    assert result.error_code == "database_write_failed_after_redis_publish"
    assert result.error_class == "RuntimeError"
    assert result.redis_published_count == 1
    assert result.event_outbox_status_updated_count == 0
    assert result.job_attempts_written_count == 0
    assert result.state.database_write_attempted is True
    assert operation_log == ["fetch", "redis_type", "publish", "mark_published"]
    assert "sentinel mark published detail" not in rendered


@pytest.mark.asyncio
async def test_commit_close_failure_after_xadd_returns_sanitized_failure() -> None:
    row = _row()
    repository = FakeRepository([row])
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )
    publisher = FakeRedisPublisher()

    result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["ok"] is False
    assert report["error_code"] == "repository_commit_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["redis_publish_attempted"] is True
    assert report["redis_published_count"] == 1
    assert report["event_outbox_status_updated_count"] == 1
    assert report["job_attempts_written_count"] == 1
    assert repository_builder.close_commits == [True]
    assert CLOSE_EXCEPTION_DETAIL not in rendered
    assert DB_LOCATOR not in rendered
    assert REDIS_LOCATOR not in rendered
    assert RAW_DEDUPE_KEY not in rendered
    assert REDIS_MESSAGE_ID not in rendered


@pytest.mark.asyncio
async def test_sanitized_output_omits_full_ids_payload_dedupe_and_exception_detail() -> None:
    row = _row()
    bundle_id = UUID(str(row.payload_json["bundle_id"]))
    result = await run_bounded_analysis_requested_outbox_publish(
        _publish_config(event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_inspector_builder=FakeRedisInspectorBuilder(FakeRedisInspector()),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        str(bundle_id),
        DB_LOCATOR,
        REDIS_LOCATOR,
        RAW_BUNDLE_DATA,
        RAW_TEXT,
        RAW_EVIDENCE,
        RAW_MESSAGE_TEXT,
        RAW_PROFILE,
        RAW_DEDUPE_KEY,
        REDIS_MESSAGE_ID,
    ):
        assert raw not in rendered
    assert rendered.count(str(row.event_id)[-8:]) == 1
    assert rendered.count(str(row.aggregate_id)[-8:]) >= 1


def test_cli_rejects_unsupported_live_authority_flags_and_raw_full_ids(capsys: pytest.CaptureFixture[str]) -> None:
    unsupported_exit = cli.main(["--allow-openai"])
    unsupported_output = json.loads(capsys.readouterr().out)
    raw_id_exit = cli.main(
        [
            "--mode",
            "preview",
            "--operator-approved",
            "--event-type",
            EVENT_TYPE,
            "--event-suffix",
            str(uuid4()),
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-redis-read",
        ],
        runtime_config_loader=_raising_runtime_config,
    )
    raw_id_output = json.loads(capsys.readouterr().out)

    assert unsupported_exit == 1
    assert unsupported_output["status"] == "blocked"
    assert unsupported_output["error_code"] == "unsupported_cli_argument"
    assert raw_id_exit == 1
    assert raw_id_output["error_code"] == "raw_event_id_not_allowed"
    rendered = json.dumps(unsupported_output, sort_keys=True) + json.dumps(raw_id_output, sort_keys=True)
    assert "--allow-openai" not in rendered
    assert DB_LOCATOR not in rendered
    assert REDIS_LOCATOR not in rendered


def test_source_ast_guard_has_no_downstream_calls_runtime_locators_or_ad_hoc_redis_ops() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tool_source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    tool_tree = ast.parse(tool_source)
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
        "xack",
        "xgroup_create",
        "xgroup_destroy",
        "consume",
        "create_subprocess_exec",
        "create_subprocess_shell",
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

    for node in ast.walk(tool_tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_call_attrs
            elif isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_call_names

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(
        imported_roots
    )
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".analysis_router" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "OutboxRelayService" not in source
    assert "RedisStreamConsumer" not in source
    assert "run_forever(" not in source
    assert "runtime.env" not in source
    assert "runtime.env" not in tool_source
    assert ".xadd(" not in source
    assert ".xread" not in source
    assert ".xack" not in source
