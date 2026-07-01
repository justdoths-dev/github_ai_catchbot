from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.outbox_relay.bounded_candidate_bundle_refresh_outbox_publish_runner import (
    BoundedCandidateBundleRefreshOutboxPublishConfig,
    BoundedCandidateBundleRefreshPublishRuntimeConfig,
    BoundedCandidateBundleRefreshRedisPublisherHandle,
    BoundedCandidateBundleRefreshRepositoryHandle,
    EVENT_TYPE,
    ROOT_OBJECT_TYPE,
    SqlAlchemyBoundedCandidateBundleRefreshOutboxRepository,
    run_bounded_candidate_bundle_refresh_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_candidate_bundle_refresh_outbox_publish_runner.py"
DB_LOCATOR = "db_locator_omitted_sentinel"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
RAW_DEDUPE_KEY = "bundle-refresh:sentinel-private-dedupe-key"
RAW_REFRESH_REASON = "sentinel private refresh reason"
RAW_TRIGGER_KIND = "analysis_router_recheck"
RAW_SOURCE_TEXT = "sentinel private source text"
RAW_URL = "https://example.invalid/private-refresh-url"
REDIS_MESSAGE_ID = "secret-candidate-refresh-redis-message-id"
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

    async def fetch_target_events(self, *, event_id, candidate_group_id, event_suffix, limit):
        self.operation_log.append("fetch")
        self.fetch_calls.append(
            {
                "event_id": event_id,
                "candidate_group_id": candidate_group_id,
                "event_suffix": event_suffix,
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

        return BoundedCandidateBundleRefreshRepositoryHandle(repository=self.repository, close=close)


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

        return BoundedCandidateBundleRefreshRedisPublisherHandle(publisher=self.publisher, close=close)


class FakeSqlResult:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows if rows is not None else []

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class FakeSqlSession:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows if rows is not None else []
        self.execute_calls: list[dict[str, object]] = []

    async def execute(self, statement, params=None):
        self.execute_calls.append(
            {
                "statement": str(statement),
                "params": dict(params or {}),
            }
        )
        return FakeSqlResult(self.rows)


class RogueRouteResolver:
    def resolve(self, row):
        del row
        return QueueRoute("q.notification.send", "notify")


def _runtime_config() -> BoundedCandidateBundleRefreshPublishRuntimeConfig:
    return BoundedCandidateBundleRefreshPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _raising_runtime_config() -> BoundedCandidateBundleRefreshPublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _payload(
    candidate_group_id: UUID,
    *,
    trigger_object_id: UUID | None = None,
    bundle_id: UUID | None = None,
) -> dict[str, object]:
    return {
        "candidate_group_id": str(candidate_group_id),
        "trigger_kind": RAW_TRIGGER_KIND,
        "trigger_object_type": "bundle",
        "trigger_object_id": str(trigger_object_id or bundle_id or uuid4()),
        "refresh_reason": RAW_REFRESH_REASON,
        "source_version_no": 7,
        "bundle_id": str(bundle_id or uuid4()),
        "source_text": RAW_SOURCE_TEXT,
        "url": RAW_URL,
    }


def _row(
    *,
    event_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
    trigger_object_id: UUID | None = None,
    bundle_id: UUID | None = None,
    event_type: str = "candidate.bundle.refresh.v1",
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
        payload_json=payload_json
        if payload_json is not None
        else _payload(aggregate_id, trigger_object_id=trigger_object_id, bundle_id=bundle_id),
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _approved_config(**overrides) -> BoundedCandidateBundleRefreshOutboxPublishConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_publish": True,
        "allow_database_write": True,
        "event_id": uuid4(),
        "candidate_group_id": None,
        "event_suffix": None,
        "max_events": 1,
    }
    values.update(overrides)
    return BoundedCandidateBundleRefreshOutboxPublishConfig(**values)


def _normalized_sql(statement: object) -> str:
    return " ".join(str(statement).split())


@pytest.mark.asyncio
async def test_sql_candidate_group_selector_filters_pending_before_uniqueness_probe() -> None:
    candidate_group_id = uuid4()
    session = FakeSqlSession()
    repository = SqlAlchemyBoundedCandidateBundleRefreshOutboxRepository(session)  # type: ignore[arg-type]

    rows = await repository.fetch_target_events(
        event_id=None,
        candidate_group_id=candidate_group_id,
        event_suffix=None,
        limit=2,
    )

    assert rows == []
    assert len(session.execute_calls) == 1
    call = session.execute_calls[0]
    sql = _normalized_sql(call["statement"])
    assert "event_type = :event_type" in sql
    assert "aggregate_type = :aggregate_type" in sql
    assert "aggregate_id = CAST(:candidate_group_id AS uuid)" in sql
    assert "status = 'pending'" in sql
    assert "ORDER BY created_at ASC, event_id ASC LIMIT :limit" in sql
    assert call["params"] == {
        "event_type": EVENT_TYPE,
        "aggregate_type": ROOT_OBJECT_TYPE,
        "candidate_group_id": str(candidate_group_id),
        "limit": 2,
    }


@pytest.mark.asyncio
async def test_sql_event_suffix_selector_filters_pending_before_uniqueness_probe() -> None:
    session = FakeSqlSession()
    repository = SqlAlchemyBoundedCandidateBundleRefreshOutboxRepository(session)  # type: ignore[arg-type]

    rows = await repository.fetch_target_events(
        event_id=None,
        candidate_group_id=None,
        event_suffix="AbCd-1234",
        limit=2,
    )

    assert rows == []
    assert len(session.execute_calls) == 1
    call = session.execute_calls[0]
    sql = _normalized_sql(call["statement"])
    assert "lower(CAST(event_id AS text)) LIKE :event_suffix_pattern" in sql
    assert "status = 'pending'" in sql
    assert "ORDER BY created_at ASC, event_id ASC LIMIT :limit" in sql
    assert call["params"] == {
        "event_suffix_pattern": "%abcd-1234",
        "limit": 2,
    }


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_redis_or_db_write() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository([_row()]))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        BoundedCandidateBundleRefreshOutboxPublishConfig(),
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
async def test_target_gate_and_authority_gate_failures_happen_before_runtime_config() -> None:
    cases = [
        await run_bounded_candidate_bundle_refresh_outbox_publish(
            BoundedCandidateBundleRefreshOutboxPublishConfig(operator_approved=True),
            runtime_config_loader=_raising_runtime_config,
        ),
        await run_bounded_candidate_bundle_refresh_outbox_publish(
            _approved_config(event_id=uuid4(), candidate_group_id=uuid4()),
            runtime_config_loader=_raising_runtime_config,
        ),
        await run_bounded_candidate_bundle_refresh_outbox_publish(
            _approved_config(max_events=2),
            runtime_config_loader=_raising_runtime_config,
        ),
        await run_bounded_candidate_bundle_refresh_outbox_publish(
            _approved_config(allow_runtime_config=False),
            runtime_config_loader=_raising_runtime_config,
        ),
        await run_bounded_candidate_bundle_refresh_outbox_publish(
            _approved_config(allow_database_write=False),
            runtime_config_loader=_raising_runtime_config,
        ),
        await run_bounded_candidate_bundle_refresh_outbox_publish(
            _approved_config(allow_redis_publish=False),
            runtime_config_loader=_raising_runtime_config,
        ),
    ]

    assert [case.error_code for case in cases] == [
        "target_missing",
        "target_conflict",
        "max_events_must_be_one",
        "runtime_config_not_allowed",
        "database_write_not_allowed",
        "redis_publish_not_allowed",
    ]
    assert all(case.state.runtime_config_loaded is False for case in cases)


@pytest.mark.asyncio
async def test_event_suffix_selector_uses_db_lookup_with_uniqueness_probe() -> None:
    row = _row()
    repository = FakeRepository([row])

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=None, event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.ok is True
    assert result.selector_type == "event_suffix"
    assert repository.fetch_calls == [
        {"event_id": None, "candidate_group_id": None, "event_suffix": str(row.event_id)[-8:], "limit": 2}
    ]


@pytest.mark.asyncio
async def test_candidate_group_selector_publishes_single_pending_row_after_db_pending_filter() -> None:
    candidate_group_id = uuid4()
    row = _row(candidate_group_id=candidate_group_id)
    repository = FakeRepository([row])
    publisher = FakeRedisPublisher()

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=None, candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )

    assert result.ok is True
    assert result.selector_type == "candidate_group_id"
    assert result.events_seen == 1
    assert result.events_published_count == 1
    assert repository.fetch_calls == [
        {
            "event_id": None,
            "candidate_group_id": candidate_group_id,
            "event_suffix": None,
            "limit": 2,
        }
    ]
    assert repository.mark_published_calls == [row.event_id]
    assert len(publisher.publish_calls) == 1


@pytest.mark.asyncio
async def test_candidate_group_selector_blocks_when_two_pending_rows_survive_pending_filter() -> None:
    candidate_group_id = uuid4()
    row_one = _row(candidate_group_id=candidate_group_id)
    row_two = _row(candidate_group_id=candidate_group_id)
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=None, candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row_one, row_two])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_count_exceeded"
    assert result.events_seen == 2
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_candidate_group_selector_not_found_when_pending_filter_returns_no_rows() -> None:
    candidate_group_id = uuid4()
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=None, candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_not_found"
    assert result.events_seen == 0
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_event_suffix_selector_blocks_when_two_pending_rows_survive_pending_filter() -> None:
    row_one = _row()
    row_two = _row()
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=None, event_suffix=str(row_one.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row_one, row_two])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_count_exceeded"
    assert result.events_seen == 2
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_event_suffix_selector_not_found_when_pending_filter_returns_no_rows() -> None:
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=None, event_suffix="abcd1234"),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_not_found"
    assert result.events_seen == 0
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_duplicate_suffix_or_candidate_group_target_blocks_before_publish() -> None:
    row_one = _row()
    row_two = _row()
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=None, candidate_group_id=row_one.aggregate_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row_one, row_two])),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_count_exceeded"
    assert result.events_seen == 2
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_non_pending_or_wrong_contract_blocks_without_publish_or_db_write() -> None:
    wrong_event = _row(event_type="analysis.requested.v1")
    wrong_aggregate = _row(aggregate_type="candidate")
    not_pending = _row(status="published")

    first = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=wrong_event.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([wrong_event])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    second = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=wrong_aggregate.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([wrong_aggregate])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    third = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=not_pending.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([not_pending])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert first.error_code == "candidate_bundle_refresh_event_contract_mismatch"
    assert second.error_code == "candidate_bundle_refresh_event_contract_mismatch"
    assert third.error_code == "target_event_not_pending"
    assert first.state.redis_publish_attempted is False
    assert second.state.redis_publish_attempted is False
    assert third.state.redis_publish_attempted is False
    assert first.state.database_write_attempted is False
    assert second.state.database_write_attempted is False
    assert third.state.database_write_attempted is False


@pytest.mark.asyncio
async def test_malformed_payload_missing_required_field_blocks_before_publish_or_db_write() -> None:
    candidate_group_id = uuid4()
    malformed_payload = _payload(candidate_group_id)
    raw_refresh_reason = str(malformed_payload.pop("refresh_reason"))
    row = _row(candidate_group_id=candidate_group_id, payload_json=malformed_payload)

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "malformed_event_payload"
    assert result.payload_has_candidate_group_id is True
    assert result.payload_has_trigger_kind is True
    assert result.payload_has_trigger_object_type is True
    assert result.payload_has_trigger_object_id is True
    assert result.payload_has_refresh_reason is False
    assert result.payload_has_source_version_no is True
    assert result.payload_has_bundle_id is True
    assert result.state.redis_publish_attempted is False
    assert result.state.database_write_attempted is False
    assert raw_refresh_reason not in rendered


@pytest.mark.asyncio
async def test_candidate_group_payload_aggregate_mismatch_blocks_before_publish_or_db_write() -> None:
    row = _row(payload_json=_payload(uuid4()))

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.error_code == "candidate_group_id_mismatch"
    assert result.state.redis_publish_attempted is False
    assert result.state.database_write_attempted is False


@pytest.mark.asyncio
async def test_successful_publish_uses_existing_route_order_and_thin_id_only_payload() -> None:
    operation_log: list[str] = []
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    row = _row(candidate_group_id=candidate_group_id, bundle_id=bundle_id)
    repository = FakeRepository([row], operation_log=operation_log)
    publisher = FakeRedisPublisher(operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "published"
    assert report["ok"] is True
    assert report["queue_name"] == "q.candidate.bundle"
    assert report["stage_name"] == "bundle"
    assert report["target_event_id_suffix"] == str(row.event_id)[-8:]
    assert report["target_candidate_group_id_suffix"] == str(candidate_group_id)[-8:]
    assert report["events_published_count"] == 1
    assert report["job_attempts_inserted_count"] == 1
    assert report["payload_has_candidate_group_id"] is True
    assert report["payload_has_trigger_kind"] is True
    assert report["payload_has_trigger_object_type"] is True
    assert report["payload_has_trigger_object_id"] is True
    assert report["payload_has_refresh_reason"] is True
    assert report["payload_has_source_version_no"] is True
    assert report["payload_has_bundle_id"] is True
    assert report["redis_publish_attempted"] is True
    assert report["database_write_attempted"] is True
    assert report["event_outbox_marked_published"] is True
    assert operation_log == ["fetch", "publish", "mark_published", "insert_job_attempt"]
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls == [
        {
            "stage_name": "bundle",
            "queue_name": "q.candidate.bundle",
            "root_object_type": "candidate_group",
            "root_object_id": candidate_group_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]

    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.candidate.bundle"
    assert route.stage_name == "bundle"
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "bundle",
        "root_object_type": "candidate_group",
        "root_object_id": str(candidate_group_id),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    assert set(fields) == {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "pipeline_run_id",
        "not_before",
        "trigger_event_id",
    }
    for forbidden in (
        "payload_json",
        "refresh_reason",
        "trigger_kind",
        "trigger_object_type",
        "trigger_object_id",
        "source_version_no",
        "bundle_id",
        "source_text",
        "url",
        "database_url",
        "redis_url",
    ):
        assert forbidden not in fields


@pytest.mark.asyncio
async def test_route_drift_blocks_before_publish() -> None:
    row = _row()
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=publisher_builder,
        route_resolver=RogueRouteResolver(),  # type: ignore[arg-type]
    )

    assert result.error_code == "route_not_allowed"
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_redis_publish_failure_does_not_mark_published_or_insert_job_attempt() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository([row], operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log, failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "redis_xadd_failed"
    assert result.error_class == "RuntimeError"
    assert result.state.redis_publish_attempted is True
    assert result.events_published_count == 0
    assert result.state.database_write_attempted is False
    assert result.event_outbox_marked_published is False
    assert result.job_attempts_inserted_count == 0
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []
    assert repository_builder.close_commits == [False]
    assert operation_log == ["fetch", "publish"]
    assert EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_database_write_failure_after_xadd_returns_sanitized_failure() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository([row], operation_log=operation_log, fail_insert_job_attempt=True)
    publisher = FakeRedisPublisher(operation_log=operation_log)

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["error_code"] == "database_write_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["redis_publish_attempted"] is True
    assert report["events_published_count"] == 1
    assert report["database_write_attempted"] is True
    assert report["event_outbox_marked_published"] is True
    assert report["job_attempts_inserted_count"] == 0
    assert report["side_effects"]["redis_mutation"] is True
    assert operation_log == ["fetch", "publish", "mark_published", "insert_job_attempt"]
    assert "sentinel job attempt detail" not in rendered


@pytest.mark.asyncio
async def test_commit_close_failure_after_xadd_returns_sanitized_failure() -> None:
    row = _row()
    repository_builder = FakeRepositoryBuilder(
        FakeRepository([row]),
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )

    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["ok"] is False
    assert report["error_code"] == "repository_commit_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["events_published_count"] == 1
    assert CLOSE_EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_sanitized_failure_report_omits_raw_values() -> None:
    row = _row()
    result = await run_bounded_candidate_bundle_refresh_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row], fail_insert_job_attempt=True)),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        str(row.payload_json["trigger_object_id"]),
        str(row.payload_json["bundle_id"]),
        row.dedupe_key,
        RAW_REFRESH_REASON,
        RAW_SOURCE_TEXT,
        RAW_URL,
        REDIS_MESSAGE_ID,
        DB_LOCATOR,
        REDIS_LOCATOR,
        "sentinel job attempt detail",
    ):
        assert raw not in rendered


def test_source_ast_guard_has_no_broad_worker_consumer_or_forbidden_imports() -> None:
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
        "xack",
        "ack",
        "consume",
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
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".analysis_router" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".router_normalizer" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "OutboxRelayService" not in source
    assert "run_forever(" not in source
    assert "xreadgroup" not in source.lower()
    assert "xack" not in source.lower()
