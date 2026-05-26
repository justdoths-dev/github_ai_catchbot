from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_outbox_relay_source_message_bounded_publish_smoke.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-outbox-publish@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-outbox-publish@127.0.0.1:6379/0"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/sensitive-runtime.env"
FAKE_DEDUPE_KEY = "source-message-created-sensitive-dedupe-key"
FAKE_LOGICAL_POST_KEY = "telegram-sensitive-logical-post-key"
FAKE_MESSAGE_TEXT = "Sensitive source message text must not be emitted"
FAKE_STREAM_ID = "1710000000000-0"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeResult:
    def __init__(self, *, scalar: Any = None, rows: list[dict[str, Any]] | None = None) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def scalar(self) -> Any:
        return self._scalar

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSession:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table_available: bool = True,
        read_only_value: str = "on",
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available
        self.read_only_value = read_only_value
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.published_event_ids: list[UUID] = []
        self.failed_event_ids: list[UUID] = []
        self.failure_errors: list[str] = []
        self.job_attempts: list[dict[str, Any]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            return FakeResult(scalar=self.table_available)
        if normalized == _normalize(module.COUNT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY):
            return FakeResult(scalar=len(self.rows))
        if normalized == _normalize(module.SELECT_PENDING_SOURCE_OUTBOX_EVENTS_QUERY):
            limit = int(params.get("limit", len(self.rows)))
            return FakeResult(rows=self.rows[:limit])
        if normalized.startswith("UPDATE event_outbox SET status = 'published'"):
            self.published_event_ids.append(UUID(str(params["event_id"])))
            return FakeResult()
        if normalized.startswith("UPDATE event_outbox SET status = 'failed'"):
            self.failed_event_ids.append(UUID(str(params["event_id"])))
            self.failure_errors.append(str(params["error_text"]))
            return FakeResult()
        if normalized.startswith("INSERT INTO job_attempts"):
            self.job_attempts.append(dict(params))
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any]] = []
        self.closed = False

    async def publish(self, route: Any, message: Any) -> str:
        self.calls.append((route, message))
        return FAKE_STREAM_ID

    async def close(self) -> None:
        self.closed = True


class FailingPublisher(FakePublisher):
    async def publish(self, route: Any, message: Any) -> str:
        self.calls.append((route, message))
        raise RuntimeError("redis publish failed with fake-secret-value")


class PublisherNotExpected(FakePublisher):
    async def publish(self, route: Any, message: Any) -> str:
        raise AssertionError("Redis publish should not be attempted")


class MismatchedRouteResolver:
    def resolve(self, row: Any) -> Any:
        module = _module()
        return module.QueueRoute(queue_name="q.unexpected", stage_name="normalize")


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_outbox_relay_source_message_bounded_publish_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "OUTBOX_RELAY_XADD_MAXLEN": "10000",
    }


def _all_approvals() -> Any:
    module = _module()
    return module.PublishApprovals(
        outbox_relay_publish_smoke=True,
        redis_publish=True,
        event_outbox_status_update=True,
        job_attempt_write=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "outbox_relay_publish_smoke": False,
        "redis_publish": False,
        "event_outbox_status_update": False,
        "job_attempt_write": False,
    }
    values.update(overrides)
    return _module().PublishApprovals(**values)


def _fake_row(
    event_type: str = "source_message.created.v1",
    *,
    event_id: UUID | None = None,
    aggregate_id: UUID | None = None,
    aggregate_type: str = "source_message",
    dedupe_key: str = FAKE_DEDUPE_KEY,
    payload_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_id = event_id or uuid4()
    aggregate_id = aggregate_id or uuid4()
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "dedupe_key": dedupe_key,
        "payload_json": payload_json
        or {
            "source_message_id": str(aggregate_id),
            "logical_post_key": FAKE_LOGICAL_POST_KEY,
            "message_text": FAKE_MESSAGE_TEXT,
            "payload_marker": "sensitive-payload-json-value",
        },
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _run_report(
    *,
    rows: list[dict[str, Any]] | None = None,
    approvals: Any | None = None,
    publisher: FakePublisher | None = None,
    route_resolver: Any | None = None,
    max_events: int = 1,
) -> tuple[Any, FakeSession, FakePublisher]:
    module = _module()
    session = FakeSession(rows)
    fake_publisher = publisher or PublisherNotExpected()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        max_events=max_events,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        publisher_factory=lambda _url, _maxlen: fake_publisher,
        route_resolver=route_resolver,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, "fake-secret-value"),
    )
    return result, session, fake_publisher


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_no_approval_mode_reports_ready_without_redis_or_db_writes() -> None:
    result, session, publisher = _run_report(rows=[_fake_row()])

    assert result.exit_code == 0
    assert (
        result.report["contract_status"]
        == "outbox_relay_source_message_bounded_publish_smoke_ready"
    )
    assert result.report["read_only_transaction"] is True
    assert result.report["redis_publish_attempted"] is False
    assert result.report["redis_mutation_performed"] is False
    assert result.report["event_outbox_status_update_attempted"] is False
    assert result.report["job_attempt_insert_attempted"] is False
    assert session.published_event_ids == []
    assert session.failed_event_ids == []
    assert session.job_attempts == []
    assert publisher.calls == []
    assert session.rolled_back is True


def test_missing_redis_publish_approval_blocks_write_capable_publish() -> None:
    result, session, publisher = _run_report(
        rows=[_fake_row()],
        approvals=_approvals(
            outbox_relay_publish_smoke=True,
            event_outbox_status_update=True,
            job_attempt_write=True,
        ),
    )

    assert result.exit_code == 1
    assert (
        result.report["contract_status"]
        == "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready"
    )
    assert "approval.redis_publish" in result.report["checks_failed"]
    assert result.report["redis_connected"] is False
    assert publisher.calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_missing_event_outbox_status_approval_blocks_event_outbox_mutation() -> None:
    result, session, publisher = _run_report(
        rows=[_fake_row()],
        approvals=_approvals(
            outbox_relay_publish_smoke=True,
            redis_publish=True,
            job_attempt_write=True,
        ),
    )

    assert result.exit_code == 1
    assert "approval.event_outbox_status_update" in result.report["checks_failed"]
    assert result.report["redis_connected"] is False
    assert publisher.calls == []
    assert session.published_event_ids == []
    assert session.failed_event_ids == []


def test_missing_job_attempt_approval_blocks_job_attempt_insert() -> None:
    result, session, publisher = _run_report(
        rows=[_fake_row()],
        approvals=_approvals(
            outbox_relay_publish_smoke=True,
            redis_publish=True,
            event_outbox_status_update=True,
        ),
    )

    assert result.exit_code == 1
    assert "approval.job_attempt_write" in result.report["checks_failed"]
    assert result.report["redis_connected"] is False
    assert publisher.calls == []
    assert session.job_attempts == []


def test_valid_write_capable_fake_run_publishes_and_marks_outbox_published() -> None:
    row = _fake_row()
    publisher = FakePublisher()
    result, session, publisher = _run_report(
        rows=[row],
        approvals=_all_approvals(),
        publisher=publisher,
    )

    assert result.exit_code == 0
    assert (
        result.report["contract_status"]
        == "outbox_relay_source_message_bounded_publish_smoke_published"
    )
    assert result.report["redis_connected"] is True
    assert result.report["redis_publish_attempted"] is True
    assert result.report["redis_publish_succeeded_bucket"] == "one"
    assert result.report["redis_mutation_performed"] is True
    assert result.report["event_outbox_status_update_attempted"] is True
    assert result.report["event_outbox_published_bucket"] == "one"
    assert session.published_event_ids == [row["event_id"]]
    assert session.failed_event_ids == []
    assert len(publisher.calls) == 1
    route, message = publisher.calls[0]
    assert route.queue_name == "q.source.normalize"
    assert route.stage_name == "normalize"
    assert message.trigger_event_id == str(row["event_id"])
    assert session.committed is True


def test_valid_write_capable_fake_run_inserts_succeeded_job_attempt() -> None:
    row = _fake_row()
    result, session, _publisher = _run_report(
        rows=[row],
        approvals=_all_approvals(),
        publisher=FakePublisher(),
    )

    assert result.report["job_attempt_insert_attempted"] is True
    assert result.report["job_attempt_succeeded_bucket"] == "one"
    assert len(session.job_attempts) == 1
    attempt = session.job_attempts[0]
    assert attempt["stage_name"] == "normalize"
    assert attempt["queue_name"] == "q.source.normalize"
    assert attempt["root_object_type"] == "source_message"
    assert attempt["root_object_id"] == str(row["aggregate_id"])
    assert attempt["attempt_status"] == "succeeded"
    assert attempt["error_code"] is None


def test_redis_publish_failure_does_not_mark_published_and_records_retryable_failure() -> None:
    row = _fake_row()
    result, session, _publisher = _run_report(
        rows=[row],
        approvals=_all_approvals(),
        publisher=FailingPublisher(),
    )

    assert result.exit_code == 1
    assert (
        result.report["contract_status"]
        == "blocked_outbox_relay_source_message_bounded_publish_smoke_publish_failed"
    )
    assert result.report["redis_publish_attempted"] is True
    assert result.report["redis_publish_succeeded_bucket"] == "zero"
    assert session.published_event_ids == []
    assert session.failed_event_ids == [row["event_id"]]
    assert session.failure_errors == ["RuntimeError"]
    assert result.report["event_outbox_failed_bucket"] == "one"
    assert result.report["job_attempt_failed_bucket"] == "one"
    assert session.job_attempts[0]["attempt_status"] == "failed_retryable"
    assert session.job_attempts[0]["error_code"] == "RuntimeError"


def test_route_mismatch_blocks_before_redis_publish_or_status_update() -> None:
    result, session, publisher = _run_report(
        rows=[_fake_row()],
        approvals=_all_approvals(),
        publisher=FakePublisher(),
        route_resolver=MismatchedRouteResolver(),
    )

    assert result.exit_code == 1
    assert (
        result.report["contract_status"]
        == "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready"
    )
    assert result.report["route_q_source_normalize_bucket"] == "zero"
    assert result.report["redis_connected"] is False
    assert publisher.calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_thin_redis_fields_include_only_allowed_fields_without_payload_json_or_raw_text() -> None:
    module = _module()
    row = module.SourceMessageOutboxRelayRepository(FakeSession([_fake_row()]))
    selected = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=None,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: row._session,
        publisher_factory=lambda _url, _maxlen: PublisherNotExpected(),
        forbidden_raw_values=(FAKE_RUNTIME_PATH,),
    )
    assert selected.report["contract_status"].endswith("_ready")

    outbox_row = module.OutboxEventRow(
        event_id=uuid4(),
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=uuid4(),
        dedupe_key=FAKE_DEDUPE_KEY,
        payload_json={"message_text": FAKE_MESSAGE_TEXT},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    route = module.OutboxRouteResolver().resolve(outbox_row)
    fields = module.build_redis_queued_message(outbox_row, route).as_stream_fields()

    assert set(fields) == module.ALLOWED_REDIS_THIN_FIELDS
    assert "payload_json" not in fields
    assert "raw_message_text" not in fields
    assert "message_text" not in fields
    assert module.validate_redis_thin_payload_shape(fields) is True


def test_max_events_hard_max_three_is_enforced_before_connections() -> None:
    module = _module()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        max_events=4,
        approvals=_all_approvals(),
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: FakeSession([_fake_row()]),
        publisher_factory=lambda _url, _maxlen: FakePublisher(),
    )

    assert result.exit_code == 1
    assert (
        result.report["contract_status"]
        == "blocked_outbox_relay_source_message_bounded_publish_smoke_not_ready"
    )
    assert "max_events.out_of_bounds" in result.report["checks_failed"]
    assert result.report["database_connected"] is False
    assert result.report["redis_connected"] is False


def test_output_redaction_excludes_raw_values_urls_runtime_path_stream_ids_and_secrets() -> None:
    event_id = uuid4()
    aggregate_id = uuid4()
    result, _session, _publisher = _run_report(
        rows=[
            _fake_row(
                event_id=event_id,
                aggregate_id=aggregate_id,
                dedupe_key=FAKE_DEDUPE_KEY,
            )
        ],
        approvals=_all_approvals(),
        publisher=FakePublisher(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    forbidden_values = (
        str(event_id),
        str(aggregate_id),
        FAKE_DEDUPE_KEY,
        FAKE_LOGICAL_POST_KEY,
        FAKE_MESSAGE_TEXT,
        "source_message_id",
        "payload_json",
        "sensitive-payload-json-value",
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-outbox-publish",
        "unit-redis-password-outbox-publish",
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        "fake-secret-value",
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
