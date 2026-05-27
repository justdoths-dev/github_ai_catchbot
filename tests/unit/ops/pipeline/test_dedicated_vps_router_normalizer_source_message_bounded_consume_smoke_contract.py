from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_router_normalizer_source_message_bounded_consume_smoke.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-normalizer-consume@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-normalizer-consume@127.0.0.1:6379/0"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000-0"
FAKE_TEXT = "Sensitive source text https://github.com/octocat/Hello-World"
FAKE_DEDUPE_KEY = "sensitive-source-message-dedupe-key"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

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
        *,
        event_row: dict[str, Any] | None,
        source_row: dict[str, Any] | None,
        version_row: dict[str, Any] | None,
        read_only_value: str = "on",
        table_available: bool = True,
        order: list[str] | None = None,
    ) -> None:
        self.event_row = event_row
        self.source_row = source_row
        self.version_row = version_row
        self.read_only_value = read_only_value
        self.table_available = table_available
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.written_tables: list[str] = []
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
        if "FROM event_outbox WHERE event_id" in normalized:
            return FakeResult(rows=[] if self.event_row is None else [self.event_row])
        if "FROM source_messages WHERE source_message_id" in normalized:
            return FakeResult(rows=[] if self.source_row is None else [self.source_row])
        if "FROM source_message_versions WHERE source_message_id" in normalized:
            return FakeResult(rows=[] if self.version_row is None else [self.version_row])

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True
        if self.order is not None:
            self.order.append("commit")

    async def rollback(self) -> None:
        self.rolled_back = True
        if self.order is not None:
            self.order.append("rollback")

    async def close(self) -> None:
        self.closed = True

    def record_write(self, table: str) -> None:
        self.written_tables.append(table)
        if self.order is not None:
            self.order.append(f"write:{table}")


class FakeRedis:
    def __init__(
        self,
        *,
        stream_fields: dict[str, Any] | None,
        stream_id: str = FAKE_STREAM_ID,
        ack_error: Exception | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.stream_fields = stream_fields
        self.stream_id = stream_id
        self.ack_error = ack_error
        self.order = order
        self.group_created = False
        self.acked: list[str] = []
        self.closed = False

    async def ping(self) -> bool:
        return True

    async def xlen(self, name: str) -> int:
        assert name == "q.source.normalize"
        return 1 if self.stream_fields is not None else 0

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        assert name == "q.source.normalize"
        if self.stream_fields is None:
            return []
        return [(self.stream_id, dict(self.stream_fields))]

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> None:
        assert name == "q.source.normalize"
        assert id == "0"
        self.group_created = True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, Any]]]]]:
        assert self.group_created is True
        assert streams == {"q.source.normalize": ">"}
        if self.stream_fields is None:
            return []
        return [("q.source.normalize", [(self.stream_id, dict(self.stream_fields))])]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        assert name == "q.source.normalize"
        if self.order is not None:
            self.order.append("ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(ids)
        return len(ids)

    async def aclose(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_router_normalizer_source_message_bounded_consume_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
    }


def _all_approvals() -> Any:
    module = _module()
    return module.ConsumeApprovals(
        router_normalizer_consume_smoke=True,
        redis_consumer_group=True,
        normalization_write=True,
        artifact_candidate_write=True,
        event_outbox_write=True,
        redis_ack=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "router_normalizer_consume_smoke": False,
        "redis_consumer_group": False,
        "normalization_write": False,
        "artifact_candidate_write": False,
        "event_outbox_write": False,
        "redis_ack": False,
    }
    values.update(overrides)
    return _module().ConsumeApprovals(**values)


def _event_row(
    *,
    event_id: UUID,
    source_message_id: UUID,
    event_type: str = "source_message.created.v1",
    version_no: int = 1,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": "source_message",
        "aggregate_id": source_message_id,
        "dedupe_key": FAKE_DEDUPE_KEY,
        "payload_json": {
            "event_id": str(event_id),
            "source_message_id": str(source_message_id),
            "current_version_no": version_no,
            "message_text": FAKE_TEXT,
        },
        "status": "published",
        "created_at": now,
        "published_at": now,
    }


def _source_row(*, source_message_id: UUID, version_no: int = 1, deleted: bool = False) -> dict[str, Any]:
    return {
        "source_message_id": source_message_id,
        "current_version_no": version_no,
        "text_body": FAKE_TEXT,
        "caption_text": None,
        "text_surface": FAKE_TEXT,
        "entities_json": [],
        "url_surface_json": [],
        "raw_message_json": {"private_text": FAKE_TEXT},
        "deleted_at": datetime.now(timezone.utc) if deleted else None,
    }


def _version_row(*, source_message_id: UUID, version_no: int = 1) -> dict[str, Any]:
    return {
        "source_message_id": source_message_id,
        "version_no": version_no,
        "text_surface": FAKE_TEXT,
        "entities_json": [],
        "raw_message_json": {"private_version_text": FAKE_TEXT},
    }


def _thin_fields(*, event_id: UUID, source_message_id: UUID, **overrides: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "job_id": str(event_id),
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": str(source_message_id),
        "idempotency_key": "private-idempotency-key",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }
    fields.update(overrides)
    return fields


def _candidate_result() -> SimpleNamespace:
    return SimpleNamespace(
        artifact_count=1,
        candidate_group_count=1,
        suppression_reason_codes=[],
    )


def _suppression_result() -> SimpleNamespace:
    return SimpleNamespace(
        artifact_count=0,
        candidate_group_count=0,
        suppression_reason_codes=["source_message_deleted_current"],
    )


def _run_report(
    *,
    event_row: dict[str, Any] | None | object = ...,
    source_row: dict[str, Any] | None | object = ...,
    version_row: dict[str, Any] | None | object = ...,
    stream_fields: dict[str, Any] | None | object = ...,
    approvals: Any | None = None,
    redis: FakeRedis | None = None,
    session: FakeSession | None = None,
    normalizer_runner: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    event_id = uuid4()
    source_message_id = uuid4()
    effective_event = (
        _event_row(event_id=event_id, source_message_id=source_message_id)
        if event_row is ...
        else event_row
    )
    effective_source = (
        _source_row(source_message_id=source_message_id) if source_row is ... else source_row
    )
    effective_version = (
        _version_row(source_message_id=source_message_id) if version_row is ... else version_row
    )
    effective_fields = (
        _thin_fields(event_id=event_id, source_message_id=source_message_id)
        if stream_fields is ...
        else stream_fields
    )
    fake_session = session or FakeSession(
        event_row=effective_event,
        source_row=effective_source,
        version_row=effective_version,
    )
    fake_redis = redis or FakeRedis(stream_fields=effective_fields)
    module = _module()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: fake_session,
        redis_client_factory=lambda _url: fake_redis,
        normalizer_runner=normalizer_runner or _default_runner(_candidate_result()),
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, "raw-ack-secret-text"),
    )
    return result, fake_session, fake_redis


def _default_runner(result: Any):
    async def runner(_config: Any, _message: Any, session: FakeSession) -> Any:
        session.record_write("normalization_runs")
        return result

    return runner


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_no_approval_mode_is_read_only_without_group_write_or_ack() -> None:
    result, session, redis = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == "router_normalizer_source_message_bounded_consume_smoke_ready"
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["redis_consumer_group_mutation_attempted"] is False
    assert result.report["normalization_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.group_created is False
    assert redis.acked == []
    assert session.written_tables == []
    assert session.rolled_back is True


def test_partial_approvals_block_before_group_ack_or_db_mutation() -> None:
    result, session, redis = _run_report(
        approvals=_approvals(
            router_normalizer_consume_smoke=True,
            normalization_write=True,
            artifact_candidate_write=True,
            event_outbox_write=True,
        )
    )

    assert result.exit_code == 1
    assert "approval.redis_consumer_group" in result.report["checks_failed"]
    assert "approval.redis_ack" in result.report["checks_failed"]
    assert result.report["redis_consumer_group_mutation_attempted"] is False
    assert result.report["normalization_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.group_created is False
    assert redis.acked == []
    assert session.written_tables == []


def test_invalid_thin_payload_shape_blocks() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    fields = _thin_fields(event_id=event_id, source_message_id=source_message_id)
    fields.pop("trigger_event_id")
    fields["payload_json"] = "{}"

    result, _session, redis = _run_report(stream_fields=fields)

    assert result.exit_code == 1
    assert "redis.thin_payload_shape" in result.report["checks_failed"]
    assert result.report["thin_payload_shape_valid_bucket"] == "zero"
    assert redis.group_created is False


def test_stage_and_root_mismatch_block() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    fields = _thin_fields(
        event_id=event_id,
        source_message_id=source_message_id,
        stage_name="enrich",
        root_object_type="artifact",
    )

    result, _session, redis = _run_report(stream_fields=fields)

    assert result.exit_code == 1
    assert "redis.stage_name_mismatch" in result.report["checks_failed"]
    assert "redis.root_object_type_mismatch" in result.report["checks_failed"]
    assert redis.group_created is False


def test_event_outbox_rehydrate_missing_blocks_safely() -> None:
    result, _session, redis = _run_report(event_row=None)

    assert result.exit_code == 1
    assert "event_outbox.rehydrate_missing" in result.report["checks_failed"]
    assert result.report["event_outbox_rehydrate_succeeded_bucket"] == "zero"
    assert redis.group_created is False


def test_source_message_missing_blocks_safely() -> None:
    result, _session, redis = _run_report(source_row=None)

    assert result.exit_code == 1
    assert "source_message.rehydrate_missing" in result.report["checks_failed"]
    assert result.report["source_message_rehydrate_succeeded_bucket"] == "zero"
    assert redis.group_created is False


def test_source_version_missing_blocks_safely() -> None:
    result, _session, redis = _run_report(version_row=None)

    assert result.exit_code == 1
    assert "source_version.rehydrate_missing" in result.report["checks_failed"]
    assert result.report["source_version_rehydrate_succeeded_bucket"] == "zero"
    assert redis.group_created is False


def test_write_mode_commits_db_before_ack() -> None:
    order: list[str] = []
    event_id = uuid4()
    source_message_id = uuid4()
    session = FakeSession(
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        source_row=_source_row(source_message_id=source_message_id),
        version_row=_version_row(source_message_id=source_message_id),
        order=order,
    )
    redis = FakeRedis(
        stream_fields=_thin_fields(event_id=event_id, source_message_id=source_message_id),
        order=order,
    )

    result, session, redis = _run_report(
        session=session,
        redis=redis,
        approvals=_all_approvals(),
        normalizer_runner=_default_runner(_candidate_result()),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == "router_normalizer_source_message_bounded_consume_smoke_consumed"
    assert order == ["write:normalization_runs", "commit", "ack"]
    assert session.committed is True
    assert redis.acked == [FAKE_STREAM_ID]


def test_db_write_failure_rolls_back_and_does_not_ack() -> None:
    async def failing_runner(_config: Any, _message: Any, _session: Any) -> Any:
        raise RuntimeError("db write failed with raw-ack-secret-text")

    result, session, redis = _run_report(
        approvals=_all_approvals(),
        normalizer_runner=failing_runner,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_router_normalizer_source_message_bounded_consume_smoke_write_failed"
    )
    assert session.rolled_back is True
    assert session.committed is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.acked == []


def test_ack_failure_after_commit_is_reported_without_raw_exception_text() -> None:
    redis = FakeRedis(
        stream_fields=None,
        ack_error=RuntimeError("ack failed with raw-ack-secret-text"),
    )
    event_id = uuid4()
    source_message_id = uuid4()
    redis.stream_fields = _thin_fields(event_id=event_id, source_message_id=source_message_id)
    session = FakeSession(
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        source_row=_source_row(source_message_id=source_message_id),
        version_row=_version_row(source_message_id=source_message_id),
    )

    result, session, redis = _run_report(
        session=session,
        redis=redis,
        approvals=_all_approvals(),
        normalizer_runner=_default_runner(_candidate_result()),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == (
        "blocked_router_normalizer_source_message_bounded_consume_smoke_ack_failed_after_commit"
    )
    assert session.committed is True
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_succeeded_bucket"] == "zero"
    assert result.report["redis_ack_failure_class"] == "RuntimeError"
    assert "raw-ack-secret-text" not in rendered
    assert "ack failed with" not in rendered


def test_candidate_path_reports_router_normalizer_owned_rows_only() -> None:
    result, session, redis = _run_report(
        approvals=_all_approvals(),
        normalizer_runner=_default_runner(_candidate_result()),
    )

    assert result.exit_code == 0
    assert result.report["normalization_runs_written_bucket"] == "one"
    assert result.report["artifacts_written_bucket"] == "one"
    assert result.report["artifact_observations_written_bucket"] == "one"
    assert result.report["candidate_groups_written_bucket"] == "one"
    assert result.report["candidate_members_written_bucket"] == "one"
    assert result.report["enrich_outbox_events_written_bucket"] == "one"
    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["registry_mutation_performed"] is False
    assert redis.acked == [FAKE_STREAM_ID]
    assert all(table != "source_messages" for table in session.written_tables)


def test_deleted_suppression_path_reports_normalization_and_suppression_only() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    result, _session, _redis = _run_report(
        event_row=_event_row(
            event_id=event_id,
            source_message_id=source_message_id,
            event_type="source_message.deleted.v1",
        ),
        source_row=_source_row(source_message_id=source_message_id, deleted=True),
        version_row=_version_row(source_message_id=source_message_id),
        stream_fields=_thin_fields(event_id=event_id, source_message_id=source_message_id),
        approvals=_all_approvals(),
        normalizer_runner=_default_runner(_suppression_result()),
    )

    assert result.exit_code == 0
    assert result.report["normalization_plan_candidate_eligible_bucket"] == "zero"
    assert result.report["normalization_runs_written_bucket"] == "one"
    assert result.report["suppression_traces_written_bucket"] == "one"
    assert result.report["artifacts_written_bucket"] == "zero"
    assert result.report["candidate_groups_written_bucket"] == "zero"
    assert result.report["enrich_outbox_events_written_bucket"] == "zero"


def test_report_does_not_emit_raw_values() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    result, _session, _redis = _run_report(
        event_row=_event_row(event_id=event_id, source_message_id=source_message_id),
        source_row=_source_row(source_message_id=source_message_id),
        version_row=_version_row(source_message_id=source_message_id),
        stream_fields=_thin_fields(event_id=event_id, source_message_id=source_message_id),
        approvals=_all_approvals(),
        normalizer_runner=_default_runner(_candidate_result()),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    forbidden = (
        str(event_id),
        str(source_message_id),
        FAKE_STREAM_ID,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-normalizer-consume",
        "unit-redis-password-normalizer-consume",
        FAKE_RUNTIME_PATH,
        FAKE_TEXT,
        "https://github.com/octocat/Hello-World",
        FAKE_DEDUPE_KEY,
        "private-idempotency-key",
    )
    for value in forbidden:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False


def test_forbidden_side_effect_flags_fail_closed_before_connections() -> None:
    result, session, redis = _run_report(side_effect_flags={"external_network_attempted": True})

    assert result.exit_code == 1
    assert result.report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert result.report["external_network_attempted"] is True
    assert result.report["runtime_env_read"] is False
    assert session.statements == []
    assert redis.group_created is False
