from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.outbox_relay.models import QueueRoute


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_outbox_relay_source_message_route_readiness_probe.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-outbox-route@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-outbox-route@127.0.0.1:6379/0"
FAKE_DEDUPE_KEY = "source-message-created-sensitive-dedupe-key"
FAKE_LOGICAL_POST_KEY = "telegram-sensitive-logical-post-key"
FAKE_MESSAGE_TEXT = "Sensitive source message text must not be emitted"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/sensitive-runtime.env"


class FakeResult:
    def __init__(self, *, scalar: Any = None, rows: list[Any] | None = None) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def fetchall(self) -> list[Any]:
        return self._rows


class FakeTransaction:
    def __init__(self) -> None:
        self.rolled_back = False

    def rollback(self) -> None:
        self.rolled_back = True


class FakeDatabaseConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        source_message_exists: bool = True,
        source_version_exists: bool = True,
        table_available: bool = True,
        read_only_value: str = "on",
    ) -> None:
        self.rows = rows or []
        self.source_message_exists = source_message_exists
        self.source_version_exists = source_version_exists
        self.table_available = table_available
        self.read_only_value = read_only_value
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.transaction: FakeTransaction | None = None
        self.closed = False

    def begin(self) -> FakeTransaction:
        self.transaction = FakeTransaction()
        return self.transaction

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(statement)
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        forbidden_sql = (" INSERT ", " UPDATE ", " DELETE ", " TRUNCATE ", " DROP ", " ALTER ")
        padded = f" {normalized.upper()} "
        assert not any(marker in padded for marker in forbidden_sql), statement

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
        if normalized == _normalize(module.SELECT_SOURCE_MESSAGE_EXISTS_QUERY):
            return FakeResult(scalar=self.source_message_exists)
        if normalized == _normalize(module.SELECT_SOURCE_MESSAGE_VERSION_EXISTS_QUERY):
            return FakeResult(scalar=self.source_version_exists)

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.closed = True


class MismatchedRouteResolver:
    def resolve(self, row: Any) -> QueueRoute:
        return QueueRoute(queue_name="q.unexpected", stage_name="normalize")


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_outbox_relay_source_message_route_readiness_probe"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {"DATABASE_URL": FAKE_DATABASE_URL, "REDIS_URL": FAKE_REDIS_URL}


def _fake_row(
    event_type: str = "source_message.created.v1",
    *,
    event_id: UUID | None = None,
    aggregate_id: UUID | None = None,
    dedupe_key: str = FAKE_DEDUPE_KEY,
    payload_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_id = event_id or uuid4()
    aggregate_id = aggregate_id or uuid4()
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": "source_message",
        "aggregate_id": aggregate_id,
        "dedupe_key": dedupe_key,
        "payload_json": payload_json
        or {
            "source_message_id": str(aggregate_id),
            "logical_post_key": FAKE_LOGICAL_POST_KEY,
            "text_body": FAKE_MESSAGE_TEXT,
            "payload_marker": "sensitive-payload-json-value",
        },
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _run_report(
    *,
    rows: list[dict[str, Any]] | None = None,
    connection: FakeDatabaseConnection | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
    max_events: int = 3,
) -> tuple[Any, FakeDatabaseConnection]:
    module = _module()
    fake_connection = connection or FakeDatabaseConnection(rows)
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        max_events=max_events,
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _url: fake_connection,
        route_resolver=route_resolver,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH,),
    )
    return result, fake_connection


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_run_with_one_pending_source_created_reports_ready() -> None:
    result, connection = _run_report(rows=[_fake_row()])
    report = result.report

    assert result.exit_code == 0
    assert report["contract_status"] == "outbox_relay_source_message_route_readiness_ready"
    assert report["runtime_env_read"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["pending_source_outbox_events_bucket"] == "one"
    assert report["selected_outbox_events_bucket"] == "one"
    assert report["route_q_source_normalize_bucket"] == "one"
    assert report["route_stage_normalize_bucket"] == "one"
    assert report["redis_thin_payload_shape_valid_bucket"] == "one"
    assert report["source_message_rehydrate_attempted"] is True
    assert report["source_message_rehydrate_succeeded_bucket"] == "one"
    assert report["source_version_rehydrate_succeeded_bucket"] == "one"
    assert connection.transaction is not None
    assert connection.transaction.rolled_back is True


@pytest.mark.parametrize(
    "event_type",
    [
        "source_message.created.v1",
        "source_message.edited.v1",
        "source_message.deleted.v1",
        "source_message.reconciled.v1",
    ],
)
def test_all_source_message_event_types_route_to_source_normalize(event_type: str) -> None:
    module = _module()
    result, _connection = _run_report(rows=[_fake_row(event_type)])
    row = module._select_pending_source_outbox_events(
        FakeDatabaseConnection([_fake_row(event_type)]),
        limit=1,
    )[0]
    route = module.OutboxRouteResolver().resolve(row)

    assert result.report["contract_status"] == "outbox_relay_source_message_route_readiness_ready"
    assert route.queue_name == "q.source.normalize"
    assert route.stage_name == "normalize"


def test_thin_redis_payload_shape_contains_only_allowed_fields_without_payload_json() -> None:
    module = _module()
    row = module._select_pending_source_outbox_events(
        FakeDatabaseConnection([_fake_row()]),
        limit=1,
    )[0]
    route = module.OutboxRouteResolver().resolve(row)
    fields = module.build_redis_thin_message(row, route).as_stream_fields()
    valid, includes_large_json = module.validate_redis_thin_payload_shape(fields)

    assert valid is True
    assert includes_large_json is False
    assert set(fields) == module.ALLOWED_REDIS_THIN_FIELDS
    assert "payload_json" not in fields


def test_no_pending_source_events_returns_no_pending_status() -> None:
    result, _connection = _run_report(rows=[])

    assert result.exit_code == 0
    assert (
        result.report["contract_status"]
        == "outbox_relay_source_message_route_readiness_no_pending_events"
    )
    assert result.report["pending_source_outbox_events_bucket"] == "zero"
    assert result.report["selected_outbox_events_bucket"] == "zero"


def test_unsupported_or_mismatched_route_returns_contract_mismatch() -> None:
    result, _connection = _run_report(
        rows=[_fake_row()],
        route_resolver=MismatchedRouteResolver(),
    )

    assert result.exit_code == 1
    assert (
        result.report["contract_status"]
        == "blocked_outbox_relay_source_message_route_contract_mismatch"
    )
    assert result.report["route_q_source_normalize_bucket"] == "zero"


def test_unsupported_event_type_returns_contract_mismatch() -> None:
    result, _connection = _run_report(
        rows=[_fake_row("source_message.unexpected.v1")],
    )

    assert result.exit_code == 1
    assert (
        result.report["contract_status"]
        == "blocked_outbox_relay_source_message_route_contract_mismatch"
    )
    assert result.report["unsupported_events_bucket"] == "one"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE event_outbox SET status = 'published'",
        "INSERT INTO event_outbox (event_type) VALUES ('x')",
        "DELETE FROM event_outbox",
    ],
)
def test_read_only_sql_allowlist_blocks_update_insert_delete(statement: str) -> None:
    module = _module()

    with pytest.raises(ValueError):
        module._execute_read(FakeDatabaseConnection(), statement)


def test_rehydration_success_is_bucketed_and_emits_no_ids_text_or_payload() -> None:
    event_id = uuid4()
    aggregate_id = uuid4()
    result, _connection = _run_report(
        rows=[
            _fake_row(
                event_id=event_id,
                aggregate_id=aggregate_id,
                payload_json={
                    "source_message_id": str(aggregate_id),
                    "logical_post_key": FAKE_LOGICAL_POST_KEY,
                    "text_body": FAKE_MESSAGE_TEXT,
                    "payload_marker": "sensitive-payload-json-value",
                },
            )
        ]
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.report["source_message_rehydrate_succeeded_bucket"] == "one"
    assert result.report["source_version_rehydrate_succeeded_bucket"] == "one"
    assert str(event_id) not in rendered
    assert str(aggregate_id) not in rendered
    assert FAKE_DEDUPE_KEY not in rendered
    assert FAKE_LOGICAL_POST_KEY not in rendered
    assert FAKE_MESSAGE_TEXT not in rendered
    assert "sensitive-payload-json-value" not in rendered


def test_missing_source_message_or_version_rehydration_fails_without_leaking_ids() -> None:
    aggregate_id = uuid4()
    connection = FakeDatabaseConnection(
        [_fake_row(aggregate_id=aggregate_id)],
        source_message_exists=False,
        source_version_exists=False,
    )
    result, _connection = _run_report(connection=connection)
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert (
        result.report["contract_status"]
        == "blocked_outbox_relay_source_message_route_readiness_failed"
    )
    assert result.report["source_message_rehydrate_succeeded_bucket"] == "zero"
    assert result.report["source_version_rehydrate_succeeded_bucket"] == "zero"
    assert str(aggregate_id) not in rendered
    assert FAKE_MESSAGE_TEXT not in rendered


@pytest.mark.parametrize(
    "flag",
    [
        "event_outbox_status_mutation_performed",
        "redis_mutation_performed",
        "source_tables_mutation_performed",
        "downstream_service_started",
        "docker_or_systemd_changed",
        "alembic_run",
    ],
)
def test_forbidden_mutation_side_effect_flags_block(flag: str) -> None:
    result, _connection = _run_report(side_effect_flags={flag: True})

    assert result.exit_code == 1
    assert result.report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert result.report[flag] is True


def test_output_redaction_excludes_raw_values_urls_runtime_paths_and_secrets() -> None:
    event_id = uuid4()
    aggregate_id = uuid4()
    result, _connection = _run_report(
        rows=[
            _fake_row(
                event_id=event_id,
                aggregate_id=aggregate_id,
                dedupe_key=FAKE_DEDUPE_KEY,
            )
        ]
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
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-outbox-route",
        "unit-redis-password-outbox-route",
        FAKE_RUNTIME_PATH,
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
