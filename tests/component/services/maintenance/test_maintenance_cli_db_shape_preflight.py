from __future__ import annotations

import json

import pytest

from services.maintenance import main as maintenance_main
from services.maintenance.db_shape_preflight import (
    REQUIRED_ENUM_LABELS,
    REQUIRED_TABLE_COLUMNS,
    DbShapeCheckResult,
    DbShapePreflightReport,
)


def _args():
    return maintenance_main.build_parser().parse_args(["db-shape-preflight", "--format", "json"])


@pytest.mark.asyncio
async def test_db_shape_preflight_command_returns_deterministic_json_with_fake_session() -> None:
    database_url = "fake-database-url-for-test"
    disposed: list[bool] = []
    emitted: list[str] = []

    exit_code = await maintenance_main._run_db_shape_preflight(
        _args(),
        env={"DATABASE_URL": database_url},
        emit_json=emitted.append,
        session_factory_builder=_session_factory_builder(FakeMetadataSession(), disposed),
    )
    first_payload = json.loads(emitted[0])
    emitted.clear()
    second_exit_code = await maintenance_main._run_db_shape_preflight(
        _args(),
        env={"DATABASE_URL": database_url},
        emit_json=emitted.append,
        session_factory_builder=_session_factory_builder(FakeMetadataSession(), disposed),
    )

    assert exit_code == 0
    assert second_exit_code == 0
    assert json.loads(emitted[0]) == first_payload
    assert first_payload["schema_version"] == "db_shape_preflight_v1"
    assert first_payload["status"] == "pass"
    assert first_payload["missing_tables"] == []
    assert first_payload["missing_columns"] == []
    assert first_payload["missing_enum_labels"] == []
    assert first_payload["warnings"] == []
    assert first_payload["checks"][0]["check_name"] == "read_only_transaction_guard"
    assert database_url not in json.dumps(first_payload, sort_keys=True)
    assert disposed == [True, True]


@pytest.mark.asyncio
async def test_db_shape_preflight_command_returns_exit_2_on_fail_report_with_fake_session() -> None:
    emitted: list[str] = []

    exit_code = await maintenance_main._run_db_shape_preflight(
        _args(),
        env={"DATABASE_URL": "fake-database-url-for-test"},
        emit_json=emitted.append,
        session_factory_builder=_session_factory_builder(FakeMetadataSession(missing_tables={"event_outbox"}), []),
    )
    payload = json.loads(emitted[0])

    assert exit_code == 2
    assert payload["schema_version"] == "db_shape_preflight_v1"
    assert payload["status"] == "fail"
    assert payload["missing_tables"] == ["event_outbox"]


@pytest.mark.asyncio
async def test_db_shape_preflight_missing_database_url_returns_sanitized_config_error() -> None:
    emitted: list[str] = []

    exit_code = await maintenance_main._run_db_shape_preflight(
        _args(),
        env={},
        emit_json=emitted.append,
        session_factory_builder=lambda database_url: (_ for _ in ()).throw(AssertionError(database_url)),
    )
    payload = json.loads(emitted[0])

    assert exit_code == 1
    assert payload["schema_version"] == "db_shape_preflight_v1"
    assert payload["status"] == "fail"
    assert payload["missing_tables"] == []
    assert payload["missing_columns"] == []
    assert payload["missing_enum_labels"] == []
    assert payload["warnings"] == ["database_url_required"]
    assert "DATABASE_URL" not in emitted[0]
    assert "postgresql" not in emitted[0]


@pytest.mark.asyncio
async def test_db_shape_preflight_command_uses_injected_runner_and_sanitizes_output() -> None:
    repository = object()
    calls: list[object] = []
    emitted: list[str] = []

    async def fake_runner(received_repository):
        calls.append(received_repository)
        return DbShapePreflightReport(
            schema_version="db_shape_preflight_v1",
            status="pass",
            checks=[
                DbShapeCheckResult(
                    check_name="read_only_transaction_guard",
                    status="pass",
                    check_type="read_only_guard",
                    expected="available_and_not_violated",
                    observed="available",
                )
            ],
            missing_tables=[],
            missing_columns=[],
            missing_enum_labels=[],
            warnings=[],
        )

    exit_code = await maintenance_main.run_db_shape_preflight_command(
        repository,
        emit_json=emitted.append,
        report_runner=fake_runner,
    )
    payload = json.loads(emitted[0])

    assert exit_code == 0
    assert calls == [repository]
    assert payload["schema_version"] == "db_shape_preflight_v1"
    assert payload["status"] == "pass"
    assert "DATABASE_URL" not in emitted[0]
    assert "REDIS_URL" not in emitted[0]
    assert "TELEGRAM_BOT_TOKEN" not in emitted[0]
    assert "OPENAI_API_KEY" not in emitted[0]


@pytest.mark.asyncio
async def test_db_shape_preflight_dispatch_does_not_load_global_config(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_db_shape_preflight(args):
        calls.append(args.command)
        return 0

    def fail_from_env(cls):
        raise AssertionError("db-shape-preflight must not load Redis-backed maintenance config")

    monkeypatch.setattr(maintenance_main, "_run_db_shape_preflight", fake_db_shape_preflight)
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_from_env))

    exit_code = await maintenance_main._run(["db-shape-preflight", "--format", "json"])

    assert exit_code == 0
    assert calls == ["db-shape-preflight"]


def _session_factory_builder(session, disposed: list[bool]):
    def build(database_url: str):
        assert database_url == "fake-database-url-for-test"

        async def dispose() -> None:
            disposed.append(True)

        return FakeSessionFactory(session), dispose

    return build


class FakeSessionFactory:
    def __init__(self, session) -> None:
        self._session = session

    def __call__(self):
        return FakeSessionContext(self._session)


class FakeSessionContext:
    def __init__(self, session) -> None:
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeMetadataSession:
    def __init__(self, *, missing_tables: set[str] | None = None) -> None:
        self.executed_sql: list[str] = []
        self.missing_tables = missing_tables or set()

    def in_transaction(self) -> bool:
        return False

    async def execute(self, statement, params=None):
        del params
        sql = " ".join(str(statement).split())
        self.executed_sql.append(sql)
        if sql in {"BEGIN READ ONLY", "SET LOCAL statement_timeout = '5s'", "ROLLBACK"}:
            return FakeResult([])
        if "information_schema.tables" in sql:
            return FakeResult(
                [
                    {"table_name": table_name}
                    for table_name in REQUIRED_TABLE_COLUMNS
                    if table_name not in self.missing_tables
                ]
            )
        if "information_schema.columns" in sql:
            return FakeResult(
                [
                    {"table_name": table_name, "column_name": column_name}
                    for table_name, columns in REQUIRED_TABLE_COLUMNS.items()
                    if table_name not in self.missing_tables
                    for column_name in columns
                ]
            )
        if "pg_catalog.pg_enum" in sql:
            return FakeResult(
                [
                    {"enum_name": enum_name, "enum_label": enum_label}
                    for enum_name, labels in REQUIRED_ENUM_LABELS.items()
                    for enum_label in labels
                ]
            )
        raise AssertionError(f"unexpected SQL: {sql}")


class FakeResult:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self) -> list[dict[str, str]]:
        return self._rows
