from __future__ import annotations

import re

import pytest

from services.maintenance.db_shape_preflight import (
    READ_ONLY_DB_SHAPE_SQL_STATEMENTS,
    REQUIRED_ENUM_LABELS,
    REQUIRED_TABLE_COLUMNS,
    DbShapeSnapshot,
    SqlAlchemyDbShapeIntrospectionRepository,
    build_db_shape_preflight_report,
    run_db_shape_preflight,
)


FORBIDDEN_SQL_PATTERN = re.compile(r"\b(insert|update|delete|alter|create|drop|truncate)\b", re.IGNORECASE)
FORBIDDEN_METHOD_PREFIXES = ("insert", "update", "delete", "alter", "create", "drop", "truncate", "write")


class FakeDbShapeRepository:
    def __init__(self, snapshot: DbShapeSnapshot) -> None:
        self.snapshot = snapshot
        self.load_count = 0

    async def load_db_shape_snapshot(self) -> DbShapeSnapshot:
        self.load_count += 1
        return self.snapshot


def _complete_snapshot(*, warnings: tuple[str, ...] = ()) -> DbShapeSnapshot:
    return DbShapeSnapshot(
        table_columns={table: set(columns) for table, columns in REQUIRED_TABLE_COLUMNS.items()},
        enum_labels={enum_name: set(labels) for enum_name, labels in REQUIRED_ENUM_LABELS.items()},
        read_only_guard_available=True,
        warnings=warnings,
    )


@pytest.mark.asyncio
async def test_preflight_passes_when_all_required_shape_exists() -> None:
    repository = FakeDbShapeRepository(_complete_snapshot())

    report = await run_db_shape_preflight(repository)

    assert repository.load_count == 1
    assert report.schema_version == "db_shape_preflight_v1"
    assert report.status == "pass"
    assert report.missing_tables == []
    assert report.missing_columns == []
    assert report.missing_enum_labels == []
    assert report.warnings == []


def test_preflight_fails_when_required_table_is_missing() -> None:
    snapshot = _complete_snapshot()
    table_columns = dict(snapshot.table_columns)
    del table_columns["event_outbox"]

    report = build_db_shape_preflight_report(
        DbShapeSnapshot(
            table_columns=table_columns,
            enum_labels=snapshot.enum_labels,
            read_only_guard_available=True,
        )
    )

    assert report.status == "fail"
    assert report.missing_tables == ["event_outbox"]
    assert "event_outbox.event_id" not in report.missing_columns
    assert _check_status(report, "required_table:event_outbox") == "fail"


def test_preflight_fails_when_required_column_is_missing() -> None:
    snapshot = _complete_snapshot()
    table_columns = {table: set(columns) for table, columns in snapshot.table_columns.items()}
    table_columns["analyses"].remove("delivery_decision")

    report = build_db_shape_preflight_report(
        DbShapeSnapshot(
            table_columns=table_columns,
            enum_labels=snapshot.enum_labels,
            read_only_guard_available=True,
        )
    )

    assert report.status == "fail"
    assert report.missing_tables == []
    assert report.missing_columns == ["analyses.delivery_decision"]
    assert _check_status(report, "required_column:analyses.delivery_decision") == "fail"


def test_preflight_fails_when_required_enum_label_is_missing() -> None:
    snapshot = _complete_snapshot()
    enum_labels = {enum_name: set(labels) for enum_name, labels in snapshot.enum_labels.items()}
    enum_labels["notification_status_enum"].remove("failed_retryable")

    report = build_db_shape_preflight_report(
        DbShapeSnapshot(
            table_columns=snapshot.table_columns,
            enum_labels=enum_labels,
            read_only_guard_available=True,
        )
    )

    assert report.status == "fail"
    assert report.missing_enum_labels == ["notification_status_enum.failed_retryable"]
    assert _check_status(report, "required_enum_label:notification_status_enum.failed_retryable") == "fail"


@pytest.mark.parametrize(
    ("guard_available", "guard_violated", "observed"),
    [
        (False, False, "unavailable"),
        (True, True, "violated"),
    ],
)
def test_preflight_fails_when_read_only_guard_is_unavailable_or_violated(
    guard_available: bool,
    guard_violated: bool,
    observed: str,
) -> None:
    snapshot = _complete_snapshot()

    report = build_db_shape_preflight_report(
        DbShapeSnapshot(
            table_columns=snapshot.table_columns,
            enum_labels=snapshot.enum_labels,
            read_only_guard_available=guard_available,
            read_only_guard_violated=guard_violated,
        )
    )

    guard_check = next(check for check in report.checks if check.check_name == "read_only_transaction_guard")
    assert report.status == "fail"
    assert guard_check.status == "fail"
    assert guard_check.observed == observed


def test_warnings_do_not_flip_pass_to_fail() -> None:
    report = build_db_shape_preflight_report(_complete_snapshot(warnings=("optional_index_not_checked",)))

    assert report.status == "pass"
    assert report.warnings == ["optional_index_not_checked"]
    assert _check_status(report, "warning:optional_index_not_checked") == "warn"


def test_no_dml_or_ddl_sql_appears_in_preflight_queries() -> None:
    sql_text = "\n".join(READ_ONLY_DB_SHAPE_SQL_STATEMENTS)

    assert "BEGIN READ ONLY" in sql_text
    assert "SET LOCAL statement_timeout = '5s'" in sql_text
    assert "information_schema.tables" in sql_text
    assert "information_schema.columns" in sql_text
    assert "pg_catalog.pg_enum" in sql_text
    assert FORBIDDEN_SQL_PATTERN.search(sql_text) is None


def test_preflight_repository_surfaces_have_no_write_methods() -> None:
    fake_public_methods = _public_methods(FakeDbShapeRepository)
    sqlalchemy_public_methods = _public_methods(SqlAlchemyDbShapeIntrospectionRepository)

    assert fake_public_methods == ["load_db_shape_snapshot"]
    assert sqlalchemy_public_methods == ["load_db_shape_snapshot"]
    for method_name in fake_public_methods + sqlalchemy_public_methods:
        assert not method_name.startswith(FORBIDDEN_METHOD_PREFIXES)


@pytest.mark.asyncio
async def test_sqlalchemy_adapter_refuses_preexisting_transaction_without_queries() -> None:
    session = ActiveTransactionSession()

    snapshot = await SqlAlchemyDbShapeIntrospectionRepository(session).load_db_shape_snapshot()

    assert snapshot.read_only_guard_available is False
    assert snapshot.read_only_guard_violated is True
    assert snapshot.warnings == ("db_shape_preflight_active_transaction_refused",)
    assert session.executed_sql == []


class ActiveTransactionSession:
    executed_sql: list[str] = []

    def in_transaction(self) -> bool:
        return True

    async def execute(self, statement, params=None):
        raise AssertionError("active transaction guard must refuse before executing SQL")


def _check_status(report, check_name: str) -> str:
    return next(check.status for check in report.checks if check.check_name == check_name)


def _public_methods(cls: type) -> list[str]:
    return [
        name
        for name, value in vars(cls).items()
        if not name.startswith("_") and callable(value)
    ]
