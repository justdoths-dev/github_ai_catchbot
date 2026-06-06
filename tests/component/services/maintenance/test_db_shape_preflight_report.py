from __future__ import annotations

import json
import re
from dataclasses import asdict
from pathlib import Path

import pytest

from services.maintenance.db_shape_preflight import (
    REQUIRED_ENUM_LABELS,
    REQUIRED_TABLE_COLUMNS,
    DbShapeSnapshot,
    SqlAlchemyDbShapeIntrospectionRepository,
    run_db_shape_preflight,
)


ROOT = Path(__file__).resolve().parents[4]
FORBIDDEN_SQL_PATTERN = re.compile(r"\b(insert|update|delete|alter|create|drop|truncate)\b", re.IGNORECASE)


@pytest.mark.asyncio
async def test_db_shape_preflight_report_is_deterministic_json_with_fake_repository() -> None:
    repository = FakeDbShapeRepository(
        DbShapeSnapshot(
            table_columns={table: set(columns) for table, columns in REQUIRED_TABLE_COLUMNS.items()},
            enum_labels={enum_name: set(labels) for enum_name, labels in REQUIRED_ENUM_LABELS.items()},
            read_only_guard_available=True,
            warnings=("optional_index_coverage_not_checked",),
        )
    )

    first_report = await run_db_shape_preflight(repository)
    second_report = await run_db_shape_preflight(repository)
    first_payload = json.dumps(asdict(first_report), ensure_ascii=False, sort_keys=True, default=str)
    second_payload = json.dumps(asdict(second_report), ensure_ascii=False, sort_keys=True, default=str)

    assert first_report.status == "pass"
    assert first_report.warnings == ["optional_index_coverage_not_checked"]
    assert first_payload == second_payload
    assert "DATABASE_URL" not in first_payload
    assert "REDIS_URL" not in first_payload
    assert "TELEGRAM_BOT_TOKEN" not in first_payload
    assert "OPENAI_API_KEY" not in first_payload
    assert repository.load_count == 2


@pytest.mark.asyncio
async def test_sqlalchemy_adapter_uses_read_only_metadata_sql_with_fake_session() -> None:
    session = FakeMetadataSession()
    repository = SqlAlchemyDbShapeIntrospectionRepository(session)

    report = await run_db_shape_preflight(repository)

    assert report.status == "pass"
    assert session.executed_sql[0] == "BEGIN READ ONLY"
    assert session.executed_sql[1] == "SET LOCAL statement_timeout = '5s'"
    assert session.executed_sql[-1] == "ROLLBACK"
    assert any("information_schema.tables" in sql for sql in session.executed_sql)
    assert any("information_schema.columns" in sql for sql in session.executed_sql)
    assert any("pg_catalog.pg_enum" in sql for sql in session.executed_sql)
    assert all(FORBIDDEN_SQL_PATTERN.search(sql) is None for sql in session.executed_sql)


def test_db_shape_preflight_module_does_not_reference_live_service_paths() -> None:
    module_text = (ROOT / "src" / "services" / "maintenance" / "db_shape_preflight.py").read_text(encoding="utf-8")

    assert "redis" not in module_text.lower()
    assert "import telegram" not in module_text.lower()
    assert "telegram." not in module_text.lower()
    assert "telegrambot" not in module_text.lower()
    assert "openai" not in module_text.lower()
    assert "docker" not in module_text.lower()
    assert "systemd" not in module_text.lower()
    assert "alembic" not in module_text.lower()


class FakeDbShapeRepository:
    def __init__(self, snapshot: DbShapeSnapshot) -> None:
        self.snapshot = snapshot
        self.load_count = 0

    async def load_db_shape_snapshot(self) -> DbShapeSnapshot:
        self.load_count += 1
        return self.snapshot


class FakeMetadataSession:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def in_transaction(self) -> bool:
        return False

    async def execute(self, statement, params=None):
        del params
        sql = " ".join(str(statement).split())
        self.executed_sql.append(sql)
        if sql == "BEGIN READ ONLY" or sql == "SET LOCAL statement_timeout = '5s'" or sql == "ROLLBACK":
            return FakeResult([])
        if "information_schema.tables" in sql:
            return FakeResult([{"table_name": table_name} for table_name in REQUIRED_TABLE_COLUMNS])
        if "information_schema.columns" in sql:
            return FakeResult(
                [
                    {"table_name": table_name, "column_name": column_name}
                    for table_name, columns in REQUIRED_TABLE_COLUMNS.items()
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
