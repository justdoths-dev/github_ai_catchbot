from __future__ import annotations

import importlib
import json
import os
import sys

import pytest
import sqlalchemy.ext.asyncio as sqlalchemy_async


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/catchbot"


def test_smoke_module_imports_without_db_connection(monkeypatch) -> None:
    calls: list[str] = []
    module_name = "scripts.ops.delivery_db_acceptance_smoke"

    def fail_create_async_engine(*args, **kwargs):
        calls.append("create_async_engine")
        raise AssertionError("import should not create a database engine")

    sys.modules.pop(module_name, None)
    monkeypatch.setattr(sqlalchemy_async, "create_async_engine", fail_create_async_engine)
    try:
        importlib.import_module(module_name)
        assert calls == []
    finally:
        sys.modules.pop(module_name, None)


def test_parser_parses_required_options() -> None:
    module = importlib.import_module("scripts.ops.delivery_db_acceptance_smoke")
    parser = module.build_parser()

    args = parser.parse_args(
        [
            "--database-url",
            "postgresql+psycopg://user:secret@localhost:5432/catchbot",
            "--check",
            "delivery-gate",
            "--format",
            "json",
        ]
    )

    assert args.database_url.startswith("postgresql+psycopg://")
    assert args.check == "delivery-gate"
    assert args.format == "json"


@pytest.mark.asyncio
async def test_engine_creation_failure_reports_json_without_raw_database_url(monkeypatch, capsys) -> None:
    module = importlib.import_module("scripts.ops.delivery_db_acceptance_smoke")
    database_url = _fake_database_url()
    password = "super" + "secret"

    def fail_create_async_engine(*args, **kwargs):
        raise RuntimeError(f"could not open {database_url}; password={password}")

    monkeypatch.setattr(module, "create_async_engine", fail_create_async_engine)

    exit_code = await module._amain(["--database-url", database_url, "--format", "json"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 1
    assert payload["report_type"] == "delivery_db_acceptance_smoke_v1"
    assert payload["database_url_redacted"] is True
    assert payload["mutation_safety"] == "select_only"
    assert database_url not in output
    assert password not in output
    assert payload["failures"][0]["check"] == "smoke.unexpected_failure"


@pytest.mark.skipif(
    not os.getenv("GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"),
    reason="set GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL for DB-backed smoke",
)
@pytest.mark.asyncio
async def test_db_backed_smoke_when_database_url_is_explicitly_provided() -> None:
    module = importlib.import_module("scripts.ops.delivery_db_acceptance_smoke")

    report = await module.run_smoke(
        database_url=os.environ["GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"],
        selected_check="schema",
    )

    assert report.report_type == "delivery_db_acceptance_smoke_v1"
    assert report.database_url_redacted is True
