from __future__ import annotations

import importlib
import socket
import subprocess
import sys
from pathlib import Path

import sqlalchemy.ext.asyncio as sqlalchemy_async


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "ops" / "collector_restricted_environment_inventory_check.py"


def test_inventory_module_imports_without_db_redis_or_network_connection(monkeypatch) -> None:
    calls: list[str] = []
    module_name = "scripts.ops.collector_restricted_environment_inventory_check"

    def fail_create_async_engine(*args, **kwargs):  # noqa: ANN001
        calls.append("create_async_engine")
        raise AssertionError("import should not create a database engine")

    def fail_socket(*args, **kwargs):  # noqa: ANN001
        calls.append("socket")
        raise AssertionError("import should not create network sockets")

    sys.modules.pop(module_name, None)
    monkeypatch.setattr(sqlalchemy_async, "create_async_engine", fail_create_async_engine)
    monkeypatch.setattr(socket, "socket", fail_socket)
    try:
        module = importlib.import_module(module_name)
        assert module.REPORT_TYPE == "collector_restricted_environment_inventory_v1"
        assert calls == []
    finally:
        sys.modules.pop(module_name, None)


def test_help_works_without_live_env_or_runtime_connections() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    output = f"{result.stdout}\n{result.stderr}"
    normalized_output = " ".join(output.split())
    assert result.returncode == 0
    assert "usage:" in output
    assert "current-env inspects current" in normalized_output
    assert "process env name presence and path metadata only" in normalized_output
    assert "never prints values" in normalized_output


def test_schema_report_generation_passes_against_current_repo_root() -> None:
    module = importlib.import_module("scripts.ops.collector_restricted_environment_inventory_check")

    report = module.generate_report()

    assert report["report_type"] == "collector_restricted_environment_inventory_v1"
    assert report["mode"] == "schema"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["authorization"]["live_ingest_authorized"] is False
    assert report["authorization"]["production_rollout_authorized"] is False
