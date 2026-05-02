from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

import sqlalchemy.ext.asyncio as sqlalchemy_async


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "ops" / "outbox_redis_routing_smoke.py"


def test_smoke_module_imports_without_db_or_redis_connection(monkeypatch) -> None:
    calls: list[str] = []
    module_name = "scripts.ops.outbox_redis_routing_smoke"

    def fail_create_async_engine(*args, **kwargs):
        calls.append("create_async_engine")
        raise AssertionError("import should not create a database engine")

    sys.modules.pop(module_name, None)
    monkeypatch.setattr(sqlalchemy_async, "create_async_engine", fail_create_async_engine)
    try:
        module = importlib.import_module(module_name)
        assert module.REPORT_TYPE == "outbox_redis_routing_smoke_v1"
        assert calls == []
    finally:
        sys.modules.pop(module_name, None)


def test_help_works_without_live_db_or_redis() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    output = f"{result.stdout}\n{result.stderr}"
    assert result.returncode == 0
    assert "usage:" in output
    assert "--database-url" in output
    assert "--redis-url" in output
    assert "--confirm {write}" in output
