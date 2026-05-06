from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

import sqlalchemy.ext.asyncio as sqlalchemy_async


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "ops" / "collector_live_startup_probe.py"


def test_probe_module_imports_without_db_redis_openai_or_network_connection(monkeypatch) -> None:
    calls: list[str] = []
    module_name = "scripts.ops.collector_live_startup_probe"

    def fail_create_async_engine(*args, **kwargs):
        calls.append("create_async_engine")
        raise AssertionError("import should not create a database engine")

    sys.modules.pop(module_name, None)
    monkeypatch.setattr(sqlalchemy_async, "create_async_engine", fail_create_async_engine)
    try:
        module = importlib.import_module(module_name)
        assert module.REPORT_TYPE == "collector_live_startup_probe_v1"
        assert calls == []
    finally:
        sys.modules.pop(module_name, None)


def test_help_works_without_live_db_redis_openai_or_network() -> None:
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
    assert "--format {json}" in output
    for marker in ("TDLib", "Telegram", "DB", "Redis", "Docker", "systemd", "live ingest"):
        assert marker in output


def test_report_generation_passes_against_current_repo_root() -> None:
    module = importlib.import_module("scripts.ops.collector_live_startup_probe")

    report = module.generate_report()

    assert report["report_type"] == "collector_live_startup_probe_v1"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
