from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_runtime_environment_consumer_preflight_runner.py"


def test_dedicated_vps_runtime_environment_consumer_preflight_runner_imports_successfully() -> None:
    module = importlib.import_module("scripts.ops.dedicated_vps_runtime_environment_consumer_preflight_runner")

    assert module.REPORT_TYPE == "dedicated_vps_runtime_environment_consumer_preflight_result_v1"


def test_cli_entrypoint_exists() -> None:
    module = importlib.import_module("scripts.ops.dedicated_vps_runtime_environment_consumer_preflight_runner")

    assert callable(module.main)


def test_no_approval_json_can_be_produced_from_current_repo_without_reading_runtime_env(tmp_path: Path) -> None:
    missing_runtime_env = tmp_path / "runtime.env"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(missing_runtime_env),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "approval_required"
    assert report["runtime_env_read"] is False
    assert report["process_env_inspected"] is False
    assert report["database_connected"] is False
    assert report["redis_connected"] is False
