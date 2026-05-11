from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_app_runtime_import_config_preflight_runner.py"


def test_dedicated_vps_app_runtime_import_config_preflight_runner_imports_successfully() -> None:
    module = importlib.import_module(
        "scripts.ops.dedicated_vps_app_runtime_import_config_preflight_runner"
    )

    assert module.SCHEMA_VERSION == "dedicated_vps_app_runtime_import_config_preflight_v1"


def test_cli_entrypoint_exists() -> None:
    module = importlib.import_module(
        "scripts.ops.dedicated_vps_app_runtime_import_config_preflight_runner"
    )

    assert callable(module.main)


def test_no_approval_json_can_be_produced_without_reading_runtime_env(tmp_path: Path) -> None:
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
    assert report["import_surface_attempted"] is False
    assert report["config_surface_attempted"] is False
    assert report["database_connected"] is False
    assert report["redis_connected"] is False
    assert report["app_runtime_started"] is False
    assert report["tdlib_auth_performed"] is False
    assert report["telegram_connected"] is False
    assert report["live_collector_started"] is False
    assert report["notifier_transport_enabled"] is False
    assert report["production_rollout_performed"] is False


def test_module_function_no_approval_requires_no_network_db_or_redis_dependency(tmp_path: Path) -> None:
    module = importlib.import_module(
        "scripts.ops.dedicated_vps_app_runtime_import_config_preflight_runner"
    )

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=tmp_path / "missing-runtime.env",
        approved_app_runtime_import_config_preflight=False,
    )

    assert result.exit_code != 0
    report = result.report
    assert report["contract_status"] == "approval_required"
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["redis_connected"] is False
    assert report["app_runtime_started"] is False
