from __future__ import annotations

import importlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_restricted_rollout_readiness_smoke_imports_successfully() -> None:
    module = importlib.import_module("scripts.ops.restricted_rollout_readiness_smoke")

    assert module.REPORT_TYPE == "restricted_rollout_readiness_smoke_v1"


def test_cli_entrypoint_exists() -> None:
    module = importlib.import_module("scripts.ops.restricted_rollout_readiness_smoke")

    assert callable(module.main)


def test_report_generation_passes_against_current_repo_root() -> None:
    module = importlib.import_module("scripts.ops.restricted_rollout_readiness_smoke")

    result = module.generate_report(ROOT)

    assert result.exit_code == 0
    assert result.report["checks_failed"] == []
    assert result.report["readiness_summary"] == {
        "delivery_gate_assets_present": True,
        "batch_recovery_assets_present": True,
        "maintenance_assets_present": True,
        "notifier_safety_assets_present": True,
        "rollback_assets_present": True,
        "handoff_assets_present": True,
    }
