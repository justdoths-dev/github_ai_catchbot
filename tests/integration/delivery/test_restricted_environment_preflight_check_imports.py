from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_restricted_environment_preflight_check_imports_successfully() -> None:
    module = importlib.import_module("scripts.ops.restricted_environment_preflight_check")

    assert module.REPORT_TYPE == "restricted_environment_preflight_check_v1"


def test_cli_entrypoint_exists() -> None:
    module = importlib.import_module("scripts.ops.restricted_environment_preflight_check")

    assert callable(module.main)


def test_report_generation_passes_against_current_repo_root() -> None:
    module = importlib.import_module("scripts.ops.restricted_environment_preflight_check")

    result = module.generate_report(ROOT)

    assert result.exit_code == 0
    assert result.report["checks_failed"] == []


def test_cli_supports_json_format() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "ops" / "restricted_environment_preflight_check.py"),
            "--format",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    report = json.loads(completed.stdout)

    assert report["report_type"] == "restricted_environment_preflight_check_v1"
    assert report["checks_failed"] == []
