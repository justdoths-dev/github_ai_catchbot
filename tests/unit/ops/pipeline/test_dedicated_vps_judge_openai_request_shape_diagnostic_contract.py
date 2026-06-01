from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_judge_openai_request_shape_diagnostic.py"


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_request_shape_diagnostic"
    )


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_report_is_no_network_request_shape_pass() -> None:
    report = _module().generate_report()

    assert report["contract_status"] == "judge_openai_request_shape_diagnostic_passed"
    assert report["request_shape_valid_bucket"] == "one"
    assert report["request_shape_issue_count_bucket"] == "zero"
    assert report["request_shape_issue_buckets"] == []
    assert report["model_bucket"] == "locked_hot_path"
    assert report["reasoning_effort_bucket"] == "low"
    assert report["text_format_json_schema_bucket"] == "one"
    assert report["strict_schema_bucket"] == "one"
    assert report["tools_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert report["live_openai_call_attempted"] is False
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["database_write_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []


def test_invalid_model_report_fails_with_sanitized_bucket() -> None:
    private_model_value = "private-model-value"

    report = _module().generate_report(model=private_model_value)
    rendered = json.dumps(report, sort_keys=True)

    assert report["contract_status"] == "blocked_judge_openai_request_shape_diagnostic_failed"
    assert report["request_shape_valid_bucket"] == "zero"
    assert report["request_shape_issue_buckets"] == ["model.outside_locked_set"]
    assert report["model_bucket"] == "other"
    assert private_model_value not in rendered
    assert report["checks_failed"] == ["request_shape.invalid"]


def test_cli_outputs_json_without_reading_runtime_or_secret_files() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    )

    report = json.loads(result.stdout)
    assert report["contract_status"] == "judge_openai_request_shape_diagnostic_passed"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["database_write_attempted"] is False
    assert report["redis_write_attempted"] is False
