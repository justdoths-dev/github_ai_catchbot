from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan.py"
)
RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan.md"
)

OUTPUT_SAFETY_BOOLEAN_KEYS = (
    "fix_executed",
    "runtime_env_read",
    "runtime_env_modified",
    "runtime_env_values_printed",
    "secret_values_printed",
    "auth_wrapper_executed",
    "tdlib_auth_attempted",
    "tdlib_auth_completed",
    "telegram_connected",
    "session_state_created_or_reused",
    "manual_intervention_required",
    "telegram_login_code_or_2fa_requested",
    "collector_main_used",
    "collector_service_used",
    "collector_runtime_used",
    "live_collector_started",
    "app_runtime_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "database_connected",
    "redis_connected",
    "alembic_run",
    "docker_or_systemd_changed",
    "source_build_attempted",
    "package_manager_mutation_attempted",
)

FORBIDDEN_OUTPUT_PATTERNS = {
    "telegram_bot_token": re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b"),
    "telegram_api_hash_assignment": re.compile(
        r"\bTELEGRAM_API_HASH\b[\"']?\s*[:=]\s*[\"']?[0-9a-fA-F]{32}\b"
    ),
    "telegram_phone_assignment": re.compile(
        r"\bTELEGRAM_PHONE_NUMBER\b[\"']?\s*[:=]\s*[\"']?\+?\d[\d\s().-]{6,}"
    ),
    "telegram_login_code_assignment": re.compile(
        r"\b(?:TELEGRAM_LOGIN_CODE|LOGIN_CODE|AUTH_CODE)\b[\"']?\s*[:=]\s*[\"']?\S+",
        re.IGNORECASE,
    ),
    "two_factor_or_password_assignment": re.compile(
        r"\b(?:TELEGRAM_2FA_PASSWORD|TWO_FACTOR_PASSWORD|2FA_PASSWORD|PASSWORD)"
        r"\b[\"']?\s*[:=]\s*[\"']?\S+",
        re.IGNORECASE,
    ),
    "postgresql_url": re.compile(r"\bpostgresql(?:\+psycopg)?://", re.IGNORECASE),
    "redis_url": re.compile(r"\bredis://", re.IGNORECASE),
    "private_invite_link": re.compile(
        r"https?://(?:t|telegram)\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+",
        re.IGNORECASE,
    ),
}


def _module():
    from scripts.ops import dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan as module

    return module


def _diagnostic(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "schema_version": (
            "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan_v1"
        ),
        "contract_name": (
            "dedicated_vps_tdlib_auth_runtime_env_invalid_redacted_diagnostic_or_fix_plan"
        ),
        "contract_status": "runtime_env_invalid_diagnostic_ready",
        "recommended_next_slice": "tdlib_auth_runtime_env_operator_fix_plan",
        "runtime_env_inspection": {
            "runtime_env_read": True,
            "runtime_env_values_printed": False,
            "secret_values_printed": False,
            "raw_values_in_output": False,
            "duplicate_key_names": [],
            "malformed_line_count": 0,
        },
        "redacted_key_checks": [
            {
                "key": "TELEGRAM_API_HASH",
                "required": True,
                "present": False,
                "empty": None,
                "value_class": "absent",
                "format_status": "invalid",
                "issue_code": "missing_required_key",
            },
            {
                "key": "TELEGRAM_API_ID",
                "required": True,
                "present": True,
                "empty": False,
                "value_class": "invalid_format",
                "format_status": "invalid",
                "issue_code": "invalid_integer",
            },
        ],
        "redacted_fix_plan": [
            {
                "action_id": "set_missing_key.telegram_api_hash",
                "action_type": "set_missing_key",
                "key": "TELEGRAM_API_HASH",
                "reason": "missing_required_key",
                "value_required_from_operator": True,
                "value_to_use": None,
                "future_operator_command_not_run": None,
            },
            {
                "action_id": "replace_invalid_value.telegram_api_id",
                "action_type": "replace_invalid_value",
                "key": "TELEGRAM_API_ID",
                "reason": "invalid_integer",
                "value_required_from_operator": True,
                "value_to_use": None,
                "future_operator_command_not_run": None,
            },
        ],
        "config_contract": {
            "secret_file_alternatives": {
                "TELEGRAM_API_HASH": "TELEGRAM_API_HASH_FILE",
                "TELEGRAM_2FA_PASSWORD": "TELEGRAM_2FA_PASSWORD_FILE",
                "TDLIB_DB_ENCRYPTION_KEY": "TDLIB_DB_ENCRYPTION_KEY_FILE",
            }
        },
        "runtime_env_values_printed": False,
        "secret_values_printed": False,
        "raw_values_in_output": False,
        "boundary_check": "pass",
    }
    data.update(overrides)
    return data


def _write_json(tmp_path: Path, data: dict[str, object], name: str = "diagnostic.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _report(path: Path | None = None) -> dict[str, object]:
    return _module().generate_report(diagnostic_json_path=path)


def _rendered(report: dict[str, object]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True)


def test_missing_diagnostic_path_reports_missing_and_reads_no_runtime_env() -> None:
    report = _module().generate_report()

    assert report["contract_status"] == "diagnostic_json_missing"
    assert report["recommended_next_slice"] == "defer_manual_review"
    assert report["diagnostic_input"]["diagnostic_json_path_provided"] is False
    assert report["diagnostic_input"]["diagnostic_json_read"] is False
    assert report["runtime_env_target"]["runtime_env_read"] is False
    assert report["runtime_env_read"] is False


def test_missing_diagnostic_file_reports_missing(tmp_path: Path) -> None:
    report = _report(tmp_path / "missing-diagnostic.json")

    assert report["contract_status"] == "diagnostic_json_missing"
    assert report["recommended_next_slice"] == "defer_manual_review"
    assert report["diagnostic_input"]["diagnostic_json_path_provided"] is True
    assert report["diagnostic_input"]["diagnostic_json_read"] is False


def test_unsafe_runtime_env_values_printed_is_rejected(tmp_path: Path) -> None:
    diagnostic = _diagnostic(runtime_env_values_printed=True)
    report = _report(_write_json(tmp_path, diagnostic))

    assert report["contract_status"] == "diagnostic_json_unsafe"
    assert report["recommended_next_slice"] == "defer_manual_review"
    assert "runtime_env_values_printed_true" in report["unsafe_diagnostic_reasons"]


def test_unsafe_secret_like_patterns_are_rejected(tmp_path: Path) -> None:
    unsafe = [
        {"leak": "postgresql" + "://user:pass@host/db"},
        {"leak": "redis" + "://:pass@host:6379/0"},
        {"leak": "TELEGRAM_API_HASH" + "=" + ("a" * 32)},
        {"leak": "TELEGRAM_PHONE_NUMBER" + "=+15555550123"},
        {"leak": "LOGIN_CODE" + "=12345"},
        {"leak": "PASSWORD" + "=secret-value"},
    ]

    for index, payload in enumerate(unsafe):
        diagnostic = _diagnostic(extra=payload)
        report = _report(_write_json(tmp_path, diagnostic, f"unsafe-{index}.json"))
        assert report["contract_status"] == "diagnostic_json_unsafe", payload
        assert report["recommended_next_slice"] == "defer_manual_review"


def test_ready_diagnostic_maps_set_and_replace_actions_to_operator_fix_plan(tmp_path: Path) -> None:
    report = _report(_write_json(tmp_path, _diagnostic()))

    assert report["contract_status"] == "runtime_env_operator_fix_plan_ready"
    assert report["recommended_next_slice"] == "tdlib_auth_runtime_env_operator_fix_execution"
    assert report["diagnostic_input"]["diagnostic_contract_status"] == (
        "runtime_env_invalid_diagnostic_ready"
    )
    actions = report["selected_plan"]["actions"]
    assert [action["action_type"] for action in actions] == [
        "set_missing_key",
        "replace_invalid_value",
    ]


def test_fix_plan_actions_contain_keys_reasons_and_no_values(tmp_path: Path) -> None:
    report = _report(_write_json(tmp_path, _diagnostic()))
    rendered = _rendered(report)

    for action in report["selected_plan"]["actions"]:
        assert action["key"] in {"TELEGRAM_API_HASH", "TELEGRAM_API_ID"}
        assert action["reason"] in {"missing_required_key", "invalid_integer"}
        assert action["value_to_use"] is None
        assert action["value_required_from_operator"] is True
        assert action["permitted_value_source"] == "operator_private_input_only"
        assert "NOT RUN / FUTURE SLICE ONLY" in action["future_operator_instruction_not_run"]

    assert "secret-value" not in rendered
    assert "operator-provided private value" in rendered


def test_duplicate_key_action_maps_without_operator_value(tmp_path: Path) -> None:
    diagnostic = _diagnostic(
        runtime_env_inspection={
            "runtime_env_read": True,
            "runtime_env_values_printed": False,
            "secret_values_printed": False,
            "raw_values_in_output": False,
            "duplicate_key_names": ["TELEGRAM_API_ID"],
            "malformed_line_count": 0,
        },
        redacted_key_checks=[],
        redacted_fix_plan=[
            {
                "action_id": "remove_duplicate_key.telegram_api_id",
                "action_type": "remove_duplicate_key",
                "key": "TELEGRAM_API_ID",
                "reason": "duplicate_key_name",
                "value_required_from_operator": False,
                "value_to_use": None,
                "future_operator_command_not_run": None,
            }
        ],
    )

    report = _report(_write_json(tmp_path, diagnostic))
    action = report["selected_plan"]["actions"][0]

    assert action["action_type"] == "remove_duplicate_key"
    assert action["key"] == "TELEGRAM_API_ID"
    assert action["value_required_from_operator"] is False
    assert action["permitted_value_source"] == "not_applicable"
    assert report["issue_summary"]["duplicate_key_names"] == ["TELEGRAM_API_ID"]


def test_manual_review_action_remains_manual_review_and_does_not_authorize_auth_retry(
    tmp_path: Path,
) -> None:
    diagnostic = _diagnostic(
        redacted_key_checks=[],
        redacted_fix_plan=[
            {
                "action_id": "manual_review.malformed_lines",
                "action_type": "manual_review",
                "key": None,
                "reason": "malformed_line_count",
                "value_required_from_operator": False,
                "value_to_use": None,
                "future_operator_command_not_run": None,
            }
        ],
    )

    report = _report(_write_json(tmp_path, diagnostic))
    action = report["selected_plan"]["actions"][0]

    assert report["contract_status"] == "runtime_env_operator_fix_plan_ready"
    assert report["recommended_next_slice"] == "tdlib_auth_runtime_env_operator_fix_execution"
    assert action["action_type"] == "manual_review"
    assert action["permitted_value_source"] == "not_applicable"
    assert report["tdlib_auth_attempted"] is False
    assert report["tdlib_auth_completed"] is False


def test_shape_already_valid_recommends_auth_rerun_later_with_safety_false(tmp_path: Path) -> None:
    diagnostic = _diagnostic(
        contract_status="runtime_env_shape_appears_valid",
        recommended_next_slice="tdlib_auth_operator_execution_rerun_after_fix",
        redacted_key_checks=[],
        redacted_fix_plan=[],
    )

    report = _report(_write_json(tmp_path, diagnostic))

    assert report["contract_status"] == "runtime_env_shape_already_valid"
    assert report["recommended_next_slice"] == "tdlib_auth_operator_execution_rerun_after_fix"
    assert report["selected_plan"]["actions"] == []
    assert report["issue_summary"]["manual_review_required"] is False
    for key in OUTPUT_SAFETY_BOOLEAN_KEYS:
        assert report[key] is False


def test_diagnostic_not_ready_defers_manual_review(tmp_path: Path) -> None:
    diagnostic = _diagnostic(
        contract_status="runtime_env_invalid_diagnostic_inconclusive",
        recommended_next_slice="defer_manual_review",
        redacted_fix_plan=[],
    )

    report = _report(_write_json(tmp_path, diagnostic))

    assert report["contract_status"] == "diagnostic_not_ready"
    assert report["recommended_next_slice"] == "defer_manual_review"
    assert report["issue_summary"]["manual_review_required"] is True


def test_all_safety_booleans_exist_and_remain_false(tmp_path: Path) -> None:
    report = _report(_write_json(tmp_path, _diagnostic()))

    assert report["boundary_check"] == "pass"
    for key in OUTPUT_SAFETY_BOOLEAN_KEYS:
        assert key in report
        assert report[key] is False


def test_source_runbook_and_tests_do_not_include_forbidden_commands() -> None:
    combined = "\n".join(
        [
            SCRIPT.read_text(encoding="utf-8"),
            RUNBOOK.read_text(encoding="utf-8"),
            Path(__file__).read_text(encoding="utf-8"),
        ]
    )

    forbidden = [
        "cat " + "/etc/github-ai-catchbot/runtime.env",
        "echo " + "KEY=value",
        "sed" + " -i",
        "dedicated_vps_tdlib_" + "auth_operator_execution_wrapper.py",
        "--approved-tdlib-" + "auth-operator-execution",
    ]
    for phrase in forbidden:
        assert phrase not in combined, phrase


def test_source_and_tests_do_not_import_forbidden_runtime_modules() -> None:
    imported: list[str] = []
    for path in (SCRIPT, Path(__file__)):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)

    forbidden_fragments = (
        "collector_telegram.main",
        "collector_telegram.service",
        "collector_telegram.runtime",
        "notifier",
        "database",
        "redis",
        "alembic",
        "docker",
        "systemd",
    )
    assert not [
        name
        for name in imported
        if any(fragment in name.lower() for fragment in forbidden_fragments)
    ]


def test_cli_emits_valid_json(tmp_path: Path) -> None:
    diagnostic_path = _write_json(tmp_path, _diagnostic())
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--diagnostic-json",
            str(diagnostic_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["schema_version"] == "dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan_v1"
    assert data["contract_name"] == "dedicated_vps_tdlib_auth_runtime_env_operator_fix_plan"
    assert data["contract_status"] == "runtime_env_operator_fix_plan_ready"
    assert data["recommended_next_slice"] == "tdlib_auth_runtime_env_operator_fix_execution"


def test_output_contains_no_secret_like_patterns(tmp_path: Path) -> None:
    diagnostic = _diagnostic()
    report = _report(_write_json(tmp_path, diagnostic))
    rendered = _rendered(report)

    for name, pattern in FORBIDDEN_OUTPUT_PATTERNS.items():
        assert pattern.search(rendered) is None, name
    assert "echo " not in rendered
    assert "sed" + " -i" not in rendered
