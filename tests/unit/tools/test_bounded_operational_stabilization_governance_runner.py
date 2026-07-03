from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_operational_stabilization_governance_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_operational_stabilization_governance_runner.py"
SOURCE_PATH = ROOT / "src/services/maintenance/operational_stabilization_governance_proof.py"

RAW_VALUES = (
    "11111111-1111-4111-8111-111111111111",
    "stream-123-0",
    "notify:retry-intent:",
    "notify:replay-intent:",
    "https://" + "private.example.invalid",
    "postgresql" + "://",
    "postgresql+psycopg" + "://",
    "redis" + "://",
    "runtime" + ".env",
    "sk-" + "private",
    "Bearer " + "private",
    "x-" + "ratelimit-reset",
    "x-" + "rate-limit-reset",
    "raw private " + "source text",
    "raw stderr " + "body",
)


def test_runner_emits_sanitized_json_only(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert captured.out.endswith("\n")
    assert "\n" not in captured.out[:-1]
    assert parsed["schema_version"] == "operational_stabilization_governance_proof_v1"
    assert parsed["runner_name"] == "bounded_operational_stabilization_governance_runner"
    assert parsed["status"] == "pass"
    assert parsed["target_exit_states"]["MONITORING_RATE_LIMIT_COST_GUARDS_CODE_REVIEW_PASS"] is True
    assert parsed["target_exit_states"]["BACKUP_RESTORE_RECOVERY_DRILL_CODE_REVIEW_PASS"] is True
    assert parsed["target_exit_states"]["PRODUCTION_ROLLOUT_GOVERNANCE_CODE_REVIEW_PASS"] is True
    assert parsed["target_exit_states"]["PRODUCTION_ROLLOUT_OPEN"] is True
    assert parsed["target_exit_states"]["PRODUCT_COMPLETE_CLOSED"] is False
    assert all(value is False for value in parsed["side_effect_authority"].values())
    for raw in RAW_VALUES:
        assert raw not in captured.out


def test_runner_plan_mode_preserves_code_review_only_decision(capsys) -> None:
    exit_code = runner.main(["--mode", "plan"])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["mode"] == "plan"
    assert parsed["o7_production_rollout_governance"]["release_decision_record"][
        "decision_bucket"
    ] == "code_review_pass_only"
    assert parsed["completion_claims"]["production_rollout_closed"] is False
    assert parsed["completion_claims"]["product_complete_closed"] is False
    assert parsed["completion_claims"]["final_bot_complete"] is False
    assert parsed["completion_claims"]["one_hundred_percent_complete"] is False


def test_runner_exits_nonzero_with_json_for_argument_error(capsys) -> None:
    exit_code = runner.main(["--unexpected"])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "unsupported_cli_argument"
    assert parsed["side_effect_authority"]["db_read_attempted"] is False
    assert parsed["side_effect_authority"]["runtime_env_read_attempted"] is False
    assert parsed["raw_values_printed"] is False


def test_static_runner_and_proof_have_no_forbidden_live_imports_or_calls() -> None:
    for path in (TOOL_PATH, SOURCE_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        source = path.read_text(encoding="utf-8")
        imported_roots: set[str] = set()
        call_names: set[str] = set()
        names: set[str] = set()
        attribute_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    call_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    call_names.add(node.func.attr)
            elif isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.Attribute):
                attribute_names.add(node.attr)

        assert {
            "redis",
            "openai",
            "telegram",
            "docker",
            "systemd",
            "requests",
            "httpx",
            "aiohttp",
            "urllib",
            "subprocess",
        }.isdisjoint(imported_roots), path
        assert {
            "create_async_engine",
            "async_sessionmaker",
            "sessionmaker",
            "from_env",
            "xadd",
            "xack",
            "xgroup_create",
            "xreadgroup",
            "run_forever",
            "systemctl",
            "pg_dump",
            "pg_restore",
            "psql",
            "send_message",
            "edit_message_text",
        }.isdisjoint(call_names | names | attribute_names), path
        for forbidden in (
            "systemctl",
            "pg_dump",
            "pg_restore",
            "psql ",
            "send_message",
            "edit_message_text",
        ):
            assert forbidden not in source
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")
