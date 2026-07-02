from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.maintenance.persistent_worker_rollout_recovery import MAX_OPERATOR_EVIDENCE_BYTES
from tools import bounded_persistent_worker_rollout_recovery_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_persistent_worker_rollout_recovery_runner.py"
SOURCE_PATH = ROOT / "src/services/maintenance/persistent_worker_rollout_recovery.py"
RAW_URL = "https://" + "private.example.invalid/operator"
RAW_SECRET = "raw private stderr value"


def test_runner_emits_sanitized_persistent_worker_rollout_recovery_proof(capsys) -> None:
    exit_code = runner.main([])
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["schema_version"] == "persistent_worker_rollout_recovery_proof_v1"
    assert parsed["runner_name"] == "bounded_persistent_worker_rollout_recovery_runner"
    assert parsed["staged_rollout"]["one_service_at_a_time"] is True
    assert parsed["rollback"]["stop_condition_represented"] is True
    assert parsed["authority"]["systemd_command_execution_attempted"] is False
    assert parsed["authority"]["docker_command_execution_attempted"] is False
    assert parsed["authority"]["redis_consume_attempted"] is False
    assert parsed["authority"]["redis_ack_attempted"] is False
    assert parsed["authority"]["redis_xadd_attempted"] is False
    assert parsed["authority"]["redis_group_mutation_attempted"] is False
    assert parsed["function_complete_packet"]["production_complete"] is False
    assert parsed["completion_claims"]["production_rollout_complete"] is False
    assert parsed["raw_values_printed"] is False


def test_runner_requires_allow_flag_before_operator_evidence_read(tmp_path: Path, capsys) -> None:
    evidence_path = tmp_path / "operator.json"
    evidence_path.write_text("{}", encoding="utf-8")

    exit_code = runner.main(["--operator-evidence-json", str(evidence_path)])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "operator_evidence_file_read_not_allowed"
    assert parsed["authority"]["systemd_command_execution_attempted"] is False


def test_runner_reads_operator_evidence_only_as_sanitized_summary(tmp_path: Path, capsys) -> None:
    evidence_path = tmp_path / "operator.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": "operator_evidence_v1",
                "status": "pass",
                "commit": "009323929978a257ec2c9b246ea404588752029f",
                "service_name": "github-ai-catchbot-maintenance.service",
                "url": RAW_URL,
                "stderr": RAW_SECRET,
            }
        ),
        encoding="utf-8",
    )

    exit_code = runner.main(
        [
            "--mode",
            "proof",
            "--operator-evidence-json",
            str(evidence_path),
            "--allow-operator-evidence-read",
        ]
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["mode"] == "proof"
    assert parsed["operator_evidence"]["supplied"] is True
    assert parsed["operator_evidence"]["head_suffix"] == "8752029f"
    assert parsed["operator_evidence"]["buckets"]["service"] == "known_maintenance_worker_service"
    assert parsed["authority"]["operator_evidence_file_read_allowed"] is True
    for forbidden in (
        RAW_URL,
        RAW_SECRET,
        "009323929978a257ec2c9b246ea404588752029f",
        "github-ai-catchbot-maintenance.service",
    ):
        assert forbidden not in output


def test_runner_rejects_non_json_runtime_env_path_and_large_evidence(tmp_path: Path, capsys) -> None:
    text_path = tmp_path / "operator.txt"
    text_path.write_text("{}", encoding="utf-8")
    assert runner.main(["--operator-evidence-json", str(text_path), "--allow-operator-evidence-read"]) == 1
    assert json.loads(capsys.readouterr().out)["reason_code"] == "operator_evidence_file_extension_not_allowed"

    runtime_env_dir = tmp_path / "runtime.env"
    runtime_env_dir.mkdir()
    runtime_env_json = runtime_env_dir / "operator.json"
    runtime_env_json.write_text("{}", encoding="utf-8")
    assert runner.main(["--operator-evidence-json", str(runtime_env_json), "--allow-operator-evidence-read"]) == 1
    assert json.loads(capsys.readouterr().out)["reason_code"] == "operator_evidence_file_not_allowed"

    env_like_json = tmp_path / ".env.json"
    env_like_json.write_text("{}", encoding="utf-8")
    assert runner.main(["--operator-evidence-json", str(env_like_json), "--allow-operator-evidence-read"]) == 1
    assert json.loads(capsys.readouterr().out)["reason_code"] == "operator_evidence_file_not_allowed"

    large_path = tmp_path / "large.json"
    large_path.write_text("{" + '"x":"' + ("a" * MAX_OPERATOR_EVIDENCE_BYTES) + '"}', encoding="utf-8")
    assert runner.main(["--operator-evidence-json", str(large_path), "--allow-operator-evidence-read"]) == 1
    assert json.loads(capsys.readouterr().out)["reason_code"] == "operator_evidence_file_too_large"


def test_static_runner_and_source_have_no_forbidden_live_imports_or_calls() -> None:
    for path in (TOOL_PATH, SOURCE_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported_roots: set[str] = set()
        call_names: set[str] = set()
        names: set[str] = set()
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

        assert {
            "redis",
            "telegram",
            "openai",
            "requests",
            "httpx",
            "aiohttp",
            "subprocess",
            "docker",
        }.isdisjoint(imported_roots)
        assert {
            "systemctl",
            "run_systemd_rollout",
            "LocalUserSystemdAdapter",
            "xreadgroup",
            "xack",
            "xadd",
            "xgroup_create",
            "create_async_engine",
            "run_forever",
        }.isdisjoint(call_names | names)
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")
