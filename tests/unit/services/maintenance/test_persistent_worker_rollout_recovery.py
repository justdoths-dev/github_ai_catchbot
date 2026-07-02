from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.maintenance.persistent_worker_rollout_recovery import (
    PersistentWorkerProofRequest,
    build_persistent_worker_rollout_recovery_proof,
    summarize_operator_evidence,
    validate_operator_evidence_path,
)


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/persistent_worker_rollout_recovery.py"
RAW_URL = "https://" + "private.example.invalid/raw"
RAW_SECRET = "private stderr body"
RAW_ID = "11111111-1111-4111-8111-111111111111"


def _request(tmp_path: Path) -> PersistentWorkerProofRequest:
    repo_root = ROOT.resolve()
    python_executable = tmp_path / "venv/bin/python"
    python_executable.parent.mkdir(parents=True, exist_ok=True)
    python_executable.write_text("# python shim\n", encoding="utf-8")
    return PersistentWorkerProofRequest(
        repo_root=repo_root,
        python_executable=python_executable.resolve(),
        runtime_env_file=(tmp_path / "worker.env").resolve(),
        systemd_user_dir=(tmp_path / "user-systemd").resolve(),
    )


def test_proof_represents_one_service_at_a_time_activation_and_rollback_stop(tmp_path: Path) -> None:
    report = build_persistent_worker_rollout_recovery_proof(_request(tmp_path))

    assert report["ok"] is True
    assert report["staged_rollout"]["activation_order"] == ["known_maintenance_worker_service"]
    assert report["staged_rollout"]["one_service_at_a_time"] is True
    assert report["staged_rollout"]["parallel_activation_opened"] is False
    assert report["staged_rollout"]["broad_worker_activation_opened"] is False
    assert report["staged_rollout"]["install_attempted"] is False
    assert report["staged_rollout"]["start_attempted"] is False
    assert report["rollback"]["stop_condition_represented"] is True
    assert report["rollback"]["rollback_reason_code_if_readback_fails"] == "rollback_readback_failed"
    assert report["rollback"]["rollback_attempted"] is False


def test_proof_represents_crash_restart_abandoned_job_and_no_duplicate_side_effects(tmp_path: Path) -> None:
    report = build_persistent_worker_rollout_recovery_proof(_request(tmp_path))

    recovery = report["abandoned_job_recovery"]
    assert report["worker_recovery"]["crash_restart_semantics_represented"] is True
    assert report["worker_recovery"]["systemd_restart_policy"] == "on-failure"
    assert report["worker_recovery"]["restart_sec_bucket"] == "bounded_10s"
    assert recovery["repository_kind"] == "fake_durable_job_repository"
    assert recovery["abandoned_jobs_detected"] == 1
    assert recovery["abandoned_jobs_recovered"] == 1
    assert recovery["first_attempt_side_effect_count"] == 1
    assert recovery["restart_side_effect_count"] == 0
    assert recovery["duplicate_side_effect_prevented_count"] == 1
    assert recovery["no_duplicate_side_effects"] is True
    assert recovery["redis_ack_attempted"] is False


def test_proof_keeps_function_packet_separate_from_production_rollout(tmp_path: Path) -> None:
    report = build_persistent_worker_rollout_recovery_proof(_request(tmp_path))

    packet = report["function_complete_packet"]
    assert packet["consumed"] is True
    assert packet["function_complete_packet_ready"] is True
    assert packet["production_complete"] is False
    assert packet["final_bot_complete"] is False
    assert packet["authority_open"] is True
    assert packet["rollout_open"] is True
    assert packet["production_rollout_open"] is True
    assert report["open_gates"] == {
        "AUTHORITY_OPEN": True,
        "ROLLOUT_OPEN": True,
        "PRODUCTION_ROLLOUT_OPEN": True,
        "FULL_ALWAYS_ON_COLLECTOR_WORKER_OPEN": True,
    }
    assert report["completion_claims"]["production_rollout_complete"] is False
    assert report["completion_claims"]["actual_systemd_activation_complete"] is False


def test_authority_flags_forbid_live_systemd_docker_redis_db_and_runtime_env(tmp_path: Path) -> None:
    report = build_persistent_worker_rollout_recovery_proof(_request(tmp_path))
    authority = report["authority"]

    assert authority["systemd_command_execution_attempted"] is False
    assert authority["docker_command_execution_attempted"] is False
    assert authority["redis_consume_attempted"] is False
    assert authority["redis_ack_attempted"] is False
    assert authority["redis_xadd_attempted"] is False
    assert authority["redis_group_mutation_attempted"] is False
    assert authority["db_write_attempted"] is False
    assert authority["runtime_env_read_attempted"] is False


def test_operator_evidence_summary_omits_raw_values() -> None:
    summary = summarize_operator_evidence(
        {
            "schema_version": "operator_evidence_v1",
            "status": "pass",
            "commit": "009323929978a257ec2c9b246ea404588752029f",
            "service_name": "github-ai-catchbot-maintenance.service",
            "url": RAW_URL,
            "raw_id": RAW_ID,
            "stderr": RAW_SECRET,
        }
    )
    rendered = json.dumps(summary, sort_keys=True)

    assert summary["status"] == "pass"
    assert summary["schema_version"] == "operator_evidence_v1"
    assert summary["head_suffix"] == "8752029f"
    assert summary["buckets"]["service"] == "known_maintenance_worker_service"
    for forbidden in (
        RAW_URL,
        RAW_ID,
        RAW_SECRET,
        "009323929978a257ec2c9b246ea404588752029f",
        "github-ai-catchbot-maintenance.service",
    ):
        assert forbidden not in rendered


def test_operator_evidence_path_gate_rejects_env_names_and_non_json(tmp_path: Path) -> None:
    allowed = tmp_path / "evidence.json"
    allowed.write_text("{}", encoding="utf-8")
    disallowed_text = tmp_path / "evidence.txt"
    disallowed_text.write_text("{}", encoding="utf-8")
    runtime_env_dir = tmp_path / "runtime.env"
    runtime_env_dir.mkdir()
    runtime_env_json = runtime_env_dir / "evidence.json"
    runtime_env_json.write_text("{}", encoding="utf-8")
    env_like_json = tmp_path / ".env.json"
    env_like_json.write_text("{}", encoding="utf-8")

    assert validate_operator_evidence_path(allowed) is None
    assert validate_operator_evidence_path(disallowed_text) == "operator_evidence_file_extension_not_allowed"
    assert validate_operator_evidence_path(runtime_env_json) == "operator_evidence_file_not_allowed"
    assert validate_operator_evidence_path(env_like_json) == "operator_evidence_file_not_allowed"


def test_static_source_has_no_forbidden_live_imports_or_calls() -> None:
    tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"))
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

    assert {"redis", "telegram", "openai", "requests", "httpx", "aiohttp", "subprocess", "docker"}.isdisjoint(
        imported_roots
    )
    assert {
        "run_systemd_rollout",
        "LocalUserSystemdAdapter",
        "systemctl",
        "xreadgroup",
        "xack",
        "xadd",
        "xgroup_create",
        "create_async_engine",
        "run_forever",
    }.isdisjoint(call_names | names)
