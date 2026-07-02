from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.policy_engine.noise_duplicate_suppression import (
    build_noise_duplicate_suppression_proof,
    render_sanitized_json,
)
from tools import bounded_function_complete_packet_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_function_complete_packet_runner.py"
F9_PROOF_PATH = ROOT / "src/services/policy_engine/noise_duplicate_suppression.py"
F10_PACKET_PATH = ROOT / "src/services/policy_engine/function_complete_packet.py"
RAW_URL = "https://" + "private.example.invalid/evidence"
RAW_SECRET = "private stderr value"


def test_runner_consumes_shared_f9_service_output_and_emits_packet_ready(capsys) -> None:
    exit_code = runner.main([])
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["schema_version"] == "function_complete_packet_v1"
    assert parsed["packet_status"] == "FUNCTION_COMPLETE_PACKET_READY"
    assert parsed["F9"]["consumed"] is True
    assert parsed["CODE_CLOSED"]["closed_gate_count"] == 9
    assert parsed["AUTHORITY_OPEN"]["open"] is True
    assert parsed["ROLLOUT_OPEN"]["open"] is True
    assert parsed["PRODUCTION_ROLLOUT_OPEN"]["open"] is True
    assert parsed["completion_claims"]["final_bot_complete"] is False
    assert parsed["raw_values_printed"] is False


def test_runner_requires_allow_flag_before_reading_f9_proof_file(tmp_path, capsys) -> None:
    proof_path = tmp_path / "f9-proof.json"
    proof_path.write_text(render_sanitized_json(build_noise_duplicate_suppression_proof()), encoding="utf-8")

    exit_code = runner.main(["--f9-proof-json", str(proof_path)])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "f9_proof_file_read_not_allowed"


def test_runner_consumes_f9_proof_file_and_sanitizes_origin_vps_evidence(tmp_path, capsys) -> None:
    proof_path = tmp_path / "f9-proof.json"
    proof_path.write_text(render_sanitized_json(build_noise_duplicate_suppression_proof()), encoding="utf-8")
    origin_path = tmp_path / "origin.json"
    origin_path.write_text(
        json.dumps(
            {
                "schema_version": "origin_readback_v1",
                "status": "pass",
                "head": "ba2c13d75c83ca7004d0e24938fc978e5221225d",
                "url": RAW_URL,
            }
        ),
        encoding="utf-8",
    )
    vps_path = tmp_path / "vps.json"
    vps_path.write_text(
        json.dumps(
            {
                "schema_version": "vps_readback_v1",
                "status": "pass",
                "commit": "ba2c13d75c83ca7004d0e24938fc978e5221225d",
                "stderr": RAW_SECRET,
            }
        ),
        encoding="utf-8",
    )

    exit_code = runner.main(
        [
            "--f9-proof-json",
            str(proof_path),
            "--allow-f9-proof-file-read",
            "--origin-evidence-json",
            str(origin_path),
            "--allow-origin-evidence-file-read",
            "--vps-evidence-json",
            str(vps_path),
            "--allow-vps-evidence-file-read",
        ]
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["ORIGIN"]["supplied"] is True
    assert parsed["ORIGIN"]["head_suffix"] == "5221225d"
    assert parsed["VPS"]["supplied"] is True
    assert parsed["VPS"]["head_suffix"] == "5221225d"
    for forbidden in (RAW_URL, RAW_SECRET, "ba2c13d75c83ca7004d0e24938fc978e5221225d"):
        assert forbidden not in output


def test_static_runner_and_modules_have_no_forbidden_live_imports_or_calls() -> None:
    for path in (TOOL_PATH, F9_PROOF_PATH, F10_PACKET_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported_roots: set[str] = set()
        imported_modules: set[str] = set()
        call_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    call_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    call_names.add(node.func.attr)

        assert {"redis", "telegram", "openai", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(
            imported_roots
        )
        assert not any(
            forbidden in module
            for module in imported_modules
            for forbidden in (
                "notifier_telegram",
                "judge_openai",
                "collector_telegram",
                "gh_enricher",
                "x_enricher",
                "web_enricher",
                "evidence_assembler",
            )
        )
        assert {"systemctl", "docker", "alembic", "run_forever"}.isdisjoint(call_names)
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")
