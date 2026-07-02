from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_noise_duplicate_suppression_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_noise_duplicate_suppression_runner.py"


def test_runner_emits_sanitized_f9_proof(capsys) -> None:
    exit_code = runner.main([])
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["schema_version"] == "noise_duplicate_suppression_proof_v1"
    assert parsed["gate"] == "F9_NOISE_DUPLICATE_SUPPRESSION"
    assert parsed["gates"]["same_subject_same_material_no_duplicate"] is True
    assert parsed["duplicate_suppression"]["same_subject_same_material_replay_action"] == "suppress_duplicate"
    assert parsed["raw_values_printed"] is False
    assert "11111111-1111-4111-8111-111111111111" not in output
    assert "redacted-db-locator" not in output


def test_execute_requires_operator_approval(capsys) -> None:
    exit_code = runner.main(["--mode", "execute"])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "operator_approval_missing"


def test_static_runner_has_no_forbidden_live_imports_or_calls() -> None:
    tree = ast.parse(TOOL_PATH.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    call_names: set[str] = set()
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

    assert {"redis", "telegram", "openai", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(
        imported_roots
    )
    assert {"systemctl", "docker", "alembic", "run_forever"}.isdisjoint(call_names)
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")
