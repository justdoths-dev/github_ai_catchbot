from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_redis_rebuild_retry_replay_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_redis_rebuild_retry_replay_runner.py"
SOURCE_PATH = ROOT / "src/services/maintenance/redis_rebuild_retry_replay_proof.py"

RAW_VALUES = (
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
    "44444444-4444-4444-8444-444444444444",
    "55555555-5555-4555-8555-555555555555",
    "66666666-6666-4666-8666-666666666666",
    "notify:retry-intent:",
    "notify:replay-intent:",
    "12345",
    "https://private.example.invalid",
    "postgresql://",
    "postgresql+psycopg://",
    "redis://",
    "runtime.env",
    "sentinel_secret",
)


def test_runner_emits_sanitized_json_only(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert captured.out.endswith("\n")
    assert "\n" not in captured.out[:-1]
    assert parsed["schema_version"] == "redis_rebuild_retry_replay_proof_v1"
    assert parsed["runner_name"] == "bounded_redis_rebuild_retry_replay_runner"
    assert parsed["status"] == "pass"
    assert parsed["target_exit_states"]["REDIS_REBUILD_CODE_REVIEW_PASS"] is True
    assert parsed["target_exit_states"]["RETRY_DLQ_REPLAY_CODE_REVIEW_PASS"] is True
    assert parsed["side_effect_authority"]["redis_flush_attempted"] is False
    assert parsed["side_effect_authority"]["db_write_attempted"] is False
    assert parsed["side_effect_authority"]["workers_started"] is False
    for raw in RAW_VALUES:
        assert raw not in captured.out


def test_runner_exits_nonzero_with_json_for_argument_error(capsys) -> None:
    exit_code = runner.main(["--unexpected"])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "unsupported_cli_argument"
    assert parsed["side_effect_authority"]["redis_mutation_attempted"] is False
    assert parsed["side_effect_authority"]["runtime_env_read_attempted"] is False


def test_static_runner_and_proof_have_no_forbidden_live_imports_or_calls() -> None:
    forbidden_import_roots = {
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
    }
    forbidden_call_names = {
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
        "send_message",
        "edit_message_text",
    }

    for path in (TOOL_PATH, SOURCE_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
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

        assert imported_roots.isdisjoint(forbidden_import_roots), path
        assert call_names.isdisjoint(forbidden_call_names), path
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")
