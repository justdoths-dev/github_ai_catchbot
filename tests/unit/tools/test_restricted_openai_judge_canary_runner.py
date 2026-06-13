from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.judge_openai import restricted_judge_canary
from src.services.judge_openai.restricted_judge_canary import CANDIDATE_GROUP_ID
from tools import restricted_openai_judge_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
API_KEY = "sentinel_openai_cli_api_key_value"
GENERATED_TEXT = "sentinel cli generated korean analysis text"


class FakeOpenAIJudgeClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[dict] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        return _response()


def _response() -> dict:
    return {
        "status": "completed",
        "output_text": json.dumps(
            {
                "judge_schema_version": "judge_output_v1",
                "candidate_group_id": CANDIDATE_GROUP_ID,
                "headline": "Synthetic developer workflow tool",
                "summary_one_line_ko": GENERATED_TEXT,
                "skeptical_take_ko": GENERATED_TEXT,
                "why_it_might_matter_ko": GENERATED_TEXT,
                "comparables": [],
                "scores": {
                    "novelty": 25,
                    "practical_usefulness": 30,
                    "evidence_strength": 20,
                    "hype_penalty": 15,
                    "confidence": 35,
                    "code_quality": 40,
                    "maintenance_signal": None,
                    "specificity": 30,
                    "reproducibility_signal": None,
                },
                "reason_codes": ["synthetic_canary_fixture"],
                "red_flags_ko": [GENERATED_TEXT],
                "evidence_limitations_ko": [GENERATED_TEXT],
                "recommended_action_ko": GENERATED_TEXT,
                "freshness_note_ko": GENERATED_TEXT,
                "model_proposed_verdict": "later",
                "model_confidence_band": "medium",
            }
        ),
    }


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_canary_module() -> None:
    assert runner.RestrictedOpenAIJudgeCanaryConfig is restricted_judge_canary.RestrictedOpenAIJudgeCanaryConfig
    assert runner.run_restricted_openai_judge_canary is restricted_judge_canary.run_restricted_openai_judge_canary


def test_main_with_no_flags_returns_json_and_nonzero_exit(capsys) -> None:
    exit_code = runner.main([])
    out = capsys.readouterr().out
    parsed = json.loads(out)

    assert exit_code == 1
    assert parsed["canary_name"] == "restricted_openai_judge_canary"
    assert parsed["mode"] == "restricted_live_judge"
    assert parsed["model"] == "gpt-5.4-mini"
    assert parsed["reasoning_effort"] == "low"
    assert parsed["network_attempted"] is False
    assert parsed["request_count"] == 0
    assert parsed["max_requests"] == 1
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"


def test_approval_and_network_missing_api_key_returns_credential_missing() -> None:
    client = FakeOpenAIJudgeClient()

    result = runner.run(
        _parse_args("--operator-approved", "--allow-network"),
        env={},
        client=client,
    )

    assert result.exit_code == 1
    assert result.report["status"] == "blocked"
    assert result.report["error_code"] == "credential_missing"
    assert result.report["network_attempted"] is False
    assert result.report["request_count"] == 0
    assert client.calls == []


def test_cli_output_does_not_contain_api_key_when_env_is_configured() -> None:
    client = FakeOpenAIJudgeClient()
    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-network",
            "--openai-api-key-env",
            "CUSTOM_OPENAI_API_KEY",
        ),
        env={"CUSTOM_OPENAI_API_KEY": API_KEY},
        client=client,
    )
    text = runner.render_json(result.report)

    assert result.exit_code == 0
    assert result.report["status"] == "pass"
    assert API_KEY not in text
    assert GENERATED_TEXT not in text
    assert client.calls and client.calls[0]["model"] == "gpt-5.4-mini"


def test_cli_success_path_uses_fake_client_without_live_network() -> None:
    client = FakeOpenAIJudgeClient()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-network",
            "--model",
            "gpt-5.4-mini",
            "--reasoning-effort",
            "low",
            "--fixture-profile",
            "github_primary_minimal",
            "--max-requests",
            "1",
            "--max-output-tokens",
            "900",
            "--max-input-chars",
            "12000",
        ),
        env={"OPENAI_API_KEY": API_KEY},
        client=client,
    )
    parsed = json.loads(runner.render_json(result.report))

    assert result.exit_code == 0
    assert parsed["ok"] is True
    assert parsed["schema_valid"] is True
    assert parsed["network_attempted"] is True
    assert parsed["request_count"] == 1
    assert len(client.calls) == 1


def test_tool_source_does_not_import_db_redis_telegram_github_x_or_web_clients_directly() -> None:
    source = (ROOT / "tools/restricted_openai_judge_canary_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)

    assert {
        "urllib",
        "requests",
        "httpx",
        "aiohttp",
        "sqlalchemy",
        "redis",
        "telegram",
        "openai",
    }.isdisjoint(imported_roots)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "DATABASE_URL" not in source
    assert "REDIS_URL" not in source
    assert "TELEGRAM" not in source
    assert "GITHUB_" not in source
    assert "X_BEARER" not in source
    assert "urlopen" not in source


def test_tool_source_only_writes_sanitized_report() -> None:
    source = (ROOT / "tools/restricted_openai_judge_canary_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    assert "print(" not in source
    assert "developer_prompt" not in source
    assert "user_context" not in source
    assert "raw_request" not in source
    assert "raw_response" not in source
    assert "output_text" not in source
    assert any(
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "write"
        and isinstance(call.func.value, ast.Attribute)
        and call.func.value.attr == "stdout"
        for call in calls
    )
    assert "render_json(result.report)" in source
