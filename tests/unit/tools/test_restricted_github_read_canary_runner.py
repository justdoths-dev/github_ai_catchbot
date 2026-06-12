from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.gh_enricher import restricted_read_canary
from tools import restricted_github_read_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOKEN_VALUE = "sentinel_auth_value"
PRIVATE_KEY_VALUE = "sentinel_private_key_material"


class FakeGitHubClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[tuple] = []

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str):
        self.calls.append(("get_repo", owner, repo, auth_mode))
        return {
            "full_name": "octocat/Hello-World",
            "private": False,
            "default_branch": "main",
            "description": TOKEN_VALUE,
        }

    async def get_default_branch_head(self, owner: str, repo: str, default_branch: str, *, auth_mode: str):
        self.calls.append(("get_default_branch_head", owner, repo, default_branch, auth_mode))
        return {"sha": "abc123", "raw": TOKEN_VALUE}


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _env() -> dict[str, str]:
    return {
        "GITHUB_APP_ID": "123",
        "GITHUB_INSTALLATION_ID": "456",
        "GITHUB_PRIVATE_KEY": PRIVATE_KEY_VALUE,
    }


def test_runner_uses_source_level_canary_module() -> None:
    assert runner.RestrictedGitHubReadCanary is restricted_read_canary.RestrictedGitHubReadCanary
    assert runner.RestrictedGitHubReadCanaryConfig is restricted_read_canary.RestrictedGitHubReadCanaryConfig


def test_cli_runner_returns_zero_on_fake_success_with_sanitized_output() -> None:
    client = FakeGitHubClient()
    result = runner.run(
        _parse_args("--repo", "octocat/Hello-World", "--operator-approved", "--allow-network"),
        env=_env(),
        github_client=client,
    )
    text = runner.render_json(result.report)
    parsed = json.loads(text)

    assert result.exit_code == 0
    assert parsed["ok"] is True
    assert parsed["status"] == "pass"
    assert parsed["repo_full_name"] == "octocat/Hello-World"
    assert parsed["request_count"] == 2
    assert parsed["credential_source_kind"] == "github_app_env"
    assert parsed["observed_repo_visibility"] == "public"
    assert parsed["observed_default_branch"] == "main"
    assert TOKEN_VALUE not in text
    assert PRIVATE_KEY_VALUE not in text
    assert client.calls == [
        ("get_repo", "octocat", "Hello-World", "app_installation"),
        ("get_default_branch_head", "octocat", "Hello-World", "main", "app_installation"),
    ]


def test_cli_runner_blocks_missing_flags_and_credentials_before_client_call() -> None:
    cases = [
        (("--repo", "octocat/Hello-World", "--allow-network"), _env(), "operator_approval_missing"),
        (("--repo", "octocat/Hello-World", "--operator-approved"), _env(), "network_not_allowed"),
        (("--repo", "octocat", "--operator-approved", "--allow-network"), _env(), "invalid_repo_target"),
        (
            ("--repo", "octocat/Hello-World", "--operator-approved", "--allow-network"),
            {},
            "credential_missing",
        ),
    ]
    for argv, env, expected_error in cases:
        client = FakeGitHubClient()

        result = runner.run(_parse_args(*argv), env=env, github_client=client)

        assert result.exit_code == 1
        assert result.report["ok"] is False
        assert result.report["error_code"] == expected_error
        assert result.report["network_attempted"] is False
        assert result.report["request_count"] == 0
        assert client.calls == []


def test_main_prints_json_and_uses_fake_client_without_live_network(monkeypatch, capsys) -> None:
    monkeypatch.setenv("GITHUB_APP_ID", "123")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "456")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", PRIVATE_KEY_VALUE)
    monkeypatch.setattr(runner, "GitHubClient", FakeGitHubClient)

    exit_code = runner.main(["--repo", "octocat/Hello-World", "--operator-approved", "--allow-network"])
    out = capsys.readouterr().out
    parsed = json.loads(out)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["error_code"] is None
    assert TOKEN_VALUE not in out
    assert PRIVATE_KEY_VALUE not in out


def test_main_exits_nonzero_with_json_when_blocked(capsys) -> None:
    exit_code = runner.main(["--repo", "octocat/Hello-World", "--allow-network"])
    out = capsys.readouterr().out
    parsed = json.loads(out)

    assert exit_code == 1
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["network_attempted"] is False


def test_tool_source_does_not_duplicate_http_db_redis_telegram_or_openai_clients() -> None:
    source = (ROOT / "tools/restricted_github_read_canary_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert {"urllib", "requests", "httpx", "aiohttp", "sqlalchemy", "redis", "telegram", "openai"}.isdisjoint(
        imported_roots
    )
    assert "urlopen" not in source
    assert "DATABASE_URL" not in source
