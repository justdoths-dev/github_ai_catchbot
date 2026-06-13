from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.x_enricher import restricted_read_canary
from src.services.x_enricher.restricted_read_canary import RestrictedXReadHttpResponse
from tools import restricted_x_read_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
POST_ID = "1881234567890123456"
TOKEN_VALUE = "sentinel_x_cli_bearer_token"
SECRET_TEXT = "sentinel_cli_post_text"
SECRET_AUTHOR = "sentinel_cli_author_identity"
SECRET_MEDIA_URL = "https://media.example/sentinel-cli.jpg"


class FakeXReadClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[tuple[str, str, str, int]] = []

    async def get_post_by_id(
        self,
        *,
        base_url: str,
        bearer_token: str,
        post_id: str,
        timeout_ms: int,
    ) -> RestrictedXReadHttpResponse:
        self.calls.append((base_url, bearer_token, post_id, timeout_ms))
        return RestrictedXReadHttpResponse(
            status_code=200,
            content_type="application/json",
            payload={
                "data": [{"id": POST_ID, "text": SECRET_TEXT, "author_id": "42"}],
                "includes": {
                    "users": [{"id": "42", "username": SECRET_AUTHOR, "name": SECRET_AUTHOR}],
                    "media": [{"media_key": "m1", "url": SECRET_MEDIA_URL}],
                },
            },
        )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _env() -> dict[str, str]:
    return {"X_BEARER_TOKEN": TOKEN_VALUE}


def test_runner_uses_source_level_canary_module() -> None:
    assert runner.RestrictedXReadCanaryConfig is restricted_read_canary.RestrictedXReadCanaryConfig
    assert runner.RestrictedXReadHttpClient is restricted_read_canary.RestrictedXReadHttpClient
    assert runner.run_restricted_x_read_canary is restricted_read_canary.run_restricted_x_read_canary


def test_main_with_no_flags_returns_json_and_nonzero_exit(capsys) -> None:
    exit_code = runner.main([])
    out = capsys.readouterr().out
    parsed = json.loads(out)

    assert exit_code == 1
    assert parsed["canary_name"] == "restricted_x_read_canary"
    assert parsed["mode"] == "restricted_live_read"
    assert parsed["post_id"] == ""
    assert parsed["network_attempted"] is False
    assert parsed["request_count"] == 0
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"


def test_cli_runner_blocks_missing_token_before_client_call() -> None:
    client = FakeXReadClient()

    result = runner.run(
        _parse_args("--post-id", POST_ID, "--operator-approved", "--allow-network"),
        env={},
        client=client,
    )

    assert result.exit_code == 1
    assert result.report["status"] == "blocked"
    assert result.report["error_code"] == "credential_missing"
    assert result.report["network_attempted"] is False
    assert result.report["request_count"] == 0
    assert client.calls == []


def test_cli_runner_returns_zero_on_fake_success_with_sanitized_output() -> None:
    client = FakeXReadClient()

    result = runner.run(
        _parse_args(
            "--post-id",
            POST_ID,
            "--operator-approved",
            "--allow-network",
            "--max-requests",
            "3",
            "--timeout-ms",
            "8000",
        ),
        env=_env(),
        client=client,
    )
    text = runner.render_json(result.report)
    parsed = json.loads(text)

    assert result.exit_code == 0
    assert parsed["ok"] is True
    assert parsed["status"] == "pass"
    assert parsed["post_id"] == POST_ID
    assert parsed["network_attempted"] is True
    assert parsed["request_count"] == 1
    assert parsed["status_code_class"] == "2xx"
    assert parsed["content_type"] == "application/json"
    assert TOKEN_VALUE not in text
    assert SECRET_TEXT not in text
    assert SECRET_AUTHOR not in text
    assert SECRET_MEDIA_URL not in text
    assert client.calls == [("https://api.x.com", TOKEN_VALUE, POST_ID, 8000)]


def test_cli_output_does_not_contain_token_when_token_env_is_configured() -> None:
    client = FakeXReadClient()
    env = {"CUSTOM_X_TOKEN_ENV": TOKEN_VALUE}

    result = runner.run(
        _parse_args(
            "--post-id",
            POST_ID,
            "--operator-approved",
            "--allow-network",
            "--x-bearer-token-env",
            "CUSTOM_X_TOKEN_ENV",
            "--x-base-url",
            "https://api.twitter.com",
        ),
        env=env,
        client=client,
    )
    text = runner.render_json(result.report)

    assert result.exit_code == 0
    assert TOKEN_VALUE not in text
    assert client.calls == [("https://api.twitter.com", TOKEN_VALUE, POST_ID, 8000)]


def test_tool_source_does_not_duplicate_db_redis_telegram_openai_or_github_clients() -> None:
    source = (ROOT / "tools/restricted_x_read_canary_runner.py").read_text(encoding="utf-8")
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
    assert "GITHUB" not in source
    assert "DATABASE_URL" not in source
    assert "REDIS_URL" not in source
    assert "TELEGRAM" not in source
    assert "OPENAI" not in source
    assert "urlopen" not in source
