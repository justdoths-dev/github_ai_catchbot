from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.web_enricher import restricted_read_canary
from src.services.web_enricher.restricted_read_canary import RestrictedWebReadHttpResponse
from tools import restricted_web_read_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SECRET_BODY = b"sentinel_cli_body_value"
SECRET_HEADER = "sentinel_cli_header_value"
SECRET_QUERY = "sentinel_cli_query_value"


class FakeHttpClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[tuple[str, float, int]] = []

    async def get(self, url: str, *, timeout_sec: float, max_bytes: int) -> RestrictedWebReadHttpResponse:
        self.calls.append((url, timeout_sec, max_bytes))
        return RestrictedWebReadHttpResponse(
            status_code=200,
            headers={
                "content-type": "text/plain",
                "set-cookie": SECRET_HEADER,
                "authorization": SECRET_HEADER,
            },
            body_bytes=SECRET_BODY,
        )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_canary_module() -> None:
    assert runner.RestrictedWebReadCanary is restricted_read_canary.RestrictedWebReadCanary
    assert runner.RestrictedWebReadCanaryConfig is restricted_read_canary.RestrictedWebReadCanaryConfig
    assert runner.RestrictedWebReadHttpClient is restricted_read_canary.RestrictedWebReadHttpClient


def test_cli_runner_returns_zero_on_fake_success_with_sanitized_output() -> None:
    client = FakeHttpClient()

    result = runner.run(
        _parse_args(
            "--url",
            f"https://example.com/readme?token={SECRET_QUERY}",
            "--operator-approved",
            "--allow-network",
            "--max-requests",
            "3",
            "--max-redirects",
            "3",
            "--max-bytes",
            "65536",
        ),
        http_client=client,
    )
    text = runner.render_json(result.report)
    parsed = json.loads(text)

    assert result.exit_code == 0
    assert parsed["ok"] is True
    assert parsed["status"] == "pass"
    assert parsed["url"] == "https://example.com/readme"
    assert parsed["final_url_host"] == "example.com"
    assert parsed["network_attempted"] is True
    assert parsed["request_count"] == 1
    assert parsed["redirect_count"] == 0
    assert parsed["content_type"] == "text/plain"
    assert parsed["side_effects"] == {
        "database_write": False,
        "redis_write": False,
        "outbox_emit": False,
        "artifact_snapshot_write": False,
        "telegram_call": False,
        "openai_call": False,
        "worker_started": False,
        "repo_state_mutation": False,
    }
    assert SECRET_BODY.decode() not in text
    assert SECRET_HEADER not in text
    assert SECRET_QUERY not in text
    assert client.calls == [(f"https://example.com/readme?token={SECRET_QUERY}", 6.0, 65536)]


def test_main_prints_json_and_uses_fake_client_without_live_network(monkeypatch, capsys) -> None:
    client = FakeHttpClient()
    monkeypatch.setattr(runner, "RestrictedWebReadHttpClient", lambda: client)

    exit_code = runner.main(["--url", "https://example.com/article", "--operator-approved", "--allow-network"])
    out = capsys.readouterr().out
    parsed = json.loads(out)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["error_code"] is None
    assert client.calls == [("https://example.com/article", 6.0, 65536)]
    assert SECRET_BODY.decode() not in out
    assert SECRET_HEADER not in out


def test_default_cli_smoke_exits_nonzero_with_sanitized_operator_approval_json(capsys) -> None:
    exit_code = runner.main([])
    out = capsys.readouterr().out
    parsed = json.loads(out)

    assert exit_code == 1
    assert parsed["canary_name"] == "restricted_web_read_canary"
    assert parsed["mode"] == "restricted_live_read"
    assert parsed["url"] == ""
    assert parsed["final_url_host"] is None
    assert parsed["network_attempted"] is False
    assert parsed["request_count"] == 0
    assert parsed["redirect_count"] == 0
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["content_type"] is None
    assert parsed["status_code_class"] is None
    assert parsed["body_bytes_observed"] is None


def test_tool_source_does_not_duplicate_db_redis_telegram_openai_or_http_client_logic() -> None:
    source = (ROOT / "tools/restricted_web_read_canary_runner.py").read_text(encoding="utf-8")
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
    assert "DATABASE_URL" not in source
    assert "REDIS_URL" not in source
    assert "TELEGRAM" not in source
    assert "OPENAI" not in source
    assert "urlopen" not in source
