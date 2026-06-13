from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from src.services.notifier_telegram import restricted_send_canary
from src.services.notifier_telegram.restricted_send_canary import RestrictedTelegramSendHttpResponse
from tools import restricted_telegram_send_canary_runner as runner


ROOT = Path(__file__).resolve().parents[3]
BOT_TOKEN = "123456:sentinel_cli_telegram_bot_token"
CHAT_ID = "123456789"
USER_MESSAGE = "sentinel cli telegram message text"
RAW_RESPONSE_TEXT = "sentinel cli raw response body"


class FakeTelegramSendTransport:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def send_message(self, **kwargs) -> RestrictedTelegramSendHttpResponse:
        self.calls.append(kwargs)
        return RestrictedTelegramSendHttpResponse(
            status_code=200,
            payload={
                "ok": True,
                "result": {
                    "message_id": 987,
                    "text": RAW_RESPONSE_TEXT,
                },
            },
        )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_canary_module() -> None:
    assert runner.RestrictedTelegramSendCanaryConfig is restricted_send_canary.RestrictedTelegramSendCanaryConfig
    assert runner.run_restricted_telegram_send_canary is restricted_send_canary.run_restricted_telegram_send_canary


def test_main_with_no_flags_returns_json_and_nonzero_exit(capsys) -> None:
    exit_code = runner.main([])
    out = capsys.readouterr().out
    parsed = json.loads(out)

    assert exit_code == 1
    assert parsed["canary_name"] == "restricted_telegram_send_canary"
    assert parsed["mode"] == "restricted_live_send"
    assert parsed["api_method"] == "sendMessage"
    assert parsed["target_chat_id_present"] is False
    assert parsed["message_chars"] == 0
    assert parsed["network_attempted"] is False
    assert parsed["request_count"] == 0
    assert parsed["max_requests"] == 1
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"


def test_approval_send_network_and_chat_id_missing_token_returns_credential_missing() -> None:
    transport = FakeTelegramSendTransport()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-send",
            "--allow-network",
            "--chat-id",
            CHAT_ID,
        ),
        env={},
        transport=transport,
    )

    assert result.exit_code == 1
    assert result.report["status"] == "blocked"
    assert result.report["error_code"] == "credential_missing"
    assert result.report["network_attempted"] is False
    assert result.report["request_count"] == 0
    assert transport.calls == []


def test_cli_output_does_not_contain_bot_token_when_env_is_configured() -> None:
    transport = FakeTelegramSendTransport()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-send",
            "--allow-network",
            "--chat-id",
            CHAT_ID,
            "--telegram-bot-token-env",
            "CUSTOM_TELEGRAM_BOT_TOKEN",
            "--message",
            USER_MESSAGE,
        ),
        env={"CUSTOM_TELEGRAM_BOT_TOKEN": BOT_TOKEN},
        transport=transport,
    )
    text = runner.render_json(result.report)

    assert result.exit_code == 0
    assert result.report["status"] == "pass"
    assert BOT_TOKEN not in text
    assert f"bot{BOT_TOKEN}" not in text
    assert "api.telegram.org/bot" not in text
    assert USER_MESSAGE not in text
    assert RAW_RESPONSE_TEXT not in text
    assert "raw_request" not in text
    assert "raw_response" not in text
    assert len(transport.calls) == 1


def test_cli_success_path_uses_fake_transport_without_live_network() -> None:
    transport = FakeTelegramSendTransport()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-send",
            "--allow-network",
            "--chat-id",
            CHAT_ID,
            "--max-requests",
            "1",
            "--max-message-chars",
            "500",
            "--timeout-ms",
            "10000",
        ),
        env={"TELEGRAM_BOT_TOKEN": BOT_TOKEN},
        transport=transport,
    )
    parsed = json.loads(runner.render_json(result.report))

    assert result.exit_code == 0
    assert parsed["ok"] is True
    assert parsed["network_attempted"] is True
    assert parsed["request_count"] == 1
    assert len(transport.calls) == 1
    assert transport.calls[0]["api_base_url"] == "https://api.telegram.org"
    assert transport.calls[0]["bot_token"] == BOT_TOKEN
    assert transport.calls[0]["chat_id"] == int(CHAT_ID)
    assert transport.calls[0]["disable_notification"] is True
    assert transport.calls[0]["protect_content"] is False
    assert transport.calls[0]["link_preview_options"] == {"is_disabled": True}


def test_tool_source_does_not_import_db_redis_openai_github_x_or_web_clients_directly() -> None:
    source = (ROOT / "tools/restricted_telegram_send_canary_runner.py").read_text(encoding="utf-8")
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
        "sqlalchemy",
        "redis",
        "openai",
        "requests",
        "httpx",
        "aiohttp",
        "telegram",
    }.isdisjoint(imported_roots)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "DATABASE_URL" not in source
    assert "REDIS_URL" not in source
    assert "OPENAI_API_KEY" not in source
    assert "GITHUB_" not in source
    assert "X_BEARER" not in source
    assert "urlopen" not in source


def test_tool_source_only_writes_sanitized_report() -> None:
    source = (ROOT / "tools/restricted_telegram_send_canary_runner.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    assert "print(" not in source
    assert "raw_request" not in source
    assert "raw_response" not in source
    assert "request.body" not in source
    assert "response.read" not in source
    assert "traceback" not in source.lower()
    assert any(
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "write"
        and isinstance(call.func.value, ast.Attribute)
        and call.func.value.attr == "stdout"
        for call in calls
    )
    assert "render_json(result.report)" in source


@pytest.mark.parametrize(
    "argv",
    [
        ["--parse-mode", "MarkdownV2"],
        ["--edit"],
        ["--allow-edits"],
        ["--reply-markup", "{}"],
    ],
)
def test_unsupported_parse_edit_or_reply_markup_flags_are_rejected(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        runner.build_parser().parse_args(argv)
