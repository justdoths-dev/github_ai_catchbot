from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pytest

from services.notifier_telegram.main import build_parser
from services.notifier_telegram.restricted_transport_canary import (
    RESTRICTED_TRANSPORT_CANARY_MAX_MESSAGE_CHARS,
    RESTRICTED_TRANSPORT_CANARY_SCHEMA_VERSION,
    run_restricted_transport_canary,
)
from services.notifier_telegram.telegram_client import (
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
)

CANARY_MESSAGE = "github_ai_catchbot restricted delivery canary"
FAKE_BOT_CREDENTIAL = "unit-telegram-credential"
VALID_ENV = {
    "APP_ENV": "prod",
    "ENABLE_NOTIFICATION_SEND": "true",
    "NOTIFIER_TELEGRAM_DRY_RUN": "false",
    "TELEGRAM_BOT_TOKEN": FAKE_BOT_CREDENTIAL,
    "TELEGRAM_CANARY_CHAT_ID": "12345",
}


class FakeTelegramTransport:
    def __init__(self, *, response: dict[str, Any] | None = None, exc: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.response = response or {"ok": True, "result": {"message_id": 321}}
        self.exc = exc

    async def send_message(self, **kwargs) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.exc is not None:
            raise self.exc
        return self.response


class FakeTelegramTransportBuilder:
    def __init__(self, transport: FakeTelegramTransport) -> None:
        self.transport = transport
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs) -> FakeTelegramTransport:
        self.calls.append(kwargs)
        return self.transport


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("env_overrides", "confirm_send", "target_chat_id", "message", "expected_reason"),
    [
        ({}, False, "12345", CANARY_MESSAGE, "confirm_send_required"),
        ({"APP_ENV": "dev"}, True, "12345", CANARY_MESSAGE, "app_env_not_prod"),
        ({"ENABLE_NOTIFICATION_SEND": "false"}, True, "12345", CANARY_MESSAGE, "notification_send_disabled"),
        ({"NOTIFIER_TELEGRAM_DRY_RUN": "true"}, True, "12345", CANARY_MESSAGE, "notifier_dry_run_enabled"),
        ({"TELEGRAM_BOT_TOKEN": None}, True, "12345", CANARY_MESSAGE, "bot_token_missing"),
        (
            {"TELEGRAM_CANARY_CHAT_ID": None, "NOTIFIER_TELEGRAM_CANARY_CHAT_ID": None},
            True,
            "12345",
            CANARY_MESSAGE,
            "canary_chat_id_missing",
        ),
        ({}, True, "99999", CANARY_MESSAGE, "target_chat_id_mismatch"),
    ],
)
async def test_guard_failures_reject_without_transport(
    env_overrides: dict[str, str | None],
    confirm_send: bool,
    target_chat_id: str,
    message: str,
    expected_reason: str,
) -> None:
    code, payload, transport, builder, _ = await _invoke(
        env_overrides=env_overrides,
        confirm_send=confirm_send,
        target_chat_id=target_chat_id,
        message=message,
    )

    assert code == 2
    assert payload["schema_version"] == RESTRICTED_TRANSPORT_CANARY_SCHEMA_VERSION
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == expected_reason
    assert payload["transport_attempted"] is False
    assert payload["authority"]["telegram_called"] is False
    assert builder.calls == []
    assert transport.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("message", ["", "x" * (RESTRICTED_TRANSPORT_CANARY_MAX_MESSAGE_CHARS + 1)])
async def test_empty_or_overlong_message_rejects_without_transport(message: str) -> None:
    code, payload, transport, builder, _ = await _invoke(message=message)

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "message_length_invalid"
    assert payload["guards"]["message_length_valid"] is False
    assert payload["transport_attempted"] is False
    assert builder.calls == []
    assert transport.calls == []


@pytest.mark.asyncio
async def test_valid_guarded_canary_calls_fake_transport_once() -> None:
    code, payload, transport, builder, _ = await _invoke()

    assert code == 0
    assert payload["status"] == "sent"
    assert payload["reason_code"] == "sent"
    assert payload["transport_attempted"] is True
    assert payload["telegram_message_id"] == 321
    assert payload["target_chat_id_matched"] is True
    assert payload["authority"] == {
        "telegram_called": True,
        "openai_called": False,
        "github_called": False,
        "redis_mutation": False,
        "workers_started": False,
        "database_mutation": False,
        "production_db_write": False,
        "env_file_mutated": False,
        "feature_flags_applied": False,
        "alembic_or_ddl_ran": False,
        "subprocess_started": False,
        "shell_invoked": False,
    }
    assert builder.calls == [
        {
            "bot_token": FAKE_BOT_CREDENTIAL,
            "base_url": "https://api.telegram.org",
            "timeout_sec": 10.0,
        }
    ]
    assert transport.calls == [
        {
            "chat_id": "12345",
            "text": CANARY_MESSAGE,
            "entities": [],
            "reply_markup": None,
            "disable_notification": True,
            "link_preview_options": {"is_disabled": True},
            "message_thread_id": None,
        }
    ]


@pytest.mark.asyncio
async def test_retryable_telegram_transport_error_emits_sanitized_failed_retryable() -> None:
    exc = TelegramTransportRetryableError(
        "RAW_EXCEPTION_SENTINEL " + FAKE_BOT_CREDENTIAL,
        error_code="telegram_network_retryable",
    )
    code, payload, transport, builder, output = await _invoke(transport=FakeTelegramTransport(exc=exc))

    assert code == 1
    assert payload["status"] == "failed_retryable"
    assert payload["reason_code"] == "telegram_network_retryable"
    assert payload["transport_attempted"] is True
    assert payload["authority"]["telegram_called"] is True
    assert len(builder.calls) == 1
    assert len(transport.calls) == 1
    assert "RAW_EXCEPTION_SENTINEL" not in output
    assert FAKE_BOT_CREDENTIAL not in output


@pytest.mark.asyncio
async def test_terminal_telegram_transport_error_emits_sanitized_failed_terminal() -> None:
    exc = TelegramTransportTerminalError(
        "RAW_EXCEPTION_SENTINEL " + FAKE_BOT_CREDENTIAL,
        error_code="telegram_invalid_chat",
    )
    code, payload, transport, builder, output = await _invoke(transport=FakeTelegramTransport(exc=exc))

    assert code == 1
    assert payload["status"] == "failed_terminal"
    assert payload["reason_code"] == "telegram_invalid_chat"
    assert payload["transport_attempted"] is True
    assert payload["authority"]["telegram_called"] is True
    assert len(builder.calls) == 1
    assert len(transport.calls) == 1
    assert "RAW_EXCEPTION_SENTINEL" not in output
    assert FAKE_BOT_CREDENTIAL not in output


@pytest.mark.asyncio
async def test_output_never_includes_secrets_env_names_traceback_or_raw_exception_text() -> None:
    fake_database_url = "postgresql+psycopg" + "://" + "user:pass" + "word@example/db"
    fake_redis_url = "redis" + "://" + ":pass" + "word@example/0"
    fake_openai_key = "openai" + "-credential-value"
    fake_telegram_credential = "telegram" + "-credential-value"
    env_overrides = {
        "DATABASE_URL": fake_database_url,
        "REDIS_URL": fake_redis_url,
        "OPENAI_API_KEY": fake_openai_key,
        "TELEGRAM_BOT_TOKEN": fake_telegram_credential,
    }
    code, payload, _, _, output = await _invoke(
        env_overrides=env_overrides,
        transport=FakeTelegramTransport(
            exc=RuntimeError(
                "RAW_EXCEPTION_SENTINEL pass"
                "word DATABASE_URL REDIS_URL TELEGRAM_BOT_TOKEN OPENAI_API_KEY"
            )
        ),
    )

    assert code == 1
    assert payload["status"] == "failed_terminal"
    assert payload["reason_code"] == "telegram_transport_unclassified"
    for forbidden in [
        fake_telegram_credential,
        fake_openai_key,
        fake_database_url,
        fake_redis_url,
        "pass" + "word",
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "OPENAI_API_KEY",
        "Traceback",
        "RAW_EXCEPTION_SENTINEL",
    ]:
        assert forbidden not in output


def test_restricted_transport_canary_module_does_not_import_forbidden_boundaries() -> None:
    source_path = Path("src/services/notifier_telegram/restricted_transport_canary.py")
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])

    assert imported_roots.isdisjoint({"redis", "openai", "github", "docker", "systemd", "subprocess"})
    assert "shell=True" not in source


def test_cli_parser_accepts_restricted_transport_canary_valid_command() -> None:
    args = build_parser().parse_args(
        [
            "restricted-transport-canary",
            "--target-chat-id",
            "12345",
            "--message",
            CANARY_MESSAGE,
            "--confirm-send",
            "--format",
            "json",
        ]
    )

    assert args.command == "restricted-transport-canary"
    assert args.target_chat_id == "12345"
    assert args.message == CANARY_MESSAGE
    assert args.confirm_send is True
    assert args.format == "json"


@pytest.mark.parametrize(
    "argv",
    [
        ["restricted-transport-canary", "--target-chat-id", "12345", "--confirm-send"],
        ["restricted-transport-canary", "--message", CANARY_MESSAGE, "--confirm-send"],
        [
            "restricted-transport-canary",
            "--target-chat-id",
            "12345",
            "--message",
            CANARY_MESSAGE,
            "--confirm-send",
            "--format",
            "text",
        ],
    ],
)
def test_cli_parser_rejects_unsafe_restricted_transport_canary_forms(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(argv)


async def _invoke(
    *,
    env_overrides: dict[str, str | None] | None = None,
    confirm_send: bool = True,
    target_chat_id: str = "12345",
    message: str = CANARY_MESSAGE,
    transport: FakeTelegramTransport | None = None,
) -> tuple[int, dict[str, Any], FakeTelegramTransport, FakeTelegramTransportBuilder, str]:
    effective_env = dict(VALID_ENV)
    for key, value in (env_overrides or {}).items():
        if value is None:
            effective_env.pop(key, None)
        else:
            effective_env[key] = value

    fake_transport = transport or FakeTelegramTransport()
    builder = FakeTelegramTransportBuilder(fake_transport)
    emitted: list[str] = []
    code = await run_restricted_transport_canary(
        target_chat_id=target_chat_id,
        message=message,
        confirm_send=confirm_send,
        env=effective_env,
        telegram_client_builder=builder,
        emit_json=emitted.append,
    )

    assert len(emitted) == 1
    return code, json.loads(emitted[0]), fake_transport, builder, emitted[0]
