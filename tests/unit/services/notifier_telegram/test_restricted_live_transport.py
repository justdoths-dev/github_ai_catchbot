from __future__ import annotations

import json

import pytest

from src.services.notifier_telegram.transport import (
    FakeTelegramTransport,
    TelegramBotApiTransport,
    TelegramTransportConstructionError,
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
    redacted_transport_response,
    safe_transport_error_code,
)
from tests.unit.services.notifier_telegram._service_fakes import config


@pytest.mark.asyncio
async def test_fake_transport_success_has_redacted_report() -> None:
    token = "unit-telegram-token"
    chat_id = 987654321
    rendered_text = "Rendered message body must not appear in report"
    transport = FakeTelegramTransport(
        response={"ok": True, "result": {"message_id": 444, "chat": {"id": chat_id}, "text": rendered_text}}
    )

    response = await transport.send_message(
        chat_id=chat_id,
        text=rendered_text,
        entities=[],
        reply_markup=None,
        disable_notification=True,
        link_preview_options={"is_disabled": True},
    )
    report = redacted_transport_response(response)
    encoded = json.dumps(report, sort_keys=True)

    assert report == {
        "ok": True,
        "telegram_message_id_present": True,
        "telegram_chat_id_present": True,
        "raw_response_omitted": True,
    }
    assert str(chat_id) not in encoded
    assert rendered_text not in encoded
    assert token not in encoded


@pytest.mark.asyncio
async def test_retryable_transport_error_is_classified_without_raw_values() -> None:
    token = "unit-telegram-token"
    exc = TelegramTransportRetryableError(
        "private retryable detail " + token,
        error_code="telegram_network_retryable",
    )
    transport = FakeTelegramTransport(exc=exc)

    with pytest.raises(TelegramTransportRetryableError) as raised:
        await transport.send_message(
            chat_id=123,
            text="rendered secret text",
            entities=[],
            reply_markup=None,
            disable_notification=True,
            link_preview_options={"is_disabled": True},
        )

    report = {"status": "failed_retryable", "reason_code": safe_transport_error_code(raised.value.error_code, "telegram_retryable")}
    encoded = json.dumps(report, sort_keys=True)
    assert report["reason_code"] == "telegram_network_retryable"
    assert token not in encoded
    assert "private retryable detail" not in encoded


@pytest.mark.asyncio
async def test_terminal_transport_error_is_classified_without_raw_values() -> None:
    exc = TelegramTransportTerminalError(
        "private terminal detail",
        error_code="telegram_invalid_chat",
    )
    transport = FakeTelegramTransport(exc=exc)

    with pytest.raises(TelegramTransportTerminalError) as raised:
        await transport.send_message(
            chat_id=123,
            text="rendered secret text",
            entities=[],
            reply_markup=None,
            disable_notification=True,
            link_preview_options={"is_disabled": True},
        )

    report = {"status": "failed_terminal", "reason_code": safe_transport_error_code(raised.value.error_code, "telegram_terminal")}
    encoded = json.dumps(report, sort_keys=True)
    assert report["reason_code"] == "telegram_invalid_chat"
    assert "private terminal detail" not in encoded
    assert "rendered secret text" not in encoded


def test_real_transport_adapter_requires_explicit_gates_and_enabled_config() -> None:
    live_config = config(dry_run=False, enable_notification_send=True)
    disabled_config = config(dry_run=False, enable_notification_send=False)

    with pytest.raises(TelegramTransportConstructionError, match="telegram_transport_not_allowed"):
        TelegramBotApiTransport.from_config(
            live_config,
            allow_telegram_transport=False,
            allow_telegram_send=True,
        )
    with pytest.raises(TelegramTransportConstructionError, match="telegram_send_not_allowed"):
        TelegramBotApiTransport.from_config(
            live_config,
            allow_telegram_transport=True,
            allow_telegram_send=False,
        )
    with pytest.raises(TelegramTransportConstructionError, match="telegram_transport_disabled_by_config"):
        TelegramBotApiTransport.from_config(
            disabled_config,
            allow_telegram_transport=True,
            allow_telegram_send=True,
        )
