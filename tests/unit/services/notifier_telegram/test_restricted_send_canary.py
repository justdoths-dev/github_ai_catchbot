from __future__ import annotations

import asyncio
import json

import pytest

from src.services.notifier_telegram.restricted_send_canary import (
    DEFAULT_MAX_MESSAGE_CHARS,
    DEFAULT_MAX_REQUESTS,
    DEFAULT_TELEGRAM_API_BASE_URL,
    DEFAULT_TIMEOUT_MS,
    RestrictedTelegramSendCanaryConfig,
    RestrictedTelegramSendCanaryRequestBudget,
    RestrictedTelegramSendHttpResponse,
    run_restricted_telegram_send_canary,
)


BOT_TOKEN = "123456:sentinel_telegram_bot_token"
CHAT_ID = "123456789"
USER_MESSAGE = "sentinel user supplied canary message"
RAW_EXCEPTION = "sentinel raw telegram exception detail"
RAW_RESPONSE_TEXT = "sentinel raw telegram response body"


class FakeTelegramSendTransport:
    def __init__(
        self,
        response: RestrictedTelegramSendHttpResponse | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.response = response or _response()
        self.error = error
        self.calls: list[dict] = []

    async def send_message(self, **kwargs) -> RestrictedTelegramSendHttpResponse:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


def _response(status_code: int = 200, payload: dict | None = None) -> RestrictedTelegramSendHttpResponse:
    return RestrictedTelegramSendHttpResponse(
        status_code=status_code,
        payload=payload if payload is not None else {"ok": True, "result": {"message_id": 321}},
    )


def _config(**overrides) -> RestrictedTelegramSendCanaryConfig:
    values = {
        "operator_approved": True,
        "allow_send": True,
        "allow_network": True,
        "bot_token": BOT_TOKEN,
        "chat_id": CHAT_ID,
        "telegram_api_base_url": DEFAULT_TELEGRAM_API_BASE_URL,
        "message": None,
        "max_requests": DEFAULT_MAX_REQUESTS,
        "timeout_ms": DEFAULT_TIMEOUT_MS,
        "max_message_chars": DEFAULT_MAX_MESSAGE_CHARS,
    }
    values.update(overrides)
    return RestrictedTelegramSendCanaryConfig(**values)


def _run(
    config: RestrictedTelegramSendCanaryConfig,
    transport: FakeTelegramSendTransport,
    *,
    budget: RestrictedTelegramSendCanaryRequestBudget | None = None,
) -> dict:
    result = asyncio.run(
        run_restricted_telegram_send_canary(
            config,
            transport=transport,
            request_budget=budget,
        )
    )
    return result.to_sanitized_dict()


def _render(report: dict) -> str:
    return json.dumps(report, sort_keys=True)


@pytest.mark.parametrize(
    ("config", "expected_code"),
    [
        (_config(operator_approved=False), "operator_approval_missing"),
        (_config(allow_send=False), "send_not_allowed"),
        (_config(allow_network=False), "network_not_allowed"),
        (_config(bot_token=""), "credential_missing"),
        (_config(chat_id=None), "chat_id_missing"),
        (_config(chat_id="not-a-chat-id"), "chat_id_invalid"),
        (_config(telegram_api_base_url="https://example.com"), "api_base_url_invalid"),
        (_config(telegram_api_base_url="http://api.telegram.org"), "api_base_url_invalid"),
        (_config(telegram_api_base_url="https://api.telegram.org:443"), "api_base_url_invalid"),
        (_config(telegram_api_base_url="https://api.telegram.org/botTOKEN"), "api_base_url_invalid"),
        (_config(max_requests=0), "request_cap_invalid"),
        (_config(max_requests=2), "request_cap_invalid"),
        (_config(timeout_ms=0), "timeout_invalid"),
        (_config(timeout_ms=60_001), "timeout_invalid"),
        (_config(max_message_chars=0), "message_char_cap_invalid"),
        (_config(max_message_chars=1_001), "message_char_cap_invalid"),
        (_config(message="x" * (DEFAULT_MAX_MESSAGE_CHARS + 1)), "message_too_long"),
        (_config(parse_mode="MarkdownV2"), "parse_mode_not_allowed"),
        (_config(edit=True), "edit_not_allowed"),
        (_config(reply_markup={"inline_keyboard": []}), "reply_markup_not_allowed"),
    ],
)
def test_precondition_failures_block_before_network(
    config: RestrictedTelegramSendCanaryConfig,
    expected_code: str,
) -> None:
    transport = FakeTelegramSendTransport()

    report = _run(config, transport)

    assert report["canary_name"] == "restricted_telegram_send_canary"
    assert report["mode"] == "restricted_live_send"
    assert report["api_method"] == "sendMessage"
    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == expected_code
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert transport.calls == []


def test_fake_success_returns_sanitized_metadata_only() -> None:
    transport = FakeTelegramSendTransport()

    report = _run(_config(message=USER_MESSAGE), transport)
    text = _render(report)

    assert report["target_chat_id_present"] is True
    assert report["message_chars"] == len(USER_MESSAGE)
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert report["max_requests"] == DEFAULT_MAX_REQUESTS
    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["error_code"] is None
    assert report["status_code_class"] == "2xx"
    assert report["telegram_ok"] is True
    assert report["telegram_message_id_present"] is True
    assert report["retry_after_seconds"] is None
    assert all(value is False for value in report["side_effects"].values())
    assert transport.calls == [
        {
            "api_base_url": DEFAULT_TELEGRAM_API_BASE_URL,
            "bot_token": BOT_TOKEN,
            "chat_id": int(CHAT_ID),
            "text": USER_MESSAGE,
            "timeout_ms": DEFAULT_TIMEOUT_MS,
            "disable_notification": True,
            "protect_content": False,
            "link_preview_options": {"is_disabled": True},
        }
    ]
    assert BOT_TOKEN not in text
    assert f"bot{BOT_TOKEN}" not in text
    assert DEFAULT_TELEGRAM_API_BASE_URL not in text
    assert USER_MESSAGE not in text
    assert "raw request" not in text.lower()
    assert "raw response" not in text.lower()


def test_fake_429_maps_to_rate_limited() -> None:
    transport = FakeTelegramSendTransport(
        _response(
            status_code=429,
            payload={"ok": False, "parameters": {"retry_after": 17}, "description": RAW_RESPONSE_TEXT},
        )
    )

    report = _run(_config(), transport)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["error_code"] == "telegram_rate_limited"
    assert report["status_code_class"] == "4xx"
    assert report["telegram_ok"] is False
    assert report["retry_after_seconds"] == 17
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert RAW_RESPONSE_TEXT not in text
    assert BOT_TOKEN not in text


@pytest.mark.parametrize("status_code", [401, 403])
def test_fake_auth_status_maps_to_auth_failed(status_code: int) -> None:
    transport = FakeTelegramSendTransport(_response(status_code=status_code, payload={"ok": False}))

    report = _run(_config(), transport)

    assert report["status"] == "fail"
    assert report["error_code"] == "telegram_auth_failed"
    assert report["status_code_class"] == "4xx"
    assert report["telegram_ok"] is False


@pytest.mark.parametrize(
    ("status_code", "description"),
    [
        (400, "Bad Request: chat not found"),
        (403, "Forbidden: bot was blocked by the user"),
        (403, "Forbidden: bot was kicked from the supergroup"),
        (403, "Forbidden: not enough rights to send text messages"),
    ],
)
def test_fake_forbidden_or_chat_missing_maps_to_chat_not_found_or_forbidden(
    status_code: int,
    description: str,
) -> None:
    transport = FakeTelegramSendTransport(
        _response(status_code=status_code, payload={"ok": False, "description": description})
    )

    report = _run(_config(), transport)

    assert report["status"] == "fail"
    assert report["error_code"] == "telegram_chat_not_found_or_forbidden"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1


def test_fake_bad_request_maps_to_bad_request() -> None:
    transport = FakeTelegramSendTransport(
        _response(status_code=400, payload={"ok": False, "description": "Bad Request: message text is empty"})
    )

    report = _run(_config(), transport)

    assert report["status"] == "fail"
    assert report["error_code"] == "telegram_bad_request"
    assert report["status_code_class"] == "4xx"


@pytest.mark.parametrize(
    "transport",
    [
        FakeTelegramSendTransport(_response(status_code=500, payload={"ok": False, "description": RAW_RESPONSE_TEXT})),
        FakeTelegramSendTransport(error=TimeoutError(RAW_EXCEPTION)),
        FakeTelegramSendTransport(error=ConnectionResetError(RAW_EXCEPTION)),
    ],
)
def test_fake_5xx_or_timeout_maps_to_transient(transport: FakeTelegramSendTransport) -> None:
    report = _run(_config(), transport)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["error_code"] == "telegram_transient_error"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert RAW_EXCEPTION not in text
    assert RAW_RESPONSE_TEXT not in text
    assert BOT_TOKEN not in text


@pytest.mark.parametrize(
    "response",
    [
        RestrictedTelegramSendHttpResponse(status_code=200, payload=None),
        _response(status_code=200, payload={"result": {"message_id": 321}}),
        _response(status_code=200, payload={"ok": True, "result": {}}),
        _response(status_code=200, payload={"ok": True, "result": {"message_id": "not-an-int"}}),
    ],
)
def test_malformed_response_maps_to_response_invalid(response: RestrictedTelegramSendHttpResponse) -> None:
    transport = FakeTelegramSendTransport(response)

    report = _run(_config(), transport)

    assert report["status"] == "fail"
    assert report["error_code"] == "telegram_response_invalid"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1


def test_request_cap_exceeded_fails_safely_without_transport_call() -> None:
    transport = FakeTelegramSendTransport()
    budget = RestrictedTelegramSendCanaryRequestBudget(max_requests=0)

    report = _run(_config(), transport, budget=budget)

    assert report["status"] == "blocked"
    assert report["error_code"] == "request_cap_exceeded"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert transport.calls == []
