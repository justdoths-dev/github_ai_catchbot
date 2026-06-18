from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

from .config import NotifierTelegramConfig
from .telegram_client import (
    TelegramBotClient,
    TelegramTransportError,
    TelegramTransportNoopError,
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
)

SAFE_TRANSPORT_CODE = re.compile(r"^[a-z0-9_]{1,80}$")


class TelegramTransport(Protocol):
    async def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        entities: list[dict[str, Any]],
        reply_markup: dict[str, Any] | None,
        disable_notification: bool,
        link_preview_options: dict[str, Any],
        message_thread_id: int | None = None,
    ) -> dict[str, Any]: ...

    async def edit_message_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        entities: list[dict[str, Any]],
        reply_markup: dict[str, Any] | None,
        link_preview_options: dict[str, Any],
    ) -> dict[str, Any]: ...


class TelegramTransportConstructionError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class TelegramBotApiTransport:
    def __init__(self, client: TelegramBotClient) -> None:
        self._client = client

    @classmethod
    def from_config(
        cls,
        config: NotifierTelegramConfig,
        *,
        allow_telegram_transport: bool,
        allow_telegram_send: bool,
    ) -> "TelegramBotApiTransport":
        if not allow_telegram_transport:
            raise TelegramTransportConstructionError("telegram_transport_not_allowed")
        if not allow_telegram_send:
            raise TelegramTransportConstructionError("telegram_send_not_allowed")
        if not config.transport_enabled:
            raise TelegramTransportConstructionError("telegram_transport_disabled_by_config")
        config.validate(require_transport_token=True)
        return cls(
            TelegramBotClient(
                bot_token=config.telegram_bot_token,
                base_url=config.telegram_api_base_url,
                timeout_sec=config.request_timeout_sec,
            )
        )

    async def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        entities: list[dict[str, Any]],
        reply_markup: dict[str, Any] | None,
        disable_notification: bool,
        link_preview_options: dict[str, Any],
        message_thread_id: int | None = None,
    ) -> dict[str, Any]:
        return await self._client.send_message(
            chat_id=chat_id,
            text=text,
            entities=entities,
            reply_markup=reply_markup,
            disable_notification=disable_notification,
            link_preview_options=link_preview_options,
            message_thread_id=message_thread_id,
        )

    async def edit_message_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        entities: list[dict[str, Any]],
        reply_markup: dict[str, Any] | None,
        link_preview_options: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._client.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            entities=entities,
            reply_markup=reply_markup,
            link_preview_options=link_preview_options,
        )


@dataclass(slots=True)
class FakeTelegramTransport:
    response: dict[str, Any] | None = None
    exc: Exception | None = None
    calls: list[dict[str, Any]] = field(default_factory=list, init=False)

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"method": "send_message", **kwargs})
        if self.exc is not None:
            raise self.exc
        return self.response or {"ok": True, "result": {"message_id": 1, "chat": {"id": kwargs.get("chat_id")}}}

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"method": "edit_message_text", **kwargs})
        if self.exc is not None:
            raise self.exc
        return self.response or {
            "ok": True,
            "result": {"message_id": kwargs.get("message_id"), "chat": {"id": kwargs.get("chat_id")}},
        }


class StateTrackingTelegramTransport:
    def __init__(
        self,
        wrapped: TelegramTransport,
        *,
        on_send: Callable[[], None],
        on_edit: Callable[[], None],
    ) -> None:
        self._wrapped = wrapped
        self._on_send = on_send
        self._on_edit = on_edit

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self._on_send()
        return await self._wrapped.send_message(**kwargs)

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        self._on_edit()
        return await self._wrapped.edit_message_text(**kwargs)


def redacted_transport_response(response: Mapping[str, Any] | None) -> dict[str, Any]:
    result = response.get("result") if isinstance(response, Mapping) else None
    message_id_present = isinstance(result, Mapping) and result.get("message_id") is not None
    chat_id_present = isinstance(result, Mapping) and isinstance(result.get("chat"), Mapping) and result["chat"].get("id") is not None
    return {
        "ok": bool(response.get("ok")) if isinstance(response, Mapping) else False,
        "telegram_message_id_present": message_id_present,
        "telegram_chat_id_present": chat_id_present,
        "raw_response_omitted": True,
    }


def safe_transport_error_code(value: object, default: str) -> str:
    text = str(value or "")
    if SAFE_TRANSPORT_CODE.fullmatch(text):
        return text
    return default


__all__ = [
    "FakeTelegramTransport",
    "StateTrackingTelegramTransport",
    "TelegramBotApiTransport",
    "TelegramTransport",
    "TelegramTransportConstructionError",
    "TelegramTransportError",
    "TelegramTransportNoopError",
    "TelegramTransportRetryableError",
    "TelegramTransportTerminalError",
    "redacted_transport_response",
    "safe_transport_error_code",
]
