from __future__ import annotations

import asyncio
import json
import socket
import urllib.error
import urllib.request
from typing import Any


class TelegramTransportError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_code: str = "telegram_transport_error",
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retry_after_seconds = retry_after_seconds


class TelegramTransportRetryableError(TelegramTransportError):
    pass


class TelegramTransportTerminalError(TelegramTransportError):
    pass


class TelegramTransportNoopError(TelegramTransportError):
    pass


class TelegramBotClient:
    def __init__(self, *, bot_token: str, base_url: str = "https://api.telegram.org", timeout_sec: float = 10) -> None:
        self._bot_token = bot_token
        self._base_url = base_url.rstrip("/")
        self._timeout_sec = timeout_sec

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
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "entities": entities,
            "disable_notification": disable_notification,
            "link_preview_options": link_preview_options,
            "protect_content": False,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        if message_thread_id is not None:
            payload["message_thread_id"] = message_thread_id
        return await self._post("sendMessage", payload)

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
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "entities": entities,
            "link_preview_options": link_preview_options,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return await self._post("editMessageText", payload)

    async def _post(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._bot_token:
            raise TelegramTransportTerminalError("missing bot token")
        return await asyncio.to_thread(self._post_sync, method, payload)

    def _post_sync(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}/bot{self._bot_token}/{method}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
                decoded = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            description, retry_after = _extract_error_details(details)
            if exc.code == 429 or exc.code >= 500:
                raise TelegramTransportRetryableError(
                    description or details,
                    error_code=_retryable_error_code(exc.code, description),
                    retry_after_seconds=retry_after,
                ) from exc
            if _is_not_modified(description):
                raise TelegramTransportNoopError(
                    description or details,
                    error_code="telegram_edit_not_modified_noop",
                ) from exc
            raise TelegramTransportTerminalError(
                description or details,
                error_code=_terminal_error_code(description),
            ) from exc
        except (TimeoutError, socket.timeout, urllib.error.URLError) as exc:
            raise TelegramTransportRetryableError(str(exc), error_code="telegram_network_retryable") from exc
        if not decoded.get("ok", False):
            description, retry_after = _extract_error_details(decoded)
            if retry_after is not None or _is_retryable_description(description):
                raise TelegramTransportRetryableError(
                    description or json.dumps(decoded, ensure_ascii=False),
                    error_code="telegram_flood_retryable" if retry_after is not None else "telegram_retryable",
                    retry_after_seconds=retry_after,
                )
            if _is_not_modified(description):
                raise TelegramTransportNoopError(
                    description or json.dumps(decoded, ensure_ascii=False),
                    error_code="telegram_edit_not_modified_noop",
                )
            raise TelegramTransportTerminalError(
                description or json.dumps(decoded, ensure_ascii=False),
                error_code=_terminal_error_code(description),
            )
        return decoded


def _extract_error_details(value: object) -> tuple[str, int | None]:
    data: object
    if isinstance(value, str):
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            return value, None
    else:
        data = value
    if not isinstance(data, dict):
        return str(value), None
    description = str(data.get("description") or "")
    parameters = data.get("parameters")
    retry_after = None
    if isinstance(parameters, dict) and parameters.get("retry_after") is not None:
        try:
            retry_after = int(parameters["retry_after"])
        except (TypeError, ValueError):
            retry_after = None
    return description, retry_after


def _is_not_modified(description: str) -> bool:
    return "message is not modified" in description.lower()


def _is_retryable_description(description: str) -> bool:
    lowered = description.lower()
    return "too many requests" in lowered or "retry after" in lowered or "flood" in lowered


def _retryable_error_code(status_code: int, description: str) -> str:
    if status_code == 429 or _is_retryable_description(description):
        return "telegram_rate_limited"
    return "telegram_5xx_retryable"


def _terminal_error_code(description: str) -> str:
    lowered = description.lower()
    if "chat not found" in lowered or "invalid chat" in lowered:
        return "telegram_invalid_chat"
    if "bot was blocked" in lowered or "bot blocked" in lowered:
        return "telegram_bot_blocked"
    if "not enough rights" in lowered or "administrator rights" in lowered or "bot was kicked" in lowered:
        return "telegram_insufficient_rights"
    if "message to edit not found" in lowered:
        return "telegram_edit_message_not_found"
    if "can't be edited" in lowered or "cannot be edited" in lowered:
        return "telegram_message_cannot_be_edited"
    if "entities" in lowered or "can't parse" in lowered or "message text is empty" in lowered:
        return "telegram_malformed_message"
    return "telegram_terminal"
