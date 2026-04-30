from __future__ import annotations

import asyncio
import json
import socket
import urllib.error
import urllib.request
from typing import Any


class TelegramTransportError(RuntimeError):
    pass


class TelegramTransportRetryableError(TelegramTransportError):
    pass


class TelegramTransportTerminalError(TelegramTransportError):
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
            if exc.code == 429 or exc.code >= 500:
                raise TelegramTransportRetryableError(details) from exc
            raise TelegramTransportTerminalError(details) from exc
        except (TimeoutError, socket.timeout, urllib.error.URLError) as exc:
            raise TelegramTransportRetryableError(str(exc)) from exc
        if not decoded.get("ok", False):
            raise TelegramTransportTerminalError(json.dumps(decoded, ensure_ascii=False))
        return decoded
