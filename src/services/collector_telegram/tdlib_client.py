"""TDLib low-level wrapper for collector-telegram C2."""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Protocol

from .config import CollectorTelegramConfig
from .exceptions import TDLibTransportError

JsonDict = dict[str, Any]


class TDLibTransportProtocol(Protocol):
    """Minimal async transport contract for a concrete TDLib binding."""

    async def initialize(self) -> None: ...

    async def send(self, request: JsonDict) -> None: ...

    async def receive(self, timeout: float) -> JsonDict | None: ...

    async def close(self) -> None: ...


class TDJsonTransport:
    """Async-shaped adapter around the official TDLib tdjson C interface."""

    def __init__(self, *, library_path: str | None = None) -> None:
        self._library_path = library_path
        self._tdjson: ctypes.CDLL | None = None
        self._client: ctypes.c_void_p | None = None

    def assert_available(self) -> None:
        self._load_tdjson()

    async def initialize(self) -> None:
        if self._client is not None:
            return
        tdjson = self._load_tdjson()
        client = tdjson.td_json_client_create()
        if not client:
            raise TDLibTransportError("tdjson client creation failed")
        self._tdjson = tdjson
        self._client = client

    async def send(self, request: JsonDict) -> None:
        if self._tdjson is None or self._client is None:
            raise TDLibTransportError("tdjson transport is not initialized")
        payload = json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._tdjson.td_json_client_send(self._client, payload)

    async def receive(self, timeout: float) -> JsonDict | None:
        if self._tdjson is None or self._client is None:
            raise TDLibTransportError("tdjson transport is not initialized")
        raw = self._tdjson.td_json_client_receive(self._client, float(timeout))
        if not raw:
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TDLibTransportError("tdjson returned an invalid JSON payload") from exc
        if not isinstance(payload, dict):
            raise TDLibTransportError("tdjson returned a non-object JSON payload")
        return payload

    async def close(self) -> None:
        if self._tdjson is None or self._client is None:
            return
        client = self._client
        self._client = None
        try:
            self._tdjson.td_json_client_destroy(client)
        finally:
            self._tdjson = None

    def _load_tdjson(self) -> ctypes.CDLL:
        candidate = (
            self._library_path
            or os.environ.get("TDJSON_LIBRARY_PATH")
            or ctypes.util.find_library("tdjson")
        )
        if not candidate:
            raise TDLibTransportError("tdjson library not found")
        try:
            tdjson = ctypes.CDLL(candidate)
        except OSError as exc:
            raise TDLibTransportError("tdjson library could not be loaded") from exc

        try:
            tdjson.td_json_client_create.restype = ctypes.c_void_p
            tdjson.td_json_client_send.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
            tdjson.td_json_client_receive.argtypes = (ctypes.c_void_p, ctypes.c_double)
            tdjson.td_json_client_receive.restype = ctypes.c_char_p
            tdjson.td_json_client_destroy.argtypes = (ctypes.c_void_p,)
        except AttributeError as exc:
            raise TDLibTransportError("tdjson library is missing required client symbols") from exc
        return tdjson


@dataclass(slots=True, frozen=True)
class TDLibRequest:
    payload: JsonDict


class TDLibClient:
    """Low-level TDLib wrapper.

    This class intentionally contains no collector domain logic.
    It only:
    - builds well-formed TDLib requests,
    - sends/receives payloads through an injected transport,
    - tracks authorization-state visibility,
    - exposes a narrow, testable boundary.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        transport: TDLibTransportProtocol,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._logger = logger or logging.getLogger(__name__)
        self._initialized = False
        self._closed = False
        self._last_authorization_state: JsonDict | None = None

    async def initialize(self) -> None:
        if self._initialized:
            return
        try:
            await self._transport.initialize()
        except Exception as exc:  # pragma: no cover
            raise TDLibTransportError("Failed to initialize TDLib transport") from exc
        self._initialized = True
        self._closed = False

    async def send(self, request: JsonDict) -> None:
        self._ensure_open()
        try:
            await self._transport.send(request)
        except Exception as exc:  # pragma: no cover
            raise TDLibTransportError("Failed to send TDLib request") from exc

    async def receive(self, timeout: float) -> JsonDict | None:
        self._ensure_open()
        try:
            payload = await self._transport.receive(timeout)
        except Exception as exc:  # pragma: no cover
            raise TDLibTransportError("Failed to receive TDLib payload") from exc

        if isinstance(payload, dict) and payload.get("@type") == "updateAuthorizationState":
            auth_state = payload.get("authorization_state")
            if isinstance(auth_state, dict):
                self._last_authorization_state = auth_state
        return payload

    async def close(self) -> None:
        if self._closed:
            return
        try:
            await self._transport.close()
        except Exception as exc:  # pragma: no cover
            raise TDLibTransportError("Failed to close TDLib transport") from exc
        finally:
            self._closed = True
            self._initialized = False

    def is_ready(self) -> bool:
        return self.current_authorization_state_type() == "authorizationStateReady"

    def current_authorization_state(self) -> JsonDict | None:
        return self._last_authorization_state

    def current_authorization_state_type(self) -> str | None:
        state = self._last_authorization_state
        if not isinstance(state, dict):
            return None
        raw = state.get("@type")
        return raw if isinstance(raw, str) else None

    def _ensure_open(self) -> None:
        if not self._initialized:
            raise TDLibTransportError("TDLib client is not initialized")
        if self._closed:
            raise TDLibTransportError("TDLib client is closed")

    def build_set_tdlib_parameters_request(self) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "setTdlibParameters",
                "parameters": {
                    "use_test_dc": False,
                    "database_directory": self._config.tdlib_state_dir,
                    "files_directory": self._config.tdlib_files_dir,
                    "use_file_database": True,
                    "use_chat_info_database": True,
                    "use_message_database": True,
                    "use_secret_chats": False,
                    "api_id": self._config.telegram_api_id,
                    "api_hash": self._config.telegram_api_hash,
                    "system_language_code": "en",
                    "device_model": "catchbot-vps",
                    "system_version": "linux",
                    "application_version": "0.1.0",
                    "database_encryption_key": self._config.tdlib_db_encryption_key,
                },
            }
        )

    def build_check_database_encryption_key_request(self) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "checkDatabaseEncryptionKey",
                "encryption_key": self._config.tdlib_db_encryption_key,
            }
        )

    def build_set_authentication_phone_number_request(self) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "setAuthenticationPhoneNumber",
                "phone_number": self._config.telegram_phone_number,
                "settings": {
                    "allow_flash_call": False,
                    "allow_missed_call": False,
                    "is_current_phone_number": False,
                    "allow_sms_retriever_api": False,
                },
            }
        )

    def build_check_authentication_code_request(self, code: str) -> TDLibRequest:
        return TDLibRequest({"@type": "checkAuthenticationCode", "code": code})

    def build_check_authentication_password_request(self, password: str) -> TDLibRequest:
        return TDLibRequest({"@type": "checkAuthenticationPassword", "password": password})

    def build_search_public_chat_request(self, username: str) -> TDLibRequest:
        normalized = username.removeprefix("@").strip()
        return TDLibRequest({"@type": "searchPublicChat", "username": normalized})

    def build_join_chat_request(self, chat_id: int) -> TDLibRequest:
        return TDLibRequest({"@type": "joinChat", "chat_id": chat_id})

    def build_join_chat_by_invite_link_request(self, invite_link: str) -> TDLibRequest:
        return TDLibRequest({"@type": "joinChatByInviteLink", "invite_link": invite_link})

    def build_get_chat_history_request(
        self,
        *,
        chat_id: int,
        from_message_id: int = 0,
        offset: int = 0,
        limit: int = 50,
        only_local: bool = False,
    ) -> TDLibRequest:
        if limit <= 0 or limit > 100:
            raise TDLibTransportError(f"getChatHistory limit must be between 1 and 100: {limit}")
        return TDLibRequest(
            {
                "@type": "getChatHistory",
                "chat_id": chat_id,
                "from_message_id": from_message_id,
                "offset": offset,
                "limit": limit,
                "only_local": only_local,
            }
        )

    def build_get_message_link_request(
        self,
        *,
        chat_id: int,
        message_id: int,
        for_album: bool = False,
        media_timestamp: int = 0,
    ) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "getMessageLink",
                "chat_id": chat_id,
                "message_id": message_id,
                "media_timestamp": media_timestamp,
                "for_album": for_album,
                "for_comment": False,
            }
        )
