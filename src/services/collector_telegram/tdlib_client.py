"""TDLib low-level wrapper for collector-telegram C2."""

from __future__ import annotations

import base64
import binascii
import ctypes
import ctypes.util
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from .config import CollectorTelegramConfig
from .exceptions import TDLibTransportError

JsonDict = dict[str, Any]
DEFAULT_TDJSON_LIBRARY_PATH_CANDIDATES = (
    "/opt/github-ai-catchbot/tdlib/lib/libtdjson.so",
)


def tdlib_json_bytes(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def build_set_tdlib_parameters_payload(config: CollectorTelegramConfig) -> JsonDict:
    return {
        "@type": "setTdlibParameters",
        "use_test_dc": False,
        "database_directory": config.tdlib_state_dir,
        "files_directory": config.tdlib_files_dir,
        "use_file_database": True,
        "use_chat_info_database": True,
        "use_message_database": True,
        "use_secret_chats": False,
        "api_id": config.telegram_api_id,
        "api_hash": config.telegram_api_hash,
        "system_language_code": "en",
        "device_model": "catchbot-vps",
        "system_version": "linux",
        "application_version": "0.1.0",
        "database_encryption_key": tdlib_json_bytes(config.tdlib_db_encryption_key),
    }


def tdlib_parameters_shape_errors(payload: Mapping[str, Any]) -> tuple[str, ...]:
    errors: list[str] = []

    if payload.get("@type") != "setTdlibParameters":
        errors.append("@type.invalid")

    api_id = payload.get("api_id")
    if isinstance(api_id, bool) or not isinstance(api_id, int) or api_id <= 0:
        errors.append("api_id.invalid")

    for field in (
        "api_hash",
        "database_directory",
        "files_directory",
        "database_encryption_key",
    ):
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field}.empty")

    expected_flags = {
        "use_file_database": True,
        "use_chat_info_database": True,
        "use_message_database": True,
        "use_secret_chats": False,
    }
    for field, expected in expected_flags.items():
        if payload.get(field) is not expected:
            errors.append(f"{field}.invalid")

    return tuple(errors)


def tdlib_parameters_semantic_errors(payload: Mapping[str, Any]) -> tuple[str, ...]:
    errors: list[str] = []

    for field in (
        "database_directory",
        "files_directory",
        "system_language_code",
        "device_model",
        "system_version",
        "application_version",
    ):
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field}.semantic_empty")

    database_encryption_key = payload.get("database_encryption_key")
    if isinstance(database_encryption_key, str) and database_encryption_key.strip():
        try:
            base64.b64decode(database_encryption_key, validate=True)
        except (binascii.Error, ValueError):
            errors.append("database_encryption_key.invalid_base64")

    if payload.get("use_message_database") is True and payload.get("use_chat_info_database") is not True:
        errors.append("use_message_database.requires_chat_info_database")
    if payload.get("use_chat_info_database") is True and payload.get("use_file_database") is not True:
        errors.append("use_chat_info_database.requires_file_database")

    return tuple(errors)


class TDLibTransportProtocol(Protocol):
    """Minimal async transport contract for a concrete TDLib binding."""

    async def initialize(self) -> None: ...

    async def send(self, request: JsonDict) -> None: ...

    async def receive(self, timeout: float) -> JsonDict | None: ...

    async def close(self) -> None: ...


class TDJsonTransport:
    """Async-shaped adapter around the official TDLib tdjson C interface."""

    def __init__(self, *, library_path: str | None = None, suppress_native_logs: bool = True) -> None:
        self._library_path = library_path
        self._suppress_native_logs = suppress_native_logs
        self._native_log_suppression_attempted = False
        self._native_log_suppression_confirmed = False
        self._tdjson: ctypes.CDLL | None = None
        self._client: ctypes.c_void_p | None = None

    def assert_available(self) -> None:
        self._load_tdjson()

    def native_log_suppression_attempted(self) -> bool:
        return self._native_log_suppression_attempted

    def native_log_suppression_confirmed(self) -> bool:
        return self._native_log_suppression_confirmed

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
            or self._default_library_path_candidate()
        )
        if not candidate:
            raise TDLibTransportError("tdjson library not found")
        try:
            tdjson = ctypes.CDLL(candidate)
        except OSError as exc:
            raise TDLibTransportError("tdjson library could not be loaded") from exc

        self._suppress_native_tdlib_logs(tdjson)

        try:
            tdjson.td_json_client_create.restype = ctypes.c_void_p
            tdjson.td_json_client_send.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
            tdjson.td_json_client_receive.argtypes = (ctypes.c_void_p, ctypes.c_double)
            tdjson.td_json_client_receive.restype = ctypes.c_char_p
            tdjson.td_json_client_destroy.argtypes = (ctypes.c_void_p,)
        except AttributeError as exc:
            raise TDLibTransportError("tdjson library is missing required client symbols") from exc
        return tdjson

    def _suppress_native_tdlib_logs(self, tdjson: ctypes.CDLL) -> None:
        if not self._suppress_native_logs:
            return
        self._native_log_suppression_attempted = True
        setter = getattr(tdjson, "td_set_log_verbosity_level", None)
        if setter is None:
            self._native_log_suppression_confirmed = False
            raise TDLibTransportError("tdjson native log suppression unavailable")
        try:
            setter.argtypes = (ctypes.c_int,)
            setter.restype = None
            setter(0)
        except Exception as exc:
            self._native_log_suppression_confirmed = False
            raise TDLibTransportError("tdjson native log suppression failed") from exc
        self._native_log_suppression_confirmed = True

    @staticmethod
    def _default_library_path_candidate() -> str | None:
        for candidate in DEFAULT_TDJSON_LIBRARY_PATH_CANDIDATES:
            if os.path.exists(candidate):
                return candidate
        return None


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
        except TDLibTransportError:
            raise
        except Exception as exc:  # pragma: no cover
            raise TDLibTransportError("Failed to initialize TDLib transport") from exc
        self._initialized = True
        self._closed = False

    async def send(self, request: JsonDict) -> None:
        self._ensure_open()
        try:
            await self._transport.send(request)
        except TDLibTransportError:
            raise
        except Exception as exc:  # pragma: no cover
            raise TDLibTransportError("Failed to send TDLib request") from exc

    async def receive(self, timeout: float) -> JsonDict | None:
        self._ensure_open()
        try:
            payload = await self._transport.receive(timeout)
        except TDLibTransportError:
            raise
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

    def native_log_suppression_attempted(self) -> bool:
        getter = getattr(self._transport, "native_log_suppression_attempted", None)
        if getter is None:
            return False
        return bool(getter())

    def native_log_suppression_confirmed(self) -> bool:
        getter = getattr(self._transport, "native_log_suppression_confirmed", None)
        if getter is None:
            return False
        return bool(getter())

    def _ensure_open(self) -> None:
        if not self._initialized:
            raise TDLibTransportError("TDLib client is not initialized")
        if self._closed:
            raise TDLibTransportError("TDLib client is closed")

    def build_set_tdlib_parameters_request(self) -> TDLibRequest:
        return TDLibRequest(build_set_tdlib_parameters_payload(self._config))

    def build_check_database_encryption_key_request(self) -> TDLibRequest:
        return TDLibRequest(
            {
                "@type": "checkDatabaseEncryptionKey",
                "encryption_key": tdlib_json_bytes(self._config.tdlib_db_encryption_key),
            }
        )

    def build_get_authorization_state_request(self) -> TDLibRequest:
        return TDLibRequest({"@type": "getAuthorizationState"})

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
