from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from .telegram_client import (
    TelegramBotClient,
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
)

RESTRICTED_TRANSPORT_CANARY_SCHEMA_VERSION = "notifier_restricted_transport_canary_v1"
RESTRICTED_TRANSPORT_CANARY_MAX_MESSAGE_CHARS = 500

_SAFE_REASON_CODE = re.compile(r"^[a-z0-9_]{1,80}$")


class RestrictedTelegramTransport(Protocol):
    async def send_message(
        self,
        *,
        chat_id: str,
        text: str,
        entities: list[dict[str, Any]],
        reply_markup: dict[str, Any] | None,
        disable_notification: bool,
        link_preview_options: dict[str, Any],
        message_thread_id: int | None = None,
    ) -> dict[str, Any]: ...


TelegramTransportBuilder = Callable[..., RestrictedTelegramTransport]


async def run_restricted_transport_canary(
    *,
    target_chat_id: str | None,
    message: str | None,
    confirm_send: bool,
    emit_json=print,
    env: Mapping[str, str] | None = None,
    telegram_client_builder: TelegramTransportBuilder = TelegramBotClient,
) -> int:
    source_env = os.environ if env is None else env
    normalized_message = _normalize_message(message)
    configured_canary_chat_id = _configured_canary_chat_id(source_env)
    target_chat_id_value = _normalize_env_value(target_chat_id)
    bot_token = _env_value(source_env, "TELEGRAM_BOT_TOKEN")

    guards = {
        "confirm_send": confirm_send,
        "app_env_prod": _env_value(source_env, "APP_ENV").lower() == "prod",
        "notification_send_enabled": _env_value(source_env, "ENABLE_NOTIFICATION_SEND").lower() == "true",
        "dry_run_disabled": _env_value(source_env, "NOTIFIER_TELEGRAM_DRY_RUN").lower() == "false",
        "bot_token_present": bool(bot_token),
        "canary_chat_id_configured": bool(configured_canary_chat_id),
        "target_chat_id_matches_canary": bool(
            configured_canary_chat_id and target_chat_id_value == configured_canary_chat_id
        ),
        "message_length_valid": bool(
            normalized_message
            and len(normalized_message) <= RESTRICTED_TRANSPORT_CANARY_MAX_MESSAGE_CHARS
        ),
    }

    rejection_reason = _first_rejection_reason(guards)
    if rejection_reason is not None:
        emit_json(
            _to_json(
                _payload(
                    status="rejected",
                    reason_code=rejection_reason,
                    guards=guards,
                    transport_attempted=False,
                    telegram_called=False,
                )
            )
        )
        return 2

    transport_attempted = True
    telegram_called = True
    try:
        client = telegram_client_builder(
            bot_token=bot_token,
            base_url=_env_value(source_env, "TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
            timeout_sec=_request_timeout_sec(source_env),
        )
        response = await client.send_message(
            chat_id=target_chat_id_value,
            text=normalized_message,
            entities=[],
            reply_markup=None,
            disable_notification=True,
            link_preview_options={"is_disabled": True},
            message_thread_id=None,
        )
    except TelegramTransportRetryableError as exc:
        emit_json(
            _to_json(
                _payload(
                    status="failed_retryable",
                    reason_code=_safe_reason_code(getattr(exc, "error_code", None), "telegram_retryable"),
                    guards=guards,
                    transport_attempted=transport_attempted,
                    telegram_called=telegram_called,
                )
            )
        )
        return 1
    except TelegramTransportTerminalError as exc:
        emit_json(
            _to_json(
                _payload(
                    status="failed_terminal",
                    reason_code=_safe_reason_code(getattr(exc, "error_code", None), "telegram_terminal"),
                    guards=guards,
                    transport_attempted=transport_attempted,
                    telegram_called=telegram_called,
                )
            )
        )
        return 1
    except Exception:
        emit_json(
            _to_json(
                _payload(
                    status="failed_terminal",
                    reason_code="telegram_transport_unclassified",
                    guards=guards,
                    transport_attempted=transport_attempted,
                    telegram_called=telegram_called,
                )
            )
        )
        return 1

    emit_json(
        _to_json(
            _payload(
                status="sent",
                reason_code="sent",
                guards=guards,
                transport_attempted=transport_attempted,
                telegram_called=telegram_called,
                telegram_message_id=_extract_message_id(response),
            )
        )
    )
    return 0


def _first_rejection_reason(guards: Mapping[str, bool]) -> str | None:
    reason_by_guard = [
        ("confirm_send", "confirm_send_required"),
        ("app_env_prod", "app_env_not_prod"),
        ("notification_send_enabled", "notification_send_disabled"),
        ("dry_run_disabled", "notifier_dry_run_enabled"),
        ("bot_token_present", "bot_token_missing"),
        ("canary_chat_id_configured", "canary_chat_id_missing"),
        ("target_chat_id_matches_canary", "target_chat_id_mismatch"),
        ("message_length_valid", "message_length_invalid"),
    ]
    for guard_name, reason_code in reason_by_guard:
        if not guards[guard_name]:
            return reason_code
    return None


def _payload(
    *,
    status: str,
    reason_code: str,
    guards: Mapping[str, bool],
    transport_attempted: bool,
    telegram_called: bool,
    telegram_message_id: int | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": RESTRICTED_TRANSPORT_CANARY_SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "transport_attempted": transport_attempted,
        "telegram_message_id": telegram_message_id,
        "target_chat_id_matched": guards["target_chat_id_matches_canary"],
        "guards": dict(guards),
        "authority": {
            "telegram_called": telegram_called,
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
        },
    }


def _configured_canary_chat_id(env: Mapping[str, str]) -> str:
    return _env_value(env, "TELEGRAM_CANARY_CHAT_ID") or _env_value(env, "NOTIFIER_TELEGRAM_CANARY_CHAT_ID")


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return _normalize_env_value(env.get(name, default))


def _normalize_env_value(value: object) -> str:
    return str(value or "").strip()


def _normalize_message(message: str | None) -> str:
    return str(message or "").strip()


def _request_timeout_sec(env: Mapping[str, str]) -> float:
    try:
        timeout_sec = float(_env_value(env, "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", "10"))
    except ValueError:
        return 10.0
    if timeout_sec <= 0:
        return 10.0
    return timeout_sec


def _extract_message_id(response: object) -> int | None:
    if not isinstance(response, dict):
        return None
    result = response.get("result")
    if not isinstance(result, dict) or result.get("message_id") is None:
        return None
    try:
        return int(result["message_id"])
    except (TypeError, ValueError):
        return None


def _safe_reason_code(value: object, default: str) -> str:
    reason_code = str(value or "")
    if _SAFE_REASON_CODE.fullmatch(reason_code):
        return reason_code
    return default


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
