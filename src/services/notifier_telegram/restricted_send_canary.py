from __future__ import annotations

import asyncio
import json
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol


CANARY_NAME = "restricted_telegram_send_canary"
MODE = "restricted_live_send"
API_METHOD = "sendMessage"
DEFAULT_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
DEFAULT_MAX_REQUESTS = 1
DEFAULT_TIMEOUT_MS = 10_000
DEFAULT_MAX_MESSAGE_CHARS = 500
HARD_MAX_MESSAGE_CHARS = 1_000
MAX_RESPONSE_BYTES = 65_536
DEFAULT_CANARY_MESSAGE = "\n".join(
    [
        "[github_ai_catchbot canary]",
        "restricted Telegram send canary.",
        "synthetic transport probe only.",
        "no production candidate, no verdict, no delivery policy.",
    ]
)
CHAT_ID_RE = re.compile(r"^-?[1-9][0-9]{0,30}$")
BLOCKED_ERROR_CODES = frozenset(
    {
        "operator_approval_missing",
        "send_not_allowed",
        "network_not_allowed",
        "credential_missing",
        "chat_id_missing",
        "chat_id_invalid",
        "api_base_url_invalid",
        "request_cap_invalid",
        "request_cap_exceeded",
        "timeout_invalid",
        "message_char_cap_invalid",
        "message_too_long",
        "parse_mode_not_allowed",
        "edit_not_allowed",
        "reply_markup_not_allowed",
    }
)
ERROR_CODES = BLOCKED_ERROR_CODES | frozenset(
    {
        "telegram_rate_limited",
        "telegram_auth_failed",
        "telegram_chat_not_found_or_forbidden",
        "telegram_bad_request",
        "telegram_transient_error",
        "telegram_response_invalid",
    }
)
SIDE_EFFECT_FLAGS = {
    "database_write": False,
    "redis_write": False,
    "outbox_emit": False,
    "notification_plan_write": False,
    "notification_render_write": False,
    "notification_delivery_record_write": False,
    "state_transition_write": False,
    "artifact_snapshot_write": False,
    "judge_run_write": False,
    "judge_output_write": False,
    "analysis_write": False,
    "openai_call": False,
    "github_call": False,
    "x_call": False,
    "web_call": False,
    "worker_started": False,
    "repo_state_mutation": False,
}


class RestrictedTelegramSendTransportProtocol(Protocol):
    async def send_message(
        self,
        *,
        api_base_url: str,
        bot_token: str,
        chat_id: int,
        text: str,
        timeout_ms: int,
        disable_notification: bool,
        protect_content: bool,
        link_preview_options: Mapping[str, bool] | None,
    ) -> "RestrictedTelegramSendHttpResponse": ...


class RedactedTelegramSendCanaryError(RuntimeError):
    def __init__(
        self,
        error_code: str,
        *,
        status_code_class: str | None = None,
        telegram_ok: bool | None = None,
        telegram_message_id_present: bool = False,
        retry_after_seconds: int | None = None,
    ) -> None:
        safe_code = error_code if error_code in ERROR_CODES else "telegram_transient_error"
        self.error_code = safe_code
        self.status_code_class = status_code_class
        self.telegram_ok = telegram_ok
        self.telegram_message_id_present = telegram_message_id_present
        self.retry_after_seconds = retry_after_seconds
        super().__init__(safe_code)


class RestrictedTelegramSendNetworkError(RuntimeError):
    pass


@dataclass(slots=True)
class RestrictedTelegramSendCanaryRequestBudget:
    max_requests: int = DEFAULT_MAX_REQUESTS
    request_count: int = 0
    network_attempted: bool = False

    def acquire(self) -> None:
        if self.request_count >= self.max_requests:
            raise RedactedTelegramSendCanaryError("request_cap_exceeded")
        self.request_count += 1
        self.network_attempted = True


@dataclass(slots=True, frozen=True)
class RestrictedTelegramSendCanaryConfig:
    operator_approved: bool
    allow_send: bool
    allow_network: bool
    bot_token: str | None
    chat_id: str | int | None
    telegram_api_base_url: str = DEFAULT_TELEGRAM_API_BASE_URL
    message: str | None = None
    max_requests: int = DEFAULT_MAX_REQUESTS
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    max_message_chars: int = DEFAULT_MAX_MESSAGE_CHARS
    parse_mode: str | None = None
    edit: bool = False
    reply_markup: Mapping[str, Any] | None = None
    mode: str = MODE
    disable_notification: bool = True
    protect_content: bool = False
    disable_link_preview: bool = True


@dataclass(slots=True, frozen=True)
class RestrictedTelegramSendHttpResponse:
    status_code: int
    payload: Mapping[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class _ValidatedSendTarget:
    api_base_url: str
    bot_token: str
    chat_id: int
    message: str
    timeout_ms: int
    link_preview_options: Mapping[str, bool] | None


@dataclass(slots=True, frozen=True)
class RestrictedTelegramSendCanaryResult:
    canary_name: str
    mode: str
    api_method: str
    target_chat_id_present: bool
    message_chars: int
    network_attempted: bool
    request_count: int
    max_requests: int
    status: str
    ok: bool
    error_code: str | None
    status_code_class: str | None
    telegram_ok: bool | None
    telegram_message_id_present: bool
    retry_after_seconds: int | None
    redactions_applied: tuple[str, ...]
    side_effects: Mapping[str, bool] = field(default_factory=dict)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "canary_name": self.canary_name,
            "mode": self.mode,
            "api_method": self.api_method,
            "target_chat_id_present": self.target_chat_id_present,
            "message_chars": self.message_chars,
            "network_attempted": self.network_attempted,
            "request_count": self.request_count,
            "max_requests": self.max_requests,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "status_code_class": self.status_code_class,
            "telegram_ok": self.telegram_ok,
            "telegram_message_id_present": self.telegram_message_id_present,
            "retry_after_seconds": self.retry_after_seconds,
            "redactions_applied": list(self.redactions_applied),
            "side_effects": dict(self.side_effects),
        }


class RestrictedTelegramSendLiveTransport:
    async def send_message(
        self,
        *,
        api_base_url: str,
        bot_token: str,
        chat_id: int,
        text: str,
        timeout_ms: int,
        disable_notification: bool,
        protect_content: bool,
        link_preview_options: Mapping[str, bool] | None,
    ) -> RestrictedTelegramSendHttpResponse:
        return await asyncio.to_thread(
            self._send_message_sync,
            api_base_url=api_base_url,
            bot_token=bot_token,
            chat_id=chat_id,
            text=text,
            timeout_ms=timeout_ms,
            disable_notification=disable_notification,
            protect_content=protect_content,
            link_preview_options=link_preview_options,
        )

    def _send_message_sync(
        self,
        *,
        api_base_url: str,
        bot_token: str,
        chat_id: int,
        text: str,
        timeout_ms: int,
        disable_notification: bool,
        protect_content: bool,
        link_preview_options: Mapping[str, bool] | None,
    ) -> RestrictedTelegramSendHttpResponse:
        request_payload = _send_message_payload(
            chat_id=chat_id,
            text=text,
            disable_notification=disable_notification,
            protect_content=protect_content,
            link_preview_options=link_preview_options,
        )
        body = json.dumps(request_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            f"{api_base_url}/bot{bot_token}/{API_METHOD}",
            data=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=_timeout_sec(timeout_ms)) as response:
                raw_body = response.read(MAX_RESPONSE_BYTES + 1)
                return RestrictedTelegramSendHttpResponse(
                    status_code=int(response.status),
                    payload=_json_object_or_none(raw_body),
                )
        except urllib.error.HTTPError as exc:
            raw_body = exc.read(MAX_RESPONSE_BYTES + 1)
            return RestrictedTelegramSendHttpResponse(
                status_code=int(exc.code),
                payload=_json_object_or_none(raw_body),
            )
        except (
            urllib.error.URLError,
            TimeoutError,
            socket.timeout,
            ConnectionError,
            ConnectionResetError,
        ) as exc:
            raise RestrictedTelegramSendNetworkError("telegram_transient_error") from exc


class RestrictedTelegramSendCanary:
    def __init__(
        self,
        config: RestrictedTelegramSendCanaryConfig,
        *,
        transport: RestrictedTelegramSendTransportProtocol,
        request_budget: RestrictedTelegramSendCanaryRequestBudget | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._request_budget = request_budget or RestrictedTelegramSendCanaryRequestBudget(
            max_requests=_int_or_zero(config.max_requests)
        )

    async def run(self) -> RestrictedTelegramSendCanaryResult:
        error_code: str | None = None
        status_code_class: str | None = None
        telegram_ok: bool | None = None
        telegram_message_id_present = False
        retry_after_seconds: int | None = None
        message_chars = _input_message_chars(self._config.message)

        try:
            target = self._validate_preconditions()
            message_chars = len(target.message)
            self._request_budget.acquire()
            response = await self._transport.send_message(
                api_base_url=target.api_base_url,
                bot_token=target.bot_token,
                chat_id=target.chat_id,
                text=target.message,
                timeout_ms=target.timeout_ms,
                disable_notification=self._config.disable_notification,
                protect_content=self._config.protect_content,
                link_preview_options=target.link_preview_options,
            )
            observed = _observe_response(response)
            status_code_class = observed.status_code_class
            telegram_ok = observed.telegram_ok
            telegram_message_id_present = observed.telegram_message_id_present
            retry_after_seconds = observed.retry_after_seconds
        except RedactedTelegramSendCanaryError as exc:
            error_code = exc.error_code
            status_code_class = exc.status_code_class
            telegram_ok = exc.telegram_ok
            telegram_message_id_present = exc.telegram_message_id_present
            retry_after_seconds = exc.retry_after_seconds
        except (RestrictedTelegramSendNetworkError, TimeoutError, ConnectionError, ConnectionResetError):
            error_code = "telegram_transient_error"
        except Exception:  # noqa: BLE001 - never expose raw exception details to operators.
            error_code = "telegram_transient_error"

        return self._result(
            error_code=error_code,
            status_code_class=status_code_class,
            telegram_ok=telegram_ok,
            telegram_message_id_present=telegram_message_id_present,
            retry_after_seconds=retry_after_seconds,
            message_chars=message_chars,
        )

    def _validate_preconditions(self) -> _ValidatedSendTarget:
        if not self._config.operator_approved:
            raise RedactedTelegramSendCanaryError("operator_approval_missing")
        if not self._config.allow_send:
            raise RedactedTelegramSendCanaryError("send_not_allowed")
        if not self._config.allow_network:
            raise RedactedTelegramSendCanaryError("network_not_allowed")

        bot_token = str(self._config.bot_token or "").strip()
        if not bot_token:
            raise RedactedTelegramSendCanaryError("credential_missing")

        chat_raw = _chat_id_raw(self._config.chat_id)
        if not chat_raw:
            raise RedactedTelegramSendCanaryError("chat_id_missing")
        if CHAT_ID_RE.fullmatch(chat_raw) is None:
            raise RedactedTelegramSendCanaryError("chat_id_invalid")
        chat_id = int(chat_raw)

        api_base_url = normalize_telegram_api_base_url(self._config.telegram_api_base_url)
        if api_base_url is None:
            raise RedactedTelegramSendCanaryError("api_base_url_invalid")

        if _int_or_zero(self._config.max_requests) != DEFAULT_MAX_REQUESTS:
            raise RedactedTelegramSendCanaryError("request_cap_invalid")

        timeout_ms = _int_or_zero(self._config.timeout_ms)
        if timeout_ms <= 0 or timeout_ms > 60_000:
            raise RedactedTelegramSendCanaryError("timeout_invalid")

        max_message_chars = _int_or_zero(self._config.max_message_chars)
        if max_message_chars <= 0 or max_message_chars > HARD_MAX_MESSAGE_CHARS:
            raise RedactedTelegramSendCanaryError("message_char_cap_invalid")

        message = _effective_message(self._config.message)
        if len(message) > max_message_chars:
            raise RedactedTelegramSendCanaryError("message_too_long")

        if str(self._config.parse_mode or "").strip():
            raise RedactedTelegramSendCanaryError("parse_mode_not_allowed")
        if self._config.edit:
            raise RedactedTelegramSendCanaryError("edit_not_allowed")
        if self._config.reply_markup is not None:
            raise RedactedTelegramSendCanaryError("reply_markup_not_allowed")

        link_preview_options = {"is_disabled": True} if self._config.disable_link_preview else None
        return _ValidatedSendTarget(
            api_base_url=api_base_url,
            bot_token=bot_token,
            chat_id=chat_id,
            message=message,
            timeout_ms=timeout_ms,
            link_preview_options=link_preview_options,
        )

    def _result(
        self,
        *,
        error_code: str | None,
        status_code_class: str | None,
        telegram_ok: bool | None,
        telegram_message_id_present: bool,
        retry_after_seconds: int | None,
        message_chars: int,
    ) -> RestrictedTelegramSendCanaryResult:
        return RestrictedTelegramSendCanaryResult(
            canary_name=CANARY_NAME,
            mode=MODE,
            api_method=API_METHOD,
            target_chat_id_present=bool(_chat_id_raw(self._config.chat_id)),
            message_chars=message_chars,
            network_attempted=self._request_budget.network_attempted,
            request_count=self._request_budget.request_count,
            max_requests=self._request_budget.max_requests,
            status=_result_status(error_code, self._request_budget.network_attempted),
            ok=error_code is None,
            error_code=error_code,
            status_code_class=status_code_class,
            telegram_ok=telegram_ok,
            telegram_message_id_present=telegram_message_id_present,
            retry_after_seconds=retry_after_seconds,
            redactions_applied=_redactions(error_code),
            side_effects=SIDE_EFFECT_FLAGS,
        )


async def run_restricted_telegram_send_canary(
    config: RestrictedTelegramSendCanaryConfig,
    *,
    transport: RestrictedTelegramSendTransportProtocol | None = None,
    request_budget: RestrictedTelegramSendCanaryRequestBudget | None = None,
) -> RestrictedTelegramSendCanaryResult:
    return await RestrictedTelegramSendCanary(
        config,
        transport=transport or RestrictedTelegramSendLiveTransport(),
        request_budget=request_budget,
    ).run()


def normalize_telegram_api_base_url(value: str | None) -> str | None:
    raw = str(value or "").strip()
    try:
        parsed = urllib.parse.urlsplit(raw)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme != "https":
        return None
    if (parsed.hostname or "").lower() != "api.telegram.org":
        return None
    if port is not None:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    if parsed.path not in {"", "/"}:
        return None
    if parsed.query or parsed.fragment:
        return None
    return DEFAULT_TELEGRAM_API_BASE_URL


def _chat_id_raw(value: str | int | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


@dataclass(slots=True, frozen=True)
class _ObservedTelegramResponse:
    status_code_class: str | None
    telegram_ok: bool | None
    telegram_message_id_present: bool
    retry_after_seconds: int | None


def _observe_response(response: RestrictedTelegramSendHttpResponse) -> _ObservedTelegramResponse:
    status_code = _safe_status_code(response.status_code)
    status_code_class = _status_code_class(status_code)
    payload = response.payload
    retry_after_seconds = _retry_after_seconds(payload)

    if status_code == 429 or retry_after_seconds is not None:
        raise RedactedTelegramSendCanaryError(
            "telegram_rate_limited",
            status_code_class=status_code_class,
            telegram_ok=_telegram_ok(payload),
            retry_after_seconds=retry_after_seconds,
        )
    if 500 <= status_code <= 599:
        raise RedactedTelegramSendCanaryError(
            "telegram_transient_error",
            status_code_class=status_code_class,
            telegram_ok=_telegram_ok(payload),
        )
    if status_code in {401, 403} and not isinstance(payload, Mapping):
        raise RedactedTelegramSendCanaryError(
            "telegram_auth_failed",
            status_code_class=status_code_class,
            telegram_ok=None,
        )
    if not isinstance(payload, Mapping) or not isinstance(payload.get("ok"), bool):
        raise RedactedTelegramSendCanaryError(
            "telegram_response_invalid",
            status_code_class=status_code_class,
            telegram_ok=_telegram_ok(payload),
        )

    telegram_ok = bool(payload["ok"])
    if telegram_ok:
        message_id_present = _message_id_present(payload)
        if status_code == 200 and message_id_present:
            return _ObservedTelegramResponse(
                status_code_class=status_code_class,
                telegram_ok=True,
                telegram_message_id_present=True,
                retry_after_seconds=None,
            )
        raise RedactedTelegramSendCanaryError(
            "telegram_response_invalid",
            status_code_class=status_code_class,
            telegram_ok=True,
            telegram_message_id_present=message_id_present,
        )

    description = str(payload.get("description") or "")
    error_code = _classify_telegram_failure(status_code, description)
    raise RedactedTelegramSendCanaryError(
        error_code,
        status_code_class=status_code_class,
        telegram_ok=False,
        retry_after_seconds=retry_after_seconds,
    )


def _classify_telegram_failure(status_code: int, description: str) -> str:
    lowered = description.lower()
    if _looks_like_chat_access_failure(lowered):
        return "telegram_chat_not_found_or_forbidden"
    if status_code in {401, 403}:
        return "telegram_auth_failed"
    if status_code == 400 or _looks_like_bad_request(lowered):
        return "telegram_bad_request"
    if 500 <= status_code <= 599:
        return "telegram_transient_error"
    return "telegram_response_invalid"


def _looks_like_chat_access_failure(lowered_description: str) -> bool:
    markers = (
        "chat not found",
        "bot was blocked",
        "bot blocked",
        "forbidden",
        "bot was kicked",
        "not enough rights",
        "administrator rights",
        "insufficient rights",
    )
    return any(marker in lowered_description for marker in markers)


def _looks_like_bad_request(lowered_description: str) -> bool:
    markers = (
        "bad request",
        "message text is empty",
        "message is too long",
        "can't parse",
        "entities",
        "invalid",
    )
    return any(marker in lowered_description for marker in markers)


def _send_message_payload(
    *,
    chat_id: int,
    text: str,
    disable_notification: bool,
    protect_content: bool,
    link_preview_options: Mapping[str, bool] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_notification": disable_notification,
        "protect_content": protect_content,
    }
    if link_preview_options is not None:
        payload["link_preview_options"] = dict(link_preview_options)
    return payload


def _effective_message(value: str | None) -> str:
    normalized = str(value or "").strip()
    return normalized or DEFAULT_CANARY_MESSAGE


def _input_message_chars(value: str | None) -> int:
    return len(str(value).strip()) if value is not None else 0


def _json_object_or_none(raw_body: bytes) -> Mapping[str, Any] | None:
    if len(raw_body) > MAX_RESPONSE_BYTES:
        return None
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None


def _safe_status_code(value: object) -> int:
    try:
        status_code = int(value)
    except (TypeError, ValueError):
        return 0
    return status_code if 100 <= status_code <= 599 else 0


def _status_code_class(status_code: int) -> str | None:
    if 100 <= status_code <= 599:
        return f"{status_code // 100}xx"
    return None


def _telegram_ok(payload: Mapping[str, Any] | None) -> bool | None:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("ok"), bool):
        return None
    return bool(payload["ok"])


def _retry_after_seconds(payload: Mapping[str, Any] | None) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    parameters = payload.get("parameters")
    if not isinstance(parameters, Mapping) or parameters.get("retry_after") is None:
        return None
    try:
        retry_after = int(parameters["retry_after"])
    except (TypeError, ValueError):
        return None
    return retry_after if retry_after >= 0 else None


def _message_id_present(payload: Mapping[str, Any]) -> bool:
    result = payload.get("result")
    if not isinstance(result, Mapping) or result.get("message_id") is None:
        return False
    try:
        int(result["message_id"])
    except (TypeError, ValueError):
        return False
    return True


def _int_or_zero(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _timeout_sec(timeout_ms: int) -> float:
    return max(0.001, timeout_ms / 1000.0)


def _result_status(error_code: str | None, network_attempted: bool) -> str:
    if error_code is None:
        return "pass"
    if not network_attempted or error_code in BLOCKED_ERROR_CODES:
        return "blocked"
    return "fail"


def _redactions(error_code: str | None) -> tuple[str, ...]:
    values = [
        "bot_token_omitted",
        "bot_api_url_omitted",
        "request_body_omitted",
        "response_body_omitted",
        "response_headers_omitted",
        "message_text_omitted",
    ]
    if error_code is not None:
        values.append("exception_detail_omitted")
    return tuple(values)
