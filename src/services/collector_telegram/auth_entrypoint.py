"""Standalone TDLib auth-only orchestration for collector-telegram.

This module intentionally stays outside the collector runtime/service entrypoint.
It only pumps TDLib authorization states through an injected transport or client.
"""

from __future__ import annotations

import getpass
import logging
import sys
from dataclasses import dataclass, field
from typing import Any, Callable

from .auth_fsm import AuthorizationFSM
from .config import CollectorTelegramConfig
from .exceptions import AuthorizationError, TDLibTransportError
from .tdlib_client import TDJsonTransport, TDLibClient, TDLibTransportProtocol

JsonDict = dict[str, Any]

SCHEMA_VERSION = "tdlib_auth_only_result_v1"
AUTH_ONLY_ENTRYPOINT_LABEL = "src.services.collector_telegram.auth_entrypoint"

_AUTH_REQUEST_TYPES = frozenset(
    {
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "setAuthenticationPhoneNumber",
        "checkAuthenticationCode",
        "checkAuthenticationPassword",
    }
)
_SENSITIVE_AUTH_REQUEST_TYPE_MARKERS = {
    "checkAuthenticationCode": "checkAuthenticationCode_redacted",
}
_SAFE_AUTH_REQUEST_TYPE_MARKERS = frozenset(
    request_type
    for request_type in _AUTH_REQUEST_TYPES
    if request_type not in _SENSITIVE_AUTH_REQUEST_TYPE_MARKERS
) | frozenset(_SENSITIVE_AUTH_REQUEST_TYPE_MARKERS.values())
_UNATTRIBUTED_OK_RESPONSE_NO_EXTRA = "unattributed_no_extra"
_UNATTRIBUTED_OK_RESPONSE_UNKNOWN_EXTRA = "unattributed_unknown_extra"

_TDLIB_ERROR_CATEGORY_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("api_id_related", ("api_id", "api id")),
    ("api_hash_related", ("api_hash", "api hash")),
    (
        "database_directory_related",
        ("database_directory", "database directory", "database dir"),
    ),
    ("files_directory_related", ("files_directory", "files directory", "files dir")),
    ("encryption_key_related", ("encryption", "encryption key")),
    (
        "tdlib_parameters_related",
        ("tdlib parameters", "tdlibparameters", "settdlibparameters"),
    ),
    ("authorization_state_related", ("authorization state", "authorizationstate")),
    (
        "timeout_or_no_update_related",
        ("timeout", "timed out", "no update", "max updates"),
    ),
    ("network_related", ("network", "connection", "socket", "proxy")),
)

_SPECIFIC_COMPLETION_CATEGORIES = (
    "api_id_related",
    "api_hash_related",
    "database_directory_related",
    "files_directory_related",
    "encryption_key_related",
    "authorization_state_related",
    "network_related",
)

_PARAMETERS_RESPONSE_ERROR_OR_TIMEOUT = "tdlib_parameters_response_error_or_timeout"
_PARAMETERS_ACCEPTED_AUTH_STATE_NOT_ADVANCED = (
    "tdlib_parameters_accepted_auth_state_not_advanced_before_max_updates"
)
_AUTH_STATE_NOT_ADVANCED = "tdlib_auth_state_not_advanced_before_max_updates"
_AUTHORIZATION_NOT_READY = "authorization_not_ready_before_max_updates"
_WAITING_PHONE_REQUEST_SENT_AUTH_STATE_NOT_ADVANCED = (
    "waiting_phone_number_request_sent_auth_state_not_advanced_before_max_updates"
)
_WAITING_PHONE_REQUEST_NOT_OBSERVED = (
    "waiting_phone_number_request_not_observed_before_max_updates"
)
_CONNECTION_NOT_READY = "connection_not_ready_before_max_updates"
_SAFE_RESPONSE_TYPE_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._"
)
_SAFE_CONNECTION_STATE_TYPES = frozenset(
    {
        "connectionStateWaitingForNetwork",
        "connectionStateConnecting",
        "connectionStateConnectingToProxy",
        "connectionStateReady",
        "connectionStateUpdating",
    }
)


@dataclass(slots=True, frozen=True)
class _RedactedTDLibError:
    code: int | None
    tdlib_type: str | None
    message_len: int | None
    categories: tuple[str, ...]
    completion_failure_category: str


@dataclass(slots=True, frozen=True)
class TDLibAuthOnlyResult:
    schema_version: str = SCHEMA_VERSION
    auth_entrypoint_status: str = "degraded"
    tdlib_auth_attempted: bool = False
    tdlib_auth_completed: bool = False
    telegram_connected: bool = False
    session_state_created_or_reused: bool = False
    manual_intervention_required: bool = False
    manual_intervention_reason: str | None = None
    final_authorization_state: str | None = None
    requests_sent_count: int = 0
    auth_request_types_sent: tuple[str, ...] = field(default_factory=tuple)
    last_auth_request_type: str | None = None
    authorization_updates_seen_count: int = 0
    non_auth_response_count: int = 0
    non_auth_response_type_counts: dict[str, int] = field(default_factory=dict)
    tdlib_ok_seen: bool = False
    last_non_auth_response_type: str | None = None
    ok_response_count: int = 0
    ok_response_auth_request_types: tuple[str, ...] = field(default_factory=tuple)
    last_ok_response_auth_request_type: str | None = None
    pending_auth_request_types_at_timeout: tuple[str, ...] = field(default_factory=tuple)
    connection_state_updates_seen_count: int = 0
    last_connection_state_type: str | None = None
    connection_state_type_counts: dict[str, int] = field(default_factory=dict)
    max_authorization_updates: int = 0
    receive_timeout_sec: float = 0.0
    runtime_env_values_printed: bool = False
    database_connected: bool = False
    redis_connected: bool = False
    alembic_run: bool = False
    app_runtime_started: bool = False
    live_collector_started: bool = False
    notifier_transport_enabled: bool = False
    production_rollout_performed: bool = False
    secret_values_printed: bool = False
    source_message_persisted: bool = False
    outbox_event_emitted: bool = False
    collector_main_imported: bool = False
    collector_runtime_started: bool = False
    error: str | None = None
    error_present: bool = False
    error_type: str | None = None
    tdlib_error_present: bool = False
    tdlib_error_code: int | None = None
    tdlib_error_type: str | None = None
    tdlib_error_message_len: int | None = None
    tdlib_error_categories: tuple[str, ...] = field(default_factory=tuple)
    completion_failure_category: str | None = None
    login_code_prompted: bool = False
    login_code_submitted: bool = False
    login_code_value_printed: bool = False
    login_code_value_stored: bool = False

    def to_redacted_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "auth_entrypoint_status": self.auth_entrypoint_status,
            "tdlib_auth_attempted": self.tdlib_auth_attempted,
            "tdlib_auth_completed": self.tdlib_auth_completed,
            "telegram_connected": self.telegram_connected,
            "session_state_created_or_reused": self.session_state_created_or_reused,
            "manual_intervention_required": self.manual_intervention_required,
            "manual_intervention_reason": self.manual_intervention_reason,
            "final_authorization_state": self.final_authorization_state,
            "requests_sent_count": self.requests_sent_count,
            "auth_request_types_sent": list(self.auth_request_types_sent),
            "last_auth_request_type": self.last_auth_request_type,
            "authorization_updates_seen_count": self.authorization_updates_seen_count,
            "non_auth_response_count": self.non_auth_response_count,
            "non_auth_response_type_counts": dict(self.non_auth_response_type_counts),
            "tdlib_ok_seen": self.tdlib_ok_seen,
            "last_non_auth_response_type": self.last_non_auth_response_type,
            "ok_response_count": self.ok_response_count,
            "ok_response_auth_request_types": list(
                self.ok_response_auth_request_types
            ),
            "last_ok_response_auth_request_type": (
                self.last_ok_response_auth_request_type
            ),
            "pending_auth_request_types_at_timeout": list(
                self.pending_auth_request_types_at_timeout
            ),
            "connection_state_updates_seen_count": self.connection_state_updates_seen_count,
            "last_connection_state_type": self.last_connection_state_type,
            "connection_state_type_counts": dict(self.connection_state_type_counts),
            "max_authorization_updates": self.max_authorization_updates,
            "receive_timeout_sec": self.receive_timeout_sec,
            "runtime_env_values_printed": self.runtime_env_values_printed,
            "database_connected": self.database_connected,
            "redis_connected": self.redis_connected,
            "alembic_run": self.alembic_run,
            "app_runtime_started": self.app_runtime_started,
            "live_collector_started": self.live_collector_started,
            "notifier_transport_enabled": self.notifier_transport_enabled,
            "production_rollout_performed": self.production_rollout_performed,
            "secret_values_printed": self.secret_values_printed,
            "source_message_persisted": self.source_message_persisted,
            "outbox_event_emitted": self.outbox_event_emitted,
            "collector_main_imported": self.collector_main_imported,
            "collector_runtime_started": self.collector_runtime_started,
            "error": self.error,
            "error_present": self.error_present,
            "error_type": self.error_type,
            "tdlib_error_present": self.tdlib_error_present,
            "tdlib_error_code": self.tdlib_error_code,
            "tdlib_error_type": self.tdlib_error_type,
            "tdlib_error_message_len": self.tdlib_error_message_len,
            "tdlib_error_categories": list(self.tdlib_error_categories),
            "completion_failure_category": self.completion_failure_category,
            "login_code_prompted": self.login_code_prompted,
            "login_code_submitted": self.login_code_submitted,
            "login_code_value_printed": self.login_code_value_printed,
            "login_code_value_stored": self.login_code_value_stored,
        }


class TDLibAuthOnlyRunner:
    """One-shot auth-state pump using injected TDLib boundaries only."""

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        transport: TDLibTransportProtocol | None = None,
        client: TDLibClient | None = None,
        fsm: AuthorizationFSM | None = None,
        logger: logging.Logger | None = None,
        receive_timeout_sec: float = 1.0,
        max_authorization_updates: int = 20,
        approved_tdlib_auth_code_entry: bool = False,
        login_code_prompt: Callable[[str], str] | None = None,
        login_code_entry_is_interactive: Callable[[], bool] | None = None,
    ) -> None:
        if client is None and transport is None:
            raise ValueError("TDLibAuthOnlyRunner requires an injected transport or client")

        self._config = config
        self._client = client or TDLibClient(config, transport=transport)  # type: ignore[arg-type]
        self._fsm = fsm or AuthorizationFSM(config)
        self._logger = logger or logging.getLogger(__name__)
        self._receive_timeout_sec = receive_timeout_sec
        self._max_authorization_updates = max_authorization_updates
        self._approved_tdlib_auth_code_entry = approved_tdlib_auth_code_entry
        self._login_code_prompt = login_code_prompt or getpass.getpass
        self._login_code_entry_is_interactive = (
            login_code_entry_is_interactive or sys.stdin.isatty
        )

    async def run_once(self) -> TDLibAuthOnlyResult:
        requests_sent_count = 0
        auth_request_types_sent: list[str] = []
        last_auth_request_type: str | None = None
        authorization_updates_seen_count = 0
        non_auth_response_count = 0
        non_auth_response_type_counts: dict[str, int] = {}
        tdlib_ok_seen = False
        last_non_auth_response_type: str | None = None
        ok_response_count = 0
        ok_response_auth_request_types: list[str] = []
        last_ok_response_auth_request_type: str | None = None
        pending_auth_requests_by_extra: dict[str, str] = {}
        auth_request_sequence = 0
        connection_state_updates_seen_count = 0
        last_connection_state_type: str | None = None
        connection_state_type_counts: dict[str, int] = {}
        connection_state_ready_seen = False
        final_state: str | None = None
        attempted = False
        login_code_prompted = False
        login_code_submitted = False

        def result(**kwargs: Any) -> TDLibAuthOnlyResult:
            return TDLibAuthOnlyResult(
                auth_request_types_sent=tuple(auth_request_types_sent),
                last_auth_request_type=last_auth_request_type,
                authorization_updates_seen_count=authorization_updates_seen_count,
                non_auth_response_count=non_auth_response_count,
                non_auth_response_type_counts=dict(non_auth_response_type_counts),
                tdlib_ok_seen=tdlib_ok_seen,
                last_non_auth_response_type=last_non_auth_response_type,
                ok_response_count=ok_response_count,
                ok_response_auth_request_types=tuple(ok_response_auth_request_types),
                last_ok_response_auth_request_type=last_ok_response_auth_request_type,
                connection_state_updates_seen_count=connection_state_updates_seen_count,
                last_connection_state_type=last_connection_state_type,
                connection_state_type_counts=dict(connection_state_type_counts),
                max_authorization_updates=self._max_authorization_updates,
                receive_timeout_sec=self._receive_timeout_sec,
                login_code_prompted=login_code_prompted,
                login_code_submitted=login_code_submitted,
                login_code_value_printed=False,
                login_code_value_stored=False,
                **kwargs,
            )

        async def send_auth_request(request: JsonDict) -> None:
            nonlocal auth_request_sequence
            nonlocal last_auth_request_type
            nonlocal requests_sent_count

            _assert_auth_request(request)
            request_type = _extract_safe_auth_request_type(request)
            if request_type is None:
                raise ValueError("Auth TDLib request type could not be sanitized")
            auth_request_sequence += 1
            extra = _build_auth_request_extra(
                sequence=auth_request_sequence,
                request_type=request_type,
            )
            request_with_extra = _auth_request_with_extra(request, extra)
            await self._client.send(request_with_extra)
            requests_sent_count += 1
            auth_request_types_sent.append(request_type)
            last_auth_request_type = request_type
            pending_auth_requests_by_extra[extra] = request_type

        try:
            await self._client.initialize()
            attempted = True

            for _ in range(self._max_authorization_updates):
                payload = await self._client.receive(self._receive_timeout_sec)
                auth_state = _extract_authorization_state(payload)
                if auth_state is None:
                    response_type = _extract_safe_response_type(payload)
                    if response_type is not None:
                        non_auth_response_count += 1
                        last_non_auth_response_type = response_type
                        non_auth_response_type_counts[response_type] = (
                            non_auth_response_type_counts.get(response_type, 0) + 1
                        )
                        if response_type == "ok":
                            ok_response_count += 1
                            ok_request_type = _pop_ok_auth_request_type_by_extra(
                                payload,
                                pending_auth_requests_by_extra,
                            )
                            if ok_request_type == "setTdlibParameters":
                                tdlib_ok_seen = True
                            ok_response_auth_request_types.append(ok_request_type)
                            last_ok_response_auth_request_type = ok_request_type

                    connection_state_type = _extract_safe_connection_state_type(payload)
                    if connection_state_type is not None:
                        connection_state_updates_seen_count += 1
                        last_connection_state_type = connection_state_type
                        connection_state_type_counts[connection_state_type] = (
                            connection_state_type_counts.get(connection_state_type, 0) + 1
                        )
                        if connection_state_type == "connectionStateReady":
                            connection_state_ready_seen = True

                    tdlib_error = _extract_tdlib_error(payload)
                    if tdlib_error is not None:
                        classification = _classify_tdlib_error(
                            tdlib_error,
                            final_authorization_state=final_state,
                            requests_sent_count=requests_sent_count,
                        )
                        return result(
                            auth_entrypoint_status="degraded",
                            tdlib_auth_attempted=attempted,
                            manual_intervention_required=False,
                            final_authorization_state=final_state,
                            requests_sent_count=requests_sent_count,
                            error="tdlib_error_redacted",
                            error_present=True,
                            error_type="tdlib_error",
                            tdlib_error_present=True,
                            tdlib_error_code=classification.code,
                            tdlib_error_type=classification.tdlib_type,
                            tdlib_error_message_len=classification.message_len,
                            tdlib_error_categories=classification.categories,
                            completion_failure_category=(
                                classification.completion_failure_category
                            ),
                        )

                    continue

                authorization_updates_seen_count += 1
                transition = self._fsm.handle_state(auth_state)
                final_state = transition.new_state

                for request in transition.requests:
                    await send_auth_request(request)

                if transition.requires_manual_intervention:
                    if (
                        self._approved_tdlib_auth_code_entry
                        and auth_state.get("@type") == "authorizationStateWaitCode"
                    ):
                        if not self._login_code_entry_is_interactive():
                            return result(
                                auth_entrypoint_status="blocked_tdlib_auth_code_entry_not_interactive",
                                tdlib_auth_attempted=attempted,
                                manual_intervention_required=False,
                                manual_intervention_reason=(
                                    "Telegram login code entry requires an interactive terminal"
                                ),
                                final_authorization_state=transition.new_state,
                                requests_sent_count=requests_sent_count,
                                error="tdlib_auth_code_entry_not_interactive",
                                error_present=True,
                                error_type="tdlib_auth_code_entry_not_interactive",
                                completion_failure_category=(
                                    "tdlib_auth_code_entry_not_interactive"
                                ),
                            )

                        login_code_prompted = True
                        login_code = self._login_code_prompt("Telegram login code: ")
                        await send_auth_request(
                            self._fsm.build_check_authentication_code_request(login_code)
                        )
                        del login_code
                        login_code_submitted = True
                        continue

                    return result(
                        auth_entrypoint_status="manual_intervention_required",
                        tdlib_auth_attempted=attempted,
                        manual_intervention_required=True,
                        manual_intervention_reason=transition.note,
                        final_authorization_state=transition.new_state,
                        requests_sent_count=requests_sent_count,
                    )

                if transition.new_state == "ready":
                    return result(
                        auth_entrypoint_status="ready",
                        tdlib_auth_attempted=attempted,
                        tdlib_auth_completed=True,
                        telegram_connected=True,
                        final_authorization_state=transition.new_state,
                        requests_sent_count=requests_sent_count,
                    )

                if transition.new_state in {"degraded", "closed"}:
                    return result(
                        auth_entrypoint_status=transition.new_state,
                        tdlib_auth_attempted=attempted,
                        manual_intervention_required=transition.requires_manual_intervention,
                        manual_intervention_reason=transition.note,
                        final_authorization_state=transition.new_state,
                        requests_sent_count=requests_sent_count,
                    )

            return result(
                auth_entrypoint_status="degraded",
                tdlib_auth_attempted=attempted,
                manual_intervention_required=False,
                manual_intervention_reason="Authorization did not reach ready before max updates",
                final_authorization_state=final_state,
                requests_sent_count=requests_sent_count,
                error="authorization_not_ready",
                error_present=True,
                error_type="completion_failure",
                tdlib_error_categories=_completion_categories_for_not_ready(
                    final_authorization_state=final_state,
                    requests_sent_count=requests_sent_count,
                    connection_state_ready_seen=connection_state_ready_seen,
                ),
                completion_failure_category=_completion_failure_category_for_not_ready(
                    final_authorization_state=final_state,
                    requests_sent_count=requests_sent_count,
                    tdlib_ok_seen=tdlib_ok_seen,
                    last_auth_request_type=last_auth_request_type,
                ),
                pending_auth_request_types_at_timeout=tuple(
                    pending_auth_requests_by_extra.values()
                ),
            )
        except (AuthorizationError, TDLibTransportError, ValueError) as exc:
            error_type = type(exc).__name__
            self._logger.info(
                "tdlib_auth_only_entrypoint_degraded",
                extra={
                    "service": "collector-telegram",
                    "event": "tdlib_auth_only_entrypoint_degraded",
                    "status": "degraded",
                    "error_type": error_type,
                },
            )
            categories = _categories_for_exception(error_type)
            return result(
                auth_entrypoint_status="degraded",
                tdlib_auth_attempted=attempted,
                manual_intervention_required=False,
                final_authorization_state=final_state,
                requests_sent_count=requests_sent_count,
                error=error_type,
                error_present=True,
                error_type=error_type,
                tdlib_error_categories=categories,
                completion_failure_category=_completion_failure_category_from_categories(
                    categories,
                    default=error_type,
                ),
            )
        finally:
            try:
                await self._client.close()
            except TDLibTransportError:
                self._logger.info(
                    "tdlib_auth_only_entrypoint_close_degraded",
                    extra={
                        "service": "collector-telegram",
                        "event": "tdlib_auth_only_entrypoint_close_degraded",
                        "status": "degraded",
                    },
                )


async def run_tdlib_auth_only_once(
    config: CollectorTelegramConfig,
    *,
    transport: TDLibTransportProtocol | None = None,
    client: TDLibClient | None = None,
    logger: logging.Logger | None = None,
    receive_timeout_sec: float = 1.0,
    max_authorization_updates: int = 20,
    approved_tdlib_auth_code_entry: bool = False,
    login_code_prompt: Callable[[str], str] | None = None,
    login_code_entry_is_interactive: Callable[[], bool] | None = None,
) -> TDLibAuthOnlyResult:
    runner = TDLibAuthOnlyRunner(
        config,
        transport=transport,
        client=client,
        logger=logger,
        receive_timeout_sec=receive_timeout_sec,
        max_authorization_updates=max_authorization_updates,
        approved_tdlib_auth_code_entry=approved_tdlib_auth_code_entry,
        login_code_prompt=login_code_prompt,
        login_code_entry_is_interactive=login_code_entry_is_interactive,
    )
    return await runner.run_once()


def build_real_tdlib_transport() -> TDLibTransportProtocol:
    transport = TDJsonTransport()
    transport.assert_available()
    return transport


def _extract_authorization_state(payload: JsonDict | None) -> JsonDict | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("@type") != "updateAuthorizationState":
        return None
    state = payload.get("authorization_state")
    if not isinstance(state, dict):
        return None
    return state


def _extract_tdlib_error(payload: JsonDict | None) -> JsonDict | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("@type") != "error":
        return None
    return payload


def _extract_safe_response_type(payload: JsonDict | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw_type = payload.get("@type")
    if not isinstance(raw_type, str):
        return None
    if not raw_type or len(raw_type) > 80:
        return "unrecognized"
    if not all(char in _SAFE_RESPONSE_TYPE_CHARS for char in raw_type):
        return "unrecognized"
    return raw_type


def _extract_safe_connection_state_type(payload: JsonDict | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("@type") != "updateConnectionState":
        return None
    state = payload.get("state")
    if not isinstance(state, dict):
        return "unrecognized"
    raw_type = state.get("@type")
    if raw_type in _SAFE_CONNECTION_STATE_TYPES:
        return raw_type
    return "unrecognized"


def _classify_tdlib_error(
    payload: JsonDict,
    *,
    final_authorization_state: str | None,
    requests_sent_count: int,
) -> _RedactedTDLibError:
    raw_code = payload.get("code")
    code = raw_code if isinstance(raw_code, int) and not isinstance(raw_code, bool) else None
    raw_type = payload.get("@type")
    tdlib_type = raw_type if isinstance(raw_type, str) else None
    raw_message = payload.get("message")
    message = raw_message if isinstance(raw_message, str) else None
    categories = _categories_from_message(message)

    if final_authorization_state == "waiting_tdlib_parameters" and requests_sent_count >= 1:
        categories = _append_category(categories, "tdlib_parameters_related")

    if not categories:
        categories = ("unclassified_redacted_error",)

    return _RedactedTDLibError(
        code=code,
        tdlib_type=tdlib_type,
        message_len=len(message) if message is not None else None,
        categories=categories,
        completion_failure_category=_completion_failure_category_from_categories(
            categories,
            default="unclassified_redacted_error",
        ),
    )


def _categories_from_message(message: str | None) -> tuple[str, ...]:
    if message is None:
        return ()

    lowered = message.lower()
    categories: tuple[str, ...] = ()
    for category, keywords in _TDLIB_ERROR_CATEGORY_KEYWORDS:
        if any(keyword in lowered for keyword in keywords):
            categories = _append_category(categories, category)
    return categories


def _append_category(categories: tuple[str, ...], category: str) -> tuple[str, ...]:
    if category in categories:
        return categories
    return (*categories, category)


def _completion_categories_for_not_ready(
    *,
    final_authorization_state: str | None,
    requests_sent_count: int,
    connection_state_ready_seen: bool,
) -> tuple[str, ...]:
    categories: tuple[str, ...]
    if final_authorization_state == "waiting_tdlib_parameters" and requests_sent_count == 1:
        categories = ("tdlib_parameters_related", "timeout_or_no_update_related")
    else:
        categories = ("timeout_or_no_update_related",)
    if not connection_state_ready_seen:
        categories = _append_category(categories, _CONNECTION_NOT_READY)
    return categories


def _completion_failure_category_for_not_ready(
    *,
    final_authorization_state: str | None,
    requests_sent_count: int,
    tdlib_ok_seen: bool,
    last_auth_request_type: str | None,
) -> str:
    if final_authorization_state == "waiting_tdlib_parameters" and requests_sent_count == 1:
        if tdlib_ok_seen:
            return _PARAMETERS_ACCEPTED_AUTH_STATE_NOT_ADVANCED
        return _AUTH_STATE_NOT_ADVANCED
    if final_authorization_state == "waiting_phone_number":
        if last_auth_request_type == "setAuthenticationPhoneNumber":
            return _WAITING_PHONE_REQUEST_SENT_AUTH_STATE_NOT_ADVANCED
        return _WAITING_PHONE_REQUEST_NOT_OBSERVED
    return _AUTHORIZATION_NOT_READY


def _categories_for_exception(error_type: str) -> tuple[str, ...]:
    if error_type == "AuthorizationError":
        return ("authorization_state_related",)
    if error_type == "TDLibTransportError":
        return ("network_related",)
    return ("unclassified_redacted_error",)


def _completion_failure_category_from_categories(
    categories: tuple[str, ...],
    *,
    default: str,
) -> str:
    for category in _SPECIFIC_COMPLETION_CATEGORIES:
        if category in categories:
            return category
    if "tdlib_parameters_related" in categories:
        return _PARAMETERS_RESPONSE_ERROR_OR_TIMEOUT
    if "timeout_or_no_update_related" in categories:
        return _AUTHORIZATION_NOT_READY
    return default


def _assert_auth_request(request: JsonDict) -> None:
    request_type = request.get("@type")
    if request_type not in _AUTH_REQUEST_TYPES:
        raise ValueError(f"Non-auth TDLib request rejected: {request_type}")


def _extract_safe_auth_request_type(request: JsonDict) -> str | None:
    request_type = request.get("@type")
    sensitive_marker = _SENSITIVE_AUTH_REQUEST_TYPE_MARKERS.get(request_type)
    if sensitive_marker is not None:
        return sensitive_marker
    if request_type in _SAFE_AUTH_REQUEST_TYPE_MARKERS:
        return request_type
    return None


def _build_auth_request_extra(*, sequence: int, request_type: str) -> str:
    if request_type not in _SAFE_AUTH_REQUEST_TYPE_MARKERS:
        raise ValueError("Auth TDLib request type could not be sanitized")
    return f"auth:{sequence}:{request_type}"


def _auth_request_with_extra(request: JsonDict, extra: str) -> JsonDict:
    request_with_extra = dict(request)
    request_with_extra["@extra"] = extra
    return request_with_extra


def _pop_ok_auth_request_type_by_extra(
    payload: JsonDict | None,
    pending_auth_requests_by_extra: dict[str, str],
) -> str:
    if not isinstance(payload, dict) or "@extra" not in payload:
        return _UNATTRIBUTED_OK_RESPONSE_NO_EXTRA

    raw_extra = payload.get("@extra")
    if not isinstance(raw_extra, str):
        return _UNATTRIBUTED_OK_RESPONSE_UNKNOWN_EXTRA

    request_type = pending_auth_requests_by_extra.pop(raw_extra, None)
    if request_type is None:
        return _UNATTRIBUTED_OK_RESPONSE_UNKNOWN_EXTRA
    return request_type
