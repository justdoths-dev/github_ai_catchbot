"""Standalone TDLib auth-only orchestration for collector-telegram.

This module intentionally stays outside the collector runtime/service entrypoint.
It only pumps TDLib authorization states through an injected transport or client.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

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
        "checkAuthenticationPassword",
    }
)


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
    ) -> None:
        if client is None and transport is None:
            raise ValueError("TDLibAuthOnlyRunner requires an injected transport or client")

        self._config = config
        self._client = client or TDLibClient(config, transport=transport)  # type: ignore[arg-type]
        self._fsm = fsm or AuthorizationFSM(config)
        self._logger = logger or logging.getLogger(__name__)
        self._receive_timeout_sec = receive_timeout_sec
        self._max_authorization_updates = max_authorization_updates

    async def run_once(self) -> TDLibAuthOnlyResult:
        requests_sent_count = 0
        final_state: str | None = None
        attempted = False

        try:
            await self._client.initialize()
            attempted = True

            for _ in range(self._max_authorization_updates):
                payload = await self._client.receive(self._receive_timeout_sec)
                auth_state = _extract_authorization_state(payload)
                if auth_state is None:
                    continue

                transition = self._fsm.handle_state(auth_state)
                final_state = transition.new_state

                for request in transition.requests:
                    _assert_auth_request(request)
                    await self._client.send(request)
                    requests_sent_count += 1

                if transition.requires_manual_intervention:
                    return TDLibAuthOnlyResult(
                        auth_entrypoint_status="manual_intervention_required",
                        tdlib_auth_attempted=attempted,
                        manual_intervention_required=True,
                        manual_intervention_reason=transition.note,
                        final_authorization_state=transition.new_state,
                        requests_sent_count=requests_sent_count,
                    )

                if transition.new_state == "ready":
                    return TDLibAuthOnlyResult(
                        auth_entrypoint_status="ready",
                        tdlib_auth_attempted=attempted,
                        tdlib_auth_completed=True,
                        telegram_connected=True,
                        final_authorization_state=transition.new_state,
                        requests_sent_count=requests_sent_count,
                    )

                if transition.new_state in {"degraded", "closed"}:
                    return TDLibAuthOnlyResult(
                        auth_entrypoint_status=transition.new_state,
                        tdlib_auth_attempted=attempted,
                        manual_intervention_required=transition.requires_manual_intervention,
                        manual_intervention_reason=transition.note,
                        final_authorization_state=transition.new_state,
                        requests_sent_count=requests_sent_count,
                    )

            return TDLibAuthOnlyResult(
                auth_entrypoint_status="degraded",
                tdlib_auth_attempted=attempted,
                manual_intervention_required=False,
                manual_intervention_reason="Authorization did not reach ready before max updates",
                final_authorization_state=final_state,
                requests_sent_count=requests_sent_count,
                error="authorization_not_ready",
            )
        except (AuthorizationError, TDLibTransportError, ValueError) as exc:
            self._logger.info(
                "tdlib_auth_only_entrypoint_degraded",
                extra={
                    "service": "collector-telegram",
                    "event": "tdlib_auth_only_entrypoint_degraded",
                    "status": "degraded",
                    "error_type": type(exc).__name__,
                },
            )
            return TDLibAuthOnlyResult(
                auth_entrypoint_status="degraded",
                tdlib_auth_attempted=attempted,
                manual_intervention_required=False,
                final_authorization_state=final_state,
                requests_sent_count=requests_sent_count,
                error=type(exc).__name__,
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
) -> TDLibAuthOnlyResult:
    runner = TDLibAuthOnlyRunner(
        config,
        transport=transport,
        client=client,
        logger=logger,
        receive_timeout_sec=receive_timeout_sec,
        max_authorization_updates=max_authorization_updates,
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


def _assert_auth_request(request: JsonDict) -> None:
    request_type = request.get("@type")
    if request_type not in _AUTH_REQUEST_TYPES:
        raise ValueError(f"Non-auth TDLib request rejected: {request_type}")
