"""Authorization state machine for collector-telegram C2."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from .config import CollectorTelegramConfig
from .exceptions import AuthorizationError, AuthorizationManualInterventionRequired
from .tdlib_client import build_set_tdlib_parameters_payload, tdlib_json_bytes

JsonDict = dict[str, Any]

AuthorizationState = Literal[
    "booting",
    "waiting_tdlib_parameters",
    "waiting_encryption_key",
    "waiting_phone_number",
    "waiting_code",
    "waiting_password",
    "ready",
    "degraded",
    "closed",
]


@dataclass(slots=True, frozen=True)
class AuthTransitionResult:
    new_state: AuthorizationState
    requests: list[JsonDict] = field(default_factory=list)
    requires_manual_intervention: bool = False
    note: str | None = None


class AuthorizationFSM:
    """Collector authorization state machine.

    Design rules carried from the stage docs:
    - first-time login may require manual operator action,
    - runtime regression back to code/password/phone states is degraded,
    - no automatic human-auth bypass is attempted.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._state: AuthorizationState = "booting"
        self._has_been_ready = False
        self._requires_manual_intervention = False

    def current_state(self) -> AuthorizationState:
        return self._state

    def is_ready(self) -> bool:
        return self._state == "ready"

    def is_degraded(self) -> bool:
        return self._state == "degraded"

    def requires_manual_intervention(self) -> bool:
        return self._requires_manual_intervention

    def handle_state(self, state: JsonDict) -> AuthTransitionResult:
        state_type = state.get("@type")
        if not isinstance(state_type, str):
            raise AuthorizationError("authorization state payload is missing @type")

        if self._has_been_ready and state_type != "authorizationStateReady":
            return self._degraded_regression(state_type)

        match state_type:
            case "authorizationStateWaitTdlibParameters":
                return self._transition(
                    "waiting_tdlib_parameters",
                    requests=[self._build_set_tdlib_parameters_request()],
                )
            case "authorizationStateWaitEncryptionKey":
                return self._transition(
                    "waiting_encryption_key",
                    requests=[self._build_check_database_encryption_key_request()],
                )
            case "authorizationStateWaitPhoneNumber":
                return self._transition(
                    "waiting_phone_number",
                    requests=[self._build_set_authentication_phone_number_request()],
                )
            case "authorizationStateWaitCode":
                return self._manual_transition(
                    "waiting_code",
                    note="Telegram login code required from operator",
                )
            case "authorizationStateWaitOtherDeviceConfirmation":
                return self._manual_transition(
                    "waiting_code",
                    note="Other-device confirmation required from operator",
                )
            case "authorizationStateWaitPassword":
                if self._config.telegram_2fa_password:
                    return self._transition(
                        "waiting_password",
                        requests=[self._build_check_authentication_password_request(self._config.telegram_2fa_password)],
                    )
                return self._manual_transition(
                    "waiting_password",
                    note="Telegram 2FA password required but not configured",
                )
            case "authorizationStateReady":
                self._has_been_ready = True
                self._requires_manual_intervention = False
                return self._transition("ready", note="TDLib authorization ready")
            case "authorizationStateLoggingOut" | "authorizationStateClosing":
                self._state = "degraded"
                return AuthTransitionResult(
                    new_state="degraded",
                    requests=[],
                    requires_manual_intervention=False,
                    note=f"TDLib authorization is leaving ready state: {state_type}",
                )
            case "authorizationStateClosed":
                self._state = "closed"
                return AuthTransitionResult(
                    new_state="closed",
                    requests=[],
                    requires_manual_intervention=False,
                    note="TDLib authorization closed",
                )
            case _:
                raise AuthorizationError(f"Unsupported authorization state: {state_type}")

    def assert_ready(self) -> None:
        if not self.is_ready():
            raise AuthorizationError(f"TDLib authorization is not ready: {self._state}")

    def raise_if_manual_intervention_required(self) -> None:
        if self._requires_manual_intervention:
            raise AuthorizationManualInterventionRequired(
                f"Manual authorization intervention required: current_state={self._state}"
            )

    def _transition(
        self,
        new_state: AuthorizationState,
        *,
        requests: list[JsonDict] | None = None,
        note: str | None = None,
    ) -> AuthTransitionResult:
        self._state = new_state
        self._requires_manual_intervention = False
        return AuthTransitionResult(
            new_state=new_state,
            requests=requests or [],
            requires_manual_intervention=False,
            note=note,
        )

    def _manual_transition(self, new_state: AuthorizationState, *, note: str) -> AuthTransitionResult:
        self._state = new_state
        self._requires_manual_intervention = True
        return AuthTransitionResult(
            new_state=new_state,
            requests=[],
            requires_manual_intervention=True,
            note=note,
        )

    def _degraded_regression(self, tdlib_state_type: str) -> AuthTransitionResult:
        self._state = "degraded"
        self._requires_manual_intervention = True
        note = f"Authorization regressed after ready: {tdlib_state_type}"
        self._logger.warning(note)
        return AuthTransitionResult(
            new_state="degraded",
            requests=[],
            requires_manual_intervention=True,
            note=note,
        )

    def _build_set_tdlib_parameters_request(self) -> JsonDict:
        return build_set_tdlib_parameters_payload(self._config)

    def _build_check_database_encryption_key_request(self) -> JsonDict:
        return {
            "@type": "checkDatabaseEncryptionKey",
            "encryption_key": tdlib_json_bytes(self._config.tdlib_db_encryption_key),
        }

    def _build_set_authentication_phone_number_request(self) -> JsonDict:
        return {
            "@type": "setAuthenticationPhoneNumber",
            "phone_number": self._config.telegram_phone_number,
            "settings": {
                "allow_flash_call": False,
                "allow_missed_call": False,
                "is_current_phone_number": False,
                "allow_sms_retriever_api": False,
            },
        }

    def build_check_authentication_code_request(self, code: str) -> JsonDict:
        return {
            "@type": "checkAuthenticationCode",
            "code": code,
        }

    def _build_check_authentication_password_request(self, password: str) -> JsonDict:
        return {
            "@type": "checkAuthenticationPassword",
            "password": password,
        }
