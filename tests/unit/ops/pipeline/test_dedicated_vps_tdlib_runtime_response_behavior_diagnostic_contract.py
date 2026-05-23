from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_tdlib_runtime_response_behavior_diagnostic.py"
)

FAKE_DATABASE_URL = "postgresql://collector:unit-db-secret@localhost:5432/catchbot"
FAKE_REDIS_URL = "redis://:unit-redis-secret@localhost:6379/0"
FAKE_API_HASH = "0123456789abcdef0123456789abcdef"
FAKE_PHONE_NUMBER = "+15551234567"
FAKE_TDLIB_KEY = "unit-tdlib-db-encryption-key"
FAKE_2FA_PASSWORD = "unit two factor password"

FORBIDDEN_REQUEST_TYPES = {
    "searchPublicChat",
    "joinChat",
    "joinChatByInviteLink",
    "getChatHistory",
    "getMessageLink",
    "checkAuthenticationCode",
    "checkAuthenticationPassword",
}

NON_TDLIB_SIDE_EFFECT_FLAGS = (
    "tdlib_auth_attempted",
    "public_username_resolve_called",
    "join_called",
    "history_fetch_called",
    "database_mutation_performed",
    "redis_mutation_performed",
    "live_collector_started",
    "collector_runtime_started",
    "notifier_transport_enabled",
    "outbox_relay_started",
    "router_normalizer_started",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "telegram_channel_registry_mutation_performed",
)


class FakeTDLibTransport:
    def __init__(
        self,
        payloads: list[dict[str, Any] | None | Any],
        *,
        initialize_error: Exception | None = None,
        receive_error: Exception | None = None,
    ) -> None:
        self.payloads = list(payloads)
        self.initialize_error = initialize_error
        self.receive_error = receive_error
        self.initialized = False
        self.closed = False
        self.sent_requests: list[dict[str, Any]] = []
        self.receive_timeouts: list[float] = []

    async def initialize(self) -> None:
        if self.initialize_error is not None:
            raise self.initialize_error
        self.initialized = True

    async def send(self, request: dict[str, Any]) -> None:
        self.sent_requests.append(dict(request))

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        if self.receive_error is not None:
            raise self.receive_error
        self.receive_timeouts.append(timeout)
        if not self.payloads:
            return None
        payload = self.payloads.pop(0)
        if callable(payload):
            payload = payload(self)
        return payload

    async def close(self) -> None:
        self.closed = True


def _module():
    from scripts.ops import (
        dedicated_vps_tdlib_runtime_response_behavior_diagnostic as module,
    )

    return module


def _runtime_env(tmp_path: Path) -> dict[str, str]:
    return {
        "APP_ENV": "dev",
        "COLLECTOR_MODE": "replay",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "TELEGRAM_API_ID": "123456",
        "TELEGRAM_API_HASH": FAKE_API_HASH,
        "TELEGRAM_PHONE_NUMBER": FAKE_PHONE_NUMBER,
        "TELEGRAM_2FA_PASSWORD": FAKE_2FA_PASSWORD,
        "TDLIB_DB_ENCRYPTION_KEY": FAKE_TDLIB_KEY,
        "TDLIB_STATE_DIR": str(tmp_path / "tdlib-state"),
        "TDLIB_FILES_DIR": str(tmp_path / "tdlib-files"),
    }


def _auth_update(state_type: str) -> dict[str, Any]:
    return {
        "@type": "updateAuthorizationState",
        "authorization_state": {"@type": state_type},
    }


def _response_for_last_request(
    request_type: str,
    payload: dict[str, Any],
) -> Any:
    def response(transport: FakeTDLibTransport) -> dict[str, Any]:
        for request in reversed(transport.sent_requests):
            if request.get("@type") == request_type:
                extra = request.get("@extra")
                return {**payload, "@extra": extra}
        raise AssertionError(f"{request_type} was not sent")

    return response


def _run_report(
    *,
    tmp_path: Path,
    transport: FakeTDLibTransport | None = None,
    approved: bool = True,
    max_observations: int = 6,
) -> tuple[dict[str, Any], FakeTDLibTransport | None]:
    fake_transport = transport
    transport_called = False

    def transport_factory(_values: dict[str, str]) -> FakeTDLibTransport:
        nonlocal transport_called
        transport_called = True
        if fake_transport is None:
            raise AssertionError("transport factory should not be called")
        return fake_transport

    result = _module().generate_report(
        repo_root=ROOT,
        runtime_env_path="/safe/unit/runtime.env",
        max_observations=max_observations,
        receive_timeout_sec=0.0,
        overall_timeout_sec=1.0,
        approved_tdlib_runtime_response_diagnostic=approved,
        runtime_env_reader=lambda _path: _runtime_env(tmp_path),
        tdjson_availability_checker=lambda _values: True,
        transport_factory=transport_factory,
    )
    if approved:
        assert transport_called is True
    else:
        assert transport_called is False
    return result.report, fake_transport


def _sent_request_types(transport: FakeTDLibTransport) -> list[str]:
    return [str(request.get("@type")) for request in transport.sent_requests]


def _render(report: dict[str, Any]) -> str:
    return _module().render_json(report)


def _assert_sensitive_values_absent(rendered: str, tmp_path: Path) -> None:
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_REDIS_URL not in rendered
    assert FAKE_API_HASH not in rendered
    assert FAKE_PHONE_NUMBER not in rendered
    assert FAKE_TDLIB_KEY not in rendered
    assert FAKE_2FA_PASSWORD not in rendered
    assert str(tmp_path) not in rendered


def _assert_non_tdlib_side_effects_false(report: dict[str, Any]) -> None:
    for flag in NON_TDLIB_SIDE_EFFECT_FLAGS:
        assert report[flag] is False, flag


def test_direct_help_execution_succeeds_without_pythonpath() -> None:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr
    assert "No module named 'scripts'" not in result.stderr


def test_dry_run_requires_approval_without_tdlib_side_effects(tmp_path: Path) -> None:
    report, _transport = _run_report(tmp_path=tmp_path, transport=None, approved=False)
    rendered = _render(report)

    assert report["contract_status"] == "dry_run_runtime_probe_requires_approval"
    assert report["runtime_env_read"] is True
    assert report["collector_config_built"] is True
    assert report["tdjson_available"] is True
    assert report["approved_runtime_probe"] is False
    assert report["tdlib_initialized"] is False
    assert report["tdlib_closed"] is False
    assert report["tdlib_send_called"] is False
    assert report["tdlib_receive_called"] is False
    assert report["request_types_sent"] == []
    assert report["observation_count_bucket"] == "zero"
    _assert_non_tdlib_side_effects_false(report)
    _assert_sensitive_values_absent(rendered, tmp_path)


def test_approved_probe_correlates_set_parameters_ok(tmp_path: Path) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request("setTdlibParameters", {"@type": "ok"}),
        ]
    )

    report, used_transport = _run_report(tmp_path=tmp_path, transport=transport)

    assert used_transport is transport
    assert report["contract_status"] == "tdlib_runtime_response_diagnostic_completed"
    assert report["tdlib_initialized"] is True
    assert report["tdlib_closed"] is True
    assert report["tdlib_send_called"] is True
    assert report["tdlib_receive_called"] is True
    assert _sent_request_types(transport) == [
        "getAuthorizationState",
        "setTdlibParameters",
    ]
    set_request = transport.sent_requests[1]
    assert isinstance(set_request.get("@extra"), str)
    assert str(set_request["@extra"]).startswith(f"{_module().SCRIPT_NAME}:")
    assert report["request_types_sent"] == [
        "getAuthorizationState",
        "setTdlibParameters",
    ]
    assert report["set_parameters_request_sent"] is True
    assert report["set_parameters_request_extra_present"] is True
    assert report["set_parameters_response_seen"] is True
    assert report["set_parameters_response_type"] == "ok"
    assert report["set_parameters_response_extra_matched"] is True
    assert report["set_parameters_error_code"] is None
    assert report["set_parameters_error_class"] is None
    assert report["timed_out_waiting_for_set_parameters_response"] is False
    _assert_non_tdlib_side_effects_false(report)


def test_approved_probe_reports_sanitized_set_parameters_error(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request(
                "setTdlibParameters",
                {
                    "@type": "error",
                    "code": 400,
                    "message": (
                        f"{FAKE_API_HASH} {FAKE_PHONE_NUMBER} {FAKE_TDLIB_KEY} "
                        f"{FAKE_DATABASE_URL} {tmp_path}"
                    ),
                },
            ),
        ]
    )

    report, _used_transport = _run_report(tmp_path=tmp_path, transport=transport)
    rendered = _render(report)

    assert report["contract_status"] == "blocked_set_parameters_error_observed"
    assert report["set_parameters_response_seen"] is True
    assert report["set_parameters_response_type"] == "error"
    assert report["set_parameters_response_extra_matched"] is True
    assert report["set_parameters_error_code"] == 400
    assert report["set_parameters_error_class"] == "tdlib_error"
    assert report["transport_error_class"] is None
    assert "set_parameters.error_response" in report["checks_failed"]
    _assert_sensitive_values_absent(rendered, tmp_path)
    _assert_non_tdlib_side_effects_false(report)


def test_approved_probe_times_out_after_set_parameters_without_correlated_response(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            None,
            None,
            None,
        ]
    )

    report, _used_transport = _run_report(
        tmp_path=tmp_path,
        transport=transport,
        max_observations=4,
    )

    assert report["contract_status"] == "blocked_set_parameters_response_timeout"
    assert report["set_parameters_request_sent"] is True
    assert report["set_parameters_response_seen"] is False
    assert report["set_parameters_response_extra_matched"] is False
    assert report["timed_out_waiting_for_set_parameters_response"] is True
    assert report["final_authorization_state"] == "authorizationStateWaitTdlibParameters"
    assert report["receive_attempt_count_bucket"] == "two_to_five"
    assert _sent_request_types(transport) == [
        "getAuthorizationState",
        "setTdlibParameters",
    ]
    _assert_non_tdlib_side_effects_false(report)


def test_approved_probe_records_wait_encryption_key_transition_without_key_check(
    tmp_path: Path,
) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request("setTdlibParameters", {"@type": "ok"}),
            _auth_update("authorizationStateWaitEncryptionKey"),
        ]
    )

    report, _used_transport = _run_report(tmp_path=tmp_path, transport=transport)

    assert report["contract_status"] == "tdlib_runtime_response_diagnostic_completed"
    assert report["set_parameters_response_seen"] is True
    assert report["set_parameters_response_type"] == "ok"
    assert report["authorization_states_seen"] == [
        "authorizationStateWaitTdlibParameters",
        "authorizationStateWaitEncryptionKey",
    ]
    assert report["final_authorization_state"] == "authorizationStateWaitEncryptionKey"
    assert (
        report["transition_after_set_parameters"]
        == "authorizationStateWaitEncryptionKey"
    )
    assert "checkDatabaseEncryptionKey" not in _sent_request_types(transport)
    _assert_non_tdlib_side_effects_false(report)


def test_forbidden_request_types_are_not_sent(tmp_path: Path) -> None:
    transport = FakeTDLibTransport(
        [
            _auth_update("authorizationStateWaitTdlibParameters"),
            _response_for_last_request("setTdlibParameters", {"@type": "ok"}),
            _auth_update("authorizationStateReady"),
        ]
    )

    report, _used_transport = _run_report(tmp_path=tmp_path, transport=transport)
    sent_types = set(_sent_request_types(transport))

    assert report["contract_status"] == "tdlib_runtime_response_diagnostic_completed"
    assert sent_types == {"getAuthorizationState", "setTdlibParameters"}
    assert sent_types.isdisjoint(FORBIDDEN_REQUEST_TYPES)
    assert set(report["request_types_sent"]).isdisjoint(FORBIDDEN_REQUEST_TYPES)
    _assert_non_tdlib_side_effects_false(report)


def test_transport_receive_failure_reports_class_only(tmp_path: Path) -> None:
    transport = FakeTDLibTransport(
        [_auth_update("authorizationStateWaitTdlibParameters")],
        receive_error=RuntimeError(
            f"{FAKE_API_HASH} {FAKE_PHONE_NUMBER} {FAKE_TDLIB_KEY} {tmp_path}"
        ),
    )

    report, _used_transport = _run_report(tmp_path=tmp_path, transport=transport)
    rendered = json.dumps(report, sort_keys=True)

    assert report["contract_status"] == "blocked_unexpected_error"
    assert report["transport_error_class"] == "TDLibTransportError"
    assert "tdlib.receive_failed" in report["checks_failed"]
    _assert_sensitive_values_absent(rendered, tmp_path)
    _assert_non_tdlib_side_effects_false(report)

