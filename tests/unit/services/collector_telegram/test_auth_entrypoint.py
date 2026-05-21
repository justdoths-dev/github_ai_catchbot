from __future__ import annotations

import inspect
import json
import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram import auth_entrypoint
from services.collector_telegram.auth_fsm import AuthTransitionResult
from services.collector_telegram.auth_entrypoint import run_tdlib_auth_only_once
from services.collector_telegram.config import CollectorTelegramConfig


class FakeTransport:
    def __init__(self, states: list[dict[str, Any]]) -> None:
        self.states = list(states)
        self.initialized = False
        self.closed = False
        self.sent_requests: list[dict[str, Any]] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def send(self, request: dict[str, Any]) -> None:
        self.sent_requests.append(request)

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        if not self.states:
            return None
        state = self.states.pop(0)
        state_type = state.get("@type")
        if isinstance(state_type, str) and not state_type.startswith("authorizationState"):
            return state
        return {
            "@type": "updateAuthorizationState",
            "authorization_state": state,
        }

    async def close(self) -> None:
        self.closed = True


def _config(*, password: str | None = None) -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env="dev",
        database_url="db-url-redacted",
        redis_url=None,
        collector_mode="replay",
        telegram_api_id=1,
        telegram_api_hash="api-hash-redacted",
        telegram_phone_number="phone-redacted",
        telegram_2fa_password=password,
        tdlib_state_dir="/tmp/catchbot-test-tdlib-state",
        tdlib_files_dir="/tmp/catchbot-test-tdlib-files",
        tdlib_db_encryption_key="tdlib-key-redacted",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=50,
        warm_backfill_limit=30,
        history_page_limit=50,
        singleton_lock_path="/tmp/catchbot-test-collector.lock",
        startup_probe_timeout_sec=30,
        startup_warm_backfill_enabled=True,
        log_level="INFO",
    )


def _state(name: str) -> dict[str, str]:
    return {"@type": name}


def _assert_no_runtime_side_effects(payload: dict[str, Any]) -> None:
    for key in (
        "runtime_env_values_printed",
        "database_connected",
        "redis_connected",
        "alembic_run",
        "app_runtime_started",
        "live_collector_started",
        "notifier_transport_enabled",
        "production_rollout_performed",
        "secret_values_printed",
        "source_message_persisted",
        "outbox_event_emitted",
        "collector_main_imported",
        "collector_runtime_started",
        "session_state_created_or_reused",
    ):
        assert payload[key] is False


@pytest.mark.asyncio
async def test_auth_only_runner_processes_fake_state_sequence_to_ready() -> None:
    transport = FakeTransport(
        [
            _state("authorizationStateWaitTdlibParameters"),
            _state("authorizationStateWaitEncryptionKey"),
            _state("authorizationStateWaitPhoneNumber"),
            _state("authorizationStateReady"),
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
    )

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "ready"
    assert payload["tdlib_auth_attempted"] is True
    assert payload["tdlib_auth_completed"] is True
    assert payload["telegram_connected"] is True
    assert payload["final_authorization_state"] == "ready"
    assert payload["requests_sent_count"] == 3
    assert payload["auth_request_types_sent"] == [
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "setAuthenticationPhoneNumber",
    ]
    assert payload["last_auth_request_type"] == "setAuthenticationPhoneNumber"
    assert payload["authorization_updates_seen_count"] == 4
    assert payload["non_auth_response_count"] == 0
    assert payload["non_auth_response_type_counts"] == {}
    assert payload["tdlib_ok_seen"] is False
    assert payload["last_non_auth_response_type"] is None
    assert payload["connection_state_updates_seen_count"] == 0
    assert payload["last_connection_state_type"] is None
    assert payload["connection_state_type_counts"] == {}
    assert payload["max_authorization_updates"] == 20
    assert payload["receive_timeout_sec"] == 0
    assert [request["@type"] for request in transport.sent_requests] == [
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "setAuthenticationPhoneNumber",
    ]
    assert transport.sent_requests[0]["database_encryption_key"] == "dGRsaWIta2V5LXJlZGFjdGVk"
    assert transport.sent_requests[1]["encryption_key"] == "dGRsaWIta2V5LXJlZGFjdGVk"
    assert transport.initialized is True
    assert transport.closed is True
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_manual_login_code_state_requires_intervention_without_recording_code() -> None:
    transport = FakeTransport([_state("authorizationStateWaitCode")])

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
    )

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "manual_intervention_required"
    assert payload["manual_intervention_required"] is True
    assert payload["manual_intervention_reason"] == "Telegram login code required from operator"
    assert payload["final_authorization_state"] == "waiting_code"
    assert payload["requests_sent_count"] == 0
    assert payload["auth_request_types_sent"] == []
    assert payload["last_auth_request_type"] is None
    rendered = json.dumps(payload)
    assert "login_code" not in rendered
    assert "checkAuthenticationCode" not in rendered
    assert transport.sent_requests == []
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_password_state_sends_request_without_exposing_password_in_result() -> None:
    password = "fake-password-for-test"
    transport = FakeTransport(
        [
            _state("authorizationStateWaitPassword"),
            _state("authorizationStateReady"),
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(password=password),
        transport=transport,
        receive_timeout_sec=0,
    )

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "ready"
    assert payload["tdlib_auth_completed"] is True
    assert payload["requests_sent_count"] == 1
    assert payload["auth_request_types_sent"] == ["checkAuthenticationPassword"]
    assert payload["last_auth_request_type"] == "checkAuthenticationPassword"
    assert transport.sent_requests == [
        {"@type": "checkAuthenticationPassword", "password": password}
    ]
    assert password not in json.dumps(payload)
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_result_side_effect_booleans_stay_false_for_ready_path() -> None:
    transport = FakeTransport([_state("authorizationStateReady")])

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
    )

    payload = result.to_redacted_dict()
    assert payload["tdlib_auth_completed"] is True
    assert payload["telegram_connected"] is True
    assert payload["live_collector_started"] is False
    assert payload["app_runtime_started"] is False
    assert payload["notifier_transport_enabled"] is False
    assert payload["production_rollout_performed"] is False
    assert payload["runtime_env_values_printed"] is False
    assert payload["secret_values_printed"] is False
    _assert_no_runtime_side_effects(payload)


def test_auth_entrypoint_does_not_import_or_start_collector_runtime_entrypoints() -> None:
    source = inspect.getsource(auth_entrypoint)

    assert "CollectorTelegramService" not in source
    assert "CollectorRuntime" not in source
    assert "collector_telegram.main" not in source
    assert "asyncio.run(" not in source


@pytest.mark.asyncio
async def test_unsupported_state_returns_safe_degraded_result() -> None:
    transport = FakeTransport([_state("authorizationStateFutureUnsupported")])

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
    )

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "degraded"
    assert payload["tdlib_auth_attempted"] is True
    assert payload["tdlib_auth_completed"] is False
    assert payload["error"] == "AuthorizationError"
    assert payload["error_present"] is True
    assert payload["error_type"] == "AuthorizationError"
    assert payload["tdlib_error_present"] is False
    assert payload["tdlib_error_categories"] == ["authorization_state_related"]
    assert payload["completion_failure_category"] == "authorization_state_related"
    assert payload["requests_sent_count"] == 0
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_tdlib_error_payload_is_classified_without_exposing_raw_message() -> None:
    raw_message = (
        "setTdlibParameters failed for api_hash=fake-api-hash-secret "
        "phone=+15555550123 encryption_key=fake-tdlib-key-secret "
        "login_code=12345"
    )
    transport = FakeTransport(
        [
            _state("authorizationStateWaitTdlibParameters"),
            {"@type": "error", "code": 400, "message": raw_message},
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
    )

    payload = result.to_redacted_dict()
    rendered = json.dumps(payload, sort_keys=True)
    assert payload["auth_entrypoint_status"] == "degraded"
    assert payload["tdlib_auth_attempted"] is True
    assert payload["tdlib_auth_completed"] is False
    assert payload["error"] == "tdlib_error_redacted"
    assert payload["error_present"] is True
    assert payload["error_type"] == "tdlib_error"
    assert payload["tdlib_error_present"] is True
    assert payload["tdlib_error_code"] == 400
    assert payload["tdlib_error_type"] == "error"
    assert payload["tdlib_error_message_len"] == len(raw_message)
    assert payload["tdlib_error_categories"] == [
        "api_hash_related",
        "encryption_key_related",
        "tdlib_parameters_related",
    ]
    assert payload["completion_failure_category"] == "api_hash_related"
    assert raw_message not in rendered
    assert "fake-api-hash-secret" not in rendered
    assert "fake-tdlib-key-secret" not in rendered
    assert "+15555550123" not in rendered
    assert "12345" not in rendered
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_waiting_tdlib_parameters_without_tdlib_ok_is_classified_as_no_progress() -> None:
    transport = FakeTransport([_state("authorizationStateWaitTdlibParameters")])

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
        max_authorization_updates=2,
    )

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "degraded"
    assert payload["tdlib_auth_attempted"] is True
    assert payload["tdlib_auth_completed"] is False
    assert payload["final_authorization_state"] == "waiting_tdlib_parameters"
    assert payload["requests_sent_count"] == 1
    assert payload["error"] == "authorization_not_ready"
    assert payload["error_present"] is True
    assert payload["error_type"] == "completion_failure"
    assert payload["tdlib_error_present"] is False
    assert payload["tdlib_error_categories"] == [
        "tdlib_parameters_related",
        "timeout_or_no_update_related",
        "connection_not_ready_before_max_updates",
    ]
    assert (
        payload["completion_failure_category"]
        == "tdlib_auth_state_not_advanced_before_max_updates"
    )
    assert payload["authorization_updates_seen_count"] == 1
    assert payload["non_auth_response_count"] == 0
    assert payload["non_auth_response_type_counts"] == {}
    assert payload["tdlib_ok_seen"] is False
    assert payload["last_non_auth_response_type"] is None
    assert payload["auth_request_types_sent"] == ["setTdlibParameters"]
    assert payload["last_auth_request_type"] == "setTdlibParameters"
    assert payload["max_authorization_updates"] == 2
    assert payload["receive_timeout_sec"] == 0
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_waiting_tdlib_parameters_with_tdlib_ok_records_progress_category() -> None:
    transport = FakeTransport(
        [
            _state("authorizationStateWaitTdlibParameters"),
            {"@type": "ok"},
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
        max_authorization_updates=2,
    )

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "degraded"
    assert payload["tdlib_auth_completed"] is False
    assert payload["final_authorization_state"] == "waiting_tdlib_parameters"
    assert payload["requests_sent_count"] == 1
    assert payload["error"] == "authorization_not_ready"
    assert payload["error_present"] is True
    assert payload["error_type"] == "completion_failure"
    assert payload["tdlib_error_present"] is False
    assert payload["tdlib_error_categories"] == [
        "tdlib_parameters_related",
        "timeout_or_no_update_related",
        "connection_not_ready_before_max_updates",
    ]
    assert (
        payload["completion_failure_category"]
        == "tdlib_parameters_accepted_auth_state_not_advanced_before_max_updates"
    )
    assert payload["authorization_updates_seen_count"] == 1
    assert payload["non_auth_response_count"] == 1
    assert payload["non_auth_response_type_counts"] == {"ok": 1}
    assert payload["tdlib_ok_seen"] is True
    assert payload["last_non_auth_response_type"] == "ok"
    assert payload["auth_request_types_sent"] == ["setTdlibParameters"]
    assert payload["last_auth_request_type"] == "setTdlibParameters"
    assert payload["max_authorization_updates"] == 2
    assert payload["receive_timeout_sec"] == 0
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_non_auth_response_records_only_safe_type_without_raw_payload() -> None:
    secret_like_value = "fake-api-hash-secret"
    transport = FakeTransport(
        [
            _state("authorizationStateWaitTdlibParameters"),
            {
                "@type": "updateOption",
                "name": "secret_option",
                "value": {
                    "@type": "optionValueString",
                    "value": secret_like_value,
                },
            },
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
        max_authorization_updates=2,
    )

    payload = result.to_redacted_dict()
    rendered = json.dumps(payload, sort_keys=True)
    assert payload["non_auth_response_count"] == 1
    assert payload["non_auth_response_type_counts"] == {"updateOption": 1}
    assert payload["last_non_auth_response_type"] == "updateOption"
    assert payload["connection_state_updates_seen_count"] == 0
    assert payload["last_connection_state_type"] is None
    assert payload["connection_state_type_counts"] == {}
    assert payload["tdlib_ok_seen"] is False
    assert secret_like_value not in rendered
    assert "secret_option" not in rendered
    assert "optionValueString" not in rendered
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_non_auth_response_type_counts_are_aggregated_without_raw_payload() -> None:
    secret_like_value = "fake-api-hash-secret"
    unsafe_type = "fake-phone-secret-+15555550123"
    transport = FakeTransport(
        [
            _state("authorizationStateWaitTdlibParameters"),
            {
                "@type": "updateOption",
                "name": "api_hash",
                "value": {
                    "@type": "optionValueString",
                    "value": secret_like_value,
                },
            },
            {
                "@type": "updateConnectionState",
                "state": {"@type": "connectionStateConnecting"},
            },
            {"@type": "ok"},
            {"@type": unsafe_type, "phone_number": "+15555550123"},
            {
                "@type": "updateOption",
                "name": "tdlib_db_encryption_key",
                "value": secret_like_value,
            },
            {
                "@type": "updateConnectionState",
                "state": {"@type": "connectionStateConnecting"},
            },
            {"@type": "ok"},
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
        max_authorization_updates=8,
    )

    payload = result.to_redacted_dict()
    rendered = json.dumps(payload, sort_keys=True)
    assert payload["non_auth_response_count"] == 7
    assert payload["non_auth_response_type_counts"] == {
        "updateOption": 2,
        "updateConnectionState": 2,
        "ok": 2,
        "unrecognized": 1,
    }
    assert payload["last_non_auth_response_type"] == "ok"
    assert payload["connection_state_updates_seen_count"] == 2
    assert payload["connection_state_type_counts"] == {
        "connectionStateConnecting": 2,
    }
    assert secret_like_value not in rendered
    assert unsafe_type not in rendered
    assert "+15555550123" not in rendered
    assert "tdlib_db_encryption_key" not in rendered
    assert "optionValueString" not in rendered
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_non_auth_response_type_is_sanitized_before_serialization() -> None:
    secret_like_type = "fake-api-hash-secret"
    transport = FakeTransport(
        [
            _state("authorizationStateWaitTdlibParameters"),
            {"@type": secret_like_type},
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
        max_authorization_updates=2,
    )

    payload = result.to_redacted_dict()
    rendered = json.dumps(payload, sort_keys=True)
    assert payload["non_auth_response_count"] == 1
    assert payload["non_auth_response_type_counts"] == {"unrecognized": 1}
    assert payload["last_non_auth_response_type"] == "unrecognized"
    assert secret_like_type not in rendered
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_max_updates_exhaustion_without_state_is_classified_as_not_ready() -> None:
    transport = FakeTransport([])

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
        max_authorization_updates=1,
    )

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "degraded"
    assert payload["tdlib_auth_attempted"] is True
    assert payload["tdlib_auth_completed"] is False
    assert payload["final_authorization_state"] is None
    assert payload["requests_sent_count"] == 0
    assert payload["auth_request_types_sent"] == []
    assert payload["last_auth_request_type"] is None
    assert payload["error_present"] is True
    assert payload["error_type"] == "completion_failure"
    assert payload["tdlib_error_present"] is False
    assert payload["tdlib_error_categories"] == [
        "timeout_or_no_update_related",
        "connection_not_ready_before_max_updates",
    ]
    assert payload["completion_failure_category"] == "authorization_not_ready_before_max_updates"
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_waiting_phone_number_progress_records_request_and_connection_state_safely() -> None:
    secret_like_value = "fake-api-hash-secret"
    unsafe_state_type = "fake-phone-secret-+15555550123"
    transport = FakeTransport(
        [
            _state("authorizationStateWaitTdlibParameters"),
            _state("authorizationStateWaitEncryptionKey"),
            _state("authorizationStateWaitPhoneNumber"),
            {
                "@type": "updateConnectionState",
                "state": {
                    "@type": "connectionStateWaitingForNetwork",
                    "api_hash": secret_like_value,
                },
            },
            {
                "@type": "updateConnectionState",
                "state": {
                    "@type": unsafe_state_type,
                    "phone_number": "+15555550123",
                },
            },
        ]
    )

    result = await run_tdlib_auth_only_once(
        _config(),
        transport=transport,
        receive_timeout_sec=0,
        max_authorization_updates=5,
    )

    payload = result.to_redacted_dict()
    rendered = json.dumps(payload, sort_keys=True)
    assert payload["auth_entrypoint_status"] == "degraded"
    assert payload["final_authorization_state"] == "waiting_phone_number"
    assert payload["requests_sent_count"] == 3
    assert payload["auth_request_types_sent"] == [
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "setAuthenticationPhoneNumber",
    ]
    assert payload["last_auth_request_type"] == "setAuthenticationPhoneNumber"
    assert payload["non_auth_response_type_counts"] == {
        "updateConnectionState": 2,
    }
    assert payload["connection_state_updates_seen_count"] == 2
    assert payload["last_connection_state_type"] == "unrecognized"
    assert payload["connection_state_type_counts"] == {
        "connectionStateWaitingForNetwork": 1,
        "unrecognized": 1,
    }
    assert payload["tdlib_error_categories"] == [
        "timeout_or_no_update_related",
        "connection_not_ready_before_max_updates",
    ]
    assert (
        payload["completion_failure_category"]
        == "waiting_phone_number_request_sent_auth_state_not_advanced_before_max_updates"
    )
    assert secret_like_value not in rendered
    assert unsafe_state_type not in rendered
    assert "+15555550123" not in rendered
    _assert_no_runtime_side_effects(payload)


@pytest.mark.asyncio
async def test_waiting_phone_number_without_phone_request_is_classified_separately() -> None:
    class FakeWaitingPhoneFSM:
        def handle_state(self, state: dict[str, Any]) -> AuthTransitionResult:
            return AuthTransitionResult(new_state="waiting_phone_number", requests=[])

    transport = FakeTransport([_state("authorizationStateWaitPhoneNumber")])
    runner = auth_entrypoint.TDLibAuthOnlyRunner(
        _config(),
        transport=transport,
        fsm=FakeWaitingPhoneFSM(),  # type: ignore[arg-type]
        receive_timeout_sec=0,
        max_authorization_updates=1,
    )

    result = await runner.run_once()

    payload = result.to_redacted_dict()
    assert payload["auth_entrypoint_status"] == "degraded"
    assert payload["final_authorization_state"] == "waiting_phone_number"
    assert payload["requests_sent_count"] == 0
    assert payload["auth_request_types_sent"] == []
    assert payload["last_auth_request_type"] is None
    assert (
        payload["completion_failure_category"]
        == "waiting_phone_number_request_not_observed_before_max_updates"
    )
    assert payload["tdlib_error_categories"] == [
        "timeout_or_no_update_related",
        "connection_not_ready_before_max_updates",
    ]
    _assert_no_runtime_side_effects(payload)
