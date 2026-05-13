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
        "telegram_connected",
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
    assert payload["final_authorization_state"] == "ready"
    assert payload["requests_sent_count"] == 3
    assert [request["@type"] for request in transport.sent_requests] == [
        "setTdlibParameters",
        "checkDatabaseEncryptionKey",
        "setAuthenticationPhoneNumber",
    ]
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

    _assert_no_runtime_side_effects(result.to_redacted_dict())


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
    assert payload["requests_sent_count"] == 0
    _assert_no_runtime_side_effects(payload)
