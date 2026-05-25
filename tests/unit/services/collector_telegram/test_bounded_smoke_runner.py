from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.bounded_smoke_runner import (
    BoundedCollectorSmokeRunner,
    BoundedCollectorSmokePartialFailure,
    BoundedSmokeRunnerConfigError,
    CollectorSmokeWriteCounter,
    build_default_bounded_collector_smoke_runner,
    classify_tdlib_update_type,
)
from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.models import CollectorEnvironment, CollectorMode


class Bounds:
    def __init__(
        self,
        *,
        max_duration_sec: int = 30,
        max_updates: int = 10,
        max_db_writes: int = 20,
    ) -> None:
        self.max_duration_sec = max_duration_sec
        self.max_updates = max_updates
        self.max_db_writes = max_db_writes


class FakeSingletonGuard:
    def __init__(self) -> None:
        self.acquire_count = 0
        self.release_count = 0
        self.acquired = False

    def acquire(self) -> None:
        self.acquire_count += 1
        self.acquired = True

    def release(self) -> None:
        self.release_count += 1
        self.acquired = False


class FakeTDLibClient:
    def __init__(
        self,
        payloads: list[dict[str, Any] | None],
        *,
        receive_error: Exception | None = None,
    ) -> None:
        self.payloads = list(payloads)
        self.receive_error = receive_error
        self.initialized = False
        self.closed = False
        self.sent_requests: list[dict[str, Any]] = []
        self.receive_timeouts: list[float] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def send(self, request: dict[str, Any]) -> None:
        self.sent_requests.append(request)

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        self.receive_timeouts.append(timeout)
        if self.receive_error is not None:
            raise self.receive_error
        if self.payloads:
            return self.payloads.pop(0)
        return None

    async def close(self) -> None:
        self.closed = True

    def build_set_tdlib_parameters_request(self) -> dict[str, Any]:
        return {"@type": "setTdlibParameters"}

    def build_check_database_encryption_key_request(self) -> dict[str, Any]:
        return {"@type": "checkDatabaseEncryptionKey"}


class FakeDispatcher:
    def __init__(self, counter: CollectorSmokeWriteCounter) -> None:
        self.counter = counter
        self.dispatched: list[dict[str, Any]] = []

    async def dispatch(self, update: dict[str, Any]) -> None:
        self.dispatched.append(update)
        update_type = update.get("@type")
        self.counter.count_table("telegram_raw_updates")
        if update_type == "updateNewMessage":
            self.counter.count_table("source_messages")
            self.counter.count_table("source_message_versions")
            self.counter.count_table("event_outbox")
        elif update_type == "updateMessageEdited":
            self.counter.count_table("source_messages")
        elif update_type == "updateDeleteMessages":
            message_ids = update.get("message_ids")
            for _message_id in message_ids if isinstance(message_ids, list) else []:
                self.counter.count_table("source_messages")
                self.counter.count_table("event_outbox")


class RawUpdateThenRaiseDispatcher(FakeDispatcher):
    async def dispatch(self, update: dict[str, Any]) -> None:
        self.dispatched.append(update)
        self.counter.count_table("telegram_raw_updates")
        raise RuntimeError("dispatcher failed after one collector-owned write")


class MessageWriteThenRaiseDispatcher(FakeDispatcher):
    async def dispatch(self, update: dict[str, Any]) -> None:
        self.dispatched.append(update)
        self.counter.count_table("telegram_raw_updates")
        self.counter.count_table("source_messages")
        self.counter.count_table("source_message_versions")
        self.counter.count_table("event_outbox")
        raise RuntimeError("dispatcher failed after canonical collector writes")


class FakeDispatcherContext:
    def __init__(self, dispatcher: FakeDispatcher) -> None:
        self.dispatcher = dispatcher
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakeDispatcher:
        self.entered = True
        return self.dispatcher

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.exited = True


def _config(tmp_path: Path) -> CollectorTelegramConfig:
    tdlib_state_dir = tmp_path / "tdlib-state"
    tdlib_files_dir = tmp_path / "tdlib-files"
    lock_dir = tmp_path / "locks"
    tdlib_state_dir.mkdir(parents=True, exist_ok=True)
    tdlib_files_dir.mkdir(parents=True, exist_ok=True)
    lock_dir.mkdir(parents=True, exist_ok=True)
    return CollectorTelegramConfig(
        app_env=CollectorEnvironment.DEV,
        database_url="postgresql+psycopg://collector:secret@127.0.0.1:5432/catchbot",
        redis_url=None,
        collector_mode=CollectorMode.REPLAY,
        telegram_api_id=12345,
        telegram_api_hash="unit-api-hash",
        telegram_phone_number="+15555550125",
        telegram_2fa_password=None,
        tdlib_state_dir=str(tdlib_state_dir),
        tdlib_files_dir=str(tdlib_files_dir),
        tdlib_db_encryption_key="unit-encryption-key",
        reconcile_interval_sec=60,
        reconcile_backfill_limit=10,
        warm_backfill_limit=5,
        history_page_limit=10,
        singleton_lock_path=str(lock_dir / "collector.lock"),
    )


def _new_message(message_id: int) -> dict[str, Any]:
    return {
        "@type": "updateNewMessage",
        "message": {
            "chat_id": 1000,
            "id": message_id,
            "date": 1713550000,
            "content": {
                "@type": "messageText",
                "text": {"text": f"message {message_id}", "entities": []},
            },
        },
    }


def _control_update(update_type: str) -> dict[str, Any]:
    return {"@type": update_type, "safe_value": "unit"}


def _reconcile_update() -> dict[str, Any]:
    return {"@type": "updateChatLastMessage", "last_message": None}


def _runner(
    tmp_path: Path,
    payloads: list[dict[str, Any] | None],
    *,
    monotonic: Any | None = None,
) -> tuple[
    BoundedCollectorSmokeRunner,
    FakeTDLibClient,
    FakeSingletonGuard,
    list[FakeDispatcher],
    list[FakeDispatcherContext],
]:
    client = FakeTDLibClient(payloads)
    guard = FakeSingletonGuard()
    dispatchers: list[FakeDispatcher] = []
    contexts: list[FakeDispatcherContext] = []

    def dispatcher_factory(counter: CollectorSmokeWriteCounter) -> FakeDispatcherContext:
        dispatcher = FakeDispatcher(counter)
        context = FakeDispatcherContext(dispatcher)
        dispatchers.append(dispatcher)
        contexts.append(context)
        return context

    runner = BoundedCollectorSmokeRunner(
        config=_config(tmp_path),
        tdlib_client=client,
        singleton_guard=guard,
        dispatcher_factory=dispatcher_factory,
        monotonic=monotonic or (lambda: 0.0),
    )
    return runner, client, guard, dispatchers, contexts


class BoundedSmokeRunnerTests(unittest.IsolatedAsyncioTestCase):
    def test_update_type_classifier_groups_message_bearing_updates(self) -> None:
        for update_type in (
            "updateNewMessage",
            "updateMessageContent",
            "updateMessageEdited",
            "updateDeleteMessages",
        ):
            self.assertEqual(
                classify_tdlib_update_type(update_type),
                "message_bearing",
            )

    def test_update_type_classifier_groups_control_and_reconcile_updates(self) -> None:
        for update_type in (
            "updateOption",
            "updateDefaultReactionType",
            "updateTrustedMiniAppBots",
            "updateSomeFutureState",
        ):
            self.assertEqual(
                classify_tdlib_update_type(update_type),
                "control_or_state",
            )
        self.assertEqual(
            classify_tdlib_update_type("updateChatLastMessage"),
            "reconcile_signal",
        )

    async def test_no_updates_reports_no_writes_and_releases_singleton(self) -> None:
        runner, client, guard, dispatchers, contexts = _runner(self._tmp(), [None])

        result = await runner.run(runtime_env={}, bounds=Bounds())

        self.assertEqual(result.updates_observed, 0)
        self.assertEqual(result.update_types_seen, ())
        self.assertEqual(result.message_bearing_updates_observed, 0)
        self.assertEqual(result.control_updates_observed, 0)
        self.assertEqual(result.reconcile_signal_updates_observed, 0)
        self.assertEqual(result.telegram_raw_updates_written, 0)
        self.assertFalse(result.canonical_ingest_writes_observed)
        self.assertFalse(result.raw_only_writes_observed)
        self.assertTrue(result.message_ingest_not_proven)
        self.assertEqual(result.written_tables, ())
        self.assertTrue(result.side_effects["tdlib_initialized"])
        self.assertTrue(result.side_effects["tdlib_receive_called"])
        self.assertTrue(result.side_effects["telegram_api_called"])
        self.assertTrue(result.side_effects["live_collector_started"])
        self.assertTrue(result.side_effects["collector_runtime_started"])
        self.assertTrue(client.initialized)
        self.assertTrue(client.closed)
        self.assertEqual(guard.acquire_count, 1)
        self.assertEqual(guard.release_count, 1)
        self.assertFalse(guard.acquired)
        self.assertEqual(dispatchers[0].dispatched, [])
        self.assertTrue(contexts[0].entered)
        self.assertTrue(contexts[0].exited)

    async def test_update_and_write_caps_stop_dispatch(self) -> None:
        runner, _client, _guard, dispatchers, _contexts = _runner(
            self._tmp(),
            [_new_message(1), _new_message(2), _new_message(3)],
        )

        result = await runner.run(
            runtime_env={},
            bounds=Bounds(max_duration_sec=30, max_updates=2, max_db_writes=20),
        )

        self.assertEqual(result.updates_observed, 2)
        self.assertEqual(result.update_types_seen, (("updateNewMessage", 2),))
        self.assertEqual(result.message_bearing_updates_observed, 2)
        self.assertTrue(result.update_cap_exhausted)
        self.assertEqual(len(dispatchers[0].dispatched), 2)

    async def test_db_write_cap_is_enforced_before_next_update(self) -> None:
        runner, _client, _guard, dispatchers, _contexts = _runner(
            self._tmp(),
            [_new_message(1), _new_message(2)],
        )

        result = await runner.run(
            runtime_env={},
            bounds=Bounds(max_duration_sec=30, max_updates=10, max_db_writes=4),
        )

        self.assertEqual(result.updates_observed, 1)
        self.assertTrue(result.db_write_cap_exhausted)
        self.assertEqual(result.telegram_raw_updates_written, 1)
        self.assertEqual(result.source_messages_written, 1)
        self.assertEqual(result.source_message_versions_written, 1)
        self.assertEqual(result.event_outbox_written, 1)
        self.assertTrue(result.canonical_ingest_writes_observed)
        self.assertFalse(result.raw_only_writes_observed)
        self.assertFalse(result.message_ingest_not_proven)
        self.assertEqual(len(dispatchers[0].dispatched), 1)

    async def test_duration_cap_can_stop_before_receive(self) -> None:
        calls = iter([0.0, 2.0])
        runner, client, _guard, _dispatchers, _contexts = _runner(
            self._tmp(),
            [_new_message(1)],
            monotonic=lambda: next(calls),
        )

        result = await runner.run(
            runtime_env={},
            bounds=Bounds(max_duration_sec=1, max_updates=10, max_db_writes=20),
        )

        self.assertTrue(result.duration_exhausted)
        self.assertEqual(result.updates_observed, 0)
        self.assertEqual(client.receive_timeouts, [])

    async def test_control_only_updates_report_raw_only_observation(self) -> None:
        runner, _client, _guard, _dispatchers, _contexts = _runner(
            self._tmp(),
            [
                _control_update("updateOption"),
                _control_update("updateDefaultReactionType"),
                _control_update("updateTrustedMiniAppBots"),
                None,
            ],
        )

        result = await runner.run(runtime_env={}, bounds=Bounds())

        self.assertEqual(result.updates_observed, 3)
        self.assertEqual(
            result.update_types_seen,
            (
                ("updateDefaultReactionType", 1),
                ("updateOption", 1),
                ("updateTrustedMiniAppBots", 1),
            ),
        )
        self.assertEqual(result.control_updates_observed, 3)
        self.assertEqual(result.message_bearing_updates_observed, 0)
        self.assertEqual(result.reconcile_signal_updates_observed, 0)
        self.assertEqual(result.telegram_raw_updates_written, 3)
        self.assertFalse(result.canonical_ingest_writes_observed)
        self.assertTrue(result.raw_only_writes_observed)
        self.assertTrue(result.message_ingest_not_proven)

    async def test_reconcile_signal_does_not_prove_canonical_ingest(self) -> None:
        runner, _client, _guard, _dispatchers, _contexts = _runner(
            self._tmp(),
            [_reconcile_update(), None],
        )

        result = await runner.run(runtime_env={}, bounds=Bounds())

        self.assertEqual(result.updates_observed, 1)
        self.assertEqual(result.update_types_seen, (("updateChatLastMessage", 1),))
        self.assertEqual(result.reconcile_signal_updates_observed, 1)
        self.assertEqual(result.telegram_raw_updates_written, 1)
        self.assertFalse(result.canonical_ingest_writes_observed)
        self.assertTrue(result.raw_only_writes_observed)
        self.assertTrue(result.message_ingest_not_proven)

    async def test_update_type_sanitizer_drops_unsafe_update_identifiers(
        self,
    ) -> None:
        unsafe_update_types = (
            "updateNewMessage:987654321",
            "updateNewMessage-sensitive_username",
            "updateNewMessage.suffix",
            "updateNewMessage/anything",
        )
        for update_type in unsafe_update_types:
            with self.subTest(update_type=update_type):
                runner, _client, _guard, dispatchers, _contexts = _runner(
                    self._tmp(),
                    [
                        {
                            "@type": update_type,
                            "message": {"chat_id": 987654321, "id": 123},
                        },
                        None,
                    ],
                )

                result = await runner.run(runtime_env={}, bounds=Bounds())

                self.assertEqual(result.updates_observed, 0)
                self.assertEqual(result.update_types_seen, ())
                self.assertEqual(result.telegram_raw_updates_written, 0)
                self.assertEqual(dispatchers[0].dispatched, [])

    async def test_update_type_sanitizer_accepts_tdlib_update_identifier(self) -> None:
        runner, _client, _guard, dispatchers, _contexts = _runner(
            self._tmp(),
            [_new_message(123), None],
        )

        result = await runner.run(runtime_env={}, bounds=Bounds())

        self.assertEqual(result.update_types_seen, (("updateNewMessage", 1),))
        self.assertEqual(result.message_bearing_updates_observed, 1)
        self.assertEqual(len(dispatchers[0].dispatched), 1)

    async def test_authorization_bootstrap_sends_only_allowed_requests(self) -> None:
        runner, client, _guard, dispatchers, _contexts = _runner(
            self._tmp(),
            [
                {
                    "@type": "updateAuthorizationState",
                    "authorization_state": {
                        "@type": "authorizationStateWaitTdlibParameters"
                    },
                },
                {
                    "@type": "updateAuthorizationState",
                    "authorization_state": {
                        "@type": "authorizationStateWaitEncryptionKey"
                    },
                },
                _new_message(1),
                None,
            ],
        )

        result = await runner.run(runtime_env={}, bounds=Bounds())

        self.assertEqual(result.updates_observed, 1)
        self.assertEqual(
            [request["@type"] for request in client.sent_requests],
            ["setTdlibParameters", "checkDatabaseEncryptionKey"],
        )
        self.assertFalse(
            {
                "getChatHistory",
                "joinChat",
                "searchPublicChat",
                "sendMessage",
            }
            & {request["@type"] for request in client.sent_requests}
        )
        self.assertEqual(len(dispatchers[0].dispatched), 1)

    async def test_invalid_bounds_fail_before_starting_client(self) -> None:
        runner, client, guard, _dispatchers, _contexts = _runner(self._tmp(), [None])

        with self.assertRaises(BoundedSmokeRunnerConfigError):
            await runner.run(
                runtime_env={},
                bounds=Bounds(max_duration_sec=121, max_updates=10, max_db_writes=20),
            )

        self.assertFalse(client.initialized)
        self.assertEqual(guard.acquire_count, 0)

    def test_default_factory_returns_bounded_runner(self) -> None:
        config = _config(self._tmp())
        runtime_env = {
            "APP_ENV": str(config.app_env),
            "COLLECTOR_MODE": str(config.collector_mode),
            "DATABASE_URL": config.database_url,
            "TELEGRAM_API_HASH": config.telegram_api_hash,
            "TELEGRAM_API_ID": str(config.telegram_api_id),
            "TELEGRAM_PHONE_NUMBER": config.telegram_phone_number,
            "TDLIB_DB_ENCRYPTION_KEY": config.tdlib_db_encryption_key,
            "TDLIB_STATE_DIR": config.tdlib_state_dir,
            "TDLIB_FILES_DIR": config.tdlib_files_dir,
            "COLLECTOR_SINGLETON_LOCK_PATH": config.singleton_lock_path,
        }

        class FakeTDJsonTransport:
            def __init__(self, *, library_path: str | None = None) -> None:
                self.library_path = library_path

            def assert_available(self) -> None:
                return None

            async def initialize(self) -> None:
                return None

            async def send(self, request: dict[str, Any]) -> None:
                return None

            async def receive(self, timeout: float) -> dict[str, Any] | None:
                return None

            async def close(self) -> None:
                return None

        with patch(
            "services.collector_telegram.bounded_smoke_runner.TDJsonTransport",
            FakeTDJsonTransport,
        ):
            runner = build_default_bounded_collector_smoke_runner(runtime_env)

        self.assertIsInstance(runner, BoundedCollectorSmokeRunner)

    async def test_dispatch_failure_carries_partial_collector_write_result(self) -> None:
        tmp_path = self._tmp()
        client = FakeTDLibClient([_new_message(1)])
        guard = FakeSingletonGuard()
        contexts: list[FakeDispatcherContext] = []

        def dispatcher_factory(counter: CollectorSmokeWriteCounter) -> FakeDispatcherContext:
            context = FakeDispatcherContext(RawUpdateThenRaiseDispatcher(counter))
            contexts.append(context)
            return context

        runner = BoundedCollectorSmokeRunner(
            config=_config(tmp_path),
            tdlib_client=client,
            singleton_guard=guard,
            dispatcher_factory=dispatcher_factory,
            monotonic=lambda: 0.0,
        )

        with self.assertRaises(BoundedCollectorSmokePartialFailure) as raised:
            await runner.run(runtime_env={}, bounds=Bounds())

        result = raised.exception.result
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.failure_class, "RuntimeError")
        self.assertEqual(result.updates_observed, 1)
        self.assertEqual(result.update_types_seen, (("updateNewMessage", 1),))
        self.assertEqual(result.message_bearing_updates_observed, 1)
        self.assertEqual(result.telegram_raw_updates_written, 1)
        self.assertTrue(result.raw_only_writes_observed)
        self.assertTrue(result.message_ingest_not_proven)
        self.assertEqual(result.written_tables, ("telegram_raw_updates",))
        self.assertTrue(result.side_effects["database_mutation_performed"])
        self.assertTrue(result.side_effects["telegram_raw_updates_written"])
        self.assertTrue(result.side_effects["live_collector_started"])
        self.assertTrue(result.side_effects["collector_runtime_started"])
        self.assertTrue(client.closed)
        self.assertEqual(guard.release_count, 1)
        self.assertTrue(contexts[0].exited)

    async def test_partial_failure_after_control_update_preserves_summary(self) -> None:
        tmp_path = self._tmp()
        client = FakeTDLibClient([_control_update("updateOption")])
        guard = FakeSingletonGuard()

        def dispatcher_factory(counter: CollectorSmokeWriteCounter) -> FakeDispatcherContext:
            return FakeDispatcherContext(RawUpdateThenRaiseDispatcher(counter))

        runner = BoundedCollectorSmokeRunner(
            config=_config(tmp_path),
            tdlib_client=client,
            singleton_guard=guard,
            dispatcher_factory=dispatcher_factory,
            monotonic=lambda: 0.0,
        )

        with self.assertRaises(BoundedCollectorSmokePartialFailure) as raised:
            await runner.run(runtime_env={}, bounds=Bounds())

        result = raised.exception.result
        self.assertEqual(result.update_types_seen, (("updateOption", 1),))
        self.assertEqual(result.control_updates_observed, 1)
        self.assertEqual(result.message_bearing_updates_observed, 0)
        self.assertEqual(result.telegram_raw_updates_written, 1)
        self.assertFalse(result.canonical_ingest_writes_observed)
        self.assertTrue(result.raw_only_writes_observed)
        self.assertTrue(result.message_ingest_not_proven)

    async def test_partial_failure_after_message_write_preserves_summary(self) -> None:
        tmp_path = self._tmp()
        client = FakeTDLibClient([_new_message(1)])
        guard = FakeSingletonGuard()

        def dispatcher_factory(counter: CollectorSmokeWriteCounter) -> FakeDispatcherContext:
            return FakeDispatcherContext(MessageWriteThenRaiseDispatcher(counter))

        runner = BoundedCollectorSmokeRunner(
            config=_config(tmp_path),
            tdlib_client=client,
            singleton_guard=guard,
            dispatcher_factory=dispatcher_factory,
            monotonic=lambda: 0.0,
        )

        with self.assertRaises(BoundedCollectorSmokePartialFailure) as raised:
            await runner.run(runtime_env={}, bounds=Bounds())

        result = raised.exception.result
        self.assertEqual(result.update_types_seen, (("updateNewMessage", 1),))
        self.assertEqual(result.message_bearing_updates_observed, 1)
        self.assertEqual(result.telegram_raw_updates_written, 1)
        self.assertEqual(result.source_messages_written, 1)
        self.assertEqual(result.source_message_versions_written, 1)
        self.assertEqual(result.event_outbox_written, 1)
        self.assertTrue(result.canonical_ingest_writes_observed)
        self.assertFalse(result.raw_only_writes_observed)
        self.assertFalse(result.message_ingest_not_proven)

    async def test_receive_failure_carries_tdlib_and_telegram_side_effects(self) -> None:
        client = FakeTDLibClient([], receive_error=RuntimeError("receive failed"))
        guard = FakeSingletonGuard()

        def dispatcher_factory(counter: CollectorSmokeWriteCounter) -> FakeDispatcherContext:
            return FakeDispatcherContext(FakeDispatcher(counter))

        runner = BoundedCollectorSmokeRunner(
            config=_config(self._tmp()),
            tdlib_client=client,
            singleton_guard=guard,
            dispatcher_factory=dispatcher_factory,
            monotonic=lambda: 0.0,
        )

        with self.assertRaises(BoundedCollectorSmokePartialFailure) as raised:
            await runner.run(runtime_env={}, bounds=Bounds())

        result = raised.exception.result
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.failure_class, "RuntimeError")
        self.assertEqual(result.updates_observed, 0)
        self.assertTrue(result.side_effects["tdlib_initialized"])
        self.assertTrue(result.side_effects["tdlib_receive_called"])
        self.assertTrue(result.side_effects["telegram_api_called"])
        self.assertTrue(result.side_effects["live_collector_started"])
        self.assertFalse(result.side_effects["database_mutation_performed"])
        self.assertTrue(client.closed)
        self.assertEqual(guard.release_count, 1)

    async def test_manual_authorization_state_fails_closed_without_auth_submission(self) -> None:
        runner, client, _guard, _dispatchers, _contexts = _runner(
            self._tmp(),
            [
                {
                    "@type": "updateAuthorizationState",
                    "authorization_state": {
                        "@type": "authorizationStateWaitPhoneNumber"
                    },
                }
            ],
        )

        with self.assertRaises(BoundedCollectorSmokePartialFailure) as raised:
            await runner.run(runtime_env={}, bounds=Bounds())

        result = raised.exception.result
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.failure_class, "manual_authorization_required")
        self.assertEqual(result.updates_observed, 0)
        self.assertEqual(client.sent_requests, [])
        self.assertFalse(result.side_effects["tdlib_auth_attempted"])
        self.assertFalse(result.side_effects["tdlib_phone_number_submitted"])
        self.assertFalse(result.side_effects["tdlib_code_submitted"])
        self.assertFalse(result.side_effects["tdlib_password_submitted"])
        self.assertTrue(result.side_effects["tdlib_receive_called"])
        self.assertTrue(result.side_effects["telegram_api_called"])

    def test_default_factory_uses_runtime_env_tdjson_path_and_checks_availability(self) -> None:
        config = _config(self._tmp())
        tdjson_path = "/safe/test/libtdjson.so"
        runtime_env = {
            "APP_ENV": str(config.app_env),
            "COLLECTOR_MODE": str(config.collector_mode),
            "DATABASE_URL": config.database_url,
            "TELEGRAM_API_HASH": config.telegram_api_hash,
            "TELEGRAM_API_ID": str(config.telegram_api_id),
            "TELEGRAM_PHONE_NUMBER": config.telegram_phone_number,
            "TDLIB_DB_ENCRYPTION_KEY": config.tdlib_db_encryption_key,
            "TDLIB_STATE_DIR": config.tdlib_state_dir,
            "TDLIB_FILES_DIR": config.tdlib_files_dir,
            "COLLECTOR_SINGLETON_LOCK_PATH": config.singleton_lock_path,
            "TDJSON_LIBRARY_PATH": tdjson_path,
        }
        captured_paths: list[str | None] = []
        availability_checked = False

        class FakeTDJsonTransport:
            def __init__(self, *, library_path: str | None = None) -> None:
                captured_paths.append(library_path)

            def assert_available(self) -> None:
                nonlocal availability_checked
                availability_checked = True

            async def initialize(self) -> None:
                return None

            async def send(self, request: dict[str, Any]) -> None:
                return None

            async def receive(self, timeout: float) -> dict[str, Any] | None:
                return None

            async def close(self) -> None:
                return None

        with patch(
            "services.collector_telegram.bounded_smoke_runner.TDJsonTransport",
            FakeTDJsonTransport,
        ):
            runner = build_default_bounded_collector_smoke_runner(runtime_env)

        self.assertIsInstance(runner, BoundedCollectorSmokeRunner)
        self.assertEqual(captured_paths, [tdjson_path])
        self.assertTrue(availability_checked)

    def _tmp(self) -> Path:
        return Path(tempfile.mkdtemp(prefix="catchbot-bounded-smoke-runner-"))


if __name__ == "__main__":
    unittest.main()
