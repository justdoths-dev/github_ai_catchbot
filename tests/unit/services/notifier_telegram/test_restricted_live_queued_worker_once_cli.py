from __future__ import annotations

import json
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest

from services.notifier_telegram import main as notifier_main
from services.notifier_telegram.main import (
    RESTRICTED_LIVE_QUEUED_WORKER_ONCE_SCHEMA_VERSION,
    _run_restricted_live_queued_worker_once,
    _run_restricted_live_queued_worker_once_command,
    build_parser,
)
from services.notifier_telegram.models import DeliveryResult, StreamMessage
from services.notifier_telegram.worker_once import (
    EXPECTED_QUEUE_NAME,
    EXPECTED_STAGE_NAME,
    REQUIRED_THIN_QUEUE_FIELDS,
    WorkerOnceRuntime,
    run_worker_once_invocation,
)
from tests.unit.services.notifier_telegram._service_fakes import config as base_config


RUNTIME_ENV_KEYS = (
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_API_BASE_URL",
    "ENABLE_NOTIFICATION_SEND",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "NOTIFIER_TELEGRAM_QUEUE_NAME",
    "NOTIFIER_TELEGRAM_CONSUMER_GROUP",
    "NOTIFIER_TELEGRAM_CONSUMER_NAME",
    "NOTIFIER_TELEGRAM_BATCH_SIZE",
    "NOTIFIER_TELEGRAM_BLOCK_MS",
)


def _clear_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _live_config():
    return replace(
        base_config(dry_run=False, enable_notification_send=True, allow_edits=False),
        app_env="prod",
        database_url="postgresql+psycopg" + "://unit/db",
        redis_url="redis" + "://unit/0",
        telegram_bot_token="unit-live-send-credential",
        telegram_api_base_url="https://api.telegram.org",
    )


def _write_env_file(tmp_path, **overrides: str) -> str:
    values = {
        "APP_ENV": "prod",
        "DATABASE_URL": "postgresql+psycopg" + "://unit/db",
        "REDIS_URL": "redis" + "://unit/0",
        "TELEGRAM_BOT_TOKEN": "unit-live-send-credential",
        "TELEGRAM_API_BASE_URL": "https://api.telegram.org",
        "ENABLE_NOTIFICATION_SEND": "true",
        "NOTIFIER_TELEGRAM_DRY_RUN": "false",
        "NOTIFIER_TELEGRAM_ALLOW_EDITS": "false",
    }
    values.update(overrides)
    env_file = tmp_path / "notifier-runtime.env"
    env_file.write_text("\n".join(f"{key}={value}" for key, value in values.items()), encoding="utf-8")
    return str(env_file)


@pytest.mark.asyncio
async def test_command_rejects_without_operator_confirmation() -> None:
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "restricted-live-queued-worker-once",
            "--env-file",
            "/tmp/notifier-runtime.env",
            "--format",
            "json",
        ]
    )

    code = await _run_restricted_live_queued_worker_once_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["schema_version"] == RESTRICTED_LIVE_QUEUED_WORKER_ONCE_SCHEMA_VERSION
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "operator_confirmation_required"
    assert payload["authority"]["database_session_opened"] is False


@pytest.mark.asyncio
async def test_command_rejects_without_env_file() -> None:
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "restricted-live-queued-worker-once",
            "--operator-confirmed",
            "--format",
            "json",
        ]
    )

    code = await _run_restricted_live_queued_worker_once_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "env_file_required"


@pytest.mark.asyncio
async def test_command_rejects_send_disabled_config(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_runtime_env(monkeypatch)
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "restricted-live-queued-worker-once",
            "--operator-confirmed",
            "--env-file",
            _write_env_file(tmp_path, ENABLE_NOTIFICATION_SEND="false"),
            "--format",
            "json",
        ]
    )

    code = await _run_restricted_live_queued_worker_once_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "notification_send_disabled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cfg", "expected_reason"),
    [
        (replace(_live_config(), dry_run=True), "notifier_dry_run_enabled"),
        (replace(_live_config(), allow_edits=True), "notifier_edits_enabled"),
        (replace(_live_config(), telegram_bot_token=""), "telegram_bot_token_missing"),
        (replace(_live_config(), telegram_api_base_url="http://127.0.0.1:8081"), "telegram_api_base_url_blackhole"),
        (replace(_live_config(), telegram_api_base_url="https://example.com"), "telegram_api_base_url_unofficial"),
    ],
)
async def test_config_guards_reject_before_redis_or_worker(cfg, expected_reason: str) -> None:
    emitted: list[str] = []

    code = await _run_restricted_live_queued_worker_once(
        cfg,
        emit_json=emitted.append,
        redis_client_builder=_unused_redis_builder,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == expected_reason
    assert payload["redis_precheck"] == {"pending": None, "lag": None, "reason_code": None}
    assert payload["authority"]["database_session_opened"] is False


@pytest.mark.asyncio
async def test_rejects_redis_pending_before_worker_invocation() -> None:
    emitted: list[str] = []
    worker_calls = 0
    redis = FakeRedis(pending=1, lag=1)

    async def worker_once_runner(config, emit_json):
        nonlocal worker_calls
        worker_calls += 1
        return await _unused_worker_once_runner(config, emit_json)

    code = await _run_restricted_live_queued_worker_once(
        _live_config(),
        emit_json=emitted.append,
        redis_client_builder=lambda redis_url: redis,
        worker_once_runner=worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "redis_pending_messages_present"
    assert payload["redis_precheck"] == {"pending": 1, "lag": 1, "reason_code": None}
    assert worker_calls == 0
    assert redis.closed is True


@pytest.mark.asyncio
async def test_lag_zero_returns_noop_without_worker_or_transport() -> None:
    emitted: list[str] = []
    redis = FakeRedis(pending=0, lag=0)

    code = await _run_restricted_live_queued_worker_once(
        _live_config(),
        emit_json=emitted.append,
        redis_client_builder=lambda redis_url: redis,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 0
    assert payload["status"] == "noop"
    assert payload["reason_code"] == "no_queued_message"
    assert payload["redis_precheck"] == {"pending": 0, "lag": 0, "reason_code": None}
    assert "worker_once" not in payload
    assert payload["authority"] == {
        "telegram_transport_possible": True,
        "database_session_opened": False,
        "workers_started": False,
        "run_forever_started": False,
        "openai_called": False,
        "github_called": False,
        "docker_or_systemd_called": False,
        "alembic_or_ddl_ran": False,
        "subprocess_started": False,
        "shell_invoked": False,
    }
    assert redis.closed is True


@pytest.mark.asyncio
async def test_rejects_lag_above_one_by_default() -> None:
    emitted: list[str] = []

    code = await _run_restricted_live_queued_worker_once(
        _live_config(),
        emit_json=emitted.append,
        redis_client_builder=lambda redis_url: FakeRedis(pending=0, lag=2),
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "queue_lag_exceeds_restricted_worker_once_limit"
    assert payload["redis_precheck"] == {"pending": 0, "lag": 2, "reason_code": None}


@pytest.mark.asyncio
async def test_processes_exactly_one_fake_queued_message_when_lag_is_one(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail_if_proof_setup_called(*args, **kwargs):
        raise AssertionError("real queued worker-once must not create proof plans or events")

    monkeypatch.setattr(
        notifier_main,
        "create_restricted_live_worker_once_proof_with_repository",
        fail_if_proof_setup_called,
    )
    emitted: list[str] = []
    redis = FakeRedis(pending=0, lag=1)
    consumer = OneMessageConsumer(_valid_message())
    service = RecordingService()

    async def worker_once_runner(config, emit_json):
        return await _run_fake_worker_once(config, emit_json, consumer=consumer, service=service)

    code = await _run_restricted_live_queued_worker_once(
        _live_config(),
        emit_json=emitted.append,
        redis_client_builder=lambda redis_url: redis,
        worker_once_runner=worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "restricted_live_queued_worker_once_processed"
    assert payload["worker_once"] == {
        "exit_code": 0,
        "status": "processed",
        "reason_code": "processed",
        "acked": True,
        "handler_called": True,
    }
    assert payload["delivery_result_summary"] == {
        "delivery_status": "sent",
        "attempt_count": 1,
        "transport_error_code": None,
        "transport_error_class": None,
        "telegram_chat_id_present": True,
        "telegram_message_id_present": True,
        "retry_after_seconds_present": False,
        "edited": False,
    }
    assert set(consumer.message.fields) == set(REQUIRED_THIN_QUEUE_FIELDS)
    assert "payload_json" not in consumer.message.fields
    assert consumer.read_calls == 1
    assert consumer.acked == ["1-0"]
    assert service.calls == [consumer.message.fields["trigger_event_id"]]
    assert payload["authority"]["database_session_opened"] is True
    assert payload["authority"]["workers_started"] is False
    assert payload["authority"]["run_forever_started"] is False
    assert payload["authority"]["openai_called"] is False
    assert payload["authority"]["github_called"] is False
    assert payload["authority"]["docker_or_systemd_called"] is False
    assert payload["authority"]["alembic_or_ddl_ran"] is False


@pytest.mark.asyncio
async def test_payload_json_field_is_rejected_by_worker_once_and_not_echoed() -> None:
    emitted: list[str] = []
    consumer = OneMessageConsumer(_valid_message(field_overrides={"payload_json": "{}"}))
    service = RecordingService()

    async def worker_once_runner(config, emit_json):
        return await _run_fake_worker_once(config, emit_json, consumer=consumer, service=service)

    code = await _run_restricted_live_queued_worker_once(
        _live_config(),
        emit_json=emitted.append,
        redis_client_builder=lambda redis_url: FakeRedis(pending=0, lag=1),
        worker_once_runner=worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 1
    assert payload["status"] == "fail"
    assert payload["reason_code"] == "worker_once_rejected"
    assert payload["worker_once"]["status"] == "rejected"
    assert payload["worker_once"]["acked"] is False
    assert service.calls == []
    assert consumer.acked == []
    assert "payload_json" not in emitted[0]


@pytest.mark.asyncio
async def test_output_is_sanitized_on_success_and_worker_exception() -> None:
    emitted: list[str] = []
    consumer = OneMessageConsumer(_valid_message())
    service = RecordingService()

    async def worker_once_runner(config, emit_json):
        return await _run_fake_worker_once(config, emit_json, consumer=consumer, service=service)

    code = await _run_restricted_live_queued_worker_once(
        _live_config(),
        emit_json=emitted.append,
        redis_client_builder=lambda redis_url: FakeRedis(pending=0, lag=1),
        worker_once_runner=worker_once_runner,
    )

    assert code == 0
    for forbidden in [
        "unit-live-send-credential",
        "postgresql+psycopg",
        "redis://",
        "telegram_response_json",
        "RAW_TELEGRAM_RESPONSE_BODY",
        "rendered message",
        "payload_json",
        "Traceback",
        "password",
    ]:
        assert forbidden not in emitted[0]

    failure_emitted: list[str] = []

    async def failing_worker_once_runner(config, emit_json):
        del config, emit_json
        raise RuntimeError(
            "RAW_EXCEPTION_SENTINEL Traceback DATABASE_URL REDIS_URL TELEGRAM_BOT_TOKEN "
            "postgresql+psycopg://user:password@example/db redis://:password@example/0"
        )

    failure_code = await _run_restricted_live_queued_worker_once(
        _live_config(),
        emit_json=failure_emitted.append,
        redis_client_builder=lambda redis_url: FakeRedis(pending=0, lag=1),
        worker_once_runner=failing_worker_once_runner,
    )
    failure_payload = json.loads(failure_emitted[0])

    assert failure_code == 1
    assert failure_payload["status"] == "fail"
    assert failure_payload["reason_code"] == "worker_once_exception"
    for forbidden in [
        "RAW_EXCEPTION_SENTINEL",
        "Traceback",
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "postgresql+psycopg",
        "redis://",
        "password",
    ]:
        assert forbidden not in failure_emitted[0]


def test_parser_accepts_restricted_live_queued_worker_once_command() -> None:
    args = build_parser().parse_args(
        [
            "restricted-live-queued-worker-once",
            "--operator-confirmed",
            "--env-file",
            "/tmp/notifier-runtime.env",
            "--max-lag",
            "1",
            "--format",
            "json",
        ]
    )

    assert args.command == "restricted-live-queued-worker-once"
    assert args.operator_confirmed is True
    assert args.env_file == "/tmp/notifier-runtime.env"
    assert args.max_lag == 1
    assert args.format == "json"


class FakeRedis:
    def __init__(self, *, pending: int = 0, lag: int = 1, metrics_unavailable: bool = False) -> None:
        self.pending = pending
        self.lag = lag
        self.metrics_unavailable = metrics_unavailable
        self.closed = False

    async def xinfo_groups(self, name: str):
        assert name == EXPECTED_QUEUE_NAME
        if self.metrics_unavailable:
            raise RuntimeError("metrics unavailable")
        return [{"name": "notifier-telegram", "pending": self.pending, "lag": self.lag}]

    async def aclose(self) -> None:
        self.closed = True


class OneMessageConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self.message = message
        self.read_calls = 0
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        return None

    async def read_batch(self) -> list[StreamMessage]:
        self.read_calls += 1
        return [self.message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class RecordingService:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def handle_trigger_event(self, trigger_event_id: str):
        self.calls.append(trigger_event_id)
        return DeliveryResult(
            delivery_status="sent",
            telegram_chat_id=12345,
            telegram_message_id=9001,
            attempt_count=1,
            telegram_response_json={
                "ok": True,
                "description": "RAW_TELEGRAM_RESPONSE_BODY",
                "result": {"text": "rendered message", "message_id": 9001},
            },
        )


async def _run_fake_worker_once(config, emit_json, *, consumer: OneMessageConsumer, service: RecordingService) -> int:
    async def runtime_builder(cfg, state, logger):
        del cfg, logger

        class StateTrackingService:
            async def handle_trigger_event(self, trigger_event_id: str):
                state.database_session_opened = True
                return await service.handle_trigger_event(trigger_event_id)

        async def dispose() -> None:
            return None

        return WorkerOnceRuntime(consumer=consumer, service=StateTrackingService(), dispose=dispose)

    return await run_worker_once_invocation(
        queue=EXPECTED_QUEUE_NAME,
        confirm_worker_once=True,
        output_format="json",
        emit_json=emit_json,
        config_loader=lambda: config,
        runtime_builder=runtime_builder,
    )


def _valid_message(*, field_overrides: dict[str, str | None] | None = None) -> StreamMessage:
    trigger_event_id = uuid4()
    fields = {
        "job_id": f"notify:{trigger_event_id}",
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": "notification_plan",
        "root_object_id": str(uuid4()),
        "idempotency_key": f"q-notification-send:{trigger_event_id}",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(trigger_event_id),
    }
    for key, value in (field_overrides or {}).items():
        if value is None:
            fields.pop(key, None)
        else:
            fields[key] = value
    return StreamMessage(stream=EXPECTED_QUEUE_NAME, message_id="1-0", fields=fields)


def _unused_redis_builder(redis_url: str):
    del redis_url
    raise AssertionError("redis must not be opened")


async def _unused_worker_once_runner(config, emit_json):
    del config, emit_json
    raise AssertionError("worker-once must not run")
