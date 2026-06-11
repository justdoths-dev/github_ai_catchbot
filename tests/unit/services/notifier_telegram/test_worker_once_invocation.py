from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from services.notifier_telegram.main import build_parser
from services.notifier_telegram.models import StreamMessage
from services.notifier_telegram.worker_once import (
    EXPECTED_QUEUE_NAME,
    EXPECTED_STAGE_NAME,
    SCHEMA_VERSION,
    WorkerOnceRuntime,
    run_worker_once_invocation,
)
from tests.unit.services.notifier_telegram._service_fakes import config


class FakeConfigLoader:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return config(dry_run=False, enable_notification_send=True)


class FakeConsumer:
    def __init__(self, messages: list[StreamMessage], events: list[str]) -> None:
        self._messages = messages
        self._events = events
        self.acked: list[str] = []
        self.read_calls = 0

    async def ensure_group(self) -> None:
        self._events.append("ensure_group")

    async def read_batch(self) -> list[StreamMessage]:
        self.read_calls += 1
        self._events.append("read_batch")
        return self._messages

    async def ack(self, message_id: str) -> None:
        self._events.append(f"ack:{message_id}")
        self.acked.append(message_id)


class FakeService:
    def __init__(self, events: list[str], exc: Exception | None = None, result: object = object()) -> None:
        self._events = events
        self._exc = exc
        self._result = result
        self.calls: list[str] = []

    async def handle_trigger_event(self, trigger_event_id: str) -> object:
        self._events.append(f"handler_start:{trigger_event_id}")
        self.calls.append(trigger_event_id)
        if self._exc is not None:
            raise self._exc
        self._events.append(f"handler_done:{trigger_event_id}")
        return self._result


class FakeRuntimeBuilder:
    def __init__(self, consumer: FakeConsumer, service: FakeService, events: list[str]) -> None:
        self._consumer = consumer
        self._service = service
        self._events = events
        self.calls = 0
        self.config_batch_sizes: list[int] = []
        self.disposed = 0

    async def __call__(self, cfg, state, logger) -> WorkerOnceRuntime:
        del state, logger
        self.calls += 1
        self.config_batch_sizes.append(cfg.batch_size)

        async def dispose() -> None:
            self.disposed += 1
            self._events.append("dispose")

        return WorkerOnceRuntime(consumer=self._consumer, service=self._service, dispose=dispose)


class RecordingWorker:
    instances: list["RecordingWorker"] = []

    def __init__(self, cfg, *, consumer, service, logger=None) -> None:
        del cfg, logger
        self._consumer = consumer
        self._service = service
        self.run_once_called = False
        self.run_forever_called = False
        RecordingWorker.instances.append(self)

    async def run_once(self):
        self.run_once_called = True
        messages = await self._consumer.read_batch()
        for message in messages:
            await self._service.handle_trigger_event(message.fields["trigger_event_id"])
            await self._consumer.ack(message.message_id)

    async def run_forever(self):
        self.run_forever_called = True
        raise AssertionError("run_forever must not be called by worker-once")


@pytest.mark.asyncio
async def test_missing_confirm_rejects_before_config_redis_db_or_handler() -> None:
    trigger_event_id = uuid4()
    events: list[str] = []
    loader = FakeConfigLoader()
    consumer = FakeConsumer([_valid_message(trigger_event_id)], events)
    service = FakeService(events)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(
        queue=EXPECTED_QUEUE_NAME,
        confirm_worker_once=False,
        config_loader=loader,
        runtime_builder=runtime_builder,
    )

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "confirm_worker_once_required"
    assert loader.calls == 0
    assert runtime_builder.calls == 0
    assert consumer.read_calls == 0
    assert consumer.acked == []
    assert service.calls == []
    assert events == []


@pytest.mark.asyncio
async def test_unsupported_queue_rejects_before_runtime_construction() -> None:
    events: list[str] = []
    loader = FakeConfigLoader()
    consumer = FakeConsumer([_valid_message()], events)
    service = FakeService(events)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(
        queue="q.notification.other",
        config_loader=loader,
        runtime_builder=runtime_builder,
    )

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "unsupported_queue"
    assert loader.calls == 0
    assert runtime_builder.calls == 0
    assert service.calls == []


@pytest.mark.asyncio
async def test_unsupported_format_rejects_before_runtime_construction() -> None:
    events: list[str] = []
    loader = FakeConfigLoader()
    consumer = FakeConsumer([_valid_message()], events)
    service = FakeService(events)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(
        output_format="text",
        config_loader=loader,
        runtime_builder=runtime_builder,
    )

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "unsupported_format"
    assert loader.calls == 0
    assert runtime_builder.calls == 0
    assert service.calls == []


@pytest.mark.asyncio
async def test_empty_queue_returns_empty_without_handler_or_ack() -> None:
    events: list[str] = []
    loader = FakeConfigLoader()
    consumer = FakeConsumer([], events)
    service = FakeService(events)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(
        config_loader=loader,
        runtime_builder=runtime_builder,
    )

    assert code == 0
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["status"] == "empty"
    assert payload["reason_code"] == "no_message_available"
    assert payload["message_seen"] is False
    assert payload["handler_called"] is False
    assert payload["acked"] is False
    assert payload["authority"]["redis_read"] is True
    assert payload["authority"]["redis_ack"] is False
    assert loader.calls == 1
    assert runtime_builder.calls == 1
    assert runtime_builder.config_batch_sizes == [1]
    assert service.calls == []
    assert consumer.acked == []
    assert events == ["read_batch", "dispose"]


@pytest.mark.asyncio
async def test_malformed_message_without_trigger_event_rejects_without_handler_or_ack() -> None:
    events: list[str] = []
    consumer = FakeConsumer([_valid_message(field_overrides={"trigger_event_id": ""})], events)
    service = FakeService(events)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(runtime_builder=runtime_builder)

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "malformed_message"
    assert payload["message_seen"] is True
    assert payload["trigger_event_id_present"] is False
    assert payload["handler_called"] is False
    assert payload["acked"] is False
    assert service.calls == []
    assert consumer.acked == []
    assert events == ["read_batch", "dispose"]


@pytest.mark.asyncio
async def test_valid_message_calls_handler_once_and_acks_after_success() -> None:
    trigger_event_id = uuid4()
    events: list[str] = []
    consumer = FakeConsumer([_valid_message(trigger_event_id)], events)
    service = FakeService(events)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(runtime_builder=runtime_builder)

    assert code == 0
    assert payload["status"] == "processed"
    assert payload["reason_code"] == "processed"
    assert payload["message_seen"] is True
    assert payload["handler_called"] is True
    assert payload["acked"] is True
    assert payload["trigger_event_id_present"] is True
    assert service.calls == [str(trigger_event_id)]
    assert consumer.acked == ["1-0"]
    assert events == [
        "read_batch",
        f"handler_start:{trigger_event_id}",
        f"handler_done:{trigger_event_id}",
        "ack:1-0",
        "dispose",
    ]


@pytest.mark.asyncio
async def test_handler_failure_does_not_report_success_or_ack() -> None:
    trigger_event_id = uuid4()
    events: list[str] = []
    consumer = FakeConsumer([_valid_message(trigger_event_id)], events)
    service = FakeService(events, exc=RuntimeError("RAW_EXCEPTION_SENTINEL password"))
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, output = await _invoke(runtime_builder=runtime_builder)

    assert code == 1
    assert payload["status"] == "failed"
    assert payload["reason_code"] == "handler_failed"
    assert payload["handler_called"] is True
    assert payload["acked"] is False
    assert consumer.acked == []
    assert events == ["read_batch", f"handler_start:{trigger_event_id}", "dispose"]
    assert "RAW_EXCEPTION_SENTINEL" not in output
    assert "password" not in output


@pytest.mark.asyncio
async def test_handler_none_result_does_not_ack_as_successful() -> None:
    trigger_event_id = uuid4()
    events: list[str] = []
    consumer = FakeConsumer([_valid_message(trigger_event_id)], events)
    service = FakeService(events, result=None)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(runtime_builder=runtime_builder)

    assert code == 1
    assert payload["status"] == "failed"
    assert payload["reason_code"] == "handler_failed"
    assert payload["handler_called"] is True
    assert payload["acked"] is False
    assert service.calls == [str(trigger_event_id)]
    assert consumer.acked == []
    assert events == [
        "read_batch",
        f"handler_start:{trigger_event_id}",
        f"handler_done:{trigger_event_id}",
        "dispose",
    ]


@pytest.mark.asyncio
async def test_worker_once_command_does_not_call_run_forever() -> None:
    RecordingWorker.instances = []
    trigger_event_id = uuid4()
    events: list[str] = []
    consumer = FakeConsumer([_valid_message(trigger_event_id)], events)
    service = FakeService(events)
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, _ = await _invoke(
        runtime_builder=runtime_builder,
        worker_builder=RecordingWorker,
    )

    assert code == 0
    assert payload["status"] == "processed"
    assert len(RecordingWorker.instances) == 1
    assert RecordingWorker.instances[0].run_once_called is True
    assert RecordingWorker.instances[0].run_forever_called is False
    assert payload["authority"]["workers_started"] is False
    assert payload["authority"]["run_forever_started"] is False


def test_parser_accepts_valid_worker_once_command() -> None:
    args = build_parser().parse_args(
        [
            "worker-once",
            "--queue",
            "q.notification.send",
            "--confirm-worker-once",
            "--format",
            "json",
        ]
    )

    assert args.command == "worker-once"
    assert args.queue == "q.notification.send"
    assert args.confirm_worker_once is True
    assert args.format == "json"


@pytest.mark.asyncio
async def test_output_sanitization_omits_secret_names_traceback_and_raw_exception_text() -> None:
    events: list[str] = []
    consumer = FakeConsumer([_valid_message()], events)
    service = FakeService(
        events,
        exc=RuntimeError(
            "RAW_EXCEPTION_SENTINEL DATABASE_URL REDIS_URL TELEGRAM_BOT_TOKEN OPENAI_API_KEY Traceback"
        ),
    )
    runtime_builder = FakeRuntimeBuilder(consumer, service, events)

    code, payload, output = await _invoke(runtime_builder=runtime_builder)

    assert code == 1
    assert payload["status"] == "failed"
    assert payload["reason_code"] == "handler_failed"
    for forbidden in [
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "OPENAI_API_KEY",
        "Traceback",
        "RAW_EXCEPTION_SENTINEL",
    ]:
        assert forbidden not in output


def test_worker_once_module_does_not_import_forbidden_boundaries() -> None:
    source_path = Path("src/services/notifier_telegram/worker_once.py")
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])

    assert imported_roots.isdisjoint({"subprocess", "openai", "github", "docker", "systemd"})
    assert "shell=True" not in source
    assert "os.system" not in source
    assert "Popen" not in source


def _valid_message(
    trigger_event_id=None,
    *,
    field_overrides: dict[str, str | None] | None = None,
) -> StreamMessage:
    event_id = trigger_event_id or uuid4()
    fields = {
        "job_id": f"notify:{event_id}",
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": "analysis",
        "root_object_id": str(uuid4()),
        "idempotency_key": f"q-notification-send:{event_id}",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }
    for key, value in (field_overrides or {}).items():
        if value is None:
            fields.pop(key, None)
        else:
            fields[key] = value
    return StreamMessage(stream=EXPECTED_QUEUE_NAME, message_id="1-0", fields=fields)


async def _invoke(
    *,
    queue: str | None = EXPECTED_QUEUE_NAME,
    confirm_worker_once: bool = True,
    output_format: str | None = "json",
    config_loader: FakeConfigLoader | None = None,
    runtime_builder: FakeRuntimeBuilder,
    worker_builder: Any = None,
) -> tuple[int, dict[str, Any], str]:
    emitted: list[str] = []
    code = await run_worker_once_invocation(
        queue=queue,
        confirm_worker_once=confirm_worker_once,
        output_format=output_format,
        emit_json=emitted.append,
        config_loader=config_loader or FakeConfigLoader(),
        runtime_builder=runtime_builder,
        worker_builder=worker_builder or __import__(
            "services.notifier_telegram.worker", fromlist=["NotifierTelegramWorker"]
        ).NotifierTelegramWorker,
    )

    assert len(emitted) == 1
    return code, json.loads(emitted[0]), emitted[0]
