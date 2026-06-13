from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import uuid4

import pytest

from src.services.notifier_telegram.bounded_invocation import (
    BoundedInvocationState,
    BoundedNotifierDryRunInvocationResult,
)
from src.services.notifier_telegram.bounded_queue_invocation import (
    BoundedNotificationQueueRuntimeConfig,
    BoundedNotifierQueueDryRunConfig,
    load_bounded_notification_queue_config,
    run_bounded_notifier_queue_dry_run_invocation,
)
from src.services.notifier_telegram.models import StreamMessage


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/notifier_telegram/bounded_queue_invocation.py"
REDIS_URL = "redis://sentinel_redis_url"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
BOT_TOKEN = "123456:sentinel_bot_token"
RAW_PAYLOAD = "sentinel raw redis payload"
RAW_MESSAGE_TEXT = "sentinel rendered message text"
EXCEPTION_DETAIL = "sentinel private exception detail"


class FakeConsumer:
    def __init__(self, message: StreamMessage | None = None, *, read_error: BaseException | None = None) -> None:
        self.message = message
        self.read_error = read_error
        self.read_calls = 0
        self.acked: list[str] = []
        self.close_calls = 0

    async def read_one(self) -> StreamMessage | None:
        self.read_calls += 1
        if self.read_error is not None:
            raise self.read_error
        return self.message

    async def ack(self, message_id: str) -> int:
        self.acked.append(message_id)
        return 1

    async def close(self) -> None:
        self.close_calls += 1


class FakeConsumerBuilder:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer
        self.calls = 0
        self.configs: list[BoundedNotificationQueueRuntimeConfig] = []

    async def __call__(self, queue_config, state, logger):
        del logger
        self.calls += 1
        self.configs.append(queue_config)
        state.queue_consumer_created = True
        return self.consumer


class FakeBoundedInvocationRunner:
    def __init__(self, result: BoundedNotifierDryRunInvocationResult | None = None) -> None:
        self.result = result or _pass_invocation_result()
        self.calls = []

    async def __call__(self, config, *, notifier_config_loader, runtime_builder=None, logger=None):
        del notifier_config_loader, runtime_builder, logger
        self.calls.append(config)
        return self.result


def _queue_config() -> BoundedNotificationQueueRuntimeConfig:
    return BoundedNotificationQueueRuntimeConfig(redis_url=REDIS_URL)


def _raising_queue_config() -> BoundedNotificationQueueRuntimeConfig:
    raise AssertionError("queue config must not be loaded")


def _message(trigger_event_id: str | None = None, *, message_id: str = "1-0") -> StreamMessage:
    fields = {
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": RAW_PAYLOAD,
        "idempotency_key": RAW_PAYLOAD,
        "rendered_message_text": RAW_MESSAGE_TEXT,
    }
    if trigger_event_id is not None:
        fields["trigger_event_id"] = trigger_event_id
    return StreamMessage(stream="q.notification.send", message_id=message_id, fields=fields)


def _pass_invocation_result() -> BoundedNotifierDryRunInvocationResult:
    state = BoundedInvocationState(
        database_session_opened=True,
        event_outbox_read_attempted=True,
        notifier_invocation_attempted=True,
    )
    return BoundedNotifierDryRunInvocationResult(
        status="pass",
        ok=True,
        error_code=None,
        trigger_event_id_present=True,
        operator_approved=True,
        database_write_allowed=True,
        processed_event_count=1,
        event_type_supported=True,
        delivery_result_summary={
            "delivery_status": "suppressed",
            "transport_error_code": "dry_run_skip_transport",
            "telegram_chat_id_present": False,
            "telegram_message_id_present": False,
        },
        notifier_owned_write_counts={"notification_delivery_records_insert_calls": 1},
        state=state,
    )


def _failed_invocation_result() -> BoundedNotifierDryRunInvocationResult:
    state = BoundedInvocationState(
        database_session_opened=True,
        event_outbox_read_attempted=True,
        notifier_invocation_attempted=True,
    )
    return BoundedNotifierDryRunInvocationResult(
        status="failed",
        ok=False,
        error_code="notifier_invocation_failed",
        trigger_event_id_present=True,
        operator_approved=True,
        database_write_allowed=True,
        processed_event_count=0,
        state=state,
    )


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_redis_db_or_action() -> None:
    consumer = FakeConsumer(_message(str(uuid4())))
    consumer_builder = FakeConsumerBuilder(consumer)
    invocation_runner = FakeBoundedInvocationRunner()

    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(),
        queue_config_loader=_raising_queue_config,
        consumer_builder=consumer_builder,
        bounded_invocation_runner=invocation_runner,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["redis_read_attempted"] is False
    assert report["redis_message_count"] == 0
    assert report["redis_ack_attempted"] is False
    assert report["redis_ack_count"] == 0
    assert report["bounded_invocation_attempted"] is False
    assert report["processed_event_count"] == 0
    assert report["side_effects"]["queue_consumer_created"] is False
    assert consumer_builder.calls == 0
    assert consumer.read_calls == 0
    assert invocation_runner.calls == []


@pytest.mark.asyncio
async def test_missing_redis_read_allowance_blocks_before_redis() -> None:
    consumer = FakeConsumer(_message(str(uuid4())))
    consumer_builder = FakeConsumerBuilder(consumer)

    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(operator_approved=True),
        queue_config_loader=_raising_queue_config,
        consumer_builder=consumer_builder,
        bounded_invocation_runner=FakeBoundedInvocationRunner(),
    )

    assert result.error_code == "redis_read_not_allowed"
    assert result.state.redis_read_attempted is False
    assert consumer_builder.calls == 0
    assert consumer.read_calls == 0


@pytest.mark.asyncio
async def test_missing_database_write_allowance_blocks_after_one_read_before_invocation() -> None:
    trigger_event_id = uuid4()
    consumer = FakeConsumer(_message(str(trigger_event_id)))
    consumer_builder = FakeConsumerBuilder(consumer)
    invocation_runner = FakeBoundedInvocationRunner()

    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(operator_approved=True, allow_redis_read=True),
        queue_config_loader=_queue_config,
        consumer_builder=consumer_builder,
        bounded_invocation_runner=invocation_runner,
    )
    report = result.to_sanitized_dict()

    assert report["error_code"] == "database_write_not_allowed"
    assert report["redis_read_attempted"] is True
    assert report["redis_message_count"] == 1
    assert report["bounded_invocation_attempted"] is False
    assert consumer.read_calls == 1
    assert consumer.acked == []
    assert consumer.close_calls == 1
    assert invocation_runner.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "expected_code", "present"),
    [
        (_message(None), "trigger_event_id_missing", False),
        (_message("not-a-uuid"), "trigger_event_id_invalid", True),
    ],
)
async def test_missing_or_invalid_trigger_event_id_blocks_before_db_invocation(
    message: StreamMessage,
    expected_code: str,
    present: bool,
) -> None:
    consumer = FakeConsumer(message)
    consumer_builder = FakeConsumerBuilder(consumer)
    invocation_runner = FakeBoundedInvocationRunner()

    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(
            operator_approved=True,
            allow_redis_read=True,
            allow_database_write=True,
        ),
        queue_config_loader=_queue_config,
        consumer_builder=consumer_builder,
        bounded_invocation_runner=invocation_runner,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == expected_code
    assert report["trigger_event_id_present"] is present
    assert report["bounded_invocation_attempted"] is False
    assert consumer.read_calls == 1
    assert consumer.acked == []
    assert invocation_runner.calls == []


@pytest.mark.asyncio
async def test_one_valid_message_delegates_once_and_preserves_forced_dry_run_summary() -> None:
    trigger_event_id = uuid4()
    consumer = FakeConsumer(_message(str(trigger_event_id)))
    consumer_builder = FakeConsumerBuilder(consumer)
    invocation_runner = FakeBoundedInvocationRunner()

    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(
            operator_approved=True,
            allow_redis_read=True,
            allow_database_write=True,
        ),
        queue_config_loader=_queue_config,
        consumer_builder=consumer_builder,
        bounded_invocation_runner=invocation_runner,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["send_enabled"] is False
    assert report["dry_run"] is True
    assert report["edits_allowed"] is False
    assert report["redis_message_count"] == 1
    assert report["processed_event_count"] == 1
    assert report["redis_ack_attempted"] is False
    assert report["redis_ack_count"] == 0
    assert report["bounded_invocation_summary"]["send_enabled"] is False
    assert report["bounded_invocation_summary"]["dry_run"] is True
    assert report["bounded_invocation_summary"]["edits_allowed"] is False
    assert report["bounded_invocation_summary"]["delivery_result_summary"]["transport_error_code"] == (
        "dry_run_skip_transport"
    )
    assert len(invocation_runner.calls) == 1
    delegated = invocation_runner.calls[0]
    assert delegated.trigger_event_id == str(trigger_event_id)
    assert delegated.operator_approved is True
    assert delegated.allow_database_write is True
    assert consumer.read_calls == 1
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_pass_with_explicit_ack_allows_exactly_one_ack() -> None:
    trigger_event_id = uuid4()
    consumer = FakeConsumer(_message(str(trigger_event_id), message_id="2-0"))

    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(
            operator_approved=True,
            allow_redis_read=True,
            allow_database_write=True,
            allow_redis_ack=True,
        ),
        queue_config_loader=_queue_config,
        consumer_builder=FakeConsumerBuilder(consumer),
        bounded_invocation_runner=FakeBoundedInvocationRunner(),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["redis_ack_attempted"] is True
    assert report["redis_ack_count"] == 1
    assert consumer.acked == ["2-0"]


@pytest.mark.asyncio
async def test_failed_bounded_invocation_does_not_ack_even_when_ack_allowed() -> None:
    trigger_event_id = uuid4()
    consumer = FakeConsumer(_message(str(trigger_event_id)))

    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(
            operator_approved=True,
            allow_redis_read=True,
            allow_database_write=True,
            allow_redis_ack=True,
        ),
        queue_config_loader=_queue_config,
        consumer_builder=FakeConsumerBuilder(consumer),
        bounded_invocation_runner=FakeBoundedInvocationRunner(_failed_invocation_result()),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "failed"
    assert report["ok"] is False
    assert report["error_code"] == "notifier_invocation_failed"
    assert report["redis_ack_attempted"] is False
    assert report["redis_ack_count"] == 0
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_sanitized_report_omits_urls_tokens_payload_text_and_exception_detail() -> None:
    trigger_event_id = uuid4()
    consumer = FakeConsumer(_message(str(trigger_event_id), message_id="secret-message-id"))
    result = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(
            operator_approved=True,
            allow_redis_read=True,
            allow_database_write=True,
        ),
        queue_config_loader=lambda: BoundedNotificationQueueRuntimeConfig(redis_url=REDIS_URL),
        consumer_builder=FakeConsumerBuilder(consumer),
        bounded_invocation_runner=FakeBoundedInvocationRunner(),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    for raw in (REDIS_URL, DB_URL, BOT_TOKEN, RAW_PAYLOAD, RAW_MESSAGE_TEXT, "secret-message-id", str(trigger_event_id)):
        assert raw not in rendered

    failing_consumer = FakeConsumer(read_error=RuntimeError(EXCEPTION_DETAIL))
    failed = await run_bounded_notifier_queue_dry_run_invocation(
        BoundedNotifierQueueDryRunConfig(operator_approved=True, allow_redis_read=True),
        queue_config_loader=_queue_config,
        consumer_builder=FakeConsumerBuilder(failing_consumer),
        bounded_invocation_runner=FakeBoundedInvocationRunner(),
    )
    failed_text = json.dumps(failed.to_sanitized_dict(), sort_keys=True)
    assert failed.error_code == "redis_read_failed"
    assert EXCEPTION_DETAIL not in failed_text


def test_queue_config_requires_redis_url_and_fixed_queue_name() -> None:
    assert load_bounded_notification_queue_config({"REDIS_URL": REDIS_URL}).queue_name == "q.notification.send"
    with pytest.raises(Exception) as missing:
        load_bounded_notification_queue_config({})
    assert getattr(missing.value, "error_code") == "redis_url_missing"
    with pytest.raises(Exception) as wrong_queue:
        load_bounded_notification_queue_config(
            {"REDIS_URL": REDIS_URL, "NOTIFIER_TELEGRAM_QUEUE_NAME": "q.other"}
        )
    assert getattr(wrong_queue.value, "error_code") == "queue_name_not_allowed"


def test_source_has_no_worker_loop_run_forever_or_polling_sleep() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert not any(isinstance(node, ast.While) for node in ast.walk(tree))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"run_forever", "sleep", "ensure_group"}
    assert "asyncio.sleep" not in source
    assert "run_forever()" not in source
