from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import uuid4

from src.services.notifier_telegram import bounded_queue_invocation
from src.services.notifier_telegram.bounded_invocation import (
    BoundedInvocationState,
    BoundedNotifierDryRunInvocationResult,
)
from src.services.notifier_telegram.bounded_queue_invocation import BoundedNotificationQueueRuntimeConfig
from src.services.notifier_telegram.models import StreamMessage
from tools import bounded_notifier_queue_dry_run_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_notifier_queue_dry_run_runner.py"
REDIS_URL = "redis://sentinel_cli_redis_url"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
BOT_TOKEN = "123456:sentinel_cli_bot_token"
RAW_PAYLOAD = "sentinel cli raw redis payload"
RAW_MESSAGE_TEXT = "sentinel cli rendered message text"


class FakeConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self.message = message
        self.read_calls = 0
        self.acked: list[str] = []

    async def read_one(self) -> StreamMessage | None:
        self.read_calls += 1
        return self.message

    async def ack(self, message_id: str) -> int:
        self.acked.append(message_id)
        return 1

    async def close(self) -> None:
        return None


class FakeConsumerBuilder:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer

    async def __call__(self, queue_config, state, logger):
        del queue_config, logger
        state.queue_consumer_created = True
        return self.consumer


class FakeBoundedInvocationRunner:
    def __init__(self) -> None:
        self.calls = []

    async def __call__(self, config, *, notifier_config_loader, runtime_builder=None, logger=None):
        del notifier_config_loader, runtime_builder, logger
        self.calls.append(config)
        return _pass_invocation_result()


def _message(trigger_event_id: str) -> StreamMessage:
    return StreamMessage(
        stream="q.notification.send",
        message_id="secret-cli-message-id",
        fields={
            "trigger_event_id": trigger_event_id,
            "stage_name": "notify",
            "root_object_type": "analysis",
            "root_object_id": RAW_PAYLOAD,
            "idempotency_key": RAW_PAYLOAD,
            "rendered_message_text": RAW_MESSAGE_TEXT,
        },
    )


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
        delivery_result_summary={"delivery_status": "suppressed", "transport_error_code": "dry_run_skip_transport"},
        notifier_owned_write_counts={"notification_delivery_records_insert_calls": 1},
        state=state,
    )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_bounded_queue_invocation_module() -> None:
    assert runner.BoundedNotifierQueueDryRunConfig is bounded_queue_invocation.BoundedNotifierQueueDryRunConfig
    assert runner.BoundedNotificationQueueRuntimeConfig is (
        bounded_queue_invocation.BoundedNotificationQueueRuntimeConfig
    )


def test_main_with_no_flags_returns_required_fail_closed_json(capsys) -> None:
    exit_code = runner.main([])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["schema_version"] == "bounded_notifier_queue_dry_run_invocation_v1"
    assert parsed["runner_name"] == "bounded_notifier_queue_dry_run_runner"
    assert parsed["mode"] == "notifier_queue_dry_run_send_disabled_one_shot"
    assert parsed["queue_name"] == "q.notification.send"
    assert parsed["operator_approved"] is False
    assert parsed["redis_read_allowed"] is False
    assert parsed["database_write_allowed"] is False
    assert parsed["redis_ack_allowed"] is False
    assert parsed["send_enabled"] is False
    assert parsed["dry_run"] is True
    assert parsed["edits_allowed"] is False
    assert parsed["redis_read_attempted"] is False
    assert parsed["redis_message_count"] == 0
    assert parsed["redis_ack_attempted"] is False
    assert parsed["redis_ack_count"] == 0
    assert parsed["trigger_event_id_present"] is False
    assert parsed["bounded_invocation_attempted"] is False
    assert parsed["processed_event_count"] == 0
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["redis_stream_read"] is False
    assert parsed["side_effects"]["database_session_opened"] is False
    assert parsed["side_effects"]["telegram_send_called"] is False


def test_valid_cli_run_prints_sanitized_json_and_delegates_once(capsys) -> None:
    trigger_event_id = uuid4()
    consumer = FakeConsumer(_message(str(trigger_event_id)))
    invocation_runner = FakeBoundedInvocationRunner()

    exit_code = runner.main(
        ["--operator-approved", "--allow-redis-read", "--allow-database-write"],
        queue_config_loader=lambda: BoundedNotificationQueueRuntimeConfig(redis_url=REDIS_URL),
        consumer_builder=FakeConsumerBuilder(consumer),
        bounded_invocation_runner=invocation_runner,
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["redis_read_attempted"] is True
    assert parsed["redis_message_count"] == 1
    assert parsed["redis_ack_attempted"] is False
    assert parsed["processed_event_count"] == 1
    assert parsed["bounded_invocation_summary"]["dry_run"] is True
    assert parsed["bounded_invocation_summary"]["send_enabled"] is False
    assert parsed["bounded_invocation_summary"]["edits_allowed"] is False
    assert len(invocation_runner.calls) == 1
    assert invocation_runner.calls[0].trigger_event_id == str(trigger_event_id)
    assert consumer.read_calls == 1
    for raw in (REDIS_URL, DB_URL, BOT_TOKEN, RAW_PAYLOAD, RAW_MESSAGE_TEXT, "secret-cli-message-id", str(trigger_event_id)):
        assert raw not in output


def test_live_send_network_edit_and_direct_event_flags_are_rejected_as_json(capsys) -> None:
    for flag in (
        "--allow-send",
        "--allow-network",
        "--allow-edits",
        "--edit",
        "--telegram-bot-token",
        "--trigger-event-id",
    ):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["redis_read_attempted"] is False
        assert parsed["bounded_invocation_attempted"] is False


def test_run_with_explicit_ack_flag_acks_fake_message_once() -> None:
    trigger_event_id = uuid4()
    consumer = FakeConsumer(_message(str(trigger_event_id)))
    invocation_runner = FakeBoundedInvocationRunner()

    result = runner.run(
        _parse_args("--operator-approved", "--allow-redis-read", "--allow-database-write", "--allow-redis-ack"),
        queue_config_loader=lambda: BoundedNotificationQueueRuntimeConfig(redis_url=REDIS_URL),
        consumer_builder=FakeConsumerBuilder(consumer),
        bounded_invocation_runner=invocation_runner,
    )

    assert result.exit_code == 0
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_count"] == 1
    assert consumer.acked == ["secret-cli-message-id"]


def test_tool_source_imports_no_db_redis_openai_github_x_web_or_telegram_clients() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert {"sqlalchemy", "redis", "openai", "requests", "httpx", "aiohttp", "telegram"}.isdisjoint(imported_roots)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert parser_flags == {
        "--operator-approved",
        "--allow-redis-read",
        "--allow-database-write",
        "--allow-redis-ack",
    }
    assert "allow-send" not in source
    assert "allow-network" not in source
    assert "allow-edits" not in source
    assert "TELEGRAM_BOT_TOKEN" not in source
    assert "DATABASE_URL" not in source
    assert "REDIS_URL" not in source
    assert "traceback" not in source.lower()
    assert "print(" not in source
    assert "render_sanitized_json(result.report)" in source
