from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from services.maintenance.bounded_runtime import BoundedMaintenanceRuntimeConfig, RedisTargetSelection
from services.maintenance.models import StreamMessage
from tests.component.services.maintenance._fakes import config
from tools import bounded_maintenance_recovery_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_maintenance_recovery_runner.py"
RAW_DB_URL = "postgresql+psycopg:" + "//" + "sentinel_user:sentinel_" + "pass" + "word@127.0.0.1/db"
RAW_REDIS_URL = "redis:" + "//" + ":sentinel_" + "pass" + "word@127.0.0.1/0"


class FakeQueueRuntime:
    def __init__(self, *, event_id, root_id, message_id: str) -> None:
        self.message = StreamMessage(
            stream="q.maintenance",
            message_id=message_id,
            fields={
                "stage_name": "maintenance",
                "root_object_type": "notification_plan",
                "root_object_id": str(root_id),
                "trigger_event_id": str(event_id),
                "payload_json": "sentinel raw payload",
                "target_chat_id": "123456789",
            },
        )
        self.state = None
        self.inspect_calls = 0
        self.consume_calls = 0
        self.ack_calls = 0

    async def inspect_target(self, config):
        self.inspect_calls += 1
        if self.state is not None:
            self.state.redis_read_attempted = True
        return RedisTargetSelection(
            status="matched",
            error_code=None,
            message=self.message,
            redis_message_count=1,
            group_lag=1,
            group_pending=0,
            message_stage_name="maintenance",
            message_root_object_type="notification_plan",
            trigger_event_id_present=True,
            root_object_id_present=True,
            redis_message_id_suffix=self.message.message_id[-5:],
            trigger_event_id_suffix=str(self.message.fields["trigger_event_id"])[-8:],
            root_object_id_suffix=str(self.message.fields["root_object_id"])[-8:],
        )

    async def consume_target(self, expected, config):
        self.consume_calls += 1
        return await self.inspect_target(config)

    async def load_outbox_event(self, trigger_event_id):
        raise AssertionError(f"preview should not load DB event {trigger_event_id}")

    async def invoke_maintenance(self, trigger_event_id):
        raise AssertionError(f"preview should not invoke maintenance {trigger_event_id}")

    async def invoke_replay(self, trigger_event_id):
        raise AssertionError(f"preview should not invoke replay {trigger_event_id}")

    async def commit_database(self):
        raise AssertionError("preview should not commit")

    async def rollback_database(self):
        return None

    async def ack(self, message_id):
        self.ack_calls += 1
        return 1

    async def close(self):
        return None


class FakeDueRuntime:
    def __init__(self) -> None:
        self.preview_calls = []

    async def preview_candidates(self, limit, now):
        self.preview_calls.append((limit, now))
        return []

    async def promote_due_retries_once(self, limit, now):
        raise AssertionError("preview should not execute due retry")

    async def commit_database(self):
        raise AssertionError("preview should not commit")

    async def rollback_database(self):
        return None

    async def close(self):
        return None


def _loader():
    return BoundedMaintenanceRuntimeConfig(maintenance_config=config())


def _maintenance_preview_argv(event_id, plan_id, message_id):
    return [
        "--operator-approved",
        "--allow-runtime-config",
        "--mode",
        "preview",
        "maintenance-result",
        "--allow-redis-read",
        "--trigger-event-suffix",
        str(event_id)[-8:],
        "--notification-plan-id-suffix",
        str(plan_id)[-8:],
        "--redis-message-suffix",
        message_id[-5:],
    ]


def test_missing_operator_approval_returns_json_without_stderr(capsys) -> None:
    event_id = uuid4()
    plan_id = uuid4()
    message_id = "1740000000000-42"

    exit_code = runner.main(
        [
            "--allow-runtime-config",
            "maintenance-result",
            "--allow-redis-read",
            "--trigger-event-suffix",
            str(event_id)[-8:],
            "--notification-plan-id-suffix",
            str(plan_id)[-8:],
            "--redis-message-suffix",
            message_id[-5:],
        ],
        runtime_config_loader=_loader,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_maintenance_recovery_runner_v1"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["runtime_config_loaded"] is False
    assert parsed["redis_consume_called"] is False
    assert parsed["redis_ack_attempted"] is False


def test_maintenance_preview_cli_delegates_and_redacts_raw_values(capsys) -> None:
    event_id = uuid4()
    plan_id = uuid4()
    message_id = "1740000000000-42"
    runtime = FakeQueueRuntime(event_id=event_id, root_id=plan_id, message_id=message_id)

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        runtime.state = state
        return runtime

    exit_code = runner.main(
        _maintenance_preview_argv(event_id, plan_id, message_id),
        runtime_config_loader=_loader,
        queue_runtime_builder=builder,
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["status"] == "pass"
    assert parsed["redis_read_attempted"] is True
    assert parsed["redis_consume_called"] is False
    assert parsed["target_event_suffix"] == str(event_id)[-8:]
    assert parsed["target_root_object_id_suffix"] == str(plan_id)[-8:]
    assert parsed["redis_message_id_suffix"] == message_id[-5:]
    assert runtime.inspect_calls == 1
    for raw in (
        str(event_id),
        str(plan_id),
        message_id,
        RAW_DB_URL,
        RAW_REDIS_URL,
        "sentinel raw payload",
        "123456789",
        "sentinel_" + "pass" + "word",
    ):
        assert raw not in output


def test_full_redis_stream_id_suffix_is_rejected_and_not_echoed(capsys) -> None:
    event_id = uuid4()
    plan_id = uuid4()
    full_message_id = "1740000000000-42"

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--mode",
            "preview",
            "maintenance-result",
            "--allow-redis-read",
            "--trigger-event-suffix",
            str(event_id)[-8:],
            "--notification-plan-id-suffix",
            str(plan_id)[-8:],
            "--redis-message-suffix",
            full_message_id,
        ],
        runtime_config_loader=lambda: (_ for _ in ()).throw(AssertionError("runtime config must not load")),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "redis_message_id_suffix_invalid"
    assert parsed["runtime_config_loaded"] is False
    assert parsed["redis_message_id_suffix"] is None
    assert full_message_id not in output


def test_full_uuid_suffix_is_rejected_and_not_echoed(capsys) -> None:
    event_id = uuid4()
    plan_id = uuid4()
    message_id = "1740000000000-42"

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--mode",
            "preview",
            "maintenance-result",
            "--allow-redis-read",
            "--trigger-event-suffix",
            str(event_id),
            "--notification-plan-id-suffix",
            str(plan_id)[-8:],
            "--redis-message-suffix",
            message_id[-5:],
        ],
        runtime_config_loader=lambda: (_ for _ in ()).throw(AssertionError("runtime config must not load")),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "suffix_ambiguous_or_missing"
    assert parsed["runtime_config_loaded"] is False
    assert parsed["target_event_suffix"] is None
    assert str(event_id) not in output


def test_due_retry_preview_cli_delegates_without_redis(capsys) -> None:
    runtime = FakeDueRuntime()

    async def builder(runtime_config, state, logger):
        del runtime_config, state, logger
        return runtime

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--mode",
            "preview",
            "due-retry",
            "--limit",
            "3",
            "--now-utc",
            "2026-06-18T00:00:00Z",
        ],
        runtime_config_loader=_loader,
        due_retry_runtime_builder=builder,
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["status"] == "pass"
    assert parsed["due_candidate_count"] == 0
    assert parsed["redis_client_created"] is False
    assert runtime.preview_calls == [(3, datetime(2026, 6, 18, tzinfo=timezone.utc))]


def test_unsupported_live_or_raw_authority_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-send",
        "--allow-network",
        "--telegram-bot-token",
        "--database-url",
        "--redis-url",
        "--runtime-env",
        "--trigger-event-id",
        "--notification-plan-id",
        "--replay-request-id",
    ):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["runtime_config_loaded"] is False
        assert parsed["redis_consume_called"] is False


def test_invalid_now_utc_returns_json_error(capsys) -> None:
    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "due-retry",
            "--limit",
            "1",
            "--now-utc",
            "not-a-date",
        ],
        runtime_config_loader=_loader,
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["command"] == "due-retry"
    assert parsed["error_code"] == "now_utc_invalid"


def test_parser_exposes_only_bounded_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert parser_flags == {
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-database-write",
        "--env-file",
        "--mode",
        "--allow-redis-read",
        "--allow-redis-consume",
        "--allow-redis-ack",
        "--trigger-event-suffix",
        "--notification-plan-id-suffix",
        "--redis-message-suffix",
        "--replay-request-id-suffix",
        "--limit",
        "--now-utc",
    }


def test_tool_source_imports_no_db_redis_external_clients_or_live_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    call_attrs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            call_attrs.add(node.func.attr)

    assert {"sqlalchemy", "redis", "openai", "requests", "httpx", "aiohttp", "telegram", "subprocess"}.isdisjoint(
        imported_roots
    )
    assert "run_forever" not in call_attrs
    assert "print(" not in source
    for forbidden in ("allow-send", "allow-network", "TELEGRAM_BOT_TOKEN", "xgroup_create", "xclaim", "xautoclaim"):
        assert forbidden not in source
