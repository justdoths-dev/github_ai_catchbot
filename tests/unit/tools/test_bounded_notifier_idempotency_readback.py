from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import uuid4

from src.services.notifier_telegram.idempotency_readback import (
    BoundedNotifierIdempotencyRepositoryHandle,
    BoundedNotifierIdempotencyRuntimeConfig,
)
from src.services.notifier_telegram.models import NotificationIntentJob, NotifierPlanIdempotencySnapshot
from tools import bounded_notifier_idempotency_readback as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_notifier_idempotency_readback.py"
SOURCE_PATH = ROOT / "src/services/notifier_telegram/idempotency_readback.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"


class FakeReadbackRepository:
    def __init__(self, intent: NotificationIntentJob, snapshots: list[NotifierPlanIdempotencySnapshot]) -> None:
        self.intent = intent
        self.snapshots = snapshots
        self.loaded_suffixes: list[str] = []

    async def load_intents_by_event_suffix(self, *, event_suffix: str, limit: int):
        self.loaded_suffixes.append(event_suffix)
        assert limit == 2
        return [self.intent]

    async def load_idempotency_plan_snapshots(self, intent: NotificationIntentJob):
        assert intent == self.intent
        return self.snapshots


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeReadbackRepository) -> None:
        self.repository = repository
        self.closed = 0
        self.calls = 0

    async def __call__(self, runtime_config, state):
        assert runtime_config.database_url == DB_URL
        self.calls += 1
        state.database_session_opened = True

        async def close() -> None:
            self.closed += 1

        return BoundedNotifierIdempotencyRepositoryHandle(repository=self.repository, close=close)


def _runtime_config() -> BoundedNotifierIdempotencyRuntimeConfig:
    return BoundedNotifierIdempotencyRuntimeConfig(database_url=DB_URL)


def _raising_runtime_config() -> BoundedNotifierIdempotencyRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _intent() -> NotificationIntentJob:
    event_id = uuid4()
    analysis_id = uuid4()
    candidate_group_id = uuid4()
    return NotificationIntentJob(
        trigger_event_id=event_id,
        event_type="notification.plan.created.v1",
        notification_plan_id=uuid4(),
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key=str(candidate_group_id),
        material_change_hash="material",
        send_after=None,
        suppress_reason_code=None,
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_notifier_idempotency_readback_v1"
    assert parsed["runner_name"] == "bounded_notifier_idempotency_readback"
    assert parsed["mode"] == "read_only"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["database_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_consume_called"] is False
    assert parsed["telegram_send_called"] is False


def test_gate_order_blocks_before_runtime_config() -> None:
    cases = (
        (["--operator-approved"], "runtime_config_not_allowed"),
        (["--operator-approved", "--allow-runtime-config"], "database_read_not_allowed"),
        (
            ["--operator-approved", "--allow-runtime-config", "--allow-database-read"],
            "suffix_ambiguous_or_missing",
        ),
    )

    for argv, error_code in cases:
        result = runner.run(runner.build_parser().parse_args(argv), runtime_config_loader=_raising_runtime_config)

        assert result.exit_code == 1
        assert result.report["error_code"] == error_code
        assert result.report["database_read_attempted"] is False
        assert result.report["redis_consume_called"] is False


def test_valid_readback_reports_duplicate_sent_state_with_suffixes_only() -> None:
    intent = _intent()
    first_plan_id = uuid4()
    second_plan_id = uuid4()
    repository = FakeReadbackRepository(
        intent,
        [
            NotifierPlanIdempotencySnapshot(
                notification_plan_id=first_plan_id,
                status="sent",
                render_count=1,
                delivery_record_count=1,
                sent_delivery_count=1,
                terminal_delivery_count=1,
                sent_delivery_chat_id_present_count=1,
                sent_delivery_message_id_present_count=1,
            ),
            NotifierPlanIdempotencySnapshot(
                notification_plan_id=second_plan_id,
                status="sent",
                render_count=1,
                delivery_record_count=1,
                sent_delivery_count=1,
                terminal_delivery_count=1,
                sent_delivery_chat_id_present_count=1,
                sent_delivery_message_id_present_count=1,
            ),
        ],
    )
    builder = FakeRepositoryBuilder(repository)

    result = runner.run(
        runner.build_parser().parse_args(
            [
                "--operator-approved",
                "--allow-runtime-config",
                "--allow-database-read",
                "--event-suffix",
                str(intent.trigger_event_id)[-8:],
                "--analysis-suffix",
                str(intent.analysis_id)[-8:],
            ]
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 0
    assert result.report["status"] == "pass"
    assert result.report["primary_classification"] == "existing_duplicate_sent_deliveries"
    assert "existing_duplicate_plans" in result.report["classifications"]
    assert result.report["notification_plan_count"] == 2
    assert result.report["notification_render_count"] == 2
    assert result.report["notification_delivery_record_count"] == 2
    assert result.report["sent_delivery_count"] == 2
    assert result.report["sent_delivery_chat_id_present_count"] == 2
    assert result.report["notification_plan_suffixes"] == [str(first_plan_id)[-8:], str(second_plan_id)[-8:]]
    assert str(intent.trigger_event_id) not in rendered
    assert str(intent.analysis_id) not in rendered
    assert DB_URL not in rendered
    assert builder.calls == 1
    assert builder.closed == 1


def test_unsupported_authority_and_raw_id_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-redis-read",
        "--allow-redis-ack",
        "--allow-telegram",
        "--allow-send",
        "--database-url",
        "--runtime-env",
        "--event-id",
        "--analysis-id",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["database_read_attempted"] is False
        assert parsed["redis_consume_called"] is False
        assert parsed["telegram_send_called"] is False


def test_tool_source_imports_no_db_redis_or_external_clients() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert {"sqlalchemy", "redis", "openai", "requests", "httpx", "aiohttp", "telegram"}.isdisjoint(imported_roots)
    assert parser_flags == {
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--event-suffix",
        "--analysis-suffix",
    }
    assert "print(" not in source
    assert "render_sanitized_json(result.report)" in source


def test_service_source_has_no_forbidden_mutation_or_external_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert {"redis", "openai", "requests", "httpx", "aiohttp", "telegram", "subprocess"}.isdisjoint(imported_roots)
    assert "DELETE " not in source.upper()
    assert "UPDATE notification_plans" not in source
    assert "notification_delivery_records (" not in source
    assert "payload_json" not in json.dumps(runner.main.__name__)

