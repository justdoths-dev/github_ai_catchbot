from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.services.outbox_relay import bounded_notification_plan_publish
from src.services.outbox_relay.bounded_notification_plan_publish import (
    BoundedNotificationPlanPublishRuntimeConfig,
    BoundedNotificationPlanRedisPublisherHandle,
    BoundedNotificationPlanRepositoryHandle,
)
from src.services.outbox_relay.models import OutboxEventRow
from tools import bounded_notification_plan_outbox_publish_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_notification_plan_outbox_publish_runner.py"
REDIS_URL = "redis://sentinel_cli_redis_url"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
RAW_PAYLOAD_VALUE = "sentinel cli raw payload"
REDIS_MESSAGE_ID = "secret-cli-redis-message-id"


class FakeRepository:
    def __init__(self, row: OutboxEventRow) -> None:
        self.row = row
        self.marked = []
        self.job_attempts = []

    async def count_pending_events(self, *, event_type: str) -> int:
        assert event_type == "notification.plan.created.v1"
        return 1

    async def fetch_oldest_pending_event(self, *, event_type: str):
        assert event_type == "notification.plan.created.v1"
        return self.row

    async def mark_published(self, *, event_id, published_at=None) -> None:
        del published_at
        self.marked.append(event_id)

    async def insert_job_attempt(self, **kwargs) -> None:
        self.job_attempts.append(kwargs)


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            del commit

        return BoundedNotificationPlanRepositoryHandle(repository=self.repository, close=close)


class FakePublisher:
    def __init__(self) -> None:
        self.publish_calls = []

    async def publish(self, route, message) -> str:
        self.publish_calls.append((route, message))
        return REDIS_MESSAGE_ID


class FakePublisherBuilder:
    def __init__(self, publisher: FakePublisher) -> None:
        self.publisher = publisher

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.redis_publisher_created = True

        async def close() -> None:
            return None

        return BoundedNotificationPlanRedisPublisherHandle(publisher=self.publisher, close=close)


def _runtime_config() -> BoundedNotificationPlanPublishRuntimeConfig:
    return BoundedNotificationPlanPublishRuntimeConfig(database_url=DB_URL, redis_url=REDIS_URL)


def _row() -> OutboxEventRow:
    return OutboxEventRow(
        event_id=uuid4(),
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=uuid4(),
        dedupe_key="notify:cli-sentinel-dedupe-key",
        payload_json={
            "notification_plan_id": str(uuid4()),
            "analysis_id": str(uuid4()),
            "candidate_group_id": str(uuid4()),
            "target_chat_id": -100123,
            "material_change_hash": RAW_PAYLOAD_VALUE,
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_module_and_config_types() -> None:
    assert runner.BoundedNotificationPlanOutboxPublishConfig is (
        bounded_notification_plan_publish.BoundedNotificationPlanOutboxPublishConfig
    )
    assert runner.BoundedNotificationPlanPublishRuntimeConfig is (
        bounded_notification_plan_publish.BoundedNotificationPlanPublishRuntimeConfig
    )


def test_main_with_no_flags_returns_required_fail_closed_json(capsys) -> None:
    exit_code = runner.main([])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["schema_version"] == "bounded_notification_plan_outbox_publish_v1"
    assert parsed["runner_name"] == "bounded_notification_plan_outbox_publish_runner"
    assert parsed["mode"] == "notification_plan_outbox_one_shot_publish"
    assert parsed["operator_approved"] is False
    assert parsed["database_read_allowed"] is False
    assert parsed["redis_write_allowed"] is False
    assert parsed["outbox_status_update_allowed"] is False
    assert parsed["database_read_attempted"] is False
    assert parsed["redis_write_attempted"] is False
    assert parsed["redis_xadd_attempted"] is False
    assert parsed["event_outbox_status_update_attempted"] is False
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["db_write"] is False
    assert parsed["side_effects"]["redis_mutation"] is False
    assert parsed["side_effects"]["telegram_send_called"] is False
    assert parsed["side_effects"]["run_forever_called"] is False


def test_parser_exposes_only_approved_bounded_flags() -> None:
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
        "--allow-database-read",
        "--allow-redis-write",
        "--allow-outbox-status-update",
        "--expected-pending-count",
    }


def test_unsupported_live_send_network_and_event_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--event-id",
        "--trigger-event-id",
        "--allow-send",
        "--allow-network",
        "--telegram-bot-token",
        "--database-url",
        "--redis-url",
    ):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["database_read_attempted"] is False
        assert parsed["redis_xadd_attempted"] is False
        assert parsed["event_outbox_status_update_attempted"] is False


def test_valid_cli_run_prints_sanitized_json_and_delegates_to_source(capsys) -> None:
    row = _row()
    repository = FakeRepository(row)
    publisher = FakePublisher()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-database-read",
            "--allow-redis-write",
            "--allow-outbox-status-update",
            "--expected-pending-count",
            "1",
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["redis_xadd_count"] == 1
    assert parsed["event_outbox_marked_published"] is True
    assert parsed["job_attempt_inserted"] is True
    assert repository.marked == [row.event_id]
    assert len(repository.job_attempts) == 1
    assert len(publisher.publish_calls) == 1
    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        row.dedupe_key,
        RAW_PAYLOAD_VALUE,
        REDIS_MESSAGE_ID,
        DB_URL,
        REDIS_URL,
    ):
        assert raw not in output


def test_run_with_explicit_args_uses_expected_pending_count() -> None:
    row = _row()
    repository = FakeRepository(row)
    publisher = FakePublisher()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-database-read",
            "--allow-redis-write",
            "--allow-outbox-status-update",
            "--expected-pending-count",
            "1",
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )

    assert result.exit_code == 0
    assert result.report["pending_count_observed"] == 1
    assert result.report["queue_name"] == "q.notification.send"
    assert len(publisher.publish_calls) == 1


def test_tool_source_imports_no_db_redis_or_external_clients_and_has_no_business_logic() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    call_attrs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            call_attrs.add(node.func.attr)

    assert {"sqlalchemy", "redis", "openai", "requests", "httpx", "aiohttp", "telegram"}.isdisjoint(
        imported_roots
    )
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "print(" not in source
    assert "run_bounded_notification_plan_outbox_publish_sync" in source
    assert "payload_json" not in source
