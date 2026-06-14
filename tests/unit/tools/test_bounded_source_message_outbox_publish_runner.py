from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.services.outbox_relay import bounded_source_message_outbox_publish_runner
from src.services.outbox_relay.bounded_source_message_outbox_publish_runner import (
    BoundedSourceMessagePublishRuntimeConfig,
    BoundedSourceMessageRedisPublisherHandle,
    BoundedSourceMessageRepositoryHandle,
)
from src.services.outbox_relay.models import OutboxEventRow
from tools import bounded_source_message_outbox_publish_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_source_message_outbox_publish_runner.py"
REDIS_URL = "redis://sentinel_cli_redis_url"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
RAW_PAYLOAD_VALUE = "sentinel cli source text"
RAW_DEDUPE_KEY = "srcmsg:cli-sentinel-dedupe-key"
REDIS_MESSAGE_ID = "secret-cli-source-redis-message-id"
CLOSE_EXCEPTION_DETAIL = "sentinel cli private repository close detail"


class FakeRepository:
    def __init__(self, row: OutboxEventRow) -> None:
        self.row = row
        self.marked = []
        self.failed = []
        self.job_attempts = []

    async def fetch_target_events(self, *, event_id, source_message_id, limit):
        del event_id, source_message_id
        return [self.row][:limit]

    async def mark_published(self, *, event_id, published_at=None) -> None:
        del published_at
        self.marked.append(event_id)

    async def mark_failed(self, *, event_id, error_text) -> None:
        self.failed.append((event_id, error_text))

    async def insert_job_attempt(self, **kwargs) -> None:
        self.job_attempts.append(kwargs)


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository, *, close_error: BaseException | None = None) -> None:
        self.repository = repository
        self.close_error = close_error
        self.close_commits = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.close_error is not None:
                raise self.close_error

        return BoundedSourceMessageRepositoryHandle(repository=self.repository, close=close)


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

        return BoundedSourceMessageRedisPublisherHandle(publisher=self.publisher, close=close)


def _runtime_config() -> BoundedSourceMessagePublishRuntimeConfig:
    return BoundedSourceMessagePublishRuntimeConfig(database_url=DB_URL, redis_url=REDIS_URL)


def _row() -> OutboxEventRow:
    return OutboxEventRow(
        event_id=uuid4(),
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=uuid4(),
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json={"source_text": RAW_PAYLOAD_VALUE},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_module_and_config_types() -> None:
    assert runner.BoundedSourceMessageOutboxPublishConfig is (
        bounded_source_message_outbox_publish_runner.BoundedSourceMessageOutboxPublishConfig
    )
    assert runner.BoundedSourceMessagePublishRuntimeConfig is (
        bounded_source_message_outbox_publish_runner.BoundedSourceMessagePublishRuntimeConfig
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_source_message_outbox_publish_v1"
    assert parsed["runner_name"] == "bounded_source_message_outbox_publish_runner"
    assert parsed["mode"] == "source_message_outbox_one_shot_publish"
    assert parsed["gates"] == {
        "operator_approved": False,
        "runtime_config_allowed": False,
        "redis_publish_allowed": False,
        "database_write_allowed": False,
        "max_events": 1,
    }
    assert parsed["redis_publish_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["db_write"] is False
    assert parsed["side_effects"]["redis_mutation"] is False
    assert parsed["side_effects"]["telegram_send_called"] is False
    assert parsed["side_effects"]["telegram_read_called"] is False
    assert parsed["side_effects"]["run_forever_called"] is False
    assert parsed["side_effects"]["openai/github/x/web"] is False
    assert parsed["side_effects"]["systemd/docker/alembic"] is False


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
        "--allow-runtime-config",
        "--allow-redis-publish",
        "--allow-database-write",
        "--event-id",
        "--source-message-id",
        "--max-events",
    }


def test_unsupported_authority_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-telegram",
        "--allow-openai",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--allow-notifier",
        "--run-forever",
        "--database-url",
        "--redis-url",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["redis_publish_attempted"] is False
        assert parsed["database_write_attempted"] is False


def test_invalid_uuid_returns_sanitized_json_without_runtime_config(capsys) -> None:
    exit_code = runner.main(["--operator-approved", "--event-id", "not-a-uuid"])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "invalid_event_id"
    assert parsed["redis_publish_attempted"] is False
    assert parsed["database_write_attempted"] is False


def test_valid_cli_fake_run_prints_json_only_and_delegates_to_source(capsys) -> None:
    row = _row()
    repository = FakeRepository(row)
    publisher = FakePublisher()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-publish",
            "--allow-database-write",
            "--event-id",
            str(row.event_id),
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["ok"] is True
    assert parsed["status"] == "published"
    assert parsed["queue_name"] == "q.source.normalize"
    assert parsed["stage_name"] == "normalize"
    assert parsed["events_published_count"] == 1
    assert parsed["job_attempts_inserted_count"] == 1
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
        assert raw not in captured.out


def test_cli_fake_commit_close_failure_returns_sanitized_json_and_empty_stderr(capsys) -> None:
    row = _row()
    repository = FakeRepository(row)
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )
    publisher = FakePublisher()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-publish",
            "--allow-database-write",
            "--event-id",
            str(row.event_id),
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["ok"] is False
    assert parsed["status"] == "failed"
    assert parsed["error_code"] == "repository_commit_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["redis_publish_attempted"] is True
    assert parsed["events_published_count"] == 1
    assert parsed["event_outbox_marked_published"] is True
    assert parsed["job_attempts_inserted_count"] == 1
    assert repository_builder.close_commits == [True]
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
        CLOSE_EXCEPTION_DETAIL,
    ):
        assert raw not in captured.out


def test_run_with_explicit_args_uses_source_message_id_target() -> None:
    row = _row()
    repository = FakeRepository(row)
    publisher = FakePublisher()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-publish",
            "--allow-database-write",
            "--source-message-id",
            str(row.aggregate_id),
            "--max-events",
            "1",
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )

    assert result.exit_code == 0
    assert result.report["target_source_message_id_suffix"] == str(row.aggregate_id)[-8:]
    assert result.report["queue_name"] == "q.source.normalize"
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
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "print(" not in source
    assert "run_bounded_source_message_outbox_publish_sync" in source
    assert "payload_json" not in source
