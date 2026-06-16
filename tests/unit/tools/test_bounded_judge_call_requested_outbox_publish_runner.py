from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.services.outbox_relay import bounded_judge_call_requested_outbox_publish_runner
from src.services.outbox_relay.bounded_judge_call_requested_outbox_publish_runner import (
    BoundedJudgeCallRequestedPublishRuntimeConfig,
    BoundedJudgeCallRequestedRedisPublisherHandle,
    BoundedJudgeCallRequestedRepositoryHandle,
    JudgeRunLocatorRecord,
)
from src.services.outbox_relay.models import OutboxEventRow
from tools import bounded_judge_call_requested_outbox_publish_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_judge_call_requested_outbox_publish_runner.py"
DB_LOCATOR = "db_locator_omitted_sentinel"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
RAW_DEDUPE_KEY = "judge-call:cli-sentinel-dedupe-key"
RAW_PROMPT_CACHE_KEY = "judge:text_idea_primary:cli-private-cache-key"
RAW_PROMPT = "sentinel cli private prompt material"
RAW_BUNDLE_DATA = "sentinel cli private bundle data"
RAW_TEXT = "sentinel cli raw source text"
RAW_MODEL_OUTPUT = "sentinel cli model output"
REDIS_MESSAGE_ID = "1700000000000-0-cli-secret-suffix"
CLOSE_EXCEPTION_DETAIL = "sentinel cli private repository close detail"


class FakeRepository:
    def __init__(self, row: OutboxEventRow, judge_run: JudgeRunLocatorRecord) -> None:
        self.row = row
        self.judge_run = judge_run
        self.fetch_calls = []
        self.load_judge_run_calls = []
        self.marked = []
        self.job_attempts = []

    async def fetch_target_events(self, *, trigger_event_id, trigger_event_suffix, limit):
        self.fetch_calls.append(
            {
                "trigger_event_id": trigger_event_id,
                "trigger_event_suffix": trigger_event_suffix,
                "limit": limit,
            }
        )
        return [self.row][:limit]

    async def load_judge_run(self, judge_run_id):
        self.load_judge_run_calls.append(judge_run_id)
        return self.judge_run

    async def mark_published(self, *, event_id, published_at=None) -> None:
        del published_at
        self.marked.append(event_id)

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

        return BoundedJudgeCallRequestedRepositoryHandle(repository=self.repository, close=close)


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

        return BoundedJudgeCallRequestedRedisPublisherHandle(publisher=self.publisher, close=close)


def _runtime_config() -> BoundedJudgeCallRequestedPublishRuntimeConfig:
    return BoundedJudgeCallRequestedPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _payload(judge_run_id, bundle_id) -> dict[str, object]:
    return {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_text_idea_primary_v1",
        "prompt_cache_key": RAW_PROMPT_CACHE_KEY,
        "prompt_material": RAW_PROMPT,
        "bundle_data": RAW_BUNDLE_DATA,
        "raw_text": RAW_TEXT,
        "model_output": RAW_MODEL_OUTPUT,
        "database_url": DB_LOCATOR,
        "redis_url": REDIS_LOCATOR,
    }


def _row() -> tuple[OutboxEventRow, JudgeRunLocatorRecord]:
    judge_run_id = uuid4()
    bundle_id = uuid4()
    row = OutboxEventRow(
        event_id=uuid4(),
        event_type="judge.call.requested.v1",
        aggregate_type="judge_run",
        aggregate_id=judge_run_id,
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json=_payload(judge_run_id, bundle_id),
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    return row, JudgeRunLocatorRecord(judge_run_id=judge_run_id, bundle_id=bundle_id, status="pending")


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_module_and_config_types() -> None:
    assert runner.BoundedJudgeCallRequestedOutboxPublishConfig is (
        bounded_judge_call_requested_outbox_publish_runner.BoundedJudgeCallRequestedOutboxPublishConfig
    )
    assert runner.BoundedJudgeCallRequestedPublishRuntimeConfig is (
        bounded_judge_call_requested_outbox_publish_runner.BoundedJudgeCallRequestedPublishRuntimeConfig
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_judge_call_requested_outbox_publish_v1"
    assert parsed["runner_name"] == "bounded_judge_call_requested_outbox_publish_runner"
    assert parsed["mode"] == "judge_call_requested_outbox_one_shot_publish"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["database_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_publish_attempted"] is False
    assert parsed["published_count"] == 0
    assert parsed["event_outbox_status_updated"] is False
    assert parsed["gates"] == {
        "operator_approved": False,
        "runtime_config_allowed": False,
        "database_read_allowed": False,
        "redis_publish_allowed": False,
        "database_write_allowed": False,
        "max_events": 1,
    }
    for key in (
        "openai_called",
        "judge_openai_called",
        "analysis_validator_called",
        "policy_called",
        "notifier_called",
        "telegram_send_called",
        "github_api_called",
        "x_api_called",
        "web_fetch_called",
        "worker_started",
        "run_forever_called",
        "systemd_called",
        "docker_called",
        "alembic_called",
        "subprocess_called",
    ):
        assert parsed["side_effects"][key] is False


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
        "--allow-database-read",
        "--allow-redis-publish",
        "--allow-database-write",
        "--trigger-event-id",
        "--trigger-event-suffix",
    }


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    for flag in (
        "--allow-openai",
        "--allow-telegram",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--allow-policy",
        "--allow-judge-openai",
        "--run-forever",
        "--consume-q-analysis-judge",
        "--database-url",
        "--redis-url",
        "--judge-run-id",
        "--bundle-id",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["database_read_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["redis_publish_attempted"] is False


def test_invalid_uuid_or_suffix_returns_sanitized_json_without_runtime_config(capsys) -> None:
    invalid_event_id_exit = runner.main(["--operator-approved", "--trigger-event-id", "not-a-uuid"])
    invalid_event_id = json.loads(capsys.readouterr().out)

    invalid_suffix_exit = runner.main(["--operator-approved", "--trigger-event-suffix", "not-a-suffix"])
    invalid_suffix = json.loads(capsys.readouterr().out)

    assert invalid_event_id_exit == 1
    assert invalid_event_id["error_code"] == "invalid_trigger_event_id"
    assert invalid_event_id["database_read_attempted"] is False
    assert invalid_event_id["redis_publish_attempted"] is False
    assert invalid_suffix_exit == 1
    assert invalid_suffix["error_code"] == "invalid_trigger_event_suffix"


def test_valid_cli_fake_run_prints_json_only_and_delegates_to_source(capsys) -> None:
    row, judge_run = _row()
    repository = FakeRepository(row, judge_run)
    publisher = FakePublisher()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-redis-publish",
            "--allow-database-write",
            "--trigger-event-id",
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
    assert parsed["queue_name"] == "q.analysis.judge"
    assert parsed["stage_name"] == "judge"
    assert parsed["target_trigger_event_id_suffix"] == str(row.event_id)[-8:]
    assert parsed["target_judge_run_id_suffix"] == str(row.aggregate_id)[-8:]
    assert parsed["target_bundle_id_suffix"] == str(judge_run.bundle_id)[-8:]
    assert parsed["redis_message_id_suffix"] == REDIS_MESSAGE_ID[-8:]
    assert parsed["published_count"] == 1
    assert parsed["event_outbox_status_updated"] is True
    assert repository.marked == [row.event_id]
    assert len(repository.job_attempts) == 1
    assert len(publisher.publish_calls) == 1
    assert captured.out.strip().startswith("{")
    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        str(judge_run.bundle_id),
        row.dedupe_key,
        RAW_PROMPT_CACHE_KEY,
        RAW_PROMPT,
        RAW_BUNDLE_DATA,
        RAW_TEXT,
        RAW_MODEL_OUTPUT,
        REDIS_MESSAGE_ID,
        DB_LOCATOR,
        REDIS_LOCATOR,
    ):
        assert raw not in captured.out


def test_valid_cli_fake_run_accepts_trigger_event_suffix_selector(capsys) -> None:
    row, judge_run = _row()
    repository = FakeRepository(row, judge_run)
    publisher = FakePublisher()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-redis-publish",
            "--allow-database-write",
            "--trigger-event-suffix",
            str(row.event_id)[-8:],
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["selector_type"] == "trigger_event_suffix"
    assert parsed["target_trigger_event_id_suffix"] == str(row.event_id)[-8:]
    assert repository.fetch_calls[0]["trigger_event_suffix"] == str(row.event_id)[-8:]
    assert len(publisher.publish_calls) == 1


def test_cli_fake_commit_close_failure_returns_sanitized_json_and_empty_stderr(capsys) -> None:
    row, judge_run = _row()
    repository = FakeRepository(row, judge_run)
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-redis-publish",
            "--allow-database-write",
            "--trigger-event-id",
            str(row.event_id),
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakePublisherBuilder(FakePublisher()),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["status"] == "failed"
    assert parsed["error_code"] == "database_commit_failed_after_redis_publish"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["published_count"] == 1
    assert CLOSE_EXCEPTION_DETAIL not in captured.out
    assert DB_LOCATOR not in captured.out
    assert REDIS_LOCATOR not in captured.out


def test_run_with_explicit_args_uses_trigger_event_suffix_selector() -> None:
    row, judge_run = _row()
    repository = FakeRepository(row, judge_run)
    publisher = FakePublisher()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-redis-publish",
            "--allow-database-write",
            "--trigger-event-suffix",
            str(row.event_id)[-8:],
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )

    assert result.exit_code == 0
    assert result.report["selector_type"] == "trigger_event_suffix"
    assert result.report["target_trigger_event_id_suffix"] == str(row.event_id)[-8:]
    assert repository.fetch_calls[0]["trigger_event_suffix"] == str(row.event_id)[-8:]


def test_tool_ast_guard_has_no_forbidden_process_network_or_consumer_calls() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = set()
    imported_roots = set()
    forbidden_call_names = {
        "system",
        "popen",
        "call",
        "check_call",
        "check_output",
        "run_forever",
    }
    forbidden_call_attrs = forbidden_call_names | {"sleep", "xreadgroup", "xread", "consume"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_call_attrs
            elif isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_call_names

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(
        imported_roots
    )
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".analysis_validator" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".router_normalizer" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever(" not in source
