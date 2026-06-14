from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import uuid4

from src.services.router_normalizer.bounded_source_normalize_runner import (
    BoundedSourceNormalizeCounters,
    BoundedSourceNormalizeDatabaseHandle,
    BoundedSourceNormalizeRedisHandle,
    BoundedSourceNormalizeRuntimeConfig,
)
from src.services.router_normalizer.config import RouterNormalizerConfig
from src.services.router_normalizer.models import NormalizationResult, RedisNormalizeMessage
from tools import bounded_router_normalizer_source_job_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_router_normalizer_source_job_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_TEXT = "sentinel cli raw message text"
RAW_EXCEPTION_DETAIL = "sentinel private close or ack failure detail"
STREAM_ID = "1710000000000-0"


class FakeConsumer:
    def __init__(self, message: RedisNormalizeMessage, *, ack_error: BaseException | None = None) -> None:
        self.message = message
        self.ack_error = ack_error
        self.acked: list[str] = []
        self.find_calls = 0

    async def find_target(self, config, state):
        self.find_calls += 1
        state.redis_consume_attempted = True
        state.redis_group_created = True
        from src.services.router_normalizer.bounded_source_normalize_runner import TargetedRedisMessage

        return (
            TargetedRedisMessage(
                redis_message_id=STREAM_ID,
                fields={
                    "job_id": self.message.job_id,
                    "stage_name": self.message.stage_name,
                    "root_object_type": self.message.root_object_type,
                    "root_object_id": self.message.root_object_id,
                    "idempotency_key": self.message.idempotency_key,
                    "trigger_event_id": self.message.trigger_event_id,
                },
                message=self.message,
            ),
            1,
            1,
        )

    async def ack(self, message_id: str, state) -> int:
        state.redis_ack_attempted = True
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.append(message_id)
        return 1


class FakeRedisBuilder:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1

        async def close() -> None:
            state.redis_cleanup_attempted = True

        return BoundedSourceNormalizeRedisHandle(consumer=self.consumer, close=close)


class FakeService:
    def __init__(self, counters: BoundedSourceNormalizeCounters) -> None:
        self.counters = counters
        self.calls: list[RedisNormalizeMessage] = []

    async def process_stream_message(self, message: RedisNormalizeMessage) -> NormalizationResult:
        self.calls.append(message)
        self.counters.normalization_runs_written_count = 1
        self.counters.suppression_traces_written_count = 1
        return NormalizationResult(
            normalization_run_id=uuid4(),
            signal_detected=True,
            candidate_eligible=False,
            trigger_strength="weak",
            artifact_count=0,
            candidate_group_count=0,
            suppression_reason_codes=["ai_without_dev_context"],
        )


class FakeDatabaseBuilder:
    def __init__(self, *, close_error: BaseException | None = None) -> None:
        self.close_error = close_error
        self.counters = BoundedSourceNormalizeCounters()
        self.service = FakeService(self.counters)
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.close_error is not None:
                raise self.close_error

        return BoundedSourceNormalizeDatabaseHandle(
            service=self.service,
            counters=self.counters,
            close=close,
        )


def _runtime_config() -> BoundedSourceNormalizeRuntimeConfig:
    return BoundedSourceNormalizeRuntimeConfig(
        router_config=RouterNormalizerConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.source.normalize",
            consumer_group="router-normalizer",
            consumer_name="bounded-cli-test",
            block_ms=100,
            batch_size=1,
            normalizer_version="bounded-cli-test-normalizer",
            short_url_allowlist=(),
            short_url_hop_limit=1,
            short_url_timeout_seconds=0.1,
            log_level="INFO",
        )
    )


def _message(event_id, source_message_id) -> RedisNormalizeMessage:
    return RedisNormalizeMessage(
        job_id=str(event_id),
        stage_name="normalize",
        root_object_type="source_message",
        root_object_id=str(source_message_id),
        idempotency_key="private-cli-idempotency-key",
        trigger_event_id=str(event_id),
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_source_normalize_runner_v1"
    assert parsed["runner_name"] == "bounded_router_normalizer_source_job_runner"
    assert parsed["mode"] == "source_normalize_one_shot"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_consume_attempted"] is False
    assert parsed["redis_ack_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["side_effects"]["worker_started"] is False
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
        "--allow-runtime-config",
        "--allow-redis-consume",
        "--allow-database-write",
        "--allow-redis-ack",
        "--trigger-event-id",
        "--source-message-id",
        "--redis-message-id",
        "--max-messages",
        "--scan-limit",
    }


def test_invalid_uuid_returns_sanitized_json_without_runtime_config(capsys) -> None:
    exit_code = runner.main(["--operator-approved", "--trigger-event-id", "not-a-uuid"])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "invalid_trigger_event_id"
    assert parsed["redis_consume_attempted"] is False
    assert parsed["database_write_attempted"] is False


def test_valid_cli_fake_run_prints_json_only_and_delegates_to_source(capsys) -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    consumer = FakeConsumer(_message(event_id, source_message_id))
    database_builder = FakeDatabaseBuilder()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--trigger-event-id",
            str(event_id),
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=database_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["ok"] is True
    assert parsed["status"] == "normalized"
    assert parsed["queue_name"] == "q.source.normalize"
    assert parsed["stage_name"] == "normalize"
    assert parsed["messages_seen"] == 1
    assert parsed["messages_matched"] == 1
    assert parsed["messages_processed_count"] == 1
    assert parsed["redis_acked_count"] == 1
    assert parsed["normalization_runs_written_count"] == 1
    assert parsed["suppression_traces_written_count"] == 1
    assert len(database_builder.service.calls) == 1
    assert database_builder.close_commits == [True]
    assert consumer.acked == [STREAM_ID]
    for raw in (str(event_id), str(source_message_id), STREAM_ID, DB_URL, REDIS_URL, RAW_TEXT):
        assert raw not in captured.out


def test_cli_db_close_failure_prints_json_only_and_does_not_ack(capsys) -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    consumer = FakeConsumer(_message(event_id, source_message_id))
    database_builder = FakeDatabaseBuilder(close_error=RuntimeError(RAW_EXCEPTION_DETAIL))

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--trigger-event-id",
            str(event_id),
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=database_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["ok"] is False
    assert parsed["error_code"] == "database_write_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["redis_ack_attempted"] is False
    assert consumer.acked == []
    assert RAW_EXCEPTION_DETAIL not in captured.out
    assert DB_URL not in captured.out
    assert REDIS_URL not in captured.out


def test_cli_ack_failure_prints_json_only_and_omits_raw_exception(capsys) -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    consumer = FakeConsumer(
        _message(event_id, source_message_id),
        ack_error=RuntimeError(RAW_EXCEPTION_DETAIL),
    )

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--trigger-event-id",
            str(event_id),
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["ok"] is False
    assert parsed["error_code"] == "redis_ack_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["redis_ack_attempted"] is True
    assert parsed["redis_acked_count"] == 0
    assert consumer.acked == []
    assert RAW_EXCEPTION_DETAIL not in captured.out
    assert STREAM_ID not in captured.out


def test_tool_source_imports_no_db_redis_or_forbidden_authority_and_has_no_business_logic() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    imported_modules: set[str] = set()
    call_attrs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            call_attrs.add(node.func.attr)

    assert {"sqlalchemy", "redis", "openai", "requests", "httpx", "aiohttp", "telegram", "subprocess"}.isdisjoint(
        imported_roots
    )
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "process_stream_message" not in call_attrs
