from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from src.services.outbox_relay import bounded_judge_output_ready_outbox_publish_runner
from src.services.outbox_relay.bounded_judge_output_ready_outbox_publish_runner import (
    BoundedJudgeOutputReadyPublishRuntimeConfig,
    BoundedJudgeOutputReadyRedisPublisherHandle,
    BoundedJudgeOutputReadyRepositoryHandle,
)
from src.services.outbox_relay.models import OutboxEventRow
from tools import bounded_judge_output_ready_outbox_publish_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_judge_output_ready_outbox_publish_runner.py"
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_judge_output_ready_outbox_publish_runner.py"
DB_LOCATOR = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_LOCATOR = "redis://sentinel_cli_redis_url"
RAW_DEDUPE_KEY = "judge-output-ready:cli-sentinel-dedupe-key"
RAW_PAYLOAD_VALUE = "sentinel cli private judge output payload"
RAW_TEXT = "sentinel cli raw source text"
REDIS_MESSAGE_ID = "1700000000000-0-cli-secret-suffix"
CLOSE_EXCEPTION_DETAIL = "sentinel cli private repository close detail"


class FakeRepository:
    def __init__(
        self,
        rows: list[OutboxEventRow],
        *,
        mark_error: BaseException | None = None,
        job_attempt_error: BaseException | None = None,
    ) -> None:
        self.rows = rows
        self.mark_error = mark_error
        self.job_attempt_error = job_attempt_error
        self.fetch_calls = []
        self.marked = []
        self.job_attempts = []

    async def fetch_target_events(self, *, trigger_event_suffix, limit):
        self.fetch_calls.append(
            {
                "trigger_event_suffix": trigger_event_suffix,
                "limit": limit,
            }
        )
        return self.rows[:limit]

    async def mark_published(self, *, event_id, published_at=None) -> None:
        if self.mark_error is not None:
            raise self.mark_error
        self.marked.append((event_id, published_at))

    async def insert_job_attempt(self, **kwargs) -> None:
        if self.job_attempt_error is not None:
            raise self.job_attempt_error
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

        return BoundedJudgeOutputReadyRepositoryHandle(repository=self.repository, close=close)


class FakePublisher:
    def __init__(self, *, publish_error: BaseException | None = None) -> None:
        self.publish_error = publish_error
        self.publish_calls = []

    async def publish(self, route, message) -> str:
        self.publish_calls.append((route, message))
        if self.publish_error is not None:
            raise self.publish_error
        return REDIS_MESSAGE_ID


class FakePublisherBuilder:
    def __init__(self, publisher: FakePublisher) -> None:
        self.publisher = publisher

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.redis_publisher_created = True

        async def close() -> None:
            return None

        return BoundedJudgeOutputReadyRedisPublisherHandle(publisher=self.publisher, close=close)


def _runtime_config() -> BoundedJudgeOutputReadyPublishRuntimeConfig:
    return BoundedJudgeOutputReadyPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _payload(judge_run_id: UUID, judge_output_id: UUID) -> dict[str, object]:
    return {
        "judge_run_id": str(judge_run_id),
        "judge_output_id": str(judge_output_id),
        "bundle_id": str(uuid4()),
        "candidate_group_id": str(uuid4()),
        "payload_json": RAW_PAYLOAD_VALUE,
        "raw_text": RAW_TEXT,
        "database_url": DB_LOCATOR,
        "redis_url": REDIS_LOCATOR,
    }


def _row(*, status: str = "pending", event_type: str = "judge.output.ready.v1") -> tuple[OutboxEventRow, UUID]:
    judge_run_id = uuid4()
    judge_output_id = uuid4()
    row = OutboxEventRow(
        event_id=uuid4(),
        event_type=event_type,
        aggregate_type="judge_run",
        aggregate_id=judge_run_id,
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json=_payload(judge_run_id, judge_output_id),
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    return row, judge_output_id


def _suffix(value: UUID) -> str:
    return str(value).replace("-", "")[-8:]


def _approved_args(row: OutboxEventRow, judge_output_id: UUID) -> list[str]:
    return [
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-database-write",
        "--allow-redis-publish",
        "--trigger-event-suffix",
        _suffix(row.event_id),
        "--judge-run-suffix",
        _suffix(row.aggregate_id),
        "--judge-output-suffix",
        _suffix(judge_output_id),
        "--scan-limit",
        "10",
    ]


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_module_and_config_types() -> None:
    assert runner.BoundedJudgeOutputReadyOutboxPublishConfig is (
        bounded_judge_output_ready_outbox_publish_runner.BoundedJudgeOutputReadyOutboxPublishConfig
    )
    assert runner.BoundedJudgeOutputReadyPublishRuntimeConfig is (
        bounded_judge_output_ready_outbox_publish_runner.BoundedJudgeOutputReadyPublishRuntimeConfig
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_judge_output_ready_outbox_publish_v1"
    assert parsed["runner_name"] == "bounded_judge_output_ready_outbox_publish_runner"
    assert parsed["mode"] == "judge_output_ready_outbox_one_shot_publish"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["database_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_publish_attempted"] is False
    assert parsed["redis_published_count"] == 0
    assert parsed["event_outbox_status_updated_count"] == 0
    assert parsed["job_attempts_written_count"] == 0
    assert parsed["gates"] == {
        "operator_approved": False,
        "runtime_config_allowed": False,
        "database_read_allowed": False,
        "database_write_allowed": False,
        "redis_publish_allowed": False,
        "scan_limit": 10,
    }
    for key in (
        "queue_consume_called",
        "redis_ack_called",
        "redis_claim_called",
        "redis_delete_called",
        "redis_group_create_called",
        "analysis_validator_called",
        "policy_called",
        "notifier_called",
        "telegram_send_called",
        "openai_called",
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
        "--allow-database-write",
        "--allow-redis-publish",
        "--trigger-event-suffix",
        "--judge-run-suffix",
        "--judge-output-suffix",
        "--scan-limit",
    }


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    for flag in (
        "--allow-openai",
        "--allow-telegram",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--allow-policy",
        "--allow-analysis-validator",
        "--run-forever",
        "--consume-q-analysis-validate",
        "--database-url",
        "--redis-url",
        "--trigger-event-id",
        "--judge-run-id",
        "--judge-output-id",
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


def test_invalid_suffixes_and_scan_limit_return_json_without_runtime_config(capsys) -> None:
    bad_trigger_exit = runner.main(["--operator-approved", "--trigger-event-suffix", "not-a-suffix"])
    bad_trigger = json.loads(capsys.readouterr().out)

    small_scan_exit = runner.main(
        [
            "--operator-approved",
            "--trigger-event-suffix",
            "96adff68",
            "--judge-run-suffix",
            "d506c8c2",
            "--judge-output-suffix",
            "7e1c46ae",
            "--scan-limit",
            "1",
        ]
    )
    small_scan = json.loads(capsys.readouterr().out)

    assert bad_trigger_exit == 1
    assert bad_trigger["error_code"] == "invalid_trigger_event_suffix"
    assert bad_trigger["database_read_attempted"] is False
    assert bad_trigger["redis_publish_attempted"] is False
    assert small_scan_exit == 1
    assert small_scan["error_code"] == "scan_limit_too_small"
    assert small_scan["database_read_attempted"] is False


def test_valid_cli_fake_run_prints_json_only_and_delegates_to_source(capsys) -> None:
    row, judge_output_id = _row()
    repository = FakeRepository([row])
    publisher = FakePublisher()

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
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
    assert parsed["queue_name"] == "q.analysis.validate"
    assert parsed["stage_name"] == "analysis_validate"
    assert parsed["target_trigger_event_id_suffix"] == _suffix(row.event_id)
    assert parsed["target_judge_run_id_suffix"] == _suffix(row.aggregate_id)
    assert parsed["target_judge_output_id_suffix"] == _suffix(judge_output_id)
    assert parsed["redis_message_id_suffix"] == REDIS_MESSAGE_ID[-8:]
    assert parsed["redis_published_count"] == 1
    assert parsed["event_outbox_status_updated_count"] == 1
    assert parsed["job_attempts_written_count"] == 1
    assert parsed["thin_stream_fields_valid"] is True
    assert repository.fetch_calls == [{"trigger_event_suffix": _suffix(row.event_id), "limit": 10}]
    assert [event_id for event_id, _published_at in repository.marked] == [row.event_id]
    assert repository.job_attempts == [
        {
            "stage_name": "analysis_validate",
            "queue_name": "q.analysis.validate",
            "root_object_type": "judge_run",
            "root_object_id": row.aggregate_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]
    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.analysis.validate"
    assert route.stage_name == "analysis_validate"
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "analysis_validate",
        "root_object_type": "judge_run",
        "root_object_id": str(row.aggregate_id),
        "idempotency_key": RAW_DEDUPE_KEY,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    for forbidden_field in (
        "payload_json",
        "judge_run_id",
        "judge_output_id",
        "bundle_id",
        "candidate_group_id",
        "raw_text",
        "database_url",
        "redis_url",
    ):
        assert forbidden_field not in fields
    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        str(judge_output_id),
        row.dedupe_key,
        RAW_PAYLOAD_VALUE,
        RAW_TEXT,
        REDIS_MESSAGE_ID,
        DB_LOCATOR,
        REDIS_LOCATOR,
    ):
        assert raw not in captured.out


def test_valid_cli_fake_run_accepts_exact_suffix_selectors(capsys) -> None:
    row, judge_output_id = _row()
    repository = FakeRepository([row])
    publisher = FakePublisher()

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["selector_type"] == "trigger_event_suffix"
    assert parsed["aggregate_judge_run_suffix_matches"] is True
    assert parsed["payload_judge_run_suffix_matches"] is True
    assert parsed["payload_judge_output_suffix_matches"] is True
    assert repository.fetch_calls[0]["trigger_event_suffix"] == _suffix(row.event_id)
    assert len(publisher.publish_calls) == 1


def test_already_published_event_returns_noop_without_redis_or_db_write(capsys) -> None:
    row, judge_output_id = _row(status="published")
    repository = FakeRepository([row])
    publisher = FakePublisher()

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["status"] == "noop"
    assert parsed["duplicate_handling_status"] == "already_published_noop"
    assert parsed["redis_publish_attempted"] is False
    assert parsed["redis_published_count"] == 0
    assert parsed["database_write_attempted"] is False
    assert repository.marked == []
    assert repository.job_attempts == []
    assert publisher.publish_calls == []


def test_non_unique_target_fails_closed_before_publish(capsys) -> None:
    row, judge_output_id = _row()
    second, _ = _row()
    repository = FakeRepository([row, second])
    publisher = FakePublisher()

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "target_event_not_unique"
    assert parsed["events_seen"] == 2
    assert parsed["redis_publish_attempted"] is False
    assert repository.marked == []
    assert publisher.publish_calls == []


def test_mismatched_judge_output_suffix_fails_closed_before_publish(capsys) -> None:
    row, _judge_output_id = _row()
    repository = FakeRepository([row])
    publisher = FakePublisher()
    argv = _approved_args(row, uuid4())

    exit_code = runner.main(
        argv,
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "payload_judge_output_suffix_mismatch"
    assert parsed["redis_publish_attempted"] is False
    assert repository.marked == []
    assert publisher.publish_calls == []


def test_disallowed_status_fails_closed_before_publish(capsys) -> None:
    row, judge_output_id = _row(status="failed")
    repository = FakeRepository([row])
    publisher = FakePublisher()

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "target_event_status_not_allowed"
    assert parsed["redis_publish_attempted"] is False
    assert repository.marked == []
    assert publisher.publish_calls == []


def test_redis_publish_failure_does_not_mark_event_published(capsys) -> None:
    row, judge_output_id = _row()
    repository = FakeRepository([row])
    publisher = FakePublisher(publish_error=RuntimeError("private redis failure detail"))

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert parsed["status"] == "failed"
    assert parsed["error_code"] == "redis_xadd_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["redis_publish_attempted"] is True
    assert parsed["redis_published_count"] == 0
    assert parsed["database_write_attempted"] is False
    assert repository.marked == []
    assert repository.job_attempts == []
    assert "private redis failure detail" not in captured.out


def test_database_write_failure_after_redis_publish_reports_sanitized_suffixes(capsys) -> None:
    row, judge_output_id = _row()
    repository = FakeRepository([row], mark_error=RuntimeError("private db write failure detail"))
    publisher = FakePublisher()

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert parsed["status"] == "failed"
    assert parsed["error_code"] == "database_write_failed_after_redis_publish"
    assert parsed["redis_published_count"] == 1
    assert parsed["event_outbox_status_updated_count"] == 0
    assert parsed["target_trigger_event_id_suffix"] == _suffix(row.event_id)
    assert parsed["target_judge_run_id_suffix"] == _suffix(row.aggregate_id)
    assert parsed["target_judge_output_id_suffix"] == _suffix(judge_output_id)
    assert "private db write failure detail" not in captured.out
    assert str(row.event_id) not in captured.out
    assert str(row.aggregate_id) not in captured.out
    assert str(judge_output_id) not in captured.out


def test_cli_fake_commit_close_failure_returns_sanitized_json_and_empty_stderr(capsys) -> None:
    row, judge_output_id = _row()
    repository = FakeRepository([row])
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )

    exit_code = runner.main(
        _approved_args(row, judge_output_id),
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
    assert parsed["redis_published_count"] == 1
    assert repository_builder.close_commits == [True]
    assert CLOSE_EXCEPTION_DETAIL not in captured.out
    assert DB_LOCATOR not in captured.out
    assert REDIS_LOCATOR not in captured.out


def test_run_with_explicit_args_uses_trigger_event_suffix_selector() -> None:
    row, judge_output_id = _row()
    repository = FakeRepository([row])
    publisher = FakePublisher()

    result = runner.run(
        _parse_args(*_approved_args(row, judge_output_id)),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )

    assert result.exit_code == 0
    assert result.report["selector_type"] == "trigger_event_suffix"
    assert result.report["target_trigger_event_id_suffix"] == _suffix(row.event_id)
    assert repository.fetch_calls[0]["trigger_event_suffix"] == _suffix(row.event_id)


def test_tool_and_source_ast_guard_has_no_consumer_process_network_or_external_calls() -> None:
    for path in (TOOL_PATH, SOURCE_PATH):
        source = path.read_text(encoding="utf-8")
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
        forbidden_call_attrs = forbidden_call_names | {
            "sleep",
            "xack",
            "xreadgroup",
            "xread",
            "xclaim",
            "xautoclaim",
            "xgroup",
            "xdel",
            "delete",
            "consume",
        }

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
