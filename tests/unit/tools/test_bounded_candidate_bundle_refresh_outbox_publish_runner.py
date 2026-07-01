from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.services.outbox_relay import bounded_candidate_bundle_refresh_outbox_publish_runner
from src.services.outbox_relay.bounded_candidate_bundle_refresh_outbox_publish_runner import (
    BoundedCandidateBundleRefreshPublishRuntimeConfig,
    BoundedCandidateBundleRefreshRedisPublisherHandle,
    BoundedCandidateBundleRefreshRepositoryHandle,
)
from src.services.outbox_relay.models import OutboxEventRow
from tools import bounded_candidate_bundle_refresh_outbox_publish_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_candidate_bundle_refresh_outbox_publish_runner.py"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
DB_LOCATOR = "db_locator_omitted_sentinel"
RAW_DEDUPE_KEY = "bundle-refresh:cli-private-dedupe-key"
RAW_REFRESH_REASON = "sentinel cli private refresh reason"
RAW_SOURCE_TEXT = "sentinel cli private source text"
RAW_URL = "https://example.invalid/cli-private-refresh-url"
REDIS_MESSAGE_ID = "secret-cli-candidate-refresh-redis-message-id"
CLOSE_EXCEPTION_DETAIL = "sentinel cli private repository close detail"


class FakeRepository:
    def __init__(self, row: OutboxEventRow) -> None:
        self.row = row
        self.marked = []
        self.job_attempts = []
        self.fetch_calls = []

    async def fetch_target_events(self, *, event_id, candidate_group_id, event_suffix, limit):
        self.fetch_calls.append(
            {
                "event_id": event_id,
                "candidate_group_id": candidate_group_id,
                "event_suffix": event_suffix,
                "limit": limit,
            }
        )
        return [self.row][:limit]

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

        return BoundedCandidateBundleRefreshRepositoryHandle(repository=self.repository, close=close)


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

        return BoundedCandidateBundleRefreshRedisPublisherHandle(publisher=self.publisher, close=close)


def _runtime_config() -> BoundedCandidateBundleRefreshPublishRuntimeConfig:
    return BoundedCandidateBundleRefreshPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _payload(candidate_group_id) -> dict[str, object]:
    bundle_id = uuid4()
    return {
        "candidate_group_id": str(candidate_group_id),
        "trigger_kind": "analysis_router_recheck",
        "trigger_object_type": "bundle",
        "trigger_object_id": str(bundle_id),
        "refresh_reason": RAW_REFRESH_REASON,
        "source_version_no": 7,
        "bundle_id": str(bundle_id),
        "source_text": RAW_SOURCE_TEXT,
        "url": RAW_URL,
    }


def _row() -> OutboxEventRow:
    candidate_group_id = uuid4()
    return OutboxEventRow(
        event_id=uuid4(),
        event_type="candidate.bundle.refresh.v1",
        aggregate_type="candidate_group",
        aggregate_id=candidate_group_id,
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json=_payload(candidate_group_id),
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def test_runner_uses_source_level_module_and_config_types() -> None:
    assert runner.BoundedCandidateBundleRefreshOutboxPublishConfig is (
        bounded_candidate_bundle_refresh_outbox_publish_runner.BoundedCandidateBundleRefreshOutboxPublishConfig
    )
    assert runner.BoundedCandidateBundleRefreshPublishRuntimeConfig is (
        bounded_candidate_bundle_refresh_outbox_publish_runner.BoundedCandidateBundleRefreshPublishRuntimeConfig
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_candidate_bundle_refresh_outbox_publish_v1"
    assert parsed["runner_name"] == "bounded_candidate_bundle_refresh_outbox_publish_runner"
    assert parsed["mode"] == "candidate_bundle_refresh_outbox_one_shot_publish"
    assert parsed["gates"] == {
        "operator_approved": False,
        "runtime_config_allowed": False,
        "redis_publish_allowed": False,
        "database_write_allowed": False,
        "max_events": 1,
    }
    assert parsed["status"] == "blocked"
    assert parsed["ok"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_publish_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["side_effects"]["db_write"] is False
    assert parsed["side_effects"]["redis_mutation"] is False
    assert parsed["side_effects"]["queue_consume_called"] is False
    assert parsed["side_effects"]["evidence_assembler_called"] is False
    assert parsed["side_effects"]["judge_called"] is False
    assert parsed["side_effects"]["policy_called"] is False
    assert parsed["side_effects"]["notifier_called"] is False
    assert parsed["side_effects"]["openai_called"] is False
    assert parsed["side_effects"]["telegram_send_called"] is False


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
        "--candidate-group-id",
        "--event-suffix",
    }


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    for flag in (
        "--allow-telegram",
        "--allow-openai",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--allow-notifier",
        "--allow-policy",
        "--allow-evidence-assembler",
        "--run-forever",
        "--consume",
        "--database-url",
        "--redis-url",
        "--max-events",
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


def test_invalid_uuid_or_suffix_returns_sanitized_json_without_runtime_config(capsys) -> None:
    invalid_event_id_exit = runner.main(["--operator-approved", "--event-id", "not-a-uuid"])
    invalid_event_id = json.loads(capsys.readouterr().out)

    invalid_candidate_group_id_exit = runner.main(["--operator-approved", "--candidate-group-id", "not-a-uuid"])
    invalid_candidate_group_id = json.loads(capsys.readouterr().out)

    invalid_suffix_exit = runner.main(["--operator-approved", "--event-suffix", "not-a-suffix"])
    invalid_suffix = json.loads(capsys.readouterr().out)

    assert invalid_event_id_exit == 1
    assert invalid_event_id["error_code"] == "invalid_event_id"
    assert invalid_event_id["redis_publish_attempted"] is False
    assert invalid_event_id["database_write_attempted"] is False
    assert invalid_candidate_group_id_exit == 1
    assert invalid_candidate_group_id["error_code"] == "invalid_candidate_group_id"
    assert invalid_candidate_group_id["redis_publish_attempted"] is False
    assert invalid_candidate_group_id["database_write_attempted"] is False
    assert invalid_suffix_exit == 1
    assert invalid_suffix["error_code"] == "invalid_event_suffix"


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
    assert parsed["queue_name"] == "q.candidate.bundle"
    assert parsed["stage_name"] == "bundle"
    assert parsed["events_published_count"] == 1
    assert parsed["job_attempts_inserted_count"] == 1
    assert parsed["payload_has_candidate_group_id"] is True
    assert parsed["payload_has_trigger_kind"] is True
    assert parsed["payload_has_trigger_object_type"] is True
    assert parsed["payload_has_trigger_object_id"] is True
    assert parsed["payload_has_refresh_reason"] is True
    assert repository.marked == [row.event_id]
    assert len(repository.job_attempts) == 1
    assert len(publisher.publish_calls) == 1
    assert captured.out.strip().startswith("{")
    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        str(row.payload_json["trigger_object_id"]),
        str(row.payload_json["bundle_id"]),
        row.dedupe_key,
        RAW_REFRESH_REASON,
        RAW_SOURCE_TEXT,
        RAW_URL,
        REDIS_MESSAGE_ID,
        DB_LOCATOR,
        REDIS_LOCATOR,
    ):
        assert raw not in captured.out


def test_valid_cli_fake_run_accepts_event_suffix_selector(capsys) -> None:
    row = _row()
    repository = FakeRepository(row)
    publisher = FakePublisher()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-publish",
            "--allow-database-write",
            "--event-suffix",
            str(row.event_id)[-8:],
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["selector_type"] == "event_suffix"
    assert parsed["target_event_id_suffix"] == str(row.event_id)[-8:]
    assert repository.fetch_calls[0]["event_suffix"] == str(row.event_id)[-8:]
    assert len(publisher.publish_calls) == 1


def test_cli_fake_commit_close_failure_returns_sanitized_json_and_empty_stderr(capsys) -> None:
    row = _row()
    repository = FakeRepository(row)
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-publish",
            "--allow-database-write",
            "--candidate-group-id",
            str(row.aggregate_id),
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
    assert parsed["error_code"] == "repository_commit_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["events_published_count"] == 1
    assert CLOSE_EXCEPTION_DETAIL not in captured.out
    assert DB_LOCATOR not in captured.out
    assert REDIS_LOCATOR not in captured.out


def test_run_with_explicit_args_uses_candidate_group_selector() -> None:
    row = _row()
    repository = FakeRepository(row)
    publisher = FakePublisher()

    result = runner.run(
        _parse_args(
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-publish",
            "--allow-database-write",
            "--candidate-group-id",
            str(row.aggregate_id),
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakePublisherBuilder(publisher),
    )

    assert result.exit_code == 0
    assert result.report["selector_type"] == "candidate_group_id"
    assert result.report["target_candidate_group_id_suffix"] == str(row.aggregate_id)[-8:]
    assert repository.fetch_calls[0]["candidate_group_id"] == row.aggregate_id


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
    forbidden_call_attrs = forbidden_call_names | {
        "sleep",
        "xreadgroup",
        "xread",
        "xack",
        "ack",
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
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".analysis_router" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".router_normalizer" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever(" not in source
