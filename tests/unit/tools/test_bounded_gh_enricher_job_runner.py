from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID, uuid4

from src.services.gh_enricher.bounded_github_enrich_runner import (
    BoundedGithubEnrichCounters,
    BoundedGithubEnrichDatabaseHandle,
    BoundedGithubEnrichRedisHandle,
    BoundedGithubEnrichRuntimeConfig,
    TargetedRedisMessage,
)
from src.services.gh_enricher.config import GhEnricherConfig
from src.services.gh_enricher.models import ArtifactEnrichmentJob, ArtifactRecord, EnrichmentResult
from tools import bounded_gh_enricher_job_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_gh_enricher_job_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
STREAM_ID = "171000382609-0"
RAW_EXCEPTION_DETAIL = "sentinel private ack detail"


class FakeConsumer:
    def __init__(self, *, trigger_event_id: UUID, artifact_id: UUID, ack_error: BaseException | None = None) -> None:
        self.trigger_event_id = trigger_event_id
        self.artifact_id = artifact_id
        self.ack_error = ack_error
        self.acked: list[str] = []
        self.find_calls = 0

    async def find_target(self, config, state):
        self.find_calls += 1
        state.redis_consume_attempted = True
        state.redis_group_created = True
        return (
            TargetedRedisMessage(
                redis_message_id=STREAM_ID,
                fields={
                    "job_id": str(self.trigger_event_id),
                    "stage_name": "enrich_github",
                    "root_object_type": "artifact",
                    "root_object_id": str(self.artifact_id),
                    "idempotency_key": "private-cli-idempotency-key",
                    "trigger_event_id": str(self.trigger_event_id),
                },
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

        return BoundedGithubEnrichRedisHandle(consumer=self.consumer, close=close)


class FakeDatabase:
    def __init__(self, *, job: ArtifactEnrichmentJob, artifact: ArtifactRecord, counters: BoundedGithubEnrichCounters, state) -> None:
        self.job = job
        self.artifact = artifact
        self.counters = counters
        self.state = state
        self.rehydrated_trigger_ids: list[str] = []
        self.handled_jobs: list[ArtifactEnrichmentJob] = []
        self.snapshot_id = uuid4()

    async def rehydrate_job(self, trigger_event_id: str):
        self.rehydrated_trigger_ids.append(trigger_event_id)
        return self.job

    async def load_artifact(self, artifact_id: UUID):
        del artifact_id
        if self.handled_jobs:
            return ArtifactRecord(
                artifact_id=self.artifact.artifact_id,
                artifact_type=self.artifact.artifact_type,
                canonical_id=self.artifact.canonical_id,
                canonical_url=self.artifact.canonical_url,
                normalized_host=self.artifact.normalized_host,
                artifact_key_json=self.artifact.artifact_key_json,
                current_snapshot_id=self.snapshot_id,
                current_status="ready",
            )
        return self.artifact

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        self.handled_jobs.append(job)
        self.state.github_read_attempted = True
        self.state.github_request_count = 4
        self.counters.enrichment_runs_written_count = 1
        self.counters.enrichment_runs_finished_count = 1
        self.counters.snapshots_written_count = 1
        self.counters.github_repo_rows_written_count = 1
        self.counters.github_file_samples_written_count = 1
        self.counters.artifact_registry_updates_count = 1
        self.counters.snapshot_updated_outbox_inserted_count = 1
        self.counters.artifact_snapshot_updated_event_suffixes = ["cafed00d"]
        return EnrichmentResult(
            artifact_id=job.artifact_id,
            snapshot_id=self.snapshot_id,
            status="ready",
            content_anchor="commit:" + "c" * 40,
            emitted_snapshot_updated=True,
        )


class FakeDatabaseBuilder:
    def __init__(self, *, job: ArtifactEnrichmentJob, artifact: ArtifactRecord, close_error: BaseException | None = None) -> None:
        self.job = job
        self.artifact = artifact
        self.close_error = close_error
        self.counters = BoundedGithubEnrichCounters()
        self.database: FakeDatabase | None = None
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True
        self.database = FakeDatabase(job=self.job, artifact=self.artifact, counters=self.counters, state=state)

        async def close() -> None:
            self.close_calls += 1
            if self.close_error is not None:
                raise self.close_error

        return BoundedGithubEnrichDatabaseHandle(
            database=self.database,
            counters=self.counters,
            close=close,
        )


def _runtime_config() -> BoundedGithubEnrichRuntimeConfig:
    return BoundedGithubEnrichRuntimeConfig(
        gh_config=GhEnricherConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.artifact.enrich.github",
            consumer_group="gh-enricher",
            consumer_name="bounded-cli-test",
            batch_size=1,
            block_ms=100,
            github_api_base_url="https://api.github.com",
            github_app_id=None,
            github_installation_id=None,
            github_private_key=None,
            request_timeout_sec=1,
            sample_max_files=5,
            sample_excerpt_chars=200,
            max_file_bytes=4096,
            stale_after_sec=3600,
            log_level="INFO",
        )
    )


def _job(trigger_event_id: UUID, artifact_id: UUID) -> ArtifactEnrichmentJob:
    return ArtifactEnrichmentJob(
        trigger_event_id=trigger_event_id,
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=uuid4(),
        artifact_id=artifact_id,
        artifact_type="github_repo",
        provider_route="github",
        refresh_mode="standard",
        depth_budget=1,
    )


def _artifact(artifact_id: UUID) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type="github_repo",
        canonical_id="github:repo:private-owner/private-repo",
        canonical_url="https://github.com/private-owner/private-repo",
        normalized_host="github.com",
        artifact_key_json={"owner": "private-owner", "repo": "private-repo"},
        current_snapshot_id=None,
        current_status=None,
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_github_enrich_runner_v1"
    assert parsed["runner_name"] == "bounded_gh_enricher_job_runner"
    assert parsed["mode"] == "github_enrich_one_shot"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_consume_attempted"] is False
    assert parsed["github_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_ack_attempted"] is False
    assert parsed["side_effects"]["worker_started"] is False
    assert parsed["side_effects"]["run_forever_called"] is False


def test_unsupported_cli_args_return_sanitized_json_without_runtime_config(capsys) -> None:
    for flag in (
        "--allow-openai",
        "--allow-telegram",
        "--allow-web",
        "--allow-x",
        "--run-forever",
        "--database-url",
        "--redis-url",
        "--runtime-env",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["redis_consume_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["github_read_attempted"] is False


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
        "--allow-github-read",
        "--allow-redis-ack",
        "--redis-message-id",
        "--artifact-id",
        "--trigger-event-id",
        "--max-messages",
        "--scan-limit",
    }


def test_valid_cli_fake_run_prints_json_only_and_delegates_to_source_runner(capsys) -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    consumer = FakeConsumer(trigger_event_id=trigger_event_id, artifact_id=artifact_id)
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id), artifact=_artifact(artifact_id))

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-github-read",
            "--allow-redis-ack",
            "--redis-message-id",
            STREAM_ID,
            "--artifact-id",
            str(artifact_id),
            "--trigger-event-id",
            str(trigger_event_id),
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
    assert parsed["status"] == "processed"
    assert parsed["queue_name"] == "q.artifact.enrich.github"
    assert parsed["stage_name"] == "enrich_github"
    assert parsed["messages_seen"] == 1
    assert parsed["messages_matched"] == 1
    assert parsed["messages_processed_count"] == 1
    assert parsed["redis_acked_count"] == 1
    assert parsed["github_request_count"] == 4
    assert parsed["snapshot_updated_outbox_inserted_count"] == 1
    assert parsed["artifact_snapshot_updated_event_suffixes"] == ["cafed00d"]
    assert database_builder.database is not None
    assert len(database_builder.database.handled_jobs) == 1
    assert consumer.acked == [STREAM_ID]
    for raw in (str(trigger_event_id), str(artifact_id), STREAM_ID, DB_URL, REDIS_URL):
        assert raw not in captured.out


def test_cli_ack_failure_prints_json_only_and_omits_raw_exception(capsys) -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    consumer = FakeConsumer(
        trigger_event_id=trigger_event_id,
        artifact_id=artifact_id,
        ack_error=RuntimeError(RAW_EXCEPTION_DETAIL),
    )

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-github-read",
            "--allow-redis-ack",
            "--redis-message-id",
            STREAM_ID,
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id), artifact=_artifact(artifact_id)),
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
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "handle_job" not in call_attrs

