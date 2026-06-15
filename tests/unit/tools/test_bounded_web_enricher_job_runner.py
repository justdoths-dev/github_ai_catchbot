from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.services.web_enricher.bounded_web_enrich_runner import (
    BoundedWebEnrichCounters,
    BoundedWebEnrichDatabaseHandle,
    BoundedWebEnrichRedisHandle,
    BoundedWebEnrichRuntimeConfig,
)
from src.services.web_enricher.config import WebEnricherConfig
from src.services.web_enricher.models import ArtifactEnrichmentJob, EnrichmentResult
from tools import bounded_web_enricher_job_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_web_enricher_job_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_EXCEPTION_DETAIL = "sentinel private ack or database failure detail"
STREAM_ID = "1781486051921-0"


class FakeConsumer:
    def __init__(self, *, trigger_event_id, artifact_id, ack_error: BaseException | None = None) -> None:
        self.trigger_event_id = trigger_event_id
        self.artifact_id = artifact_id
        self.ack_error = ack_error
        self.acked: list[str] = []
        self.find_calls = 0

    async def find_target(self, config, state):
        self.find_calls += 1
        state.redis_consume_attempted = True
        state.redis_group_created = True
        from src.services.web_enricher.bounded_web_enrich_runner import TargetedRedisMessage

        return (
            TargetedRedisMessage(
                redis_message_id=STREAM_ID,
                fields={
                    "job_id": str(self.trigger_event_id),
                    "stage_name": "enrich_web",
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

        return BoundedWebEnrichRedisHandle(consumer=self.consumer, close=close)


class FakeService:
    def __init__(self, *, job: ArtifactEnrichmentJob, counters: BoundedWebEnrichCounters, state) -> None:
        self.job = job
        self.counters = counters
        self.state = state
        self.rehydrated_trigger_ids: list[str] = []
        self.handled_jobs: list[ArtifactEnrichmentJob] = []

    async def rehydrate_job(self, trigger_event_id: str):
        self.rehydrated_trigger_ids.append(trigger_event_id)
        return self.job

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        self.handled_jobs.append(job)
        self.state.web_fetch_attempted = True
        self.counters.artifact_enrichment_runs_inserted_count = 1
        self.counters.artifact_enrichment_runs_finished_count = 1
        self.counters.artifact_snapshots_written_count = 1
        self.counters.artifact_snapshot_web_article_written_count = 1
        self.counters.artifact_registry_updates_count = 1
        self.counters.artifact_snapshot_updated_outbox_count = 1
        return EnrichmentResult(
            artifact_id=job.artifact_id,
            snapshot_id=uuid4(),
            status="ready",
            content_anchor="web:" + "b" * 64,
            emitted_snapshot_updated=True,
        )


class FakeDatabaseBuilder:
    def __init__(self, *, job: ArtifactEnrichmentJob, close_error: BaseException | None = None) -> None:
        self.job = job
        self.close_error = close_error
        self.counters = BoundedWebEnrichCounters()
        self.service: FakeService | None = None
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True
        self.service = FakeService(job=self.job, counters=self.counters, state=state)

        async def close() -> None:
            self.close_calls += 1
            if self.close_error is not None:
                raise self.close_error

        return BoundedWebEnrichDatabaseHandle(
            service=self.service,
            counters=self.counters,
            close=close,
        )


def _runtime_config() -> BoundedWebEnrichRuntimeConfig:
    return BoundedWebEnrichRuntimeConfig(
        web_config=WebEnricherConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.artifact.enrich.web",
            consumer_group="web-enricher",
            consumer_name="bounded-cli-test",
            batch_size=1,
            block_ms=100,
            request_timeout_sec=1,
            max_redirects=2,
            max_bytes=4096,
            excerpt_chars=200,
            max_outbound_links=10,
            user_agent="bounded-cli-test",
            content_type_allowlist=("text/html", "application/xhtml+xml", "text/plain", "text/markdown"),
            log_level="INFO",
        )
    )


def _job(trigger_event_id, artifact_id) -> ArtifactEnrichmentJob:
    return ArtifactEnrichmentJob(
        trigger_event_id=trigger_event_id,
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=uuid4(),
        artifact_id=artifact_id,
        artifact_type="web_article",
        provider_route="web",
        refresh_mode="standard",
        depth_budget=1,
        requested_at=datetime.now(timezone.utc),
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_web_enrich_runner_v1"
    assert parsed["runner_name"] == "bounded_web_enricher_job_runner"
    assert parsed["mode"] == "web_enrich_one_shot"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_consume_attempted"] is False
    assert parsed["web_fetch_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_ack_attempted"] is False
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
        "--allow-web-fetch",
        "--trigger-event-id",
        "--artifact-id",
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
    assert parsed["web_fetch_attempted"] is False


def test_valid_cli_fake_run_prints_json_only_and_delegates_to_service(capsys) -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    consumer = FakeConsumer(trigger_event_id=trigger_event_id, artifact_id=artifact_id)
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id))

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--allow-web-fetch",
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
    assert parsed["queue_name"] == "q.artifact.enrich.web"
    assert parsed["stage_name"] == "enrich_web"
    assert parsed["messages_seen"] == 1
    assert parsed["messages_matched"] == 1
    assert parsed["messages_processed_count"] == 1
    assert parsed["redis_acked_count"] == 1
    assert parsed["artifact_enrichment_runs_inserted_count"] == 1
    assert parsed["artifact_snapshots_written_count"] == 1
    assert parsed["artifact_snapshot_updated_outbox_count"] == 1
    assert parsed["snapshot_status"] == "ready"
    assert parsed["web_fetch_attempted"] is True
    assert database_builder.service is not None
    assert len(database_builder.service.handled_jobs) == 1
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
            "--allow-redis-ack",
            "--allow-web-fetch",
            "--trigger-event-id",
            str(trigger_event_id),
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id)),
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
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "handle_job" not in call_attrs
