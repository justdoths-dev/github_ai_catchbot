from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.gh_enricher.bounded_github_enrich_runner import (
    BoundedGithubEnrichConfig,
    BoundedGithubEnrichCounters,
    BoundedGithubEnrichDatabaseHandle,
    BoundedGithubEnrichRedisHandle,
    BoundedGithubEnrichRuntimeConfig,
    TemporaryGroupRedisTargetConsumer,
    run_bounded_github_enrich,
)
from src.services.gh_enricher.config import GhEnricherConfig
from src.services.gh_enricher.models import ArtifactEnrichmentJob, ArtifactRecord, EnrichmentResult


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/gh_enricher/bounded_github_enrich_runner.py"
TOOL_PATH = ROOT / "tools/bounded_gh_enricher_job_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_CANONICAL_URL = "https://github.com/private-owner/private-repo"
RAW_GITHUB_RESPONSE = '{"token":"sentinel-token","full_name":"private-owner/private-repo"}'
RAW_EXCEPTION_DETAIL = "private database or github detail with sentinel-token and private-owner/private-repo"
STREAM_ID = "171000382609-0"


class FakeRedisClient:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, object]]] | None = None,
        *,
        ack_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.entries = entries or []
        self.ack_error = ack_error
        self.order = order
        self.group_created = False
        self.group_destroyed = False
        self.acked: list[str] = []
        self.read_calls = 0

    async def xlen(self, name: str) -> int:
        assert name == "q.artifact.enrich.github"
        return len(self.entries)

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> None:
        del groupname
        assert name == "q.artifact.enrich.github"
        assert id == "0"
        assert mkstream is False
        self.group_created = True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, object]]]]]:
        del groupname, consumername, block
        assert self.group_created is True
        assert streams == {"q.artifact.enrich.github": ">"}
        self.read_calls += 1
        if self.read_calls > 1:
            return []
        return [("q.artifact.enrich.github", self.entries[:count])]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        del groupname
        assert name == "q.artifact.enrich.github"
        if self.order is not None:
            self.order.append("redis:ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(ids)
        return len(ids)

    async def xgroup_destroy(self, name: str, groupname: str) -> int:
        del groupname
        assert name == "q.artifact.enrich.github"
        self.group_destroyed = True
        return 1


class FakeRedisBuilder:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        consumer = TemporaryGroupRedisTargetConsumer(self.client, queue_name="q.artifact.enrich.github")

        async def close() -> None:
            await consumer.cleanup(state)

        return BoundedGithubEnrichRedisHandle(consumer=consumer, close=close)


class FakeDatabase:
    def __init__(
        self,
        *,
        job: ArtifactEnrichmentJob | None,
        artifact: ArtifactRecord | None,
        artifact_after: ArtifactRecord | None,
        counters: BoundedGithubEnrichCounters,
        state,
        result: EnrichmentResult | None = None,
        handle_error: BaseException | None = None,
        github_failure_supported: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.job = job
        self.artifact = artifact
        self.artifact_after = artifact_after
        self.counters = counters
        self.state = state
        self.result = result
        self.handle_error = handle_error
        self.github_failure_supported = github_failure_supported
        self.order = order
        self.rehydrated_trigger_ids: list[str] = []
        self.handled_jobs: list[ArtifactEnrichmentJob] = []
        self.handle_called = False

    async def rehydrate_job(self, trigger_event_id: str):
        self.rehydrated_trigger_ids.append(trigger_event_id)
        return self.job

    async def load_artifact(self, artifact_id: UUID):
        del artifact_id
        if self.handle_called:
            return self.artifact_after
        return self.artifact

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        self.handled_jobs.append(job)
        self.handle_called = True
        self.state.github_read_attempted = True
        self.state.github_request_count = max(self.state.github_request_count, 4)
        self.counters.enrichment_runs_written_count = 1
        if self.handle_error is not None:
            raise self.handle_error
        if self.github_failure_supported:
            self.counters.enrichment_runs_finished_count = 1
            if self.order is not None:
                self.order.append("db:commit")
            return EnrichmentResult(
                artifact_id=job.artifact_id,
                snapshot_id=None,
                status="rate_limited",
                content_anchor=None,
                emitted_snapshot_updated=False,
            )
        self.counters.enrichment_runs_finished_count = 1
        self.counters.snapshots_written_count = 1
        self.counters.github_repo_rows_written_count = 1
        self.counters.github_file_samples_written_count = 2
        self.counters.discovered_urls_written_count = 1
        self.counters.artifact_registry_updates_count = 1
        self.counters.snapshot_updated_outbox_inserted_count = 1
        self.counters.artifact_snapshot_updated_event_suffixes = ["deadbeef"]
        if self.order is not None:
            self.order.append("db:commit")
        if self.result is not None:
            return self.result
        snapshot_id = uuid4()
        self.artifact_after = _artifact(job.artifact_id, current_snapshot_id=snapshot_id, current_status="ready")
        return EnrichmentResult(
            artifact_id=job.artifact_id,
            snapshot_id=snapshot_id,
            status="ready",
            content_anchor="commit:" + "a" * 40,
            emitted_snapshot_updated=True,
        )


class FakeDatabaseBuilder:
    def __init__(
        self,
        *,
        job: ArtifactEnrichmentJob | None,
        artifact: ArtifactRecord | None,
        artifact_after: ArtifactRecord | None = None,
        result: EnrichmentResult | None = None,
        handle_error: BaseException | None = None,
        github_failure_supported: bool = False,
        close_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.job = job
        self.artifact = artifact
        self.artifact_after = artifact_after or artifact
        self.result = result
        self.handle_error = handle_error
        self.github_failure_supported = github_failure_supported
        self.close_error = close_error
        self.order = order
        self.calls = 0
        self.counters = BoundedGithubEnrichCounters()
        self.database: FakeDatabase | None = None
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True
        self.database = FakeDatabase(
            job=self.job,
            artifact=self.artifact,
            artifact_after=self.artifact_after,
            counters=self.counters,
            state=state,
            result=self.result,
            handle_error=self.handle_error,
            github_failure_supported=self.github_failure_supported,
            order=self.order,
        )

        async def close() -> None:
            self.closed = True
            if self.close_error is not None:
                raise self.close_error

        return BoundedGithubEnrichDatabaseHandle(
            database=self.database,
            counters=self.counters,
            close=close,
        )


class RaisingDatabaseBuilder:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger
        self.calls += 1
        raise AssertionError("database builder must not be called")


def _runtime_config() -> BoundedGithubEnrichRuntimeConfig:
    return BoundedGithubEnrichRuntimeConfig(
        gh_config=GhEnricherConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.artifact.enrich.github",
            consumer_group="gh-enricher",
            consumer_name="bounded-test",
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


def _raising_runtime_config() -> BoundedGithubEnrichRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _approved_config(**overrides) -> BoundedGithubEnrichConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_consume": True,
        "allow_database_write": True,
        "allow_github_read": True,
        "allow_redis_ack": True,
        "redis_message_id": STREAM_ID,
        "artifact_id": None,
        "trigger_event_id": None,
        "max_messages": 1,
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedGithubEnrichConfig(**values)


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


def _artifact(
    artifact_id: UUID,
    *,
    artifact_type: str = "github_repo",
    normalized_host: str | None = "github.com",
    canonical_url: str | None = RAW_CANONICAL_URL,
    current_snapshot_id: UUID | None = None,
    current_status: str | None = None,
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        canonical_id="github:repo:private-owner/private-repo",
        canonical_url=canonical_url,
        normalized_host=normalized_host,
        artifact_key_json={"owner": "private-owner", "repo": "private-repo"},
        current_snapshot_id=current_snapshot_id,
        current_status=current_status,
    )


def _thin_fields(trigger_event_id: UUID, artifact_id: UUID, **overrides: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "job_id": str(trigger_event_id),
        "stage_name": "enrich_github",
        "root_object_type": "artifact",
        "root_object_id": str(artifact_id),
        "idempotency_key": "private-idempotency-key",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(trigger_event_id),
    }
    fields.update(overrides)
    return fields


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_redis_db_or_github() -> None:
    redis_builder = FakeRedisBuilder(FakeRedisClient())
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_github_enrich(
        BoundedGithubEnrichConfig(),
        runtime_config_loader=_raising_runtime_config,
        redis_builder=redis_builder,
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "operator_approval_missing"
    assert report["redis_consume_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["github_read_attempted"] is False
    assert redis_builder.calls == 0
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_exact_redis_message_rehydrates_artifact_and_validates_github_route() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))])
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id), artifact=_artifact(artifact_id))

    result = await run_bounded_github_enrich(
        _approved_config(redis_message_id=STREAM_ID),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["queue_name"] == "q.artifact.enrich.github"
    assert report["stage_name"] == "enrich_github"
    assert report["artifact_type"] == "github_repo"
    assert report["normalized_host"] == "github.com"
    assert report["canonical_url_present"] is True
    assert report["messages_seen"] == 1
    assert report["messages_matched"] == 1
    assert database_builder.database is not None
    assert database_builder.database.rehydrated_trigger_ids == [str(trigger_event_id)]
    assert len(database_builder.database.handled_jobs) == 1
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_multiple_exact_ids_are_cross_checks_not_target_conflict() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))])
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id), artifact=_artifact(artifact_id))

    result = await run_bounded_github_enrich(
        _approved_config(
            redis_message_id=STREAM_ID,
            trigger_event_id=trigger_event_id,
            artifact_id=artifact_id,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.ok is True
    assert result.error_code is None
    assert result.target_trigger_event_id_suffix == str(trigger_event_id)[-8:]
    assert result.target_artifact_id_suffix == str(artifact_id)[-8:]
    assert result.redis_message_id_suffix == "382609-0"


@pytest.mark.asyncio
async def test_redis_artifact_or_event_mismatch_blocks_before_db_github_or_ack() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))])
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_github_enrich(
        _approved_config(redis_message_id=STREAM_ID, artifact_id=uuid4()),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "target_artifact_mismatch"
    assert result.messages_matched == 1
    assert result.messages_processed_count == 0
    assert result.state.github_read_attempted is False
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert database_builder.calls == 0
    assert redis.acked == []
    assert redis.group_destroyed is True


@pytest.mark.asyncio
async def test_github_read_gate_missing_blocks_before_network_db_or_redis() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis_builder = FakeRedisBuilder(FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))]))
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_github_enrich(
        _approved_config(allow_github_read=False),
        runtime_config_loader=_runtime_config,
        redis_builder=redis_builder,
        database_builder=database_builder,
    )

    assert result.error_code == "github_read_not_allowed"
    assert result.state.runtime_config_loaded is True
    assert result.state.redis_consume_attempted is False
    assert result.state.github_read_attempted is False
    assert result.state.database_write_attempted is False
    assert redis_builder.calls == 0
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_execute_writes_snapshot_outbox_and_acks_after_durable_readback() -> None:
    order: list[str] = []
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    snapshot_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))], order=order)
    database_builder = FakeDatabaseBuilder(
        job=_job(trigger_event_id, artifact_id),
        artifact=_artifact(artifact_id),
        artifact_after=_artifact(artifact_id, current_snapshot_id=snapshot_id, current_status="ready"),
        result=EnrichmentResult(
            artifact_id=artifact_id,
            snapshot_id=snapshot_id,
            status="ready",
            content_anchor="commit:" + "b" * 40,
            emitted_snapshot_updated=True,
        ),
        order=order,
    )

    result = await run_bounded_github_enrich(
        _approved_config(redis_message_id=STREAM_ID),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["messages_processed_count"] == 1
    assert report["redis_acked_count"] == 1
    assert report["github_request_count"] == 4
    assert report["enrichment_runs_written_count"] == 1
    assert report["snapshots_written_count"] == 1
    assert report["github_repo_rows_written_count"] == 1
    assert report["github_file_samples_written_count"] == 2
    assert report["discovered_urls_written_count"] == 1
    assert report["snapshot_updated_outbox_inserted_count"] == 1
    assert report["artifact_snapshot_updated_event_suffixes"] == ["deadbeef"]
    assert report["artifact_current_status"] == "ready"
    assert order == ["db:commit", "redis:ack"]
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_db_write_failure_leaves_redis_unacked() -> None:
    order: list[str] = []
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))], order=order)
    database_builder = FakeDatabaseBuilder(
        job=_job(trigger_event_id, artifact_id),
        artifact=_artifact(artifact_id),
        handle_error=RuntimeError(RAW_EXCEPTION_DETAIL),
        order=order,
    )

    result = await run_bounded_github_enrich(
        _approved_config(redis_message_id=STREAM_ID),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "database_write_failed"
    assert result.state.database_write_attempted is True
    assert result.state.redis_ack_attempted is False
    assert result.redis_acked_count == 0
    assert "redis:ack" not in order
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_github_failure_with_safe_failed_run_support_acks_after_durable_failed_run() -> None:
    order: list[str] = []
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))], order=order)
    database_builder = FakeDatabaseBuilder(
        job=_job(trigger_event_id, artifact_id),
        artifact=_artifact(artifact_id),
        github_failure_supported=True,
        order=order,
    )

    result = await run_bounded_github_enrich(
        _approved_config(redis_message_id=STREAM_ID),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["snapshot_status"] == "rate_limited"
    assert report["enrichment_runs_finished_count"] == 1
    assert report["snapshots_written_count"] == 0
    assert report["snapshot_updated_outbox_inserted_count"] == 0
    assert report["redis_acked_count"] == 1
    assert order == ["db:commit", "redis:ack"]
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_report_redacts_raw_github_response_url_token_db_redis_and_full_ids() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient(
        [
            (
                STREAM_ID,
                _thin_fields(
                    trigger_event_id,
                    artifact_id,
                    github_response=RAW_GITHUB_RESPONSE,
                ),
            )
        ]
    )

    result = await run_bounded_github_enrich(
        _approved_config(redis_message_id=STREAM_ID),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=RaisingDatabaseBuilder(),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "redis_message_contract_invalid"
    for raw in (
        RAW_GITHUB_RESPONSE,
        RAW_CANONICAL_URL,
        "sentinel-token",
        DB_URL,
        REDIS_URL,
        str(trigger_event_id),
        str(artifact_id),
        STREAM_ID,
    ):
        assert raw not in rendered
    assert result.to_sanitized_dict()["redactions_applied"]["raw_github_response_omitted"] is True


def test_source_static_authority_guard_has_no_forbidden_runtime_surfaces() -> None:
    for path in (SOURCE_PATH, TOOL_PATH):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots: set[str] = set()
        imported_modules: set[str] = set()
        call_attrs: set[str] = set()
        call_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    call_attrs.add(node.func.attr)
                elif isinstance(node.func, ast.Name):
                    call_names.add(node.func.id)

        assert {"openai", "telegram", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(imported_roots)
        assert not any(".notifier_telegram" in module for module in imported_modules)
        assert not any(".policy_engine" in module for module in imported_modules)
        assert not any(".judge_openai" in module for module in imported_modules)
        assert not any(".x_enricher" in module for module in imported_modules)
        assert not any(".web_enricher" in module for module in imported_modules)
        assert not any(".worker" in module for module in imported_modules)
        assert {"run_forever", "xclaim", "xautoclaim", "xdel", "system", "popen"}.isdisjoint(call_attrs)
        assert {"urlopen", "systemctl"}.isdisjoint(call_names)
        assert "runtime.env" not in source
        assert "docker" not in source.lower().replace("docker_called", "")
        assert "alembic" not in source.lower().replace("alembic_called", "")
