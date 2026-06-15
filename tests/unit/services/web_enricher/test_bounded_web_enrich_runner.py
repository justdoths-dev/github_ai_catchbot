from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from src.services.web_enricher.bounded_web_enrich_runner import (
    BoundedWebEnrichConfig,
    BoundedWebEnrichCounters,
    BoundedWebEnrichDatabaseHandle,
    BoundedWebEnrichRedisHandle,
    BoundedWebEnrichRuntimeConfig,
    TemporaryGroupRedisTargetConsumer,
    run_bounded_web_enrich,
)
from src.services.web_enricher.config import WebEnricherConfig
from src.services.web_enricher.models import ArtifactEnrichmentJob, EnrichmentResult


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/web_enricher/bounded_web_enrich_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_ARTICLE_TEXT = "sentinel raw article text https://private.example.invalid/path"
RAW_EXCEPTION_DETAIL = "private database or redis detail with sentinel raw article text"
STREAM_ID = "1781486051921-0"


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
        self.group_start_id: str | None = None

    async def xlen(self, name: str) -> int:
        assert name == "q.artifact.enrich.web"
        return len(self.entries)

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> None:
        del groupname
        assert name == "q.artifact.enrich.web"
        assert id == "0"
        assert mkstream is False
        self.group_created = True
        self.group_start_id = id

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
        assert streams == {"q.artifact.enrich.web": ">"}
        self.read_calls += 1
        if self.read_calls > 1:
            return []
        return [("q.artifact.enrich.web", self.entries[:count])]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        del groupname
        assert name == "q.artifact.enrich.web"
        if self.order is not None:
            self.order.append("redis:ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(ids)
        return len(ids)

    async def xgroup_destroy(self, name: str, groupname: str) -> int:
        del groupname
        assert name == "q.artifact.enrich.web"
        self.group_destroyed = True
        return 1


class FakeRedisBuilder:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        consumer = TemporaryGroupRedisTargetConsumer(self.client, queue_name="q.artifact.enrich.web")

        async def close() -> None:
            await consumer.cleanup(state)

        return BoundedWebEnrichRedisHandle(consumer=consumer, close=close)


class FakeService:
    def __init__(
        self,
        *,
        job: ArtifactEnrichmentJob | None,
        counters: BoundedWebEnrichCounters,
        state,
        result: EnrichmentResult | None = None,
        handle_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.job = job
        self.counters = counters
        self.state = state
        self.result = result
        self.handle_error = handle_error
        self.order = order
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
        if self.handle_error is not None:
            raise self.handle_error
        self.counters.artifact_snapshots_written_count = 1
        self.counters.artifact_snapshot_web_article_written_count = 1
        self.counters.discovered_url_observations_written_count = 1
        self.counters.artifact_registry_updates_count = 1
        self.counters.artifact_snapshot_updated_outbox_count = 1
        if self.order is not None:
            self.order.append("db:commit")
        return self.result or EnrichmentResult(
            artifact_id=job.artifact_id,
            snapshot_id=uuid4(),
            status="ready",
            content_anchor="web:" + "a" * 64,
            emitted_snapshot_updated=True,
        )


class FakeDatabaseBuilder:
    def __init__(
        self,
        *,
        job: ArtifactEnrichmentJob | None,
        result: EnrichmentResult | None = None,
        handle_error: BaseException | None = None,
        close_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.job = job
        self.result = result
        self.handle_error = handle_error
        self.close_error = close_error
        self.order = order
        self.calls = 0
        self.counters = BoundedWebEnrichCounters()
        self.service: FakeService | None = None
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True
        self.service = FakeService(
            job=self.job,
            counters=self.counters,
            state=state,
            result=self.result,
            handle_error=self.handle_error,
            order=self.order,
        )

        async def close() -> None:
            self.closed = True
            if self.close_error is not None:
                raise self.close_error

        return BoundedWebEnrichDatabaseHandle(
            service=self.service,
            counters=self.counters,
            close=close,
        )


class RaisingDatabaseBuilder:
    calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger
        self.calls += 1
        raise AssertionError("database builder must not be called")


def _runtime_config() -> BoundedWebEnrichRuntimeConfig:
    return BoundedWebEnrichRuntimeConfig(
        web_config=WebEnricherConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.artifact.enrich.web",
            consumer_group="web-enricher",
            consumer_name="bounded-test",
            batch_size=1,
            block_ms=100,
            request_timeout_sec=1,
            max_redirects=2,
            max_bytes=4096,
            excerpt_chars=200,
            max_outbound_links=10,
            user_agent="bounded-test",
            content_type_allowlist=("text/html", "application/xhtml+xml", "text/plain", "text/markdown"),
            log_level="INFO",
        )
    )


def _raising_runtime_config() -> BoundedWebEnrichRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _approved_config(**overrides) -> BoundedWebEnrichConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_consume": True,
        "allow_database_write": True,
        "allow_redis_ack": True,
        "allow_web_fetch": True,
        "trigger_event_id": uuid4(),
        "artifact_id": None,
        "redis_message_id": None,
        "max_messages": 1,
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedWebEnrichConfig(**values)


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


def _thin_fields(trigger_event_id, artifact_id, **overrides: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "job_id": str(trigger_event_id),
        "stage_name": "enrich_web",
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
async def test_no_flags_fail_closed_before_runtime_config_redis_db_or_web() -> None:
    redis_builder = FakeRedisBuilder(FakeRedisClient())
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_web_enrich(
        BoundedWebEnrichConfig(),
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
    assert report["web_fetch_attempted"] is False
    assert redis_builder.calls == 0
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_conflicting_targets_and_hard_caps_fail_before_runtime_config() -> None:
    missing = await run_bounded_web_enrich(
        BoundedWebEnrichConfig(operator_approved=True),
        runtime_config_loader=_raising_runtime_config,
    )
    conflict = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=uuid4(), artifact_id=uuid4()),
        runtime_config_loader=_raising_runtime_config,
    )
    max_messages = await run_bounded_web_enrich(
        _approved_config(max_messages=2),
        runtime_config_loader=_raising_runtime_config,
    )
    scan_limit = await run_bounded_web_enrich(
        _approved_config(scan_limit=101),
        runtime_config_loader=_raising_runtime_config,
    )

    assert missing.error_code == "target_missing"
    assert conflict.error_code == "target_conflict"
    assert max_messages.error_code == "max_messages_must_be_one"
    assert scan_limit.error_code == "scan_limit_out_of_range"
    assert missing.state.runtime_config_loaded is False
    assert conflict.state.runtime_config_loaded is False
    assert max_messages.state.runtime_config_loaded is False
    assert scan_limit.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_missing_web_fetch_gate_blocks_before_redis_or_db() -> None:
    config = _approved_config(allow_web_fetch=False)
    result = await run_bounded_web_enrich(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(FakeRedisClient()),
        database_builder=RaisingDatabaseBuilder(),
    )

    assert result.error_code == "web_fetch_not_allowed"
    assert result.state.runtime_config_loaded is True
    assert result.state.redis_consume_attempted is False
    assert result.state.database_write_attempted is False
    assert result.state.web_fetch_attempted is False


@pytest.mark.asyncio
async def test_redis_business_payload_fields_are_rejected_without_leakage() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient(
        [
            (
                STREAM_ID,
                _thin_fields(
                    trigger_event_id,
                    artifact_id,
                    payload_json={"raw": RAW_ARTICLE_TEXT},
                    html="<html>private</html>",
                    article_text=RAW_ARTICLE_TEXT,
                ),
            )
        ]
    )

    result = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=trigger_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=RaisingDatabaseBuilder(),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "redis_message_contract_invalid"
    assert result.messages_matched == 1
    assert result.messages_processed_count == 0
    assert result.state.database_write_attempted is False
    assert result.state.web_fetch_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert redis.group_destroyed is True
    assert RAW_ARTICLE_TEXT not in rendered
    assert str(trigger_event_id) not in rendered
    assert str(artifact_id) not in rendered
    assert STREAM_ID not in rendered


@pytest.mark.asyncio
async def test_non_target_message_is_not_processed_or_acked() -> None:
    target_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([("100-0", _thin_fields(uuid4(), artifact_id))])
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=target_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "target_message_not_found"
    assert result.messages_seen == 1
    assert result.messages_matched == 0
    assert result.messages_processed_count == 0
    assert result.state.redis_ack_attempted is False
    assert result.state.web_fetch_attempted is False
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_stage_and_root_contract_mismatches_block_before_db_fetch_or_ack() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    for overrides, expected_error in (
        ({"stage_name": "enrich_x"}, "stage_not_allowed"),
        ({"root_object_type": "source_message"}, "root_object_type_not_allowed"),
    ):
        redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id, **overrides))])
        database_builder = RaisingDatabaseBuilder()

        result = await run_bounded_web_enrich(
            _approved_config(trigger_event_id=trigger_event_id),
            runtime_config_loader=_runtime_config,
            redis_builder=FakeRedisBuilder(redis),
            database_builder=database_builder,
        )

        assert result.error_code == expected_error
        assert result.messages_matched == 1
        assert result.messages_processed_count == 0
        assert result.state.redis_ack_attempted is False
        assert result.state.database_write_attempted is False
        assert result.state.web_fetch_attempted is False
        assert redis.acked == []
        assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_matching_job_rehydrates_handles_and_acks_after_service_commit() -> None:
    order: list[str] = []
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))], order=order)
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id), order=order)

    result = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=trigger_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is True
    assert result.status == "processed"
    assert result.snapshot_status == "ready"
    assert result.counters.artifact_enrichment_runs_inserted_count == 1
    assert result.counters.artifact_snapshots_written_count == 1
    assert result.counters.artifact_snapshot_web_article_written_count == 1
    assert result.counters.discovered_url_observations_written_count == 1
    assert result.counters.artifact_registry_updates_count == 1
    assert result.counters.artifact_snapshot_updated_outbox_count == 1
    assert result.state.database_write_attempted is True
    assert result.state.web_fetch_attempted is True
    assert result.redis_acked_count == 1
    assert order == ["db:commit", "redis:ack"]
    assert redis.acked == [STREAM_ID]
    assert database_builder.service is not None
    assert database_builder.service.rehydrated_trigger_ids == [str(trigger_event_id)]
    assert len(database_builder.service.handled_jobs) == 1
    assert str(trigger_event_id) not in rendered
    assert str(artifact_id) not in rendered
    assert STREAM_ID not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered


@pytest.mark.asyncio
async def test_artifact_id_selector_matches_root_and_rehydrated_job() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))])
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id))

    result = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=None, artifact_id=artifact_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.ok is True
    assert result.messages_matched == 1
    assert result.target_artifact_id_suffix == str(artifact_id)[-8:]


@pytest.mark.asyncio
async def test_job_artifact_mismatch_blocks_without_handle_or_ack() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))])
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, uuid4()))

    result = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=trigger_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "target_artifact_mismatch"
    assert result.messages_processed_count == 0
    assert result.state.redis_ack_attempted is False
    assert result.state.web_fetch_attempted is False
    assert database_builder.service is not None
    assert database_builder.service.handled_jobs == []
    assert redis.acked == []


@pytest.mark.asyncio
async def test_db_write_failure_returns_sanitized_json_and_does_not_ack() -> None:
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))])
    database_builder = FakeDatabaseBuilder(
        job=_job(trigger_event_id, artifact_id),
        handle_error=RuntimeError(RAW_EXCEPTION_DETAIL),
    )

    result = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=trigger_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "database_write_failed"
    assert result.error_class == "RuntimeError"
    assert result.state.database_write_attempted is True
    assert result.state.web_fetch_attempted is True
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered
    assert RAW_ARTICLE_TEXT not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered


@pytest.mark.asyncio
async def test_redis_ack_failure_after_service_commit_returns_sanitized_json() -> None:
    order: list[str] = []
    trigger_event_id = uuid4()
    artifact_id = uuid4()
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(trigger_event_id, artifact_id))],
        ack_error=RuntimeError(RAW_EXCEPTION_DETAIL),
        order=order,
    )
    database_builder = FakeDatabaseBuilder(job=_job(trigger_event_id, artifact_id), order=order)

    result = await run_bounded_web_enrich(
        _approved_config(trigger_event_id=trigger_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "redis_ack_failed"
    assert result.error_class == "RuntimeError"
    assert result.state.database_write_attempted is True
    assert result.state.redis_ack_attempted is True
    assert result.redis_acked_count == 0
    assert order == ["db:commit", "redis:ack"]
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered
    assert STREAM_ID not in rendered


def test_source_ast_guard_has_no_broad_worker_or_forbidden_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
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

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(imported_roots)
    assert "urllib.request" not in imported_modules
    assert "urlopen" not in call_names
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".worker" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "system" not in call_attrs
    assert "popen" not in call_attrs
    assert "xdel" not in call_attrs
