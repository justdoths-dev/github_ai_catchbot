from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.router_normalizer.bounded_source_normalize_runner import (
    BoundedSourceNormalizeConfig,
    BoundedSourceNormalizeCounters,
    BoundedSourceNormalizeDatabaseHandle,
    BoundedSourceNormalizeRuntimeConfig,
    BoundedSourceNormalizeRedisHandle,
    CountingRouterNormalizerRepository,
    OfflineShortUrlResolver,
    TemporaryGroupRedisTargetConsumer,
    run_bounded_source_normalize,
)
from src.services.router_normalizer.config import RouterNormalizerConfig
from src.services.router_normalizer.models import (
    CanonicalArtifact,
    NormalizationResult,
    OutboxEventRow,
    SourceMessageSnapshot,
)
from src.services.router_normalizer.service import RouterNormalizerService


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/router_normalizer/bounded_source_normalize_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_TEXT = "sentinel raw source text https://github.com/example/example-tool"
RAW_EXCEPTION_DETAIL = "private redis ack failure detail with sentinel raw source text"
STREAM_ID = "1710000000000-0"


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
        assert name == "q.source.normalize"
        return len(self.entries)

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> None:
        del groupname
        assert name == "q.source.normalize"
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
        assert streams == {"q.source.normalize": ">"}
        self.read_calls += 1
        if self.read_calls > 1:
            return []
        return [("q.source.normalize", self.entries[:count])]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        del groupname
        assert name == "q.source.normalize"
        if self.order is not None:
            self.order.append("redis:ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(ids)
        return len(ids)

    async def xgroup_destroy(self, name: str, groupname: str) -> int:
        del groupname
        assert name == "q.source.normalize"
        self.group_destroyed = True
        return 1


class FakeRedisBuilder:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        consumer = TemporaryGroupRedisTargetConsumer(self.client, queue_name="q.source.normalize")

        async def close() -> None:
            await consumer.cleanup(state)

        return BoundedSourceNormalizeRedisHandle(consumer=consumer, close=close)


class FakeRepository:
    def __init__(self, *, event: OutboxEventRow, snapshot: SourceMessageSnapshot) -> None:
        self.event = event
        self.snapshot = snapshot
        self.requested_event_ids: list[UUID] = []
        self.requested_source_ids: list[UUID] = []
        self.normalization_runs: list[dict] = []
        self.suppression_traces: list[dict] = []
        self.artifacts_by_id: dict[str, UUID] = {}
        self.observations: list[dict] = []
        self.candidate_groups: list[dict] = []
        self.members: list[dict] = []
        self.enrich_events: list[dict] = []

    async def get_outbox_event(self, event_id: UUID) -> OutboxEventRow | None:
        self.requested_event_ids.append(event_id)
        return self.event if event_id == self.event.event_id else None

    async def get_current_source_message(self, source_message_id: UUID) -> SourceMessageSnapshot | None:
        self.requested_source_ids.append(source_message_id)
        return self.snapshot if source_message_id == self.snapshot.source_message_id else None

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int):
        raise AssertionError(f"unexpected version lookup for {source_message_id} v{version_no}")

    async def upsert_normalization_run(self, **kwargs) -> UUID:
        self.normalization_runs.append(kwargs)
        return uuid4()

    async def insert_suppression_trace(self, **kwargs) -> None:
        self.suppression_traces.append(kwargs)

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact) -> UUID:
        artifact_id = self.artifacts_by_id.get(artifact.canonical_id)
        if artifact_id is None:
            artifact_id = uuid4()
            self.artifacts_by_id[artifact.canonical_id] = artifact_id
        return artifact_id

    async def insert_artifact_observation_if_absent(self, **kwargs) -> None:
        self.observations.append(kwargs)

    async def upsert_candidate_group(self, **kwargs) -> UUID:
        candidate_group_id = uuid4()
        self.candidate_groups.append({"candidate_group_id": candidate_group_id, **kwargs})
        return candidate_group_id

    async def upsert_candidate_member(self, **kwargs) -> None:
        self.members.append(kwargs)

    async def insert_enrichment_requested_outbox(self, **kwargs) -> None:
        if kwargs["artifact"].provider_route is not None:
            self.enrich_events.append(kwargs)


class FakeDatabaseBuilder:
    def __init__(
        self,
        repository: FakeRepository,
        *,
        close_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.repository = repository
        self.close_error = close_error
        self.order = order
        self.calls = 0
        self.close_commits: list[bool] = []
        self.counters = BoundedSourceNormalizeCounters()

    async def __call__(self, runtime_config, state, logger):
        self.calls += 1
        state.database_session_opened = True
        service = RouterNormalizerService(
            runtime_config.router_config,
            repository=CountingRouterNormalizerRepository(self.repository, self.counters),  # type: ignore[arg-type]
            short_url_resolver=OfflineShortUrlResolver(()),
            logger=logger,
        )

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.order is not None:
                self.order.append("db:commit" if commit else "db:rollback")
            if self.close_error is not None:
                raise self.close_error

        return BoundedSourceNormalizeDatabaseHandle(
            service=service,
            counters=self.counters,
            close=close,
        )


class RaisingDatabaseBuilder:
    calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger
        self.calls += 1
        raise AssertionError("database builder must not be called")


def _runtime_config() -> BoundedSourceNormalizeRuntimeConfig:
    return BoundedSourceNormalizeRuntimeConfig(router_config=_router_config())


def _raising_runtime_config() -> BoundedSourceNormalizeRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _router_config() -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="test",
        database_url=DB_URL,
        redis_url=REDIS_URL,
        queue_name="q.source.normalize",
        consumer_group="router-normalizer",
        consumer_name="bounded-test",
        block_ms=100,
        batch_size=1,
        normalizer_version="bounded-test-normalizer",
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


def _approved_config(**overrides) -> BoundedSourceNormalizeConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_consume": True,
        "allow_database_write": True,
        "allow_redis_ack": True,
        "trigger_event_id": uuid4(),
        "source_message_id": None,
        "redis_message_id": None,
        "max_messages": 1,
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedSourceNormalizeConfig(**values)


def _event(event_id: UUID, source_message_id: UUID) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event_id,
        event_type="source_message.created.v1",
        aggregate_type="source_message",
        aggregate_id=source_message_id,
        dedupe_key="private-dedupe-key",
        payload_json={
            "source_message_id": str(source_message_id),
            "current_version_no": 1,
            "message_text": RAW_TEXT,
        },
        status="published",
        created_at=datetime.now(timezone.utc),
    )


def _snapshot(source_message_id: UUID, *, text: str = "AI") -> SourceMessageSnapshot:
    return SourceMessageSnapshot(
        source_message_id=source_message_id,
        source_version_no=1,
        text_body=text,
        caption_text=None,
        text_surface=text,
        entities_json=[],
        url_surface_json=[],
        raw_message_json={"private_text": RAW_TEXT},
    )


def _thin_fields(event_id: UUID, source_message_id: UUID, **overrides: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "job_id": str(event_id),
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": str(source_message_id),
        "idempotency_key": "private-idempotency-key",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }
    fields.update(overrides)
    return fields


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_redis_or_db() -> None:
    redis_builder = FakeRedisBuilder(FakeRedisClient())
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_source_normalize(
        BoundedSourceNormalizeConfig(),
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
    assert redis_builder.calls == 0
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_and_conflicting_targets_fail_before_runtime_config() -> None:
    missing = await run_bounded_source_normalize(
        BoundedSourceNormalizeConfig(operator_approved=True),
        runtime_config_loader=_raising_runtime_config,
    )
    conflict = await run_bounded_source_normalize(
        _approved_config(trigger_event_id=uuid4(), source_message_id=uuid4()),
        runtime_config_loader=_raising_runtime_config,
    )

    assert missing.error_code == "target_missing"
    assert missing.state.runtime_config_loaded is False
    assert conflict.error_code == "target_conflict"
    assert conflict.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_max_messages_and_scan_limit_hard_caps_fail_closed() -> None:
    max_messages = await run_bounded_source_normalize(
        _approved_config(max_messages=2),
        runtime_config_loader=_raising_runtime_config,
    )
    scan_limit = await run_bounded_source_normalize(
        _approved_config(scan_limit=101),
        runtime_config_loader=_raising_runtime_config,
    )

    assert max_messages.error_code == "max_messages_must_be_one"
    assert max_messages.state.runtime_config_loaded is False
    assert scan_limit.error_code == "scan_limit_out_of_range"
    assert scan_limit.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_redis_thin_payload_with_raw_payload_fields_is_rejected_without_leakage() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    redis = FakeRedisClient(
        [
            (
                STREAM_ID,
                _thin_fields(
                    event_id,
                    source_message_id,
                    payload_json={"private": RAW_TEXT},
                    message_text=RAW_TEXT,
                ),
            )
        ]
    )

    result = await run_bounded_source_normalize(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=RaisingDatabaseBuilder(),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "redis_message_contract_invalid"
    assert result.messages_matched == 1
    assert result.messages_processed_count == 0
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert redis.group_destroyed is True
    assert RAW_TEXT not in rendered
    assert str(event_id) not in rendered
    assert str(source_message_id) not in rendered
    assert STREAM_ID not in rendered


@pytest.mark.asyncio
async def test_non_target_message_is_not_processed_or_acked() -> None:
    target_event_id = uuid4()
    source_message_id = uuid4()
    redis = FakeRedisClient(
        [
            (
                "100-0",
                _thin_fields(uuid4(), source_message_id),
            )
        ]
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_source_normalize(
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
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_stage_and_root_contract_mismatches_block_before_processing_or_ack() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    for overrides, expected_error in (
        ({"stage_name": "enrich"}, "stage_not_allowed"),
        ({"root_object_type": "artifact"}, "root_object_type_not_allowed"),
    ):
        redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, source_message_id, **overrides))])
        database_builder = RaisingDatabaseBuilder()

        result = await run_bounded_source_normalize(
            _approved_config(trigger_event_id=event_id),
            runtime_config_loader=_runtime_config,
            redis_builder=FakeRedisBuilder(redis),
            database_builder=database_builder,
        )

        assert result.error_code == expected_error
        assert result.messages_matched == 1
        assert result.messages_processed_count == 0
        assert result.state.redis_ack_attempted is False
        assert result.state.database_write_attempted is False
        assert redis.acked == []
        assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_matching_job_rehydrates_event_outbox_and_source_message_through_service_once() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    redis_root_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, source_message_id),
        snapshot=_snapshot(source_message_id, text="AI"),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, redis_root_id))])
    database_builder = FakeDatabaseBuilder(repository)

    result = await run_bounded_source_normalize(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.ok is True
    assert repository.requested_event_ids == [event_id]
    assert repository.requested_source_ids == [source_message_id]
    assert len(repository.normalization_runs) == 1
    assert result.candidate_eligible is False
    assert result.signal_detected is True
    assert result.counters.normalization_runs_written_count == 1
    assert result.counters.suppression_traces_written_count == 1
    assert result.redis_acked_count == 1
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_suppressed_message_writes_run_and_suppression_trace_then_acks_after_commit() -> None:
    order: list[str] = []
    event_id = uuid4()
    source_message_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, source_message_id),
        snapshot=_snapshot(source_message_id, text="AI"),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, source_message_id))], order=order)
    database_builder = FakeDatabaseBuilder(repository, order=order)

    result = await run_bounded_source_normalize(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.ok is True
    assert result.counters.normalization_runs_written_count == 1
    assert result.counters.suppression_traces_written_count == 1
    assert result.counters.artifacts_upserted_count == 0
    assert result.counters.candidate_groups_upserted_count == 0
    assert order == ["db:commit", "redis:ack"]
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_candidate_eligible_message_writes_artifacts_candidate_data_and_acks_after_commit() -> None:
    order: list[str] = []
    event_id = uuid4()
    source_message_id = uuid4()
    text = "New developer SDK https://github.com/example/example-tool"
    repository = FakeRepository(
        event=_event(event_id, source_message_id),
        snapshot=_snapshot(source_message_id, text=text),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, source_message_id))], order=order)
    database_builder = FakeDatabaseBuilder(repository, order=order)

    result = await run_bounded_source_normalize(
        _approved_config(source_message_id=source_message_id, trigger_event_id=None),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.ok is True
    assert result.candidate_eligible is True
    assert result.counters.normalization_runs_written_count == 1
    assert result.counters.artifacts_upserted_count == 1
    assert result.counters.artifact_observations_written_count == 1
    assert result.counters.candidate_groups_upserted_count == 1
    assert result.counters.candidate_members_written_count == 1
    assert result.counters.enrich_outbox_inserted_count == 1
    assert len(repository.enrich_events) == 1
    assert order == ["db:commit", "redis:ack"]
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_db_source_message_missing_returns_source_missing_without_ack_or_write_claim() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, source_message_id),
        snapshot=_snapshot(uuid4(), text="AI"),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, source_message_id))])
    database_builder = FakeDatabaseBuilder(repository)

    result = await run_bounded_source_normalize(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.ok is False
    assert result.error_code == "source_message_missing"
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.counters.normalization_runs_written_count == 0
    assert database_builder.close_commits == [False]
    assert redis.acked == []


@pytest.mark.asyncio
async def test_redis_ack_failure_returns_sanitized_json_and_does_not_claim_ok() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, source_message_id),
        snapshot=_snapshot(source_message_id, text="AI"),
    )
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(event_id, source_message_id))],
        ack_error=RuntimeError(RAW_EXCEPTION_DETAIL),
    )

    result = await run_bounded_source_normalize(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "redis_ack_failed"
    assert result.error_class == "RuntimeError"
    assert result.state.redis_ack_attempted is True
    assert result.redis_acked_count == 0
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered
    assert RAW_TEXT not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered


@pytest.mark.asyncio
async def test_db_commit_failure_returns_sanitized_json_and_does_not_ack() -> None:
    event_id = uuid4()
    source_message_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, source_message_id),
        snapshot=_snapshot(source_message_id, text="AI"),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, source_message_id))])

    result = await run_bounded_source_normalize(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(
            repository,
            close_error=RuntimeError("private database commit failure detail"),
        ),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "database_write_failed"
    assert result.error_class == "RuntimeError"
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert "private database commit failure detail" not in rendered
    assert RAW_TEXT not in rendered


def test_source_ast_guard_has_no_forbidden_authority_or_broad_worker() -> None:
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

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(
        imported_roots
    )
    assert "urllib.request" not in imported_modules
    assert "urlopen" not in call_names
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert not any(".worker" in module for module in imported_modules)
    assert "run_forever" not in call_attrs
    assert "system" not in call_attrs
    assert "popen" not in call_attrs
    assert "xdel" not in call_attrs
