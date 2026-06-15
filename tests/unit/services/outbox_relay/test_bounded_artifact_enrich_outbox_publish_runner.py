from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.outbox_relay.bounded_artifact_enrich_outbox_publish_runner import (
    BoundedArtifactEnrichOutboxPublishConfig,
    BoundedArtifactEnrichPublishRuntimeConfig,
    BoundedArtifactEnrichRedisPublisherHandle,
    BoundedArtifactEnrichRepositoryHandle,
    run_bounded_artifact_enrich_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/outbox_relay/bounded_artifact_enrich_outbox_publish_runner.py"
DB_LOCATOR = "db_locator_omitted_sentinel"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
RAW_DEDUPE_KEY = "artifact:enrich:sentinel-dedupe-key"
RAW_URL = "https://example.invalid/sentinel-article-url"
RAW_ARTICLE_TEXT = "sentinel private article text"
REDIS_MESSAGE_ID = "secret-artifact-enrich-redis-message-id"
EXCEPTION_DETAIL = "sentinel private xadd failure detail"
CLOSE_EXCEPTION_DETAIL = "sentinel private repository close detail"


class FakeRepository:
    def __init__(
        self,
        rows: list[OutboxEventRow] | None = None,
        *,
        operation_log: list[str] | None = None,
        fail_mark_published: bool = False,
        fail_insert_job_attempt: bool = False,
    ) -> None:
        self.rows = rows if rows is not None else []
        self.operation_log = operation_log if operation_log is not None else []
        self.fail_mark_published = fail_mark_published
        self.fail_insert_job_attempt = fail_insert_job_attempt
        self.fetch_calls: list[dict] = []
        self.mark_published_calls: list[UUID] = []
        self.job_attempt_calls: list[dict] = []

    async def fetch_target_events(self, *, event_id, artifact_id, event_suffix, limit):
        self.operation_log.append("fetch")
        self.fetch_calls.append(
            {
                "event_id": event_id,
                "artifact_id": artifact_id,
                "event_suffix": event_suffix,
                "limit": limit,
            }
        )
        return self.rows[:limit]

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        del published_at
        self.operation_log.append("mark_published")
        if self.fail_mark_published:
            raise RuntimeError("sentinel mark published detail")
        self.mark_published_calls.append(event_id)

    async def insert_job_attempt(self, **kwargs) -> None:
        self.operation_log.append("insert_job_attempt")
        if self.fail_insert_job_attempt:
            raise RuntimeError("sentinel job attempt detail")
        self.job_attempt_calls.append(kwargs)


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository, *, close_error: BaseException | None = None) -> None:
        self.repository = repository
        self.close_error = close_error
        self.calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.close_error is not None:
                raise self.close_error

        return BoundedArtifactEnrichRepositoryHandle(repository=self.repository, close=close)


class FakeRedisPublisher:
    def __init__(
        self,
        *,
        operation_log: list[str] | None = None,
        message_id: str = REDIS_MESSAGE_ID,
        failure: BaseException | None = None,
    ) -> None:
        self.operation_log = operation_log if operation_log is not None else []
        self.message_id = message_id
        self.failure = failure
        self.publish_calls: list[tuple[object, object]] = []

    async def publish(self, route, message) -> str:
        self.operation_log.append("publish")
        self.publish_calls.append((route, message))
        if self.failure is not None:
            raise self.failure
        return self.message_id


class FakeRedisPublisherBuilder:
    def __init__(self, publisher: FakeRedisPublisher) -> None:
        self.publisher = publisher
        self.calls = 0
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_publisher_created = True

        async def close() -> None:
            self.close_calls += 1

        return BoundedArtifactEnrichRedisPublisherHandle(publisher=self.publisher, close=close)


class RogueRouteResolver:
    def resolve(self, row):
        del row
        return QueueRoute("q.notification.send", "notify")


def _runtime_config() -> BoundedArtifactEnrichPublishRuntimeConfig:
    return BoundedArtifactEnrichPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _raising_runtime_config() -> BoundedArtifactEnrichPublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _payload(artifact_id: UUID, *, provider_route: str = "web") -> dict[str, object]:
    return {
        "candidate_group_id": str(uuid4()),
        "artifact_id": str(artifact_id),
        "artifact_type": "web_article",
        "canonical_id": RAW_URL,
        "provider_route": provider_route,
        "refresh_mode": "standard",
        "depth_budget": 1,
        "article_text": RAW_ARTICLE_TEXT,
    }


def _row(
    *,
    event_id: UUID | None = None,
    artifact_id: UUID | None = None,
    event_type: str = "artifact.enrich.requested.v1",
    aggregate_type: str = "artifact",
    status: str = "pending",
    provider_route: str = "web",
    payload_json: dict[str, object] | None = None,
) -> OutboxEventRow:
    aggregate_id = artifact_id or uuid4()
    return OutboxEventRow(
        event_id=event_id or uuid4(),
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        dedupe_key=RAW_DEDUPE_KEY,
        payload_json=payload_json if payload_json is not None else _payload(aggregate_id, provider_route=provider_route),
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _approved_config(**overrides) -> BoundedArtifactEnrichOutboxPublishConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_publish": True,
        "allow_database_write": True,
        "event_id": uuid4(),
        "artifact_id": None,
        "event_suffix": None,
        "max_events": 1,
    }
    values.update(overrides)
    return BoundedArtifactEnrichOutboxPublishConfig(**values)


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_redis_or_db_write() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository([_row()]))
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_artifact_enrich_outbox_publish(
        BoundedArtifactEnrichOutboxPublishConfig(),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["redis_publish_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["side_effects"]["redis_mutation"] is False
    assert report["side_effects"]["db_write"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_or_conflicting_target_fails_before_runtime_config() -> None:
    missing = await run_bounded_artifact_enrich_outbox_publish(
        BoundedArtifactEnrichOutboxPublishConfig(operator_approved=True),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    conflict = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=uuid4(), artifact_id=uuid4()),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert missing.error_code == "target_missing"
    assert missing.state.runtime_config_loaded is False
    assert conflict.error_code == "target_conflict"
    assert conflict.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_missing_redis_or_database_gate_blocks_before_runtime_config() -> None:
    no_db = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(allow_database_write=False),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    no_redis = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(allow_redis_publish=False),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert no_db.error_code == "database_write_not_allowed"
    assert no_db.state.runtime_config_loaded is False
    assert no_redis.error_code == "redis_publish_not_allowed"
    assert no_redis.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_max_events_hard_max_one_fails_closed() -> None:
    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(max_events=2),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row()])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.error_code == "max_events_must_be_one"
    assert result.state.runtime_config_loaded is False


@pytest.mark.asyncio
async def test_event_suffix_selector_uses_db_lookup_with_uniqueness_probe() -> None:
    row = _row()
    repository = FakeRepository([row])

    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=None, event_suffix=str(row.event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert result.ok is True
    assert result.selector_type == "event_suffix"
    assert repository.fetch_calls == [
        {"event_id": None, "artifact_id": None, "event_suffix": str(row.event_id)[-8:], "limit": 2}
    ]


@pytest.mark.asyncio
async def test_duplicate_suffix_or_artifact_target_blocks_before_publish() -> None:
    row_one = _row()
    row_two = _row()
    repository = FakeRepository([row_one, row_two])
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=None, artifact_id=row_one.aggregate_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        redis_publisher_builder=publisher_builder,
    )

    assert result.error_code == "target_event_count_exceeded"
    assert result.events_seen == 2
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_non_pending_or_wrong_contract_blocks_without_publish_or_db_write() -> None:
    wrong_event = _row(event_type="source_message.created.v1", aggregate_type="source_message")
    not_pending = _row(status="published")

    first = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=wrong_event.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([wrong_event])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    second = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=not_pending.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([not_pending])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert first.error_code == "artifact_enrich_event_contract_mismatch"
    assert first.state.redis_publish_attempted is False
    assert first.state.database_write_attempted is False
    assert second.error_code == "target_event_not_pending"
    assert second.state.redis_publish_attempted is False
    assert second.state.database_write_attempted is False


@pytest.mark.asyncio
async def test_malformed_payload_provider_and_artifact_mismatch_block_before_publish() -> None:
    artifact_id = uuid4()
    malformed_payload = _payload(artifact_id)
    malformed_payload.pop("depth_budget")
    bad_provider = _row(provider_route="email")
    mismatched = _row(payload_json=_payload(uuid4()))

    malformed = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=uuid4()),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([_row(artifact_id=artifact_id, payload_json=malformed_payload)])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    provider = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=bad_provider.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([bad_provider])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    artifact = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=mismatched.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([mismatched])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert malformed.error_code == "malformed_event_payload"
    assert malformed.payload_has_depth_budget is False
    assert provider.error_code == "provider_route_not_allowed"
    assert artifact.error_code == "artifact_id_mismatch"
    assert malformed.state.redis_publish_attempted is False
    assert provider.state.redis_publish_attempted is False
    assert artifact.state.redis_publish_attempted is False


@pytest.mark.asyncio
async def test_web_artifact_publish_uses_existing_route_and_thin_id_only_payload() -> None:
    operation_log: list[str] = []
    artifact_id = uuid4()
    row = _row(artifact_id=artifact_id, provider_route="web")
    repository = FakeRepository([row], operation_log=operation_log)
    publisher = FakeRedisPublisher(operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)

    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "published"
    assert report["ok"] is True
    assert report["queue_name"] == "q.artifact.enrich.web"
    assert report["stage_name"] == "enrich_web"
    assert report["selected_provider_route"] == "web"
    assert report["target_event_id_suffix"] == str(row.event_id)[-8:]
    assert report["target_artifact_id_suffix"] == str(artifact_id)[-8:]
    assert report["events_seen"] == 1
    assert report["events_published_count"] == 1
    assert report["job_attempts_inserted_count"] == 1
    assert report["redis_publish_attempted"] is True
    assert report["database_write_attempted"] is True
    assert report["event_outbox_marked_published"] is True
    assert operation_log == ["fetch", "publish", "mark_published", "insert_job_attempt"]
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls == [
        {
            "stage_name": "enrich_web",
            "queue_name": "q.artifact.enrich.web",
            "root_object_type": "artifact",
            "root_object_id": artifact_id,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]

    assert len(publisher.publish_calls) == 1
    route, message = publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.artifact.enrich.web"
    assert route.stage_name == "enrich_web"
    assert fields == {
        "job_id": str(row.event_id),
        "stage_name": "enrich_web",
        "root_object_type": "artifact",
        "root_object_id": str(artifact_id),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }
    for forbidden in (
        "payload_json",
        "observed_url",
        "canonical_url",
        "canonical_id",
        "article_text",
        "source_text",
        "database_url",
        "redis_url",
    ):
        assert forbidden not in fields


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_route", "queue_name", "stage_name"),
    [
        ("github", "q.artifact.enrich.github", "enrich_github"),
        ("x", "q.artifact.enrich.x", "enrich_x"),
        ("web", "q.artifact.enrich.web", "enrich_web"),
    ],
)
async def test_allowed_provider_routes_resolve_through_outbox_route_contract(
    provider_route: str,
    queue_name: str,
    stage_name: str,
) -> None:
    row = _row(provider_route=provider_route)
    publisher = FakeRedisPublisher()

    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )

    assert result.ok is True
    assert result.queue_name == queue_name
    assert result.stage_name == stage_name
    assert publisher.publish_calls[0][0].queue_name == queue_name
    assert publisher.publish_calls[0][0].stage_name == stage_name


@pytest.mark.asyncio
async def test_custom_route_drift_blocks_before_publish() -> None:
    row = _row()
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())

    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=publisher_builder,
        route_resolver=RogueRouteResolver(),  # type: ignore[arg-type]
    )

    assert result.error_code == "route_not_allowed"
    assert result.state.redis_publish_attempted is False
    assert publisher_builder.calls == 0


@pytest.mark.asyncio
async def test_redis_publish_failure_does_not_mark_published_or_insert_job_attempt() -> None:
    operation_log: list[str] = []
    row = _row()
    repository = FakeRepository([row], operation_log=operation_log)
    repository_builder = FakeRepositoryBuilder(repository)
    publisher = FakeRedisPublisher(operation_log=operation_log, failure=RuntimeError(EXCEPTION_DETAIL))

    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "redis_publish_failed"
    assert result.error_class == "RuntimeError"
    assert result.state.redis_publish_attempted is True
    assert result.events_published_count == 0
    assert result.state.database_write_attempted is False
    assert result.event_outbox_marked_published is False
    assert result.job_attempts_inserted_count == 0
    assert repository.mark_published_calls == []
    assert repository.job_attempt_calls == []
    assert repository_builder.close_commits == [False]
    assert operation_log == ["fetch", "publish"]
    assert EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_commit_close_failure_after_xadd_returns_sanitized_failure() -> None:
    row = _row()
    repository = FakeRepository([row])
    repository_builder = FakeRepositoryBuilder(
        repository,
        close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL),
    )
    publisher = FakeRedisPublisher()

    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        redis_publisher_builder=FakeRedisPublisherBuilder(publisher),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert report["status"] == "failed"
    assert report["ok"] is False
    assert report["error_code"] == "repository_commit_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["redis_publish_attempted"] is True
    assert report["events_published_count"] == 1
    assert report["event_outbox_marked_published"] is True
    assert report["job_attempts_inserted_count"] == 1
    assert repository_builder.close_commits == [True]
    assert repository.mark_published_calls == [row.event_id]
    assert repository.job_attempt_calls[0]["attempt_status"] == "succeeded"
    assert len(publisher.publish_calls) == 1
    assert CLOSE_EXCEPTION_DETAIL not in rendered
    assert DB_LOCATOR not in rendered
    assert REDIS_LOCATOR not in rendered
    assert RAW_URL not in rendered
    assert RAW_ARTICLE_TEXT not in rendered
    assert RAW_DEDUPE_KEY not in rendered
    assert REDIS_MESSAGE_ID not in rendered


@pytest.mark.asyncio
async def test_sanitized_output_omits_full_ids_urls_payload_dedupe_redis_id_and_exception_detail() -> None:
    row = _row()
    result = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    for raw in (
        str(row.event_id),
        str(row.aggregate_id),
        DB_LOCATOR,
        REDIS_LOCATOR,
        RAW_URL,
        RAW_ARTICLE_TEXT,
        RAW_DEDUPE_KEY,
        REDIS_MESSAGE_ID,
    ):
        assert raw not in rendered
    assert rendered.count(str(row.event_id)[-8:]) == 1
    assert rendered.count(str(row.aggregate_id)[-8:]) == 1

    failing = await run_bounded_artifact_enrich_outbox_publish(
        _approved_config(event_id=row.event_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(FakeRepository([row])),
        redis_publisher_builder=FakeRedisPublisherBuilder(
            FakeRedisPublisher(failure=RuntimeError(EXCEPTION_DETAIL))
        ),
    )
    assert EXCEPTION_DETAIL not in json.dumps(failing.to_sanitized_dict(), sort_keys=True)


def test_source_ast_guard_has_no_broad_worker_or_forbidden_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
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
    forbidden_call_attrs = forbidden_call_names | {"sleep"}

    assert not any(isinstance(node, ast.While) for node in ast.walk(tree))
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
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "OutboxRelayService" not in source
    assert "run_forever(" not in source
