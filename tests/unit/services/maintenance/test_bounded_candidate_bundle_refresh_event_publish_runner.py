from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.analysis_router.repositories import BundleRefreshOutboxRecord
from src.services.maintenance.bounded_candidate_bundle_refresh_event_publish_runner import (
    BoundedCandidateBundleRefreshEventPublishConfig,
    CandidateBundleRefreshEventRepositoryHandle,
    CandidateCurrentBundleState,
    CONFIRM_TOKEN,
    RefreshEventReference,
    run_bounded_candidate_bundle_refresh_event_publish,
)
from src.services.outbox_relay.bounded_candidate_bundle_refresh_outbox_publish_runner import (
    BoundedCandidateBundleRefreshOutboxPublishConfig,
    BoundedCandidateBundleRefreshOutboxPublishResult,
    BoundedCandidateBundleRefreshOutboxPublishState,
    BoundedCandidateBundleRefreshPublishRuntimeConfig,
)


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/bounded_candidate_bundle_refresh_event_publish_runner.py"
DB_LOCATOR = "db_locator_omitted_sentinel"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
RAW_DEDUPE_KEY = "bundle-refresh:private-dedupe-key"
RAW_REFRESH_REASON = "secret_reason"
RAW_PAYLOAD = "payload_json_private"
EXCEPTION_DETAIL = "private exception detail should not render"


class FakeRepository:
    def __init__(
        self,
        *,
        candidate: CandidateCurrentBundleState | None,
        suffix_matches: list[UUID] | None = None,
        existing_event: RefreshEventReference | None = None,
        inserted_record: BundleRefreshOutboxRecord | None = None,
        operation_log: list[str] | None = None,
    ) -> None:
        self.candidate = candidate
        self.suffix_matches = suffix_matches
        self.existing_event = existing_event
        self.inserted_record = inserted_record
        self.operation_log = operation_log if operation_log is not None else []
        self.suffix_calls: list[dict[str, object]] = []
        self.load_candidate_calls: list[UUID] = []
        self.load_existing_calls: list[dict[str, object]] = []
        self.insert_calls: list[dict[str, object]] = []

    async def find_candidate_groups_by_suffix(self, suffix: str, *, limit: int) -> list[UUID]:
        self.operation_log.append("find_suffix")
        self.suffix_calls.append({"suffix": suffix, "limit": limit})
        if self.suffix_matches is not None:
            return list(self.suffix_matches)
        if self.candidate is None:
            return []
        return [self.candidate.candidate_group_id]

    async def load_candidate_current_bundle(self, candidate_group_id: UUID):
        self.operation_log.append("load_candidate")
        self.load_candidate_calls.append(candidate_group_id)
        return self.candidate

    async def load_matching_refresh_event(self, *, candidate_group_id, bundle_id, refresh_reason):
        self.operation_log.append("load_existing")
        self.load_existing_calls.append(
            {
                "candidate_group_id": candidate_group_id,
                "bundle_id": bundle_id,
                "refresh_reason": refresh_reason,
            }
        )
        return self.existing_event

    async def insert_bundle_refresh_outbox(self, *, candidate_group_id, bundle_id, refresh_reason):
        self.operation_log.append("insert_refresh")
        self.insert_calls.append(
            {
                "candidate_group_id": candidate_group_id,
                "bundle_id": bundle_id,
                "refresh_reason": refresh_reason,
            }
        )
        if self.inserted_record is None:
            return BundleRefreshOutboxRecord(event_id=uuid4(), created=True, status="pending")
        return self.inserted_record


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository, *, operation_log: list[str] | None = None) -> None:
        self.repository = repository
        self.operation_log = operation_log if operation_log is not None else []
        self.calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.operation_log.append(f"close:{commit}")
            self.close_commits.append(commit)

        return CandidateBundleRefreshEventRepositoryHandle(repository=self.repository, close=close)


class FakePublisherRunner:
    def __init__(
        self,
        *,
        status: str = "published",
        error_code: str | None = None,
        error_class: str | None = None,
        operation_log: list[str] | None = None,
    ) -> None:
        self.status = status
        self.error_code = error_code
        self.error_class = error_class
        self.operation_log = operation_log if operation_log is not None else []
        self.calls: list[BoundedCandidateBundleRefreshOutboxPublishConfig] = []

    async def __call__(self, config: BoundedCandidateBundleRefreshOutboxPublishConfig, **kwargs):
        self.operation_log.append("publisher")
        self.calls.append(config)
        state = BoundedCandidateBundleRefreshOutboxPublishState()
        state.redis_publish_attempted = True
        state.event_outbox_status_write_attempted = True
        state.job_attempt_insert_attempted = self.status == "published"
        return BoundedCandidateBundleRefreshOutboxPublishResult(
            status=self.status,
            ok=self.status == "published" and self.error_code is None,
            error_code=self.error_code,
            error_class=self.error_class,
            config=config,
            state=state,
            target_event_id_suffix=None if config.event_id is None else str(config.event_id)[-8:],
            events_seen=1,
            events_published_count=1 if state.redis_publish_attempted else 0,
            job_attempts_inserted_count=1 if self.status == "published" else 0,
            queue_name="q.candidate.bundle",
            stage_name="bundle",
            event_outbox_marked_published=self.status == "published",
        )


def _runtime_config() -> BoundedCandidateBundleRefreshPublishRuntimeConfig:
    return BoundedCandidateBundleRefreshPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def _raising_runtime_config() -> BoundedCandidateBundleRefreshPublishRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _candidate(
    *,
    candidate_group_id: UUID | None = None,
    bundle_id: UUID | None = None,
    present: bool = True,
    ready: bool = True,
) -> CandidateCurrentBundleState:
    return CandidateCurrentBundleState(
        candidate_group_id=candidate_group_id or uuid4(),
        current_bundle_id=bundle_id,
        current_bundle_present=present,
        current_bundle_ready_for_analysis=ready,
    )


def _approved_config(**overrides) -> BoundedCandidateBundleRefreshEventPublishConfig:
    candidate_group_id = overrides.pop("candidate_group_id", uuid4())
    values = {
        "mode": "execute",
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "allow_database_write": True,
        "allow_redis_publish": True,
        "candidate_group_id": candidate_group_id,
        "candidate_group_suffix": None,
        "bundle_id": None,
        "refresh_reason": RAW_REFRESH_REASON,
        "confirm": CONFIRM_TOKEN,
    }
    values.update(overrides)
    return BoundedCandidateBundleRefreshEventPublishConfig(**values)


@pytest.mark.asyncio
async def test_missing_operator_approval_blocks_before_runtime_config() -> None:
    repository_builder = FakeRepositoryBuilder(FakeRepository(candidate=None))
    publisher = FakePublisherRunner()

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        BoundedCandidateBundleRefreshEventPublishConfig(refresh_reason=RAW_REFRESH_REASON),
        runtime_config_loader=_raising_runtime_config,
        repository_builder=repository_builder,
        publisher_runner=publisher,
    )

    assert result.reason_code == "operator_approval_missing"
    assert result.state.runtime_config_loaded is False
    assert repository_builder.calls == 0
    assert publisher.calls == []


@pytest.mark.asyncio
async def test_selector_missing_or_conflict_blocks_before_runtime_config() -> None:
    cases = [
        await run_bounded_candidate_bundle_refresh_event_publish(
            BoundedCandidateBundleRefreshEventPublishConfig(
                operator_approved=True,
                refresh_reason=RAW_REFRESH_REASON,
            ),
            runtime_config_loader=_raising_runtime_config,
        ),
        await run_bounded_candidate_bundle_refresh_event_publish(
            BoundedCandidateBundleRefreshEventPublishConfig(
                operator_approved=True,
                candidate_group_id=uuid4(),
                candidate_group_suffix="abcd1234",
                refresh_reason=RAW_REFRESH_REASON,
            ),
            runtime_config_loader=_raising_runtime_config,
        ),
    ]

    assert [case.reason_code for case in cases] == ["selector_missing", "selector_conflict"]
    assert all(case.state.runtime_config_loaded is False for case in cases)


@pytest.mark.asyncio
async def test_plan_reads_candidate_current_bundle_without_write_or_publish() -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=bundle_id))
    publisher = FakePublisherRunner()

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(
            mode="plan",
            candidate_group_id=candidate_group_id,
            allow_database_write=False,
            allow_redis_publish=False,
            confirm=None,
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        publisher_runner=publisher,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "planned"
    assert report["database_read_attempted"] is True
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["current_bundle_present"] is True
    assert report["current_bundle_ready_for_analysis"] is True
    assert report["existing_refresh_event_status_bucket"] == "none"
    assert report["publish_would_be_attempted"] is False
    assert repository.insert_calls == []
    assert publisher.calls == []


@pytest.mark.asyncio
async def test_candidate_group_suffix_ambiguity_blocks() -> None:
    repository = FakeRepository(candidate=None, suffix_matches=[uuid4(), uuid4()])

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(
            mode="plan",
            candidate_group_id=None,
            candidate_group_suffix="abcd1234",
            allow_database_write=False,
            allow_redis_publish=False,
            confirm=None,
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )

    assert result.reason_code == "candidate_group_suffix_ambiguous"
    assert repository.suffix_calls == [{"suffix": "abcd1234", "limit": 2}]
    assert repository.load_candidate_calls == []


@pytest.mark.asyncio
async def test_current_bundle_missing_blocks() -> None:
    candidate_group_id = uuid4()
    repository = FakeRepository(candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=None, present=False))

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["reason_code"] == "current_bundle_missing"
    assert report["current_bundle_present"] is False
    assert report["publisher_attempted"] is False


@pytest.mark.asyncio
async def test_bundle_not_ready_blocks() -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=bundle_id, ready=False)
    )

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )

    assert result.reason_code == "bundle_not_ready"
    assert result.current_bundle_present is True
    assert result.current_bundle_ready_for_analysis is False
    assert repository.insert_calls == []


@pytest.mark.asyncio
async def test_explicit_bundle_mismatch_blocks_as_stale() -> None:
    candidate_group_id = uuid4()
    current_bundle_id = uuid4()
    explicit_bundle_id = uuid4()
    repository = FakeRepository(
        candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=current_bundle_id)
    )

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id, bundle_id=explicit_bundle_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )

    assert result.reason_code == "stale_bundle_request"
    assert result.bundle_id == explicit_bundle_id
    assert repository.insert_calls == []


@pytest.mark.asyncio
async def test_execute_creates_pending_refresh_event_then_delegates_to_existing_publisher() -> None:
    operation_log: list[str] = []
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event_id = uuid4()
    repository = FakeRepository(
        candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=bundle_id),
        inserted_record=BundleRefreshOutboxRecord(event_id=event_id, created=True, status="pending"),
        operation_log=operation_log,
    )
    repository_builder = FakeRepositoryBuilder(repository, operation_log=operation_log)
    publisher = FakePublisherRunner(operation_log=operation_log)

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        publisher_runner=publisher,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "published"
    assert report["refresh_event_created"] is True
    assert report["refresh_event_suffix"] == str(event_id)[-8:]
    assert report["queue_name"] == "q.candidate.bundle"
    assert report["stage_name"] == "bundle"
    assert report["event_outbox_marked_published"] is True
    assert report["events_published_count"] == 1
    assert report["job_attempts_inserted_count"] == 1
    assert repository.insert_calls == [
        {"candidate_group_id": candidate_group_id, "bundle_id": bundle_id, "refresh_reason": RAW_REFRESH_REASON}
    ]
    assert publisher.calls[0].event_id == event_id
    assert operation_log.index("close:True") < operation_log.index("publisher")


@pytest.mark.asyncio
async def test_execute_reuses_existing_pending_refresh_event_then_delegates_to_publisher() -> None:
    operation_log: list[str] = []
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event_id = uuid4()
    repository = FakeRepository(
        candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=bundle_id),
        existing_event=RefreshEventReference(event_id=event_id, status="pending"),
        operation_log=operation_log,
    )
    repository_builder = FakeRepositoryBuilder(repository, operation_log=operation_log)
    publisher = FakePublisherRunner(operation_log=operation_log)

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=repository_builder,
        publisher_runner=publisher,
    )

    assert result.ok is True
    assert result.refresh_event_created is False
    assert repository.insert_calls == []
    assert publisher.calls[0].event_id == event_id
    assert operation_log.index("close:False") < operation_log.index("publisher")


@pytest.mark.asyncio
async def test_execute_blocks_when_matching_event_already_published_or_non_pending() -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=bundle_id),
        existing_event=RefreshEventReference(event_id=uuid4(), status="published"),
    )
    publisher = FakePublisherRunner()

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        publisher_runner=publisher,
    )

    assert result.reason_code == "refresh_event_not_pending_for_reason"
    assert result.refresh_event_status_bucket == "published"
    assert repository.insert_calls == []
    assert publisher.calls == []


@pytest.mark.asyncio
async def test_existing_publisher_failure_is_propagated_and_sanitized() -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event_id = uuid4()
    repository = FakeRepository(
        candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=bundle_id),
        existing_event=RefreshEventReference(event_id=event_id, status="pending"),
    )
    publisher = FakePublisherRunner(
        status="failed",
        error_code="redis_xadd_failed",
        error_class="RuntimeError",
    )

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        publisher_runner=publisher,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.reason_code == "redis_xadd_failed"
    assert result.publisher_status == "failed"
    assert result.publisher_reason_code == "redis_xadd_failed"
    assert EXCEPTION_DETAIL not in rendered
    assert DB_LOCATOR not in rendered
    assert REDIS_LOCATOR not in rendered


@pytest.mark.asyncio
async def test_report_omits_full_ids_dedupe_reason_payload_urls_and_exception_detail() -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event_id = uuid4()
    repository = FakeRepository(
        candidate=_candidate(candidate_group_id=candidate_group_id, bundle_id=bundle_id),
        inserted_record=BundleRefreshOutboxRecord(event_id=event_id, created=True, status="pending"),
    )

    result = await run_bounded_candidate_bundle_refresh_event_publish(
        _approved_config(candidate_group_id=candidate_group_id),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        publisher_runner=FakePublisherRunner(),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    forbidden = (
        str(candidate_group_id),
        str(bundle_id),
        str(event_id),
        RAW_DEDUPE_KEY,
        RAW_REFRESH_REASON,
        RAW_PAYLOAD,
        DB_LOCATOR,
        REDIS_LOCATOR,
        EXCEPTION_DETAIL,
    )
    for raw in forbidden:
        assert raw not in rendered
    assert str(candidate_group_id)[-8:] in rendered
    assert str(bundle_id)[-8:] in rendered
    assert str(event_id)[-8:] in rendered


def test_source_ast_guard_has_no_forbidden_providers_consumers_or_broad_workers() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules: set[str] = set()
    imported_roots: set[str] = set()
    forbidden_call_names = {
        "system",
        "popen",
        "call",
        "check_call",
        "check_output",
        "run_forever",
    }
    forbidden_call_attrs = forbidden_call_names | {
        "xreadgroup",
        "xread",
        "xack",
        "ack",
        "consume",
        "run_forever",
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
    assert not any("evidence_assembler" in module for module in imported_modules)
    assert not any("judge_openai" in module for module in imported_modules)
    assert not any("notifier_telegram" in module for module in imported_modules)
    assert not any("gh_enricher" in module for module in imported_modules)
    assert not any("x_enricher" in module for module in imported_modules)
    assert not any("web_enricher" in module for module in imported_modules)
    assert "OutboxRelayService" not in source
    assert "run_forever(" not in source
