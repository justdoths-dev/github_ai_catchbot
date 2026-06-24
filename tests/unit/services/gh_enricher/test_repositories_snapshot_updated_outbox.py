from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from src.services.gh_enricher.bounded_github_enrich_runner import (
    BoundedGithubEnrichCounters,
    CountingGhEnricherRepository,
)
from src.services.gh_enricher.repositories import GhEnricherRepository


class FakeExecuteResult:
    def __init__(
        self,
        row: UUID | str | None = None,
        *,
        mapping_row: dict[str, Any] | None = None,
        mapping_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.row = row
        self.mapping_row = mapping_row
        self.mapping_rows = mapping_rows

    def scalar_one_or_none(self) -> UUID | str | None:
        return self.row

    def scalar_one(self) -> UUID | str:
        assert self.row is not None
        return self.row

    def mappings(self) -> "FakeExecuteResult":
        return self

    def first(self) -> dict[str, Any] | None:
        if self.mapping_row is not None:
            return self.mapping_row
        if self.mapping_rows:
            return self.mapping_rows[0]
        return None

    def all(self) -> list[dict[str, Any]]:
        if self.mapping_rows is not None:
            return self.mapping_rows
        if self.mapping_row is not None:
            return [self.mapping_row]
        return []


class FakeAsyncSession:
    def __init__(self, result: FakeExecuteResult) -> None:
        self.result = result
        self.execute_calls: list[tuple[Any, dict[str, Any] | None]] = []

    def in_transaction(self) -> bool:
        return False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeExecuteResult:
        self.execute_calls.append((statement, params))
        return self.result


@pytest.mark.asyncio
async def test_insert_snapshot_updated_outbox_returns_returning_event_id() -> None:
    artifact_id = uuid4()
    snapshot_id = uuid4()
    event_id = uuid4()
    session = FakeAsyncSession(FakeExecuteResult(str(event_id)))
    repository = GhEnricherRepository(session)

    result = await repository.insert_snapshot_updated_outbox(
        artifact_id=artifact_id,
        snapshot_id=snapshot_id,
        status="ready",
        content_anchor="commit:" + "a" * 40,
    )

    assert result == event_id
    assert len(session.execute_calls) == 1
    statement, params = session.execute_calls[0]
    assert "RETURNING event_id" in str(statement)
    assert params is not None
    assert params["dedupe_key"] == f"artifact:snapshot_updated:{artifact_id}:{snapshot_id}"


@pytest.mark.asyncio
async def test_insert_snapshot_updated_outbox_returns_none_on_conflict() -> None:
    session = FakeAsyncSession(FakeExecuteResult(None))
    repository = GhEnricherRepository(session)

    result = await repository.insert_snapshot_updated_outbox(
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        status="ready",
        content_anchor="commit:" + "a" * 40,
    )

    assert result is None
    assert len(session.execute_calls) == 1


@pytest.mark.asyncio
async def test_claim_failed_transient_enrichment_run_for_retry_fetching_state() -> None:
    run_id = uuid4()
    session = FakeAsyncSession(FakeExecuteResult(str(run_id)))
    repository = GhEnricherRepository(session)

    result = await repository.claim_failed_transient_enrichment_run_for_retry(
        job_idempotency_key="enrich:github:artifact:hash",
    )

    assert result == run_id
    assert len(session.execute_calls) == 1
    statement, params = session.execute_calls[0]
    rendered = str(statement)
    assert "status = 'fetching'::snapshot_status_enum" in rendered
    assert "finished_at = NULL" in rendered
    assert "status = 'failed_transient'::snapshot_status_enum" in rendered
    assert "RETURNING artifact_enrichment_run_id" in rendered
    assert params == {"job_idempotency_key": "enrich:github:artifact:hash"}


@pytest.mark.asyncio
async def test_load_enrichment_run_status_by_job_idempotency_key_returns_status() -> None:
    run_id = uuid4()
    session = FakeAsyncSession(
        FakeExecuteResult(
            mapping_row={
                "artifact_enrichment_run_id": str(run_id),
                "status": "fetching",
            }
        )
    )
    repository = GhEnricherRepository(session)

    result = await repository.load_enrichment_run_status_by_job_idempotency_key(
        job_idempotency_key="enrich:github:artifact:hash",
    )

    assert result == "fetching"
    assert len(session.execute_calls) == 1
    statement, params = session.execute_calls[0]
    assert "FROM artifact_enrichment_runs" in str(statement)
    assert params == {"job_idempotency_key": "enrich:github:artifact:hash"}


@pytest.mark.asyncio
async def test_load_enrichment_run_by_job_idempotency_key_returns_run_ref() -> None:
    run_id = uuid4()
    session = FakeAsyncSession(
        FakeExecuteResult(
            mapping_row={
                "artifact_enrichment_run_id": str(run_id),
                "status": "pending",
            }
        )
    )
    repository = GhEnricherRepository(session)

    result = await repository.load_enrichment_run_by_job_idempotency_key(
        job_idempotency_key="enrich:github:artifact:hash",
    )

    assert result is not None
    assert result.run_id == run_id
    assert result.status == "pending"
    assert len(session.execute_calls) == 1
    statement, params = session.execute_calls[0]
    rendered = str(statement)
    assert "artifact_enrichment_run_id" in rendered
    assert "ORDER BY requested_at ASC" in rendered
    assert params == {"job_idempotency_key": "enrich:github:artifact:hash"}


@pytest.mark.asyncio
async def test_load_valid_orphan_provider_snapshots_limits_to_two_usable_github_snapshots() -> None:
    artifact_id = uuid4()
    snapshot_id = uuid4()
    fetched_at = datetime.now(timezone.utc)
    session = FakeAsyncSession(
        FakeExecuteResult(
            mapping_rows=[
                {
                    "snapshot_id": str(snapshot_id),
                    "status": "ready",
                    "fetched_at": fetched_at,
                    "content_anchor": "commit:abc123",
                    "normalized_projection": {"repo_full_name": "example/project"},
                }
            ]
        )
    )
    repository = GhEnricherRepository(session)

    result = await repository.load_valid_orphan_provider_snapshots(
        artifact_id=artifact_id,
        provider="github",
        limit=2,
    )

    assert len(result) == 1
    assert result[0].snapshot_id == snapshot_id
    assert result[0].status == "ready"
    assert result[0].content_anchor == "commit:abc123"
    assert len(session.execute_calls) == 1
    statement, params = session.execute_calls[0]
    rendered = str(statement)
    assert "FROM artifact_snapshots" in rendered
    assert "snapshot_type IN" in rendered
    assert "status IN" in rendered
    assert "LIMIT :limit" in rendered
    assert params == {"artifact_id": str(artifact_id), "provider": "github", "limit": 2}


@pytest.mark.asyncio
async def test_counting_repository_records_snapshot_updated_event_suffix_on_insert() -> None:
    event_id = uuid4()
    counters = BoundedGithubEnrichCounters()
    repository = CountingGhEnricherRepository(
        GhEnricherRepository(FakeAsyncSession(FakeExecuteResult(event_id))),
        counters,
    )

    result = await repository.insert_snapshot_updated_outbox(
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        status="ready",
        content_anchor="commit:" + "a" * 40,
    )

    assert result == event_id
    assert counters.snapshot_updated_outbox_inserted_count == 1
    assert counters.artifact_snapshot_updated_event_suffixes == [str(event_id)[-8:]]


@pytest.mark.asyncio
async def test_counting_repository_does_not_count_snapshot_updated_conflict() -> None:
    counters = BoundedGithubEnrichCounters()
    repository = CountingGhEnricherRepository(
        GhEnricherRepository(FakeAsyncSession(FakeExecuteResult(None))),
        counters,
    )

    result = await repository.insert_snapshot_updated_outbox(
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        status="ready",
        content_anchor="commit:" + "a" * 40,
    )

    assert result is None
    assert counters.snapshot_updated_outbox_inserted_count == 0
    assert counters.artifact_snapshot_updated_event_suffixes == []
