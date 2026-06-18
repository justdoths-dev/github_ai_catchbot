from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

from src.services.gh_enricher.bounded_github_enrich_runner import (
    BoundedGithubEnrichCounters,
    CountingGhEnricherRepository,
)
from src.services.gh_enricher.repositories import GhEnricherRepository


class FakeExecuteResult:
    def __init__(self, row: UUID | str | None) -> None:
        self.row = row

    def scalar_one_or_none(self) -> UUID | str | None:
        return self.row


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
