from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from services.maintenance.repositories import MaintenanceRepository
from tests.component.services.maintenance._batch_recovery_fakes import AtomicReplayInsertSession


ROOT = Path(__file__).resolve().parents[4]


def test_replay_insert_sql_distincts_source_plan_ids_and_checks_open_requests() -> None:
    text = (ROOT / "src" / "services" / "maintenance" / "repositories.py").read_text(encoding="utf-8")

    assert "SELECT DISTINCT unnest(CAST(:plan_ids AS uuid[])) AS notification_plan_id" in text
    assert "rr.status IN ('requested', 'dispatched', 'pending')" in text


@pytest.mark.asyncio
async def test_repository_atomically_inserts_exact_replay_requested_outbox_contract() -> None:
    plan_id = uuid4()
    session = AtomicReplayInsertSession()
    repository = MaintenanceRepository(session)

    async with repository.transaction():
        inserted_count = await repository.insert_replay_requests_for_selected_plans(
            plan_ids=[plan_id],
            requested_by="test/operator",
        )

    assert inserted_count == 1
    assert len(session.committed_replay_requests) == 1
    assert len(session.committed_replay_events) == 1
    assert session.rolled_back is False
    assert session.params == {"plan_ids": [str(plan_id)], "requested_by": "test/operator"}
    assert "INSERT INTO replay_requests" in session.statement
    assert "INSERT INTO event_outbox" in session.statement
    assert "'replay.requested.v1'" in session.statement
    assert "'replay_request'" in session.statement
    assert "'maintenance:replay-requested:v1:' || replay_request_id::text" in session.statement
    assert "'replay_request_id', replay_request_id::text" in session.statement
    assert "'replay_type', 'delivery'" in session.statement
    assert "'root_object_type', 'notification_plan'" in session.statement
    assert "'root_object_id', root_object_id::text" in session.statement
    assert "'replay_reason', 'batch_recovery_replay_selected'" in session.statement
    assert "'pending'::outbox_status_enum" in session.statement
    assert "ON CONFLICT ON CONSTRAINT uq_event_outbox_dedupe_key DO NOTHING" in session.statement


@pytest.mark.asyncio
async def test_replay_event_count_mismatch_rolls_back_without_orphan_request() -> None:
    session = AtomicReplayInsertSession(replay_event_count_override=0)
    repository = MaintenanceRepository(session)
    success_result_returned = False

    with pytest.raises(RuntimeError, match="^replay_request_outbox_atomicity_mismatch$"):
        async with repository.transaction():
            await repository.insert_replay_requests_for_selected_plans(
                plan_ids=[uuid4()],
                requested_by="test/operator",
            )
            success_result_returned = True

    assert success_result_returned is False
    assert session.rolled_back is True
    assert session.pending_replay_requests == []
    assert session.pending_replay_events == []
    assert session.committed_replay_requests == []
    assert session.committed_replay_events == []


def test_batch_recovery_does_not_mutate_notifier_owned_tables() -> None:
    for relative_path in [
        "src/services/maintenance/batch_recovery_tool.py",
        "src/services/maintenance/repositories.py",
    ]:
        text = (ROOT / relative_path).read_text(encoding="utf-8").lower()
        assert "update notification_plans" not in text
        assert "delete from notification_plans" not in text
        assert "insert into notification_plans" not in text
        assert "update notification_delivery_records" not in text
        assert "update notification_renders" not in text
        assert "insert into state_transitions" not in text
