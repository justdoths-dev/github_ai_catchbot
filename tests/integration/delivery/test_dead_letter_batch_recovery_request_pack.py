from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OBSERVABILITY_SQL = ROOT / "ops" / "delivery" / "sql" / "delivery_observability_queries.sql"
BATCH_RUNBOOK = ROOT / "ops" / "delivery" / "runbooks" / "delivery_batch_recovery.md"


def test_batch_replay_request_skeleton_inserts_replay_requests_only() -> None:
    text = OBSERVABILITY_SQL.read_text(encoding="utf-8").lower()

    assert "-- query: batch_replay_request_insert_skeleton" in text
    assert "insert into replay_requests" in text
    assert "'delivery'::replay_type_enum" in text
    assert "'notification_plan'" in text
    assert ":notification_plan_ids" in text
    assert "insert into notification_plans" not in text
    assert "event_outbox" not in text


def test_batch_recovery_runbook_uses_allowed_bridges_and_forbids_reset() -> None:
    text = BATCH_RUNBOOK.read_text(encoding="utf-8").lower()

    assert "`replay_requests` with `replay_type = delivery`" in text
    assert "`event_outbox` bridge using `notification.plan.created.v1`" in text
    assert "must never reset" in text
    assert "do not reset `notification_plans.status`" in text
