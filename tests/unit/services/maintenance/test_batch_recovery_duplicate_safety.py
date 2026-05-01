from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]


def test_replay_insert_sql_distincts_source_plan_ids_and_checks_open_requests() -> None:
    text = (ROOT / "src" / "services" / "maintenance" / "repositories.py").read_text(encoding="utf-8")

    assert "SELECT DISTINCT unnest(CAST(:plan_ids AS uuid[])) AS notification_plan_id" in text
    assert "rr.status IN ('requested', 'dispatched', 'pending')" in text


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
