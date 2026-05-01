from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ROLLOUT_SQL = ROOT / "ops" / "delivery" / "sql" / "delivery_rollout_gate_queries.sql"
DASHBOARD = ROOT / "ops" / "delivery" / "dashboards" / "delivery_minimal_dashboard.md"


def test_restricted_gate_has_open_delivery_dlq_scorecard_input() -> None:
    text = ROLLOUT_SQL.read_text(encoding="utf-8")

    assert "-- query: restricted_open_delivery_dlq_count" in text
    assert "FROM dead_letter_entries dle" in text
    assert "dle.root_object_type = 'notification_plan'" in text
    assert "open_delivery_dlq_count" in text


def test_restricted_rollout_doc_fails_on_unexplained_delivery_dlq() -> None:
    text = DASHBOARD.read_text(encoding="utf-8").lower()

    assert "restricted rollout can fail on any unexplained delivery dlq row" in text
