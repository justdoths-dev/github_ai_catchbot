from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ROLLOUT_SQL = ROOT / "ops" / "delivery" / "sql" / "delivery_rollout_gate_queries.sql"
DASHBOARD = ROOT / "ops" / "delivery" / "dashboards" / "delivery_minimal_dashboard.md"


def test_full_gate_has_send_disabled_suppress_scorecard_input() -> None:
    text = ROLLOUT_SQL.read_text(encoding="utf-8")

    assert "-- query: restricted_unexpected_send_disabled_suppress_count" in text
    assert "unexpected_send_disabled_suppress_count" in text
    assert "telegram_response_json ->> 'send_disabled' = 'true'" in text
    assert "'suppressed'::notification_status_enum" in text


def test_full_rollout_doc_requires_zero_unexpected_send_disabled_rows() -> None:
    text = DASHBOARD.read_text(encoding="utf-8").lower()

    assert "full rollout requires zero unexpected send-disabled suppress rows" in text
