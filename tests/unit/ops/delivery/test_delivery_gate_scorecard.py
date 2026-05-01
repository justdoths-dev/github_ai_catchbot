from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
ALERTS = ROOT / "ops" / "delivery" / "alerts" / "delivery_minimal_alerts.yaml"
OBSERVABILITY_SQL = ROOT / "ops" / "delivery" / "sql" / "delivery_observability_queries.sql"
DASHBOARD = ROOT / "ops" / "delivery" / "dashboards" / "delivery_minimal_dashboard.md"
ROLLOUT_SQL = ROOT / "ops" / "delivery" / "sql" / "delivery_rollout_gate_queries.sql"


def _query_section(sql: str, anchor: str) -> str:
    start = sql.index(f"-- query: {anchor}")
    next_anchor = sql.find("-- query:", start + 1)
    return sql[start:] if next_anchor == -1 else sql[start:next_anchor]


def _aliases(section: str) -> set[str]:
    return set(re.findall(r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", section, flags=re.IGNORECASE))


def test_rollout_gate_docs_distinguish_restricted_and_full_gates() -> None:
    text = DASHBOARD.read_text(encoding="utf-8").lower()

    assert "restricted rollout gate inputs" in text
    assert "full rollout gate inputs" in text
    assert "restricted rollout can fail on any unexplained delivery dlq row" in text
    assert "full rollout requires zero unexpected send-disabled suppress rows" in text


def test_rollout_sql_contains_restricted_and_full_scorecard_anchors() -> None:
    text = ROLLOUT_SQL.read_text(encoding="utf-8")

    required_anchors = {
        "restricted_delivery_success_rate_trailing_1h",
        "full_delivery_success_rate_trailing_24h",
        "restricted_high_source_to_delivery_p95",
        "restricted_plan_to_transport_p95",
        "restricted_oldest_due_retry_lag",
        "restricted_open_delivery_dlq_count",
        "full_oldest_delivery_dlq_age",
        "restricted_unexpected_send_disabled_suppress_count",
        "full_replay_guard_reject_count",
        "full_retry_ceiling_exceeded_count",
        "full_duplicate_noop_ratio",
    }

    for anchor in required_anchors:
        assert f"-- query: {anchor}" in text


def test_rollout_sql_declares_query_contract_only() -> None:
    text = ROLLOUT_SQL.read_text(encoding="utf-8").lower()

    assert "query contract only" in text
    assert "no stage 43 gate runner" in text


def test_alert_threshold_fields_are_emitted_by_query_anchors() -> None:
    alerts = ALERTS.read_text(encoding="utf-8")
    observability_sql = OBSERVABILITY_SQL.read_text(encoding="utf-8")
    rollout_sql = ROLLOUT_SQL.read_text(encoding="utf-8")

    fields_by_anchor = {
        "current_unsent_backlog": {
            "oldest_plan_age_sec",
            "high_oldest_plan_age_sec",
        },
        "due_retry_backlog": {
            "due_retry_count",
            "oldest_due_retry_lag_sec",
        },
        "full_replay_guard_reject_count": {
            "replay_guard_reject_count",
        },
        "restricted_delivery_success_rate_trailing_1h": {
            "success_rate",
        },
    }

    for anchor, fields in fields_by_anchor.items():
        sql = observability_sql if f"-- query: {anchor}" in observability_sql else rollout_sql
        aliases = _aliases(_query_section(sql, anchor))
        for field in fields:
            assert field in alerts
            assert field in aliases


def test_full_duplicate_noop_ratio_uses_delivery_attempt_denominator() -> None:
    text = ROLLOUT_SQL.read_text(encoding="utf-8")
    section = _query_section(text, "full_duplicate_noop_ratio")

    assert "FROM notification_delivery_records dr" in section
    assert "delivery_attempt_count" in section
    assert "notification_duplicate_noop" in section
    assert "telegram_edit_not_modified_noop" in section
    assert "notification_delivered" not in section
    assert "notification_edited" not in section
