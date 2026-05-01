from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OBSERVABILITY_SQL = ROOT / "ops" / "delivery" / "sql" / "delivery_observability_queries.sql"
ROLLOUT_SQL = ROOT / "ops" / "delivery" / "sql" / "delivery_rollout_gate_queries.sql"

FORBIDDEN_SQL_MUTATIONS = (
    "update notification_plans",
    "delete from notification_plans",
    "alter table",
    "create table",
    "drop table",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_delivery_observability_sql_exists_and_contains_required_anchors() -> None:
    text = _read(OBSERVABILITY_SQL)

    required_anchors = {
        "current_unsent_backlog",
        "due_retry_backlog",
        "trailing_1h_delivery_outcome_mix",
        "trailing_1h_transport_error_class_mix",
        "high_plan_to_transport_p95_lag",
        "high_source_to_delivery_p95_lag",
        "delivery_dlq_triage_view",
        "send_disabled_suppress_backlog_selection",
        "batch_replay_request_insert_skeleton",
    }

    for anchor in required_anchors:
        assert f"-- query: {anchor}" in text


def test_delivery_slo_queries_use_existing_delivery_root_and_queues() -> None:
    text = _read(OBSERVABILITY_SQL)

    assert "'notification_plan'" in text
    assert "'q.notification.send'" in text
    assert "'q.maintenance'" in text
    assert "'q.replay'" in text
    assert "notification_plans" in text
    assert "notification_delivery_records" in text
    assert "dead_letter_entries" in text


def test_current_unsent_backlog_emits_high_backlog_alert_fields() -> None:
    text = _read(OBSERVABILITY_SQL)
    start = text.index("-- query: current_unsent_backlog")
    end = text.index("-- query: due_retry_backlog")
    section = text[start:end]

    assert "unsent_plan_count" in section
    assert "oldest_plan_created_at" in section
    assert "oldest_plan_age_sec" in section
    assert "high_unsent_plan_count" in section
    assert "high_oldest_plan_created_at" in section
    assert "high_oldest_plan_age_sec" in section
    assert "'high'::urgency_profile_enum" in section


def test_sql_assets_do_not_mutate_notification_plans_or_schema() -> None:
    for path in (OBSERVABILITY_SQL, ROLLOUT_SQL):
        text = _read(path).lower()
        for forbidden in FORBIDDEN_SQL_MUTATIONS:
            assert forbidden not in text
