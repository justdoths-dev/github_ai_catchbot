from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
TRIAGE_RUNBOOK = ROOT / "ops" / "delivery" / "runbooks" / "delivery_dead_letter_triage.md"
BATCH_RUNBOOK = ROOT / "ops" / "delivery" / "runbooks" / "delivery_batch_recovery.md"


LAST_ERROR_CODES = {
    "max_notification_retry_attempts_exceeded",
    "notify_transport_terminal_chat_access",
    "notify_transport_terminal_edit_forbidden",
    "notify_render_invalid_payload",
    "delivery_replay_env_guard_rejected",
    "delivery_replay_unsupported_request",
    "maintenance_due_retry_emit_failed",
}

NEXT_MANUAL_ACTIONS = {
    "request_explicit_delivery_replay",
    "fix_chat_access_then_delivery_replay",
    "disable_edits_then_delivery_replay",
    "fix_template_then_delivery_replay",
    "acknowledge_and_close_no_recovery",
    "fix_env_guard_then_retry_replay_request",
}


def test_dead_letter_triage_runbook_contains_locked_vocabulary() -> None:
    text = TRIAGE_RUNBOOK.read_text(encoding="utf-8")

    for value in LAST_ERROR_CODES | NEXT_MANUAL_ACTIONS | {"delivery_replay_from_notification_plan"}:
        assert f"`{value}`" in text


def test_dead_letter_triage_forbids_auto_close_and_terminal_auto_retry() -> None:
    text = TRIAGE_RUNBOOK.read_text(encoding="utf-8").lower()

    assert "do not auto-close delivery dlq" in text
    assert "do not auto-retry terminal failures" in text
    assert "must not reset `notification_plans.status`" in text


def test_batch_recovery_preserves_notification_plan_root_and_no_upstream_recompute() -> None:
    text = BATCH_RUNBOOK.read_text(encoding="utf-8").lower()

    assert "`root_object_type = notification_plan`" in text
    assert "`root_object_id = notification_plan_id`" in text
    assert "must not recalculate upstream analysis" in text
    assert "does not start from analysis, judge, bundle, candidate, artifact, or source message roots" in text


def test_send_disabled_suppress_backlog_is_explicit_replay_only() -> None:
    text = BATCH_RUNBOOK.read_text(encoding="utf-8").lower()

    assert "send-disabled suppress backlog is explicit replay only, not auto retry" in text
    assert "send-disabled suppress backlog" in text
    assert "explicit delivery replay is required" in text
