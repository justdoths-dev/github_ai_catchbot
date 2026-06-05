from __future__ import annotations

from services.maintenance.delivery_operations_gate import (
    ALLOWED_DELIVERY_DLQ_LAST_ERROR_CODES,
    ALLOWED_DELIVERY_DLQ_NEXT_MANUAL_ACTIONS,
    ALLOWED_DELIVERY_DLQ_REPLAY_HINTS,
)


def test_delivery_dlq_taxonomy_is_exact() -> None:
    assert ALLOWED_DELIVERY_DLQ_LAST_ERROR_CODES == {
        "max_notification_retry_attempts_exceeded",
        "notify_transport_terminal_chat_access",
        "notify_transport_terminal_edit_forbidden",
        "notify_render_invalid_payload",
        "delivery_replay_env_guard_rejected",
        "delivery_replay_unsupported_request",
        "maintenance_due_retry_emit_failed",
    }
    assert ALLOWED_DELIVERY_DLQ_NEXT_MANUAL_ACTIONS == {
        "request_explicit_delivery_replay",
        "fix_chat_access_then_delivery_replay",
        "disable_edits_then_delivery_replay",
        "fix_template_then_delivery_replay",
        "acknowledge_and_close_no_recovery",
        "fix_env_guard_then_retry_replay_request",
    }
    assert ALLOWED_DELIVERY_DLQ_REPLAY_HINTS == {"delivery_replay_from_notification_plan"}
