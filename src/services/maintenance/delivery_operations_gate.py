from __future__ import annotations

from typing import TypeAlias

from .delivery_gate import (
    FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC,
    DeliveryGate,
    DeliveryGateRepository,
    evaluate_delivery_gate,
)
from .models import DeliveryGateReportV1, GateMode, GateStatus


DeliveryGateMode: TypeAlias = GateMode
DeliveryGateStatus: TypeAlias = GateStatus
DeliveryOperationsGateReport: TypeAlias = DeliveryGateReportV1
DeliveryOperationsGateRepository = DeliveryGateRepository
DeliveryOperationsGate = DeliveryGate
evaluate_delivery_operations_gate = evaluate_delivery_gate

ALLOWED_DELIVERY_DLQ_LAST_ERROR_CODES = frozenset(
    {
        "max_notification_retry_attempts_exceeded",
        "notify_transport_terminal_chat_access",
        "notify_transport_terminal_edit_forbidden",
        "notify_render_invalid_payload",
        "delivery_replay_env_guard_rejected",
        "delivery_replay_unsupported_request",
        "maintenance_due_retry_emit_failed",
    }
)
ALLOWED_DELIVERY_DLQ_NEXT_MANUAL_ACTIONS = frozenset(
    {
        "request_explicit_delivery_replay",
        "fix_chat_access_then_delivery_replay",
        "disable_edits_then_delivery_replay",
        "fix_template_then_delivery_replay",
        "acknowledge_and_close_no_recovery",
        "fix_env_guard_then_retry_replay_request",
    }
)
ALLOWED_DELIVERY_DLQ_REPLAY_HINTS = frozenset({"delivery_replay_from_notification_plan"})
