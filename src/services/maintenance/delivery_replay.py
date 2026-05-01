from __future__ import annotations

from typing import Any
from uuid import UUID

from .config import MaintenanceConfig
from .models import DeliveryReplayDecision, NotificationPlanRecord, ReplayRequestRecord


REPLAY_REQUESTED_EVENT_TYPE = "replay.requested.v1"
REPLAY_QUEUE_NAME = "q.replay"
REPLAY_INTENT_EVENT_TYPE = "notification.plan.created.v1"

REQUIRED_REPLAY_PAYLOAD_FIELDS = {
    "notification_plan_id",
    "analysis_id",
    "candidate_group_id",
    "delivery_decision",
    "urgency_profile",
    "target_chat_id",
    "target_thread_id",
    "render_profile",
    "dedupe_subject_key",
    "material_change_hash",
    "send_after",
    "suppress_reason_code",
    "replay_request_id",
    "replay_reason",
}


def evaluate_delivery_replay(
    *,
    config: MaintenanceConfig,
    replay_request: ReplayRequestRecord | None,
    plan: NotificationPlanRecord | None,
    replay_reason: str | None = None,
) -> DeliveryReplayDecision:
    if replay_request is None:
        return DeliveryReplayDecision(action="reject", reason_code="replay_request_missing")
    if replay_request.replay_type != "delivery":
        return DeliveryReplayDecision(action="reject", reason_code="unsupported_replay_type")
    if replay_request.root_object_type != "notification_plan":
        return DeliveryReplayDecision(action="reject", reason_code="unsupported_replay_root")
    if not config.replay_dispatch_allowed():
        return DeliveryReplayDecision(action="reject", reason_code="rejected_by_env_guard")
    if plan is None:
        return DeliveryReplayDecision(action="reject", reason_code="notification_plan_missing")
    if plan.notification_plan_id != replay_request.root_object_id:
        return DeliveryReplayDecision(action="reject", reason_code="replay_request_plan_mismatch")

    reason = replay_reason or "explicit_delivery_replay"
    return DeliveryReplayDecision(
        action="emit_replay_intent",
        reason_code="explicit_delivery_replay",
        dedupe_key=replay_intent_dedupe_key(replay_request.replay_request_id),
        payload=build_replay_intent_payload(
            plan=plan,
            replay_request_id=replay_request.replay_request_id,
            replay_reason=reason,
        ),
    )


def build_replay_intent_payload(
    *,
    plan: NotificationPlanRecord,
    replay_request_id: UUID,
    replay_reason: str,
) -> dict[str, Any]:
    return {
        "notification_plan_id": str(plan.notification_plan_id),
        "analysis_id": str(plan.analysis_id),
        "candidate_group_id": str(plan.candidate_group_id),
        "delivery_decision": plan.delivery_decision,
        "urgency_profile": plan.urgency_profile,
        "target_chat_id": plan.target_chat_id,
        "target_thread_id": plan.target_thread_id,
        "render_profile": plan.render_profile,
        "dedupe_subject_key": plan.dedupe_subject_key,
        "material_change_hash": plan.material_change_hash,
        "send_after": None,
        "suppress_reason_code": plan.suppress_reason_code,
        "replay_request_id": str(replay_request_id),
        "replay_reason": replay_reason,
    }


def replay_intent_dedupe_key(replay_request_id: UUID) -> str:
    return f"notify:replay-intent:{replay_request_id}"
