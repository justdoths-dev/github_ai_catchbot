from __future__ import annotations

from .models import DeliveryReplayDecision, DeliveryResultWorkerResult


def maintenance_result_allows_ack(result: DeliveryResultWorkerResult | None) -> bool:
    if result is None or not result.processed:
        return False
    if result.classification in {"ignored", "unsupported", "identity_invalid"}:
        return False
    if result.action in {"ignored", "unsupported", "fail_closed"}:
        return False
    return True


def replay_result_allows_ack(result: DeliveryReplayDecision | None) -> bool:
    if result is None:
        return False
    if result.action == "emit_replay_intent":
        return True
    return (
        result.action == "already_completed_noop"
        and result.reason_code == "replay_request_already_completed_noop"
    )
