from __future__ import annotations

from uuid import uuid4


def _delivery_replay_request(notification_plan_id: str) -> dict:
    return {
        "event_type": "replay.requested.v1",
        "root_object_type": "notification_plan",
        "root_object_id": notification_plan_id,
        "replay_type": "delivery",
        "start_stage": "notifier_telegram",
        "forbidden_upstream_recompute": [
            "source_message",
            "artifact",
            "candidate",
            "bundle",
            "judge_output",
            "analysis",
        ],
    }


def test_delivery_replay_root_is_notification_plan_and_targets_notifier_only() -> None:
    notification_plan_id = str(uuid4())
    replay = _delivery_replay_request(notification_plan_id)

    assert replay["root_object_type"] == "notification_plan"
    assert replay["root_object_id"] == notification_plan_id
    assert replay["replay_type"] == "delivery"
    assert replay["start_stage"] == "notifier_telegram"
    assert "analysis" in replay["forbidden_upstream_recompute"]
    assert "judge_output" in replay["forbidden_upstream_recompute"]


def test_delivery_replay_contract_does_not_call_upstream_recompute_hooks() -> None:
    calls: list[str] = []

    def recompute_analysis() -> None:  # pragma: no cover - must not be called
        calls.append("analysis")

    def recompute_judge_output() -> None:  # pragma: no cover - must not be called
        calls.append("judge_output")

    def recompute_bundle() -> None:  # pragma: no cover - must not be called
        calls.append("bundle")

    replay = _delivery_replay_request(str(uuid4()))

    assert replay["start_stage"] == "notifier_telegram"
    assert calls == []
    assert callable(recompute_analysis)
    assert callable(recompute_judge_output)
    assert callable(recompute_bundle)
