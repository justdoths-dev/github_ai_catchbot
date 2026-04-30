from __future__ import annotations

from uuid import uuid4

from services.policy_engine.notification_intent import material_change_hash_for_analysis


def _hash(
    *,
    candidate_group_id: str,
    verdict: str = "inspect_now",
    reason_codes_json: list[str] | None = None,
) -> str:
    return material_change_hash_for_analysis(
        candidate_group_id=candidate_group_id,
        verdict=verdict,
        delivery_decision="send_now",
        urgency_profile="high",
        reason_codes_json=reason_codes_json or ["policy_threshold_inspect_now"],
        recommended_action_ko="inspect repository",
        freshness_note_ko="fresh",
    )


def test_material_hash_is_stable() -> None:
    candidate_group_id = str(uuid4())

    assert _hash(candidate_group_id=candidate_group_id) == _hash(candidate_group_id=candidate_group_id)


def test_material_hash_excludes_analysis_id() -> None:
    candidate_group_id = str(uuid4())
    first_analysis_id = uuid4()
    second_analysis_id = uuid4()

    assert first_analysis_id != second_analysis_id
    assert _hash(candidate_group_id=candidate_group_id) == _hash(candidate_group_id=candidate_group_id)


def test_material_hash_changes_when_verdict_or_reason_material_changes() -> None:
    candidate_group_id = str(uuid4())

    assert _hash(candidate_group_id=candidate_group_id) != _hash(
        candidate_group_id=candidate_group_id,
        verdict="later",
    )
    assert _hash(candidate_group_id=candidate_group_id) != _hash(
        candidate_group_id=candidate_group_id,
        reason_codes_json=["policy_threshold_later"],
    )
