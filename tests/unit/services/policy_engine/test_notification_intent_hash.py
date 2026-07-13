from __future__ import annotations

from uuid import uuid4

from services.policy_engine.notification_intent import material_change_hash_for_analysis


def _hash(
    *,
    candidate_group_id: str,
    dedupe_subject_key: str | None = None,
    verdict: str = "inspect_now",
    reason_codes_json: list[str] | None = None,
) -> str:
    return material_change_hash_for_analysis(
        candidate_group_id=candidate_group_id,
        dedupe_subject_key=dedupe_subject_key,
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


def test_canonical_subject_suppresses_cross_candidate_repost_but_material_change_escapes() -> None:
    first_candidate_group_id = str(uuid4())
    repost_candidate_group_id = str(uuid4())
    canonical_subject = "github:repo:example/example-tool"

    first = _hash(
        candidate_group_id=first_candidate_group_id,
        dedupe_subject_key=canonical_subject,
    )
    exact_repost = _hash(
        candidate_group_id=repost_candidate_group_id,
        dedupe_subject_key=canonical_subject,
    )
    material_change = _hash(
        candidate_group_id=repost_candidate_group_id,
        dedupe_subject_key=canonical_subject,
        reason_codes_json=["policy_threshold_inspect_now", "new_material_evidence"],
    )
    other_subject = _hash(
        candidate_group_id=repost_candidate_group_id,
        dedupe_subject_key="github:repo:other/other-tool",
    )

    assert first_candidate_group_id != repost_candidate_group_id
    assert exact_repost == first
    assert material_change != first
    assert other_subject != first
