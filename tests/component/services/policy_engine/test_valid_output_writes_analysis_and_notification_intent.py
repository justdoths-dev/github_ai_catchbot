from __future__ import annotations

import pytest

from ._fakes import repo_with_valid_case, service, valid_payload


@pytest.mark.asyncio
async def test_valid_output_writes_analysis_state_transition_and_notification_intent() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()

    await service(repository).handle_job(job)

    assert len(repository.analyses) == 1
    analysis_id, analysis = repository.analyses[0]
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"
    assert analysis.evidence_limitations_ko == "only public docs were checked"
    assert repository.state_transitions == [
        {
            "object_type": "analysis",
            "object_id": analysis_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_finalized",
            "reason_code": "policy_applied:inspect_now:send_now",
        }
    ]
    assert len(repository.notification_outbox) == 1
    intent = repository.notification_outbox[0]
    assert intent.analysis_id == analysis_id
    assert intent.candidate_group_id == job.candidate_group_id
    assert intent.delivery_decision == "send_now"
    assert intent.urgency_profile == "high"
    assert intent.target_chat_id == 12345


@pytest.mark.asyncio
async def test_early_github_tool_bucket_uses_later_delivery_policy() -> None:
    payload = valid_payload(
        practical_usefulness=35,
        evidence_strength=15,
        confidence=20,
        hype_penalty=20,
        code_quality=35,
        specificity=45,
        reproducibility_signal=10,
        maintenance_signal=10,
    )
    payload["model_proposed_verdict"] = "later"
    repository, job, _run, _output, _bundle = repo_with_valid_case(payload=payload)

    await service(repository).handle_job(job)

    assert len(repository.analyses) == 1
    analysis_id, analysis = repository.analyses[0]
    assert analysis.verdict == "later"
    assert analysis.delivery_decision == "send_now"
    assert "policy_threshold_early_github_tool_later" in analysis.reason_codes_json
    assert "policy_threshold_later" not in analysis.reason_codes_json
    assert repository.state_transitions[0]["reason_code"] == "policy_applied:later:send_now"
    assert len(repository.notification_outbox) == 1
    intent = repository.notification_outbox[0]
    assert intent.analysis_id == analysis_id
    assert intent.delivery_decision == "send_now"
    assert intent.urgency_profile == "normal_silent"
    assert intent.render_profile == "telegram_single_alert_normal_v1"
