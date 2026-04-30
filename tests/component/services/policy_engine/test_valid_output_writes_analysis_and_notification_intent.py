from __future__ import annotations

import pytest

from ._fakes import repo_with_valid_case, service


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
