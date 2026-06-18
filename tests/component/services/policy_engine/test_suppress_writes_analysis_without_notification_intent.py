from __future__ import annotations

import pytest

from ._fakes import repo_with_valid_case, service, valid_payload


@pytest.mark.asyncio
async def test_suppress_writes_analysis_without_notification_intent() -> None:
    payload = valid_payload(practical_usefulness=20, evidence_strength=20, confidence=20)
    repository, job, _run, _output, _bundle = repo_with_valid_case(payload=payload)

    await service(repository).handle_job(job)

    assert len(repository.analyses) == 1
    _analysis_id, analysis = repository.analyses[0]
    assert analysis.verdict == "skip"
    assert analysis.delivery_decision == "suppress"
    assert "policy_verdict_skip" in analysis.reason_codes_json
    assert repository.state_transitions[0]["to_state"] == "analysis_suppressed"
    assert repository.notification_outbox == []
