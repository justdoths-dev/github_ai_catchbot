from __future__ import annotations

import pytest

from ._fakes import repo_with_valid_case, service, stale_candidate


@pytest.mark.asyncio
async def test_stale_bundle_request_noop_records_stale_transition() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()
    stale_candidate(repository, job)

    await service(repository).handle_job(job)

    assert repository.analyses == []
    assert repository.notification_outbox == []
    assert repository.state_transitions == [
        {
            "object_type": "candidate_group",
            "object_id": job.candidate_group_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_policy_stale_bundle",
            "reason_code": "policy_stale_bundle_request",
        }
    ]
