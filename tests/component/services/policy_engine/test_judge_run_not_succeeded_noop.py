from __future__ import annotations

from dataclasses import replace

import pytest

from ._fakes import repo_with_valid_case, service


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending", "failed_terminal"])
async def test_judge_run_not_succeeded_noop_records_policy_failure(status: str) -> None:
    repository, job, run, _output, _bundle = repo_with_valid_case()
    repository.runs[job.judge_run_id] = replace(run, status=status)

    await service(repository).handle_job(job)

    assert repository.analyses == []
    assert repository.notification_outbox == []
    assert repository.state_transitions == [
        {
            "object_type": "candidate_group",
            "object_id": job.candidate_group_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_policy_failed",
            "reason_code": "policy_judge_run_not_succeeded",
        }
    ]
