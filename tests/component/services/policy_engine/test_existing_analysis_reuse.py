from __future__ import annotations

from uuid import uuid4

import pytest

from services.policy_engine.models import ExistingAnalysisRecord

from ._fakes import repo_with_valid_case, service


@pytest.mark.asyncio
async def test_existing_analysis_reuse_prevents_new_analysis_and_notification_intent() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()
    repository.existing[(job.judge_output_id, "verdict_policy_v1", "delivery_policy_v1")] = ExistingAnalysisRecord(
        analysis_id=uuid4(),
        judge_output_id=job.judge_output_id,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )

    await service(repository).handle_job(job)

    assert repository.analyses == []
    assert repository.state_transitions == []
    assert repository.notification_outbox == []
