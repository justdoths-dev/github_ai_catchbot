from __future__ import annotations

from uuid import uuid4

import pytest

from services.policy_engine.models import StreamMessage
from services.policy_engine.worker import PolicyEngineWorker

from ._fakes import FakeConsumer, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_worker_uses_trigger_event_id_to_rehydrate_policy_apply_not_redis_payload() -> None:
    repository, job, run, output, _bundle = repo_with_valid_case()
    consumer = FakeConsumer(
        [
            StreamMessage(
                stream="q.analysis.policy",
                message_id="1-0",
                fields={
                    "trigger_event_id": str(job.trigger_event_id),
                    "judge_run_id": str(uuid4()),
                    "judge_output_id": str(uuid4()),
                    "candidate_group_id": str(uuid4()),
                    "bundle_id": str(uuid4()),
                },
            )
        ]
    )
    worker = PolicyEngineWorker(config(), consumer=consumer, service=service(repository))

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert repository.load_job_ids == [job.trigger_event_id]
    assert repository.analyses[0][1].judge_output_id == output.judge_output_id
    assert repository.analyses[0][1].prompt_version == run.prompt_version
    assert consumer.acked == ["1-0"]
