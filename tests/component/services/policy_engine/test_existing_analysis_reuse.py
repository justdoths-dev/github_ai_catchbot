from __future__ import annotations

from uuid import uuid4

import pytest

from services.policy_engine.models import AnalysisDraft, ExistingAnalysisRecord

from ._fakes import repo_with_valid_case, service


@pytest.mark.asyncio
async def test_legacy_analysis_coexists_with_feedback_aware_analysis_and_retry_is_noop() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()
    legacy_identity = (job.judge_output_id, "verdict_policy_v1", "delivery_policy_v1")
    feedback_aware_identity = (
        job.judge_output_id,
        "verdict_policy_v1",
        "delivery_policy_v1_feedback_aware_channel_policy_v1",
    )
    legacy_analysis_id = uuid4()
    legacy_analysis = AnalysisDraft(
        candidate_group_id=job.candidate_group_id,
        judge_output_id=job.judge_output_id,
        schema_version="analysis_v1",
        policy_version="verdict_policy_v1",
        prompt_version="judge_prompt_v1",
        delivery_policy_version="delivery_policy_v1",
        verdict="inspect_now",
        delivery_decision="send_now",
        scores_json={"practical_usefulness": 76},
        reason_codes_json=["legacy_policy_result"],
        evidence_limitations_ko="legacy evidence limitations",
        recommended_action_ko="legacy recommendation",
        freshness_note_ko="legacy freshness note",
        model_proposed_verdict="inspect_now",
        policy_reconciled_flag=False,
    )
    legacy_record = ExistingAnalysisRecord(
        analysis_id=legacy_analysis_id,
        judge_output_id=job.judge_output_id,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )
    repository.analyses.append((legacy_analysis_id, legacy_analysis))
    repository.existing[legacy_identity] = legacy_record

    await service(repository).handle_job(job)

    assert len(repository.analyses) == 2
    assert repository.analyses[0] == (legacy_analysis_id, legacy_analysis)
    analysis_id, analysis = repository.analyses[1]
    assert (
        analysis.judge_output_id,
        analysis.policy_version,
        analysis.delivery_policy_version,
    ) == feedback_aware_identity
    assert set(repository.existing) == {legacy_identity, feedback_aware_identity}
    assert repository.existing[legacy_identity] is legacy_record
    assert repository.existing[feedback_aware_identity].analysis_id == analysis_id
    assert repository.existing_analysis_lookups == [feedback_aware_identity]
    assert repository.analysis_insert_attempts == [feedback_aware_identity]
    assert len(repository.state_transitions) == 1
    assert len(repository.notification_outbox) == 1

    analyses_before_retry = list(repository.analyses)
    existing_before_retry = dict(repository.existing)
    transitions_before_retry = list(repository.state_transitions)
    outbox_before_retry = list(repository.notification_outbox)

    await service(repository).handle_job(job)

    assert repository.analyses == analyses_before_retry
    assert repository.existing == existing_before_retry
    assert repository.analyses[0] == (legacy_analysis_id, legacy_analysis)
    assert repository.existing[legacy_identity] is legacy_record
    assert repository.state_transitions == transitions_before_retry
    assert repository.notification_outbox == outbox_before_retry
    assert repository.existing_analysis_lookups == [
        feedback_aware_identity,
        feedback_aware_identity,
    ]
    assert repository.analysis_insert_attempts == [feedback_aware_identity]
