from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.policy_engine.feedback_eval import ChannelFeedbackObservation, ChannelFeedbackSample, channel_fp

from ._fakes import repo_with_valid_case, service, valid_payload


FEEDBACK_AWARE_DELIVERY_POLICY_VERSION = "delivery_policy_v1_feedback_aware_channel_policy_v1"


def _feedback_observation(category: str) -> ChannelFeedbackObservation:
    return ChannelFeedbackObservation(
        feedback_category=category,
        verdict="later",
        delivery_decision="send_now",
        primary_artifact_type="github_repo",
        created_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )


def _feedback_sample(*categories: str) -> ChannelFeedbackSample:
    return ChannelFeedbackSample(
        channel_fingerprint=channel_fp("component-channel"),
        observations=tuple(_feedback_observation(category) for category in categories),
        sample_limit=100,
        window_days=90,
    )


@pytest.mark.asyncio
async def test_valid_output_writes_analysis_state_transition_and_notification_intent() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()

    await service(repository).handle_job(job)

    assert len(repository.analyses) == 1
    analysis_id, analysis = repository.analyses[0]
    assert analysis.policy_version == "verdict_policy_v1"
    assert analysis.delivery_policy_version == FEEDBACK_AWARE_DELIVERY_POLICY_VERSION
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
    assert intent.dedupe_subject_key == "github:repo:example/useful-repository"


@pytest.mark.asyncio
async def test_analysis_insert_conflict_loser_emits_no_transition_or_notification() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()
    repository.feedback_samples[job.candidate_group_id] = _feedback_sample(
        "false_positive",
        "duplicate",
        "stale",
        "wrong_priority",
        "useful",
    )
    repository.conflict_on_next_analysis_insert = True

    await service(repository).handle_job(job)

    feedback_aware_identity = (
        job.judge_output_id,
        "verdict_policy_v1",
        FEEDBACK_AWARE_DELIVERY_POLICY_VERSION,
    )
    assert repository.analyses == []
    assert set(repository.existing) == {feedback_aware_identity}
    assert repository.existing_analysis_lookups == [feedback_aware_identity, feedback_aware_identity]
    assert repository.analysis_insert_attempts == [feedback_aware_identity]
    assert repository.state_transitions == []
    assert repository.notification_outbox == []


@pytest.mark.asyncio
async def test_concurrent_workers_insert_one_feedback_aware_analysis_and_one_notification_intent() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()
    repository.synchronize_next_analysis_lookups(2)

    await asyncio.gather(
        service(repository).handle_job(job),
        service(repository).handle_job(job),
    )

    feedback_aware_identity = (
        job.judge_output_id,
        "verdict_policy_v1",
        FEEDBACK_AWARE_DELIVERY_POLICY_VERSION,
    )
    assert len(repository.analyses) == 1
    analysis_id, analysis = repository.analyses[0]
    assert (
        analysis.judge_output_id,
        analysis.policy_version,
        analysis.delivery_policy_version,
    ) == feedback_aware_identity
    assert set(repository.existing) == {feedback_aware_identity}
    assert repository.existing[feedback_aware_identity].analysis_id == analysis_id
    assert repository.existing_analysis_lookups == [feedback_aware_identity] * 3
    assert repository.analysis_insert_attempts == [feedback_aware_identity] * 2
    assert len(repository.state_transitions) == 1
    assert repository.state_transitions[0]["object_id"] == analysis_id
    assert len(repository.notification_outbox) == 1
    assert repository.notification_outbox[0].analysis_id == analysis_id


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


@pytest.mark.asyncio
async def test_below_minimum_feedback_is_field_compatible_with_default_policy() -> None:
    repository, job, _run, _output, _bundle = repo_with_valid_case()
    repository.feedback_samples[job.candidate_group_id] = _feedback_sample("false_positive")

    await service(repository).handle_job(job)

    analysis = repository.analyses[0][1]
    assert analysis.policy_version == "verdict_policy_v1"
    assert analysis.delivery_policy_version == FEEDBACK_AWARE_DELIVERY_POLICY_VERSION
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"
    assert analysis.reason_codes_json == ["repo_has_clear_scope", "policy_threshold_inspect_now"]
    assert len(repository.notification_outbox) == 1
    assert repository.notification_outbox[0].urgency_profile == "high"


@pytest.mark.asyncio
async def test_sufficient_noisy_channel_suppresses_later_without_changing_verdict() -> None:
    payload = valid_payload(
        practical_usefulness=55,
        evidence_strength=40,
        confidence=45,
        code_quality=50,
    )
    payload["model_proposed_verdict"] = "later"
    repository, job, _run, _output, _bundle = repo_with_valid_case(payload=payload)
    repository.feedback_samples[job.candidate_group_id] = _feedback_sample(
        "false_positive",
        "duplicate",
        "stale",
        "wrong_priority",
        "useful",
    )

    await service(repository).handle_job(job)

    analysis = repository.analyses[0][1]
    assert analysis.policy_version == "verdict_policy_v1"
    assert analysis.delivery_policy_version == FEEDBACK_AWARE_DELIVERY_POLICY_VERSION
    assert analysis.verdict == "later"
    assert analysis.delivery_decision == "suppress"
    assert "channel_policy_tier_c_suppress_non_strong" in analysis.reason_codes_json
    assert repository.notification_outbox == []
    assert repository.state_transitions[0]["to_state"] == "analysis_suppressed"


@pytest.mark.asyncio
async def test_false_negative_feedback_and_unrelated_candidate_sample_cannot_force_delivery() -> None:
    payload = valid_payload(
        practical_usefulness=55,
        evidence_strength=40,
        confidence=45,
        code_quality=50,
    )
    payload["model_proposed_verdict"] = "later"
    repository, job, _run, _output, _bundle = repo_with_valid_case(payload=payload)
    repository.feedback_samples[job.candidate_group_id] = _feedback_sample(
        "false_negative",
        "false_negative",
        "false_negative",
        "false_negative",
        "false_negative",
    )
    repository.feedback_samples[uuid4()] = _feedback_sample(
        "false_positive",
        "duplicate",
        "stale",
        "wrong_priority",
        "bad_channel_fit",
    )

    await service(repository).handle_job(job)

    analysis = repository.analyses[0][1]
    assert analysis.verdict == "later"
    assert analysis.delivery_decision == "send_now"
    assert not any(reason.startswith("channel_policy_") for reason in analysis.reason_codes_json)
    assert len(repository.notification_outbox) == 1
