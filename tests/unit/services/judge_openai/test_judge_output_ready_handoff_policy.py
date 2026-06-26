from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.judge_openai.config import JudgeOpenAIConfig
from services.judge_openai.models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord, OpenAIJudgeUsage
from services.judge_openai.openai_client import OpenAITransientError
from services.judge_openai.service import JudgeOpenAIService


PROMPT_CACHE_KEY = "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"


class FakeRepository:
    def __init__(
        self,
        *,
        job: JudgeCallJob,
        judge_run: JudgeRunRecord | None,
        bundle: BundleJudgeContext | None,
    ) -> None:
        self.job = job
        self.judge_run = judge_run
        self.bundle = bundle
        self.running: list[UUID] = []
        self.schema_retry_count = 0
        self.finished: list[dict[str, Any]] = []
        self.outputs: list[dict[str, Any]] = []
        self.outbox: list[dict[str, Any]] = []

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeCallJob | None:
        return self.job if trigger_event_id == self.job.trigger_event_id else None

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunRecord | None:
        if self.judge_run is None or self.judge_run.judge_run_id != judge_run_id:
            return None
        return self.judge_run

    async def load_bundle_context(self, bundle_id: UUID) -> BundleJudgeContext | None:
        if self.bundle is None or self.bundle.bundle_id != bundle_id:
            return None
        return self.bundle

    async def mark_judge_run_running(self, judge_run_id: UUID) -> None:
        self.running.append(judge_run_id)

    async def increment_schema_retry_count(self, judge_run_id: UUID) -> None:
        self.schema_retry_count += 1

    async def finish_judge_run(
        self,
        *,
        judge_run_id: UUID,
        status: str,
        usage: OpenAIJudgeUsage | None,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> None:
        self.finished.append(
            {
                "judge_run_id": judge_run_id,
                "status": status,
                "usage": usage,
                "finish_reason": finish_reason,
                "refusal_detected": refusal_detected,
            }
        )

    async def insert_judge_output(self, **kwargs) -> UUID:
        judge_output_id = uuid4()
        self.outputs.append({"judge_output_id": judge_output_id, **kwargs})
        return judge_output_id

    async def insert_judge_output_ready_outbox(self, **kwargs) -> None:
        self.outbox.append({"event_type": "judge.output.ready.v1", **kwargs})


class FakeClient:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _config() -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url="unused-db",
        redis_url="unused-redis",
        queue_name="q.analysis.judge",
        consumer_group="judge-openai",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        openai_api_key="unused",
        openai_project=None,
        request_timeout_sec=1.0,
        max_output_tokens=800,
        enable_prompt_guard_preflight=False,
        log_level="INFO",
    )


def _bundle(*, primary_summary: dict[str, Any] | None = None) -> BundleJudgeContext:
    return BundleJudgeContext(
        bundle_id=uuid4(),
        candidate_group_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        primary_summary=primary_summary if primary_summary is not None else {"title": "fixture", "summary": "usable"},
        supporting_summaries_json=[],
        discovered_links_summary_json=[],
        evidence_limitations=["fixture-only evidence"],
        token_budget_profile="small",
        reroot_count=0,
    )


def _run(
    bundle: BundleJudgeContext,
    *,
    status: str = "pending",
    prompt_cache_key: str | None = PROMPT_CACHE_KEY,
    prompt_version: str = "judge_github_primary_v1",
) -> JudgeRunRecord:
    return JudgeRunRecord(
        judge_run_id=uuid4(),
        bundle_id=bundle.bundle_id,
        judge_profile="github_primary",
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version=prompt_version,
        schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        prompt_cache_key=prompt_cache_key,
        status=status,
        schema_retry_count=0,
    )


def _job(run: JudgeRunRecord) -> JudgeCallJob:
    return JudgeCallJob(
        trigger_event_id=uuid4(),
        event_type="judge.call.requested.v1",
        judge_run_id=run.judge_run_id,
        bundle_id=run.bundle_id,
        model=run.model,
        reasoning_effort=run.reasoning_effort,
        prompt_version=run.prompt_version,
        prompt_cache_key=run.prompt_cache_key,
    )


def _valid_payload(candidate_group_id: UUID) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful fake project",
        "summary_one_line_ko": "bounded summary",
        "skeptical_take_ko": "skeptical take",
        "why_it_might_matter_ko": "why it matters",
        "comparables": ["existing tool"],
        "scores": {
            "novelty": 60,
            "practical_usefulness": 70,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 55,
            "code_quality": 50,
            "maintenance_signal": 45,
            "specificity": 60,
            "reproducibility_signal": None,
        },
        "reason_codes": ["specific_evidence"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["fixture only"],
        "recommended_action_ko": "inspect later",
        "freshness_note_ko": "freshness unknown",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _structured_response(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "completed",
        "output_text": json.dumps(payload),
        "usage": {
            "input_tokens": 100,
            "input_tokens_details": {"cached_tokens": 80},
            "output_tokens": 25,
            "output_tokens_details": {"reasoning_tokens": 7},
        },
    }


def _refusal_response() -> dict[str, Any]:
    return {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [{"type": "refusal", "refusal": "I cannot evaluate this."}],
            }
        ],
    }


async def _handle(repo: FakeRepository, client: FakeClient, job: JudgeCallJob) -> None:
    service = JudgeOpenAIService(_config(), repository=repo, openai_client=client)  # type: ignore[arg-type]
    await service.handle_job(job)


@pytest.mark.asyncio
async def test_structured_success_response_maps_to_valid_judge_output_payload() -> None:
    bundle = _bundle()
    run = _run(bundle)
    job = _job(run)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    client = FakeClient([_structured_response(_valid_payload(bundle.candidate_group_id))])

    await _handle(repo, client, job)

    assert len(client.calls) == 1
    assert repo.outputs[0]["judge_schema_version"] == "judge_output_v1"
    assert repo.outputs[0]["payload_json"]["candidate_group_id"] == str(bundle.candidate_group_id)
    assert repo.finished[-1]["status"] == "succeeded"
    assert repo.finished[-1]["usage"].input_tokens == 100
    assert repo.finished[-1]["usage"].cached_input_tokens == 80
    assert repo.finished[-1]["usage"].output_tokens == 25
    assert repo.finished[-1]["usage"].reasoning_tokens == 7
    assert set(repo.outbox[0]) == {
        "event_type",
        "judge_run_id",
        "judge_output_id",
        "finish_reason",
        "refusal_detected",
    }


@pytest.mark.asyncio
async def test_refusal_response_maps_to_refusal_envelope() -> None:
    bundle = _bundle()
    run = _run(bundle)
    job = _job(run)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    client = FakeClient([_refusal_response()])

    await _handle(repo, client, job)

    assert repo.outputs[0]["payload_json"] == {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(bundle.candidate_group_id),
        "output_kind": "refusal",
        "refusal_text": "I cannot evaluate this.",
    }
    assert repo.finished[-1]["status"] == "succeeded"
    assert repo.finished[-1]["refusal_detected"] is True
    assert repo.outbox[0]["refusal_detected"] is True


@pytest.mark.asyncio
async def test_transient_openai_error_maps_to_failed_retryable_without_output_or_outbox() -> None:
    bundle = _bundle()
    run = _run(bundle)
    job = _job(run)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    private_body = "private provider response body"
    client = FakeClient(
        [
            OpenAITransientError(
                private_body,
                safe_code="openai_retryable_rate_limited",
            )
        ]
    )

    await _handle(repo, client, job)

    assert len(client.calls) == 1
    assert repo.finished[-1]["status"] == "failed_retryable"
    assert repo.finished[-1]["finish_reason"] == "openai_retryable_rate_limited"
    assert repo.outputs == []
    assert repo.outbox == []
    assert private_body not in repr(repo.finished)


@pytest.mark.asyncio
async def test_schema_invalid_after_one_retry_maps_to_failed_terminal_without_output_or_outbox() -> None:
    bundle = _bundle()
    run = _run(bundle)
    job = _job(run)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    client = FakeClient([{"output_text": "not json"}, {"output_text": "still not json"}])

    await _handle(repo, client, job)

    assert len(client.calls) == 2
    assert repo.schema_retry_count == 1
    assert repo.finished[-1]["status"] == "failed_terminal"
    assert repo.finished[-1]["finish_reason"] == "schema_invalid_after_retry"
    assert repo.outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_prompt_cache_key_missing_remains_backward_compatible() -> None:
    bundle = _bundle()
    run = _run(bundle, prompt_cache_key=None)
    job = _job(run)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    client = FakeClient([_structured_response(_valid_payload(bundle.candidate_group_id))])

    await _handle(repo, client, job)

    assert client.calls[0]["prompt_cache_key"] is None
    assert repo.finished[-1]["status"] == "succeeded"
    assert len(repo.outputs) == 1
    assert len(repo.outbox) == 1


@pytest.mark.asyncio
async def test_unsupported_prompt_version_marks_terminal_before_fake_client_call() -> None:
    bundle = _bundle()
    run = _run(bundle, prompt_version="unsupported_prompt_v0")
    job = _job(run)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    client = FakeClient([_structured_response(_valid_payload(bundle.candidate_group_id))])

    await _handle(repo, client, job)

    assert client.calls == []
    assert repo.running == []
    assert repo.finished[-1]["status"] == "failed_terminal"
    assert repo.finished[-1]["finish_reason"] == "unsupported_prompt_version"
    assert repo.outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_structurally_unusable_bundle_marks_terminal_before_fake_client_call() -> None:
    bundle = _bundle(primary_summary={})
    run = _run(bundle)
    job = _job(run)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    client = FakeClient([_structured_response(_valid_payload(bundle.candidate_group_id))])

    await _handle(repo, client, job)

    assert client.calls == []
    assert repo.running == []
    assert repo.finished[-1]["status"] == "failed_terminal"
    assert repo.finished[-1]["finish_reason"] == "bundle_invalid"
    assert repo.outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_job_prompt_cache_key_absent_can_still_match_current_run() -> None:
    bundle = _bundle()
    run = _run(bundle)
    job = replace(_job(run), prompt_cache_key=None)
    repo = FakeRepository(job=job, judge_run=run, bundle=bundle)
    client = FakeClient([_structured_response(_valid_payload(bundle.candidate_group_id))])

    await _handle(repo, client, job)

    assert len(client.calls) == 1
    assert client.calls[0]["prompt_cache_key"] == PROMPT_CACHE_KEY
    assert repo.finished[-1]["status"] == "succeeded"
