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


PROMPT_CACHE_KEY = "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1"


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
        self.loaded_trigger_event_ids: list[UUID] = []
        self.running: list[UUID] = []
        self.schema_retry_count = 0
        self.finished: list[dict[str, Any]] = []
        self.judge_outputs: list[dict[str, Any]] = []
        self.outbox: list[dict[str, Any]] = []
        self.transactions = 0

    @asynccontextmanager
    async def transaction(self):
        self.transactions += 1
        yield self

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeCallJob | None:
        self.loaded_trigger_event_ids.append(trigger_event_id)
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

    async def insert_judge_output(
        self,
        *,
        judge_run_id: UUID,
        candidate_group_id: UUID,
        judge_schema_version: str,
        payload_json: dict[str, Any],
        model_proposed_verdict: str | None,
        model_confidence_band: str | None,
    ) -> UUID:
        judge_output_id = uuid4()
        self.judge_outputs.append(
            {
                "judge_output_id": judge_output_id,
                "judge_run_id": judge_run_id,
                "candidate_group_id": candidate_group_id,
                "judge_schema_version": judge_schema_version,
                "payload_json": payload_json,
                "model_proposed_verdict": model_proposed_verdict,
                "model_confidence_band": model_confidence_band,
            }
        )
        return judge_output_id

    async def insert_judge_output_ready_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        finish_reason: str | None,
        refusal_detected: bool,
    ) -> None:
        self.outbox.append(
            {
                "event_type": "judge.output.ready.v1",
                "judge_run_id": judge_run_id,
                "judge_output_id": judge_output_id,
                "finish_reason": finish_reason,
                "refusal_detected": refusal_detected,
            }
        )


class FakeOpenAIClient:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _config() -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url="unused-database",
        redis_url="unused-redis",
        queue_name="q.analysis.judge",
        consumer_group="judge-openai",
        consumer_name="judge-openai-test",
        batch_size=1,
        block_ms=1,
        openai_api_key="unused",
        openai_project=None,
        request_timeout_sec=1.0,
        max_output_tokens=500,
        enable_prompt_guard_preflight=False,
        log_level="INFO",
    )


def _bundle() -> BundleJudgeContext:
    return BundleJudgeContext(
        bundle_id=uuid4(),
        candidate_group_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        primary_summary={"title": "repo", "summary": "useful"},
        supporting_summaries_json=[{"kind": "x"}],
        discovered_links_summary_json=[{"kind": "repo"}],
        evidence_limitations=["limited stars snapshot"],
        token_budget_profile="small",
        reroot_count=0,
    )


def _judge_run(bundle: BundleJudgeContext, *, status: str = "pending") -> JudgeRunRecord:
    return JudgeRunRecord(
        judge_run_id=uuid4(),
        bundle_id=bundle.bundle_id,
        judge_profile="github_primary",
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_prompt_v1",
        schema_version="judge_output_v1",
        policy_version="policy_v1",
        prompt_cache_key=PROMPT_CACHE_KEY,
        status=status,
        schema_retry_count=0,
    )


def _job(judge_run: JudgeRunRecord, *, event_type: str = "judge.call.requested.v1") -> JudgeCallJob:
    return JudgeCallJob(
        trigger_event_id=uuid4(),
        event_type=event_type,
        judge_run_id=judge_run.judge_run_id,
        bundle_id=judge_run.bundle_id,
        model=judge_run.model,
        reasoning_effort=judge_run.reasoning_effort,
        prompt_version=judge_run.prompt_version,
        prompt_cache_key=judge_run.prompt_cache_key,
    )


def _valid_payload(candidate_group_id: UUID) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful repo",
        "summary_one_line_ko": "summary",
        "skeptical_take_ko": "skeptical take",
        "why_it_might_matter_ko": "why it matters",
        "comparables": [],
        "scores": {
            "novelty": 60,
            "practical_usefulness": 70,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 55,
            "code_quality": 50,
            "maintenance_signal": 45,
            "specificity": 60,
            "reproducibility_signal": 40,
        },
        "reason_codes": ["has_repo"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["limited"],
        "recommended_action_ko": "inspect",
        "freshness_note_ko": "fresh",
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


def _invalid_response() -> dict[str, str]:
    return {"status": "completed", "output_text": "{not-valid-json"}


def _refusal_response() -> dict[str, Any]:
    return {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [{"type": "refusal", "refusal": "I cannot comply."}],
            }
        ],
    }


def _subject(
    *,
    repo: FakeRepository,
    responses: list[Any],
) -> tuple[JudgeOpenAIService, FakeOpenAIClient]:
    client = FakeOpenAIClient(responses)
    service = JudgeOpenAIService(
        _config(),
        repository=repo,  # type: ignore[arg-type]
        openai_client=client,
    )
    return service, client


@pytest.mark.asyncio
async def test_structured_success_writes_judge_output_finishes_run_and_emits_ready_outbox() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = _job(judge_run)
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    payload = _valid_payload(bundle.candidate_group_id)
    service, client = _subject(repo=repo, responses=[_structured_response(payload)])

    await service.handle_job(job)

    assert len(client.calls) == 1
    assert client.calls[0]["prompt_cache_key"] == PROMPT_CACHE_KEY
    assert repo.judge_outputs[0]["payload_json"] == payload
    assert repo.judge_outputs[0]["model_proposed_verdict"] == "later"
    assert repo.judge_outputs[0]["model_confidence_band"] == "medium"
    assert repo.finished[-1]["status"] == "succeeded"
    assert repo.finished[-1]["refusal_detected"] is False
    assert repo.finished[-1]["usage"].input_tokens == 100
    assert repo.finished[-1]["usage"].cached_input_tokens == 80
    assert repo.finished[-1]["usage"].output_tokens == 25
    assert repo.finished[-1]["usage"].reasoning_tokens == 7
    assert repo.outbox == [
        {
            "event_type": "judge.output.ready.v1",
            "judge_run_id": judge_run.judge_run_id,
            "judge_output_id": repo.judge_outputs[0]["judge_output_id"],
            "finish_reason": "completed",
            "refusal_detected": False,
        }
    ]


@pytest.mark.asyncio
async def test_refusal_writes_refusal_envelope_finishes_run_and_emits_ready_outbox() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = _job(judge_run)
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    service, client = _subject(repo=repo, responses=[_refusal_response()])

    await service.handle_job(job)

    assert len(client.calls) == 1
    assert repo.judge_outputs[0]["payload_json"] == {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(bundle.candidate_group_id),
        "output_kind": "refusal",
        "refusal_text": "I cannot comply.",
    }
    assert repo.finished[-1]["status"] == "succeeded"
    assert repo.finished[-1]["refusal_detected"] is True
    assert repo.outbox[0]["event_type"] == "judge.output.ready.v1"
    assert repo.outbox[0]["refusal_detected"] is True


@pytest.mark.asyncio
async def test_invalid_structured_payload_retries_once_then_fails_terminal_without_output_or_outbox() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = _job(judge_run)
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    service, client = _subject(repo=repo, responses=[_invalid_response(), _invalid_response()])

    await service.handle_job(job)

    assert len(client.calls) == 2
    assert repo.schema_retry_count == 1
    assert repo.finished[-1]["status"] == "failed_terminal"
    assert repo.finished[-1]["finish_reason"] == "schema_invalid_after_retry"
    assert repo.judge_outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_first_invalid_then_second_valid_succeeds_with_one_output_and_outbox() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = _job(judge_run)
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    payload = _valid_payload(bundle.candidate_group_id)
    service, client = _subject(repo=repo, responses=[_invalid_response(), _structured_response(payload)])

    await service.handle_job(job)

    assert len(client.calls) == 2
    assert repo.schema_retry_count == 1
    assert len(repo.judge_outputs) == 1
    assert repo.judge_outputs[0]["payload_json"] == payload
    assert repo.finished[-1]["status"] == "succeeded"
    assert len(repo.outbox) == 1


@pytest.mark.asyncio
async def test_missing_bundle_marks_terminal_and_does_not_call_fake_client() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = _job(judge_run)
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=None)
    service, client = _subject(repo=repo, responses=[_structured_response(_valid_payload(bundle.candidate_group_id))])

    await service.handle_job(job)

    assert client.calls == []
    assert repo.finished[-1]["status"] == "failed_terminal"
    assert repo.finished[-1]["finish_reason"] == "bundle_missing"
    assert repo.judge_outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_non_pending_judge_run_noops_and_does_not_call_fake_client() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle, status="succeeded")
    job = _job(judge_run)
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    service, client = _subject(repo=repo, responses=[_structured_response(_valid_payload(bundle.candidate_group_id))])

    await service.handle_job(job)

    assert client.calls == []
    assert repo.running == []
    assert repo.finished == []
    assert repo.judge_outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_bundle_id_mismatch_noops_and_does_not_call_fake_client() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = replace(_job(judge_run), bundle_id=uuid4())
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    service, client = _subject(repo=repo, responses=[_structured_response(_valid_payload(bundle.candidate_group_id))])

    await service.handle_job(job)

    assert client.calls == []
    assert repo.running == []
    assert repo.finished == []
    assert repo.judge_outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_fake_transport_failure_marks_retryable_and_emits_no_output_or_outbox() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = _job(judge_run)
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    service, client = _subject(repo=repo, responses=[OpenAITransientError("raw transport detail")])

    await service.handle_job(job)

    assert len(client.calls) == 1
    assert repo.finished[-1]["status"] == "failed_retryable"
    assert repo.finished[-1]["finish_reason"] == "openai_transport_retryable"
    assert repo.judge_outputs == []
    assert repo.outbox == []


@pytest.mark.asyncio
async def test_non_judge_call_event_type_noops_before_fake_client() -> None:
    bundle = _bundle()
    judge_run = _judge_run(bundle)
    job = _job(judge_run, event_type="other.event.v1")
    repo = FakeRepository(job=job, judge_run=judge_run, bundle=bundle)
    service, client = _subject(repo=repo, responses=[_structured_response(_valid_payload(bundle.candidate_group_id))])

    await service.handle_job(job)

    assert client.calls == []
    assert repo.running == []
    assert repo.finished == []
    assert repo.judge_outputs == []
    assert repo.outbox == []
