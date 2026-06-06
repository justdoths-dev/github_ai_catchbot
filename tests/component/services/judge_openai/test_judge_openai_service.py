from __future__ import annotations

import json
import logging
from uuid import UUID, uuid4

import pytest

from services.judge_openai.config import JudgeOpenAIConfig
from services.judge_openai import main as judge_openai_main
from services.judge_openai.models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord, StreamMessage
from services.judge_openai.openai_client import OpenAITransientError
from services.judge_openai.service import JudgeOpenAIService
from services.judge_openai.worker import JudgeOpenAIWorker


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.jobs: dict[UUID, JudgeCallJob] = {}
        self.runs: dict[UUID, JudgeRunRecord] = {}
        self.bundles: dict[UUID, BundleJudgeContext] = {}
        self.load_job_ids: list[UUID] = []
        self.status_history: list[str] = []
        self.schema_retry_increments = 0
        self.outputs: list[dict] = []
        self.outbox: list[dict] = []
        self.finished: list[dict] = []

    def transaction(self):
        return Tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        self.load_job_ids.append(trigger_event_id)
        return self.jobs.get(trigger_event_id)

    async def load_judge_run(self, judge_run_id: UUID):
        return self.runs.get(judge_run_id)

    async def load_bundle_context(self, bundle_id: UUID):
        return self.bundles.get(bundle_id)

    async def mark_judge_run_running(self, judge_run_id: UUID) -> None:
        run = self.runs[judge_run_id]
        self.runs[judge_run_id] = _replace_run_status(run, "running")
        self.status_history.append("running")

    async def increment_schema_retry_count(self, judge_run_id: UUID) -> None:
        self.schema_retry_increments += 1

    async def finish_judge_run(self, **kwargs) -> None:
        status = kwargs["status"]
        run = self.runs[kwargs["judge_run_id"]]
        self.runs[run.judge_run_id] = _replace_run_status(run, status)
        self.status_history.append(status)
        self.finished.append(kwargs)

    async def insert_judge_output(self, **kwargs):
        judge_output_id = uuid4()
        self.outputs.append({"judge_output_id": judge_output_id, **kwargs})
        return judge_output_id

    async def insert_judge_output_ready_outbox(self, **kwargs) -> None:
        self.outbox.append(kwargs)


class FakeClient:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self.message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self):
        return [self.message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


def _config() -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.analysis.judge",
        consumer_group="judge-openai",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        openai_api_key="test-key",
        openai_project=None,
        request_timeout_sec=1,
        max_output_tokens=800,
        enable_prompt_guard_preflight=False,
        log_level="INFO",
    )


def _run_record(
    *,
    status: str = "pending",
    bundle_id: UUID | None = None,
    profile: str = "github_primary",
    prompt_cache_key: str | None = "judge:github:v1",
):
    return JudgeRunRecord(
        judge_run_id=uuid4(),
        bundle_id=bundle_id or uuid4(),
        judge_profile=profile,
        model="gpt-5.4-mini",
        reasoning_effort="low",
        prompt_version="judge_github_primary_v1",
        schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        prompt_cache_key=prompt_cache_key,
        status=status,
        schema_retry_count=0,
    )


def _job(run: JudgeRunRecord, *, bundle_id: UUID | None = None) -> JudgeCallJob:
    return JudgeCallJob(
        trigger_event_id=uuid4(),
        event_type="judge.call.requested.v1",
        judge_run_id=run.judge_run_id,
        bundle_id=bundle_id or run.bundle_id,
        model=run.model,
        reasoning_effort=run.reasoning_effort,
        prompt_version=run.prompt_version,
        prompt_cache_key=run.prompt_cache_key,
    )


def _bundle(bundle_id: UUID, *, primary_summary=None) -> BundleJudgeContext:
    return BundleJudgeContext(
        bundle_id=bundle_id,
        candidate_group_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        primary_summary=primary_summary if primary_summary is not None else {"title": "useful repo"},
        supporting_summaries_json=[],
        discovered_links_summary_json=[],
        evidence_limitations=[],
        token_budget_profile="small",
        reroot_count=0,
    )


def _success_response(*, verdict: str = "later") -> dict:
    return {
        "id": "resp_success",
        "status": "completed",
        "output_text": json.dumps(
            {
                "judge_schema_version": "judge_output_v1",
                "candidate_group_id": str(uuid4()),
                "headline": "A useful repo",
                "summary_one_line_ko": "요약",
                "skeptical_take_ko": "회의적 평가",
                "why_it_might_matter_ko": "이유",
                "comparables": ["existing tool"],
                "scores": {
                    "novelty": 42,
                    "practical_usefulness": 70,
                    "evidence_strength": 64,
                    "hype_penalty": 18,
                    "confidence": 61,
                    "code_quality": 55,
                    "maintenance_signal": 48,
                    "specificity": 72,
                    "reproducibility_signal": None,
                },
                "reason_codes": ["specific_evidence"],
                "red_flags_ko": ["과장 가능성"],
                "evidence_limitations_ko": ["제한"],
                "recommended_action_ko": "나중에 확인",
                "freshness_note_ko": "신선도 제한",
                "model_proposed_verdict": verdict,
                "model_confidence_band": "medium",
            }
        ),
        "usage": {
            "input_tokens": 100,
            "input_tokens_details": {"cached_tokens": 50},
            "output_tokens": 40,
            "output_tokens_details": {"reasoning_tokens": 5},
        },
    }


def _refusal_response() -> dict:
    return {
        "id": "resp_refusal",
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [{"type": "refusal", "refusal": "I cannot evaluate this."}],
            }
        ],
        "usage": {"input_tokens": 30, "output_tokens": 8},
    }


async def _handle(repository: FakeRepository, client: FakeClient, job: JudgeCallJob) -> None:
    service = JudgeOpenAIService(_config(), repository=repository, openai_client=client)  # type: ignore[arg-type]
    await service.handle_job(job)


def test_success_response_fixture_uses_corrected_score_payload() -> None:
    payload = json.loads(_success_response()["output_text"])

    assert payload["scores"] == {
        "novelty": 42,
        "practical_usefulness": 70,
        "evidence_strength": 64,
        "hype_penalty": 18,
        "confidence": 61,
        "code_quality": 55,
        "maintenance_signal": 48,
        "specificity": 72,
        "reproducibility_signal": None,
    }
    assert "usefulness" not in payload["scores"]
    assert "execution_risk" not in payload["scores"]


def _replace_run_status(run: JudgeRunRecord, status: str) -> JudgeRunRecord:
    return JudgeRunRecord(
        judge_run_id=run.judge_run_id,
        bundle_id=run.bundle_id,
        judge_profile=run.judge_profile,
        model=run.model,
        reasoning_effort=run.reasoning_effort,
        prompt_version=run.prompt_version,
        schema_version=run.schema_version,
        policy_version=run.policy_version,
        prompt_cache_key=run.prompt_cache_key,
        status=status,
        schema_retry_count=run.schema_retry_count,
    )


@pytest.mark.asyncio
async def test_worker_rehydrates_judge_call_from_event_outbox_trigger_id_not_redis_business_payload() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.jobs[job.trigger_event_id] = job
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([_success_response()])
    service = JudgeOpenAIService(_config(), repository=repository, openai_client=client)  # type: ignore[arg-type]
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.analysis.judge",
            message_id="1-0",
            fields={
                "trigger_event_id": str(job.trigger_event_id),
                "judge_run_id": str(uuid4()),
                "bundle_id": str(uuid4()),
            },
        )
    )
    worker = JudgeOpenAIWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert repository.load_job_ids == [job.trigger_event_id]
    assert client.calls[0]["model"] == run.model
    assert consumer.acked == ["1-0"]


@pytest.mark.asyncio
async def test_runtime_session_backed_service_does_not_hold_transaction_across_openai_call(monkeypatch) -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.jobs[job.trigger_event_id] = job
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)

    state = {"active_transactions": 0, "events": []}

    class FakeSession:
        def __init__(self, tx_id: int) -> None:
            self.tx_id = tx_id

    class FakeSessionTransaction:
        def __init__(self, tx_id: int) -> None:
            self.tx_id = tx_id

        async def __aenter__(self):
            state["active_transactions"] += 1
            state["events"].append(("tx_enter", self.tx_id))
            return FakeSession(self.tx_id)

        async def __aexit__(self, exc_type, exc, tb):
            state["events"].append(("tx_exit", self.tx_id))
            state["active_transactions"] -= 1
            return False

    class FakeSessionFactory:
        def __init__(self) -> None:
            self.next_tx_id = 0

        def begin(self):
            self.next_tx_id += 1
            return FakeSessionTransaction(self.next_tx_id)

    class RecordingRepository:
        def __init__(self, session: FakeSession) -> None:
            self.session = session

        async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
            state["events"].append(("load_job", self.session.tx_id))
            return await repository.load_job_by_trigger_event_id(trigger_event_id)

        async def load_judge_run(self, judge_run_id: UUID):
            state["events"].append(("load_run", self.session.tx_id))
            return await repository.load_judge_run(judge_run_id)

        async def load_bundle_context(self, bundle_id: UUID):
            state["events"].append(("load_bundle", self.session.tx_id))
            return await repository.load_bundle_context(bundle_id)

        async def mark_judge_run_running(self, judge_run_id: UUID) -> None:
            state["events"].append(("mark_running", self.session.tx_id))
            await repository.mark_judge_run_running(judge_run_id)

        async def increment_schema_retry_count(self, judge_run_id: UUID) -> None:
            state["events"].append(("increment_retry", self.session.tx_id))
            await repository.increment_schema_retry_count(judge_run_id)

        async def finish_judge_run(self, **kwargs) -> None:
            state["events"].append(("finish_run", self.session.tx_id))
            await repository.finish_judge_run(**kwargs)

        async def insert_judge_output(self, **kwargs):
            state["events"].append(("insert_output", self.session.tx_id))
            return await repository.insert_judge_output(**kwargs)

        async def insert_judge_output_ready_outbox(self, **kwargs) -> None:
            state["events"].append(("insert_outbox", self.session.tx_id))
            await repository.insert_judge_output_ready_outbox(**kwargs)

    class TransactionAssertingClient(FakeClient):
        async def create_structured_response(self, **kwargs):
            state["events"].append(("openai_call", None))
            assert state["active_transactions"] == 0
            return await super().create_structured_response(**kwargs)

    monkeypatch.setattr(judge_openai_main, "JudgeOpenAIRepository", RecordingRepository)
    client = TransactionAssertingClient([_success_response()])
    service = judge_openai_main.SessionBackedJudgeOpenAIService(
        _config(),
        session_factory=FakeSessionFactory(),
        openai_client=client,  # type: ignore[arg-type]
        logger=logging.getLogger("test"),
    )

    await service.handle_trigger_event(str(job.trigger_event_id))

    events = state["events"]
    mark_running_event = next(event for event in events if event[0] == "mark_running")
    mark_running_tx_id = mark_running_event[1]
    assert events.index(("tx_exit", mark_running_tx_id)) < events.index(("openai_call", None))

    final_write_tx_ids = {
        event[1]
        for event in events
        if event[0] in {"insert_output", "finish_run", "insert_outbox"}
    }
    assert len(final_write_tx_ids) == 1
    assert state["active_transactions"] == 0
    assert repository.status_history == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_non_pending_judge_run_noops_without_openai_call() -> None:
    repository = FakeRepository()
    run = _run_record(status="succeeded")
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    client = FakeClient([])

    await _handle(repository, client, job)

    assert client.calls == []
    assert repository.outputs == []
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_judge_run_bundle_mismatch_noops_without_openai_call() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run, bundle_id=uuid4())
    repository.runs[run.judge_run_id] = run
    client = FakeClient([])

    await _handle(repository, client, job)

    assert client.calls == []
    assert repository.outputs == []
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_bundle_missing_marks_failed_terminal_without_outbox() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    client = FakeClient([])

    await _handle(repository, client, job)

    assert repository.status_history == ["failed_terminal"]
    assert repository.finished[0]["finish_reason"] == "bundle_missing"
    assert client.calls == []
    assert repository.outputs == []
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_successful_structured_output_writes_output_outbox_and_usage() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([_success_response(verdict="inspect_now")])

    await _handle(repository, client, job)

    assert repository.status_history == ["running", "succeeded"]
    assert len(repository.outputs) == 1
    assert repository.outputs[0]["model_proposed_verdict"] == "inspect_now"
    assert len(repository.outbox) == 1
    assert repository.outbox[0]["judge_run_id"] == run.judge_run_id
    assert repository.finished[0]["usage"].input_tokens == 100
    assert repository.finished[0]["usage"].cached_input_tokens == 50
    assert repository.finished[0]["usage"].output_tokens == 40
    assert repository.finished[0]["usage"].reasoning_tokens == 5


@pytest.mark.asyncio
async def test_missing_prompt_cache_key_is_backward_compatible_for_legacy_events() -> None:
    repository = FakeRepository()
    run = _run_record(prompt_cache_key=None)
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([_success_response()])

    await _handle(repository, client, job)

    assert client.calls[0]["prompt_cache_key"] is None
    assert repository.status_history == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_refusal_writes_envelope_marks_succeeded_and_emits_outbox() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([_refusal_response()])

    await _handle(repository, client, job)

    assert repository.status_history == ["running", "succeeded"]
    assert len(repository.outputs) == 1
    assert repository.outputs[0]["payload_json"]["output_kind"] == "refusal"
    assert repository.finished[0]["refusal_detected"] is True
    assert len(repository.outbox) == 1
    assert repository.outbox[0]["refusal_detected"] is True


@pytest.mark.asyncio
async def test_schema_parse_failure_once_then_success_increments_retry_and_writes_output() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([{"output_text": "not json"}, _success_response()])

    await _handle(repository, client, job)

    assert repository.schema_retry_increments == 1
    assert len(client.calls) == 2
    assert len(repository.outputs) == 1
    assert repository.status_history == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_schema_parse_failure_twice_marks_terminal_without_output_or_outbox() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([{"output_text": "not json"}, {"output_text": "still not json"}])

    await _handle(repository, client, job)

    assert repository.schema_retry_increments == 1
    assert len(client.calls) == 2
    assert repository.status_history == ["running", "failed_terminal"]
    assert repository.finished[0]["finish_reason"] == "schema_invalid_after_retry"
    assert repository.outputs == []
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_openai_transient_exception_marks_failed_retryable_without_output_or_outbox() -> None:
    repository = FakeRepository()
    run = _run_record()
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([OpenAITransientError("RateLimitError")])

    await _handle(repository, client, job)

    assert repository.status_history == ["running", "failed_retryable"]
    assert repository.finished[0]["finish_reason"] == "openai_transport_retryable"
    assert repository.outputs == []
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_unsupported_prompt_profile_marks_failed_terminal_without_openai_output_or_outbox() -> None:
    repository = FakeRepository()
    run = _run_record(profile="web_primary")
    job = _job(run)
    repository.runs[run.judge_run_id] = run
    repository.bundles[run.bundle_id] = _bundle(run.bundle_id)
    client = FakeClient([])

    await _handle(repository, client, job)

    assert repository.status_history == ["failed_terminal"]
    assert repository.finished[0]["finish_reason"] == "unsupported_judge_profile"
    assert client.calls == []
    assert repository.outputs == []
    assert repository.outbox == []
