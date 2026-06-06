from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.judge_openai.config import JudgeOpenAIConfig
from services.judge_openai.models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord, StreamMessage
from services.judge_openai.openai_client import OpenAITransientError
from services.judge_openai.service import JudgeOpenAIService
from services.judge_openai.worker import JudgeOpenAIWorker


FIXTURE_ROOT = Path(__file__).resolve().parents[3] / "fixtures" / "upstream"


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


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


class JudgeCallLedger:
    def __init__(self, fixture: dict) -> None:
        self.fixture = fixture
        self.trigger_event_id = UUID(fixture["trigger_event_id"])
        self.judge_run_id = UUID(fixture["judge_run_id"])
        self.bundle_id = UUID(fixture["bundle_id"])
        self.candidate_group_id = UUID(fixture["candidate_group_id"])
        self.event_outbox: list[dict] = []
        self._event_by_id: dict[UUID, dict] = {}
        self._outbox_dedupe_keys: set[str] = set()
        self.runs = {
            self.judge_run_id: JudgeRunRecord(
                judge_run_id=self.judge_run_id,
                bundle_id=self.bundle_id,
                judge_profile=fixture["judge_profile"],
                model=fixture["model"],
                reasoning_effort=fixture["reasoning_effort"],
                prompt_version=fixture["prompt_version"],
                schema_version=fixture["schema_version"],
                policy_version=fixture["policy_version"],
                prompt_cache_key=fixture["prompt_cache_key"],
                status="pending",
                schema_retry_count=0,
            )
        }
        self.bundles = {
            self.bundle_id: BundleJudgeContext(
                bundle_id=self.bundle_id,
                candidate_group_id=self.candidate_group_id,
                current_primary_artifact_id=UUID(fixture["current_primary_artifact_id"]),
                primary_summary=fixture["primary_summary"],
                supporting_summaries_json=fixture["supporting_summaries_json"],
                discovered_links_summary_json=fixture["discovered_links_summary_json"],
                evidence_limitations=fixture["evidence_limitations"],
                token_budget_profile=fixture["token_budget_profile"],
                reroot_count=int(fixture["reroot_count"]),
            )
        }
        self.loaded_trigger_event_ids: list[UUID] = []
        self.status_history: list[str] = []
        self.schema_retry_increments = 0
        self.outputs: list[dict] = []
        self.finished: list[dict] = []
        self._append_event(
            event_id=self.trigger_event_id,
            event_type="judge.call.requested.v1",
            aggregate_type="judge_run",
            aggregate_id=self.judge_run_id,
            dedupe_key=f"judge-call:{self.judge_run_id}",
            payload_json={
                "judge_run_id": str(self.judge_run_id),
                "candidate_group_id": str(self.candidate_group_id),
                "bundle_id": str(self.bundle_id),
                "judge_profile": fixture["judge_profile"],
                "model": fixture["model"],
                "reasoning_effort": fixture["reasoning_effort"],
                "prompt_version": fixture["prompt_version"],
                "prompt_cache_key": fixture["prompt_cache_key"],
            },
        )

    def transaction(self):
        return Tx()

    def _append_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        dedupe_key: str,
        payload_json: dict,
        event_id: UUID | None = None,
    ) -> UUID:
        if dedupe_key in self._outbox_dedupe_keys:
            return next(row["event_id"] for row in self.event_outbox if row["dedupe_key"] == dedupe_key)
        event_id = event_id or uuid4()
        row = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "dedupe_key": dedupe_key,
            "payload_json": payload_json,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
        }
        self.event_outbox.append(row)
        self._event_by_id[event_id] = row
        self._outbox_dedupe_keys.add(dedupe_key)
        return event_id

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        self.loaded_trigger_event_ids.append(trigger_event_id)
        row = self._event_by_id.get(trigger_event_id)
        if row is None or row["event_type"] != "judge.call.requested.v1":
            return None
        payload = row["payload_json"]
        return JudgeCallJob(
            trigger_event_id=trigger_event_id,
            event_type=row["event_type"],
            judge_run_id=UUID(payload["judge_run_id"]),
            bundle_id=UUID(payload["bundle_id"]),
            model=payload["model"],
            reasoning_effort=payload["reasoning_effort"],
            prompt_version=payload["prompt_version"],
            prompt_cache_key=payload.get("prompt_cache_key"),
        )

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
        run = self.runs[kwargs["judge_run_id"]]
        self.runs[run.judge_run_id] = _replace_run_status(run, kwargs["status"])
        self.status_history.append(kwargs["status"])
        self.finished.append(kwargs)

    async def insert_judge_output(self, **kwargs):
        judge_output_id = uuid4()
        self.outputs.append({"judge_output_id": judge_output_id, **kwargs})
        return judge_output_id

    async def insert_judge_output_ready_outbox(self, **kwargs) -> None:
        payload_json = {
            "judge_run_id": str(kwargs["judge_run_id"]),
            "judge_output_id": str(kwargs["judge_output_id"]),
            "finish_reason": kwargs["finish_reason"],
            "refusal_detected": kwargs["refusal_detected"],
        }
        self._append_event(
            event_type="judge.output.ready.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=f"judge-output-ready:{kwargs['judge_run_id']}:{kwargs['judge_output_id']}",
            payload_json=payload_json,
        )


class FakeClient:
    def __init__(self, responses: list[object]) -> None:
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _config() -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.judge",
        consumer_group="judge-openai",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        openai_api_key="unused",
        openai_project=None,
        request_timeout_sec=1.0,
        max_output_tokens=800,
        enable_prompt_guard_preflight=False,
        log_level="INFO",
    )


def _load_fixture() -> dict:
    return json.loads((FIXTURE_ROOT / "judge_call_requested_ready_bundle.json").read_text(encoding="utf-8"))


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


def _valid_payload(candidate_group_id: UUID, *, verdict: str = "later") -> dict:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Fixture project",
        "summary_one_line_ko": "summary",
        "skeptical_take_ko": "skeptical take",
        "why_it_might_matter_ko": "why it matters",
        "comparables": ["existing project"],
        "scores": {
            "novelty": 50,
            "practical_usefulness": 60,
            "evidence_strength": 70,
            "hype_penalty": 10,
            "confidence": 65,
            "code_quality": 55,
            "maintenance_signal": 45,
            "specificity": 75,
            "reproducibility_signal": None,
        },
        "reason_codes": ["specific_evidence"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["fixture only"],
        "recommended_action_ko": "inspect",
        "freshness_note_ko": "freshness unknown",
        "model_proposed_verdict": verdict,
        "model_confidence_band": "medium",
    }


def _success_response(candidate_group_id: UUID) -> dict:
    return {
        "status": "completed",
        "output_text": json.dumps(_valid_payload(candidate_group_id)),
        "usage": {
            "input_tokens": 90,
            "input_tokens_details": {"cached_tokens": 70},
            "output_tokens": 20,
            "output_tokens_details": {"reasoning_tokens": 6},
        },
    }


def _refusal_response() -> dict:
    return {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [{"type": "refusal", "refusal": "I cannot evaluate this."}],
            }
        ],
        "usage": {"input_tokens": 30, "output_tokens": 8},
    }


async def _run_worker(ledger: JudgeCallLedger, client: FakeClient, *, message_fields: dict[str, str] | None = None):
    fields = {"trigger_event_id": str(ledger.trigger_event_id)}
    if message_fields:
        fields.update(message_fields)
    service = JudgeOpenAIService(_config(), repository=ledger, openai_client=client)  # type: ignore[arg-type]
    consumer = FakeConsumer(StreamMessage(stream="q.analysis.judge", message_id="1-0", fields=fields))
    worker = JudgeOpenAIWorker(_config(), consumer=consumer, service=service)
    return await worker.run_once(), consumer


def _ready_events(ledger: JudgeCallLedger) -> list[dict]:
    return [row for row in ledger.event_outbox if row["event_type"] == "judge.output.ready.v1"]


@pytest.mark.asyncio
async def test_judge_call_requested_creates_output_and_thin_ready_event_from_event_outbox() -> None:
    ledger = JudgeCallLedger(_load_fixture())
    client = FakeClient([_success_response(ledger.candidate_group_id)])

    result, consumer = await _run_worker(
        ledger,
        client,
        message_fields={
            "judge_run_id": str(uuid4()),
            "bundle_id": str(uuid4()),
            "payload_json": json.dumps({"model": "do-not-trust"}),
        },
    )

    ready_events = _ready_events(ledger)
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert ledger.loaded_trigger_event_ids == [ledger.trigger_event_id]
    assert ledger.status_history == ["running", "succeeded"]
    assert len(client.calls) == 1
    assert client.calls[0]["model"] == ledger.fixture["model"]
    assert len(ledger.outputs) == 1
    assert ledger.outputs[0]["payload_json"]["judge_schema_version"] == "judge_output_v1"
    assert len(ready_events) == 1
    assert ready_events[0]["aggregate_type"] == "judge_run"
    assert ready_events[0]["aggregate_id"] == ledger.judge_run_id
    assert set(ready_events[0]["payload_json"]) == {
        "judge_run_id",
        "judge_output_id",
        "finish_reason",
        "refusal_detected",
    }


@pytest.mark.asyncio
async def test_duplicate_non_pending_judge_run_does_not_create_duplicate_output_or_outbox() -> None:
    ledger = JudgeCallLedger(_load_fixture())
    client = FakeClient([_success_response(ledger.candidate_group_id)])

    await _run_worker(ledger, client)
    before_outputs = deepcopy(ledger.outputs)
    before_ready_events = deepcopy(_ready_events(ledger))
    await _run_worker(ledger, client)

    assert ledger.status_history == ["running", "succeeded"]
    assert ledger.outputs == before_outputs
    assert _ready_events(ledger) == before_ready_events
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_fake_refusal_still_emits_judge_output_ready() -> None:
    ledger = JudgeCallLedger(_load_fixture())
    client = FakeClient([_refusal_response()])

    await _run_worker(ledger, client)

    ready_events = _ready_events(ledger)
    assert ledger.status_history == ["running", "succeeded"]
    assert ledger.outputs[0]["payload_json"]["output_kind"] == "refusal"
    assert ledger.finished[0]["refusal_detected"] is True
    assert len(ready_events) == 1
    assert ready_events[0]["payload_json"]["refusal_detected"] is True


@pytest.mark.asyncio
async def test_invalid_twice_fails_terminal_without_output_or_outbox() -> None:
    ledger = JudgeCallLedger(_load_fixture())
    client = FakeClient([{"output_text": "not json"}, {"output_text": "still not json"}])

    await _run_worker(ledger, client)

    assert len(client.calls) == 2
    assert ledger.schema_retry_increments == 1
    assert ledger.status_history == ["running", "failed_terminal"]
    assert ledger.finished[0]["finish_reason"] == "schema_invalid_after_retry"
    assert ledger.outputs == []
    assert _ready_events(ledger) == []


@pytest.mark.asyncio
async def test_transient_error_fails_retryable_without_output_or_outbox() -> None:
    ledger = JudgeCallLedger(_load_fixture())
    client = FakeClient([OpenAITransientError("RateLimitError")])

    await _run_worker(ledger, client)

    assert len(client.calls) == 1
    assert ledger.status_history == ["running", "failed_retryable"]
    assert ledger.finished[0]["finish_reason"] == "openai_transport_retryable"
    assert ledger.outputs == []
    assert _ready_events(ledger) == []
