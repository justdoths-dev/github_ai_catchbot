from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.judge_openai.config import JudgeOpenAIConfig
from services.judge_openai.models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord, StreamMessage
from services.judge_openai.service import JudgeOpenAIService
from services.judge_openai.worker import JudgeOpenAIWorker


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "upstream"


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


class FakeClient:
    def __init__(self, response: dict) -> None:
        self.response = response
        self.calls: list[dict] = []

    async def create_structured_response(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class JudgeOutputReadyLedger:
    def __init__(self, fixture: dict) -> None:
        self.fixture = fixture
        self.trigger_event_id = UUID(fixture["trigger_event_id"])
        self.judge_run_id = UUID(fixture["judge_run_id"])
        self.bundle_id = UUID(fixture["bundle_id"])
        self.candidate_group_id = UUID(fixture["candidate_group_id"])
        self.event_outbox: list[dict] = []
        self._event_by_id: dict[UUID, dict] = {}
        self._outbox_dedupe_keys: set[str] = set()
        self.candidate_evidence_bundles = {
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
        self.judge_runs = {
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
        self.judge_run_updates: list[dict] = []
        self.judge_outputs: list[dict] = []
        self.analyses: list[dict] = []
        self.state_transitions: list[dict] = []
        self.notification_plans: list[dict] = []
        self.notification_renders: list[dict] = []
        self.notification_delivery_records: list[dict] = []
        self.replay_requests: list[dict] = []
        self.dead_letter_entries: list[dict] = []
        self.redis_dispatches: list[dict] = []
        self.telegram_calls: list[dict] = []
        self.maintenance_calls: list[dict] = []
        self.policy_calls: list[dict] = []
        self.validator_calls: list[dict] = []
        self.loaded_trigger_event_ids: list[UUID] = []
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
        return self.judge_runs.get(judge_run_id)

    async def load_bundle_context(self, bundle_id: UUID):
        return self.candidate_evidence_bundles.get(bundle_id)

    async def mark_judge_run_running(self, judge_run_id: UUID) -> None:
        run = self.judge_runs[judge_run_id]
        self.judge_runs[judge_run_id] = _replace_run_status(run, "running")
        self.judge_run_updates.append({"status": "running"})

    async def increment_schema_retry_count(self, judge_run_id: UUID) -> None:
        run = self.judge_runs[judge_run_id]
        self.judge_runs[judge_run_id] = JudgeRunRecord(
            judge_run_id=run.judge_run_id,
            bundle_id=run.bundle_id,
            judge_profile=run.judge_profile,
            model=run.model,
            reasoning_effort=run.reasoning_effort,
            prompt_version=run.prompt_version,
            schema_version=run.schema_version,
            policy_version=run.policy_version,
            prompt_cache_key=run.prompt_cache_key,
            status=run.status,
            schema_retry_count=run.schema_retry_count + 1,
        )
        self.judge_run_updates.append({"schema_retry_count": run.schema_retry_count + 1})

    async def finish_judge_run(self, **kwargs) -> None:
        run = self.judge_runs[kwargs["judge_run_id"]]
        self.judge_runs[run.judge_run_id] = _replace_run_status(run, kwargs["status"])
        self.judge_run_updates.append(
            {
                "status": kwargs["status"],
                "input_tokens": kwargs["usage"].input_tokens if kwargs["usage"] else None,
                "cached_input_tokens": kwargs["usage"].cached_input_tokens if kwargs["usage"] else None,
                "output_tokens": kwargs["usage"].output_tokens if kwargs["usage"] else None,
                "reasoning_tokens": kwargs["usage"].reasoning_tokens if kwargs["usage"] else None,
                "latency_ms_present": kwargs["usage"].latency_ms is not None if kwargs["usage"] else False,
                "finish_reason": kwargs["finish_reason"],
                "refusal_detected": kwargs["refusal_detected"],
            }
        )

    async def insert_judge_output(self, **kwargs):
        judge_output_id = uuid4()
        self.judge_outputs.append({"judge_output_id": judge_output_id, **kwargs})
        return judge_output_id

    async def insert_judge_output_ready_outbox(self, **kwargs) -> None:
        self._append_event(
            event_type="judge.output.ready.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=f"judge-output-ready:{kwargs['judge_run_id']}:{kwargs['judge_output_id']}",
            payload_json={
                "judge_run_id": str(kwargs["judge_run_id"]),
                "judge_output_id": str(kwargs["judge_output_id"]),
                "finish_reason": kwargs["finish_reason"],
                "refusal_detected": kwargs["refusal_detected"],
            },
        )


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


def _config() -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
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


def _load_fixture() -> dict:
    return json.loads((FIXTURE_ROOT / "judge_call_requested_ready_bundle.json").read_text(encoding="utf-8"))


def _payload(candidate_group_id: UUID) -> dict:
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
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _response(candidate_group_id: UUID) -> dict:
    return {
        "status": "completed",
        "output_text": json.dumps(_payload(candidate_group_id)),
        "usage": {
            "input_tokens": 90,
            "input_tokens_details": {"cached_tokens": 70},
            "output_tokens": 20,
            "output_tokens_details": {"reasoning_tokens": 6},
        },
    }


def _forbidden_state(ledger: JudgeOutputReadyLedger) -> dict:
    return {
        "analyses": ledger.analyses,
        "state_transitions": ledger.state_transitions,
        "notification_plans": ledger.notification_plans,
        "notification_renders": ledger.notification_renders,
        "notification_delivery_records": ledger.notification_delivery_records,
        "replay_requests": ledger.replay_requests,
        "dead_letter_entries": ledger.dead_letter_entries,
        "redis_dispatches": ledger.redis_dispatches,
        "telegram_calls": ledger.telegram_calls,
        "maintenance_calls": ledger.maintenance_calls,
        "policy_calls": ledger.policy_calls,
        "validator_calls": ledger.validator_calls,
    }


def _install_downstream_tripwires(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.analysis_validator import worker as analysis_validator_worker
    from services.maintenance import batch_recovery_tool, worker as maintenance_worker
    from services.notifier_telegram import telegram_client, worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.policy_engine import worker as policy_worker

    def fail_downstream(*args, **kwargs):
        raise AssertionError("judge-openai handoff acceptance must stop at judge.output.ready.v1")

    monkeypatch.setattr(analysis_validator_worker, "AnalysisValidatorWorker", fail_downstream)
    monkeypatch.setattr(policy_worker, "PolicyEngineWorker", fail_downstream)
    monkeypatch.setattr(notifier_worker, "NotifierTelegramWorker", fail_downstream)
    monkeypatch.setattr(telegram_client, "TelegramBotClient", fail_downstream)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "MaintenanceQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "ReplayQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "DueRetryPromotionWorker", fail_downstream)
    monkeypatch.setattr(batch_recovery_tool, "DeliveryBatchRecoveryTool", fail_downstream)


@pytest.mark.asyncio
async def test_judge_call_requested_to_output_ready_preserves_downstream_boundaries(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = JudgeOutputReadyLedger(_load_fixture())
    before_forbidden = deepcopy(_forbidden_state(ledger))
    decoy_judge_run_id = uuid4()
    decoy_bundle_id = uuid4()
    service = JudgeOpenAIService(
        _config(),
        repository=ledger,  # type: ignore[arg-type]
        openai_client=FakeClient(_response(ledger.candidate_group_id)),
    )
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.analysis.judge",
            message_id="1-0",
            fields={
                "trigger_event_id": str(ledger.trigger_event_id),
                "judge_run_id": str(decoy_judge_run_id),
                "bundle_id": str(decoy_bundle_id),
                "model": "do-not-trust",
            },
        )
    )
    worker = JudgeOpenAIWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    ready_events = [row for row in ledger.event_outbox if row["event_type"] == "judge.output.ready.v1"]
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert ledger.loaded_trigger_event_ids == [ledger.trigger_event_id]
    assert ledger.judge_run_updates[0] == {"status": "running"}
    assert ledger.judge_run_updates[-1]["status"] == "succeeded"
    assert ledger.judge_run_updates[-1]["input_tokens"] == 90
    assert ledger.judge_run_updates[-1]["cached_input_tokens"] == 70
    assert ledger.judge_run_updates[-1]["output_tokens"] == 20
    assert ledger.judge_run_updates[-1]["reasoning_tokens"] == 6
    assert ledger.judge_run_updates[-1]["latency_ms_present"] is True
    assert ledger.judge_run_updates[-1]["finish_reason"] == "completed"
    assert ledger.judge_run_updates[-1]["refusal_detected"] is False
    assert len(ledger.judge_outputs) == 1
    assert ledger.judge_outputs[0]["judge_run_id"] == ledger.judge_run_id
    assert ledger.judge_outputs[0]["candidate_group_id"] == ledger.candidate_group_id
    assert len(ready_events) == 1
    assert ready_events[0]["aggregate_type"] == "judge_run"
    assert ready_events[0]["aggregate_id"] == ledger.judge_run_id
    assert set(ready_events[0]["payload_json"]) == {
        "judge_run_id",
        "judge_output_id",
        "finish_reason",
        "refusal_detected",
    }
    assert ready_events[0]["payload_json"]["finish_reason"] == "completed"
    assert ready_events[0]["payload_json"]["refusal_detected"] is False
    assert _forbidden_state(ledger) == before_forbidden
