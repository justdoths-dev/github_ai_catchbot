from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.analysis_validator.config import AnalysisValidatorConfig
from services.analysis_validator.models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
    StreamMessage,
)
from services.analysis_validator.service import AnalysisValidatorService
from services.analysis_validator.worker import AnalysisValidatorWorker


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


class JudgeOutputReadyPolicyLedger:
    def __init__(self, fixture: dict) -> None:
        self.fixture = fixture
        self.trigger_event_id = UUID(fixture["trigger_event_id"])
        self.judge_run_id = UUID(fixture["judge_run_id"])
        self.judge_output_id = UUID(fixture["judge_output_id"])
        self.bundle_id = UUID(fixture["bundle_id"])
        self.candidate_group_id = UUID(fixture["candidate_group_id"])
        self.current_primary_artifact_id = UUID(fixture["current_primary_artifact_id"])
        self.event_outbox: list[dict] = []
        self._event_by_id: dict[UUID, dict] = {}
        self._outbox_dedupe_keys: set[str] = set()
        self.loaded_trigger_event_ids: list[UUID] = []
        self.loaded_judge_run_ids: list[UUID] = []
        self.loaded_judge_output_ids: list[UUID] = []
        self.loaded_bundle_ids: list[UUID] = []
        self.judge_run_updates: list[dict] = []
        self.state_transitions: list[dict] = []
        self.analyses: list[dict] = []
        self.notification_plans: list[dict] = []
        self.notification_renders: list[dict] = []
        self.notification_delivery_records: list[dict] = []
        self.redis_dispatches: list[dict] = []
        self.telegram_calls: list[dict] = []
        self.maintenance_calls: list[dict] = []
        self.policy_calls: list[dict] = []
        self.replay_requests: list[dict] = []
        self.bundle_refresh_requests: list[dict] = []
        self.judge_openai_calls: list[dict] = []
        self.judge_outputs = {
            self.judge_output_id: JudgeOutputRecord(
                judge_output_id=self.judge_output_id,
                judge_run_id=self.judge_run_id,
                candidate_group_id=self.candidate_group_id,
                judge_schema_version=fixture["schema_version"],
                payload_json=_valid_judge_output_payload(self.candidate_group_id),
                model_proposed_verdict="later",
                model_confidence_band="medium",
                created_at=datetime.now(timezone.utc),
            )
        }
        self.judge_runs = {
            self.judge_run_id: JudgeRunValidationRecord(
                judge_run_id=self.judge_run_id,
                bundle_id=self.bundle_id,
                judge_profile=fixture["judge_profile"],
                schema_version=fixture["schema_version"],
                policy_version=fixture["policy_version"],
                status="succeeded",
                finish_reason=fixture["finish_reason"],
                refusal_detected=bool(fixture["refusal_detected"]),
            )
        }
        self.candidate_evidence_bundles = {
            self.bundle_id: BundleValidationContext(
                bundle_id=self.bundle_id,
                candidate_group_id=self.candidate_group_id,
                current_primary_artifact_id=self.current_primary_artifact_id,
                current_primary_artifact_type=fixture["current_primary_artifact_type"],
                created_at=datetime.now(timezone.utc),
            )
        }
        self._append_event(
            event_id=self.trigger_event_id,
            event_type="judge.output.ready.v1",
            aggregate_type="judge_run",
            aggregate_id=self.judge_run_id,
            dedupe_key=f"judge-output-ready:{self.judge_run_id}:{self.judge_output_id}",
            payload_json={
                "judge_run_id": str(self.judge_run_id),
                "judge_output_id": str(self.judge_output_id),
                "finish_reason": fixture["finish_reason"],
                "refusal_detected": bool(fixture["refusal_detected"]),
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

    def set_ready_payload(self, **updates) -> None:
        row = self._event_by_id[self.trigger_event_id]
        row["payload_json"] = {**row["payload_json"], **updates}

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        self.loaded_trigger_event_ids.append(trigger_event_id)
        row = self._event_by_id.get(trigger_event_id)
        if row is None or row["event_type"] != "judge.output.ready.v1":
            return None
        payload = row["payload_json"]
        if "finish_reason" not in payload or "refusal_detected" not in payload:
            return None
        return JudgeOutputReadyJob(
            trigger_event_id=trigger_event_id,
            event_type=row["event_type"],
            judge_run_id=UUID(payload["judge_run_id"]),
            judge_output_id=UUID(payload["judge_output_id"]),
            finish_reason=payload["finish_reason"],
            refusal_detected=bool(payload["refusal_detected"]),
        )

    async def load_judge_run(self, judge_run_id: UUID):
        self.loaded_judge_run_ids.append(judge_run_id)
        return self.judge_runs.get(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID):
        self.loaded_judge_output_ids.append(judge_output_id)
        return self.judge_outputs.get(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID):
        self.loaded_bundle_ids.append(bundle_id)
        return self.candidate_evidence_bundles.get(bundle_id)

    async def update_judge_run_status(self, *, judge_run_id: UUID, status: str, finish_reason: str | None) -> None:
        run = self.judge_runs[judge_run_id]
        self.judge_runs[judge_run_id] = replace(run, status=status, finish_reason=finish_reason)
        self.judge_run_updates.append(
            {
                "judge_run_id": judge_run_id,
                "status": status,
                "finish_reason": finish_reason,
            }
        )

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

    async def insert_analysis_policy_apply_outbox(self, **kwargs) -> None:
        dedupe_key = f"analysis-policy-apply:{kwargs['judge_run_id']}:{kwargs['judge_output_id']}"
        self._append_event(
            event_type="analysis.policy.apply.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=dedupe_key,
            payload_json={
                "judge_run_id": str(kwargs["judge_run_id"]),
                "judge_output_id": str(kwargs["judge_output_id"]),
                "candidate_group_id": str(kwargs["candidate_group_id"]),
                "bundle_id": str(kwargs["bundle_id"]),
            },
        )


def _load_fixture() -> dict:
    return json.loads((FIXTURE_ROOT / "judge_output_ready_valid_bundle.json").read_text(encoding="utf-8"))


def _valid_judge_output_payload(candidate_group_id: UUID) -> dict:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Fixture repository",
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


def _config() -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.validate",
        consumer_group="analysis-validator",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="INFO",
    )


def _policy_apply_events(ledger: JudgeOutputReadyPolicyLedger) -> list[dict]:
    return [row for row in ledger.event_outbox if row["event_type"] == "analysis.policy.apply.v1"]


def _forbidden_state(ledger: JudgeOutputReadyPolicyLedger) -> dict:
    return {
        "analyses": ledger.analyses,
        "notification_plans": ledger.notification_plans,
        "notification_renders": ledger.notification_renders,
        "notification_delivery_records": ledger.notification_delivery_records,
        "redis_dispatches": ledger.redis_dispatches,
        "telegram_calls": ledger.telegram_calls,
        "maintenance_calls": ledger.maintenance_calls,
        "policy_calls": ledger.policy_calls,
        "replay_requests": ledger.replay_requests,
        "bundle_refresh_requests": ledger.bundle_refresh_requests,
        "judge_openai_calls": ledger.judge_openai_calls,
    }


def _install_downstream_tripwires(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.maintenance import worker as maintenance_worker
    from services.notifier_telegram import telegram_client, worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.policy_engine import worker as policy_worker

    def fail_downstream(*args, **kwargs):
        raise AssertionError("analysis-validator acceptance must stop at analysis.policy.apply.v1")

    monkeypatch.setattr(policy_worker, "PolicyEngineWorker", fail_downstream)
    monkeypatch.setattr(notifier_worker, "NotifierTelegramWorker", fail_downstream)
    monkeypatch.setattr(telegram_client, "TelegramBotClient", fail_downstream)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "MaintenanceQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "ReplayQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "DueRetryPromotionWorker", fail_downstream)


async def _run_worker(
    ledger: JudgeOutputReadyPolicyLedger,
    *,
    trigger_event_id: UUID | None = None,
) -> tuple[FakeConsumer, object]:
    service = AnalysisValidatorService(_config(), repository=ledger)  # type: ignore[arg-type]
    decoy_judge_run_id = uuid4()
    decoy_output_id = uuid4()
    decoy_candidate_id = uuid4()
    decoy_bundle_id = uuid4()
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.analysis.validate",
            message_id="1-0",
            fields={
                "trigger_event_id": str(trigger_event_id or ledger.trigger_event_id),
                "judge_run_id": str(decoy_judge_run_id),
                "judge_output_id": str(decoy_output_id),
                "candidate_group_id": str(decoy_candidate_id),
                "bundle_id": str(decoy_bundle_id),
                "event_type": "decoy.event.v1",
                "payload_json": json.dumps({"judge_run_id": str(decoy_judge_run_id)}),
            },
        )
    )
    worker = AnalysisValidatorWorker(_config(), consumer=consumer, service=service)
    return consumer, await worker.run_once()


@pytest.mark.asyncio
async def test_judge_output_ready_rehydrates_event_outbox_and_emits_one_thin_policy_apply(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = JudgeOutputReadyPolicyLedger(_load_fixture())
    before_forbidden = deepcopy(_forbidden_state(ledger))
    before_outputs = deepcopy(ledger.judge_outputs)

    consumer, result = await _run_worker(ledger)

    policy_events = _policy_apply_events(ledger)
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert ledger.loaded_trigger_event_ids == [ledger.trigger_event_id]
    assert ledger.loaded_judge_run_ids == [ledger.judge_run_id]
    assert ledger.loaded_judge_output_ids == [ledger.judge_output_id]
    assert ledger.loaded_bundle_ids == [ledger.bundle_id]
    assert ledger.state_transitions == [
        {
            "object_type": "judge_run",
            "object_id": ledger.judge_run_id,
            "from_state": "succeeded",
            "to_state": "analysis_validated",
            "reason_code": "validator_passed",
        }
    ]
    assert len(policy_events) == 1
    assert policy_events[0]["aggregate_type"] == "judge_run"
    assert policy_events[0]["aggregate_id"] == ledger.judge_run_id
    assert policy_events[0]["payload_json"] == {
        "judge_run_id": str(ledger.judge_run_id),
        "judge_output_id": str(ledger.judge_output_id),
        "candidate_group_id": str(ledger.candidate_group_id),
        "bundle_id": str(ledger.bundle_id),
    }
    assert ledger.judge_outputs == before_outputs
    assert _forbidden_state(ledger) == before_forbidden


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal_source", ["ready_payload", "judge_output_payload"])
async def test_refusal_stops_without_policy_apply_and_preserves_judge_output(monkeypatch, refusal_source: str) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = JudgeOutputReadyPolicyLedger(_load_fixture())
    if refusal_source == "ready_payload":
        ledger.set_ready_payload(refusal_detected=True)
    else:
        output = ledger.judge_outputs[ledger.judge_output_id]
        ledger.judge_outputs[ledger.judge_output_id] = replace(
            output,
            payload_json={
                "judge_schema_version": "judge_output_v1",
                "candidate_group_id": str(ledger.candidate_group_id),
                "output_kind": "refusal",
                "refusal_text": "cannot evaluate",
            },
        )
    before_outputs = deepcopy(ledger.judge_outputs)

    await _run_worker(ledger)

    assert _policy_apply_events(ledger) == []
    assert ledger.judge_runs[ledger.judge_run_id].status == "succeeded"
    assert ledger.state_transitions[0]["to_state"] == "analysis_refused"
    assert ledger.state_transitions[0]["reason_code"] == "model_refusal"
    assert ledger.judge_outputs == before_outputs


@pytest.mark.asyncio
@pytest.mark.parametrize("event_case", ["missing_event", "wrong_event_type"])
async def test_missing_or_wrong_ready_event_is_noop(monkeypatch, event_case: str) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = JudgeOutputReadyPolicyLedger(_load_fixture())
    trigger_event_id = ledger.trigger_event_id
    if event_case == "missing_event":
        trigger_event_id = uuid4()
    else:
        ledger._event_by_id[ledger.trigger_event_id]["event_type"] = "judge.call.requested.v1"
    before_forbidden = deepcopy(_forbidden_state(ledger))
    before_outputs = deepcopy(ledger.judge_outputs)

    await _run_worker(ledger, trigger_event_id=trigger_event_id)

    assert _policy_apply_events(ledger) == []
    assert ledger.state_transitions == []
    assert ledger.judge_run_updates == []
    assert ledger.judge_outputs == before_outputs
    assert _forbidden_state(ledger) == before_forbidden


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "durable_gap",
    [
        "missing_judge_run",
        "missing_judge_output",
        "judge_run_output_mismatch",
        "missing_bundle",
        "bundle_candidate_mismatch",
        "payload_candidate_mismatch",
    ],
)
async def test_missing_or_mismatched_durable_rows_do_not_emit_policy_apply(monkeypatch, durable_gap: str) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = JudgeOutputReadyPolicyLedger(_load_fixture())
    if durable_gap == "missing_judge_run":
        del ledger.judge_runs[ledger.judge_run_id]
    elif durable_gap == "missing_judge_output":
        del ledger.judge_outputs[ledger.judge_output_id]
    elif durable_gap == "judge_run_output_mismatch":
        output = ledger.judge_outputs[ledger.judge_output_id]
        ledger.judge_outputs[ledger.judge_output_id] = replace(output, judge_run_id=uuid4())
    elif durable_gap == "missing_bundle":
        del ledger.candidate_evidence_bundles[ledger.bundle_id]
    elif durable_gap == "bundle_candidate_mismatch":
        bundle = ledger.candidate_evidence_bundles[ledger.bundle_id]
        ledger.candidate_evidence_bundles[ledger.bundle_id] = replace(bundle, candidate_group_id=uuid4())
    elif durable_gap == "payload_candidate_mismatch":
        output = ledger.judge_outputs[ledger.judge_output_id]
        payload = deepcopy(output.payload_json)
        payload["candidate_group_id"] = str(uuid4())
        ledger.judge_outputs[ledger.judge_output_id] = replace(output, payload_json=payload)
    before_outputs = deepcopy(ledger.judge_outputs)

    await _run_worker(ledger)

    assert _policy_apply_events(ledger) == []
    assert ledger.judge_outputs == before_outputs
    assert ledger.state_transitions != []
    if durable_gap != "missing_judge_run":
        assert ledger.judge_runs[ledger.judge_run_id].status == "failed_terminal"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_case",
    [
        "missing_skeptical_take",
        "invalid_model_proposed_verdict",
        "score_outside_range",
        "inspect_now_low_evidence",
        "github_primary_empty_comparables",
        "wrong_judge_output_schema_version",
    ],
)
async def test_invalid_schema_or_business_output_does_not_emit_policy_apply(monkeypatch, invalid_case: str) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = JudgeOutputReadyPolicyLedger(_load_fixture())
    output = ledger.judge_outputs[ledger.judge_output_id]
    payload = deepcopy(output.payload_json)
    if invalid_case == "missing_skeptical_take":
        del payload["skeptical_take_ko"]
    elif invalid_case == "invalid_model_proposed_verdict":
        payload["model_proposed_verdict"] = "inspect_whenever"
    elif invalid_case == "score_outside_range":
        payload["scores"]["evidence_strength"] = 101
    elif invalid_case == "inspect_now_low_evidence":
        payload["model_proposed_verdict"] = "inspect_now"
        payload["scores"]["evidence_strength"] = 49
    elif invalid_case == "github_primary_empty_comparables":
        payload["comparables"] = []
    elif invalid_case == "wrong_judge_output_schema_version":
        payload["judge_schema_version"] = "judge_output_v2"
    ledger.judge_outputs[ledger.judge_output_id] = replace(output, payload_json=payload)
    before_outputs = deepcopy(ledger.judge_outputs)

    await _run_worker(ledger)

    assert _policy_apply_events(ledger) == []
    assert ledger.judge_runs[ledger.judge_run_id].status == "failed_terminal"
    assert ledger.state_transitions != []
    assert ledger.judge_outputs == before_outputs


@pytest.mark.asyncio
async def test_truncation_finish_reason_stops_retryable_without_rejudge_or_policy_apply(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = JudgeOutputReadyPolicyLedger(_load_fixture())
    ledger.set_ready_payload(finish_reason="max_output_tokens")
    before_outputs = deepcopy(ledger.judge_outputs)
    before_forbidden = deepcopy(_forbidden_state(ledger))

    await _run_worker(ledger)

    assert _policy_apply_events(ledger) == []
    assert ledger.judge_runs[ledger.judge_run_id].status == "failed_retryable"
    assert ledger.state_transitions[0]["to_state"] == "analysis_failed_truncation"
    assert ledger.state_transitions[0]["reason_code"] == "analysis_failed_truncation"
    assert ledger.judge_outputs == before_outputs
    assert _forbidden_state(ledger) == before_forbidden
