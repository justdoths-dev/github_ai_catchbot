from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.policy_engine.config import PolicyEngineConfig
from services.policy_engine.models import (
    AnalysisDraft,
    AnalysisPolicyJob,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
    StreamMessage,
)
from services.policy_engine.service import PolicyEngineService
from services.policy_engine.worker import PolicyEngineWorker


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "upstream"
FEEDBACK_AWARE_DELIVERY_POLICY_VERSION = "delivery_policy_v1_feedback_aware_channel_policy_v1"


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


class AnalysisPolicyApplyLedger:
    def __init__(self, fixture: dict[str, str]) -> None:
        self.fixture = fixture
        self.trigger_event_id = UUID(fixture["trigger_event_id"])
        self.judge_run_id = UUID(fixture["judge_run_id"])
        self.judge_output_id = UUID(fixture["judge_output_id"])
        self.bundle_id = UUID(fixture["bundle_id"])
        self.candidate_group_id = UUID(fixture["candidate_group_id"])
        self.current_primary_artifact_id = UUID(fixture["current_primary_artifact_id"])

        self.event_outbox: list[dict[str, Any]] = []
        self._event_by_id: dict[UUID, dict[str, Any]] = {}
        self._outbox_dedupe_keys: set[str] = set()
        self.loaded_trigger_event_ids: list[UUID] = []
        self.loaded_candidate_group_ids: list[UUID] = []
        self.loaded_judge_run_ids: list[UUID] = []
        self.loaded_judge_output_ids: list[UUID] = []
        self.loaded_bundle_ids: list[UUID] = []

        self.analyses: list[tuple[UUID, AnalysisDraft]] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.existing: dict[tuple[UUID, str, str], ExistingAnalysisRecord] = {}
        self.notification_outbox: list[NotificationPlanIntent] = []

        self.notification_plans: list[dict[str, Any]] = []
        self.notification_renders: list[dict[str, Any]] = []
        self.notification_delivery_records: list[dict[str, Any]] = []
        self.redis_dispatches: list[dict[str, Any]] = []
        self.telegram_calls: list[dict[str, Any]] = []
        self.maintenance_calls: list[dict[str, Any]] = []
        self.replay_requests: list[dict[str, Any]] = []
        self.validator_calls: list[dict[str, Any]] = []
        self.judge_openai_calls: list[dict[str, Any]] = []

        self.candidates = {
            self.candidate_group_id: CandidatePolicyContext(
                candidate_group_id=self.candidate_group_id,
                current_bundle_id=self.bundle_id,
                current_analysis_id=None,
            )
        }
        self.judge_runs = {
            self.judge_run_id: JudgeRunPolicyContext(
                judge_run_id=self.judge_run_id,
                bundle_id=self.bundle_id,
                prompt_version=fixture["prompt_version"],
                policy_version=fixture["policy_version"],
                status="succeeded",
            )
        }
        payload = _valid_judge_output_payload(model_proposed_verdict="later")
        self.judge_outputs = {
            self.judge_output_id: JudgeOutputPolicyContext(
                judge_output_id=self.judge_output_id,
                judge_run_id=self.judge_run_id,
                candidate_group_id=self.candidate_group_id,
                payload_json=payload,
                model_proposed_verdict=payload["model_proposed_verdict"],
                model_confidence_band="medium",
                created_at=datetime.now(timezone.utc),
            )
        }
        self.candidate_evidence_bundles = {
            self.bundle_id: BundlePolicyContext(
                bundle_id=self.bundle_id,
                candidate_group_id=self.candidate_group_id,
                current_primary_artifact_id=self.current_primary_artifact_id,
                current_primary_artifact_type=fixture["current_primary_artifact_type"],
                created_at=datetime.now(timezone.utc),
            )
        }
        self._append_event(
            event_id=self.trigger_event_id,
            event_type="analysis.policy.apply.v1",
            aggregate_type="judge_run",
            aggregate_id=self.judge_run_id,
            dedupe_key=f"analysis-policy-apply:{self.judge_run_id}:{self.judge_output_id}",
            payload_json={
                "judge_run_id": str(self.judge_run_id),
                "judge_output_id": str(self.judge_output_id),
                "candidate_group_id": str(self.candidate_group_id),
                "bundle_id": str(self.bundle_id),
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
        payload_json: dict[str, Any],
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
        if row is None or row["event_type"] != "analysis.policy.apply.v1":
            return None
        payload = row["payload_json"]
        try:
            return AnalysisPolicyJob(
                trigger_event_id=trigger_event_id,
                event_type=row["event_type"],
                judge_run_id=UUID(payload["judge_run_id"]),
                judge_output_id=UUID(payload["judge_output_id"]),
                candidate_group_id=UUID(payload["candidate_group_id"]),
                bundle_id=UUID(payload["bundle_id"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    async def load_candidate_context(self, candidate_group_id: UUID):
        self.loaded_candidate_group_ids.append(candidate_group_id)
        return self.candidates.get(candidate_group_id)

    async def load_judge_run(self, judge_run_id: UUID):
        self.loaded_judge_run_ids.append(judge_run_id)
        return self.judge_runs.get(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID):
        self.loaded_judge_output_ids.append(judge_output_id)
        return self.judge_outputs.get(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID):
        self.loaded_bundle_ids.append(bundle_id)
        return self.candidate_evidence_bundles.get(bundle_id)

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ):
        return self.existing.get((judge_output_id, policy_version, delivery_policy_version))

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        existing = await self.load_existing_analysis(
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        if existing is not None:
            return existing.analysis_id
        analysis_id = uuid4()
        self.existing[(draft.judge_output_id, draft.policy_version, draft.delivery_policy_version)] = (
            ExistingAnalysisRecord(
                analysis_id=analysis_id,
                judge_output_id=draft.judge_output_id,
                policy_version=draft.policy_version,
                delivery_policy_version=draft.delivery_policy_version,
            )
        )
        self.analyses.append((analysis_id, draft))
        return analysis_id

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None:
        self.notification_outbox.append(intent)
        self._append_event(
            event_type="notification.plan.created.v1",
            aggregate_type="analysis",
            aggregate_id=intent.analysis_id,
            dedupe_key=(
                f"notification-plan-created:{intent.analysis_id}:"
                f"{intent.target_chat_id}:{intent.material_change_hash}"
            ),
            payload_json={
                "notification_plan_id": str(intent.notification_plan_id),
                "analysis_id": str(intent.analysis_id),
                "candidate_group_id": str(intent.candidate_group_id),
                "delivery_decision": intent.delivery_decision,
                "urgency_profile": intent.urgency_profile,
                "target_chat_id": intent.target_chat_id,
                "target_thread_id": intent.target_thread_id,
                "render_profile": intent.render_profile,
                "dedupe_subject_key": intent.dedupe_subject_key,
                "material_change_hash": intent.material_change_hash,
                "send_after": intent.send_after,
                "suppress_reason_code": intent.suppress_reason_code,
            },
        )


def _load_fixture() -> dict[str, str]:
    return json.loads((FIXTURE_ROOT / "analysis_policy_apply_valid_bundle.json").read_text(encoding="utf-8"))


def _valid_judge_output_payload(*, model_proposed_verdict: str | None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "judge_schema_version": "judge_output_v1",
        "headline": "Fixture repository",
        "scores": {
            "novelty": 75,
            "practical_usefulness": 76,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 72,
            "code_quality": 70,
            "maintenance_signal": 60,
            "specificity": 65,
            "reproducibility_signal": 50,
        },
        "reason_codes": ["repo_has_clear_scope"],
        "evidence_limitations_ko": ["fixture only"],
        "recommended_action_ko": "inspect",
        "freshness_note_ko": "fresh",
        "model_confidence_band": "medium",
    }
    if model_proposed_verdict is not None:
        payload["model_proposed_verdict"] = model_proposed_verdict
    return payload


def _config(*, enable_later_delivery: bool = True) -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=12345,
        enable_later_delivery=enable_later_delivery,
        enable_silent_later=True,
        enable_notification_send=True,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )


def _notification_plan_events(ledger: AnalysisPolicyApplyLedger) -> list[dict[str, Any]]:
    return [row for row in ledger.event_outbox if row["event_type"] == "notification.plan.created.v1"]


def _forbidden_state(ledger: AnalysisPolicyApplyLedger) -> dict[str, Any]:
    return {
        "notification_plans": ledger.notification_plans,
        "notification_renders": ledger.notification_renders,
        "notification_delivery_records": ledger.notification_delivery_records,
        "redis_dispatches": ledger.redis_dispatches,
        "telegram_calls": ledger.telegram_calls,
        "maintenance_calls": ledger.maintenance_calls,
        "replay_requests": ledger.replay_requests,
        "validator_calls": ledger.validator_calls,
        "judge_openai_calls": ledger.judge_openai_calls,
    }


def _install_downstream_tripwires(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.analysis_validator import worker as analysis_validator_worker
    from services.judge_openai import worker as judge_openai_worker
    from services.maintenance import worker as maintenance_worker
    from services.notifier_telegram import telegram_client, worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams

    def fail_downstream(*args, **kwargs):
        raise AssertionError("policy-engine acceptance must stop at notification.plan.created.v1 intent")

    monkeypatch.setattr(analysis_validator_worker, "AnalysisValidatorWorker", fail_downstream)
    monkeypatch.setattr(judge_openai_worker, "JudgeOpenAIWorker", fail_downstream)
    monkeypatch.setattr(notifier_worker, "NotifierTelegramWorker", fail_downstream)
    monkeypatch.setattr(telegram_client, "TelegramBotClient", fail_downstream)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "MaintenanceQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "ReplayQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "DueRetryPromotionWorker", fail_downstream)


async def _run_worker(
    ledger: AnalysisPolicyApplyLedger,
    *,
    trigger_event_id: UUID | None = None,
    enable_later_delivery: bool = True,
):
    service = PolicyEngineService(_config(enable_later_delivery=enable_later_delivery), repository=ledger)  # type: ignore[arg-type]
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.analysis.policy",
            message_id="1-0",
            fields={
                "trigger_event_id": str(trigger_event_id or ledger.trigger_event_id),
                "judge_run_id": str(uuid4()),
                "judge_output_id": str(uuid4()),
                "candidate_group_id": str(uuid4()),
                "bundle_id": str(uuid4()),
                "event_type": "decoy.event.v1",
                "payload_json": json.dumps({"judge_run_id": str(uuid4())}),
            },
        )
    )
    worker = PolicyEngineWorker(_config(enable_later_delivery=enable_later_delivery), consumer=consumer, service=service)
    return consumer, await worker.run_once()


@pytest.mark.asyncio
async def test_policy_apply_rehydrates_outbox_and_emits_single_notification_plan_intent(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = AnalysisPolicyApplyLedger(_load_fixture())
    before_outputs = deepcopy(ledger.judge_outputs)
    before_bundles = deepcopy(ledger.candidate_evidence_bundles)
    before_candidates = deepcopy(ledger.candidates)
    before_forbidden = deepcopy(_forbidden_state(ledger))

    consumer, result = await _run_worker(ledger)

    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert ledger.loaded_trigger_event_ids == [ledger.trigger_event_id]
    assert ledger.loaded_candidate_group_ids == [ledger.candidate_group_id]
    assert ledger.loaded_judge_run_ids == [ledger.judge_run_id]
    assert ledger.loaded_judge_output_ids == [ledger.judge_output_id]
    assert ledger.loaded_bundle_ids == [ledger.bundle_id]

    assert len(ledger.analyses) == 1
    analysis_id, analysis = ledger.analyses[0]
    assert analysis.candidate_group_id == ledger.candidate_group_id
    assert analysis.judge_output_id == ledger.judge_output_id
    assert analysis.schema_version == "analysis_v1"
    assert analysis.policy_version == "verdict_policy_v1"
    assert analysis.prompt_version == "judge_prompt_v1"
    assert analysis.delivery_policy_version == FEEDBACK_AWARE_DELIVERY_POLICY_VERSION
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"
    assert analysis.model_proposed_verdict == "later"
    assert analysis.policy_reconciled_flag is False
    assert "policy_overrode_model_verdict" in analysis.reason_codes_json

    plan_events = _notification_plan_events(ledger)
    assert len(plan_events) == 1
    assert len(ledger.notification_outbox) == 1
    payload = plan_events[0]["payload_json"]
    assert payload["notification_plan_id"] == str(ledger.notification_outbox[0].notification_plan_id)
    assert payload["analysis_id"] == str(analysis_id)
    assert payload["candidate_group_id"] == str(ledger.candidate_group_id)
    assert payload["delivery_decision"] == "send_now"
    assert payload["urgency_profile"] == "high"
    assert payload["target_chat_id"] == 12345
    assert payload["target_thread_id"] is None
    assert payload["render_profile"] == "telegram_single_alert_high_v1"
    assert payload["dedupe_subject_key"] == str(ledger.candidate_group_id)
    assert isinstance(payload["material_change_hash"], str)
    assert payload["send_after"] is None
    assert payload["suppress_reason_code"] is None

    assert ledger.state_transitions == [
        {
            "object_type": "analysis",
            "object_id": analysis_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_finalized",
            "reason_code": "policy_applied:inspect_now:send_now",
        }
    ]
    assert ledger.judge_outputs == before_outputs
    assert ledger.candidate_evidence_bundles == before_bundles
    assert ledger.candidates == before_candidates
    assert _forbidden_state(ledger) == before_forbidden


@pytest.mark.asyncio
async def test_skip_appends_analysis_without_notification_plan_intent(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = AnalysisPolicyApplyLedger(_load_fixture())
    output = ledger.judge_outputs[ledger.judge_output_id]
    payload = deepcopy(output.payload_json)
    payload["scores"]["practical_usefulness"] = 20
    payload["scores"]["evidence_strength"] = 20
    payload["scores"]["confidence"] = 20
    payload["model_proposed_verdict"] = "skip"
    ledger.judge_outputs[ledger.judge_output_id] = replace(output, payload_json=payload, model_proposed_verdict="skip")

    await _run_worker(ledger)

    assert len(ledger.analyses) == 1
    _analysis_id, analysis = ledger.analyses[0]
    assert analysis.verdict == "skip"
    assert analysis.delivery_decision == "suppress"
    assert analysis.policy_reconciled_flag is True
    assert "verdict_skip" in analysis.reason_codes_json
    assert _notification_plan_events(ledger) == []
    assert ledger.notification_outbox == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("enable_later_delivery", "expected_delivery", "expected_urgency", "expected_plan_count", "expected_reason"),
    [
        (True, "send_now", "normal_silent", 1, None),
        (False, "suppress", "suppressed", 0, "later_delivery_disabled"),
    ],
)
async def test_later_delivery_policy_path_is_deterministic(
    monkeypatch,
    enable_later_delivery: bool,
    expected_delivery: str,
    expected_urgency: str,
    expected_plan_count: int,
    expected_reason: str | None,
) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = AnalysisPolicyApplyLedger(_load_fixture())
    output = ledger.judge_outputs[ledger.judge_output_id]
    payload = deepcopy(output.payload_json)
    payload["scores"]["practical_usefulness"] = 50
    payload["scores"]["evidence_strength"] = 40
    payload["scores"]["confidence"] = 40
    payload["scores"]["code_quality"] = 20
    payload["model_proposed_verdict"] = "later"
    ledger.judge_outputs[ledger.judge_output_id] = replace(output, payload_json=payload, model_proposed_verdict="later")

    await _run_worker(ledger, enable_later_delivery=enable_later_delivery)

    assert len(ledger.analyses) == 1
    _analysis_id, analysis = ledger.analyses[0]
    assert analysis.verdict == "later"
    assert analysis.delivery_decision == expected_delivery
    assert analysis.policy_reconciled_flag is True
    if expected_reason is not None:
        assert expected_reason in analysis.reason_codes_json
    plan_events = _notification_plan_events(ledger)
    assert len(plan_events) == expected_plan_count
    if plan_events:
        assert plan_events[0]["payload_json"]["urgency_profile"] == expected_urgency


@pytest.mark.asyncio
async def test_stale_current_bundle_stops_before_analysis_or_notification(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = AnalysisPolicyApplyLedger(_load_fixture())
    ledger.candidates[ledger.candidate_group_id] = replace(
        ledger.candidates[ledger.candidate_group_id],
        current_bundle_id=uuid4(),
    )

    await _run_worker(ledger)

    assert ledger.analyses == []
    assert _notification_plan_events(ledger) == []
    assert ledger.notification_outbox == []
    assert ledger.state_transitions == [
        {
            "object_type": "candidate_group",
            "object_id": ledger.candidate_group_id,
            "from_state": "analysis_validated",
            "to_state": "analysis_policy_stale_bundle",
            "reason_code": "policy_stale_bundle_request",
        }
    ]


@pytest.mark.asyncio
async def test_legacy_analysis_coexists_with_feedback_aware_analysis_and_retry_is_noop(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = AnalysisPolicyApplyLedger(_load_fixture())
    legacy_identity = (ledger.judge_output_id, "verdict_policy_v1", "delivery_policy_v1")
    feedback_aware_identity = (
        ledger.judge_output_id,
        "verdict_policy_v1",
        FEEDBACK_AWARE_DELIVERY_POLICY_VERSION,
    )
    legacy_record = ExistingAnalysisRecord(
        analysis_id=uuid4(),
        judge_output_id=ledger.judge_output_id,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )
    ledger.existing[legacy_identity] = legacy_record

    await _run_worker(ledger)

    assert len(ledger.analyses) == 1
    _analysis_id, analysis = ledger.analyses[0]
    assert (
        analysis.judge_output_id,
        analysis.policy_version,
        analysis.delivery_policy_version,
    ) == feedback_aware_identity
    assert set(ledger.existing) == {legacy_identity, feedback_aware_identity}
    assert ledger.existing[legacy_identity] is legacy_record
    assert len(ledger.state_transitions) == 1
    assert len(_notification_plan_events(ledger)) == 1
    assert len(ledger.notification_outbox) == 1

    analyses_before_retry = list(ledger.analyses)
    existing_before_retry = dict(ledger.existing)
    transitions_before_retry = list(ledger.state_transitions)
    outbox_before_retry = list(ledger.notification_outbox)
    plan_events_before_retry = list(_notification_plan_events(ledger))

    await _run_worker(ledger)

    assert ledger.analyses == analyses_before_retry
    assert ledger.existing == existing_before_retry
    assert ledger.existing[legacy_identity] is legacy_record
    assert ledger.state_transitions == transitions_before_retry
    assert ledger.notification_outbox == outbox_before_retry
    assert _notification_plan_events(ledger) == plan_events_before_retry


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_case",
    [
        "missing_event",
        "wrong_event_type",
        "missing_payload_field",
        "missing_candidate",
        "missing_judge_run",
        "missing_judge_output",
        "missing_bundle",
        "judge_run_bundle_mismatch",
        "judge_output_run_mismatch",
        "judge_output_candidate_mismatch",
        "bundle_candidate_mismatch",
    ],
)
async def test_missing_or_mismatched_durable_rows_stop_before_analysis_or_notification(
    monkeypatch,
    bad_case: str,
) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = AnalysisPolicyApplyLedger(_load_fixture())
    trigger_event_id = ledger.trigger_event_id
    if bad_case == "missing_event":
        trigger_event_id = uuid4()
    elif bad_case == "wrong_event_type":
        ledger._event_by_id[ledger.trigger_event_id]["event_type"] = "judge.output.ready.v1"
    elif bad_case == "missing_payload_field":
        del ledger._event_by_id[ledger.trigger_event_id]["payload_json"]["judge_output_id"]
    elif bad_case == "missing_candidate":
        del ledger.candidates[ledger.candidate_group_id]
    elif bad_case == "missing_judge_run":
        del ledger.judge_runs[ledger.judge_run_id]
    elif bad_case == "missing_judge_output":
        del ledger.judge_outputs[ledger.judge_output_id]
    elif bad_case == "missing_bundle":
        del ledger.candidate_evidence_bundles[ledger.bundle_id]
    elif bad_case == "judge_run_bundle_mismatch":
        run = ledger.judge_runs[ledger.judge_run_id]
        ledger.judge_runs[ledger.judge_run_id] = replace(run, bundle_id=uuid4())
    elif bad_case == "judge_output_run_mismatch":
        output = ledger.judge_outputs[ledger.judge_output_id]
        ledger.judge_outputs[ledger.judge_output_id] = replace(output, judge_run_id=uuid4())
    elif bad_case == "judge_output_candidate_mismatch":
        output = ledger.judge_outputs[ledger.judge_output_id]
        ledger.judge_outputs[ledger.judge_output_id] = replace(output, candidate_group_id=uuid4())
    elif bad_case == "bundle_candidate_mismatch":
        bundle = ledger.candidate_evidence_bundles[ledger.bundle_id]
        ledger.candidate_evidence_bundles[ledger.bundle_id] = replace(bundle, candidate_group_id=uuid4())
    before_outputs = deepcopy(ledger.judge_outputs)
    before_bundles = deepcopy(ledger.candidate_evidence_bundles)

    await _run_worker(ledger, trigger_event_id=trigger_event_id)

    assert ledger.analyses == []
    assert _notification_plan_events(ledger) == []
    assert ledger.notification_outbox == []
    assert ledger.judge_outputs == before_outputs
    assert ledger.candidate_evidence_bundles == before_bundles
