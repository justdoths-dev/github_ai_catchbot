from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.analysis_router.config import AnalysisRouterConfig
from services.analysis_router.models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, CandidateRouteState, StreamMessage
from services.analysis_router.service import AnalysisRouterService
from services.analysis_router.worker import AnalysisRouterWorker


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "upstream"


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self._message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self):
        return [self._message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class AnalysisRequestedBoundaryLedger:
    def __init__(self, fixture: dict) -> None:
        self.candidate_group_id = UUID(fixture["candidate_group_id"])
        self.bundle_id = UUID(fixture["bundle_id"])
        self.trigger_event_id = UUID(fixture["trigger_event_id"])
        self.event_outbox: list[dict] = []
        self._event_by_id: dict[UUID, dict] = {}
        self._outbox_dedupe_keys: set[str] = set()
        self.candidate_group_proposals = {
            self.candidate_group_id: CandidateRouteState(self.candidate_group_id, self.bundle_id)
        }
        self.candidate_evidence_bundles = {
            self.bundle_id: BundleRouteRecord(
                bundle_id=self.bundle_id,
                candidate_group_id=self.candidate_group_id,
                bundle_profile_version=fixture["bundle_profile_version"],
                reroot_count=int(fixture["reroot_count"]),
                ready_for_analysis=bool(fixture["ready_for_analysis"]),
                token_budget_profile=fixture["token_budget_profile"],
            )
        }
        self.candidate_evidence_members = {
            self.bundle_id: BundleShapeStats(
                member_count=int(fixture["member_count"]),
                supporting_count=int(fixture["supporting_count"]),
            )
        }
        self.judge_runs: list[dict] = []
        self._judge_run_by_route: dict[tuple[UUID, str, str, str], UUID] = {}
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
        self.openai_calls: list[dict] = []
        self.maintenance_calls: list[dict] = []
        self._append_event(
            event_id=self.trigger_event_id,
            event_type="analysis.requested.v1",
            aggregate_type="candidate_group",
            aggregate_id=self.candidate_group_id,
            dedupe_key=f"analysis-request:{self.candidate_group_id}:{self.bundle_id}",
            payload_json={
                "candidate_group_id": str(self.candidate_group_id),
                "bundle_id": str(self.bundle_id),
                "judge_profile": fixture["judge_profile"],
                "escalation_allowed": bool(fixture["escalation_allowed"]),
            },
        )

    def transaction(self):
        return Tx()

    def _append_event(
        self,
        *,
        event_id: UUID | None = None,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        dedupe_key: str,
        payload_json: dict,
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
        row = self._event_by_id.get(trigger_event_id)
        if row is None or row["event_type"] != "analysis.requested.v1":
            return None
        payload = row["payload_json"]
        return AnalysisRequestedJob(
            trigger_event_id=trigger_event_id,
            event_type=row["event_type"],
            candidate_group_id=UUID(payload["candidate_group_id"]),
            bundle_id=UUID(payload["bundle_id"]),
            judge_profile=payload.get("judge_profile"),
            escalation_allowed=bool(payload.get("escalation_allowed", False)),
        )

    async def load_candidate_route_state(self, candidate_group_id: UUID):
        return self.candidate_group_proposals.get(candidate_group_id)

    async def load_bundle(self, bundle_id: UUID):
        return self.candidate_evidence_bundles.get(bundle_id)

    async def load_bundle_shape_stats(self, bundle_id: UUID):
        return self.candidate_evidence_members.get(bundle_id, BundleShapeStats(member_count=0, supporting_count=0))

    async def get_or_create_judge_run(self, **kwargs):
        key = (
            kwargs["bundle_id"],
            kwargs["prompt_version"],
            kwargs["model"],
            kwargs["reasoning_effort"],
        )
        if key in self._judge_run_by_route:
            return self._judge_run_by_route[key], False

        judge_run_id = uuid4()
        self._judge_run_by_route[key] = judge_run_id
        bundle = self.candidate_evidence_bundles[kwargs["bundle_id"]]
        self.judge_runs.append(
            {
                "judge_run_id": judge_run_id,
                "candidate_group_id": bundle.candidate_group_id,
                "bundle_id": kwargs["bundle_id"],
                "judge_profile": kwargs["judge_profile"],
                "model": kwargs["model"],
                "reasoning_effort": kwargs["reasoning_effort"],
                "prompt_version": kwargs["prompt_version"],
                "schema_version": kwargs["schema_version"],
                "policy_version": kwargs["policy_version"],
                "prompt_cache_key": kwargs["prompt_cache_key"],
                "status": "pending",
            }
        )
        return judge_run_id, True

    async def insert_judge_call_requested_outbox(self, **kwargs):
        self._append_event(
            event_type="judge.call.requested.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=f"judge-call:{kwargs['judge_run_id']}",
            payload_json={
                "judge_run_id": str(kwargs["judge_run_id"]),
                "candidate_group_id": str(kwargs["candidate_group_id"]),
                "bundle_id": str(kwargs["bundle_id"]),
                "judge_profile": kwargs["judge_profile"],
                "model": kwargs["model"],
                "reasoning_effort": kwargs["reasoning_effort"],
                "prompt_version": kwargs["prompt_version"],
                "prompt_cache_key": kwargs["prompt_cache_key"],
            },
        )

    async def insert_bundle_refresh_outbox(self, **kwargs):
        raise AssertionError("ready analysis request must not request bundle refresh")


def _config() -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        enable_model_escalation=False,
        default_model="gpt-5.4-mini",
        escalation_model="gpt-5.4",
        default_reasoning_effort="low",
        escalation_reasoning_effort="medium",
        github_prompt_version="judge_github_primary_v1",
        x_prompt_version="judge_x_primary_v1",
        text_idea_prompt_version="judge_text_idea_primary_v1",
        judge_schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        log_level="INFO",
    )


def _load_fixture() -> dict:
    return json.loads((FIXTURE_ROOT / "analysis_requested_ready_bundle.json").read_text(encoding="utf-8"))


def _forbidden_state(ledger: AnalysisRequestedBoundaryLedger) -> dict:
    return {
        "judge_outputs": ledger.judge_outputs,
        "analyses": ledger.analyses,
        "state_transitions": ledger.state_transitions,
        "notification_plans": ledger.notification_plans,
        "notification_renders": ledger.notification_renders,
        "notification_delivery_records": ledger.notification_delivery_records,
        "replay_requests": ledger.replay_requests,
        "dead_letter_entries": ledger.dead_letter_entries,
        "redis_dispatches": ledger.redis_dispatches,
        "telegram_calls": ledger.telegram_calls,
        "openai_calls": ledger.openai_calls,
        "maintenance_calls": ledger.maintenance_calls,
    }


def _install_downstream_tripwires(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.analysis_validator import worker as analysis_validator_worker
    from services.judge_openai import openai_client, worker as judge_openai_worker
    from services.maintenance import batch_recovery_tool, worker as maintenance_worker
    from services.notifier_telegram import telegram_client, worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.policy_engine import worker as policy_worker

    def fail_downstream(*args, **kwargs):
        raise AssertionError("analysis-router handoff acceptance must stop at judge.call.requested.v1")

    monkeypatch.setattr(openai_client, "OpenAIJudgeClient", fail_downstream)
    monkeypatch.setattr(judge_openai_worker, "JudgeOpenAIWorker", fail_downstream)
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
async def test_analysis_requested_to_judge_call_requested_preserves_downstream_boundaries(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = AnalysisRequestedBoundaryLedger(_load_fixture())
    before_forbidden = deepcopy(_forbidden_state(ledger))
    decoy_candidate_group_id = uuid4()
    decoy_bundle_id = uuid4()
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.analysis.route",
            message_id="1-0",
            fields={
                "trigger_event_id": str(ledger.trigger_event_id),
                "candidate_group_id": str(decoy_candidate_group_id),
                "bundle_id": str(decoy_bundle_id),
                "judge_profile": "web_primary",
            },
        )
    )
    service = AnalysisRouterService(_config(), repository=ledger)  # type: ignore[arg-type]
    worker = AnalysisRouterWorker(_config(), consumer=consumer, service=service)  # type: ignore[arg-type]

    result = await worker.run_once()

    judge_call_events = [row for row in ledger.event_outbox if row["event_type"] == "judge.call.requested.v1"]
    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert len(ledger.judge_runs) == 1
    assert ledger.judge_runs[0]["candidate_group_id"] == ledger.candidate_group_id
    assert ledger.judge_runs[0]["candidate_group_id"] != decoy_candidate_group_id
    assert ledger.judge_runs[0]["bundle_id"] == ledger.bundle_id
    assert ledger.judge_runs[0]["bundle_id"] != decoy_bundle_id
    assert ledger.judge_runs[0]["judge_profile"] == "github_primary"
    assert ledger.judge_runs[0]["status"] == "pending"
    assert len(judge_call_events) == 1
    assert judge_call_events[0]["aggregate_type"] == "judge_run"
    assert judge_call_events[0]["aggregate_id"] == ledger.judge_runs[0]["judge_run_id"]
    assert judge_call_events[0]["payload_json"] == {
        "judge_run_id": str(ledger.judge_runs[0]["judge_run_id"]),
        "candidate_group_id": str(ledger.candidate_group_id),
        "bundle_id": str(ledger.bundle_id),
        "judge_profile": "github_primary",
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_github_primary_v1",
        "prompt_cache_key": "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
    }
    assert [row["event_type"] for row in ledger.event_outbox] == [
        "analysis.requested.v1",
        "judge.call.requested.v1",
    ]
    assert _forbidden_state(ledger) == before_forbidden
