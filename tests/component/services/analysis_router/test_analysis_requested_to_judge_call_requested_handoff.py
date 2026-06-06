from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.analysis_router.config import AnalysisRouterConfig
from services.analysis_router.models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, CandidateRouteState
from services.analysis_router.service import AnalysisRouterService


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class AnalysisRouterHandoffLedger:
    def __init__(self) -> None:
        self.event_outbox: list[dict] = []
        self._event_by_id: dict[UUID, dict] = {}
        self._outbox_dedupe_keys: set[str] = set()
        self.candidate_states: dict[UUID, CandidateRouteState] = {}
        self.bundles: dict[UUID, BundleRouteRecord] = {}
        self.shapes: dict[UUID, BundleShapeStats] = {}
        self.judge_runs: list[dict] = []
        self._judge_run_by_route: dict[tuple[UUID, str, str, str], UUID] = {}
        self.refresh_events: list[dict] = []

    def transaction(self):
        return Tx()

    def append_analysis_requested(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        judge_profile: str | None = "github_primary",
        escalation_allowed: bool = True,
    ) -> UUID:
        return self._append_event(
            event_type="analysis.requested.v1",
            aggregate_type="candidate_group",
            aggregate_id=candidate_group_id,
            dedupe_key=f"analysis-request:{candidate_group_id}:{bundle_id}",
            payload_json={
                "candidate_group_id": str(candidate_group_id),
                "bundle_id": str(bundle_id),
                "judge_profile": judge_profile,
                "escalation_allowed": escalation_allowed,
            },
        )

    def seed_ready_bundle(
        self,
        *,
        candidate_group_id: UUID | None = None,
        bundle_id: UUID | None = None,
        judge_profile: str | None = "github_primary",
        ready_for_analysis: bool = True,
    ) -> tuple[UUID, UUID, UUID]:
        candidate_group_id = candidate_group_id or uuid4()
        bundle_id = bundle_id or uuid4()
        trigger_event_id = self.append_analysis_requested(
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
            judge_profile=judge_profile,
        )
        self.candidate_states[candidate_group_id] = CandidateRouteState(candidate_group_id, bundle_id)
        self.bundles[bundle_id] = BundleRouteRecord(
            bundle_id=bundle_id,
            candidate_group_id=candidate_group_id,
            bundle_profile_version="bundle_profile_v1",
            reroot_count=0,
            ready_for_analysis=ready_for_analysis,
            token_budget_profile="small",
        )
        self.shapes[bundle_id] = BundleShapeStats(member_count=1, supporting_count=0)
        return trigger_event_id, candidate_group_id, bundle_id

    def _append_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        dedupe_key: str,
        payload_json: dict,
    ) -> UUID:
        if dedupe_key in self._outbox_dedupe_keys:
            return next(row["event_id"] for row in self.event_outbox if row["dedupe_key"] == dedupe_key)
        event_id = uuid4()
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
        return self.candidate_states.get(candidate_group_id)

    async def load_bundle(self, bundle_id: UUID):
        return self.bundles.get(bundle_id)

    async def load_bundle_shape_stats(self, bundle_id: UUID):
        return self.shapes.get(bundle_id, BundleShapeStats(member_count=0, supporting_count=0))

    async def get_or_create_judge_run(self, **kwargs):
        key = (
            kwargs["bundle_id"],
            kwargs["prompt_version"],
            kwargs["model"],
            kwargs["reasoning_effort"],
        )
        if key in self._judge_run_by_route:
            return self._judge_run_by_route[key], False

        bundle = self.bundles[kwargs["bundle_id"]]
        judge_run_id = uuid4()
        self._judge_run_by_route[key] = judge_run_id
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
        payload = {
            "judge_run_id": str(kwargs["judge_run_id"]),
            "candidate_group_id": str(kwargs["candidate_group_id"]),
            "bundle_id": str(kwargs["bundle_id"]),
            "judge_profile": kwargs["judge_profile"],
            "model": kwargs["model"],
            "reasoning_effort": kwargs["reasoning_effort"],
            "prompt_version": kwargs["prompt_version"],
            "prompt_cache_key": kwargs["prompt_cache_key"],
        }
        self._append_event(
            event_type="judge.call.requested.v1",
            aggregate_type="judge_run",
            aggregate_id=kwargs["judge_run_id"],
            dedupe_key=f"judge-call:{kwargs['judge_run_id']}",
            payload_json=payload,
        )

    async def insert_bundle_refresh_outbox(self, **kwargs):
        self.refresh_events.append(kwargs)


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


async def _handle(ledger: AnalysisRouterHandoffLedger, trigger_event_id: UUID) -> None:
    await AnalysisRouterService(_config(), repository=ledger).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]


def _judge_call_events(ledger: AnalysisRouterHandoffLedger) -> list[dict]:
    return [row for row in ledger.event_outbox if row["event_type"] == "judge.call.requested.v1"]


@pytest.mark.asyncio
async def test_analysis_requested_creates_pending_judge_run_and_judge_call_requested_outbox() -> None:
    ledger = AnalysisRouterHandoffLedger()
    trigger_event_id, candidate_group_id, bundle_id = ledger.seed_ready_bundle()

    await _handle(ledger, trigger_event_id)

    assert len(ledger.judge_runs) == 1
    assert ledger.judge_runs[0]["candidate_group_id"] == candidate_group_id
    assert ledger.judge_runs[0]["bundle_id"] == bundle_id
    assert ledger.judge_runs[0]["judge_profile"] == "github_primary"
    assert ledger.judge_runs[0]["model"] == "gpt-5.4-mini"
    assert ledger.judge_runs[0]["reasoning_effort"] == "low"
    assert ledger.judge_runs[0]["status"] == "pending"
    judge_call_events = _judge_call_events(ledger)
    assert len(judge_call_events) == 1
    assert judge_call_events[0]["aggregate_type"] == "judge_run"
    assert judge_call_events[0]["aggregate_id"] == ledger.judge_runs[0]["judge_run_id"]


@pytest.mark.asyncio
async def test_duplicate_processing_reuses_existing_judge_run_without_duplicate_outbox() -> None:
    ledger = AnalysisRouterHandoffLedger()
    trigger_event_id, _, _ = ledger.seed_ready_bundle()

    await _handle(ledger, trigger_event_id)
    await _handle(ledger, trigger_event_id)

    assert len(ledger.judge_runs) == 1
    assert len(_judge_call_events(ledger)) == 1


@pytest.mark.asyncio
async def test_missing_bundle_does_not_emit_judge_call_requested() -> None:
    ledger = AnalysisRouterHandoffLedger()
    trigger_event_id, _, bundle_id = ledger.seed_ready_bundle()
    ledger.bundles.pop(bundle_id)

    await _handle(ledger, trigger_event_id)

    assert ledger.judge_runs == []
    assert _judge_call_events(ledger) == []


@pytest.mark.asyncio
async def test_not_ready_bundle_does_not_emit_judge_call_requested() -> None:
    ledger = AnalysisRouterHandoffLedger()
    trigger_event_id, _, _ = ledger.seed_ready_bundle(ready_for_analysis=False)

    await _handle(ledger, trigger_event_id)

    assert ledger.judge_runs == []
    assert _judge_call_events(ledger) == []


@pytest.mark.asyncio
async def test_candidate_group_mismatch_does_not_emit_judge_call_requested() -> None:
    ledger = AnalysisRouterHandoffLedger()
    trigger_event_id, _, bundle_id = ledger.seed_ready_bundle()
    bundle = ledger.bundles[bundle_id]
    ledger.bundles[bundle_id] = BundleRouteRecord(
        bundle_id=bundle.bundle_id,
        candidate_group_id=uuid4(),
        bundle_profile_version=bundle.bundle_profile_version,
        reroot_count=bundle.reroot_count,
        ready_for_analysis=bundle.ready_for_analysis,
        token_budget_profile=bundle.token_budget_profile,
    )

    await _handle(ledger, trigger_event_id)

    assert ledger.judge_runs == []
    assert _judge_call_events(ledger) == []


@pytest.mark.asyncio
async def test_judge_call_requested_payload_is_thin_handoff_ids_and_route_metadata() -> None:
    ledger = AnalysisRouterHandoffLedger()
    trigger_event_id, candidate_group_id, bundle_id = ledger.seed_ready_bundle(judge_profile="x_primary")

    await _handle(ledger, trigger_event_id)

    payload = _judge_call_events(ledger)[0]["payload_json"]
    assert set(payload) == {
        "judge_run_id",
        "candidate_group_id",
        "bundle_id",
        "judge_profile",
        "model",
        "reasoning_effort",
        "prompt_version",
        "prompt_cache_key",
    }
    assert payload["candidate_group_id"] == str(candidate_group_id)
    assert payload["bundle_id"] == str(bundle_id)
    assert payload["judge_profile"] == "x_primary"
    assert payload["model"] == "gpt-5.4-mini"
    assert payload["reasoning_effort"] == "low"
    assert payload["prompt_version"] == "judge_x_primary_v1"
    assert payload["prompt_cache_key"] == "judge:x_primary:judge_x_primary_v1:judge_output_v1:verdict_policy_v1"
