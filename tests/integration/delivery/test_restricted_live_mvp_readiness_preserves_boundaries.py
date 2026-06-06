from __future__ import annotations

import json
from copy import deepcopy
from uuid import uuid4

import pytest

from services.maintenance import main as maintenance_main
from services.maintenance.delivery_gate_runner import DeliveryGateRunner
from services.maintenance.models import DeliveryGateSnapshot
from services.maintenance.mvp_readiness import REQUIRED_RECOVERY_CLI_CHECKS, UPSTREAM_HOT_PATH_CHECKS
from tests.unit.services.maintenance.test_delivery_gate_runner import _config, _snapshot


class BoundaryReadinessRepository:
    def __init__(self, snapshot: DeliveryGateSnapshot) -> None:
        self.snapshot = snapshot
        self.event_outbox: list[dict] = []
        self.replay_requests: list[dict] = []
        self.notification_plans: dict = {}
        self.notification_renders: dict = {}
        self.notification_delivery_records: dict = {}
        self.state_transitions: list[dict] = []
        self.analyses: dict = {}
        self.judge_outputs: dict = {}
        self.candidate_groups: dict = {}
        self.evidence_bundles: dict = {}
        self.source_messages: dict = {}
        self.artifacts: dict = {}
        self.redis_dispatches: list[dict] = []
        self.telegram_calls: list[dict] = []
        self.openai_calls: list[dict] = []

    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot:
        return self.snapshot

    async def insert_replay_requests_for_selected_plans(self, **kwargs):
        raise AssertionError("mvp-readiness must not create replay_requests")

    async def insert_plan_created_outbox(self, **kwargs):
        raise AssertionError("mvp-readiness must not create event_outbox")

    async def insert_manual_retry_intent_outbox(self, **kwargs):
        raise AssertionError("mvp-readiness must not create manual retry outbox")


def _recovery_cli_surface() -> dict[str, bool]:
    return {check_name: True for check_name in REQUIRED_RECOVERY_CLI_CHECKS}


def _upstream_statuses() -> dict[str, str]:
    return {check_name: "pass" for check_name in UPSTREAM_HOT_PATH_CHECKS}


@pytest.mark.asyncio
async def test_mvp_readiness_report_preserves_delivery_and_upstream_ledgers(monkeypatch) -> None:
    from services.judge_openai import openai_client
    from services.notifier_telegram import worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams

    def fail_transport(*args, **kwargs):
        raise AssertionError("mvp-readiness must not instantiate dispatch or transport clients")

    async def fail_replay_selected(self, **kwargs):
        raise AssertionError("mvp-readiness must not call replay-selected")

    async def fail_retry_selected_due(self, **kwargs):
        raise AssertionError("mvp-readiness must not call retry-selected-due")

    monkeypatch.setattr(maintenance_main.DeliveryBatchRecoveryTool, "replay_selected", fail_replay_selected)
    monkeypatch.setattr(maintenance_main.DeliveryBatchRecoveryTool, "retry_selected_due", fail_retry_selected_due)
    monkeypatch.setattr(notifier_worker, "NotifierTelegramWorker", fail_transport)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_transport)
    monkeypatch.setattr(openai_client, "OpenAIJudgeClient", fail_transport)

    repository = BoundaryReadinessRepository(_snapshot())
    plan_id = uuid4()
    render_id = uuid4()
    delivery_record_id = uuid4()
    state_transition_id = uuid4()
    analysis_id = uuid4()
    judge_output_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    source_message_id = uuid4()
    artifact_id = uuid4()
    repository.notification_plans = {plan_id: {"status": "ready"}}
    repository.notification_renders = {render_id: {"notification_plan_id": plan_id}}
    repository.notification_delivery_records = {delivery_record_id: {"notification_plan_id": plan_id}}
    repository.state_transitions = [{"state_transition_id": state_transition_id, "object_id": plan_id}]
    repository.analyses = {analysis_id: {"candidate_group_id": candidate_group_id}}
    repository.judge_outputs = {judge_output_id: {"analysis_id": analysis_id, "schema_version": "judge_output_v1"}}
    repository.candidate_groups = {candidate_group_id: {"status": "ready_for_analysis"}}
    repository.evidence_bundles = {bundle_id: {"candidate_group_id": candidate_group_id}}
    repository.source_messages = {source_message_id: {"platform": "telegram"}}
    repository.artifacts = {artifact_id: {"source_message_id": source_message_id}}
    before = deepcopy(
        {
            "event_outbox": repository.event_outbox,
            "replay_requests": repository.replay_requests,
            "notification_plans": repository.notification_plans,
            "notification_renders": repository.notification_renders,
            "notification_delivery_records": repository.notification_delivery_records,
            "state_transitions": repository.state_transitions,
            "analyses": repository.analyses,
            "judge_outputs": repository.judge_outputs,
            "candidate_groups": repository.candidate_groups,
            "evidence_bundles": repository.evidence_bundles,
            "source_messages": repository.source_messages,
            "artifacts": repository.artifacts,
            "redis_dispatches": repository.redis_dispatches,
            "telegram_calls": repository.telegram_calls,
            "openai_calls": repository.openai_calls,
        }
    )
    emitted: list[str] = []

    exit_code = await maintenance_main.run_mvp_readiness(
        _config(),
        maintenance_main.build_parser().parse_args(["mvp-readiness", "--mode", "restricted", "--format", "json"]),
        DeliveryGateRunner(_config(), repository=repository),
        recovery_cli_surface=_recovery_cli_surface(),
        upstream_component_statuses=_upstream_statuses(),
        emit_json=emitted.append,
    )

    assert exit_code == 0
    assert {
        "event_outbox": repository.event_outbox,
        "replay_requests": repository.replay_requests,
        "notification_plans": repository.notification_plans,
        "notification_renders": repository.notification_renders,
        "notification_delivery_records": repository.notification_delivery_records,
        "state_transitions": repository.state_transitions,
        "analyses": repository.analyses,
        "judge_outputs": repository.judge_outputs,
        "candidate_groups": repository.candidate_groups,
        "evidence_bundles": repository.evidence_bundles,
        "source_messages": repository.source_messages,
        "artifacts": repository.artifacts,
        "redis_dispatches": repository.redis_dispatches,
        "telegram_calls": repository.telegram_calls,
        "openai_calls": repository.openai_calls,
    } == before
    assert json.loads(emitted[0])["readiness_status"] == "pass"
