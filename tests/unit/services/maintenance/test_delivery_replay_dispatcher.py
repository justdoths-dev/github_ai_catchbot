from __future__ import annotations

from uuid import uuid4

import pytest

from services.maintenance.ack_decision import replay_result_allows_ack
from services.maintenance.models import ReplayRequestRecord
from services.maintenance.service import MaintenanceService
from tests.component.services.maintenance._fakes import FakeRepository, config, outbox_event, plan


def _replay_request(
    *,
    root_object_id,
    replay_type: str = "delivery",
    root_object_type: str = "notification_plan",
    status: str = "requested",
):
    return ReplayRequestRecord(
        replay_request_id=uuid4(),
        replay_type=replay_type,
        root_object_type=root_object_type,
        root_object_id=root_object_id,
        status=status,
        requested_by="operator",
    )


def _replay_event(request: ReplayRequestRecord):
    return outbox_event(
        "replay.requested.v1",
        aggregate_type="replay_request",
        aggregate_id=request.replay_request_id,
        payload_json={
            "replay_request_id": str(request.replay_request_id),
            "replay_type": request.replay_type,
            "root_object_type": request.root_object_type,
            "root_object_id": str(request.root_object_id),
            "replay_reason": "operator_requested",
        },
    )


@pytest.mark.asyncio
async def test_delivery_replay_request_for_notification_plan_emits_replay_intent() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    request = _replay_request(root_object_id=notification_plan.notification_plan_id)
    repository.replay_requests[request.replay_request_id] = request
    event = _replay_event(request)
    repository.events[event.event_id] = event
    service = MaintenanceService(config(app_env="test"), repository=repository)

    first_decision = await service.handle_replay_trigger_event(event.event_id)

    assert first_decision is not None
    assert first_decision.action == "emit_replay_intent"
    assert replay_result_allows_ack(first_decision) is True
    assert repository.replay_status_updates == [
        (request.replay_request_id, "dispatched"),
        (request.replay_request_id, "completed"),
    ]
    assert len(repository.plan_created_outbox) == 1
    emitted = repository.plan_created_outbox[0]
    assert emitted["event_type"] == "notification.plan.created.v1"
    assert emitted["aggregate_type"] == "analysis"
    assert emitted["aggregate_id"] == notification_plan.analysis_id
    assert emitted["dedupe_key"] == f"notify:replay-intent:{request.replay_request_id}"
    assert emitted["payload_json"]["notification_plan_id"] == str(notification_plan.notification_plan_id)
    assert emitted["payload_json"]["analysis_id"] == str(notification_plan.analysis_id)
    assert emitted["payload_json"]["candidate_group_id"] == str(notification_plan.candidate_group_id)
    assert emitted["payload_json"]["send_after"] is None
    assert emitted["payload_json"]["replay_reason"] == "explicit_delivery_replay"
    assert emitted["payload_json"]["replay_request_id"] == str(request.replay_request_id)
    assert repository.upstream_recompute_calls == 0
    assert repository.plan_created_outbox_insert_calls == 1
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["attempt_status"] == "succeeded"
    assert repository.plans[notification_plan.notification_plan_id] == notification_plan

    second_decision = await service.handle_replay_trigger_event(event.event_id)

    assert second_decision is not None
    assert second_decision.action == "already_completed_noop"
    assert second_decision.reason_code == "replay_request_already_completed_noop"
    assert replay_result_allows_ack(second_decision) is True
    assert repository.replay_status_updates == [
        (request.replay_request_id, "dispatched"),
        (request.replay_request_id, "completed"),
    ]
    assert repository.plan_created_outbox_insert_calls == 1
    assert len(repository.plan_created_outbox) == 1
    assert len(repository.job_attempts) == 1
    assert repository.plans[notification_plan.notification_plan_id] == notification_plan
    assert repository.upstream_recompute_calls == 0


@pytest.mark.asyncio
async def test_prod_env_guard_rejects_delivery_replay_without_explicit_opt_in() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    request = _replay_request(root_object_id=notification_plan.notification_plan_id)
    repository.replay_requests[request.replay_request_id] = request
    event = _replay_event(request)
    repository.events[event.event_id] = event
    service = MaintenanceService(config(app_env="prod", enable_replay_to_prod_db=False), repository=repository)

    decision = await service.handle_replay_trigger_event(event.event_id)

    assert decision is not None
    assert decision.action == "reject"
    assert replay_result_allows_ack(decision) is False
    assert repository.replay_status_updates == [(request.replay_request_id, "rejected_by_env_guard")]
    assert repository.plan_created_outbox == []
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["error_code"] == "rejected_by_env_guard"


@pytest.mark.asyncio
async def test_prod_env_guard_allows_delivery_replay_with_explicit_opt_in() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    request = _replay_request(root_object_id=notification_plan.notification_plan_id)
    repository.replay_requests[request.replay_request_id] = request
    event = _replay_event(request)
    repository.events[event.event_id] = event
    service = MaintenanceService(config(app_env="prod", enable_replay_to_prod_db=True), repository=repository)

    decision = await service.handle_replay_trigger_event(event.event_id)

    assert decision is not None
    assert decision.action == "emit_replay_intent"
    assert replay_result_allows_ack(decision) is True
    assert repository.replay_status_updates[-1] == (request.replay_request_id, "completed")
    assert len(repository.plan_created_outbox) == 1


@pytest.mark.asyncio
async def test_non_delivery_replay_request_is_rejected_without_downstream_emit() -> None:
    repository = FakeRepository()
    request = _replay_request(replay_type="source", root_object_id=uuid4())
    repository.replay_requests[request.replay_request_id] = request
    event = _replay_event(request)
    repository.events[event.event_id] = event
    service = MaintenanceService(config(app_env="test"), repository=repository)

    decision = await service.handle_replay_trigger_event(event.event_id)

    assert decision is not None
    assert decision.action == "reject"
    assert replay_result_allows_ack(decision) is False
    assert repository.replay_status_updates == [(request.replay_request_id, "unsupported_in_stage41")]
    assert repository.plan_created_outbox == []
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["error_code"] == "unsupported_replay_type"


@pytest.mark.asyncio
async def test_completed_unsupported_replay_is_still_rejected_and_not_ack_safe() -> None:
    repository = FakeRepository()
    request = _replay_request(replay_type="source", root_object_id=uuid4(), status="completed")
    repository.replay_requests[request.replay_request_id] = request
    service = MaintenanceService(config(app_env="test"), repository=repository)

    decision = await service.dispatch_delivery_replay_request(request.replay_request_id)

    assert decision is not None
    assert decision.action == "reject"
    assert decision.reason_code == "unsupported_replay_type"
    assert replay_result_allows_ack(decision) is False
    assert repository.replay_status_updates == [(request.replay_request_id, "unsupported_in_stage41")]
    assert repository.plan_created_outbox_insert_calls == 0
    assert repository.plan_created_outbox == []
    assert len(repository.job_attempts) == 1


@pytest.mark.asyncio
async def test_missing_replay_request_is_rejected_without_durable_mutation_or_ack() -> None:
    repository = FakeRepository()
    service = MaintenanceService(config(app_env="test"), repository=repository)

    decision = await service.dispatch_delivery_replay_request(uuid4())

    assert decision is not None
    assert decision.action == "reject"
    assert decision.reason_code == "replay_request_missing"
    assert replay_result_allows_ack(decision) is False
    assert repository.replay_status_updates == []
    assert repository.plan_created_outbox_insert_calls == 0
    assert repository.plan_created_outbox == []
    assert repository.job_attempts == []


@pytest.mark.asyncio
async def test_missing_notification_plan_fails_closed_without_downstream_emit() -> None:
    repository = FakeRepository()
    request = _replay_request(root_object_id=uuid4())
    repository.replay_requests[request.replay_request_id] = request
    event = _replay_event(request)
    repository.events[event.event_id] = event
    service = MaintenanceService(config(app_env="test"), repository=repository)

    await service.handle_replay_trigger_event(event.event_id)

    assert repository.replay_status_updates == [(request.replay_request_id, "failed")]
    assert repository.plan_created_outbox == []
    assert len(repository.job_attempts) == 1
    assert repository.job_attempts[0]["error_code"] == "notification_plan_missing"
    assert repository.upstream_recompute_calls == 0


@pytest.mark.asyncio
async def test_replay_event_payload_request_id_mismatch_fails_closed_without_status_update() -> None:
    repository = FakeRepository()
    notification_plan = plan(status="failed_terminal", send_after=None)
    repository.plans[notification_plan.notification_plan_id] = notification_plan
    request = _replay_request(root_object_id=notification_plan.notification_plan_id)
    repository.replay_requests[request.replay_request_id] = request
    event = outbox_event(
        "replay.requested.v1",
        aggregate_type="replay_request",
        aggregate_id=request.replay_request_id,
        payload_json={
            "replay_request_id": str(uuid4()),
            "replay_type": "delivery",
            "root_object_type": "notification_plan",
            "root_object_id": str(notification_plan.notification_plan_id),
        },
    )
    repository.events[event.event_id] = event
    service = MaintenanceService(config(app_env="test"), repository=repository)

    await service.handle_replay_trigger_event(event.event_id)

    assert repository.replay_status_updates == []
    assert repository.plan_created_outbox == []
    assert repository.job_attempts == []
