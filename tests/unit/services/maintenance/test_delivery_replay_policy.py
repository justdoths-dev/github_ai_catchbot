from __future__ import annotations

from uuid import uuid4

from services.maintenance.config import MaintenanceConfig
from services.maintenance.delivery_replay import evaluate_delivery_replay
from services.maintenance.models import NotificationPlanRecord, ReplayRequestRecord


def _config(*, app_env: str = "test", enable_replay_to_prod_db: bool = False) -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env=app_env,
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        maintenance_consumer_name="test",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
        replay_consumer_name="test",
        batch_size=50,
        block_ms=100,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=True,
        notifier_telegram_dry_run=False,
        enable_delivery_retry_promotion=True,
        enable_replay_to_prod_db=enable_replay_to_prod_db,
        delivery_gate_min_success_rate_1h=0.99,
        delivery_gate_min_success_rate_24h=0.99,
        delivery_gate_max_high_source_to_delivery_p95_sec=120,
        delivery_gate_max_plan_to_transport_p95_sec=120,
        delivery_gate_max_due_retry_lag_sec=120,
        delivery_gate_max_open_dlq_count=0,
        delivery_gate_max_send_disabled_count=0,
        delivery_gate_max_replay_guard_reject_count=0,
        delivery_gate_require_operator_review_for_full=True,
        log_level="INFO",
    )


def _plan(plan_id):
    return NotificationPlanRecord(
        notification_plan_id=plan_id,
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject",
        material_change_hash="material",
        send_after=None,
        suppress_reason_code=None,
        status="sent",
    )


def _request(*, replay_type: str = "delivery", root_object_type: str = "notification_plan", root_object_id=None):
    return ReplayRequestRecord(
        replay_request_id=uuid4(),
        replay_type=replay_type,
        root_object_type=root_object_type,
        root_object_id=root_object_id or uuid4(),
        status="pending",
    )


def test_delivery_replay_from_notification_plan_is_valid() -> None:
    plan_id = uuid4()
    request = _request(root_object_id=plan_id)

    decision = evaluate_delivery_replay(config=_config(), replay_request=request, plan=_plan(plan_id))

    assert decision.action == "emit_replay_intent"
    assert decision.payload is not None
    assert decision.payload["notification_plan_id"] == str(plan_id)


def test_full_pipeline_replay_is_rejected() -> None:
    plan_id = uuid4()
    decision = evaluate_delivery_replay(
        config=_config(),
        replay_request=_request(replay_type="full_pipeline", root_object_id=plan_id),
        plan=_plan(plan_id),
    )

    assert decision.action == "reject"
    assert decision.reason_code == "unsupported_replay_type"


def test_analysis_root_is_rejected() -> None:
    decision = evaluate_delivery_replay(
        config=_config(),
        replay_request=_request(root_object_type="analysis"),
        plan=None,
    )

    assert decision.action == "reject"
    assert decision.reason_code == "unsupported_replay_root"


def test_prod_without_enable_replay_to_prod_db_is_rejected() -> None:
    plan_id = uuid4()
    decision = evaluate_delivery_replay(
        config=_config(app_env="prod", enable_replay_to_prod_db=False),
        replay_request=_request(root_object_id=plan_id),
        plan=_plan(plan_id),
    )

    assert decision.action == "reject"
    assert decision.reason_code == "rejected_by_env_guard"


def test_dev_replay_is_accepted() -> None:
    plan_id = uuid4()
    decision = evaluate_delivery_replay(
        config=_config(app_env="replay"),
        replay_request=_request(root_object_id=plan_id),
        plan=_plan(plan_id),
    )

    assert decision.action == "emit_replay_intent"
