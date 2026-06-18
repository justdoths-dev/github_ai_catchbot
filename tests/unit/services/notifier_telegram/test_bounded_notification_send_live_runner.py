from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.notifier_telegram.bounded_notification_send_dry_run_runner import (
    NotificationDurableReadback,
    NotificationSendContext,
)
from src.services.notifier_telegram.bounded_notification_send_live_runner import (
    BoundedNotificationSendLiveConfig,
    BoundedNotificationSendLiveRuntimeConfig,
    BoundedNotificationSendLiveState,
    NotificationLiveExecution,
    RedisTargetSelection,
    run_bounded_notification_send_live,
)
from src.services.notifier_telegram.models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    DeliveryResult,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotificationRenderDraft,
    StreamMessage,
)
from src.services.outbox_relay.models import OutboxEventRow
from tests.unit.services.notifier_telegram._service_fakes import config as notifier_config


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/notifier_telegram/bounded_notification_send_live_runner.py"
TOOL_PATH = ROOT / "tools/bounded_notification_send_live_runner.py"
TRANSPORT_PATH = ROOT / "src/services/notifier_telegram/transport.py"


class FakeRuntime:
    def __init__(
        self,
        *,
        context: NotificationSendContext,
        selection: RedisTargetSelection | None = None,
        consume_selection: RedisTargetSelection | None = None,
        execution: NotificationLiveExecution | None = None,
        readback: NotificationDurableReadback | None = None,
        execute_error: BaseException | None = None,
        publish_error: BaseException | None = None,
        readback_error: BaseException | None = None,
        ack_error: BaseException | None = None,
        ack_count: int = 1,
        transport_constructed: bool = True,
        telegram_send_called: bool = True,
    ) -> None:
        self.context = context
        self.selection = selection or _matched_selection(context.intent)
        self.consume_selection = consume_selection or self.selection
        self.execution = execution or _execution(context)
        self.readback = readback or _successful_readback()
        self.execute_error = execute_error
        self.publish_error = publish_error
        self.readback_error = readback_error
        self.ack_error = ack_error
        self.ack_count = ack_count
        self.transport_constructed = transport_constructed
        self.telegram_send_called = telegram_send_called
        self.state: BoundedNotificationSendLiveState | None = None
        self.call_order: list[str] = []
        self.acked: list[str] = []

    async def inspect_target(self, config):
        del config
        self.call_order.append("inspect")
        self.state.redis_read_attempted = True
        return self.selection

    async def consume_target(self, expected, config):
        del config
        self.call_order.append("consume")
        self.state.redis_consume_attempted = True
        assert expected == self.selection.message
        return self.consume_selection

    async def load_context(self, trigger_event_id: UUID, config):
        del config
        self.call_order.append("load_context")
        self.state.database_session_opened = True
        self.state.database_read_attempted = True
        assert trigger_event_id == self.context.trigger_event_id
        return self.context

    async def execute_live(self, trigger_event_id: UUID, context, config):
        del config
        self.call_order.append("execute")
        assert trigger_event_id == self.context.trigger_event_id
        assert context == self.context
        self.state.database_write_attempted = True
        self.state.render_write_attempted = True
        self.state.delivery_record_write_attempted = True
        self.state.delivery_result_outbox_write_attempted = True
        self.state.telegram_transport_constructed = self.transport_constructed
        self.state.telegram_send_called = self.telegram_send_called
        if self.execute_error is not None:
            self.state.database_rolled_back = True
            raise self.execute_error
        self.state.database_committed = True
        return self.execution

    async def publish_maintenance(self, event_row):
        self.call_order.append("publish_maintenance")
        self.state.maintenance_redis_publish_attempted = True
        assert event_row.event_type == "notification.delivery.result.v1"
        if self.publish_error is not None:
            raise self.publish_error
        return "1718000000001-0"

    async def mark_delivery_result_published(self, event_id: UUID):
        self.call_order.append("mark_published")
        self.state.maintenance_outbox_status_update_attempted = True
        assert event_id == self.execution.delivery_result_event_row.event_id
        self.state.maintenance_outbox_status_committed = True

    async def readback_final_state(self, execution):
        self.call_order.append("readback")
        assert execution.notification_delivery_record_id == self.execution.notification_delivery_record_id
        if self.readback_error is not None:
            raise self.readback_error
        return self.readback

    async def ack(self, message_id: str):
        self.call_order.append("ack")
        self.state.redis_ack_attempted = True
        self.acked.append(message_id)
        if self.ack_error is not None:
            raise self.ack_error
        return self.ack_count

    async def close(self):
        self.call_order.append("close")


class FakeRuntimeBuilder:
    def __init__(self, runtime: FakeRuntime) -> None:
        self.runtime = runtime

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.runtime.state = state
        return self.runtime


def _runtime_config_loader(*, send_enabled: bool = True, dry_run: bool = False):
    def _load(cfg) -> BoundedNotificationSendLiveRuntimeConfig:
        del cfg
        return BoundedNotificationSendLiveRuntimeConfig(
            notifier_config=notifier_config(
                dry_run=dry_run,
                enable_notification_send=send_enabled,
                allow_edits=False,
            ),
            redis_url="red" + "is://not-in-report",
        )

    return _load


def _base_config(intent: NotificationIntentJob, *, mode: str = "execute", telegram_gates: bool = True):
    return BoundedNotificationSendLiveConfig(
        mode=mode,
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        allow_database_write=True,
        allow_redis_read=True,
        allow_redis_consume=True,
        allow_redis_ack=True,
        allow_maintenance_publish=True,
        allow_render_write=True,
        allow_delivery_record_write=True,
        allow_delivery_result_outbox_write=True,
        allow_telegram_transport=telegram_gates,
        allow_telegram_send=telegram_gates,
        trigger_event_suffix=str(intent.trigger_event_id)[-8:],
        notification_plan_id_suffix=str(intent.notification_plan_id)[-8:],
        analysis_id_suffix=str(intent.analysis_id)[-8:],
        target_chat_id_suffix=str(intent.target_chat_id)[-4:],
        redis_message_suffix="0000000000000-0"[-8:],
        scan_limit=5,
    )


def _preview_config(intent: NotificationIntentJob) -> BoundedNotificationSendLiveConfig:
    base = _base_config(intent, mode="preview", telegram_gates=False)
    return BoundedNotificationSendLiveConfig(
        mode="preview",
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        allow_redis_read=True,
        trigger_event_suffix=base.trigger_event_suffix,
        notification_plan_id_suffix=base.notification_plan_id_suffix,
        analysis_id_suffix=base.analysis_id_suffix,
        target_chat_id_suffix=base.target_chat_id_suffix,
        redis_message_suffix=base.redis_message_suffix,
        scan_limit=base.scan_limit,
    )


def _intent(*, send_after=None) -> NotificationIntentJob:
    return NotificationIntentJob(
        trigger_event_id=uuid4(),
        event_type="notification.plan.created.v1",
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject",
        material_change_hash="material",
        send_after=send_after,
        suppress_reason_code=None,
    )


def _context(
    intent: NotificationIntentJob,
    *,
    error_code: str | None = None,
    planned_action: str = "preview_only",
) -> NotificationSendContext:
    event_row = OutboxEventRow(
        event_id=intent.trigger_event_id,
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=intent.analysis_id,
        dedupe_key=f"notify:{intent.analysis_id}",
        payload_json={
            "notification_plan_id": str(intent.notification_plan_id),
            "analysis_id": str(intent.analysis_id),
            "candidate_group_id": str(intent.candidate_group_id),
            "target_chat_id": intent.target_chat_id,
            "delivery_decision": intent.delivery_decision,
            "urgency_profile": intent.urgency_profile,
            "dedupe_subject_key": intent.dedupe_subject_key,
            "material_change_hash": intent.material_change_hash,
        },
        status="published",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    analysis = AnalysisRenderContext(
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        judge_output_id=uuid4(),
        verdict="inspect_now",
        delivery_decision=intent.delivery_decision,
        reason_codes_json=["policy_pass"],
        evidence_limitations_ko="limited",
        recommended_action_ko="inspect",
        freshness_note_ko=None,
    )
    candidate = CandidateRenderContext(
        candidate_group_id=intent.candidate_group_id,
        source_message_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        primary_artifact_type="github_repo",
        primary_canonical_url=None,
        primary_canonical_id="example/repo",
        source_message_link=None,
        source_text_surface=None,
    )
    return NotificationSendContext(
        trigger_event_id=intent.trigger_event_id,
        event_row=event_row,
        intent=intent,
        analysis=analysis,
        judge_output=JudgeOutputRenderContext(judge_output_id=analysis.judge_output_id, payload_json={"headline": "x"}),
        candidate=candidate,
        plan_id=intent.notification_plan_id,
        existing_plan_status=None,
        plan_action="concretize",
        render_draft=None
        if error_code is not None
        else NotificationRenderDraft(
            notification_plan_id=intent.notification_plan_id,
            message_text="Rendered operator text that must stay out of reports",
            entities_json=[],
            link_preview_options_json={"is_disabled": True},
            reply_markup_json=None,
            disable_notification=False,
            protect_content=False,
            parse_strategy="entities",
            render_hash="render-hash",
        ),
        render_action="not_due" if error_code == "notification_send_after_deferred" else "append_render",
        delivery_action="defer_until_due" if error_code == "notification_send_after_deferred" else "send",
        delivery_status=None if error_code else "would_send",
        planned_action=planned_action,
        error_code=error_code,
    )


def _execution(
    context: NotificationSendContext,
    *,
    delivery_result: DeliveryResult | None = None,
    write_counts: dict[str, int] | None = None,
) -> NotificationLiveExecution:
    result = delivery_result or DeliveryResult(
        delivery_status="sent",
        telegram_chat_id=context.intent.target_chat_id,
        telegram_message_id=777,
        attempt_count=1,
        telegram_response_json={"ok": True, "result": {"message_id": 777, "chat": {"id": context.intent.target_chat_id}}},
    )
    event_row = OutboxEventRow(
        event_id=uuid4(),
        event_type="notification.delivery.result.v1",
        aggregate_type="notification_plan",
        aggregate_id=context.plan_id,
        dedupe_key=f"notification-delivery-result:{context.plan_id}:{uuid4()}",
        payload_json={
            "notification_plan_id": str(context.plan_id),
            "delivery_status": result.delivery_status,
            "attempt_count": result.attempt_count,
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    return NotificationLiveExecution(
        context=replace(
            context,
            delivery_status=result.delivery_status,
            delivery_action="send" if result.delivery_status == "sent" else "transport_failure",
            planned_action="execute_live_delivery",
        ),
        delivery_result=result,
        notification_delivery_record_id=uuid4(),
        delivery_result_event_row=event_row,
        notifier_owned_write_counts=write_counts
        or {
            "notification_plans_insert_calls": 1,
            "notification_renders_insert_calls": 1,
            "notification_delivery_records_insert_calls": 1,
            "event_outbox_delivery_result_insert_calls": 1,
        },
    )


def _successful_readback() -> NotificationDurableReadback:
    return NotificationDurableReadback(
        notification_plan_count=1,
        notification_plan_material_count=1,
        notification_render_count=1,
        notification_delivery_record_count=1,
        notification_delivery_result_event_count=1,
        delivery_result_event_status="published",
        q_maintenance_route_verified=True,
        q_maintenance_message_thin=True,
    )


def _message(intent: NotificationIntentJob) -> StreamMessage:
    return StreamMessage(
        stream="q.notification.send",
        message_id="1718000000000-0",
        fields={
            "job_id": str(intent.trigger_event_id),
            "stage_name": "notify",
            "root_object_type": "analysis",
            "root_object_id": str(intent.analysis_id),
            "idempotency_key": f"notify:{intent.analysis_id}",
            "pipeline_run_id": "",
            "not_before": "",
            "trigger_event_id": str(intent.trigger_event_id),
        },
    )


def _matched_selection(intent: NotificationIntentJob, *, target_is_next: bool = True) -> RedisTargetSelection:
    message = _message(intent)
    return RedisTargetSelection(
        status="matched",
        error_code=None,
        message=message,
        redis_message_count=1,
        group_lag=1,
        group_pending=0,
        message_stage_name="notify",
        message_root_object_type="analysis",
        trigger_event_id_present=True,
        analysis_id_present=True,
        target_is_next=target_is_next,
    )


@pytest.mark.asyncio
async def test_preview_exact_target_rehydrates_without_writes_or_transport() -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent))

    result = await run_bounded_notification_send_live(
        _preview_config(intent),
        runtime_config_loader=_runtime_config_loader(send_enabled=True),
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["mode"] == "preview"
    assert report["side_effects"]["redis_consume_attempted"] is False
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["side_effects"]["telegram_transport_constructed"] is False
    assert runtime.call_order == ["inspect", "load_context", "close"]


@pytest.mark.asyncio
async def test_preview_group_missing_fails_closed_before_db_or_transport() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        context=_context(intent),
        selection=RedisTargetSelection(status="blocked", error_code="consumer_group_missing"),
    )

    result = await run_bounded_notification_send_live(
        _preview_config(intent),
        runtime_config_loader=_runtime_config_loader(send_enabled=True),
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "consumer_group_missing"
    assert report["side_effects"]["database_read_attempted"] is False
    assert report["side_effects"]["telegram_transport_constructed"] is False
    assert runtime.call_order == ["inspect", "close"]


@pytest.mark.asyncio
async def test_preview_target_not_next_unconsumed_fails_closed() -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent), selection=_matched_selection(intent, target_is_next=False))

    result = await run_bounded_notification_send_live(
        _preview_config(intent),
        runtime_config_loader=_runtime_config_loader(send_enabled=True),
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "selected_target_not_next_unconsumed"
    assert report["side_effects"]["redis_consume_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False


@pytest.mark.asyncio
async def test_pending_target_under_other_consumer_fails_closed_without_takeover() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        context=_context(intent),
        selection=RedisTargetSelection(
            status="blocked",
            error_code="redis_pending_messages_present",
            group_pending=1,
            group_lag=1,
        ),
    )

    result = await run_bounded_notification_send_live(
        _preview_config(intent),
        runtime_config_loader=_runtime_config_loader(send_enabled=True),
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["error_code"] == "redis_pending_messages_present"
    assert report["side_effects"]["redis_consume_attempted"] is False
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert runtime.call_order == ["inspect", "close"]


@pytest.mark.asyncio
async def test_forbidden_redis_payload_fails_closed_before_db_write_or_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        context=_context(intent),
        selection=RedisTargetSelection(
            status="blocked",
            error_code="forbidden_redis_payload_field",
            redis_message_count=1,
            group_lag=1,
            group_pending=0,
            checks_failed=("forbidden_redis_payload_field",),
        ),
    )

    report = (
        await run_bounded_notification_send_live(
            _preview_config(intent),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "forbidden_redis_payload_field"
    assert report["side_effects"]["database_read_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert runtime.call_order == ["inspect", "close"]


@pytest.mark.asyncio
async def test_future_send_after_blocks_before_consume_write_or_ack() -> None:
    intent = _intent(send_after=datetime.now(timezone.utc) + timedelta(hours=1))
    runtime = FakeRuntime(
        context=_context(
            intent,
            error_code="notification_send_after_deferred",
            planned_action="wait_until_send_after",
        )
    )

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "notification_send_after_deferred"
    assert report["planned_action"] == "wait_until_send_after"
    assert report["side_effects"]["redis_consume_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert runtime.call_order == ["inspect", "load_context", "close"]


@pytest.mark.asyncio
async def test_execute_success_publishes_maintenance_then_acks() -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent))

    result = await run_bounded_notification_send_live(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader(send_enabled=True),
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()
    encoded = json.dumps(report, sort_keys=True)

    assert report["status"] == "pass"
    assert report["delivery_status"] == "sent"
    assert report["telegram_chat_id_present"] is True
    assert report["telegram_message_id_present"] is True
    assert report["q_maintenance_published"] is True
    assert report["redis_ack_status"] == "acked"
    assert report["redis_acked_count"] == 1
    assert report["durable_readback"]["ack_safe"] is True
    assert report["durable_readback"]["notification_plan_exactly_once"] is True
    assert report["durable_readback"]["notification_render_exactly_once"] is True
    assert report["durable_readback"]["notification_delivery_record_exactly_once"] is True
    assert report["durable_readback"]["notification_delivery_result_event_exactly_once"] is True
    assert report["durable_readback"]["delivery_result_event_published"] is True
    assert report["durable_readback"]["q_maintenance_message_thin"] is True
    assert report["redis_ack_after_durable_readback"] is True
    assert report["target_chat_suffix_verified"] is True
    assert report["side_effects"]["telegram_transport_constructed"] is True
    assert report["side_effects"]["telegram_send_called"] is True
    assert runtime.call_order == [
        "inspect",
        "load_context",
        "consume",
        "execute",
        "publish_maintenance",
        "mark_published",
        "readback",
        "ack",
        "close",
    ]
    assert str(intent.target_chat_id) not in encoded
    assert "Rendered operator text" not in encoded
    assert "1718000000001-0" not in encoded


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delivery_status", "error_code", "error_class"),
    [
        ("failed_retryable", "telegram_network_retryable", "TelegramTransportRetryableError"),
        ("failed_terminal", "telegram_invalid_chat", "TelegramTransportTerminalError"),
    ],
)
async def test_transport_failure_records_result_handoff_and_ack(delivery_status: str, error_code: str, error_class: str) -> None:
    intent = _intent()
    context = _context(intent)
    result = DeliveryResult(
        delivery_status=delivery_status,  # type: ignore[arg-type]
        telegram_chat_id=intent.target_chat_id,
        telegram_message_id=None,
        attempt_count=1,
        transport_error_code=error_code,
        transport_error_class=error_class,
    )
    runtime = FakeRuntime(context=context, execution=_execution(context, delivery_result=result))

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["delivery_status"] == delivery_status
    assert report["transport_error_code"] == error_code
    assert report["q_maintenance_published"] is True
    assert report["redis_ack_status"] == "acked"
    assert runtime.call_order.index("publish_maintenance") < runtime.call_order.index("readback")
    assert runtime.call_order.index("readback") < runtime.call_order.index("ack")


@pytest.mark.asyncio
async def test_handoff_failure_does_not_ack_original_message() -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent), publish_error=RuntimeError("private redis detail"))

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "failed"
    assert report["error_code"] == "q_maintenance_publish_failed"
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_ack_failure_after_durable_completion_is_reported_without_retry() -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent), ack_count=0)

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "failed"
    assert report["error_code"] == "ack_failed_after_durable_completion"
    assert report["q_maintenance_published"] is True
    assert report["side_effects"]["maintenance_outbox_status_committed"] is True
    assert report["redis_ack_status"] == "failed_after_durable_completion"
    assert runtime.acked == ["1718000000000-0"]


@pytest.mark.asyncio
async def test_send_disabled_config_suppresses_without_transport_and_still_hands_off() -> None:
    intent = _intent()
    context = _context(intent)
    suppressed = DeliveryResult(
        delivery_status="suppressed",
        telegram_chat_id=intent.target_chat_id,
        telegram_message_id=None,
        attempt_count=0,
        transport_error_code="notification_send_flag_disabled",
        telegram_response_json={"send_disabled": True, "transport_skipped": True},
    )
    runtime = FakeRuntime(
        context=context,
        execution=_execution(
            replace(context, delivery_action="suppress_send_disabled_no_transport", delivery_status="suppressed"),
            delivery_result=suppressed,
        ),
        transport_constructed=False,
        telegram_send_called=False,
    )

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent, telegram_gates=False),
            runtime_config_loader=_runtime_config_loader(send_enabled=False),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["delivery_status"] == "suppressed"
    assert report["transport_error_code"] == "notification_send_flag_disabled"
    assert report["transport_gate_mode"] == "suppressed_by_config"
    assert report["side_effects"]["telegram_transport_constructed"] is False
    assert report["side_effects"]["telegram_send_called"] is False
    assert report["q_maintenance_published"] is True
    assert report["redis_ack_status"] == "acked"
    assert report["durable_readback"]["ack_safe"] is True
    assert report["redis_ack_after_durable_readback"] is True


@pytest.mark.asyncio
async def test_live_send_requires_telegram_gates_when_config_enabled() -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent))

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent, telegram_gates=False),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "telegram_transport_not_allowed"
    assert report["side_effects"]["redis_read_attempted"] is False
    assert runtime.call_order == []


@pytest.mark.asyncio
async def test_full_uuid_and_full_redis_id_selectors_are_rejected_without_echo() -> None:
    intent = _intent()
    full_uuid = str(intent.trigger_event_id)
    full_stream_id = "1718000000000-0"
    runtime = FakeRuntime(context=_context(intent))

    uuid_report = (
        await run_bounded_notification_send_live(
            replace(_base_config(intent), trigger_event_suffix=full_uuid),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()
    redis_report = (
        await run_bounded_notification_send_live(
            replace(_base_config(intent), redis_message_suffix=full_stream_id),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert uuid_report["error_code"] == "trigger_event_suffix_missing_or_invalid"
    assert redis_report["error_code"] == "redis_message_suffix_invalid"
    assert full_uuid not in json.dumps(uuid_report, sort_keys=True)
    assert full_stream_id not in json.dumps(redis_report, sort_keys=True)
    assert runtime.call_order == []


@pytest.mark.asyncio
async def test_target_chat_suffix_is_required_and_verified_without_raw_chat_id() -> None:
    intent = _intent()
    context = _context(intent)
    runtime = FakeRuntime(context=context)

    missing = (
        await run_bounded_notification_send_live(
            replace(_base_config(intent), target_chat_id_suffix=None),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()
    mismatch = (
        await run_bounded_notification_send_live(
            replace(_base_config(intent), target_chat_id_suffix="9999"),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert missing["error_code"] == "target_chat_id_suffix_missing_or_invalid"
    assert mismatch["error_code"] == "target_chat_id_mismatch"
    assert str(intent.target_chat_id) not in json.dumps(mismatch, sort_keys=True)


@pytest.mark.asyncio
async def test_durable_readback_failure_blocks_ack_after_publish_mark() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        context=_context(intent),
        readback=NotificationDurableReadback(
            notification_plan_count=1,
            notification_plan_material_count=1,
            notification_render_count=0,
            notification_delivery_record_count=1,
            notification_delivery_result_event_count=1,
            delivery_result_event_status="published",
            q_maintenance_route_verified=True,
            q_maintenance_message_thin=True,
            checks_failed=("notification_render_readback_not_exactly_once",),
        ),
    )

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "failed"
    assert report["error_code"] == "durable_readback_failed"
    assert report["durable_readback"]["ack_safe"] is False
    assert report["durable_readback"]["checks_failed"] == ["notification_render_readback_not_exactly_once"]
    assert report["side_effects"]["maintenance_outbox_status_committed"] is True
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert runtime.acked == []
    assert runtime.call_order == [
        "inspect",
        "load_context",
        "consume",
        "execute",
        "publish_maintenance",
        "mark_published",
        "readback",
        "close",
    ]


@pytest.mark.asyncio
async def test_already_published_delivery_result_acks_after_readback_without_republish() -> None:
    intent = _intent()
    context = _context(intent)
    execution = _execution(
        context,
        write_counts={
            "notification_plans_insert_calls": 0,
            "notification_renders_insert_calls": 0,
            "notification_delivery_records_insert_calls": 0,
            "event_outbox_delivery_result_insert_calls": 0,
        },
    )
    execution = replace(
        execution,
        delivery_result_event_row=replace(execution.delivery_result_event_row, status="published"),
    )
    runtime = FakeRuntime(context=context, execution=execution)

    report = (
        await run_bounded_notification_send_live(
            _base_config(intent),
            runtime_config_loader=_runtime_config_loader(send_enabled=True),
            runtime_builder=FakeRuntimeBuilder(runtime),
        )
    ).to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["q_maintenance_published"] is False
    assert report["q_maintenance_message_suffix"] is None
    assert report["redis_ack_status"] == "acked"
    assert report["durable_readback"]["ack_safe"] is True
    assert report["notifier_owned_write_counts"]["notification_plans_insert_calls"] == 0
    assert report["notifier_owned_write_counts"]["notification_renders_insert_calls"] == 0
    assert report["notifier_owned_write_counts"]["notification_delivery_records_insert_calls"] == 0
    assert report["notifier_owned_write_counts"]["event_outbox_delivery_result_insert_calls"] == 0
    assert runtime.call_order == ["inspect", "load_context", "consume", "execute", "readback", "ack", "close"]


def test_exact_live_runner_static_authority_guards() -> None:
    for path in (SOURCE_PATH, TOOL_PATH):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots: set[str] = set()
        imported_modules: set[str] = set()
        forbidden_attrs = {
            "send_message",
            "edit_message_text",
            "run_forever",
            "xclaim",
            "xautoclaim",
            "xgroup_create",
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_attrs

        assert {"openai", "requests", "httpx", "aiohttp", "subprocess", "docker"}.isdisjoint(imported_roots)
        assert not any(".gh_enricher" in module for module in imported_modules)
        assert not any(".x_enricher" in module for module in imported_modules)
        assert not any(".web_enricher" in module for module in imported_modules)
        assert not any(".judge_openai" in module for module in imported_modules)
        lowered = source.lower()
        for forbidden in (
            "systemctl",
            "runtime.env",
            "run_forever(",
            "xclaim",
            "xautoclaim",
            "xgroup_create",
            "policy_engine",
            "analysis_router",
            "judge_openai",
        ):
            assert forbidden not in lowered

    transport_source = TRANSPORT_PATH.read_text(encoding="utf-8")
    assert ".send_message(" in transport_source
    assert ".edit_message_text(" in transport_source
