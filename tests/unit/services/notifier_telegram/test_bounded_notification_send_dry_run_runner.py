from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.notifier_telegram.bounded_notification_send_dry_run_runner import (
    DELIVERY_RESULT_EVENT_TYPE,
    DRY_RUN_REASON_CODE,
    EVENT_TYPE,
    QUEUE_NAME,
    BoundedNotificationSendDryRunConfig,
    BoundedNotificationSendDryRunRuntimeConfig,
    NotificationDryRunExecution,
    NotificationSendContext,
    RedisTargetSelection,
    run_bounded_notification_send_dry_run,
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
SOURCE_PATH = ROOT / "src/services/notifier_telegram/bounded_notification_send_dry_run_runner.py"
TOOL_PATH = ROOT / "tools/bounded_notification_send_dry_run_runner.py"


class FakeRuntime:
    def __init__(
        self,
        *,
        context: NotificationSendContext,
        selection: RedisTargetSelection | None = None,
        consume_selection: RedisTargetSelection | None = None,
        execution: NotificationDryRunExecution | None = None,
        execute_error: BaseException | None = None,
        publish_error: BaseException | None = None,
        ack_count: int = 1,
    ) -> None:
        self.context = context
        self.selection = selection or _matched_selection(context.intent)
        self.consume_selection = consume_selection or self.selection
        self.execution = execution or _execution(context)
        self.execute_error = execute_error
        self.publish_error = publish_error
        self.ack_count = ack_count
        self.state = None
        self.call_order: list[str] = []
        self.acked: list[str] = []
        self.closed = False

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

    async def execute_dry_run(self, trigger_event_id: UUID, config):
        del config
        self.call_order.append("execute")
        assert trigger_event_id == self.context.trigger_event_id
        self.state.database_write_attempted = True
        self.state.render_write_attempted = True
        self.state.delivery_record_write_attempted = True
        self.state.delivery_result_outbox_write_attempted = True
        if self.execute_error is not None:
            self.state.database_rolled_back = True
            raise self.execute_error
        self.state.database_committed = True
        return self.execution

    async def publish_maintenance(self, event_row):
        self.call_order.append("publish_maintenance")
        self.state.maintenance_redis_publish_attempted = True
        assert event_row.event_type == DELIVERY_RESULT_EVENT_TYPE
        if self.publish_error is not None:
            raise self.publish_error
        return "1718000000001-0"

    async def mark_delivery_result_published(self, event_id: UUID):
        self.call_order.append("mark_published")
        self.state.maintenance_outbox_status_update_attempted = True
        assert event_id == self.execution.delivery_result_event_row.event_id
        self.state.maintenance_outbox_status_committed = True

    async def ack(self, message_id: str):
        self.call_order.append("ack")
        self.state.redis_ack_attempted = True
        self.acked.append(message_id)
        return self.ack_count

    async def close(self):
        self.call_order.append("close")
        self.closed = True


class FakeRuntimeBuilder:
    def __init__(self, runtime: FakeRuntime) -> None:
        self.runtime = runtime

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.runtime.state = state
        return self.runtime


def _runtime_config_loader(cfg) -> BoundedNotificationSendDryRunRuntimeConfig:
    del cfg
    return BoundedNotificationSendDryRunRuntimeConfig(
        notifier_config=notifier_config(dry_run=True, enable_notification_send=False, allow_edits=False),
        redis_url="red" + "is://not-in-report",
    )


def _base_config(intent: NotificationIntentJob, *, mode: str = "execute") -> BoundedNotificationSendDryRunConfig:
    return BoundedNotificationSendDryRunConfig(
        mode=mode,
        operator_approved=True,
        allow_runtime_config=True,
        allow_redis_read=True,
        allow_database_read=True,
        allow_redis_consume=True,
        allow_database_write=True,
        allow_redis_ack=True,
        allow_render_write=True,
        allow_delivery_record_write=True,
        allow_delivery_result_outbox_write=True,
        allow_maintenance_outbox_publish=True,
        allow_maintenance_redis_publish=True,
        trigger_event_suffix=str(intent.trigger_event_id)[-8:],
        notification_plan_id_suffix=str(intent.notification_plan_id)[-8:],
        analysis_id_suffix=str(intent.analysis_id)[-8:],
        redis_message_suffix="0000000000000-0"[-8:],
        scan_limit=5,
    )


def _preview_config(intent: NotificationIntentJob) -> BoundedNotificationSendDryRunConfig:
    base = _base_config(intent, mode="preview")
    return BoundedNotificationSendDryRunConfig(
        mode="preview",
        operator_approved=True,
        allow_runtime_config=True,
        allow_redis_read=True,
        allow_database_read=True,
        trigger_event_suffix=base.trigger_event_suffix,
        notification_plan_id_suffix=base.notification_plan_id_suffix,
        analysis_id_suffix=base.analysis_id_suffix,
        redis_message_suffix=base.redis_message_suffix,
        scan_limit=base.scan_limit,
    )


def _intent(*, send_after: datetime | None = None) -> NotificationIntentJob:
    return NotificationIntentJob(
        trigger_event_id=uuid4(),
        event_type=EVENT_TYPE,
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
    plan_action: str = "concretize",
    render_action: str = "append_render",
    error_code: str | None = None,
    planned_action: str = "execute_dry_run_delivery",
) -> NotificationSendContext:
    event_row = OutboxEventRow(
        event_id=intent.trigger_event_id,
        event_type=EVENT_TYPE,
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
    render = None
    if error_code is None and planned_action != "wait_until_send_after":
        render = NotificationRenderDraft(
            notification_plan_id=intent.notification_plan_id,
            message_text="Rendered operator text that must stay out of reports",
            entities_json=[],
            link_preview_options_json={"is_disabled": True},
            reply_markup_json=None,
            disable_notification=False,
            protect_content=False,
            parse_strategy="entities",
            render_hash="render-hash",
        )
    return NotificationSendContext(
        trigger_event_id=intent.trigger_event_id,
        event_row=event_row,
        intent=intent,
        analysis=analysis,
        judge_output=JudgeOutputRenderContext(judge_output_id=analysis.judge_output_id, payload_json={"headline": "x"}),
        candidate=candidate,
        plan_id=intent.notification_plan_id,
        existing_plan_status="suppressed" if plan_action.startswith("reuse") else None,
        plan_action=plan_action,
        render_draft=render,
        render_action=render_action,
        delivery_action="suppress_dry_run_no_transport" if error_code is None else "not_evaluated",
        delivery_status="suppressed" if error_code is None else None,
        planned_action=planned_action,
        error_code=error_code,
    )


def _execution(context: NotificationSendContext, *, write_counts: dict[str, int] | None = None) -> NotificationDryRunExecution:
    record_id = uuid4()
    event_row = OutboxEventRow(
        event_id=uuid4(),
        event_type=DELIVERY_RESULT_EVENT_TYPE,
        aggregate_type="notification_plan",
        aggregate_id=context.plan_id,
        dedupe_key=f"notification-delivery-result:{context.plan_id}:{record_id}",
        payload_json={
            "notification_plan_id": str(context.plan_id),
            "notification_delivery_record_id": str(record_id),
            "delivery_status": "suppressed",
            "telegram_chat_id": None,
            "telegram_message_id": None,
            "attempt_count": 0,
            "transport_error_code": DRY_RUN_REASON_CODE,
            "edited": False,
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
    return NotificationDryRunExecution(
        context=context,
        delivery_result=DeliveryResult(
            delivery_status="suppressed",
            telegram_chat_id=None,
            telegram_message_id=None,
            attempt_count=0,
            transport_error_code=DRY_RUN_REASON_CODE,
            telegram_response_json={"dry_run": True, "transport_skipped": True},
        ),
        notification_delivery_record_id=record_id,
        delivery_result_event_row=event_row,
        notifier_owned_write_counts=write_counts
        or {
            "notification_plans_insert_calls": 1 if context.plan_action == "concretize" else 0,
            "notification_renders_insert_calls": 1 if context.render_action == "append_render" else 0,
            "notification_delivery_records_insert_calls": 1,
            "event_outbox_delivery_result_insert_calls": 1,
        },
    )


def _message(intent: NotificationIntentJob, *, fields: dict[str, str] | None = None) -> StreamMessage:
    base = {
        "job_id": str(intent.trigger_event_id),
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": str(intent.analysis_id),
        "idempotency_key": f"notify:{intent.analysis_id}",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(intent.trigger_event_id),
    }
    if fields:
        base.update(fields)
    return StreamMessage(stream=QUEUE_NAME, message_id="1718000000000-0", fields=base)


def _matched_selection(intent: NotificationIntentJob) -> RedisTargetSelection:
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
        target_is_next=True,
    )


@pytest.mark.asyncio
async def test_preview_exact_target_rehydrates_and_reports_without_side_effects() -> None:
    intent = _intent()
    context = _context(intent, planned_action="preview_only")
    runtime = FakeRuntime(context=context)

    result = await run_bounded_notification_send_dry_run(
        _preview_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["mode"] == "preview"
    assert report["plan_action"] == "concretize"
    assert report["render_action"] == "append_render"
    assert report["delivery_action"] == "suppress_dry_run_no_transport"
    assert report["planned_action"] == "preview_only"
    assert report["q_maintenance_published"] is False
    assert report["redis_acked_count"] == 0
    assert report["side_effects"]["redis_consume_attempted"] is False
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["side_effects"]["maintenance_redis_publish_attempted"] is False
    assert runtime.call_order == ["inspect", "load_context", "close"]


@pytest.mark.asyncio
async def test_execute_happy_path_commits_publishes_maintenance_then_acks() -> None:
    intent = _intent()
    context = _context(intent)
    runtime = FakeRuntime(context=context)

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["delivery_status"] == "suppressed"
    assert report["delivery_result_event_suffix"]
    assert report["q_maintenance_published"] is True
    assert report["q_maintenance_message_suffix"] != "1718000000001-0"
    assert len(report["q_maintenance_message_suffix"]) <= 12
    assert report["q_maintenance_message_suffix"].endswith("-0")
    assert "1718000000001-0" not in json.dumps(report)
    assert report["redis_ack_status"] == "acked"
    assert report["redis_acked_count"] == 1
    assert report["side_effects"]["database_committed"] is True
    assert report["side_effects"]["maintenance_outbox_status_committed"] is True
    assert report["side_effects"]["telegram_transport_called"] is False
    assert runtime.call_order == [
        "inspect",
        "load_context",
        "consume",
        "execute",
        "publish_maintenance",
        "mark_published",
        "ack",
        "close",
    ]


@pytest.mark.asyncio
async def test_idempotent_reconsume_reuses_plan_and_render_without_duplicate_plan_render() -> None:
    intent = _intent()
    context = _context(intent, plan_action="reuse_existing_plan", render_action="reuse_existing_render")
    runtime = FakeRuntime(
        context=context,
        execution=_execution(
            context,
            write_counts={
                "notification_plans_insert_calls": 0,
                "notification_renders_insert_calls": 0,
                "notification_delivery_records_insert_calls": 1,
                "event_outbox_delivery_result_insert_calls": 1,
            },
        ),
    )

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["plan_action"] == "reuse_existing_plan"
    assert report["render_action"] == "reuse_existing_render"
    assert report["notifier_owned_write_counts"]["notification_plans_insert_calls"] == 0
    assert report["notifier_owned_write_counts"]["notification_renders_insert_calls"] == 0
    assert report["notifier_owned_write_counts"]["notification_delivery_records_insert_calls"] == 1
    assert report["side_effects"]["telegram_transport_called"] is False


@pytest.mark.asyncio
async def test_same_material_duplicate_records_suppressed_no_transport_result() -> None:
    intent = _intent()
    context = _context(intent, plan_action="reuse_existing_material_plan", render_action="reuse_existing_render")
    runtime = FakeRuntime(context=context)

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["plan_action"] == "reuse_existing_material_plan"
    assert report["delivery_status"] == "suppressed"
    assert result.execution.delivery_result.transport_error_code == DRY_RUN_REASON_CODE
    assert result.execution.delivery_result.telegram_chat_id is None
    assert result.execution.delivery_result.telegram_message_id is None
    assert report["side_effects"]["telegram_send_called"] is False
    assert report["side_effects"]["telegram_edit_called"] is False


@pytest.mark.asyncio
async def test_future_send_after_blocks_before_consume_write_or_ack() -> None:
    intent = _intent(send_after=datetime.now(timezone.utc) + timedelta(hours=1))
    context = _context(
        intent,
        plan_action="defer_until_due",
        render_action="not_due",
        error_code="notification_send_after_deferred",
        planned_action="wait_until_send_after",
    )
    runtime = FakeRuntime(context=context)

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "notification_send_after_deferred"
    assert report["planned_action"] == "wait_until_send_after"
    assert report["side_effects"]["redis_consume_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["redis_acked_count"] == 0
    assert runtime.call_order == ["inspect", "load_context", "close"]


@pytest.mark.asyncio
async def test_selector_mismatch_fails_closed_before_db_write_or_ack() -> None:
    intent = _intent()
    selection = RedisTargetSelection(status="blocked", error_code="analysis_mismatch")
    runtime = FakeRuntime(context=_context(intent), selection=selection)

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "analysis_mismatch"
    assert report["side_effects"]["database_read_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["redis_acked_count"] == 0
    assert runtime.call_order == ["inspect", "close"]


@pytest.mark.asyncio
async def test_forbidden_redis_payload_fields_fail_closed_before_db_write_or_ack() -> None:
    intent = _intent()
    selection = RedisTargetSelection(
        status="blocked",
        error_code="forbidden_redis_payload_field",
        checks_failed=("forbidden_redis_payload_field",),
    )
    runtime = FakeRuntime(context=_context(intent), selection=selection)

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "forbidden_redis_payload_field"
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["side_effects"]["redis_ack_attempted"] is False


@pytest.mark.asyncio
async def test_full_uuid_suffix_input_is_rejected_without_full_uuid_in_report() -> None:
    intent = _intent()
    full_uuid = str(intent.trigger_event_id)
    config = replace(_base_config(intent), trigger_event_suffix=full_uuid)
    runtime = FakeRuntime(context=_context(intent))

    result = await run_bounded_notification_send_dry_run(
        config,
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()
    encoded = json.dumps(report, sort_keys=True)

    assert report["status"] == "blocked"
    assert report["error_code"] == "trigger_event_suffix_missing_or_invalid"
    assert full_uuid not in encoded
    assert report["trigger_event_suffix"] == full_uuid[-8:]
    assert runtime.call_order == []


@pytest.mark.asyncio
async def test_full_redis_message_suffix_input_is_rejected_without_full_stream_id_in_report() -> None:
    intent = _intent()
    full_stream_id = "1718000000000-0"
    config = replace(_base_config(intent), redis_message_suffix=full_stream_id)
    runtime = FakeRuntime(context=_context(intent))

    result = await run_bounded_notification_send_dry_run(
        config,
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()
    encoded = json.dumps(report, sort_keys=True)

    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_message_suffix_invalid"
    assert full_stream_id not in encoded
    assert report["target_redis_message_suffix"] != full_stream_id
    assert len(report["target_redis_message_suffix"]) <= 12
    assert runtime.call_order == []


@pytest.mark.asyncio
async def test_malformed_notification_intent_payload_blocks_before_consume_write_or_ack() -> None:
    intent = _intent()
    context = _context(intent, error_code="malformed_notification_intent_payload")
    runtime = FakeRuntime(context=context)

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "malformed_notification_intent_payload"
    assert report["side_effects"]["redis_consume_attempted"] is False
    assert report["side_effects"]["database_write_attempted"] is False
    assert report["side_effects"]["redis_ack_attempted"] is False


@pytest.mark.asyncio
async def test_db_commit_failure_does_not_ack_and_sanitizes_failure() -> None:
    intent = _intent()
    private_detail = "private " + "database failure detail"
    runtime = FakeRuntime(context=_context(intent), execute_error=RuntimeError(private_detail))

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()
    encoded = json.dumps(report)

    assert report["status"] == "failed"
    assert report["error_code"] == "database_commit_failed"
    assert private_detail not in encoded
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_q_maintenance_publish_failure_does_not_ack_and_sanitizes_failure() -> None:
    intent = _intent()
    private_detail = "private " + "redis failure detail"
    runtime = FakeRuntime(context=_context(intent), publish_error=RuntimeError(private_detail))

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()
    encoded = json.dumps(report)

    assert report["status"] == "failed"
    assert report["error_code"] == "q_maintenance_publish_failed"
    assert private_detail not in encoded
    assert report["side_effects"]["maintenance_redis_publish_attempted"] is True
    assert report["side_effects"]["redis_ack_attempted"] is False
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_report_redacts_full_ids_payload_chat_text_token_and_urls() -> None:
    intent = _intent()
    context = _context(intent)
    runtime = FakeRuntime(context=context)

    result = await run_bounded_notification_send_dry_run(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    encoded = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert str(intent.trigger_event_id) not in encoded
    assert str(intent.analysis_id) not in encoded
    assert str(intent.notification_plan_id) not in encoded
    assert str(intent.target_chat_id) not in encoded
    assert "Rendered operator text" not in encoded
    assert "notify:" not in encoded
    assert "red" + "is://not-in-report" not in encoded
    assert "postgresql" not in encoded
    assert "telegram_bot_token" not in encoded


def test_source_has_no_forbidden_runtime_or_transport_authority() -> None:
    for path in (SOURCE_PATH, TOOL_PATH):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots = set()
        forbidden_attrs = {"send_message", "edit_message_text", "run_forever", "xclaim", "xautoclaim", "xgroup_create"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_attrs
        assert {"openai", "requests", "httpx", "aiohttp", "subprocess", "docker"}.isdisjoint(imported_roots)
        lowered = source.lower()
        for forbidden in ("systemctl", "runtime.env", "run_forever(", "xclaim", "xautoclaim", "xgroup_create"):
            assert forbidden not in lowered
