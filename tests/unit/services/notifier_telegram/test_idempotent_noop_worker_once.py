from __future__ import annotations

import ast
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from src.services.notifier_telegram.idempotent_noop_worker_once import (
    BoundedNotifierIdempotentNoopRuntimeConfig,
    BoundedNotifierIdempotentNoopWorkerOnceConfig,
    BoundedNotifierIdempotentNoopWorkerOnceState,
    RedisExactNextNotificationConsumer,
    RedisTargetSelection,
    run_bounded_notifier_idempotent_noop_worker_once,
)
from src.services.notifier_telegram.models import (
    DeliveryResult,
    NotificationIntentJob,
    NotifierPlanIdempotencySnapshot,
    StreamMessage,
)
from src.services.notifier_telegram.service import NotifierIdempotencyGuardError
from tests.unit.services.notifier_telegram._service_fakes import config as notifier_config


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/notifier_telegram/idempotent_noop_worker_once.py"
QUEUE_NAME = "q.notification.send"


class FakeRuntime:
    def __init__(
        self,
        *,
        intent: NotificationIntentJob,
        selection: RedisTargetSelection | None = None,
        consume_selection: RedisTargetSelection | None = None,
        post_readback_selection: RedisTargetSelection | None = None,
        readbacks: list[list[NotifierPlanIdempotencySnapshot]] | None = None,
        delivery_result: DeliveryResult | None = None,
        invoke_error: BaseException | None = None,
        ack_count: int = 1,
        ack_error: BaseException | None = None,
        write_counts: dict[str, int] | None = None,
    ) -> None:
        self.intent = intent
        self.selection = selection or _matched_selection(intent)
        self.consume_selection = consume_selection or self.selection
        self.post_readback_selection = post_readback_selection or self.selection
        self.readbacks = list(readbacks or [_sent_snapshot(intent), _sent_snapshot(intent)])
        self.delivery_result = delivery_result or DeliveryResult(
            delivery_status="suppressed",
            telegram_chat_id=intent.target_chat_id,
            telegram_message_id=None,
            attempt_count=0,
            transport_error_code="notification_duplicate_noop",
        )
        self.invoke_error = invoke_error
        self.ack_count = ack_count
        self.ack_error = ack_error
        self.write_counts = write_counts if write_counts is not None else {
            "notification_plans_insert_calls": 0,
            "notification_renders_insert_calls": 0,
            "notification_delivery_records_insert_calls": 0,
            "notification_plans_status_update_calls": 0,
            "state_transitions_insert_calls": 0,
            "event_outbox_delivery_result_insert_calls": 0,
        }
        self.inspect_calls = 0
        self.consume_calls = 0
        self.load_intent_calls: list[UUID] = []
        self.invoke_calls: list[UUID] = []
        self.acked: list[str] = []
        self.state = None
        self.committed = False
        self.rolled_back = False
        self.closed = False

    async def inspect_target(self, config):
        del config
        self.inspect_calls += 1
        return self.selection if self.inspect_calls == 1 else self.post_readback_selection

    async def consume_target(self, expected, config):
        assert expected == self.selection.message
        self.consume_calls += 1
        if config.expect_preclaimed_message:
            self.state.redis_preclaimed_message_read = True
        else:
            self.state.redis_consume_called = True
        return self.consume_selection

    async def load_intent(self, trigger_event_id: UUID):
        self.load_intent_calls.append(trigger_event_id)
        return self.intent

    async def load_readback(self, intent: NotificationIntentJob):
        assert intent == self.intent
        snapshots = self.readbacks.pop(0) if self.readbacks else _sent_snapshot(intent)
        from src.services.notifier_telegram.idempotency import classify_notifier_idempotency_state

        return classify_notifier_idempotency_state(snapshots)

    async def invoke_notifier(self, trigger_event_id: UUID):
        self.invoke_calls.append(trigger_event_id)
        if self.invoke_error is not None:
            raise self.invoke_error
        return self.delivery_result, dict(self.write_counts)

    async def commit_database(self) -> None:
        self.committed = True

    async def rollback_database(self) -> None:
        self.rolled_back = True

    async def ack(self, message_id: str, *, expect_preclaimed_message: bool) -> int:
        assert expect_preclaimed_message == (
            self.selection.preclaimed_message
        )
        self.state.redis_ack_attempted = True
        self.acked.append(message_id)
        if self.ack_error is not None:
            raise self.ack_error
        return self.ack_count

    async def close(self) -> None:
        if not self.committed:
            self.rolled_back = True
        self.closed = True


class FakeRuntimeBuilder:
    def __init__(self, runtime: FakeRuntime) -> None:
        self.runtime = runtime
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True
        self.runtime.state = state
        return self.runtime


class FakePreclaimedRedis:
    def __init__(self, message: StreamMessage, *, times_delivered: int = 1) -> None:
        self.message = message
        self.times_delivered = times_delivered
        self.xreadgroup_calls = 0
        self.eval_calls: list[tuple[object, ...]] = []

    async def type(self, queue_name):
        assert queue_name == QUEUE_NAME
        return "stream"

    async def xinfo_groups(self, queue_name):
        assert queue_name == QUEUE_NAME
        return [{"name": "notifier-telegram", "pending": 1, "lag": 0}]

    async def xpending_range(self, queue_name, group_name, *, min, max, count, consumername):
        assert queue_name == QUEUE_NAME
        assert group_name == "notifier-telegram"
        assert (min, max, count) == ("-", "+", 2)
        assert consumername == "bounded-notifier-idempotent-noop-proof"
        return [
            {
                "message_id": self.message.message_id,
                "consumer": consumername,
                "times_delivered": self.times_delivered,
            }
        ]

    async def xrange(self, queue_name, *, min, max, count):
        assert queue_name == QUEUE_NAME
        assert min == max == self.message.message_id
        assert count == 2
        return [(self.message.message_id, self.message.fields)]

    async def xreadgroup(self, *args, **kwargs):
        del args, kwargs
        self.xreadgroup_calls += 1
        raise AssertionError("preclaimed mode must not perform a second XREADGROUP")

    async def eval(self, script, key_count, *args):
        assert "XPENDING" in script
        assert "XACK" in script
        assert key_count == 1
        self.eval_calls.append(args)
        return 1


def _runtime_config_loader(cfg) -> BoundedNotifierIdempotentNoopRuntimeConfig:
    return BoundedNotifierIdempotentNoopRuntimeConfig(
        notifier_config=notifier_config(dry_run=True, enable_notification_send=False, allow_edits=False),
        redis_url="redis://unit/0",
    )


def _raising_runtime_config_loader(cfg) -> BoundedNotifierIdempotentNoopRuntimeConfig:
    del cfg
    raise AssertionError("runtime config must not be loaded")


def _base_config(
    intent: NotificationIntentJob,
    *,
    mode: str = "execute",
    expect_preclaimed_message: bool = False,
) -> BoundedNotifierIdempotentNoopWorkerOnceConfig:
    return BoundedNotifierIdempotentNoopWorkerOnceConfig(
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        allow_database_write_for_notifier_noop_only=True,
        allow_redis_read=True,
        allow_redis_consume=True,
        allow_redis_ack=True,
        require_telegram_disabled=True,
        mode=mode,
        queue_name=QUEUE_NAME,
        redis_message_id_suffix=(
            _message(intent).message_id[-8:] if expect_preclaimed_message else None
        ),
        trigger_event_suffix=str(intent.trigger_event_id)[-8:],
        analysis_suffix=str(intent.analysis_id)[-8:],
        expect_preclaimed_message=expect_preclaimed_message,
    )


def _intent() -> NotificationIntentJob:
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
        send_after=None,
        suppress_reason_code=None,
    )


def _message(intent: NotificationIntentJob, *, message_id: str = "1718000000000-0") -> StreamMessage:
    return StreamMessage(
        stream=QUEUE_NAME,
        message_id=message_id,
        fields={
            "job_id": f"notify:{intent.trigger_event_id}",
            "stage_name": "notify",
            "root_object_type": "analysis",
            "root_object_id": str(intent.analysis_id),
            "idempotency_key": f"idempotent_noop_reprocess_v1:{intent.analysis_id}",
            "proof_kind": "idempotent_noop_reprocess_v1",
            "pipeline_run_id": "",
            "not_before": "",
            "trigger_event_id": str(intent.trigger_event_id),
        },
    )


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
    )


def _matched_preclaimed_selection(intent: NotificationIntentJob) -> RedisTargetSelection:
    selection = _matched_selection(intent)
    return RedisTargetSelection(
        status=selection.status,
        error_code=selection.error_code,
        message=selection.message,
        redis_message_count=selection.redis_message_count,
        group_lag=0,
        group_pending=1,
        message_stage_name=selection.message_stage_name,
        message_root_object_type=selection.message_root_object_type,
        trigger_event_id_present=selection.trigger_event_id_present,
        analysis_id_present=selection.analysis_id_present,
        preclaimed_message=True,
        preclaimed_owner_matched=True,
        preclaimed_delivery_count=1,
    )


def _sent_snapshot(intent: NotificationIntentJob):
    return [
        NotifierPlanIdempotencySnapshot(
            notification_plan_id=intent.notification_plan_id,
            status="sent",
            render_count=1,
            delivery_record_count=2,
            sent_delivery_count=1,
            suppressed_delivery_count=1,
            terminal_delivery_count=2,
            sent_delivery_chat_id_present_count=1,
            sent_delivery_message_id_present_count=1,
        )
    ]


def _pending_duplicate_snapshot(intent: NotificationIntentJob):
    return [
        NotifierPlanIdempotencySnapshot(notification_plan_id=intent.notification_plan_id, status="planned"),
        NotifierPlanIdempotencySnapshot(notification_plan_id=uuid4(), status="queued"),
    ]


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_db_redis_or_telegram() -> None:
    result = await run_bounded_notifier_idempotent_noop_worker_once(
        BoundedNotifierIdempotentNoopWorkerOnceConfig(),
        runtime_config_loader=_raising_runtime_config_loader,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "operator_approval_missing"
    assert report["runtime_config_loaded"] is False
    assert report["database_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_read_attempted"] is False
    assert report["redis_consume_called"] is False
    assert report["redis_ack_attempted"] is False
    assert report["telegram_send_called"] is False
    assert report["telegram_edit_called"] is False


@pytest.mark.asyncio
async def test_preview_exact_target_reports_ack_safe_candidate_without_service_write_or_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(intent=intent)
    builder = FakeRuntimeBuilder(runtime)

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent, mode="preview"),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["ack_safe_candidate"] is True
    assert report["ack_safe"] is False
    assert report["idempotency_classification_before"] == "existing_plan_sent"
    assert report["pre_notification_plan_count"] == 1
    assert report["pre_notification_render_count"] == 1
    assert report["pre_notification_delivery_record_count"] == 2
    assert report["notifier_called"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_consume_called"] is False
    assert report["acked"] is False
    assert runtime.inspect_calls == 1
    assert runtime.consume_calls == 0
    assert runtime.invoke_calls == []
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_execute_existing_plan_sent_noop_commits_and_acks_exact_message() -> None:
    intent = _intent()
    runtime = FakeRuntime(intent=intent)

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["handled_result_status"] == "suppressed"
    assert report["handled_transport_error_code"] == "notification_duplicate_noop"
    assert report["no_new_plan_created"] is True
    assert report["no_new_render_created"] is True
    assert report["no_new_delivery_record_created"] is True
    assert report["ack_attempted"] is True
    assert report["acked"] is True
    assert report["telegram_send_called"] is False
    assert report["telegram_edit_called"] is False
    assert report["notifier_owned_write_counts"] == {
        "notification_plans_insert_calls": 0,
        "notification_renders_insert_calls": 0,
        "notification_delivery_records_insert_calls": 0,
        "notification_plans_status_update_calls": 0,
        "state_transitions_insert_calls": 0,
        "event_outbox_delivery_result_insert_calls": 0,
    }
    assert runtime.consume_calls == 1
    assert runtime.invoke_calls == [intent.trigger_event_id]
    assert runtime.committed is True
    assert runtime.acked == ["1718000000000-0"]


@pytest.mark.asyncio
async def test_execute_preclaimed_exact_message_reads_pending_without_second_consume_then_acks() -> None:
    intent = _intent()
    selection = _matched_preclaimed_selection(intent)
    runtime = FakeRuntime(intent=intent, selection=selection, consume_selection=selection)

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent, expect_preclaimed_message=True),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["expect_preclaimed_message"] is True
    assert report["redis_group_pending"] == 1
    assert report["redis_group_lag"] == 0
    assert report["redis_consume_called"] is False
    assert report["redis_preclaimed_message_read"] is True
    assert report["notifier_owned_write_counts"] == {
        "notification_plans_insert_calls": 0,
        "notification_renders_insert_calls": 0,
        "notification_delivery_records_insert_calls": 0,
        "notification_plans_status_update_calls": 0,
        "state_transitions_insert_calls": 0,
        "event_outbox_delivery_result_insert_calls": 0,
    }
    assert report["acked"] is True
    assert runtime.consume_calls == 1
    assert runtime.inspect_calls == 2
    assert runtime.acked == ["1718000000000-0"]


@pytest.mark.asyncio
async def test_redis_preclaimed_consumer_rechecks_exact_owned_payload_without_xreadgroup() -> None:
    intent = _intent()
    message = _message(intent)
    client = FakePreclaimedRedis(message)
    state = BoundedNotifierIdempotentNoopWorkerOnceState()
    consumer = RedisExactNextNotificationConsumer(
        client,
        queue_name=QUEUE_NAME,
        consumer_group="notifier-telegram",
        consumer_name="unused-normal-consumer",
        block_ms=1,
        state=state,
    )
    config = _base_config(intent, expect_preclaimed_message=True)

    inspected = await consumer.inspect_target(config)
    consumed = await consumer.consume_target(inspected.message, config)  # type: ignore[arg-type]
    ack_count = await consumer.ack(
        message.message_id,
        expect_preclaimed_message=True,
    )

    assert inspected.status == "matched"
    assert inspected.preclaimed_owner_matched is True
    assert inspected.preclaimed_delivery_count == 1
    assert consumed.message == message
    assert state.redis_preclaimed_message_read is True
    assert state.redis_consume_called is False
    assert client.xreadgroup_calls == 0
    assert ack_count == 1
    assert client.eval_calls == [
        (
            QUEUE_NAME,
            "notifier-telegram",
            "bounded-notifier-idempotent-noop-proof",
            message.message_id,
        )
    ]


@pytest.mark.asyncio
async def test_post_readback_preclaimed_target_loss_blocks_commit_and_ack() -> None:
    intent = _intent()
    selection = _matched_preclaimed_selection(intent)
    runtime = FakeRuntime(
        intent=intent,
        selection=selection,
        consume_selection=selection,
        post_readback_selection=RedisTargetSelection(
            status="blocked",
            error_code="redis_preclaimed_target_not_owned",
            group_pending=0,
            group_lag=0,
        ),
    )

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent, expect_preclaimed_message=True),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.status == "failed"
    assert result.error_code == "redis_preclaimed_target_not_owned"
    assert runtime.committed is False
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_atomic_preclaimed_ack_revalidation_failure_reports_failed_after_readback() -> None:
    intent = _intent()
    selection = _matched_preclaimed_selection(intent)
    runtime = FakeRuntime(
        intent=intent,
        selection=selection,
        consume_selection=selection,
        ack_count=0,
    )

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent, expect_preclaimed_message=True),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.status == "failed"
    assert result.error_code == "redis_ack_failed"
    assert result.idempotency_after is not None
    assert runtime.committed is True
    assert runtime.acked == ["1718000000000-0"]
    assert result.acked is False


@pytest.mark.asyncio
async def test_lost_atomic_ack_response_reports_unknown_without_claiming_zero() -> None:
    intent = _intent()
    selection = _matched_preclaimed_selection(intent)
    runtime = FakeRuntime(
        intent=intent,
        selection=selection,
        consume_selection=selection,
        ack_error=ConnectionError("lost EVAL response"),
    )

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent, expect_preclaimed_message=True),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "failed"
    assert report["error_code"] == "redis_ack_outcome_unknown"
    assert report["error_class"] == "ConnectionError"
    assert report["ack_attempted"] is True
    assert report["redis_ack_attempted"] is True
    assert report["ack_outcome_unknown"] is True
    assert report["acked"] is False
    assert runtime.committed is True
    assert runtime.acked == ["1718000000000-0"]


@pytest.mark.asyncio
async def test_execute_result_that_would_send_or_edit_fails_without_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        intent=intent,
        delivery_result=DeliveryResult(
            delivery_status="sent",
            telegram_chat_id=12345,
            telegram_message_id=9001,
            attempt_count=1,
        ),
    )

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "failed"
    assert report["error_code"] == "result_not_idempotent_noop"
    assert report["ack_attempted"] is False
    assert report["acked"] is False
    assert runtime.acked == []
    assert runtime.committed is False
    assert runtime.rolled_back is True


@pytest.mark.asyncio
async def test_execute_nonzero_notifier_owned_write_fails_without_commit_or_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        intent=intent,
        write_counts={
            "notification_plans_insert_calls": 0,
            "notification_renders_insert_calls": 0,
            "notification_delivery_records_insert_calls": 0,
            "notification_plans_status_update_calls": 0,
            "state_transitions_insert_calls": 1,
            "event_outbox_delivery_result_insert_calls": 0,
        },
    )

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == "notifier_owned_write_detected"
    assert result.acked is False
    assert runtime.committed is False
    assert runtime.rolled_back is True
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_execute_incomplete_notifier_owned_write_shape_fails_without_commit_or_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        intent=intent,
        write_counts={
            "notification_plans_insert_calls": 0,
            "notification_renders_insert_calls": 0,
            "notification_delivery_records_insert_calls": 0,
            "state_transitions_insert_calls": 0,
            "event_outbox_delivery_result_insert_calls": 0,
        },
    )

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == "notifier_owned_write_count_shape_mismatch"
    assert result.acked is False
    assert runtime.committed is False
    assert runtime.rolled_back is True
    assert runtime.acked == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection",
    [
        RedisTargetSelection(status="blocked", error_code="redis_target_ambiguous_or_missing"),
        RedisTargetSelection(
            status="blocked",
            error_code="redis_target_ambiguous_or_missing",
            redis_message_count=2,
            group_lag=2,
            group_pending=0,
        ),
    ],
)
async def test_missing_or_ambiguous_redis_target_blocks_before_db_service_or_ack(
    selection: RedisTargetSelection,
) -> None:
    intent = _intent()
    runtime = FakeRuntime(intent=intent, selection=selection)

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == "redis_target_ambiguous_or_missing"
    assert runtime.load_intent_calls == []
    assert runtime.invoke_calls == []
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_event_or_analysis_mismatch_blocks_before_service_or_ack() -> None:
    intent = _intent()
    selection = RedisTargetSelection(
        status="blocked",
        error_code="analysis_mismatch",
        redis_message_count=1,
        group_lag=1,
        group_pending=0,
        message_stage_name="notify",
        message_root_object_type="analysis",
        trigger_event_id_present=True,
        analysis_id_present=True,
    )
    runtime = FakeRuntime(intent=intent, selection=selection)

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == "analysis_mismatch"
    assert runtime.load_intent_calls == []
    assert runtime.invoke_calls == []
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_duplicate_pending_classification_blocks_before_service_or_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(intent=intent, readbacks=[_pending_duplicate_snapshot(intent)])

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "pre_readback_not_ack_safe"
    assert report["idempotency_classification_before"] == "existing_duplicate_plans"
    assert report["notifier_called"] is False
    assert report["acked"] is False
    assert runtime.invoke_calls == []
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_notifier_idempotency_guard_error_blocks_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        intent=intent,
        invoke_error=NotifierIdempotencyGuardError("duplicate_existing_state"),
    )

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == "duplicate_existing_state"
    assert result.acked is False
    assert runtime.acked == []


@pytest.mark.asyncio
async def test_post_readback_count_change_fails_without_ack() -> None:
    intent = _intent()
    increased = [
        NotifierPlanIdempotencySnapshot(
            notification_plan_id=intent.notification_plan_id,
            status="sent",
            render_count=2,
            delivery_record_count=3,
            sent_delivery_count=1,
            suppressed_delivery_count=2,
            terminal_delivery_count=3,
        )
    ]
    runtime = FakeRuntime(intent=intent, readbacks=[_sent_snapshot(intent), increased])

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == "post_readback_count_changed"
    assert result.acked is False
    assert runtime.acked == []
    assert runtime.committed is False


@pytest.mark.asyncio
async def test_post_readback_count_decrease_fails_without_ack() -> None:
    intent = _intent()
    before = [
        NotifierPlanIdempotencySnapshot(
            notification_plan_id=intent.notification_plan_id,
            status="sent",
            render_count=2,
            delivery_record_count=3,
            sent_delivery_count=1,
            suppressed_delivery_count=2,
            terminal_delivery_count=3,
        )
    ]
    runtime = FakeRuntime(intent=intent, readbacks=[before, _sent_snapshot(intent)])

    result = await run_bounded_notifier_idempotent_noop_worker_once(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == "post_readback_count_changed"
    assert result.acked is False
    assert runtime.acked == []
    assert runtime.committed is False


def test_source_has_no_broad_worker_loop_claim_group_creation_or_forbidden_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"xclaim", "xautoclaim", "xgroup_create", "run_forever"}
    assert {"openai", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(imported_roots)
    lowered = source.lower()
    for forbidden in (
        "xclaim",
        "xautoclaim",
        "xgroup_create",
        "run_forever(",
        "runtime.env",
        "delete ",
        "update notification_plans",
        "send_message(",
        "edit_message_text(",
    ):
        if forbidden in {"send_message(", "edit_message_text("}:
            assert f"async def {forbidden}" in lowered
            continue
        assert forbidden not in lowered
