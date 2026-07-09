from __future__ import annotations

import json
from uuid import UUID

import pytest

from services.maintenance.bounded_runtime import (
    MAINTENANCE_RESULT_COMMAND,
    BoundedMaintenanceQueueOnceConfig,
    BoundedMaintenanceResult,
    BoundedMaintenanceRuntimeState,
    RedisTargetSelection,
)
from services.maintenance.models import DeliveryResultWorkerResult, StreamMessage
from services.maintenance.restricted_delivery_result_maintenance_drain_proof_runner import (
    DeliveryResultMaintenanceDrainReadback,
    RestrictedDeliveryResultMaintenanceDrainProofConfig,
    _redis_group_lag_pending,
    build_parser,
    run_restricted_delivery_result_maintenance_drain_proof,
)
from src.services.outbox_relay.bounded_delivery_result_outbox_publish import (
    BoundedDeliveryResultOutboxPublishConfig,
    BoundedDeliveryResultOutboxPublishResult,
    BoundedDeliveryResultOutboxPublishState,
)


TARGET_EVENT_ID = UUID("11111111-1111-4111-8111-111111111111")
PLAN_SUFFIX = "22222222"
DELIVERY_RECORD_SUFFIX = "33333333"
REDIS_MESSAGE_SUFFIX = "0000-42"
REDIS_MESSAGE_ID = "1740000000000-42"
RAW_SECRET = "sentinel_private_runtime_detail"


class FakePublisherRunner:
    def __init__(self, result: BoundedDeliveryResultOutboxPublishResult) -> None:
        self.result = result
        self.calls: list[BoundedDeliveryResultOutboxPublishConfig] = []

    async def __call__(self, config, **kwargs):
        del kwargs
        self.calls.append(config)
        return self.result


class FakeMaintenanceRunner:
    def __init__(
        self,
        *,
        preview_result: BoundedMaintenanceResult,
        execute_result: BoundedMaintenanceResult,
    ) -> None:
        self.preview_result = preview_result
        self.execute_result = execute_result
        self.calls = []

    async def __call__(self, config, **kwargs):
        del kwargs
        self.calls.append(config)
        return self.preview_result if config.mode == "preview" else self.execute_result


class FakeReadbackLoader:
    def __init__(self, readback: DeliveryResultMaintenanceDrainReadback) -> None:
        self.readback = readback
        self.calls: list[UUID] = []

    async def __call__(self, target_event_id: UUID):
        self.calls.append(target_event_id)
        return self.readback


def _config(**overrides) -> RestrictedDeliveryResultMaintenanceDrainProofConfig:
    values = {
        "operator_confirmed": True,
        "target_event_id": TARGET_EVENT_ID,
        "allow_database_read": True,
        "allow_redis_write": True,
        "allow_outbox_status_update": True,
        "allow_redis_consume": True,
        "allow_redis_ack": True,
        "max_lag": 1,
    }
    values.update(overrides)
    return RestrictedDeliveryResultMaintenanceDrainProofConfig(**values)


def _publisher_result(
    *,
    ok: bool = True,
    status: str = "pass",
    error_code: str | None = None,
    event_suffix: str = "11111111",
    plan_suffix: str = PLAN_SUFFIX,
    delivery_record_suffix: str = DELIVERY_RECORD_SUFFIX,
    redis_message_suffix: str | None = REDIS_MESSAGE_SUFFIX,
) -> BoundedDeliveryResultOutboxPublishResult:
    return BoundedDeliveryResultOutboxPublishResult(
        status=status,
        ok=ok,
        error_code=error_code,
        error_class=None,
        config=BoundedDeliveryResultOutboxPublishConfig(
            operator_approved=True,
            target_event_id=TARGET_EVENT_ID,
            allow_database_read=True,
            allow_redis_write=True,
            allow_outbox_status_update=True,
        ),
        state=BoundedDeliveryResultOutboxPublishState(
            database_read_attempted=True,
            redis_xadd_attempted=ok,
            event_outbox_status_update_attempted=ok,
            job_attempt_insert_attempted=ok,
        ),
        target_event_id_requested=True,
        selected_event_present=ok,
        selected_event_status="pending" if ok else None,
        selected_event_type="notification.delivery.result.v1" if ok else None,
        selected_event_id_suffix=event_suffix,
        selected_aggregate_type="notification_plan" if ok else None,
        selected_aggregate_id_suffix=plan_suffix,
        payload_has_notification_plan_id=ok,
        payload_has_notification_delivery_record_id=ok,
        payload_has_delivery_status=ok,
        payload_has_attempt_count=ok,
        payload_notification_plan_id_matches_aggregate=ok,
        payload_notification_plan_id_suffix=plan_suffix,
        payload_notification_delivery_record_id_suffix=delivery_record_suffix,
        payload_delivery_status="sent" if ok else None,
        payload_attempt_count_present=ok,
        queue_name="q.maintenance" if ok else None,
        stage_name="maintenance" if ok else None,
        redis_xadd_count=1 if ok else 0,
        redis_message_id_suffix=redis_message_suffix,
        event_outbox_marked_published=ok,
        job_attempt_inserted=ok,
        thin_stream_fields_valid=ok,
    )


def _maintenance_result(
    *,
    mode: str,
    ok: bool = True,
    lag: int | None = 1,
    pending: int | None = 0,
    acked: bool = False,
    handler_called: bool = False,
    error_code: str | None = None,
    selected_message: StreamMessage | None = None,
    queue_config: BoundedMaintenanceQueueOnceConfig | None = None,
) -> BoundedMaintenanceResult:
    state = BoundedMaintenanceRuntimeState(
        runtime_config_loaded=True,
        redis_read_attempted=True,
        redis_consume_called=mode == "execute",
        redis_ack_attempted=acked,
        service_called=handler_called,
        database_read_attempted=mode == "execute",
        database_write_attempted=handler_called,
        database_committed=mode == "execute" and ok,
    )
    selection = RedisTargetSelection(
        status="matched" if ok else "blocked",
        error_code=error_code,
        message=selected_message,
        group_lag=lag,
        group_pending=pending,
        redis_message_count=1,
        message_stage_name="maintenance",
        message_root_object_type="notification_plan",
        trigger_event_id_present=True,
        root_object_id_present=True,
        redis_message_id_suffix=REDIS_MESSAGE_SUFFIX,
        trigger_event_id_suffix="11111111",
        root_object_id_suffix=PLAN_SUFFIX,
    )
    service_result = (
        DeliveryResultWorkerResult(
            processed=True,
            classification="terminal_success",
            action="mark_terminal_success",
            reason_code="delivery_result_terminal_success",
            marker_written=True,
        )
        if handler_called and ok
        else None
    )
    return BoundedMaintenanceResult(
        command=MAINTENANCE_RESULT_COMMAND,
        mode=mode,
        status="pass" if ok else "blocked",
        ok=ok,
        error_code=error_code,
        queue_config=queue_config,
        state=state,
        queue_name="q.maintenance",
        consumer_group="maintenance",
        redis_selection=selection,
        service_result=service_result,
        ack_attempted=acked,
        acked=acked,
    )


def _readback(**overrides) -> DeliveryResultMaintenanceDrainReadback:
    values = {
        "outbox_status": "published",
        "outbox_published_at_present": True,
        "target_event_id_suffix": "11111111",
        "target_plan_id_suffix": PLAN_SUFFIX,
        "maintenance_receipt_present": True,
        "maintenance_receipt_code": "delivery_result_terminal_success_handled",
        "redis_lag": 0,
        "redis_pending": 0,
    }
    values.update(overrides)
    return DeliveryResultMaintenanceDrainReadback(**values)


def _stream_message() -> StreamMessage:
    return StreamMessage(
        stream="q.maintenance",
        message_id=REDIS_MESSAGE_ID,
        fields={
            "stage_name": "maintenance",
            "root_object_type": "notification_plan",
            "root_object_id": f"00000000-0000-4000-8000-0000{PLAN_SUFFIX}",
            "trigger_event_id": f"00000000-0000-4000-8000-000011111111",
        },
    )


def test_redis_group_readback_preserves_zero_lag_and_pending_with_string_keys() -> None:
    redis_lag, redis_pending = _redis_group_lag_pending(
        [{"name": "maintenance", "lag": 0, "pending": 0}],
        "maintenance",
    )

    assert redis_lag == 0
    assert redis_pending == 0


def test_redis_group_readback_preserves_zero_lag_and_pending_with_bytes_keys() -> None:
    redis_lag, redis_pending = _redis_group_lag_pending(
        [{b"name": b"maintenance", b"lag": 0, b"pending": 0}],
        "maintenance",
    )

    assert redis_lag == 0
    assert redis_pending == 0


@pytest.mark.asyncio
async def test_no_flags_block_before_publish_or_worker() -> None:
    publisher = FakePublisherRunner(_publisher_result())
    maintenance = FakeMaintenanceRunner(
        preview_result=_maintenance_result(mode="preview"),
        execute_result=_maintenance_result(mode="execute", acked=True, handler_called=True),
    )
    readback = FakeReadbackLoader(_readback())

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        RestrictedDeliveryResultMaintenanceDrainProofConfig(
            operator_confirmed=False,
            target_event_id=TARGET_EVENT_ID,
            allow_database_read=False,
            allow_redis_write=False,
            allow_outbox_status_update=False,
            allow_redis_consume=False,
            allow_redis_ack=False,
        ),
        publisher_runner=publisher,
        maintenance_runner=maintenance,
        readback_loader=readback,
    )

    assert report["status"] == "blocked"
    assert report["reason_code"] == "operator_confirmation_required"
    assert publisher.calls == []
    assert maintenance.calls == []
    assert readback.calls == []


@pytest.mark.asyncio
async def test_pass_path_publishes_previews_executes_one_message_and_reads_back_drain() -> None:
    publisher = FakePublisherRunner(_publisher_result())
    maintenance = FakeMaintenanceRunner(
        preview_result=_maintenance_result(mode="preview", lag=1, pending=0),
        execute_result=_maintenance_result(mode="execute", lag=1, pending=0, acked=True, handler_called=True),
    )
    readback = FakeReadbackLoader(_readback())

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(),
        publisher_runner=publisher,
        maintenance_runner=maintenance,
        readback_loader=readback,
    )

    assert report["status"] == "pass"
    assert report["reason_code"] == "delivery_result_maintenance_drain_proof_closed"
    assert report["publisher"]["redis_xadd_count"] == 1
    assert report["publisher"]["event_outbox_marked_published"] is True
    assert report["publisher"]["job_attempt_inserted"] is True
    assert report["redis_precheck"]["lag"] == 1
    assert report["redis_precheck"]["pending"] == 0
    assert report["worker_once"]["handler_called"] is True
    assert report["worker_once"]["acked"] is True
    assert report["readback"]["outbox_status"] == "published"
    assert report["readback"]["redis_lag"] == 0
    assert report["readback"]["redis_pending"] == 0
    assert report["readback"]["maintenance_receipt_present"] is True
    assert publisher.calls[0].target_event_id == TARGET_EVENT_ID
    assert publisher.calls[0].allow_redis_write is True
    assert [call.mode for call in maintenance.calls] == ["preview", "execute"]
    assert maintenance.calls[0].allow_redis_consume is False
    assert maintenance.calls[0].allow_redis_ack is False
    assert maintenance.calls[0].allow_pending_target_resume is True
    assert maintenance.calls[0].allow_database_read is False
    assert maintenance.calls[1].allow_redis_consume is True
    assert maintenance.calls[1].allow_redis_ack is True
    assert maintenance.calls[1].allow_pending_target_resume is True
    assert maintenance.calls[1].allow_database_read is True
    assert maintenance.calls[1].allow_database_write is True
    assert maintenance.calls[1].trigger_event_suffix == "11111111"
    assert maintenance.calls[1].root_object_id_suffix == PLAN_SUFFIX
    assert maintenance.calls[1].redis_message_id_suffix == REDIS_MESSAGE_SUFFIX
    assert readback.calls == [TARGET_EVENT_ID]


@pytest.mark.asyncio
async def test_pass_path_resumes_exact_pending_target_under_same_consumer() -> None:
    class PendingResumeMaintenanceRunner:
        def __init__(self) -> None:
            self.calls: list[BoundedMaintenanceQueueOnceConfig] = []

        async def __call__(self, config, **kwargs):
            del kwargs
            self.calls.append(config)
            return _maintenance_result(
                mode=config.mode,
                lag=1,
                pending=1,
                acked=config.mode == "execute",
                handler_called=config.mode == "execute",
                selected_message=_stream_message(),
                queue_config=config,
            )

    maintenance = PendingResumeMaintenanceRunner()
    readback = FakeReadbackLoader(_readback(redis_lag=0, redis_pending=0, maintenance_receipt_present=True))

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(),
        publisher_runner=FakePublisherRunner(_publisher_result()),
        maintenance_runner=maintenance,
        readback_loader=readback,
    )

    assert report["status"] == "pass"
    assert report["reason_code"] == "delivery_result_maintenance_drain_proof_closed"
    assert report["publisher"]["redis_xadd_count"] == 1
    assert report["redis_precheck"]["pending"] == 1
    assert report["worker_once"]["acked"] is True
    assert report["worker_once"]["processed"] is True
    assert report["worker_once"]["reason_code"] == "delivery_result_terminal_success"
    assert report["readback"]["redis_pending"] == 0
    assert report["readback"]["redis_lag"] == 0
    assert report["readback"]["maintenance_receipt_present"] is True
    assert report["authority"]["q_notification_send_consumed"] is False
    assert report["authority"]["telegram_transport_attempted"] is False
    assert report["authority"]["raw_payload_printed"] is False
    assert report["authority"]["raw_ids_printed"] is False
    assert report["authority"]["pending_target_resume_allowed"] is True
    assert [call.mode for call in maintenance.calls] == ["preview", "execute"]
    assert all(call.allow_pending_target_resume is True for call in maintenance.calls)
    assert maintenance.calls[0].allow_redis_consume is False
    assert maintenance.calls[1].allow_redis_consume is True
    assert maintenance.calls[1].allow_redis_ack is True
    assert readback.calls == [TARGET_EVENT_ID]


@pytest.mark.asyncio
async def test_publisher_failure_stops_before_maintenance_worker() -> None:
    publisher = FakePublisherRunner(
        _publisher_result(ok=False, status="blocked", error_code="target_event_not_pending")
    )
    maintenance = FakeMaintenanceRunner(
        preview_result=_maintenance_result(mode="preview"),
        execute_result=_maintenance_result(mode="execute", acked=True, handler_called=True),
    )

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(),
        publisher_runner=publisher,
        maintenance_runner=maintenance,
        readback_loader=FakeReadbackLoader(_readback()),
    )

    assert report["status"] == "blocked"
    assert report["reason_code"] == "target_event_not_pending"
    assert maintenance.calls == []


@pytest.mark.asyncio
async def test_precheck_lag_guard_fails_closed_before_consume() -> None:
    maintenance = FakeMaintenanceRunner(
        preview_result=_maintenance_result(mode="preview", lag=2, pending=0),
        execute_result=_maintenance_result(mode="execute", acked=True, handler_called=True),
    )

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(max_lag=1),
        publisher_runner=FakePublisherRunner(_publisher_result()),
        maintenance_runner=maintenance,
        readback_loader=FakeReadbackLoader(_readback()),
    )

    assert report["status"] == "blocked"
    assert report["reason_code"] == "redis_lag_exceeds_max"
    assert [call.mode for call in maintenance.calls] == ["preview"]


@pytest.mark.asyncio
async def test_precheck_pending_guard_fails_closed_before_consume_or_readback() -> None:
    maintenance = FakeMaintenanceRunner(
        preview_result=_maintenance_result(mode="preview", lag=1, pending=1),
        execute_result=_maintenance_result(mode="execute", acked=True, handler_called=True),
    )
    readback = FakeReadbackLoader(_readback())

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(),
        publisher_runner=FakePublisherRunner(_publisher_result()),
        maintenance_runner=maintenance,
        readback_loader=readback,
    )

    assert report["status"] == "blocked"
    assert report["reason_code"] == "redis_pending_not_zero"
    assert report["redis_precheck"]["pending"] == 1
    assert [call.mode for call in maintenance.calls] == ["preview"]
    assert readback.calls == []


@pytest.mark.asyncio
async def test_handler_failure_does_not_report_ack_or_readback_success() -> None:
    maintenance = FakeMaintenanceRunner(
        preview_result=_maintenance_result(mode="preview", lag=1, pending=0),
        execute_result=_maintenance_result(
            mode="execute",
            ok=False,
            lag=1,
            pending=0,
            acked=False,
            handler_called=False,
            error_code="service_execution_failed",
        ),
    )
    readback = FakeReadbackLoader(_readback())

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(),
        publisher_runner=FakePublisherRunner(_publisher_result()),
        maintenance_runner=maintenance,
        readback_loader=readback,
    )

    assert report["status"] == "blocked"
    assert report["reason_code"] == "service_execution_failed"
    assert report["worker_once"]["acked"] is False
    assert readback.calls == []


@pytest.mark.asyncio
async def test_readback_requires_outbox_published_redis_drained_and_maintenance_receipt() -> None:
    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(),
        publisher_runner=FakePublisherRunner(_publisher_result()),
        maintenance_runner=FakeMaintenanceRunner(
            preview_result=_maintenance_result(mode="preview", lag=1, pending=0),
            execute_result=_maintenance_result(mode="execute", acked=True, handler_called=True),
        ),
        readback_loader=FakeReadbackLoader(_readback(maintenance_receipt_present=False)),
    )

    assert report["status"] == "failed"
    assert report["reason_code"] == "maintenance_receipt_missing"
    assert report["readback"]["outbox_status"] == "published"
    assert report["readback"]["redis_pending"] == 0
    assert report["readback"]["redis_lag"] == 0
    assert report["readback"]["maintenance_receipt_present"] is False


@pytest.mark.asyncio
async def test_sanitized_output_omits_raw_ids_payloads_urls_and_secret_details() -> None:
    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _config(),
        publisher_runner=FakePublisherRunner(_publisher_result()),
        maintenance_runner=FakeMaintenanceRunner(
            preview_result=_maintenance_result(mode="preview", lag=1, pending=0),
            execute_result=_maintenance_result(mode="execute", acked=True, handler_called=True),
        ),
        readback_loader=FakeReadbackLoader(_readback()),
    )
    rendered = json.dumps(report, sort_keys=True)

    for raw in (
        str(TARGET_EVENT_ID),
        RAW_SECRET,
        "postgresql://",
        "redis://",
        "openai_api_key",
        "github_token",
    ):
        assert raw not in rendered
    assert '"payload_json":' not in rendered
    assert '"telegram_response_json":' not in rendered
    assert "11111111" in rendered
    assert PLAN_SUFFIX in rendered
    assert DELIVERY_RECORD_SUFFIX in rendered
    assert report["authority"]["q_notification_send_consumed"] is False
    assert report["authority"]["telegram_transport_attempted"] is False


def test_parser_accepts_suggested_operator_flags() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--operator-confirmed",
            "--target-event-id",
            str(TARGET_EVENT_ID),
            "--allow-database-read",
            "--allow-redis-write",
            "--allow-outbox-status-update",
            "--allow-redis-consume",
            "--allow-redis-ack",
            "--max-lag",
            "1",
            "--format",
            "json",
        ]
    )

    assert args.operator_confirmed is True
    assert args.target_event_id == str(TARGET_EVENT_ID)
    assert args.allow_database_read is True
    assert args.allow_redis_write is True
    assert args.allow_outbox_status_update is True
    assert args.allow_redis_consume is True
    assert args.allow_redis_ack is True
    assert args.max_lag == 1
    assert args.format == "json"
