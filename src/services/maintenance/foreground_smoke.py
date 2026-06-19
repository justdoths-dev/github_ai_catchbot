from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal, Protocol

from .config import MaintenanceConfig
from .models import StreamMessage
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, MaintenanceServiceProtocol, ReplayQueueWorker


ForegroundSmokeMode = Literal["execute"]

SCHEMA_VERSION = "maintenance_foreground_smoke_report_v1"
MIN_TICKS = 1
MAX_TICKS = 5
MIN_MAX_MESSAGES = 1
MAX_MAX_MESSAGES = 10


@dataclass(frozen=True, slots=True)
class ForegroundSmokeRequest:
    mode: ForegroundSmokeMode
    ticks: int
    max_messages: int
    confirm_run: bool
    maintenance_queue_name: str
    maintenance_consumer_group: str
    replay_queue_name: str
    replay_consumer_group: str


@dataclass(frozen=True, slots=True)
class ForegroundSmokeReport:
    schema_version: str
    mode: str
    status: str
    ticks_requested: int
    ticks_completed: int
    maintenance_processed_count: int
    maintenance_acked_count: int
    replay_processed_count: int
    replay_acked_count: int
    due_retry_action_count: int
    reason_code: str | None = None
    redactions_applied: dict[str, bool] = field(default_factory=dict)


class ForegroundSmokeConsumerProtocol(Protocol):
    async def ensure_group(self, *, allow_create: bool = True) -> bool: ...
    async def read_batch(self) -> list[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...


async def run_foreground_smoke(
    request: ForegroundSmokeRequest,
    *,
    config: MaintenanceConfig,
    maintenance_consumer: ForegroundSmokeConsumerProtocol,
    replay_consumer: ForegroundSmokeConsumerProtocol,
    service: MaintenanceServiceProtocol,
    logger: logging.Logger | None = None,
) -> ForegroundSmokeReport:
    request_error = foreground_smoke_request_error(request)
    if request_error is not None:
        return _report(request, status="blocked", ticks_completed=0, reason_code=request_error)

    try:
        maintenance_group_exists = await maintenance_consumer.ensure_group(allow_create=False)
        replay_group_exists = await replay_consumer.ensure_group(allow_create=False)
    except Exception:
        return _report(request, status="failed", ticks_completed=0, reason_code="consumer_group_check_failed")
    if not maintenance_group_exists:
        return _report(request, status="blocked", ticks_completed=0, reason_code="maintenance_consumer_group_missing")
    if not replay_group_exists:
        return _report(request, status="blocked", ticks_completed=0, reason_code="replay_consumer_group_missing")

    worker_logger = logger or logging.getLogger(__name__)
    bounded_service = _MaxMessagesDueRetryService(service, max_messages=request.max_messages)
    maintenance_worker = MaintenanceQueueWorker(
        config,
        consumer=_MaxMessagesConsumer(maintenance_consumer, max_messages=request.max_messages),
        service=bounded_service,
        logger=worker_logger,
    )
    replay_worker = ReplayQueueWorker(
        config,
        consumer=_MaxMessagesConsumer(replay_consumer, max_messages=request.max_messages),
        service=bounded_service,
        logger=worker_logger,
    )
    due_retry_worker = DueRetryPromotionWorker(config, service=bounded_service, logger=worker_logger)

    ticks_completed = 0
    maintenance_processed = 0
    maintenance_acked = 0
    replay_processed = 0
    replay_acked = 0
    due_retry_actions = 0

    try:
        for _ in range(request.ticks):
            maintenance_result = await maintenance_worker.run_once()
            replay_result = await replay_worker.run_once()
            due_retry_result = await due_retry_worker.run_once()
            ticks_completed += 1

            maintenance_processed += maintenance_result.processed
            maintenance_acked += maintenance_result.acked
            replay_processed += replay_result.processed
            replay_acked += replay_result.acked
            due_retry_actions += due_retry_result.processed

            maintenance_left_unacked = maintenance_result.processed != maintenance_result.acked
            replay_left_unacked = replay_result.processed != replay_result.acked
            if maintenance_left_unacked or replay_left_unacked:
                return _report(
                    request,
                    status="blocked",
                    ticks_completed=ticks_completed,
                    maintenance_processed_count=maintenance_processed,
                    maintenance_acked_count=maintenance_acked,
                    replay_processed_count=replay_processed,
                    replay_acked_count=replay_acked,
                    due_retry_action_count=due_retry_actions,
                    reason_code="worker_run_once_left_messages_unacked",
                )
    except Exception:
        return _report(
            request,
            status="failed",
            ticks_completed=ticks_completed,
            maintenance_processed_count=maintenance_processed,
            maintenance_acked_count=maintenance_acked,
            replay_processed_count=replay_processed,
            replay_acked_count=replay_acked,
            due_retry_action_count=due_retry_actions,
            reason_code="foreground_smoke_run_once_failed",
        )

    return _report(
        request,
        status="pass",
        ticks_completed=ticks_completed,
        maintenance_processed_count=maintenance_processed,
        maintenance_acked_count=maintenance_acked,
        replay_processed_count=replay_processed,
        replay_acked_count=replay_acked,
        due_retry_action_count=due_retry_actions,
        reason_code=None,
    )


def foreground_smoke_request_error(request: ForegroundSmokeRequest) -> str | None:
    if request.mode != "execute":
        return "mode_not_allowed"
    if request.ticks < MIN_TICKS or request.ticks > MAX_TICKS:
        return "ticks_not_allowed"
    if request.max_messages < MIN_MAX_MESSAGES or request.max_messages > MAX_MAX_MESSAGES:
        return "max_messages_not_allowed"
    if not request.confirm_run:
        return "run_confirm_missing"
    if (
        not request.maintenance_queue_name
        or not request.maintenance_consumer_group
        or not request.replay_queue_name
        or not request.replay_consumer_group
    ):
        return "consumer_identity_missing"
    return None


class _MaxMessagesConsumer:
    def __init__(self, consumer: ForegroundSmokeConsumerProtocol, *, max_messages: int) -> None:
        self._consumer = consumer
        self._max_messages = max_messages

    async def ensure_group(self, *, allow_create: bool = True) -> bool:
        return await self._consumer.ensure_group(allow_create=allow_create)

    async def read_batch(self) -> list[StreamMessage]:
        messages = await self._consumer.read_batch()
        return messages[: self._max_messages]

    async def ack(self, message_id: str) -> None:
        await self._consumer.ack(message_id)


class _MaxMessagesDueRetryService:
    def __init__(self, service: MaintenanceServiceProtocol, *, max_messages: int) -> None:
        self._service = service
        self._max_messages = max_messages

    async def handle_maintenance_trigger_event(self, trigger_event_id):
        return await self._service.handle_maintenance_trigger_event(trigger_event_id)

    async def handle_replay_trigger_event(self, trigger_event_id):
        return await self._service.handle_replay_trigger_event(trigger_event_id)

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        return await self._service.promote_due_retries_once(limit=limit or self._max_messages)


def _report(
    request: ForegroundSmokeRequest,
    *,
    status: str,
    ticks_completed: int,
    reason_code: str | None,
    maintenance_processed_count: int = 0,
    maintenance_acked_count: int = 0,
    replay_processed_count: int = 0,
    replay_acked_count: int = 0,
    due_retry_action_count: int = 0,
) -> ForegroundSmokeReport:
    return ForegroundSmokeReport(
        schema_version=SCHEMA_VERSION,
        mode=request.mode,
        status=status,
        ticks_requested=request.ticks,
        ticks_completed=ticks_completed,
        maintenance_processed_count=maintenance_processed_count,
        maintenance_acked_count=maintenance_acked_count,
        replay_processed_count=replay_processed_count,
        replay_acked_count=replay_acked_count,
        due_retry_action_count=due_retry_action_count,
        reason_code=reason_code,
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "runtime_env_values_omitted": True,
            "exception_body_omitted": True,
        },
    )
