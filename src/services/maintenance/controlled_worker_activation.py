from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal, Protocol

from .config import MaintenanceConfig
from .models import StreamMessage
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, MaintenanceServiceProtocol, ReplayQueueWorker


ControlledWorkerMode = Literal["execute"]
ControlledWorkerStatus = Literal["pass", "blocked", "failed"]
ControlledWorkerStopReason = Literal[
    "max_ticks_reached",
    "max_runtime_reached",
    "unacked_detected",
    "failed",
    "no_work_observed",
]

SCHEMA_VERSION = "maintenance_controlled_worker_activation_report_v1"
MIN_MAX_TICKS = 1
MAX_MAX_TICKS = 20
MIN_MAX_RUNTIME_SEC = 1
MAX_MAX_RUNTIME_SEC = 300
MIN_MAX_MESSAGES = 1
MAX_MAX_MESSAGES = 10
MIN_IDLE_SLEEP_MS = 0
MAX_IDLE_SLEEP_MS = 5000


@dataclass(frozen=True, slots=True)
class ControlledWorkerActivationRequest:
    mode: ControlledWorkerMode
    max_ticks: int
    max_runtime_sec: int
    max_messages: int
    idle_sleep_ms: int
    confirm_run: bool
    maintenance_queue_name: str
    maintenance_consumer_group: str
    replay_queue_name: str
    replay_consumer_group: str


@dataclass(frozen=True, slots=True)
class ControlledWorkerActivationReport:
    schema_version: str
    mode: str
    status: ControlledWorkerStatus
    reason_code: str | None
    ticks_requested: int
    ticks_completed: int
    runtime_limit_sec: int
    elapsed_ms: int
    maintenance_processed_count: int
    maintenance_acked_count: int
    replay_processed_count: int
    replay_acked_count: int
    due_retry_action_count: int
    stop_reason: ControlledWorkerStopReason
    redactions_applied: dict[str, bool] = field(default_factory=dict)


class ControlledWorkerConsumerProtocol(Protocol):
    async def ensure_group(self, *, allow_create: bool) -> bool: ...
    async def read_batch(self) -> list[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...


async def run_controlled_worker_activation(
    request: ControlledWorkerActivationRequest,
    *,
    config: MaintenanceConfig,
    maintenance_consumer: ControlledWorkerConsumerProtocol,
    replay_consumer: ControlledWorkerConsumerProtocol,
    service: MaintenanceServiceProtocol,
    logger: logging.Logger | None = None,
    monotonic: Callable[[], float] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> ControlledWorkerActivationReport:
    clock = monotonic or time.monotonic
    sleep_fn = sleep or asyncio.sleep
    started_at = clock()

    request_error = controlled_worker_activation_request_error(request)
    if request_error is not None:
        return _report(
            request,
            status="blocked",
            reason_code=request_error,
            ticks_completed=0,
            elapsed_ms=_elapsed_ms(clock, started_at),
            stop_reason="failed",
        )

    try:
        maintenance_group_exists = await maintenance_consumer.ensure_group(allow_create=False)
        replay_group_exists = await replay_consumer.ensure_group(allow_create=False)
    except Exception:
        return _report(
            request,
            status="failed",
            reason_code="consumer_group_check_failed",
            ticks_completed=0,
            elapsed_ms=_elapsed_ms(clock, started_at),
            stop_reason="failed",
        )
    if not maintenance_group_exists:
        return _report(
            request,
            status="blocked",
            reason_code="maintenance_consumer_group_missing",
            ticks_completed=0,
            elapsed_ms=_elapsed_ms(clock, started_at),
            stop_reason="failed",
        )
    if not replay_group_exists:
        return _report(
            request,
            status="blocked",
            reason_code="replay_consumer_group_missing",
            ticks_completed=0,
            elapsed_ms=_elapsed_ms(clock, started_at),
            stop_reason="failed",
        )

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
    work_observed = False
    stop_reason: ControlledWorkerStopReason | None = None

    try:
        while ticks_completed < request.max_ticks and _elapsed_sec(clock, started_at) < request.max_runtime_sec:
            maintenance_result = await maintenance_worker.run_once()
            replay_result = await replay_worker.run_once()
            due_retry_result = await due_retry_worker.run_once()
            ticks_completed += 1

            maintenance_processed += maintenance_result.processed
            maintenance_acked += maintenance_result.acked
            replay_processed += replay_result.processed
            replay_acked += replay_result.acked
            due_retry_actions += due_retry_result.processed

            tick_work_observed = (
                maintenance_result.processed > 0
                or replay_result.processed > 0
                or due_retry_result.processed > 0
            )
            work_observed = work_observed or tick_work_observed

            maintenance_left_unacked = maintenance_result.processed != maintenance_result.acked
            replay_left_unacked = replay_result.processed != replay_result.acked
            if maintenance_left_unacked or replay_left_unacked:
                return _report(
                    request,
                    status="blocked",
                    reason_code="worker_run_once_left_messages_unacked",
                    ticks_completed=ticks_completed,
                    elapsed_ms=_elapsed_ms(clock, started_at),
                    maintenance_processed_count=maintenance_processed,
                    maintenance_acked_count=maintenance_acked,
                    replay_processed_count=replay_processed,
                    replay_acked_count=replay_acked,
                    due_retry_action_count=due_retry_actions,
                    stop_reason="unacked_detected",
                )

            elapsed = _elapsed_sec(clock, started_at)
            if elapsed >= request.max_runtime_sec:
                stop_reason = "max_runtime_reached"
                break
            if ticks_completed >= request.max_ticks:
                stop_reason = "max_ticks_reached"
                break
            if not tick_work_observed and request.idle_sleep_ms > 0:
                remaining_sec = max(0.0, float(request.max_runtime_sec) - elapsed)
                bounded_sleep_sec = min(request.idle_sleep_ms / 1000.0, remaining_sec)
                if bounded_sleep_sec > 0:
                    await sleep_fn(bounded_sleep_sec)
    except Exception:
        return _report(
            request,
            status="failed",
            reason_code="controlled_worker_run_once_failed",
            ticks_completed=ticks_completed,
            elapsed_ms=_elapsed_ms(clock, started_at),
            maintenance_processed_count=maintenance_processed,
            maintenance_acked_count=maintenance_acked,
            replay_processed_count=replay_processed,
            replay_acked_count=replay_acked,
            due_retry_action_count=due_retry_actions,
            stop_reason="failed",
        )

    if stop_reason is None:
        if _elapsed_sec(clock, started_at) >= request.max_runtime_sec:
            stop_reason = "max_runtime_reached"
        elif ticks_completed >= request.max_ticks:
            stop_reason = "max_ticks_reached"
        else:
            stop_reason = "no_work_observed"
    if not work_observed and stop_reason == "max_ticks_reached":
        stop_reason = "no_work_observed"

    return _report(
        request,
        status="pass",
        reason_code=None,
        ticks_completed=ticks_completed,
        elapsed_ms=_elapsed_ms(clock, started_at),
        maintenance_processed_count=maintenance_processed,
        maintenance_acked_count=maintenance_acked,
        replay_processed_count=replay_processed,
        replay_acked_count=replay_acked,
        due_retry_action_count=due_retry_actions,
        stop_reason=stop_reason,
    )


def controlled_worker_activation_request_error(request: ControlledWorkerActivationRequest) -> str | None:
    if request.mode != "execute":
        return "mode_not_allowed"
    if request.max_ticks < MIN_MAX_TICKS or request.max_ticks > MAX_MAX_TICKS:
        return "max_ticks_not_allowed"
    if request.max_runtime_sec < MIN_MAX_RUNTIME_SEC or request.max_runtime_sec > MAX_MAX_RUNTIME_SEC:
        return "max_runtime_sec_not_allowed"
    if request.max_messages < MIN_MAX_MESSAGES or request.max_messages > MAX_MAX_MESSAGES:
        return "max_messages_not_allowed"
    if request.idle_sleep_ms < MIN_IDLE_SLEEP_MS or request.idle_sleep_ms > MAX_IDLE_SLEEP_MS:
        return "idle_sleep_ms_not_allowed"
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
    def __init__(self, consumer: ControlledWorkerConsumerProtocol, *, max_messages: int) -> None:
        self._consumer = consumer
        self._max_messages = max_messages

    async def ensure_group(self, *, allow_create: bool) -> bool:
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
    request: ControlledWorkerActivationRequest,
    *,
    status: ControlledWorkerStatus,
    reason_code: str | None,
    ticks_completed: int,
    elapsed_ms: int,
    stop_reason: ControlledWorkerStopReason,
    maintenance_processed_count: int = 0,
    maintenance_acked_count: int = 0,
    replay_processed_count: int = 0,
    replay_acked_count: int = 0,
    due_retry_action_count: int = 0,
) -> ControlledWorkerActivationReport:
    return ControlledWorkerActivationReport(
        schema_version=SCHEMA_VERSION,
        mode=request.mode,
        status=status,
        reason_code=reason_code,
        ticks_requested=request.max_ticks,
        ticks_completed=ticks_completed,
        runtime_limit_sec=request.max_runtime_sec,
        elapsed_ms=elapsed_ms,
        maintenance_processed_count=maintenance_processed_count,
        maintenance_acked_count=maintenance_acked_count,
        replay_processed_count=replay_processed_count,
        replay_acked_count=replay_acked_count,
        due_retry_action_count=due_retry_action_count,
        stop_reason=stop_reason,
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "secret_values_omitted": True,
            "raw_source_text_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _elapsed_sec(clock: Callable[[], float], started_at: float) -> float:
    return max(0.0, clock() - started_at)


def _elapsed_ms(clock: Callable[[], float], started_at: float) -> int:
    return int(round(_elapsed_sec(clock, started_at) * 1000))
