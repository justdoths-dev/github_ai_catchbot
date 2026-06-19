from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal, Protocol

from .config import MaintenanceConfig
from .models import StreamMessage
from .worker import MaintenanceQueueWorker, MaintenanceServiceProtocol, ReplayQueueWorker


WorkerOnceType = Literal["maintenance", "replay"]
WorkerOnceMode = Literal["execute"]

SCHEMA_VERSION = "maintenance_worker_once_report_v1"


@dataclass(frozen=True, slots=True)
class WorkerOnceRequest:
    worker_type: WorkerOnceType
    queue_name: str
    consumer_group: str
    mode: WorkerOnceMode
    max_messages: int
    confirm_ack: bool


@dataclass(frozen=True, slots=True)
class WorkerOnceReport:
    schema_version: str
    worker_type: WorkerOnceType
    queue_name: str
    consumer_group: str
    mode: WorkerOnceMode
    status: str
    processed_count: int
    acked_count: int
    reason_code: str | None = None
    redactions_applied: dict[str, bool] = field(default_factory=dict)


class WorkerOnceConsumerProtocol(Protocol):
    async def ensure_group(self, *, allow_create: bool = True) -> bool: ...
    async def read_batch(self) -> list[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...


async def run_worker_once(
    request: WorkerOnceRequest,
    *,
    config: MaintenanceConfig,
    consumer: WorkerOnceConsumerProtocol,
    service: MaintenanceServiceProtocol,
    logger: logging.Logger | None = None,
) -> WorkerOnceReport:
    request_error = worker_once_request_error(request)
    if request_error is not None:
        return _report(request, status="blocked", processed_count=0, acked_count=0, reason_code=request_error)

    try:
        group_exists = await consumer.ensure_group(allow_create=False)
    except Exception:
        return _report(
            request,
            status="failed",
            processed_count=0,
            acked_count=0,
            reason_code="consumer_group_check_failed",
        )
    if not group_exists:
        return _report(
            request,
            status="blocked",
            processed_count=0,
            acked_count=0,
            reason_code="consumer_group_missing",
        )

    bounded_consumer = _MaxMessagesConsumer(consumer, max_messages=request.max_messages)
    worker_logger = logger or logging.getLogger(__name__)
    if request.worker_type == "maintenance":
        worker = MaintenanceQueueWorker(config, consumer=bounded_consumer, service=service, logger=worker_logger)
    else:
        worker = ReplayQueueWorker(config, consumer=bounded_consumer, service=service, logger=worker_logger)

    try:
        result = await worker.run_once()
    except Exception:
        return _report(
            request,
            status="failed",
            processed_count=0,
            acked_count=0,
            reason_code="worker_run_once_failed",
        )

    status = "pass" if result.processed == result.acked else "blocked"
    reason_code = None if status == "pass" else "worker_run_once_left_messages_unacked"
    return _report(
        request,
        status=status,
        processed_count=result.processed,
        acked_count=result.acked,
        reason_code=reason_code,
    )


def worker_once_request_error(request: WorkerOnceRequest) -> str | None:
    if request.worker_type not in {"maintenance", "replay"}:
        return "worker_type_not_allowed"
    if request.mode != "execute":
        return "mode_not_allowed"
    if request.max_messages < 1 or request.max_messages > 10:
        return "max_messages_not_allowed"
    if not request.confirm_ack:
        return "ack_confirm_missing"
    if not request.queue_name or not request.consumer_group:
        return "consumer_identity_missing"
    return None


class _MaxMessagesConsumer:
    def __init__(self, consumer: WorkerOnceConsumerProtocol, *, max_messages: int) -> None:
        self._consumer = consumer
        self._max_messages = max_messages

    async def ensure_group(self, *, allow_create: bool = True) -> bool:
        return await self._consumer.ensure_group(allow_create=allow_create)

    async def read_batch(self) -> list[StreamMessage]:
        messages = await self._consumer.read_batch()
        return messages[: self._max_messages]

    async def ack(self, message_id: str) -> None:
        await self._consumer.ack(message_id)


def _report(
    request: WorkerOnceRequest,
    *,
    status: str,
    processed_count: int,
    acked_count: int,
    reason_code: str | None,
) -> WorkerOnceReport:
    return WorkerOnceReport(
        schema_version=SCHEMA_VERSION,
        worker_type=request.worker_type,
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        mode=request.mode,
        status=status,
        processed_count=processed_count,
        acked_count=acked_count,
        reason_code=reason_code,
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "exception_body_omitted": True,
        },
    )
