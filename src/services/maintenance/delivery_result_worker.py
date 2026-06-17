from __future__ import annotations

import logging
from typing import Protocol
from uuid import UUID

from .delivery_retry import DELIVERY_RESULT_EVENT_TYPE, MAINTENANCE_QUEUE_NAME
from .models import (
    DeliveryResultWorkerResult,
    LatestDeliveryRecord,
    NotificationPlanRecord,
    OutboxEvent,
)
from .repositories import delivery_result_from_outbox
from .retry_policy import (
    DELIVERY_RESULT_NOOP_CLASSIFICATION,
    DELIVERY_RESULT_SUPPRESSED_NOOP_ERROR_CODE,
    DELIVERY_RESULT_SENT_SUCCESS_CLASSIFICATION,
    DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
    classify_delivery_result_dry_run_noop,
    classify_delivery_result_send_disabled_noop,
    classify_delivery_result_sent_success,
)


class DeliveryResultRepositoryProtocol(Protocol):
    def transaction(self): ...
    async def load_outbox_event(self, event_id: UUID) -> OutboxEvent | None: ...
    async def load_notification_plan(self, notification_plan_id: UUID) -> NotificationPlanRecord | None: ...
    async def load_latest_delivery_record(self, notification_plan_id: UUID) -> LatestDeliveryRecord | None: ...
    async def count_delivery_result_noop_job_attempts(self, notification_plan_id: UUID) -> int: ...
    async def insert_delivery_result_noop_job_attempt(self, notification_plan_id: UUID) -> bool: ...
    async def count_delivery_result_logical_noop_job_attempts(
        self,
        notification_plan_id: UUID,
        *,
        error_code: str,
    ) -> int: ...
    async def insert_delivery_result_logical_noop_job_attempt(
        self,
        notification_plan_id: UUID,
        *,
        error_code: str,
    ) -> bool: ...
    async def count_delivery_result_sent_success_job_attempts(self, notification_delivery_record_id: UUID) -> int: ...
    async def insert_delivery_result_sent_success_job_attempt(self, notification_delivery_record_id: UUID) -> bool: ...
    async def insert_delivery_terminal_dead_letter(
        self,
        *,
        notification_plan_id: UUID,
        retry_count: int,
        last_error_code: str | None,
    ) -> bool: ...
    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None: ...


class DeliveryResultWorker:
    def __init__(
        self,
        *,
        repository: DeliveryResultRepositoryProtocol,
        logger: logging.Logger | None = None,
    ) -> None:
        self._repository = repository
        self._logger = logger or logging.getLogger(__name__)

    async def handle_trigger_event(self, trigger_event_id: UUID) -> DeliveryResultWorkerResult:
        event = await self._repository.load_outbox_event(trigger_event_id)
        if event is None:
            return _ignored("event_outbox_missing")
        if event.event_type != DELIVERY_RESULT_EVENT_TYPE:
            return DeliveryResultWorkerResult(
                processed=False,
                classification="unsupported",
                action="unsupported",
                reason_code="unsupported_event_type",
            )
        if event.aggregate_type != "notification_plan":
            return DeliveryResultWorkerResult(
                processed=False,
                classification="unsupported",
                action="unsupported",
                reason_code="unsupported_aggregate_type",
            )

        delivery_result = delivery_result_from_outbox(event)
        if delivery_result is None:
            return DeliveryResultWorkerResult(
                processed=False,
                classification="unsupported",
                action="unsupported",
                reason_code="invalid_delivery_result_payload",
            )

        plan = await self._repository.load_notification_plan(delivery_result.notification_plan_id)
        if plan is None:
            return _ignored("notification_plan_missing")

        latest = await self._repository.load_latest_delivery_record(delivery_result.notification_plan_id)
        if latest is None:
            return _ignored("notification_delivery_record_missing")

        delivery_status = latest.delivery_status
        delivery_reason = latest.transport_error_code

        sent_success = classify_delivery_result_sent_success(delivery_status=delivery_status)
        if sent_success.action == "mark_terminal_success":
            return await self._mark_sent_success(latest)

        dry_run_noop = classify_delivery_result_dry_run_noop(
            delivery_status=delivery_status,
            delivery_reason=delivery_reason,
        )
        if dry_run_noop.action == "mark_logical_noop_success":
            return await self._mark_logical_noop(plan, error_code=dry_run_noop.reason_code)

        send_disabled_noop = classify_delivery_result_send_disabled_noop(
            delivery_status=delivery_status,
            delivery_reason=delivery_reason,
        )
        if send_disabled_noop.action == "mark_logical_noop_success":
            return await self._mark_logical_noop(plan, error_code=send_disabled_noop.reason_code)

        if delivery_status == "suppressed":
            return await self._mark_logical_noop(plan, error_code=DELIVERY_RESULT_SUPPRESSED_NOOP_ERROR_CODE)

        if delivery_status == "failed_retryable":
            return await self._record_retryable_candidate(plan)
        if delivery_status == "failed_terminal":
            return await self._record_terminal_failure(plan, latest)

        return DeliveryResultWorkerResult(
            processed=True,
            classification="unsupported",
            action="unsupported",
            reason_code="delivery_result_status_not_supported",
        )

    async def _mark_sent_success(self, latest: LatestDeliveryRecord) -> DeliveryResultWorkerResult:
        existing = await self._repository.count_delivery_result_sent_success_job_attempts(
            latest.notification_delivery_record_id
        )
        if existing:
            return DeliveryResultWorkerResult(
                processed=True,
                classification=DELIVERY_RESULT_SENT_SUCCESS_CLASSIFICATION,
                action="already_marked",
                reason_code=DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
                already_marked=True,
            )

        async with self._repository.transaction():
            inserted = await self._repository.insert_delivery_result_sent_success_job_attempt(
                latest.notification_delivery_record_id
            )
        return DeliveryResultWorkerResult(
            processed=True,
            classification=DELIVERY_RESULT_SENT_SUCCESS_CLASSIFICATION,
            action="mark_terminal_success" if inserted else "already_marked",
            reason_code=DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
            marker_written=inserted,
            already_marked=not inserted,
        )

    async def _mark_logical_noop(
        self,
        plan: NotificationPlanRecord,
        *,
        error_code: str,
    ) -> DeliveryResultWorkerResult:
        existing = await self._repository.count_delivery_result_logical_noop_job_attempts(
            plan.notification_plan_id,
            error_code=error_code,
        )
        if existing:
            return DeliveryResultWorkerResult(
                processed=True,
                classification=DELIVERY_RESULT_NOOP_CLASSIFICATION,
                action="already_marked",
                reason_code=error_code,
                already_marked=True,
            )

        async with self._repository.transaction():
            inserted = await self._repository.insert_delivery_result_logical_noop_job_attempt(
                plan.notification_plan_id,
                error_code=error_code,
            )
        return DeliveryResultWorkerResult(
            processed=True,
            classification=DELIVERY_RESULT_NOOP_CLASSIFICATION,
            action="mark_logical_noop_success" if inserted else "already_marked",
            reason_code=error_code,
            marker_written=inserted,
            already_marked=not inserted,
        )

    async def _record_retryable_candidate(self, plan: NotificationPlanRecord) -> DeliveryResultWorkerResult:
        async with self._repository.transaction():
            await self._repository.insert_job_attempt(
                stage_name="maintenance_delivery_result",
                queue_name=MAINTENANCE_QUEUE_NAME,
                root_object_type="notification_plan",
                root_object_id=plan.notification_plan_id,
                attempt_status="failed_retryable",
                error_code="delivery_result_failed_retryable_due_scan_candidate",
            )
        return DeliveryResultWorkerResult(
            processed=True,
            classification="retryable_candidate",
            action="record_retryable_interpretation",
            reason_code="failed_retryable_deferred_to_due_scan",
        )

    async def _record_terminal_failure(
        self,
        plan: NotificationPlanRecord,
        latest: LatestDeliveryRecord,
    ) -> DeliveryResultWorkerResult:
        async with self._repository.transaction():
            await self._repository.insert_job_attempt(
                stage_name="maintenance_delivery_result",
                queue_name=MAINTENANCE_QUEUE_NAME,
                root_object_type="notification_plan",
                root_object_id=plan.notification_plan_id,
                attempt_status="failed_terminal",
                error_code="delivery_result_failed_terminal_dlq_candidate",
            )
            dead_letter_written = await self._repository.insert_delivery_terminal_dead_letter(
                notification_plan_id=plan.notification_plan_id,
                retry_count=latest.attempt_count,
                last_error_code=latest.transport_error_code,
            )
        return DeliveryResultWorkerResult(
            processed=True,
            classification="terminal_failure",
            action="record_terminal_failure",
            reason_code="failed_terminal_dlq_candidate",
            dead_letter_written=dead_letter_written,
        )


def _ignored(reason_code: str) -> DeliveryResultWorkerResult:
    return DeliveryResultWorkerResult(
        processed=False,
        classification="ignored",
        action="ignored",
        reason_code=reason_code,
    )
