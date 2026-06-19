from __future__ import annotations

import logging
from typing import Protocol
from uuid import UUID

from .delivery_retry import DELIVERY_RESULT_EVENT_TYPE
from .delivery_result_policy import DeliveryResultDecision, decide_delivery_result
from .models import (
    DeliveryResultEvent,
    DeliveryResultWorkerResult,
    LatestDeliveryRecord,
    NotificationPlanRecord,
    OutboxEvent,
)
from .repositories import delivery_result_from_outbox


class DeliveryResultRepositoryProtocol(Protocol):
    def transaction(self): ...
    async def load_outbox_event(self, event_id: UUID) -> OutboxEvent | None: ...
    async def load_notification_plan(self, notification_plan_id: UUID) -> NotificationPlanRecord | None: ...
    async def load_latest_delivery_record(self, notification_plan_id: UUID) -> LatestDeliveryRecord | None: ...
    async def load_delivery_record(self, notification_delivery_record_id: UUID) -> LatestDeliveryRecord | None: ...
    async def later_success_exists(self, *, notification_plan_id: UUID, exact_created_at) -> bool: ...
    async def has_delivery_result_receipt(self, event_id: UUID, *, receipt_code: str | None = None) -> bool: ...
    async def insert_delivery_result_receipt(self, *, event_id: UUID, receipt_code: str) -> bool: ...
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
                classification="identity_invalid",
                action="fail_closed",
                reason_code="invalid_delivery_result_payload",
            )

        plan = await self._repository.load_notification_plan(delivery_result.notification_plan_id)
        if plan is None:
            return _identity_invalid("notification_plan_missing")

        if delivery_result.notification_delivery_record_id is None:
            return _identity_invalid("exact_delivery_record_missing")

        exact = await self._repository.load_delivery_record(delivery_result.notification_delivery_record_id)
        if exact is None:
            return _identity_invalid("exact_delivery_record_missing")
        if exact.notification_plan_id != plan.notification_plan_id:
            return _identity_invalid("delivery_result_identity_mismatch")
        identity_error = _payload_identity_error(delivery_result, exact)
        if identity_error is not None:
            return _identity_invalid(identity_error)

        latest = await self._repository.load_latest_delivery_record(delivery_result.notification_plan_id)
        if latest is None:
            return _identity_invalid("notification_delivery_record_missing")

        later_success_exists = await self._repository.later_success_exists(
            notification_plan_id=plan.notification_plan_id,
            exact_created_at=exact.created_at,
        )
        decision = decide_delivery_result(
            event=delivery_result,
            exact_record=exact,
            latest_record=latest,
            plan=plan,
            later_success_exists=later_success_exists,
            now=exact.created_at,
            retry_max_attempts=1,
        )
        if decision.receipt_code is None:
            return _identity_invalid(decision.reason_code)
        if await self._repository.has_delivery_result_receipt(
            delivery_result.trigger_event_id,
            receipt_code=decision.receipt_code,
        ):
            return DeliveryResultWorkerResult(
                processed=True,
                classification=_classification_for_outcome(decision.outcome),
                action="already_marked",
                reason_code=decision.reason_code,
                already_marked=True,
            )
        return await self._record_decision(delivery_result, plan, exact, decision)

    async def _record_decision(
        self,
        event: DeliveryResultEvent,
        plan: NotificationPlanRecord,
        exact: LatestDeliveryRecord,
        decision: DeliveryResultDecision,
    ) -> DeliveryResultWorkerResult:
        async with self._repository.transaction():
            if decision.should_write_dlq:
                dead_letter_written = await self._repository.insert_delivery_terminal_dead_letter(
                    notification_plan_id=plan.notification_plan_id,
                    retry_count=exact.attempt_count,
                    last_error_code=exact.transport_error_code,
                )
            else:
                dead_letter_written = False
            inserted = await self._repository.insert_delivery_result_receipt(
                event_id=event.trigger_event_id,
                receipt_code=decision.receipt_code,
            )
        return DeliveryResultWorkerResult(
            processed=True,
            classification=_classification_for_outcome(decision.outcome),
            action=_action_for_outcome(decision.outcome, inserted),
            reason_code=decision.reason_code,
            marker_written=inserted,
            already_marked=not inserted,
            dead_letter_written=dead_letter_written,
        )


def _ignored(reason_code: str) -> DeliveryResultWorkerResult:
    return DeliveryResultWorkerResult(
        processed=False,
        classification="ignored",
        action="ignored",
        reason_code=reason_code,
    )


def _identity_invalid(reason_code: str) -> DeliveryResultWorkerResult:
    return DeliveryResultWorkerResult(
        processed=False,
        classification="identity_invalid",
        action="fail_closed",
        reason_code=reason_code,
    )


def _payload_identity_error(event: DeliveryResultEvent, exact: LatestDeliveryRecord) -> str | None:
    if event.delivery_status != exact.delivery_status:
        return "delivery_result_identity_mismatch"
    if event.attempt_count is not None and event.attempt_count != exact.attempt_count:
        return "delivery_result_identity_mismatch"
    if event.transport_error_code is not None and event.transport_error_code != exact.transport_error_code:
        return "delivery_result_identity_mismatch"
    if event.transport_error_class is not None and event.transport_error_class != exact.transport_error_class:
        return "delivery_result_identity_mismatch"
    return None


def _classification_for_outcome(outcome: str):
    return {
        "terminal_success": "terminal_success",
        "suppressed_noop": "logical_noop_success",
        "superseded_noop": "superseded_noop",
        "failed_retryable": "retryable_candidate",
        "failed_terminal": "terminal_failure",
    }.get(outcome, "identity_invalid")


def _action_for_outcome(outcome: str, inserted: bool):
    if not inserted:
        return "already_marked"
    return {
        "terminal_success": "mark_terminal_success",
        "suppressed_noop": "mark_logical_noop_success",
        "superseded_noop": "mark_superseded_noop_success",
        "failed_retryable": "record_retryable_interpretation",
        "failed_terminal": "record_terminal_failure",
    }.get(outcome, "fail_closed")
