from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID

from .config import MaintenanceConfig
from .delivery_result_worker import DeliveryResultWorker
from .delivery_replay import REPLAY_REQUESTED_EVENT_TYPE, evaluate_delivery_replay
from .delivery_retry import evaluate_retry_promotion
from .models import (
    DeliveryResultWorkerResult,
    LatestDeliveryRecord,
    NotificationPlanRecord,
    OutboxEvent,
    ReplayRequestRecord,
    RetryPromotionCandidate,
)
from .repositories import replay_requested_from_outbox
from .retry_policy import DeliveryResultDryRunNoopDecision, classify_delivery_result_dry_run_noop


class MaintenanceRepositoryProtocol(Protocol):
    def transaction(self): ...
    async def load_outbox_event(self, event_id: UUID) -> OutboxEvent | None: ...
    async def load_notification_plan(self, notification_plan_id: UUID) -> NotificationPlanRecord | None: ...
    async def load_latest_delivery_record(self, notification_plan_id: UUID) -> LatestDeliveryRecord | None: ...
    async def count_delivery_attempts(self, notification_plan_id: UUID) -> int: ...
    async def load_due_retry_candidates(self, limit: int, now: datetime) -> list[RetryPromotionCandidate]: ...
    async def insert_plan_created_outbox(
        self, *, notification_plan_id: UUID, dedupe_key: str, payload_json: dict
    ) -> bool: ...
    async def insert_retry_ceiling_dead_letter(self, *, notification_plan_id: UUID, retry_count: int) -> bool: ...
    async def load_replay_request(self, replay_request_id: UUID) -> ReplayRequestRecord | None: ...
    async def update_replay_request_status(self, replay_request_id: UUID, status: str) -> None: ...
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
    async def count_delivery_result_noop_job_attempts(self, notification_plan_id: UUID) -> int: ...
    async def insert_delivery_result_noop_job_attempt(self, notification_plan_id: UUID) -> bool: ...
    async def count_delivery_result_sent_success_job_attempts(self, notification_delivery_record_id: UUID) -> int: ...
    async def insert_delivery_result_sent_success_job_attempt(self, notification_delivery_record_id: UUID) -> bool: ...


class MaintenanceService:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        repository: MaintenanceRepositoryProtocol,
        logger: logging.Logger | None = None,
        now_fn=None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._logger = logger or logging.getLogger(__name__)
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    async def handle_maintenance_trigger_event(self, trigger_event_id: str | UUID) -> DeliveryResultWorkerResult | None:
        event_id = _parse_uuid(trigger_event_id)
        if event_id is None:
            self._logger.warning("maintenance_invalid_trigger_event_id")
            return None
        return await DeliveryResultWorker(
            repository=self._repository,
            logger=self._logger,
        ).handle_trigger_event(event_id)

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        if not self._config.enable_delivery_retry_promotion:
            return 0

        now = self._now_fn()
        candidates = await self._repository.load_due_retry_candidates(limit=limit or self._config.batch_size, now=now)
        action_count = 0
        for candidate in candidates:
            plan = candidate.plan
            delivery_status = candidate.latest_delivery.delivery_status if candidate.latest_delivery is not None else plan.status
            decision = evaluate_retry_promotion(
                delivery_status=delivery_status,
                plan=plan,
                latest_attempt_count=candidate.delivery_attempt_count,
                max_attempts=self._config.delivery_retry_max_attempts,
                enabled=self._config.enable_delivery_retry_promotion,
                now=now,
            )
            async with self._repository.transaction():
                if decision.action == "emit_retry_intent" and decision.dedupe_key and decision.payload:
                    inserted = await self._repository.insert_plan_created_outbox(
                        notification_plan_id=plan.notification_plan_id,
                        dedupe_key=decision.dedupe_key,
                        payload_json=decision.payload,
                    )
                    if inserted:
                        action_count += 1
                        await self._record_job_attempt(
                            queue_name=self._config.maintenance_queue_name,
                            root_object_id=plan.notification_plan_id,
                            status="succeeded",
                            error_code=None,
                        )
                elif decision.action == "dead_letter_retry_ceiling":
                    inserted = await self._repository.insert_retry_ceiling_dead_letter(
                        notification_plan_id=plan.notification_plan_id,
                        retry_count=candidate.delivery_attempt_count,
                    )
                    if inserted:
                        action_count += 1
                        await self._record_job_attempt(
                            queue_name=self._config.maintenance_queue_name,
                            root_object_id=plan.notification_plan_id,
                            status="failed_terminal",
                            error_code=decision.reason_code,
                        )
        return action_count

    async def record_delivery_result_dry_run_noop_success(
        self,
        *,
        notification_plan_id: UUID,
        delivery_status: str,
        delivery_reason: str | None,
    ) -> DeliveryResultDryRunNoopDecision:
        decision = classify_delivery_result_dry_run_noop(
            delivery_status=delivery_status,
            delivery_reason=delivery_reason,
        )
        if decision.action != "mark_logical_noop_success":
            return decision
        async with self._repository.transaction():
            await self._repository.insert_delivery_result_noop_job_attempt(notification_plan_id)
        return decision

    async def handle_replay_trigger_event(self, trigger_event_id: str | UUID) -> None:
        event_id = _parse_uuid(trigger_event_id)
        if event_id is None:
            self._logger.warning("maintenance_replay_invalid_trigger_event_id")
            return
        event = await self._repository.load_outbox_event(event_id)
        if event is None or event.event_type != REPLAY_REQUESTED_EVENT_TYPE:
            return
        replay_event = replay_requested_from_outbox(event)
        if replay_event is None:
            return

        replay_request = await self._repository.load_replay_request(replay_event.replay_request_id)
        plan = None
        if replay_request is not None and replay_request.root_object_type == "notification_plan":
            plan = await self._repository.load_notification_plan(replay_request.root_object_id)

        decision = evaluate_delivery_replay(
            config=self._config,
            replay_request=replay_request,
            plan=plan,
            replay_reason=replay_event.replay_reason,
        )
        async with self._repository.transaction():
            if replay_request is None:
                return
            if decision.action == "emit_replay_intent" and plan is not None and decision.dedupe_key and decision.payload:
                await self._repository.update_replay_request_status(replay_request.replay_request_id, "dispatched")
                await self._repository.insert_plan_created_outbox(
                    notification_plan_id=plan.notification_plan_id,
                    dedupe_key=decision.dedupe_key,
                    payload_json=decision.payload,
                )
                await self._repository.update_replay_request_status(replay_request.replay_request_id, "completed")
                await self._record_job_attempt(
                    queue_name=self._config.replay_queue_name,
                    root_object_id=plan.notification_plan_id,
                    status="succeeded",
                    error_code=None,
                )
            else:
                await self._repository.update_replay_request_status(
                    replay_request.replay_request_id,
                    _replay_reject_status(decision.reason_code),
                )
                await self._record_job_attempt(
                    queue_name=self._config.replay_queue_name,
                    root_object_id=replay_request.root_object_id,
                    status="failed_terminal",
                    error_code=decision.reason_code,
                )

    async def _record_job_attempt(
        self,
        *,
        queue_name: str,
        root_object_id: UUID,
        status: str,
        error_code: str | None,
    ) -> None:
        await self._repository.insert_job_attempt(
            stage_name="maintenance",
            queue_name=queue_name,
            root_object_type="notification_plan",
            root_object_id=root_object_id,
            attempt_status=status,
            error_code=error_code,
        )


def _parse_uuid(value: str | UUID) -> UUID | None:
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _replay_reject_status(reason_code: str) -> str:
    if reason_code == "rejected_by_env_guard":
        return "rejected_by_env_guard"
    if reason_code in {"unsupported_replay_type", "unsupported_replay_root"}:
        return "unsupported_in_stage41"
    return "failed"
