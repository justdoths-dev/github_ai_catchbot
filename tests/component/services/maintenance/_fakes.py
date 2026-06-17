from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from services.maintenance.config import MaintenanceConfig
from services.maintenance.models import (
    LatestDeliveryRecord,
    NotificationPlanRecord,
    OutboxEvent,
    ReplayRequestRecord,
    RetryPromotionCandidate,
    StreamMessage,
)
from services.maintenance.retry_policy import (
    DELIVERY_RESULT_NOOP_ERROR_CODE,
    DELIVERY_RESULT_NOOP_STAGE_NAME,
    DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
    DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
)


PG_SCHEME = "postgresql+psycopg:" + "//"
REDIS_SCHEME = "redis:" + "//"


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.events: dict[UUID, OutboxEvent] = {}
        self.plans: dict[UUID, NotificationPlanRecord] = {}
        self.latest_delivery_records: dict[UUID, LatestDeliveryRecord] = {}
        self.delivery_attempt_counts: dict[UUID, int] = {}
        self.plan_created_outbox: list[dict] = []
        self.dead_letters: list[dict] = []
        self.replay_requests: dict[UUID, ReplayRequestRecord] = {}
        self.replay_status_updates: list[tuple[UUID, str]] = []
        self.job_attempts: list[dict] = []
        self.upstream_recompute_calls = 0

    def transaction(self):
        return Tx()

    async def load_outbox_event(self, event_id: UUID):
        return self.events.get(event_id)

    async def load_notification_plan(self, notification_plan_id: UUID):
        return self.plans.get(notification_plan_id)

    async def load_latest_delivery_record(self, notification_plan_id: UUID):
        return self.latest_delivery_records.get(notification_plan_id)

    async def count_delivery_attempts(self, notification_plan_id: UUID) -> int:
        return self.delivery_attempt_counts.get(notification_plan_id, 0)

    async def load_due_retry_candidates(self, limit: int, now: datetime):
        candidates = []
        plans = sorted(
            self.plans.values(),
            key=lambda row: (row.send_after or datetime.max.replace(tzinfo=timezone.utc), row.notification_plan_id),
        )
        for notification_plan in plans:
            if notification_plan.status != "failed_retryable":
                continue
            if notification_plan.send_after is None or notification_plan.send_after > now:
                continue
            latest_delivery = self.latest_delivery_records.get(notification_plan.notification_plan_id)
            if latest_delivery is None or latest_delivery.delivery_status != "failed_retryable":
                continue
            delivery_attempt_count = latest_delivery.attempt_count
            candidates.append(
                RetryPromotionCandidate(
                    plan=notification_plan,
                    latest_delivery=latest_delivery,
                    delivery_attempt_count=delivery_attempt_count,
                )
            )
            if len(candidates) >= limit:
                break
        return candidates

    async def insert_plan_created_outbox(self, *, notification_plan_id: UUID, dedupe_key: str, payload_json: dict):
        if any(row["dedupe_key"] == dedupe_key for row in self.plan_created_outbox):
            return False
        aggregate_id = UUID(str(payload_json["analysis_id"])) if payload_json.get("analysis_id") else notification_plan_id
        aggregate_type = "analysis" if payload_json.get("analysis_id") else "notification_plan"
        self.plan_created_outbox.append(
            {
                "event_type": "notification.plan.created.v1",
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "dedupe_key": dedupe_key,
                "payload_json": payload_json,
                "status": "pending",
            }
        )
        return True

    async def insert_retry_ceiling_dead_letter(self, *, notification_plan_id: UUID, retry_count: int) -> bool:
        if any(row["root_object_id"] == notification_plan_id for row in self.dead_letters):
            return False
        self.dead_letters.append(
            {
                "stage_name": "maintenance_delivery_retry",
                "queue_name": "q.maintenance",
                "root_object_type": "notification_plan",
                "root_object_id": notification_plan_id,
                "last_error_code": "max_notification_retry_attempts_exceeded",
                "retry_count": retry_count,
                "next_manual_action": "request_delivery_replay_after_operator_fix",
                "replay_hint": "delivery_replay_from_notification_plan",
            }
        )
        return True

    async def insert_delivery_terminal_dead_letter(
        self,
        *,
        notification_plan_id: UUID,
        retry_count: int,
        last_error_code: str | None,
    ) -> bool:
        error_code = last_error_code or "delivery_result_failed_terminal"
        if any(
            row["stage_name"] == "maintenance_delivery_result"
            and row["root_object_id"] == notification_plan_id
            and row["last_error_code"] == error_code
            for row in self.dead_letters
        ):
            return False
        self.dead_letters.append(
            {
                "stage_name": "maintenance_delivery_result",
                "queue_name": "q.maintenance",
                "root_object_type": "notification_plan",
                "root_object_id": notification_plan_id,
                "last_error_code": error_code,
                "retry_count": retry_count,
                "next_manual_action": "request_delivery_replay_after_operator_fix",
                "replay_hint": "delivery_replay_from_notification_plan",
            }
        )
        return True

    async def load_replay_request(self, replay_request_id: UUID):
        return self.replay_requests.get(replay_request_id)

    async def update_replay_request_status(self, replay_request_id: UUID, status: str) -> None:
        request = self.replay_requests[replay_request_id]
        self.replay_requests[replay_request_id] = replace(request, status=status)
        self.replay_status_updates.append((replay_request_id, status))

    async def insert_job_attempt(self, **kwargs) -> None:
        self.job_attempts.append(kwargs)

    async def count_delivery_result_noop_job_attempts(self, notification_plan_id: UUID) -> int:
        return await self.count_delivery_result_logical_noop_job_attempts(
            notification_plan_id,
            error_code=DELIVERY_RESULT_NOOP_ERROR_CODE,
        )

    async def count_delivery_result_logical_noop_job_attempts(
        self,
        notification_plan_id: UUID,
        *,
        error_code: str,
    ) -> int:
        return sum(
            1
            for row in self.job_attempts
            if row["stage_name"] == DELIVERY_RESULT_NOOP_STAGE_NAME
            and row["queue_name"] == "q.maintenance"
            and row["root_object_type"] == "notification_plan"
            and row["root_object_id"] == notification_plan_id
            and row["attempt_status"] == "succeeded"
            and row["error_code"] == error_code
        )

    async def insert_delivery_result_noop_job_attempt(self, notification_plan_id: UUID) -> bool:
        return await self.insert_delivery_result_logical_noop_job_attempt(
            notification_plan_id,
            error_code=DELIVERY_RESULT_NOOP_ERROR_CODE,
        )

    async def insert_delivery_result_logical_noop_job_attempt(
        self,
        notification_plan_id: UUID,
        *,
        error_code: str,
    ) -> bool:
        if await self.count_delivery_result_logical_noop_job_attempts(notification_plan_id, error_code=error_code):
            return False
        await self.insert_job_attempt(
            stage_name=DELIVERY_RESULT_NOOP_STAGE_NAME,
            queue_name="q.maintenance",
            root_object_type="notification_plan",
            root_object_id=notification_plan_id,
            attempt_status="succeeded",
            error_code=error_code,
        )
        return True

    async def count_delivery_result_sent_success_job_attempts(self, notification_delivery_record_id: UUID) -> int:
        return sum(
            1
            for row in self.job_attempts
            if row["stage_name"] == DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME
            and row["queue_name"] == "q.maintenance"
            and row["root_object_type"] == "notification_delivery_record"
            and row["root_object_id"] == notification_delivery_record_id
            and row["attempt_status"] == "succeeded"
            and row["error_code"] == DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE
        )

    async def insert_delivery_result_sent_success_job_attempt(self, notification_delivery_record_id: UUID) -> bool:
        if await self.count_delivery_result_sent_success_job_attempts(notification_delivery_record_id):
            return False
        await self.insert_job_attempt(
            stage_name=DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
            queue_name="q.maintenance",
            root_object_type="notification_delivery_record",
            root_object_id=notification_delivery_record_id,
            attempt_status="succeeded",
            error_code=DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
        )
        return True


class FakeConsumer:
    def __init__(self, messages: list[StreamMessage]) -> None:
        self.messages = messages
        self.acked: list[str] = []
        self.ensure_group_called = False

    async def ensure_group(self) -> None:
        self.ensure_group_called = True

    async def read_batch(self):
        messages = self.messages
        self.messages = []
        return messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


def config(
    *,
    app_env: str = "test",
    enable_retry: bool = True,
    enable_replay_to_prod_db: bool = False,
    max_attempts: int = 3,
) -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env=app_env,
        database_url=PG_SCHEME + "example",
        redis_url=REDIS_SCHEME + "example",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        maintenance_consumer_name="test",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
        replay_consumer_name="test",
        batch_size=50,
        block_ms=100,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=max_attempts,
        enable_notification_send=True,
        notifier_telegram_dry_run=False,
        enable_delivery_retry_promotion=enable_retry,
        enable_replay_to_prod_db=enable_replay_to_prod_db,
        delivery_gate_min_success_rate_1h=0.99,
        delivery_gate_min_success_rate_24h=0.99,
        delivery_gate_max_high_source_to_delivery_p95_sec=120,
        delivery_gate_max_plan_to_transport_p95_sec=120,
        delivery_gate_max_due_retry_lag_sec=120,
        delivery_gate_max_open_dlq_count=0,
        delivery_gate_max_send_disabled_count=0,
        delivery_gate_max_replay_guard_reject_count=0,
        delivery_gate_require_operator_review_for_full=True,
        log_level="INFO",
    )


def plan(*, status: str = "failed_retryable", send_after=None, suppress_reason_code: str | None = None):
    now = datetime.now(timezone.utc)
    return NotificationPlanRecord(
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
        send_after=send_after if send_after is not None else now - timedelta(minutes=1),
        suppress_reason_code=suppress_reason_code,
        status=status,
    )


def latest_delivery_record(
    *,
    notification_plan_id: UUID,
    delivery_status: str = "failed_retryable",
    attempt_count: int = 1,
    transport_error_code: str | None = "telegram_retryable_error",
    transport_error_class: str | None = "retryable",
) -> LatestDeliveryRecord:
    return LatestDeliveryRecord(
        notification_delivery_record_id=uuid4(),
        notification_plan_id=notification_plan_id,
        delivery_status=delivery_status,
        attempt_count=attempt_count,
        transport_error_code=transport_error_code,
        transport_error_class=transport_error_class,
        telegram_response_json=None,
        created_at=datetime.now(timezone.utc),
    )


def outbox_event(
    event_type: str,
    *,
    aggregate_id: UUID,
    payload_json: dict,
    aggregate_type: str = "notification_plan",
) -> OutboxEvent:
    return OutboxEvent(
        event_id=uuid4(),
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        payload_json=payload_json,
    )
