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
            delivery_attempt_count = self.delivery_attempt_counts.get(notification_plan.notification_plan_id, 0)
            if latest_delivery is not None:
                delivery_attempt_count = max(delivery_attempt_count, latest_delivery.attempt_count)
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
        self.plan_created_outbox.append(
            {
                "event_type": "notification.plan.created.v1",
                "aggregate_type": "notification_plan",
                "aggregate_id": notification_plan_id,
                "dedupe_key": dedupe_key,
                "payload_json": payload_json,
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
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
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
        enable_delivery_retry_promotion=enable_retry,
        enable_replay_to_prod_db=enable_replay_to_prod_db,
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
) -> LatestDeliveryRecord:
    return LatestDeliveryRecord(
        notification_delivery_record_id=uuid4(),
        notification_plan_id=notification_plan_id,
        delivery_status=delivery_status,
        attempt_count=attempt_count,
        transport_error_code="telegram_retryable_error",
        transport_error_class="retryable",
        telegram_response_json=None,
        created_at=datetime.now(timezone.utc),
    )


def outbox_event(event_type: str, *, aggregate_id: UUID, payload_json: dict) -> OutboxEvent:
    return OutboxEvent(
        event_id=uuid4(),
        event_type=event_type,
        aggregate_type="notification_plan",
        aggregate_id=aggregate_id,
        payload_json=payload_json,
    )
