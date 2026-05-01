from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from .delivery_retry import MAINTENANCE_QUEUE_NAME
from .delivery_replay import REPLAY_QUEUE_NAME
from .models import (
    DeliveryResultEvent,
    LatestDeliveryRecord,
    NotificationPlanRecord,
    OutboxEvent,
    ReplayRequestedEvent,
    ReplayRequestRecord,
    RetryPromotionCandidate,
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class MaintenanceRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_outbox_event(self, event_id: UUID) -> OutboxEvent | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(event_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = _json_loads(row["payload_json"])
        return OutboxEvent(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            aggregate_type=str(row["aggregate_type"]),
            aggregate_id=UUID(str(row["aggregate_id"])),
            payload_json=payload if isinstance(payload, dict) else {},
        )

    async def load_notification_plan(self, notification_plan_id: UUID) -> NotificationPlanRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_plan_id, analysis_id, candidate_group_id,
                       delivery_decision, urgency_profile, target_chat_id,
                       target_thread_id, render_profile, dedupe_subject_key,
                       material_change_hash, send_after, suppress_reason_code, status
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        row = result.mappings().first()
        return _plan_from_row(row) if row is not None else None

    async def count_delivery_attempts(self, notification_plan_id: UUID) -> int:
        result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        return int(result.scalar_one())

    async def load_latest_delivery_record(self, notification_plan_id: UUID) -> LatestDeliveryRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_delivery_record_id, notification_plan_id, delivery_status,
                       attempt_count, transport_error_code, transport_error_class,
                       telegram_response_json, created_at
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        row = result.mappings().first()
        return _latest_delivery_from_row(row) if row is not None else None

    async def load_due_retry_candidates(self, limit: int, now: datetime) -> list[RetryPromotionCandidate]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT np.notification_plan_id, np.analysis_id, np.candidate_group_id,
                       np.delivery_decision, np.urgency_profile, np.target_chat_id,
                       np.target_thread_id, np.render_profile, np.dedupe_subject_key,
                       np.material_change_hash, np.send_after, np.suppress_reason_code, np.status,
                       ldr.notification_delivery_record_id AS ldr_notification_delivery_record_id,
                       ldr.notification_plan_id AS ldr_notification_plan_id,
                       ldr.delivery_status AS ldr_delivery_status,
                       ldr.attempt_count AS ldr_attempt_count,
                       ldr.transport_error_code AS ldr_transport_error_code,
                       ldr.transport_error_class AS ldr_transport_error_class,
                       ldr.telegram_response_json AS ldr_telegram_response_json,
                       ldr.created_at AS ldr_created_at,
                       COALESCE(dac.delivery_attempt_count, 0) AS delivery_attempt_count
                FROM notification_plans np
                LEFT JOIN LATERAL (
                    SELECT notification_delivery_record_id, notification_plan_id, delivery_status,
                           attempt_count, transport_error_code, transport_error_class,
                           telegram_response_json, created_at
                    FROM notification_delivery_records ndr
                    WHERE ndr.notification_plan_id = np.notification_plan_id
                    ORDER BY ndr.created_at DESC
                    LIMIT 1
                ) ldr ON true
                LEFT JOIN LATERAL (
                    SELECT count(*)::int AS delivery_attempt_count
                    FROM notification_delivery_records ndr_count
                    WHERE ndr_count.notification_plan_id = np.notification_plan_id
                ) dac ON true
                WHERE np.status = 'failed_retryable'::notification_status_enum
                  AND np.send_after IS NOT NULL
                  AND np.send_after <= :now
                ORDER BY np.send_after ASC, np.created_at ASC
                LIMIT :limit
                """
            ),
            {"limit": limit, "now": now},
        )
        return [_retry_candidate_from_row(row) for row in result.mappings().all()]

    async def insert_plan_created_outbox(
        self,
        *,
        notification_plan_id: UUID,
        dedupe_key: str,
        payload_json: dict[str, Any],
    ) -> bool:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status, created_at
                ) VALUES (
                    'notification.plan.created.v1',
                    'notification_plan',
                    CAST(:notification_plan_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                RETURNING event_id
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload_json),
            },
        )
        return result.scalar_one_or_none() is not None

    async def insert_retry_ceiling_dead_letter(
        self,
        *,
        notification_plan_id: UUID,
        retry_count: int,
    ) -> bool:
        existing = await self._session.execute(
            sa.text(
                """
                SELECT dead_letter_entry_id
                FROM dead_letter_entries
                WHERE stage_name = 'maintenance_delivery_retry'
                  AND queue_name = :queue_name
                  AND root_object_type = 'notification_plan'
                  AND root_object_id = CAST(:notification_plan_id AS uuid)
                  AND last_error_code = 'max_notification_retry_attempts_exceeded'
                  AND replay_hint = 'delivery_replay_from_notification_plan'
                LIMIT 1
                """
            ),
            {"queue_name": MAINTENANCE_QUEUE_NAME, "notification_plan_id": str(notification_plan_id)},
        )
        if existing.scalar_one_or_none() is not None:
            return False
        await self._session.execute(
            sa.text(
                """
                INSERT INTO dead_letter_entries (
                    stage_name, queue_name, root_object_type, root_object_id,
                    last_error_code, last_error_snippet, retry_count,
                    first_failed_at, last_failed_at, next_manual_action, replay_hint
                ) VALUES (
                    'maintenance_delivery_retry',
                    :queue_name,
                    'notification_plan',
                    CAST(:notification_plan_id AS uuid),
                    'max_notification_retry_attempts_exceeded',
                    'delivery retry ceiling reached',
                    :retry_count,
                    now(),
                    now(),
                    'request_delivery_replay_after_operator_fix',
                    'delivery_replay_from_notification_plan'
                )
                """
            ),
            {
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "notification_plan_id": str(notification_plan_id),
                "retry_count": retry_count,
            },
        )
        return True

    async def load_replay_request(self, replay_request_id: UUID) -> ReplayRequestRecord | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT replay_request_id, replay_type, root_object_type, root_object_id,
                       status, requested_by, requested_at
                FROM replay_requests
                WHERE replay_request_id = CAST(:replay_request_id AS uuid)
                """
            ),
            {"replay_request_id": str(replay_request_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ReplayRequestRecord(
            replay_request_id=UUID(str(row["replay_request_id"])),
            replay_type=str(row["replay_type"]),
            root_object_type=str(row["root_object_type"]),
            root_object_id=UUID(str(row["root_object_id"])),
            status=_string_or_none(row["status"]),
            requested_by=_string_or_none(row["requested_by"]),
            requested_at=row["requested_at"],
        )

    async def update_replay_request_status(self, replay_request_id: UUID, status: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE replay_requests
                SET status = :status
                WHERE replay_request_id = CAST(:replay_request_id AS uuid)
                """
            ),
            {"replay_request_id": str(replay_request_id), "status": status},
        )

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO job_attempts (
                    stage_name, queue_name, root_object_type, root_object_id,
                    attempt_no, started_at, finished_at, attempt_status, error_code, created_at
                ) VALUES (
                    :stage_name,
                    :queue_name,
                    :root_object_type,
                    CAST(:root_object_id AS uuid),
                    1,
                    now(),
                    now(),
                    CAST(:attempt_status AS job_attempt_status_enum),
                    :error_code,
                    now()
                )
                """
            ),
            {
                "stage_name": stage_name,
                "queue_name": queue_name,
                "root_object_type": root_object_type,
                "root_object_id": str(root_object_id),
                "attempt_status": attempt_status,
                "error_code": error_code,
            },
        )


def delivery_result_from_outbox(event: OutboxEvent) -> DeliveryResultEvent | None:
    payload = event.payload_json
    notification_plan_id = _uuid_or_none(payload.get("notification_plan_id") or event.aggregate_id)
    if notification_plan_id is None:
        return None
    return DeliveryResultEvent(
        trigger_event_id=event.event_id,
        notification_plan_id=notification_plan_id,
        delivery_status=str(payload.get("delivery_status") or ""),
        notification_delivery_record_id=_uuid_or_none(payload.get("notification_delivery_record_id")),
        attempt_count=_int_or_none(payload.get("attempt_count")),
        transport_error_code=_string_or_none(payload.get("transport_error_code")),
        transport_error_class=_string_or_none(payload.get("transport_error_class")),
    )


def replay_requested_from_outbox(event: OutboxEvent) -> ReplayRequestedEvent | None:
    payload = event.payload_json
    replay_request_id = _uuid_or_none(payload.get("replay_request_id") or event.aggregate_id)
    if replay_request_id is None:
        return None
    return ReplayRequestedEvent(
        trigger_event_id=event.event_id,
        replay_request_id=replay_request_id,
        replay_type=_string_or_none(payload.get("replay_type")),
        root_object_type=_string_or_none(payload.get("root_object_type")),
        root_object_id=_uuid_or_none(payload.get("root_object_id")),
        replay_reason=_string_or_none(payload.get("replay_reason")),
    )


def _plan_from_row(row: Any) -> NotificationPlanRecord:
    return NotificationPlanRecord(
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        analysis_id=UUID(str(row["analysis_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        delivery_decision=str(row["delivery_decision"]),
        urgency_profile=str(row["urgency_profile"]),
        target_chat_id=int(row["target_chat_id"]),
        target_thread_id=_int_or_none(row["target_thread_id"]),
        render_profile=_string_or_none(row["render_profile"]),
        dedupe_subject_key=str(row["dedupe_subject_key"]),
        material_change_hash=str(row["material_change_hash"]),
        send_after=row["send_after"],
        suppress_reason_code=_string_or_none(row["suppress_reason_code"]),
        status=str(row["status"]),
    )


def _latest_delivery_from_row(row: Any) -> LatestDeliveryRecord:
    payload = _json_loads(row["telegram_response_json"])
    return LatestDeliveryRecord(
        notification_delivery_record_id=UUID(str(row["notification_delivery_record_id"])),
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        delivery_status=str(row["delivery_status"]),
        attempt_count=int(row["attempt_count"]),
        transport_error_code=_string_or_none(row["transport_error_code"]),
        transport_error_class=_string_or_none(row["transport_error_class"]),
        telegram_response_json=payload if isinstance(payload, dict) else None,
        created_at=row["created_at"],
    )


def _latest_delivery_from_due_row(row: Any) -> LatestDeliveryRecord | None:
    if row["ldr_notification_delivery_record_id"] is None:
        return None
    payload = _json_loads(row["ldr_telegram_response_json"])
    return LatestDeliveryRecord(
        notification_delivery_record_id=UUID(str(row["ldr_notification_delivery_record_id"])),
        notification_plan_id=UUID(str(row["ldr_notification_plan_id"])),
        delivery_status=str(row["ldr_delivery_status"]),
        attempt_count=int(row["ldr_attempt_count"]),
        transport_error_code=_string_or_none(row["ldr_transport_error_code"]),
        transport_error_class=_string_or_none(row["ldr_transport_error_class"]),
        telegram_response_json=payload if isinstance(payload, dict) else None,
        created_at=row["ldr_created_at"],
    )


def _retry_candidate_from_row(row: Any) -> RetryPromotionCandidate:
    latest_delivery = _latest_delivery_from_due_row(row)
    delivery_attempt_count = int(row["delivery_attempt_count"])
    if latest_delivery is not None:
        delivery_attempt_count = max(delivery_attempt_count, latest_delivery.attempt_count)
    return RetryPromotionCandidate(
        plan=_plan_from_row(row),
        latest_delivery=latest_delivery,
        delivery_attempt_count=delivery_attempt_count,
    )


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported json type: {type(value)!r}")


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
