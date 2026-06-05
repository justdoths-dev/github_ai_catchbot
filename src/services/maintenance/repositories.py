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
    DeliveryGateSnapshot,
    DeliveryResultEvent,
    LatestDeliveryRecord,
    NotificationPlanRecord,
    OutboxEvent,
    ReplayRequestedEvent,
    ReplayRequestRecord,
    RetryPromotionCandidate,
    SelectedPlanRecoveryRow,
)
from .retry_policy import (
    DELIVERY_RESULT_NOOP_ERROR_CODE,
    DELIVERY_RESULT_NOOP_STAGE_NAME,
    DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
    DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
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
                       COALESCE(ldr.attempt_count, 0) AS delivery_attempt_count
                FROM notification_plans np
                JOIN LATERAL (
                    SELECT notification_delivery_record_id, notification_plan_id, delivery_status,
                           attempt_count, transport_error_code, transport_error_class,
                           telegram_response_json, created_at
                    FROM notification_delivery_records ndr
                    WHERE ndr.notification_plan_id = np.notification_plan_id
                    ORDER BY ndr.created_at DESC
                    LIMIT 1
                ) ldr ON true
                WHERE np.status = 'failed_retryable'::notification_status_enum
                  AND np.send_after IS NOT NULL
                  AND np.send_after <= :now
                  AND ldr.delivery_status = 'failed_retryable'::notification_status_enum
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
        analysis_id = _uuid_or_none(payload_json.get("analysis_id"))
        aggregate_type = "analysis" if analysis_id is not None else "notification_plan"
        aggregate_id = analysis_id or notification_plan_id
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status, created_at
                ) VALUES (
                    'notification.plan.created.v1',
                    :aggregate_type,
                    CAST(:aggregate_id AS uuid),
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
                "aggregate_type": aggregate_type,
                "aggregate_id": str(aggregate_id),
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
                    'request_explicit_delivery_replay',
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

    async def count_delivery_result_noop_job_attempts(self, notification_plan_id: UUID) -> int:
        result = await self._session.execute(
            sa.text(
                """
                SELECT COUNT(*)
                FROM job_attempts
                WHERE stage_name = :stage_name
                  AND queue_name = :queue_name
                  AND root_object_type = 'notification_plan'
                  AND root_object_id = CAST(:notification_plan_id AS uuid)
                  AND attempt_status = 'succeeded'::job_attempt_status_enum
                  AND error_code = :error_code
                """
            ),
            {
                "stage_name": DELIVERY_RESULT_NOOP_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "notification_plan_id": str(notification_plan_id),
                "error_code": DELIVERY_RESULT_NOOP_ERROR_CODE,
            },
        )
        return int(result.scalar_one())

    async def insert_delivery_result_noop_job_attempt(self, notification_plan_id: UUID) -> bool:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO job_attempts (
                    stage_name, queue_name, root_object_type, root_object_id,
                    attempt_no, started_at, finished_at, attempt_status, error_code, created_at
                )
                SELECT
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
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM job_attempts
                    WHERE stage_name = :stage_name
                      AND queue_name = :queue_name
                      AND root_object_type = :root_object_type
                      AND root_object_id = CAST(:root_object_id AS uuid)
                      AND attempt_status = CAST(:attempt_status AS job_attempt_status_enum)
                      AND error_code = :error_code
                )
                RETURNING job_attempt_id
                """
            ),
            {
                "stage_name": DELIVERY_RESULT_NOOP_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "root_object_type": "notification_plan",
                "root_object_id": str(notification_plan_id),
                "notification_plan_id": str(notification_plan_id),
                "attempt_status": "succeeded",
                "error_code": DELIVERY_RESULT_NOOP_ERROR_CODE,
            },
        )
        return result.scalar_one_or_none() is not None

    async def count_delivery_result_sent_success_job_attempts(self, notification_delivery_record_id: UUID) -> int:
        result = await self._session.execute(
            sa.text(
                """
                SELECT COUNT(*)
                FROM job_attempts
                WHERE stage_name = :stage_name
                  AND queue_name = :queue_name
                  AND root_object_type = 'notification_delivery_record'
                  AND root_object_id = CAST(:notification_delivery_record_id AS uuid)
                  AND attempt_status = 'succeeded'::job_attempt_status_enum
                  AND error_code = :error_code
                """
            ),
            {
                "stage_name": DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "notification_delivery_record_id": str(notification_delivery_record_id),
                "error_code": DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
            },
        )
        return int(result.scalar_one())

    async def insert_delivery_result_sent_success_job_attempt(self, notification_delivery_record_id: UUID) -> bool:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO job_attempts (
                    stage_name, queue_name, root_object_type, root_object_id,
                    attempt_no, started_at, finished_at, attempt_status, error_code, created_at
                )
                SELECT
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
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM job_attempts
                    WHERE stage_name = :stage_name
                      AND queue_name = :queue_name
                      AND root_object_type = :root_object_type
                      AND root_object_id = CAST(:root_object_id AS uuid)
                      AND attempt_status = CAST(:attempt_status AS job_attempt_status_enum)
                      AND error_code = :error_code
                )
                RETURNING job_attempt_id
                """
            ),
            {
                "stage_name": DELIVERY_RESULT_SENT_SUCCESS_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "root_object_type": "notification_delivery_record",
                "root_object_id": str(notification_delivery_record_id),
                "notification_delivery_record_id": str(notification_delivery_record_id),
                "attempt_status": "succeeded",
                "error_code": DELIVERY_RESULT_SENT_SUCCESS_ERROR_CODE,
            },
        )
        return result.scalar_one_or_none() is not None

    async def load_delivery_gate_snapshot(self) -> DeliveryGateSnapshot:
        result = await self._session.execute(
            sa.text(
                """
                WITH
                success_1h AS (
                    SELECT CASE
                             WHEN COUNT(*) = 0 THEN NULL
                             ELSE COUNT(*) FILTER (
                               WHERE delivery_status IN (
                                 'sent'::notification_status_enum,
                                 'edited'::notification_status_enum
                               )
                             )::numeric / COUNT(*)
                           END AS success_rate
                    FROM notification_delivery_records
                    WHERE created_at >= now() - interval '1 hour'
                ),
                success_24h AS (
                    SELECT CASE
                             WHEN COUNT(*) = 0 THEN NULL
                             ELSE COUNT(*) FILTER (
                               WHERE delivery_status IN (
                                 'sent'::notification_status_enum,
                                 'edited'::notification_status_enum
                               )
                             )::numeric / COUNT(*)
                           END AS success_rate
                    FROM notification_delivery_records
                    WHERE created_at >= now() - interval '24 hours'
                ),
                high_source_p95 AS (
                    WITH high_delivered AS (
                        SELECT sm.posted_at AS source_posted_at,
                               COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
                        FROM notification_plans np
                        JOIN candidate_group_proposals cgp
                          ON cgp.candidate_group_id = np.candidate_group_id
                        JOIN source_messages sm
                          ON sm.source_message_id = cgp.source_message_id
                        JOIN LATERAL (
                            SELECT ndr.sent_at, ndr.edited_at, ndr.created_at
                            FROM notification_delivery_records ndr
                            WHERE ndr.notification_plan_id = np.notification_plan_id
                              AND ndr.delivery_status IN (
                                'sent'::notification_status_enum,
                                'edited'::notification_status_enum
                              )
                            ORDER BY ndr.created_at DESC
                            LIMIT 1
                        ) dr ON true
                        WHERE np.urgency_profile = 'high'::urgency_profile_enum
                    )
                    SELECT percentile_cont(0.95)
                           WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - source_posted_at)))
                           AS value
                    FROM high_delivered
                    WHERE delivered_at IS NOT NULL
                ),
                plan_transport_p95 AS (
                    WITH delivered AS (
                        SELECT np.created_at AS plan_created_at,
                               COALESCE(dr.sent_at, dr.edited_at) AS delivered_at
                        FROM notification_plans np
                        JOIN LATERAL (
                            SELECT ndr.sent_at, ndr.edited_at, ndr.created_at
                            FROM notification_delivery_records ndr
                            WHERE ndr.notification_plan_id = np.notification_plan_id
                              AND ndr.delivery_status IN (
                                'sent'::notification_status_enum,
                                'edited'::notification_status_enum
                              )
                            ORDER BY ndr.created_at DESC
                            LIMIT 1
                        ) dr ON true
                    )
                    SELECT percentile_cont(0.95)
                           WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - plan_created_at)))
                           AS value
                    FROM delivered
                    WHERE delivered_at IS NOT NULL
                ),
                due_retry AS (
                    SELECT EXTRACT(EPOCH FROM (now() - MIN(send_after))) AS oldest_lag_sec
                    FROM notification_plans
                    WHERE status = 'failed_retryable'::notification_status_enum
                      AND send_after IS NOT NULL
                      AND send_after <= now()
                ),
                delivery_dlq AS (
                    SELECT COUNT(*) AS open_count,
                           EXTRACT(EPOCH FROM (now() - MIN(last_failed_at))) AS oldest_age_sec
                    FROM dead_letter_entries
                    WHERE root_object_type = 'notification_plan'
                      AND queue_name IN ('q.notification.send', 'q.maintenance', 'q.replay')
                ),
                send_disabled AS (
                    WITH latest_delivery AS (
                        SELECT DISTINCT ON (notification_plan_id)
                               delivery_status,
                               telegram_response_json
                        FROM notification_delivery_records
                        ORDER BY notification_plan_id, created_at DESC
                    )
                    SELECT COUNT(*) AS row_count
                    FROM latest_delivery
                    WHERE delivery_status = 'suppressed'::notification_status_enum
                      AND lower(telegram_response_json ->> 'send_disabled') = 'true'
                ),
                replay_guard AS (
                    SELECT COUNT(*) AS row_count
                    FROM replay_requests
                    WHERE replay_type = 'delivery'::replay_type_enum
                      AND root_object_type = 'notification_plan'
                      AND status = 'rejected_by_env_guard'
                      AND requested_at >= now() - interval '24 hours'
                ),
                retry_ceiling AS (
                    SELECT COUNT(*) AS row_count
                    FROM dead_letter_entries
                    WHERE root_object_type = 'notification_plan'
                      AND last_error_code = 'max_notification_retry_attempts_exceeded'
                      AND last_failed_at >= now() - interval '24 hours'
                ),
                duplicate_noop AS (
                    WITH duplicate_transitions AS (
                        SELECT COUNT(*) AS duplicate_or_noop_count
                        FROM state_transitions
                        WHERE object_type = 'notification_plan'
                          AND created_at >= now() - interval '24 hours'
                          AND reason_code IN (
                            'notification_duplicate_noop',
                            'telegram_edit_not_modified_noop'
                          )
                    ),
                    delivery_attempts AS (
                        SELECT COUNT(*) AS delivery_attempt_count
                        FROM notification_delivery_records
                        WHERE created_at >= now() - interval '24 hours'
                    )
                    SELECT CASE
                             WHEN delivery_attempt_count = 0 THEN NULL
                             ELSE duplicate_or_noop_count::numeric / delivery_attempt_count
                           END AS ratio
                    FROM duplicate_transitions
                    CROSS JOIN delivery_attempts
                )
                SELECT success_1h.success_rate AS success_rate_1h,
                       success_24h.success_rate AS success_rate_24h,
                       high_source_p95.value AS high_source_to_delivery_p95_sec,
                       plan_transport_p95.value AS plan_to_transport_p95_sec,
                       due_retry.oldest_lag_sec AS due_retry_oldest_lag_sec,
                       delivery_dlq.open_count AS open_delivery_dlq_count,
                       delivery_dlq.oldest_age_sec AS oldest_delivery_dlq_age_sec,
                       send_disabled.row_count AS unexpected_send_disabled_count,
                       replay_guard.row_count AS replay_guard_reject_count_24h,
                       retry_ceiling.row_count AS retry_ceiling_exceeded_count_24h,
                       duplicate_noop.ratio AS duplicate_noop_ratio_1h
                FROM success_1h, success_24h, high_source_p95, plan_transport_p95,
                     due_retry, delivery_dlq, send_disabled, replay_guard,
                     retry_ceiling, duplicate_noop
                """
            )
        )
        row = result.mappings().one()
        return DeliveryGateSnapshot(
            success_rate_1h=_float_or_none(row["success_rate_1h"]),
            success_rate_24h=_float_or_none(row["success_rate_24h"]),
            high_source_to_delivery_p95_sec=_float_or_none(row["high_source_to_delivery_p95_sec"]),
            plan_to_transport_p95_sec=_float_or_none(row["plan_to_transport_p95_sec"]),
            due_retry_oldest_lag_sec=_float_or_none(row["due_retry_oldest_lag_sec"]),
            open_delivery_dlq_count=int(row["open_delivery_dlq_count"] or 0),
            oldest_delivery_dlq_age_sec=_float_or_none(row["oldest_delivery_dlq_age_sec"]),
            unexpected_send_disabled_count=int(row["unexpected_send_disabled_count"] or 0),
            replay_guard_reject_count_24h=int(row["replay_guard_reject_count_24h"] or 0),
            retry_ceiling_exceeded_count_24h=int(row["retry_ceiling_exceeded_count_24h"] or 0),
            duplicate_noop_ratio_1h=_float_or_none(row["duplicate_noop_ratio_1h"]),
        )

    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]) -> list[SelectedPlanRecoveryRow]:
        if not notification_plan_ids:
            return []
        result = await self._session.execute(
            sa.text(
                """
                SELECT np.notification_plan_id,
                       np.analysis_id,
                       np.candidate_group_id,
                       np.status AS plan_status,
                       np.delivery_decision,
                       np.urgency_profile,
                       np.target_chat_id,
                       np.target_thread_id,
                       np.render_profile,
                       np.dedupe_subject_key,
                       np.material_change_hash,
                       np.send_after,
                       ldr.telegram_chat_id,
                       ldr.delivery_status,
                       ldr.attempt_count,
                       COALESCE(lower(ldr.telegram_response_json ->> 'send_disabled') = 'true', false) AS send_disabled,
                       EXISTS (
                         SELECT 1
                         FROM replay_requests rr
                         WHERE rr.replay_type = 'delivery'::replay_type_enum
                           AND rr.root_object_type = 'notification_plan'
                           AND rr.root_object_id = np.notification_plan_id
                           AND rr.status IN ('requested', 'dispatched', 'pending')
                       ) AS has_open_replay_request,
                       dld.dead_letter_entry_id IS NOT NULL AS has_delivery_dlq,
                       dld.next_manual_action AS delivery_dlq_next_manual_action,
                       dld.replay_hint AS delivery_dlq_replay_hint
                FROM notification_plans np
                LEFT JOIN LATERAL (
                    SELECT ndr.telegram_chat_id,
                           ndr.delivery_status,
                           ndr.attempt_count,
                           ndr.telegram_response_json,
                           ndr.created_at
                    FROM notification_delivery_records ndr
                    WHERE ndr.notification_plan_id = np.notification_plan_id
                    ORDER BY ndr.created_at DESC
                    LIMIT 1
                ) ldr ON true
                LEFT JOIN LATERAL (
                    SELECT dle.dead_letter_entry_id,
                           dle.next_manual_action,
                           dle.replay_hint
                    FROM dead_letter_entries dle
                    WHERE dle.root_object_type = 'notification_plan'
                      AND dle.root_object_id = np.notification_plan_id
                      AND dle.queue_name IN ('q.notification.send', 'q.maintenance', 'q.replay')
                    ORDER BY dle.last_failed_at DESC NULLS LAST, dle.dead_letter_entry_id DESC
                    LIMIT 1
                ) dld ON true
                WHERE np.notification_plan_id = ANY(CAST(:plan_ids AS uuid[]))
                ORDER BY np.notification_plan_id
                """
            ),
            {"plan_ids": [str(plan_id) for plan_id in notification_plan_ids]},
        )
        return [_selected_recovery_row_from_row(row) for row in result.mappings().all()]

    async def insert_replay_requests_for_selected_plans(
        self,
        *,
        plan_ids: list[UUID],
        requested_by: str,
    ) -> int:
        if not plan_ids:
            return 0
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO replay_requests (
                    replay_type, root_object_type, root_object_id, requested_by, requested_at, status
                )
                SELECT 'delivery'::replay_type_enum,
                       'notification_plan',
                       src.notification_plan_id,
                       :requested_by,
                       now(),
                       'requested'
                FROM (
                    SELECT DISTINCT unnest(CAST(:plan_ids AS uuid[])) AS notification_plan_id
                ) src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM replay_requests rr
                    WHERE rr.replay_type = 'delivery'::replay_type_enum
                      AND rr.root_object_type = 'notification_plan'
                      AND rr.root_object_id = src.notification_plan_id
                      AND rr.status IN ('requested', 'dispatched', 'pending')
                )
                RETURNING replay_request_id
                """
            ),
            {"plan_ids": [str(plan_id) for plan_id in plan_ids], "requested_by": requested_by},
        )
        return len(result.fetchall())

    async def insert_manual_retry_intent_outbox(
        self,
        *,
        row: SelectedPlanRecoveryRow,
        recovery_batch_id: str,
        dedupe_key: str,
        payload_json: dict[str, Any],
    ) -> bool:
        return await self.insert_plan_created_outbox(
            notification_plan_id=row.notification_plan_id,
            dedupe_key=dedupe_key,
            payload_json=payload_json,
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


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
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


def _selected_recovery_row_from_row(row: Any) -> SelectedPlanRecoveryRow:
    return SelectedPlanRecoveryRow(
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        analysis_id=UUID(str(row["analysis_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        plan_status=str(row["plan_status"]),
        delivery_status=_string_or_none(row["delivery_status"]),
        attempt_count=_int_or_none(row["attempt_count"]),
        send_after=row["send_after"],
        telegram_chat_id=_int_or_none(row["telegram_chat_id"]),
        target_chat_id=int(row["target_chat_id"]),
        target_thread_id=_int_or_none(row["target_thread_id"]),
        render_profile=_string_or_none(row["render_profile"]),
        dedupe_subject_key=str(row["dedupe_subject_key"]),
        material_change_hash=str(row["material_change_hash"]),
        urgency_profile=str(row["urgency_profile"]),
        delivery_decision=str(row["delivery_decision"]),
        send_disabled=bool(row["send_disabled"]),
        has_open_replay_request=bool(row["has_open_replay_request"]),
        has_delivery_dlq=bool(row["has_delivery_dlq"]),
        delivery_dlq_next_manual_action=_string_or_none(row["delivery_dlq_next_manual_action"]),
        delivery_dlq_replay_hint=_string_or_none(row["delivery_dlq_replay_hint"]),
    )
