from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from .models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    ExistingRecentDelivery,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotifierPlanIdempotencySnapshot,
    NotificationPlanDraft,
    NotificationRenderDraft,
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class NotifierTelegramRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSessionLike]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def load_intent_job(self, trigger_event_id: UUID) -> NotificationIntentJob | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, payload_json
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None or str(row["event_type"]) != "notification.plan.created.v1":
            return None
        payload = _json_loads(row["payload_json"]) or {}
        notification_plan_id = _uuid_or_none(payload.get("notification_plan_id"))
        analysis_id = _uuid_or_none(payload.get("analysis_id"))
        candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
        target_chat_id = _int_or_none(payload.get("target_chat_id"))
        if None in {notification_plan_id, analysis_id, candidate_group_id, target_chat_id}:
            return None
        delivery_decision = str(payload.get("delivery_decision") or "")
        urgency_profile = str(payload.get("urgency_profile") or "")
        dedupe_subject_key = str(payload.get("dedupe_subject_key") or "")
        material_change_hash = str(payload.get("material_change_hash") or "")
        if not delivery_decision or not urgency_profile or not dedupe_subject_key or not material_change_hash:
            return None
        return NotificationIntentJob(
            trigger_event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            notification_plan_id=notification_plan_id,
            analysis_id=analysis_id,
            candidate_group_id=candidate_group_id,
            delivery_decision=delivery_decision,  # type: ignore[arg-type]
            urgency_profile=urgency_profile,  # type: ignore[arg-type]
            target_chat_id=int(target_chat_id),
            target_thread_id=_int_or_none(payload.get("target_thread_id")),
            render_profile=_string_or_none(payload.get("render_profile")),
            dedupe_subject_key=dedupe_subject_key,
            material_change_hash=material_change_hash,
            send_after=_datetime_or_none(payload.get("send_after")),
            suppress_reason_code=_string_or_none(payload.get("suppress_reason_code")),
        )

    async def load_notification_plan(self, notification_plan_id: UUID) -> dict[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_plan_id, analysis_id, candidate_group_id, delivery_decision,
                       urgency_profile, target_chat_id, target_thread_id, render_profile,
                       dedupe_subject_key, material_change_hash, send_after,
                       suppress_reason_code, status
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def load_event_outbox(self, event_id: UUID) -> dict[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status,
                       fail_count, created_at, published_at
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(event_id)},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def mark_event_outbox_published(self, *, event_id: UUID, published_at: datetime) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE event_outbox
                SET status = 'published'::outbox_status_enum,
                    published_at = :published_at
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(event_id), "published_at": published_at},
        )

    async def load_notification_plan_intent(self, notification_plan_id: UUID) -> NotificationIntentJob | None:
        row = await self.load_notification_plan(notification_plan_id)
        if row is None:
            return None
        analysis_id = _uuid_or_none(row.get("analysis_id"))
        candidate_group_id = _uuid_or_none(row.get("candidate_group_id"))
        target_chat_id = _int_or_none(row.get("target_chat_id"))
        if None in {analysis_id, candidate_group_id, target_chat_id}:
            return None
        delivery_decision = str(row.get("delivery_decision") or "")
        urgency_profile = str(row.get("urgency_profile") or "")
        dedupe_subject_key = str(row.get("dedupe_subject_key") or "")
        material_change_hash = str(row.get("material_change_hash") or "")
        if not delivery_decision or not urgency_profile or not dedupe_subject_key or not material_change_hash:
            return None
        return NotificationIntentJob(
            trigger_event_id=notification_plan_id,
            event_type="notification.plan.created.v1",
            notification_plan_id=notification_plan_id,
            analysis_id=analysis_id,
            candidate_group_id=candidate_group_id,
            delivery_decision=delivery_decision,  # type: ignore[arg-type]
            urgency_profile=urgency_profile,  # type: ignore[arg-type]
            target_chat_id=int(target_chat_id),
            target_thread_id=_int_or_none(row.get("target_thread_id")),
            render_profile=_string_or_none(row.get("render_profile")),
            dedupe_subject_key=dedupe_subject_key,
            material_change_hash=material_change_hash,
            send_after=_datetime_or_none(row.get("send_after")),
            suppress_reason_code=_string_or_none(row.get("suppress_reason_code")),
        )

    async def load_existing_plan_by_material(
        self,
        *,
        analysis_id: UUID,
        target_chat_id: int,
        material_change_hash: str,
    ) -> dict[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_plan_id, analysis_id, candidate_group_id, delivery_decision,
                       urgency_profile, target_chat_id, target_thread_id, render_profile,
                       dedupe_subject_key, material_change_hash, send_after,
                       suppress_reason_code, status
                FROM notification_plans
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                  AND target_chat_id = :target_chat_id
                  AND material_change_hash = :material_change_hash
                """
            ),
            {
                "analysis_id": str(analysis_id),
                "target_chat_id": target_chat_id,
                "material_change_hash": material_change_hash,
            },
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def load_idempotency_plan_snapshots(self, intent: NotificationIntentJob) -> list[NotifierPlanIdempotencySnapshot]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT p.notification_plan_id,
                       p.status::text AS status,
                       COUNT(DISTINCT r.notification_render_id) AS render_count,
                       COUNT(DISTINCT d.notification_delivery_record_id) AS delivery_record_count,
                       COUNT(DISTINCT d.notification_delivery_record_id) FILTER (
                           WHERE d.delivery_status::text IN ('sent', 'edited')
                       ) AS sent_delivery_count,
                       COUNT(DISTINCT d.notification_delivery_record_id) FILTER (
                           WHERE d.delivery_status::text = 'suppressed'
                       ) AS suppressed_delivery_count,
                       COUNT(DISTINCT d.notification_delivery_record_id) FILTER (
                           WHERE d.delivery_status::text IN ('sent', 'edited', 'suppressed', 'failed_terminal')
                       ) AS terminal_delivery_count,
                       COUNT(DISTINCT d.notification_delivery_record_id) FILTER (
                           WHERE d.delivery_status::text = 'failed_retryable'
                       ) AS retryable_failure_count,
                       COUNT(DISTINCT d.notification_delivery_record_id) FILTER (
                           WHERE d.delivery_status::text IN ('sent', 'edited')
                             AND d.telegram_chat_id IS NOT NULL
                       ) AS sent_delivery_chat_id_present_count,
                       COUNT(DISTINCT d.notification_delivery_record_id) FILTER (
                           WHERE d.delivery_status::text IN ('sent', 'edited')
                             AND d.telegram_message_id IS NOT NULL
                       ) AS sent_delivery_message_id_present_count
                FROM notification_plans p
                LEFT JOIN notification_renders r ON r.notification_plan_id = p.notification_plan_id
                LEFT JOIN notification_delivery_records d ON d.notification_plan_id = p.notification_plan_id
                WHERE p.notification_plan_id = CAST(:notification_plan_id AS uuid)
                   OR (
                       p.analysis_id = CAST(:analysis_id AS uuid)
                       AND p.candidate_group_id = CAST(:candidate_group_id AS uuid)
                       AND p.target_chat_id = :target_chat_id
                       AND p.dedupe_subject_key = :dedupe_subject_key
                       AND p.material_change_hash = :material_change_hash
                   )
                GROUP BY p.notification_plan_id, p.status, p.created_at
                ORDER BY p.created_at ASC, p.notification_plan_id ASC
                """
            ),
            {
                "notification_plan_id": str(intent.notification_plan_id),
                "analysis_id": str(intent.analysis_id),
                "candidate_group_id": str(intent.candidate_group_id),
                "target_chat_id": intent.target_chat_id,
                "dedupe_subject_key": intent.dedupe_subject_key,
                "material_change_hash": intent.material_change_hash,
            },
        )
        snapshots: list[NotifierPlanIdempotencySnapshot] = []
        for row in result.mappings().all():
            snapshots.append(
                NotifierPlanIdempotencySnapshot(
                    notification_plan_id=UUID(str(row["notification_plan_id"])),
                    status=str(row["status"]),
                    render_count=int(row["render_count"] or 0),
                    delivery_record_count=int(row["delivery_record_count"] or 0),
                    sent_delivery_count=int(row["sent_delivery_count"] or 0),
                    suppressed_delivery_count=int(row["suppressed_delivery_count"] or 0),
                    terminal_delivery_count=int(row["terminal_delivery_count"] or 0),
                    retryable_failure_count=int(row["retryable_failure_count"] or 0),
                    sent_delivery_chat_id_present_count=int(row["sent_delivery_chat_id_present_count"] or 0),
                    sent_delivery_message_id_present_count=int(row["sent_delivery_message_id_present_count"] or 0),
                )
            )
        return snapshots

    async def insert_notification_plan(self, draft: NotificationPlanDraft) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO notification_plans (
                    notification_plan_id,
                    analysis_id,
                    candidate_group_id,
                    delivery_decision,
                    urgency_profile,
                    target_chat_id,
                    target_thread_id,
                    render_profile,
                    dedupe_subject_key,
                    material_change_hash,
                    send_after,
                    suppress_reason_code,
                    status,
                    created_at
                ) VALUES (
                    CAST(:notification_plan_id AS uuid),
                    CAST(:analysis_id AS uuid),
                    CAST(:candidate_group_id AS uuid),
                    CAST(:delivery_decision AS delivery_decision_enum),
                    CAST(:urgency_profile AS urgency_profile_enum),
                    :target_chat_id,
                    :target_thread_id,
                    :render_profile,
                    :dedupe_subject_key,
                    :material_change_hash,
                    :send_after,
                    :suppress_reason_code,
                    CAST(:status AS notification_status_enum),
                    now()
                )
                ON CONFLICT ON CONSTRAINT uq_notification_plans_analysis_target_material
                DO NOTHING
                RETURNING notification_plan_id
                """
            ),
            {
                "notification_plan_id": str(draft.notification_plan_id),
                "analysis_id": str(draft.analysis_id),
                "candidate_group_id": str(draft.candidate_group_id),
                "delivery_decision": draft.delivery_decision,
                "urgency_profile": draft.urgency_profile,
                "target_chat_id": draft.target_chat_id,
                "target_thread_id": draft.target_thread_id,
                "render_profile": draft.render_profile,
                "dedupe_subject_key": draft.dedupe_subject_key,
                "material_change_hash": draft.material_change_hash,
                "send_after": draft.send_after,
                "suppress_reason_code": draft.suppress_reason_code,
                "status": draft.status,
            },
        )
        inserted = result.scalar_one_or_none()
        if inserted is not None:
            return UUID(str(inserted))
        existing = await self.load_existing_plan_by_material(
            analysis_id=draft.analysis_id,
            target_chat_id=draft.target_chat_id,
            material_change_hash=draft.material_change_hash,
        )
        if existing is None:
            existing = await self.load_notification_plan(draft.notification_plan_id)
        if existing is None:
            raise RuntimeError("notification plan insert conflicted but existing row was not found")
        return UUID(str(existing["notification_plan_id"]))

    async def insert_published_notification_plan_created_outbox(
        self,
        *,
        event_id: UUID,
        notification_plan_id: UUID,
        dedupe_key: str,
        payload_json: dict[str, Any],
    ) -> UUID | None:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_id,
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status,
                    created_at,
                    published_at
                ) VALUES (
                    CAST(:event_id AS uuid),
                    'notification.plan.created.v1',
                    'notification_plan',
                    CAST(:notification_plan_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'published'::outbox_status_enum,
                    now(),
                    now()
                )
                ON CONFLICT DO NOTHING
                RETURNING event_id
                """
            ),
            {
                "event_id": str(event_id),
                "notification_plan_id": str(notification_plan_id),
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload_json),
            },
        )
        inserted = result.scalar_one_or_none()
        return UUID(str(inserted)) if inserted else None

    async def load_analysis(self, analysis_id: UUID) -> AnalysisRenderContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT analysis_id, candidate_group_id, judge_output_id, verdict, delivery_decision,
                       reason_codes_json, evidence_limitations_ko, recommended_action_ko,
                       freshness_note_ko, created_at
                FROM analyses
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                """
            ),
            {"analysis_id": str(analysis_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return AnalysisRenderContext(
            analysis_id=UUID(str(row["analysis_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            judge_output_id=UUID(str(row["judge_output_id"])),
            verdict=str(row["verdict"]),
            delivery_decision=str(row["delivery_decision"]),
            reason_codes_json=_string_list(_json_loads(row["reason_codes_json"])),
            evidence_limitations_ko=_string_or_none(row["evidence_limitations_ko"]),
            recommended_action_ko=_string_or_none(row["recommended_action_ko"]),
            freshness_note_ko=_string_or_none(row["freshness_note_ko"]),
            created_at=row["created_at"],
        )

    async def load_judge_output_render_fields(self, judge_output_id: UUID) -> JudgeOutputRenderContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT judge_output_id, payload_json, model_confidence_band
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": str(judge_output_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = _json_loads(row["payload_json"])
        return JudgeOutputRenderContext(
            judge_output_id=UUID(str(row["judge_output_id"])),
            payload_json=payload if isinstance(payload, dict) else {},
            model_confidence_band=_string_or_none(row["model_confidence_band"]),
        )

    async def load_candidate_render_context(self, candidate_group_id: UUID) -> CandidateRenderContext | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT cgp.candidate_group_id, cgp.source_message_id, cgp.current_primary_artifact_id,
                       ar.artifact_type AS primary_artifact_type,
                       ar.canonical_url AS primary_canonical_url,
                       ar.canonical_id AS primary_canonical_id,
                       sm.message_link AS source_message_link,
                       sm.text_surface AS source_text_surface
                FROM candidate_group_proposals cgp
                LEFT JOIN artifact_registry ar ON ar.artifact_id = cgp.current_primary_artifact_id
                LEFT JOIN source_messages sm ON sm.source_message_id = cgp.source_message_id
                WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return CandidateRenderContext(
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            source_message_id=_uuid_or_none(row["source_message_id"]),
            current_primary_artifact_id=_uuid_or_none(row["current_primary_artifact_id"]),
            primary_artifact_type=_string_or_none(row["primary_artifact_type"]),
            primary_canonical_url=_string_or_none(row["primary_canonical_url"]),
            primary_canonical_id=_string_or_none(row["primary_canonical_id"]),
            source_message_link=_string_or_none(row["source_message_link"]),
            source_text_surface=_string_or_none(row["source_text_surface"]),
        )

    async def load_recent_successful_delivery(
        self,
        *,
        dedupe_subject_key: str,
        target_chat_id: int,
    ) -> ExistingRecentDelivery | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT p.notification_plan_id,
                       p.material_change_hash,
                       p.urgency_profile,
                       p.render_profile,
                       d.telegram_message_id,
                       d.telegram_chat_id,
                       d.created_at,
                       ar.canonical_url AS primary_canonical_url
                FROM notification_delivery_records d
                JOIN notification_plans p ON p.notification_plan_id = d.notification_plan_id
                LEFT JOIN candidate_group_proposals cgp ON cgp.candidate_group_id = p.candidate_group_id
                LEFT JOIN artifact_registry ar ON ar.artifact_id = cgp.current_primary_artifact_id
                WHERE p.dedupe_subject_key = :dedupe_subject_key
                  AND p.target_chat_id = :target_chat_id
                  AND d.delivery_status IN ('sent'::notification_status_enum, 'edited'::notification_status_enum)
                ORDER BY d.created_at DESC
                LIMIT 1
                """
            ),
            {"dedupe_subject_key": dedupe_subject_key, "target_chat_id": target_chat_id},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ExistingRecentDelivery(
            notification_plan_id=UUID(str(row["notification_plan_id"])),
            telegram_message_id=_int_or_none(row["telegram_message_id"]),
            telegram_chat_id=_int_or_none(row["telegram_chat_id"]),
            material_change_hash=str(row["material_change_hash"]),
            primary_canonical_url=_string_or_none(row["primary_canonical_url"]),
            urgency_profile=_string_or_none(row["urgency_profile"]),
            render_profile=_string_or_none(row["render_profile"]),
            created_at=row["created_at"],
        )

    async def load_successful_delivery_for_material(
        self,
        *,
        dedupe_subject_key: str,
        target_chat_id: int,
        material_change_hash: str,
    ) -> ExistingRecentDelivery | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT p.notification_plan_id,
                       p.material_change_hash,
                       p.urgency_profile,
                       p.render_profile,
                       d.telegram_message_id,
                       d.telegram_chat_id,
                       d.created_at,
                       ar.canonical_url AS primary_canonical_url
                FROM notification_delivery_records d
                JOIN notification_plans p ON p.notification_plan_id = d.notification_plan_id
                LEFT JOIN candidate_group_proposals cgp ON cgp.candidate_group_id = p.candidate_group_id
                LEFT JOIN artifact_registry ar ON ar.artifact_id = cgp.current_primary_artifact_id
                WHERE p.dedupe_subject_key = :dedupe_subject_key
                  AND p.target_chat_id = :target_chat_id
                  AND p.material_change_hash = :material_change_hash
                  AND d.delivery_status IN ('sent'::notification_status_enum, 'edited'::notification_status_enum)
                ORDER BY d.created_at DESC
                LIMIT 1
                """
            ),
            {
                "dedupe_subject_key": dedupe_subject_key,
                "target_chat_id": target_chat_id,
                "material_change_hash": material_change_hash,
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        return ExistingRecentDelivery(
            notification_plan_id=UUID(str(row["notification_plan_id"])),
            telegram_message_id=_int_or_none(row["telegram_message_id"]),
            telegram_chat_id=_int_or_none(row["telegram_chat_id"]),
            material_change_hash=str(row["material_change_hash"]),
            primary_canonical_url=_string_or_none(row["primary_canonical_url"]),
            urgency_profile=_string_or_none(row["urgency_profile"]),
            render_profile=_string_or_none(row["render_profile"]),
            created_at=row["created_at"],
        )

    async def has_previous_edit_restriction(self, *, notification_plan_id: UUID) -> bool:
        result = await self._session.execute(
            sa.text(
                """
                SELECT 1
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND transport_error_code IN ('telegram_message_cannot_be_edited', 'telegram_edit_message_not_found')
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        return result.first() is not None

    async def count_delivery_attempts(self, *, notification_plan_id: UUID) -> int:
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

    async def insert_notification_render(self, draft: NotificationRenderDraft) -> UUID | None:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO notification_renders (
                    notification_plan_id,
                    message_text,
                    entities_json,
                    link_preview_options_json,
                    reply_markup_json,
                    disable_notification,
                    protect_content,
                    parse_strategy,
                    render_hash,
                    created_at
                ) VALUES (
                    CAST(:notification_plan_id AS uuid),
                    :message_text,
                    CAST(:entities_json AS jsonb),
                    CAST(:link_preview_options_json AS jsonb),
                    CAST(:reply_markup_json AS jsonb),
                    :disable_notification,
                    :protect_content,
                    :parse_strategy,
                    :render_hash,
                    now()
                )
                ON CONFLICT ON CONSTRAINT uq_notification_renders_plan_render_hash
                DO NOTHING
                RETURNING notification_render_id
                """
            ),
            {
                "notification_plan_id": str(draft.notification_plan_id),
                "message_text": draft.message_text,
                "entities_json": _jsonb_dumps(draft.entities_json),
                "link_preview_options_json": _jsonb_dumps(draft.link_preview_options_json),
                "reply_markup_json": _jsonb_dumps(draft.reply_markup_json),
                "disable_notification": draft.disable_notification,
                "protect_content": draft.protect_content,
                "parse_strategy": draft.parse_strategy,
                "render_hash": draft.render_hash,
            },
        )
        inserted = result.scalar_one_or_none()
        return UUID(str(inserted)) if inserted else None

    async def load_notification_render_by_hash(
        self,
        *,
        notification_plan_id: UUID,
        render_hash: str,
    ) -> dict[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_render_id, notification_plan_id, render_hash, created_at
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND render_hash = :render_hash
                """
            ),
            {"notification_plan_id": str(notification_plan_id), "render_hash": render_hash},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def insert_delivery_record(
        self,
        *,
        notification_plan_id: UUID,
        result_status: str,
        telegram_chat_id: int | None,
        telegram_message_id: int | None,
        attempt_count: int,
        transport_error_code: str | None,
        transport_error_class: str | None,
        telegram_response_json: dict[str, Any] | None,
    ) -> UUID:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO notification_delivery_records (
                    notification_plan_id,
                    telegram_chat_id,
                    telegram_message_id,
                    delivery_status,
                    sent_at,
                    edited_at,
                    attempt_count,
                    transport_error_code,
                    transport_error_class,
                    telegram_response_json,
                    created_at
                ) VALUES (
                    CAST(:notification_plan_id AS uuid),
                    :telegram_chat_id,
                    :telegram_message_id,
                    CAST(:delivery_status AS notification_status_enum),
                    CASE WHEN :delivery_status = 'sent' THEN now() ELSE NULL END,
                    CASE WHEN :delivery_status = 'edited' THEN now() ELSE NULL END,
                    :attempt_count,
                    :transport_error_code,
                    :transport_error_class,
                    CAST(:telegram_response_json AS jsonb),
                    now()
                )
                RETURNING notification_delivery_record_id
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "telegram_chat_id": telegram_chat_id,
                "telegram_message_id": telegram_message_id,
                "delivery_status": result_status,
                "attempt_count": attempt_count,
                "transport_error_code": transport_error_code,
                "transport_error_class": transport_error_class,
                "telegram_response_json": _jsonb_dumps(telegram_response_json),
            },
        )
        return UUID(str(result.scalar_one()))

    async def load_suppressed_delivery_record_by_reason(
        self,
        *,
        notification_plan_id: UUID,
        transport_error_code: str,
    ) -> dict[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT notification_delivery_record_id,
                       notification_plan_id,
                       delivery_status::text AS delivery_status,
                       telegram_chat_id,
                       telegram_message_id,
                       attempt_count,
                       transport_error_code,
                       transport_error_class,
                       telegram_response_json,
                       created_at
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND delivery_status = 'suppressed'::notification_status_enum
                  AND transport_error_code = :transport_error_code
                  AND telegram_chat_id IS NULL
                  AND telegram_message_id IS NULL
                  AND telegram_response_json ->> 'dry_run' = 'true'
                ORDER BY created_at ASC, notification_delivery_record_id ASC
                LIMIT 1
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "transport_error_code": transport_error_code,
            },
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def update_plan_status(
        self,
        *,
        notification_plan_id: UUID,
        status: str,
        send_after: datetime | None = None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE notification_plans
                SET status = CAST(:status AS notification_status_enum),
                    send_after = COALESCE(CAST(:send_after AS timestamptz), send_after)
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id), "status": status, "send_after": send_after},
        )

    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: UUID,
        from_state: str | None,
        to_state: str,
        reason_code: str | None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                INSERT INTO state_transitions (
                    state_transition_id, object_type, object_id, from_state, to_state, reason_code, created_at
                ) VALUES (
                    gen_random_uuid(), :object_type, CAST(:object_id AS uuid), :from_state, :to_state, :reason_code, now()
                )
                """
            ),
            {
                "object_type": object_type,
                "object_id": str(object_id),
                "from_state": from_state,
                "to_state": to_state,
                "reason_code": reason_code,
            },
        )

    async def load_delivery_result_outbox_by_record(
        self,
        *,
        notification_plan_id: UUID,
        notification_delivery_record_id: UUID,
    ) -> dict[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status,
                       fail_count, created_at, published_at
                FROM event_outbox
                WHERE event_type = 'notification.delivery.result.v1'
                  AND dedupe_key = :dedupe_key
                  AND aggregate_type = 'notification_plan'
                  AND aggregate_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "dedupe_key": _delivery_result_dedupe_key(
                    notification_plan_id=notification_plan_id,
                    notification_delivery_record_id=notification_delivery_record_id,
                ),
            },
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def insert_delivery_result_outbox(
        self,
        *,
        notification_plan_id: UUID,
        delivery_status: str,
        telegram_chat_id: int | None,
        telegram_message_id: int | None,
        notification_delivery_record_id: UUID,
        attempt_count: int,
        transport_error_code: str | None,
        transport_error_class: str | None,
        edited: bool,
    ) -> None:
        payload = {
            "notification_plan_id": str(notification_plan_id),
            "notification_delivery_record_id": str(notification_delivery_record_id),
            "delivery_status": delivery_status,
            "telegram_chat_id": telegram_chat_id,
            "telegram_message_id": telegram_message_id,
            "attempt_count": attempt_count,
            "transport_error_code": transport_error_code,
            "transport_error_class": transport_error_class,
            "edited": edited,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status, created_at
                ) VALUES (
                    'notification.delivery.result.v1',
                    'notification_plan',
                    CAST(:notification_plan_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "dedupe_key": _delivery_result_dedupe_key(
                    notification_plan_id=notification_plan_id,
                    notification_delivery_record_id=notification_delivery_record_id,
                ),
                "payload_json": _jsonb_dumps(payload),
            },
        )

    async def insert_delivery_result_outbox_returning(
        self,
        *,
        notification_plan_id: UUID,
        delivery_status: str,
        telegram_chat_id: int | None,
        telegram_message_id: int | None,
        notification_delivery_record_id: UUID,
        attempt_count: int,
        transport_error_code: str | None,
        transport_error_class: str | None,
        edited: bool,
    ) -> UUID | None:
        payload = {
            "notification_plan_id": str(notification_plan_id),
            "notification_delivery_record_id": str(notification_delivery_record_id),
            "delivery_status": delivery_status,
            "telegram_chat_id": telegram_chat_id,
            "telegram_message_id": telegram_message_id,
            "attempt_count": attempt_count,
            "transport_error_code": transport_error_code,
            "transport_error_class": transport_error_class,
            "edited": edited,
        }
        result = await self._session.execute(
            sa.text(
                """
                WITH inserted AS (
                    INSERT INTO event_outbox (
                        event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status, created_at
                    ) VALUES (
                        'notification.delivery.result.v1',
                        'notification_plan',
                        CAST(:notification_plan_id AS uuid),
                        :dedupe_key,
                        CAST(:payload_json AS jsonb),
                        'pending'::outbox_status_enum,
                        now()
                    )
                    ON CONFLICT (dedupe_key) DO NOTHING
                    RETURNING event_id
                )
                SELECT event_id FROM inserted
                UNION ALL
                SELECT event_id
                FROM event_outbox
                WHERE dedupe_key = :dedupe_key
                LIMIT 1
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "dedupe_key": _delivery_result_dedupe_key(
                    notification_plan_id=notification_plan_id,
                    notification_delivery_record_id=notification_delivery_record_id,
                ),
                "payload_json": _jsonb_dumps(payload),
            },
        )
        inserted = result.scalar_one_or_none()
        return UUID(str(inserted)) if inserted else None

    async def load_bounded_notification_send_dry_run_readback(
        self,
        *,
        notification_plan_id: UUID,
        analysis_id: UUID,
        candidate_group_id: UUID,
        target_chat_id: int,
        dedupe_subject_key: str,
        material_change_hash: str,
        render_hash: str,
        notification_delivery_record_id: UUID,
        delivery_result_event_id: UUID,
        dry_run_reason_code: str,
    ) -> dict[str, Any]:
        return await self.load_bounded_notification_send_readback(
            notification_plan_id=notification_plan_id,
            analysis_id=analysis_id,
            candidate_group_id=candidate_group_id,
            target_chat_id=target_chat_id,
            dedupe_subject_key=dedupe_subject_key,
            material_change_hash=material_change_hash,
            render_hash=render_hash,
            notification_delivery_record_id=notification_delivery_record_id,
            delivery_result_event_id=delivery_result_event_id,
            delivery_status="suppressed",
            telegram_chat_id=None,
            telegram_message_id=None,
            attempt_count=0,
            transport_error_code=dry_run_reason_code,
            edited=False,
        )

    async def load_bounded_notification_send_readback(
        self,
        *,
        notification_plan_id: UUID,
        analysis_id: UUID,
        candidate_group_id: UUID,
        target_chat_id: int,
        dedupe_subject_key: str,
        material_change_hash: str,
        render_hash: str,
        notification_delivery_record_id: UUID,
        delivery_result_event_id: UUID,
        delivery_status: str,
        telegram_chat_id: int | None,
        telegram_message_id: int | None,
        attempt_count: int,
        transport_error_code: str | None,
        edited: bool,
    ) -> dict[str, Any]:
        dedupe_key = _delivery_result_dedupe_key(
            notification_plan_id=notification_plan_id,
            notification_delivery_record_id=notification_delivery_record_id,
        )
        notification_plan_id_text = str(notification_plan_id)
        notification_delivery_record_id_text = str(notification_delivery_record_id)
        telegram_chat_id_text = str(telegram_chat_id) if telegram_chat_id is not None else None
        telegram_message_id_text = str(telegram_message_id) if telegram_message_id is not None else None
        attempt_count_text = str(attempt_count)
        edited_text = "true" if edited else "false"
        plan_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND analysis_id = CAST(:analysis_id AS uuid)
                  AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND target_chat_id = CAST(:target_chat_id AS bigint)
                  AND dedupe_subject_key = :dedupe_subject_key
                  AND material_change_hash = :material_change_hash
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "analysis_id": str(analysis_id),
                "candidate_group_id": str(candidate_group_id),
                "target_chat_id": target_chat_id,
                "dedupe_subject_key": dedupe_subject_key,
                "material_change_hash": material_change_hash,
            },
        )
        material_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_plans
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                  AND target_chat_id = CAST(:target_chat_id AS bigint)
                  AND material_change_hash = :material_change_hash
                """
            ),
            {
                "analysis_id": str(analysis_id),
                "target_chat_id": target_chat_id,
                "material_change_hash": material_change_hash,
            },
        )
        render_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND render_hash = :render_hash
                """
            ),
            {"notification_plan_id": str(notification_plan_id), "render_hash": render_hash},
        )
        delivery_record_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_delivery_records
                WHERE notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
                  AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND delivery_status = CAST(:delivery_status AS notification_status_enum)
                  AND telegram_chat_id IS NOT DISTINCT FROM CAST(:telegram_chat_id AS bigint)
                  AND telegram_message_id IS NOT DISTINCT FROM CAST(:telegram_message_id AS bigint)
                  AND attempt_count = CAST(:attempt_count AS integer)
                  AND transport_error_code IS NOT DISTINCT FROM CAST(:transport_error_code AS text)
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "notification_delivery_record_id": str(notification_delivery_record_id),
                "delivery_status": delivery_status,
                "telegram_chat_id": telegram_chat_id,
                "telegram_message_id": telegram_message_id,
                "attempt_count": attempt_count,
                "transport_error_code": transport_error_code,
            },
        )
        event_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*) AS event_count,
                       max(status::text) AS event_status
                FROM event_outbox
                WHERE event_id = CAST(:delivery_result_event_id AS uuid)
                  AND event_type = 'notification.delivery.result.v1'
                  AND aggregate_type = 'notification_plan'
                  AND aggregate_id = CAST(:notification_plan_id AS uuid)
                  AND dedupe_key = :dedupe_key
                  AND payload_json ->> 'notification_plan_id' = :notification_plan_id_text
                  AND payload_json ->> 'notification_delivery_record_id' = :notification_delivery_record_id_text
                  AND payload_json ->> 'delivery_status' = :delivery_status
                  AND payload_json ->> 'attempt_count' = :attempt_count_text
                  AND (
                      (CAST(:telegram_chat_id_text AS text) IS NULL AND payload_json ->> 'telegram_chat_id' IS NULL)
                      OR payload_json ->> 'telegram_chat_id' = CAST(:telegram_chat_id_text AS text)
                  )
                  AND (
                      (CAST(:telegram_message_id_text AS text) IS NULL AND payload_json ->> 'telegram_message_id' IS NULL)
                      OR payload_json ->> 'telegram_message_id' = CAST(:telegram_message_id_text AS text)
                  )
                  AND (
                      (CAST(:transport_error_code AS text) IS NULL AND payload_json ->> 'transport_error_code' IS NULL)
                      OR payload_json ->> 'transport_error_code' = CAST(:transport_error_code AS text)
                  )
                  AND payload_json ->> 'edited' = :edited_text
                """
            ),
            {
                "delivery_result_event_id": str(delivery_result_event_id),
                "notification_plan_id": str(notification_plan_id),
                "notification_plan_id_text": notification_plan_id_text,
                "notification_delivery_record_id_text": notification_delivery_record_id_text,
                "dedupe_key": dedupe_key,
                "delivery_status": delivery_status,
                "attempt_count_text": attempt_count_text,
                "telegram_chat_id_text": telegram_chat_id_text,
                "telegram_message_id_text": telegram_message_id_text,
                "transport_error_code": transport_error_code,
                "edited_text": edited_text,
            },
        )
        event_row = event_result.mappings().first()
        return {
            "notification_plan_count": int(plan_result.scalar_one()),
            "notification_plan_material_count": int(material_result.scalar_one()),
            "notification_render_count": int(render_result.scalar_one()),
            "notification_delivery_record_count": int(delivery_record_result.scalar_one()),
            "notification_delivery_result_event_count": int(event_row["event_count"] or 0) if event_row else 0,
            "delivery_result_event_status": str(event_row["event_status"]) if event_row and event_row["event_status"] else None,
        }

    async def load_send_disabled_worker_once_proof_verification(
        self,
        *,
        notification_plan_id: UUID,
    ) -> dict[str, Any]:
        plan_result = await self._session.execute(
            sa.text(
                """
                SELECT status
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        render_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        delivery_count_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        delivery_result = await self._session.execute(
            sa.text(
                """
                SELECT delivery_status, attempt_count, transport_error_code, telegram_response_json
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        transition_result = await self._session.execute(
            sa.text(
                """
                SELECT st.reason_code
                FROM state_transitions st
                JOIN notification_plans np
                  ON np.notification_plan_id = CAST(:notification_plan_id AS uuid)
                WHERE st.object_type = 'notification_plan'
                  AND st.object_id = np.notification_plan_id
                ORDER BY st.created_at DESC,
                         CASE
                           WHEN st.to_state = np.status::text THEN 100
                           WHEN st.to_state IN ('sent', 'edited', 'suppressed', 'failed_retryable', 'failed_terminal') THEN 90
                           WHEN st.to_state = 'queued' THEN 20
                           WHEN st.to_state = 'rendered' THEN 10
                           WHEN st.to_state = 'planned' THEN 0
                           ELSE -1
                         END DESC
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        outbox_result = await self._session.execute(
            sa.text(
                """
                SELECT 1
                FROM event_outbox
                WHERE event_type = 'notification.delivery.result.v1'
                  AND aggregate_type = 'notification_plan'
                  AND aggregate_id = CAST(:notification_plan_id AS uuid)
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )

        delivery_row = delivery_result.mappings().first()
        return {
            "proof_plan_final_status": plan_result.scalar_one_or_none(),
            "notification_render_count": int(render_result.scalar_one()),
            "notification_delivery_record_count": int(delivery_count_result.scalar_one()),
            "delivery_status": str(delivery_row["delivery_status"]) if delivery_row else None,
            "attempt_count": int(delivery_row["attempt_count"]) if delivery_row else None,
            "transport_error_code": str(delivery_row["transport_error_code"]) if delivery_row else None,
            "telegram_response_json": _json_loads(delivery_row["telegram_response_json"]) if delivery_row else None,
            "latest_state_transition_reason_code": transition_result.scalar_one_or_none(),
            "delivery_result_outbox_exists": outbox_result.first() is not None,
        }

    async def load_restricted_live_worker_once_proof_verification(
        self,
        *,
        notification_plan_id: UUID,
    ) -> dict[str, Any]:
        plan_result = await self._session.execute(
            sa.text(
                """
                SELECT status
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        render_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        delivery_count_result = await self._session.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        delivery_result = await self._session.execute(
            sa.text(
                """
                SELECT delivery_status, attempt_count, transport_error_code,
                       telegram_chat_id, telegram_message_id
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        transition_result = await self._session.execute(
            sa.text(
                """
                SELECT st.to_state, st.reason_code
                FROM state_transitions st
                JOIN notification_plans np
                  ON np.notification_plan_id = CAST(:notification_plan_id AS uuid)
                WHERE st.object_type = 'notification_plan'
                  AND st.object_id = np.notification_plan_id
                ORDER BY st.created_at DESC,
                         CASE
                           WHEN st.to_state = np.status::text THEN 100
                           WHEN st.to_state IN ('sent', 'edited', 'suppressed', 'failed_retryable', 'failed_terminal') THEN 90
                           WHEN st.to_state = 'queued' THEN 20
                           WHEN st.to_state = 'rendered' THEN 10
                           WHEN st.to_state = 'planned' THEN 0
                           ELSE -1
                         END DESC
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        outbox_result = await self._session.execute(
            sa.text(
                """
                SELECT 1
                FROM event_outbox
                WHERE event_type = 'notification.delivery.result.v1'
                  AND aggregate_type = 'notification_plan'
                  AND aggregate_id = CAST(:notification_plan_id AS uuid)
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )

        delivery_row = delivery_result.mappings().first()
        transition_row = transition_result.mappings().first()
        return {
            "proof_plan_final_status": plan_result.scalar_one_or_none(),
            "notification_render_count": int(render_result.scalar_one()),
            "notification_delivery_record_count": int(delivery_count_result.scalar_one()),
            "delivery_status": str(delivery_row["delivery_status"]) if delivery_row else None,
            "attempt_count": int(delivery_row["attempt_count"]) if delivery_row else None,
            "transport_error_code": (
                str(delivery_row["transport_error_code"])
                if delivery_row and delivery_row["transport_error_code"]
                else None
            ),
            "telegram_chat_id_present": bool(delivery_row and delivery_row["telegram_chat_id"] is not None),
            "telegram_message_id_present": bool(delivery_row and delivery_row["telegram_message_id"] is not None),
            "latest_state_transition_to_state": str(transition_row["to_state"]) if transition_row else None,
            "latest_state_transition_reason_code": str(transition_row["reason_code"]) if transition_row else None,
            "delivery_result_outbox_exists": outbox_result.first() is not None,
        }


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


def _datetime_or_none(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except ValueError:
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


def _delivery_result_dedupe_key(*, notification_plan_id: UUID, notification_delivery_record_id: UUID) -> str:
    return f"notification-delivery-result:{notification_plan_id}:{notification_delivery_record_id}"


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [value]
    return []
