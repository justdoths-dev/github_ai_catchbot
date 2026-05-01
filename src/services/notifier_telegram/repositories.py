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
                SELECT notification_plan_id, analysis_id, candidate_group_id, target_chat_id,
                       target_thread_id, render_profile, dedupe_subject_key,
                       material_change_hash, send_after, status
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        )
        row = result.mappings().first()
        return dict(row) if row else None

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
                SELECT notification_plan_id, analysis_id, candidate_group_id, target_chat_id,
                       target_thread_id, render_profile, dedupe_subject_key,
                       material_change_hash, send_after, status
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
                "dedupe_key": f"notification-delivery-result:{notification_plan_id}:{notification_delivery_record_id}",
                "payload_json": _jsonb_dumps(payload),
            },
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
