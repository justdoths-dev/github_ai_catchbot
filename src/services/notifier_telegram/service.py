from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Protocol
from uuid import UUID

from .config import NotifierTelegramConfig
from .models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    DeliveryAction,
    DeliveryResult,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotificationPlanDraft,
    NotificationRenderDraft,
)
from .renderer import NotificationRenderer, RenderInput
from .telegram_client import (
    TelegramBotClient,
    TelegramTransportNoopError,
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
)


class NotifierTelegramRepositoryProtocol(Protocol):
    def transaction(self): ...
    async def load_intent_job(self, trigger_event_id: UUID) -> NotificationIntentJob | None: ...
    async def load_notification_plan(self, notification_plan_id: UUID) -> dict | None: ...
    async def load_existing_plan_by_material(
        self, *, analysis_id: UUID, target_chat_id: int, material_change_hash: str
    ) -> dict | None: ...
    async def insert_notification_plan(self, draft: NotificationPlanDraft) -> UUID: ...
    async def load_analysis(self, analysis_id: UUID) -> AnalysisRenderContext | None: ...
    async def load_judge_output_render_fields(self, judge_output_id: UUID) -> JudgeOutputRenderContext | None: ...
    async def load_candidate_render_context(self, candidate_group_id: UUID) -> CandidateRenderContext | None: ...
    async def load_successful_delivery_for_material(
        self, *, dedupe_subject_key: str, target_chat_id: int, material_change_hash: str
    ): ...
    async def load_recent_successful_delivery(self, *, dedupe_subject_key: str, target_chat_id: int): ...
    async def has_previous_edit_restriction(self, *, notification_plan_id: UUID) -> bool: ...
    async def count_delivery_attempts(self, *, notification_plan_id: UUID) -> int: ...
    async def insert_notification_render(self, draft: NotificationRenderDraft) -> UUID | None: ...
    async def insert_delivery_record(self, **kwargs) -> UUID: ...
    async def update_plan_status(
        self, *, notification_plan_id: UUID, status: str, send_after: datetime | None = None
    ) -> None: ...
    async def insert_state_transition(self, **kwargs) -> None: ...
    async def insert_delivery_result_outbox(self, **kwargs) -> None: ...


class NotifierTelegramService:
    def __init__(
        self,
        config: NotifierTelegramConfig,
        *,
        repository: NotifierTelegramRepositoryProtocol,
        renderer: NotificationRenderer | None = None,
        telegram_client: TelegramBotClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._renderer = renderer or NotificationRenderer(max_message_chars=config.max_message_chars)
        self._telegram_client = telegram_client
        self._logger = logger or logging.getLogger(__name__)

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None:
        intent = await self.rehydrate_intent(trigger_event_id)
        if intent is None:
            return
        await self.handle_intent(intent)

    async def rehydrate_intent(self, trigger_event_id: str | UUID) -> NotificationIntentJob | None:
        try:
            parsed = UUID(str(trigger_event_id))
        except (TypeError, ValueError, AttributeError):
            self._logger.warning("notifier_telegram_invalid_trigger_event_id")
            return None
        return await self._repository.load_intent_job(parsed)

    async def handle_intent(self, intent: NotificationIntentJob) -> None:
        analysis = await self._repository.load_analysis(intent.analysis_id)
        if analysis is None or analysis.candidate_group_id != intent.candidate_group_id:
            await self._transition(intent, to_state="failed_terminal", reason_code="notification_intent_context_mismatch")
            return
        if analysis.delivery_decision != intent.delivery_decision:
            await self._transition(intent, to_state="failed_terminal", reason_code="notification_delivery_decision_mismatch")
            return
        if intent.delivery_decision == "suppress" or not intent.target_chat_id:
            await self._concretize_plan(intent, status="suppressed")
            await self._transition(intent, to_state="suppressed", reason_code=intent.suppress_reason_code or "notification_suppressed")
            return
        if intent.delivery_decision == "send_digest" or (
            intent.urgency_profile == "digest" and not self._config.enable_digest_runtime
        ):
            await self._concretize_plan(intent, status="suppressed")
            await self._transition(intent, to_state="suppressed", reason_code="notification_digest_deferred")
            return

        judge_output = await self._repository.load_judge_output_render_fields(analysis.judge_output_id)
        candidate = await self._repository.load_candidate_render_context(intent.candidate_group_id)
        if candidate is None:
            await self._transition(intent, to_state="failed_terminal", reason_code="notification_missing_candidate_render_context")
            return

        plan_id = await self._concretize_plan(intent, status="planned")
        if plan_id != intent.notification_plan_id:
            intent = replace(intent, notification_plan_id=plan_id)

        plan_row = await self._repository.load_notification_plan(intent.notification_plan_id)
        send_after = _effective_send_after(plan_row, intent)
        if _is_future(send_after):
            await self._transition(
                intent,
                to_state=str(plan_row.get("status") if plan_row else "planned"),
                reason_code="notification_send_after_deferred",
            )
            return

        action = await self.decide_delivery_action(intent, candidate=candidate)
        if action.mode == "noop":
            await self._transition(
                intent,
                to_state=str(plan_row.get("status") if plan_row else "planned"),
                reason_code=action.reason_code or "notification_noop",
            )
            return
        if _should_terminal_duplicate_noop(plan_row, transport_enabled=self._config.transport_enabled):
            await self._transition(
                intent,
                to_state=str(plan_row.get("status")),
                reason_code="notification_duplicate_terminal_noop",
            )
            return

        render = self._renderer.render(
            notification_plan_id=intent.notification_plan_id,
            payload=RenderInput(
                analysis=analysis,
                judge_output=judge_output,
                candidate=candidate,
                urgency_profile=intent.urgency_profile,
            ),
        )

        async with self._repository.transaction():
            await self._repository.insert_notification_render(render)
            await self._repository.update_plan_status(notification_plan_id=intent.notification_plan_id, status="rendered")
            await self._repository.insert_state_transition(
                object_type="notification_plan",
                object_id=intent.notification_plan_id,
                from_state=str(plan_row.get("status") if plan_row else "planned"),
                to_state="rendered",
                reason_code="notification_rendered",
            )

        queued = False
        if self._config.transport_enabled:
            queued = True
            async with self._repository.transaction():
                await self._repository.update_plan_status(notification_plan_id=intent.notification_plan_id, status="queued")
                await self._repository.insert_state_transition(
                    object_type="notification_plan",
                    object_id=intent.notification_plan_id,
                    from_state="rendered",
                    to_state="queued",
                    reason_code="notification_transport_queued",
                )

        result = await self._perform_delivery(intent=intent, render=render, action=action)
        next_retry_at = _next_retry_at(result.retry_after_seconds) if result.delivery_status == "failed_retryable" else None

        async with self._repository.transaction():
            await self._repository.update_plan_status(
                notification_plan_id=intent.notification_plan_id,
                status=result.delivery_status,
                send_after=next_retry_at,
            )
            record_id = await self._repository.insert_delivery_record(
                notification_plan_id=intent.notification_plan_id,
                result_status=result.delivery_status,
                telegram_chat_id=result.telegram_chat_id,
                telegram_message_id=result.telegram_message_id,
                attempt_count=result.attempt_count,
                transport_error_code=result.transport_error_code,
                transport_error_class=result.transport_error_class,
                telegram_response_json=result.telegram_response_json,
            )
            await self._repository.insert_state_transition(
                object_type="notification_plan",
                object_id=intent.notification_plan_id,
                from_state="queued" if queued else "rendered",
                to_state=result.delivery_status,
                reason_code=result.transport_error_code or action.reason_code or "notification_delivery_result",
            )
            await self._repository.insert_delivery_result_outbox(
                notification_plan_id=intent.notification_plan_id,
                delivery_status=result.delivery_status,
                telegram_chat_id=result.telegram_chat_id,
                telegram_message_id=result.telegram_message_id,
                notification_delivery_record_id=record_id,
                attempt_count=result.attempt_count,
                transport_error_code=result.transport_error_code,
                transport_error_class=result.transport_error_class,
                edited=result.edited,
            )

    async def decide_delivery_action(
        self,
        intent: NotificationIntentJob,
        *,
        candidate: CandidateRenderContext | None = None,
    ) -> DeliveryAction:
        if intent.delivery_decision != "send_now":
            return DeliveryAction(mode="noop", reason_code="notification_not_immediate_send")
        existing_material = await self._repository.load_successful_delivery_for_material(
            dedupe_subject_key=intent.dedupe_subject_key,
            target_chat_id=intent.target_chat_id,
            material_change_hash=intent.material_change_hash,
        )
        if existing_material is not None:
            return DeliveryAction(mode="noop", reason_code="notification_duplicate_noop")
        recent = await self._repository.load_recent_successful_delivery(
            dedupe_subject_key=intent.dedupe_subject_key,
            target_chat_id=intent.target_chat_id,
        )
        if recent is None:
            return DeliveryAction(mode="send", reason_code="notification_no_recent_delivery")
        if not self._config.allow_edits:
            return DeliveryAction(mode="send", reason_code="notification_edits_disabled")
        if recent.telegram_message_id is None:
            return DeliveryAction(mode="send", reason_code="notification_missing_recent_message_id")
        if _urgency_escalates_to_high(recent.urgency_profile, intent.urgency_profile):
            return DeliveryAction(mode="send", reason_code="notification_urgency_escalation_new_send")
        current_primary_url = candidate.primary_canonical_url if candidate else None
        if recent.primary_canonical_url != current_primary_url:
            return DeliveryAction(mode="send", reason_code="notification_primary_subject_changed")
        if recent.render_profile != intent.render_profile:
            return DeliveryAction(mode="send", reason_code="notification_render_profile_changed")
        if _edit_window_exceeded(recent.created_at, self._config.edit_window_minutes):
            return DeliveryAction(mode="send", reason_code="notification_edit_window_exceeded")
        if await self._repository.has_previous_edit_restriction(notification_plan_id=recent.notification_plan_id):
            return DeliveryAction(mode="send", reason_code="notification_previous_edit_restriction")
        return DeliveryAction(
            mode="edit",
            existing_message_id=recent.telegram_message_id,
            reason_code="notification_edit_existing_message",
        )

    async def _concretize_plan(self, intent: NotificationIntentJob, *, status: str) -> UUID:
        existing = await self._repository.load_notification_plan(intent.notification_plan_id)
        if existing is not None:
            return UUID(str(existing["notification_plan_id"]))
        material_existing = await self._repository.load_existing_plan_by_material(
            analysis_id=intent.analysis_id,
            target_chat_id=intent.target_chat_id,
            material_change_hash=intent.material_change_hash,
        )
        if material_existing is not None:
            return UUID(str(material_existing["notification_plan_id"]))
        async with self._repository.transaction():
            return await self._repository.insert_notification_plan(
                NotificationPlanDraft(
                    notification_plan_id=intent.notification_plan_id,
                    analysis_id=intent.analysis_id,
                    candidate_group_id=intent.candidate_group_id,
                    delivery_decision=intent.delivery_decision,
                    urgency_profile=intent.urgency_profile,
                    target_chat_id=intent.target_chat_id,
                    target_thread_id=intent.target_thread_id,
                    render_profile=intent.render_profile,
                    dedupe_subject_key=intent.dedupe_subject_key,
                    material_change_hash=intent.material_change_hash,
                    send_after=intent.send_after,
                    suppress_reason_code=intent.suppress_reason_code,
                    status=status,
                )
            )

    async def _perform_delivery(
        self,
        *,
        intent: NotificationIntentJob,
        render: NotificationRenderDraft,
        action: DeliveryAction,
    ) -> DeliveryResult:
        if action.mode == "noop":
            return DeliveryResult(
                delivery_status="suppressed",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=0,
                transport_error_code=action.reason_code,
                transport_error_class=None,
            )
        if not self._config.transport_enabled:
            reason_code = "dry_run_skip_transport" if self._config.dry_run else "notification_send_flag_disabled"
            return DeliveryResult(
                delivery_status="suppressed",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=0,
                transport_error_code=reason_code,
                transport_error_class=None,
                telegram_response_json={
                    "dry_run": self._config.dry_run,
                    "send_disabled": not self._config.enable_notification_send,
                    "send_enabled": self._config.enable_notification_send,
                    "transport_skipped": True,
                    "reason_code": reason_code,
                    "delivery_action": action.mode,
                },
            )
        if self._telegram_client is None:
            return DeliveryResult(
                delivery_status="failed_terminal",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=0,
                transport_error_code="telegram_client_missing",
                transport_error_class="ConfigurationError",
            )
        try:
            if action.mode == "edit":
                if action.existing_message_id is None:
                    raise TelegramTransportTerminalError(
                        "missing message id for edit",
                        error_code="telegram_edit_message_not_found",
                    )
                response = await self._telegram_client.edit_message_text(
                    chat_id=intent.target_chat_id,
                    message_id=action.existing_message_id,
                    text=render.message_text,
                    entities=render.entities_json,
                    reply_markup=render.reply_markup_json,
                    link_preview_options=render.link_preview_options_json,
                )
                message = response.get("result") if isinstance(response, dict) else {}
                return DeliveryResult(
                    delivery_status="edited",
                    telegram_chat_id=_extract_chat_id(message, intent.target_chat_id),
                    telegram_message_id=_extract_message_id(message) or action.existing_message_id,
                    attempt_count=await self._next_attempt_count(intent.notification_plan_id),
                    telegram_response_json=response,
                    edited=True,
                )
            response = await self._telegram_client.send_message(
                chat_id=intent.target_chat_id,
                text=render.message_text,
                entities=render.entities_json,
                reply_markup=render.reply_markup_json,
                disable_notification=render.disable_notification,
                link_preview_options=render.link_preview_options_json,
                message_thread_id=intent.target_thread_id,
            )
            message = response.get("result") if isinstance(response, dict) else {}
            return DeliveryResult(
                delivery_status="sent",
                telegram_chat_id=_extract_chat_id(message, intent.target_chat_id),
                telegram_message_id=_extract_message_id(message),
                attempt_count=await self._next_attempt_count(intent.notification_plan_id),
                telegram_response_json=response,
            )
        except TelegramTransportNoopError as exc:
            return DeliveryResult(
                delivery_status="suppressed",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=action.existing_message_id,
                attempt_count=await self._next_attempt_count(intent.notification_plan_id),
                transport_error_code=getattr(exc, "error_code", "telegram_edit_not_modified_noop"),
                transport_error_class=type(exc).__name__,
                telegram_response_json={"transport_noop": True, "description": str(exc)},
            )
        except TelegramTransportRetryableError as exc:
            previous_attempts = await self._count_attempts(intent.notification_plan_id)
            retry_after_seconds = _retry_after_seconds(exc, previous_attempts)
            return DeliveryResult(
                delivery_status="failed_retryable",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=previous_attempts + 1,
                transport_error_code=getattr(exc, "error_code", "telegram_retryable"),
                transport_error_class=type(exc).__name__,
                telegram_response_json={"description": str(exc), "retry_after_seconds": retry_after_seconds},
                retry_after_seconds=retry_after_seconds,
            )
        except TelegramTransportTerminalError as exc:
            return DeliveryResult(
                delivery_status="failed_terminal",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=await self._next_attempt_count(intent.notification_plan_id),
                transport_error_code=getattr(exc, "error_code", "telegram_terminal"),
                transport_error_class=type(exc).__name__,
                telegram_response_json={"description": str(exc)},
            )

    async def _next_attempt_count(self, notification_plan_id: UUID) -> int:
        return (await self._count_attempts(notification_plan_id)) + 1

    async def _count_attempts(self, notification_plan_id: UUID) -> int:
        return await self._repository.count_delivery_attempts(notification_plan_id=notification_plan_id)

    async def _transition(self, intent: NotificationIntentJob, *, to_state: str, reason_code: str) -> None:
        async with self._repository.transaction():
            await self._repository.insert_state_transition(
                object_type="notification_plan",
                object_id=intent.notification_plan_id,
                from_state=None,
                to_state=to_state,
                reason_code=reason_code,
            )


def _extract_chat_id(message: object, fallback: int) -> int:
    if isinstance(message, dict):
        chat = message.get("chat")
        if isinstance(chat, dict) and chat.get("id") is not None:
            return int(chat["id"])
    return fallback


def _extract_message_id(message: object) -> int | None:
    if isinstance(message, dict) and message.get("message_id") is not None:
        return int(message["message_id"])
    return None


def _effective_send_after(plan_row: dict | None, intent: NotificationIntentJob) -> datetime | None:
    if plan_row is not None and plan_row.get("send_after") is not None:
        value = plan_row["send_after"]
        if isinstance(value, datetime):
            return value
    return intent.send_after


def _is_future(value: datetime | None) -> bool:
    if value is None:
        return False
    return _as_utc(value) > datetime.now(timezone.utc)


def _should_terminal_duplicate_noop(plan_row: dict | None, *, transport_enabled: bool) -> bool:
    if plan_row is None:
        return False
    status = str(plan_row.get("status") or "")
    if status in {"sent", "edited"}:
        return True
    if status in {"suppressed", "failed_terminal"} and not transport_enabled:
        return True
    return False


def _edit_window_exceeded(created_at: datetime, edit_window_minutes: int) -> bool:
    return datetime.now(timezone.utc) - _as_utc(created_at) > timedelta(minutes=edit_window_minutes)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _urgency_escalates_to_high(existing: str | None, incoming: str | None) -> bool:
    return incoming == "high" and existing != "high"


def _retry_after_seconds(exc: TelegramTransportRetryableError, previous_attempts: int) -> int:
    if exc.retry_after_seconds is not None:
        return max(0, exc.retry_after_seconds)
    return min(300, 30 * (2 ** max(0, previous_attempts)))


def _next_retry_at(retry_after_seconds: int | None) -> datetime | None:
    if retry_after_seconds is None:
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=retry_after_seconds)
