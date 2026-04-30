from __future__ import annotations

import logging
from dataclasses import replace
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
from .telegram_client import TelegramBotClient, TelegramTransportRetryableError, TelegramTransportTerminalError


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
    async def insert_notification_render(self, draft: NotificationRenderDraft) -> UUID | None: ...
    async def insert_delivery_record(self, **kwargs) -> UUID: ...
    async def update_plan_status(self, *, notification_plan_id: UUID, status: str) -> None: ...
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
        if intent.delivery_decision == "send_digest":
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

        render = self._renderer.render(
            notification_plan_id=intent.notification_plan_id,
            payload=RenderInput(
                analysis=analysis,
                judge_output=judge_output,
                candidate=candidate,
                urgency_profile=intent.urgency_profile,
            ),
        )
        action = await self.decide_delivery_action(intent)
        result = await self._perform_delivery(intent=intent, render=render, action=action)

        async with self._repository.transaction():
            await self._repository.insert_notification_render(render)
            await self._repository.update_plan_status(notification_plan_id=intent.notification_plan_id, status=result.delivery_status)
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
                from_state="planned",
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

    async def decide_delivery_action(self, intent: NotificationIntentJob) -> DeliveryAction:
        if intent.delivery_decision != "send_now":
            return DeliveryAction(mode="noop", reason_code="notification_not_immediate_send")
        return DeliveryAction(mode="send")

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
            reason_code = "telegram_dry_run" if self._config.dry_run else "telegram_send_disabled"
            return DeliveryResult(
                delivery_status="suppressed",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=0,
                transport_error_code=reason_code,
                transport_error_class=None,
                telegram_response_json={
                    "dry_run": self._config.dry_run,
                    "send_enabled": self._config.enable_notification_send,
                    "transport_skipped": True,
                    "reason_code": reason_code,
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
                attempt_count=1,
                telegram_response_json=response,
            )
        except TelegramTransportRetryableError as exc:
            return DeliveryResult(
                delivery_status="failed_retryable",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=1,
                transport_error_code="telegram_retryable",
                transport_error_class=type(exc).__name__,
            )
        except TelegramTransportTerminalError as exc:
            return DeliveryResult(
                delivery_status="failed_terminal",
                telegram_chat_id=intent.target_chat_id,
                telegram_message_id=None,
                attempt_count=1,
                transport_error_code="telegram_terminal",
                transport_error_class=type(exc).__name__,
            )

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
