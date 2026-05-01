from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import UUID, uuid4

from services.notifier_telegram.config import NotifierTelegramConfig
from services.notifier_telegram.models import (
    AnalysisRenderContext,
    CandidateRenderContext,
    ExistingRecentDelivery,
    JudgeOutputRenderContext,
    NotificationIntentJob,
    NotificationPlanDraft,
    NotificationRenderDraft,
    StreamMessage,
)
from services.notifier_telegram.service import NotifierTelegramService


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.jobs: dict[UUID, NotificationIntentJob] = {}
        self.analyses: dict[UUID, AnalysisRenderContext] = {}
        self.judge_outputs: dict[UUID, JudgeOutputRenderContext] = {}
        self.candidates: dict[UUID, CandidateRenderContext] = {}
        self.plans: dict[UUID, NotificationPlanDraft] = {}
        self.renders: list[NotificationRenderDraft] = []
        self.delivery_records: list[dict] = []
        self.state_transitions: list[dict] = []
        self.delivery_outbox: list[dict] = []
        self.loaded_trigger_ids: list[UUID] = []
        self.operations: list[str] = []
        self.previous_edit_restrictions: set[UUID] = set()

    def transaction(self):
        return Tx()

    async def load_intent_job(self, trigger_event_id: UUID):
        self.loaded_trigger_ids.append(trigger_event_id)
        return self.jobs.get(trigger_event_id)

    async def load_notification_plan(self, notification_plan_id: UUID):
        plan = self.plans.get(notification_plan_id)
        return _plan_row(plan) if plan else None

    async def load_existing_plan_by_material(self, *, analysis_id: UUID, target_chat_id: int, material_change_hash: str):
        for plan in self.plans.values():
            if (
                plan.analysis_id == analysis_id
                and plan.target_chat_id == target_chat_id
                and plan.material_change_hash == material_change_hash
            ):
                return _plan_row(plan)
        return None

    async def insert_notification_plan(self, draft: NotificationPlanDraft) -> UUID:
        existing = await self.load_existing_plan_by_material(
            analysis_id=draft.analysis_id,
            target_chat_id=draft.target_chat_id,
            material_change_hash=draft.material_change_hash,
        )
        if existing is not None:
            return UUID(str(existing["notification_plan_id"]))
        self.plans[draft.notification_plan_id] = draft
        self.operations.append("plan")
        return draft.notification_plan_id

    async def load_analysis(self, analysis_id: UUID):
        return self.analyses.get(analysis_id)

    async def load_judge_output_render_fields(self, judge_output_id: UUID):
        return self.judge_outputs.get(judge_output_id)

    async def load_candidate_render_context(self, candidate_group_id: UUID):
        return self.candidates.get(candidate_group_id)

    async def load_recent_successful_delivery(self, *, dedupe_subject_key: str, target_chat_id: int):
        successful = []
        for record in self.delivery_records:
            if record["result_status"] not in {"sent", "edited"}:
                continue
            plan = self.plans.get(record["notification_plan_id"])
            if plan is None:
                continue
            if plan.dedupe_subject_key == dedupe_subject_key and plan.target_chat_id == target_chat_id:
                successful.append((record, plan))
        if not successful:
            return None
        record, plan = successful[-1]
        candidate = self.candidates.get(plan.candidate_group_id)
        return ExistingRecentDelivery(
            notification_plan_id=plan.notification_plan_id,
            telegram_message_id=record.get("telegram_message_id"),
            telegram_chat_id=record.get("telegram_chat_id"),
            material_change_hash=plan.material_change_hash,
            primary_canonical_url=candidate.primary_canonical_url if candidate else None,
            urgency_profile=plan.urgency_profile,
            render_profile=plan.render_profile,
            created_at=record.get("created_at") or datetime.now(timezone.utc),
        )

    async def load_successful_delivery_for_material(
        self,
        *,
        dedupe_subject_key: str,
        target_chat_id: int,
        material_change_hash: str,
    ):
        successful = []
        for record in self.delivery_records:
            if record["result_status"] not in {"sent", "edited"}:
                continue
            plan = self.plans.get(record["notification_plan_id"])
            if plan is None:
                continue
            if (
                plan.dedupe_subject_key == dedupe_subject_key
                and plan.target_chat_id == target_chat_id
                and plan.material_change_hash == material_change_hash
            ):
                successful.append((record, plan))
        if not successful:
            return None
        record, plan = successful[-1]
        candidate = self.candidates.get(plan.candidate_group_id)
        return ExistingRecentDelivery(
            notification_plan_id=plan.notification_plan_id,
            telegram_message_id=record.get("telegram_message_id"),
            telegram_chat_id=record.get("telegram_chat_id"),
            material_change_hash=plan.material_change_hash,
            primary_canonical_url=candidate.primary_canonical_url if candidate else None,
            urgency_profile=plan.urgency_profile,
            render_profile=plan.render_profile,
            created_at=record.get("created_at") or datetime.now(timezone.utc),
        )

    async def has_previous_edit_restriction(self, *, notification_plan_id: UUID) -> bool:
        return notification_plan_id in self.previous_edit_restrictions

    async def count_delivery_attempts(self, *, notification_plan_id: UUID) -> int:
        return sum(1 for record in self.delivery_records if record["notification_plan_id"] == notification_plan_id)

    async def insert_notification_render(self, draft: NotificationRenderDraft):
        for existing in self.renders:
            if (
                existing.notification_plan_id == draft.notification_plan_id
                and existing.render_hash == draft.render_hash
            ):
                return None
        self.renders.append(draft)
        self.operations.append("render")
        return uuid4()

    async def insert_delivery_record(self, **kwargs):
        record_id = uuid4()
        self.delivery_records.append(
            {"notification_delivery_record_id": record_id, "created_at": datetime.now(timezone.utc), **kwargs}
        )
        self.operations.append("delivery_record")
        return record_id

    async def update_plan_status(self, *, notification_plan_id: UUID, status: str, send_after=None) -> None:
        plan = self.plans.get(notification_plan_id)
        if plan is not None:
            self.plans[notification_plan_id] = replace(plan, status=status, send_after=send_after or plan.send_after)
        self.operations.append(f"status:{status}")

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)
        self.operations.append("state_transition")

    async def insert_delivery_result_outbox(self, **kwargs) -> None:
        self.delivery_outbox.append(kwargs)
        self.operations.append("delivery_outbox")


class FakeConsumer:
    def __init__(self, messages: list[StreamMessage]) -> None:
        self.messages = messages
        self.acked: list[str] = []
        self.ensure_group_called = False

    async def ensure_group(self) -> None:
        self.ensure_group_called = True

    async def read_batch(self):
        return self.messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class RaisingTelegramClient:
    def __init__(self) -> None:
        self.calls = 0

    async def send_message(self, **kwargs):
        self.calls += 1
        raise AssertionError("telegram client must not be called")


def config(
    *,
    dry_run: bool = True,
    enable_notification_send: bool = False,
    allow_edits: bool = False,
) -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        telegram_bot_token="token" if enable_notification_send else "",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        dry_run=dry_run,
        allow_edits=allow_edits,
        enable_notification_send=enable_notification_send,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10,
        log_level="INFO",
    )


def repo_with_valid_case() -> tuple[FakeRepository, NotificationIntentJob]:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    analysis_id = uuid4()
    candidate_group_id = uuid4()
    judge_output_id = uuid4()
    plan_id = uuid4()
    intent = NotificationIntentJob(
        trigger_event_id=trigger_event_id,
        event_type="notification.plan.created.v1",
        notification_plan_id=plan_id,
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key=str(candidate_group_id),
        material_change_hash="material-1",
        send_after=None,
        suppress_reason_code=None,
    )
    repository.jobs[trigger_event_id] = intent
    repository.analyses[analysis_id] = AnalysisRenderContext(
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        judge_output_id=judge_output_id,
        verdict="inspect_now",
        delivery_decision="send_now",
        reason_codes_json=["repo_has_clear_scope"],
        evidence_limitations_ko="public evidence only",
        recommended_action_ko="inspect repository",
        freshness_note_ko="fresh",
    )
    repository.judge_outputs[judge_output_id] = JudgeOutputRenderContext(
        judge_output_id=judge_output_id,
        payload_json={
            "headline": "Useful repo",
            "summary_one_line_ko": "clear utility",
            "skeptical_take_ko": "check maintenance first",
            "why_it_might_matter": "could save triage time",
        },
        model_confidence_band="medium",
    )
    repository.candidates[candidate_group_id] = CandidateRenderContext(
        candidate_group_id=candidate_group_id,
        source_message_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        primary_artifact_type="github_repo",
        primary_canonical_url="https://github.com/example/repo",
        primary_canonical_id="github.com/example/repo",
        source_message_link="https://t.me/c/1/2",
        source_text_surface="source text",
    )
    return repository, intent


def service(repository: FakeRepository, *, cfg: NotifierTelegramConfig | None = None, client=None) -> NotifierTelegramService:
    return NotifierTelegramService(
        cfg or config(),
        repository=repository,
        telegram_client=client if client is not None else RaisingTelegramClient(),
    )


def _plan_row(plan: NotificationPlanDraft) -> dict:
    return {
        "notification_plan_id": plan.notification_plan_id,
        "analysis_id": plan.analysis_id,
        "target_chat_id": plan.target_chat_id,
        "target_thread_id": plan.target_thread_id,
        "render_profile": plan.render_profile,
        "dedupe_subject_key": plan.dedupe_subject_key,
        "material_change_hash": plan.material_change_hash,
        "send_after": plan.send_after,
        "status": plan.status,
    }
