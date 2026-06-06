from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

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
from services.notifier_telegram.worker import NotifierTelegramWorker


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "upstream"


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self.message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self):
        return [self.message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class TripwireTelegramClient:
    def __init__(self) -> None:
        self.send_calls = 0
        self.edit_calls = 0

    async def send_message(self, **kwargs):
        self.send_calls += 1
        raise AssertionError("telegram send transport must not be called")

    async def edit_message_text(self, **kwargs):
        self.edit_calls += 1
        raise AssertionError("telegram edit transport must not be called")


class NotificationPlanCreatedLedger:
    def __init__(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture
        self.trigger_event_id = UUID(fixture["trigger_event_id"])
        self.notification_plan_id = UUID(fixture["notification_plan_id"])
        self.analysis_id = UUID(fixture["analysis_id"])
        self.judge_output_id = UUID(fixture["judge_output_id"])
        self.candidate_group_id = UUID(fixture["candidate_group_id"])
        self.source_message_id = UUID(fixture["source_message_id"])
        self.current_primary_artifact_id = UUID(fixture["current_primary_artifact_id"])

        self.event_outbox: list[dict[str, Any]] = []
        self._event_by_id: dict[UUID, dict[str, Any]] = {}
        self._outbox_dedupe_keys: set[str] = set()

        self.loaded_trigger_event_ids: list[UUID] = []
        self.loaded_analysis_ids: list[UUID] = []
        self.loaded_judge_output_ids: list[UUID] = []
        self.loaded_candidate_group_ids: list[UUID] = []

        self.notification_plans: dict[UUID, NotificationPlanDraft] = {}
        self.notification_renders: list[NotificationRenderDraft] = []
        self.notification_delivery_records: list[dict[str, Any]] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.delivery_result_events: list[dict[str, Any]] = []

        self.analyses = {
            self.analysis_id: AnalysisRenderContext(
                analysis_id=self.analysis_id,
                candidate_group_id=self.candidate_group_id,
                judge_output_id=self.judge_output_id,
                verdict=fixture["verdict"],
                delivery_decision=fixture["delivery_decision"],
                reason_codes_json=["repo_has_clear_scope"],
                evidence_limitations_ko="fixture evidence only",
                recommended_action_ko="inspect repository",
                freshness_note_ko="fresh",
                created_at=datetime.now(timezone.utc),
            )
        }
        self.judge_outputs = {
            self.judge_output_id: JudgeOutputRenderContext(
                judge_output_id=self.judge_output_id,
                payload_json={
                    "judge_schema_version": "judge_output_v1",
                    "headline": "Fixture repository",
                    "summary_one_line_ko": "clear utility",
                    "skeptical_take_ko": "check maintenance first",
                    "why_it_might_matter_ko": "could save triage time",
                },
                model_confidence_band="medium",
            )
        }
        self.candidate_group_proposals = {
            self.candidate_group_id: CandidateRenderContext(
                candidate_group_id=self.candidate_group_id,
                source_message_id=self.source_message_id,
                current_primary_artifact_id=self.current_primary_artifact_id,
                primary_artifact_type=fixture["current_primary_artifact_type"],
                primary_canonical_url=fixture["primary_canonical_url"],
                primary_canonical_id=fixture["primary_canonical_id"],
                source_message_link=fixture["source_message_link"],
                source_text_surface=fixture["source_text_surface"],
            )
        }
        self.artifact_registry = {
            self.current_primary_artifact_id: {
                "artifact_id": self.current_primary_artifact_id,
                "artifact_type": fixture["current_primary_artifact_type"],
                "canonical_url": fixture["primary_canonical_url"],
                "canonical_id": fixture["primary_canonical_id"],
            }
        }
        self.source_messages = {
            self.source_message_id: {
                "source_message_id": self.source_message_id,
                "message_link": fixture["source_message_link"],
                "text_surface": fixture["source_text_surface"],
            }
        }
        self._append_event(
            event_id=self.trigger_event_id,
            event_type="notification.plan.created.v1",
            aggregate_type="analysis",
            aggregate_id=self.analysis_id,
            dedupe_key=f"notification-plan-created:{self.analysis_id}:{fixture['target_chat_id']}:{fixture['material_change_hash']}",
            payload_json=self._plan_payload(),
        )

    def transaction(self):
        return Tx()

    def _plan_payload(self, **updates: Any) -> dict[str, Any]:
        payload = {
            "notification_plan_id": str(self.notification_plan_id),
            "analysis_id": str(self.analysis_id),
            "candidate_group_id": str(self.candidate_group_id),
            "delivery_decision": self.fixture["delivery_decision"],
            "urgency_profile": self.fixture["urgency_profile"],
            "target_chat_id": self.fixture["target_chat_id"],
            "target_thread_id": self.fixture["target_thread_id"],
            "render_profile": self.fixture["render_profile"],
            "dedupe_subject_key": self.fixture["dedupe_subject_key"],
            "material_change_hash": self.fixture["material_change_hash"],
            "send_after": self.fixture["send_after"],
            "suppress_reason_code": self.fixture["suppress_reason_code"],
        }
        payload.update(updates)
        return payload

    def append_plan_event(
        self,
        *,
        event_id: UUID | None = None,
        event_type: str = "notification.plan.created.v1",
        payload_json: dict[str, Any] | None = None,
    ) -> UUID:
        event_id = event_id or uuid4()
        return self._append_event(
            event_id=event_id,
            event_type=event_type,
            aggregate_type="analysis",
            aggregate_id=self.analysis_id,
            dedupe_key=f"notification-plan-created:{event_id}",
            payload_json=payload_json if payload_json is not None else self._plan_payload(),
        )

    def _append_event(
        self,
        *,
        event_id: UUID,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        dedupe_key: str,
        payload_json: dict[str, Any],
    ) -> UUID:
        if dedupe_key in self._outbox_dedupe_keys:
            return next(row["event_id"] for row in self.event_outbox if row["dedupe_key"] == dedupe_key)
        row = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "dedupe_key": dedupe_key,
            "payload_json": payload_json,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
        }
        self.event_outbox.append(row)
        self._event_by_id[event_id] = row
        self._outbox_dedupe_keys.add(dedupe_key)
        return event_id

    async def load_intent_job(self, trigger_event_id: UUID):
        self.loaded_trigger_event_ids.append(trigger_event_id)
        row = self._event_by_id.get(trigger_event_id)
        if row is None or row["event_type"] != "notification.plan.created.v1":
            return None
        payload = row["payload_json"]
        try:
            return NotificationIntentJob(
                trigger_event_id=trigger_event_id,
                event_type=row["event_type"],
                notification_plan_id=UUID(str(payload["notification_plan_id"])),
                analysis_id=UUID(str(payload["analysis_id"])),
                candidate_group_id=UUID(str(payload["candidate_group_id"])),
                delivery_decision=str(payload["delivery_decision"]),  # type: ignore[arg-type]
                urgency_profile=str(payload["urgency_profile"]),  # type: ignore[arg-type]
                target_chat_id=int(payload["target_chat_id"]),
                target_thread_id=_int_or_none(payload.get("target_thread_id")),
                render_profile=_string_or_none(payload.get("render_profile")),
                dedupe_subject_key=str(payload["dedupe_subject_key"]),
                material_change_hash=str(payload["material_change_hash"]),
                send_after=_datetime_or_none(payload.get("send_after")),
                suppress_reason_code=_string_or_none(payload.get("suppress_reason_code")),
            )
        except (KeyError, TypeError, ValueError):
            return None

    async def load_notification_plan(self, notification_plan_id: UUID):
        plan = self.notification_plans.get(notification_plan_id)
        return _plan_row(plan) if plan else None

    async def load_existing_plan_by_material(
        self,
        *,
        analysis_id: UUID,
        target_chat_id: int,
        material_change_hash: str,
    ):
        for plan in self.notification_plans.values():
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
        self.notification_plans[draft.notification_plan_id] = draft
        return draft.notification_plan_id

    async def load_analysis(self, analysis_id: UUID):
        self.loaded_analysis_ids.append(analysis_id)
        return self.analyses.get(analysis_id)

    async def load_judge_output_render_fields(self, judge_output_id: UUID):
        self.loaded_judge_output_ids.append(judge_output_id)
        return self.judge_outputs.get(judge_output_id)

    async def load_candidate_render_context(self, candidate_group_id: UUID):
        self.loaded_candidate_group_ids.append(candidate_group_id)
        return self.candidate_group_proposals.get(candidate_group_id)

    async def load_successful_delivery_for_material(
        self,
        *,
        dedupe_subject_key: str,
        target_chat_id: int,
        material_change_hash: str,
    ):
        successful = [
            (record, self.notification_plans[record["notification_plan_id"]])
            for record in self.notification_delivery_records
            if record["result_status"] in {"sent", "edited"}
            and record["notification_plan_id"] in self.notification_plans
            and self.notification_plans[record["notification_plan_id"]].dedupe_subject_key == dedupe_subject_key
            and self.notification_plans[record["notification_plan_id"]].target_chat_id == target_chat_id
            and self.notification_plans[record["notification_plan_id"]].material_change_hash == material_change_hash
        ]
        if not successful:
            return None
        record, plan = successful[-1]
        candidate = self.candidate_group_proposals.get(plan.candidate_group_id)
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

    async def load_recent_successful_delivery(self, *, dedupe_subject_key: str, target_chat_id: int):
        successful = [
            (record, self.notification_plans[record["notification_plan_id"]])
            for record in self.notification_delivery_records
            if record["result_status"] in {"sent", "edited"}
            and record["notification_plan_id"] in self.notification_plans
            and self.notification_plans[record["notification_plan_id"]].dedupe_subject_key == dedupe_subject_key
            and self.notification_plans[record["notification_plan_id"]].target_chat_id == target_chat_id
        ]
        if not successful:
            return None
        record, plan = successful[-1]
        candidate = self.candidate_group_proposals.get(plan.candidate_group_id)
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
        return False

    async def count_delivery_attempts(self, *, notification_plan_id: UUID) -> int:
        return sum(
            1
            for record in self.notification_delivery_records
            if record["notification_plan_id"] == notification_plan_id
        )

    async def insert_notification_render(self, draft: NotificationRenderDraft):
        for existing in self.notification_renders:
            if existing.notification_plan_id == draft.notification_plan_id and existing.render_hash == draft.render_hash:
                return None
        self.notification_renders.append(draft)
        return uuid4()

    async def insert_delivery_record(self, **kwargs) -> UUID:
        record_id = uuid4()
        self.notification_delivery_records.append(
            {
                "notification_delivery_record_id": record_id,
                "created_at": datetime.now(timezone.utc),
                **kwargs,
            }
        )
        return record_id

    async def update_plan_status(
        self,
        *,
        notification_plan_id: UUID,
        status: str,
        send_after: datetime | None = None,
    ) -> None:
        plan = self.notification_plans.get(notification_plan_id)
        if plan is not None:
            self.notification_plans[notification_plan_id] = replace(
                plan,
                status=status,
                send_after=send_after or plan.send_after,
            )

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

    async def insert_delivery_result_outbox(self, **kwargs) -> None:
        payload = {
            "notification_plan_id": str(kwargs["notification_plan_id"]),
            "notification_delivery_record_id": str(kwargs["notification_delivery_record_id"]),
            "delivery_status": kwargs["delivery_status"],
            "telegram_chat_id": kwargs["telegram_chat_id"],
            "telegram_message_id": kwargs["telegram_message_id"],
            "attempt_count": kwargs["attempt_count"],
            "transport_error_code": kwargs["transport_error_code"],
            "transport_error_class": kwargs["transport_error_class"],
            "edited": kwargs["edited"],
        }
        row = {
            "event_id": uuid4(),
            "event_type": "notification.delivery.result.v1",
            "aggregate_type": "notification_plan",
            "aggregate_id": kwargs["notification_plan_id"],
            "dedupe_key": (
                f"notification-delivery-result:{kwargs['notification_plan_id']}:"
                f"{kwargs['notification_delivery_record_id']}"
            ),
            "payload_json": payload,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
        }
        self.event_outbox.append(row)
        self.delivery_result_events.append(row)


def _load_fixture() -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / "notification_plan_created_valid_bundle.json").read_text(encoding="utf-8"))


def _config(*, dry_run: bool = True, enable_notification_send: bool = False) -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        telegram_bot_token="token" if enable_notification_send else "",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        dry_run=dry_run,
        allow_edits=False,
        enable_notification_send=enable_notification_send,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10,
        log_level="INFO",
    )


def _stream_message(trigger_event_id: UUID, *, decoys: dict[str, str] | None = None) -> StreamMessage:
    fields = {
        "job_id": str(uuid4()),
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": str(uuid4()),
        "idempotency_key": "decoy-idempotency-key",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(trigger_event_id),
    }
    if decoys:
        fields.update(decoys)
    return StreamMessage(stream="q.notification.send", message_id="1-0", fields=fields)


def _service(ledger: NotificationPlanCreatedLedger, client: TripwireTelegramClient) -> NotifierTelegramService:
    return NotifierTelegramService(_config(dry_run=False, enable_notification_send=False), repository=ledger, telegram_client=client)  # type: ignore[arg-type]


async def _run_worker(
    ledger: NotificationPlanCreatedLedger,
    *,
    trigger_event_id: UUID | None = None,
    decoys: dict[str, str] | None = None,
    client: TripwireTelegramClient | None = None,
):
    client = client or TripwireTelegramClient()
    consumer = FakeConsumer(_stream_message(trigger_event_id or ledger.trigger_event_id, decoys=decoys))
    worker = NotifierTelegramWorker(
        _config(dry_run=False, enable_notification_send=False),
        consumer=consumer,
        service=_service(ledger, client),
    )
    return consumer, client, await worker.run_once()


def _install_downstream_tripwires(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.analysis_validator import worker as analysis_validator_worker
    from services.judge_openai import worker as judge_openai_worker
    from services.maintenance import worker as maintenance_worker
    from services.notifier_telegram import telegram_client
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.outbox_relay import service as outbox_relay_service
    from services.policy_engine import worker as policy_engine_worker

    def fail_downstream(*args, **kwargs):
        raise AssertionError("notifier acceptance must stop at notification.delivery.result.v1 outbox intent")

    monkeypatch.setattr(telegram_client.TelegramBotClient, "send_message", fail_downstream)
    monkeypatch.setattr(telegram_client.TelegramBotClient, "edit_message_text", fail_downstream)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_downstream)
    monkeypatch.setattr(outbox_relay_service, "OutboxRelayService", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "MaintenanceQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "ReplayQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "DueRetryPromotionWorker", fail_downstream)
    monkeypatch.setattr(policy_engine_worker, "PolicyEngineWorker", fail_downstream)
    monkeypatch.setattr(analysis_validator_worker, "AnalysisValidatorWorker", fail_downstream)
    monkeypatch.setattr(judge_openai_worker, "JudgeOpenAIWorker", fail_downstream)


def _upstream_snapshot(ledger: NotificationPlanCreatedLedger) -> dict[str, Any]:
    return {
        "analyses": deepcopy(ledger.analyses),
        "judge_outputs": deepcopy(ledger.judge_outputs),
        "candidate_group_proposals": deepcopy(ledger.candidate_group_proposals),
        "artifact_registry": deepcopy(ledger.artifact_registry),
        "source_messages": deepcopy(ledger.source_messages),
    }


def _notifier_delivery_side_effects(ledger: NotificationPlanCreatedLedger) -> dict[str, Any]:
    return {
        "renders": ledger.notification_renders,
        "delivery_records": ledger.notification_delivery_records,
        "delivery_result_events": ledger.delivery_result_events,
    }


@pytest.mark.asyncio
async def test_notification_plan_created_dry_run_result_rehydrates_event_outbox_and_preserves_boundaries(
    monkeypatch,
) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = NotificationPlanCreatedLedger(_load_fixture())
    before_upstream = _upstream_snapshot(ledger)
    client = TripwireTelegramClient()
    decoys = {
        "payload_json": json.dumps({"notification_plan_id": str(uuid4()), "delivery_decision": "suppress"}),
        "notification_plan_id": str(uuid4()),
        "analysis_id": str(uuid4()),
        "candidate_group_id": str(uuid4()),
        "delivery_decision": "suppress",
        "target_chat_id": "999999",
        "telegram_bot_token": "poisoned",
        "openai_api_key": "poisoned",
    }

    consumer, client, result = await _run_worker(ledger, decoys=decoys, client=client)

    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["1-0"]
    assert client.send_calls == 0
    assert client.edit_calls == 0
    assert ledger.loaded_trigger_event_ids == [ledger.trigger_event_id]
    assert ledger.loaded_analysis_ids == [ledger.analysis_id]
    assert ledger.loaded_judge_output_ids == [ledger.judge_output_id]
    assert ledger.loaded_candidate_group_ids == [ledger.candidate_group_id]

    assert set(ledger.notification_plans) == {ledger.notification_plan_id}
    plan = ledger.notification_plans[ledger.notification_plan_id]
    assert plan.notification_plan_id == ledger.notification_plan_id
    assert plan.analysis_id == ledger.analysis_id
    assert plan.candidate_group_id == ledger.candidate_group_id
    assert plan.delivery_decision == "send_now"
    assert plan.target_chat_id == 12345
    assert plan.status == "suppressed"

    assert len(ledger.notification_renders) == 1
    assert ledger.notification_renders[0].notification_plan_id == ledger.notification_plan_id
    assert len(ledger.notification_delivery_records) == 1
    delivery = ledger.notification_delivery_records[0]
    assert delivery["notification_plan_id"] == ledger.notification_plan_id
    assert delivery["result_status"] == "suppressed"
    assert delivery["telegram_chat_id"] == 12345
    assert delivery["telegram_message_id"] is None
    assert delivery["attempt_count"] == 0
    assert delivery["transport_error_code"] == "notification_send_flag_disabled"
    assert delivery["telegram_response_json"] == {
        "dry_run": False,
        "send_disabled": True,
        "send_enabled": False,
        "transport_skipped": True,
        "reason_code": "notification_send_flag_disabled",
        "delivery_action": "send",
    }

    assert [transition["to_state"] for transition in ledger.state_transitions] == ["rendered", "suppressed"]
    assert ledger.state_transitions[-1]["reason_code"] == "notification_send_flag_disabled"
    assert len(ledger.delivery_result_events) == 1
    result_event = ledger.delivery_result_events[0]
    assert result_event["event_type"] == "notification.delivery.result.v1"
    assert result_event["aggregate_type"] == "notification_plan"
    assert result_event["aggregate_id"] == ledger.notification_plan_id
    assert result_event["payload_json"] == {
        "notification_plan_id": str(ledger.notification_plan_id),
        "notification_delivery_record_id": str(delivery["notification_delivery_record_id"]),
        "delivery_status": "suppressed",
        "telegram_chat_id": 12345,
        "telegram_message_id": None,
        "attempt_count": 0,
        "transport_error_code": "notification_send_flag_disabled",
        "transport_error_class": None,
        "edited": False,
    }
    assert set(result_event["payload_json"]) == {
        "notification_plan_id",
        "notification_delivery_record_id",
        "delivery_status",
        "telegram_chat_id",
        "telegram_message_id",
        "attempt_count",
        "transport_error_code",
        "transport_error_class",
        "edited",
    }
    assert _upstream_snapshot(ledger) == before_upstream


@pytest.mark.asyncio
async def test_duplicate_same_plan_intent_does_not_duplicate_render_delivery_or_result(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = NotificationPlanCreatedLedger(_load_fixture())

    await _run_worker(ledger)
    before_counts = {
        "plans": len(ledger.notification_plans),
        "renders": len(ledger.notification_renders),
        "delivery_records": len(ledger.notification_delivery_records),
        "delivery_result_events": len(ledger.delivery_result_events),
    }

    _consumer, client, result = await _run_worker(ledger)

    assert result.processed == 1
    assert result.acked == 1
    assert client.send_calls == 0
    assert client.edit_calls == 0
    assert {
        "plans": len(ledger.notification_plans),
        "renders": len(ledger.notification_renders),
        "delivery_records": len(ledger.notification_delivery_records),
        "delivery_result_events": len(ledger.delivery_result_events),
    } == before_counts
    assert ledger.state_transitions[-1]["reason_code"] == "notification_duplicate_terminal_noop"


@pytest.mark.asyncio
async def test_future_send_after_stops_before_render_delivery_record_or_result(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = NotificationPlanCreatedLedger(_load_fixture())
    future_event_id = ledger.append_plan_event(
        payload_json=ledger._plan_payload(send_after=(datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat())
    )

    _consumer, client, result = await _run_worker(ledger, trigger_event_id=future_event_id)

    assert result.processed == 1
    assert result.acked == 1
    assert client.send_calls == 0
    assert client.edit_calls == 0
    assert len(ledger.notification_plans) == 1
    assert ledger.notification_plans[ledger.notification_plan_id].status == "planned"
    assert _notifier_delivery_side_effects(ledger) == {
        "renders": [],
        "delivery_records": [],
        "delivery_result_events": [],
    }
    assert ledger.state_transitions[-1]["reason_code"] == "notification_send_after_deferred"


@pytest.mark.asyncio
async def test_suppress_decision_stops_before_render_delivery_record_or_result(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = NotificationPlanCreatedLedger(_load_fixture())
    ledger.analyses[ledger.analysis_id] = replace(ledger.analyses[ledger.analysis_id], delivery_decision="suppress")
    suppress_event_id = ledger.append_plan_event(
        payload_json=ledger._plan_payload(
            delivery_decision="suppress",
            urgency_profile="suppressed",
            suppress_reason_code="policy_suppressed",
        )
    )

    _consumer, client, result = await _run_worker(ledger, trigger_event_id=suppress_event_id)

    assert result.processed == 1
    assert result.acked == 1
    assert client.send_calls == 0
    assert client.edit_calls == 0
    assert len(ledger.notification_plans) == 1
    assert ledger.notification_plans[ledger.notification_plan_id].status == "suppressed"
    assert _notifier_delivery_side_effects(ledger) == {
        "renders": [],
        "delivery_records": [],
        "delivery_result_events": [],
    }
    assert ledger.state_transitions[-1]["reason_code"] == "policy_suppressed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_case",
    [
        "missing_event",
        "wrong_event_type",
        "malformed_payload",
        "missing_analysis",
        "analysis_candidate_mismatch",
        "missing_candidate_render_context",
    ],
)
async def test_invalid_or_mismatched_inputs_stop_before_notifier_delivery_side_effects(
    monkeypatch,
    bad_case: str,
) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = NotificationPlanCreatedLedger(_load_fixture())
    trigger_event_id = ledger.trigger_event_id
    if bad_case == "missing_event":
        trigger_event_id = uuid4()
    elif bad_case == "wrong_event_type":
        ledger._event_by_id[ledger.trigger_event_id]["event_type"] = "analysis.policy.apply.v1"
    elif bad_case == "malformed_payload":
        del ledger._event_by_id[ledger.trigger_event_id]["payload_json"]["analysis_id"]
    elif bad_case == "missing_analysis":
        del ledger.analyses[ledger.analysis_id]
    elif bad_case == "analysis_candidate_mismatch":
        ledger.analyses[ledger.analysis_id] = replace(ledger.analyses[ledger.analysis_id], candidate_group_id=uuid4())
    elif bad_case == "missing_candidate_render_context":
        del ledger.candidate_group_proposals[ledger.candidate_group_id]
    before_upstream = _upstream_snapshot(ledger)

    _consumer, client, result = await _run_worker(ledger, trigger_event_id=trigger_event_id)

    assert result.processed == 1
    assert result.acked == 1
    assert client.send_calls == 0
    assert client.edit_calls == 0
    assert ledger.notification_plans == {}
    assert _notifier_delivery_side_effects(ledger) == {
        "renders": [],
        "delivery_records": [],
        "delivery_result_events": [],
    }
    assert _upstream_snapshot(ledger) == before_upstream


def _plan_row(plan: NotificationPlanDraft) -> dict[str, Any]:
    return {
        "notification_plan_id": plan.notification_plan_id,
        "analysis_id": plan.analysis_id,
        "candidate_group_id": plan.candidate_group_id,
        "target_chat_id": plan.target_chat_id,
        "target_thread_id": plan.target_thread_id,
        "render_profile": plan.render_profile,
        "dedupe_subject_key": plan.dedupe_subject_key,
        "material_change_hash": plan.material_change_hash,
        "send_after": plan.send_after,
        "status": plan.status,
    }


def _int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def _string_or_none(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    return text if text else None


def _datetime_or_none(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
