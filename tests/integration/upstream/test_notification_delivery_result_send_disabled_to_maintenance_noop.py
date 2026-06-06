from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.maintenance.config import MaintenanceConfig
from services.maintenance.models import (
    LatestDeliveryRecord,
    NotificationPlanRecord,
    OutboxEvent,
    StreamMessage,
)
from services.maintenance.retry_policy import classify_delivery_result_dry_run_noop
from services.maintenance.service import MaintenanceService
from services.maintenance.worker import MaintenanceQueueWorker


FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "upstream"
    / "notification_delivery_result_send_disabled_valid_bundle.json"
)


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


class RuntimeTripwires:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fail(self, name: str):
        def _fail(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(f"forbidden runtime path invoked: {name}")

        return _fail


class RecordingMaintenanceService:
    def __init__(self, service: MaintenanceService) -> None:
        self._service = service
        self.trigger_event_ids: list[str] = []
        self.replay_trigger_event_ids: list[str] = []
        self.due_retry_calls = 0
        self.results: list[Any] = []

    async def handle_maintenance_trigger_event(self, trigger_event_id: str):
        self.trigger_event_ids.append(str(trigger_event_id))
        result = await self._service.handle_maintenance_trigger_event(trigger_event_id)
        self.results.append(result)
        return result

    async def handle_replay_trigger_event(self, trigger_event_id: str) -> None:
        self.replay_trigger_event_ids.append(str(trigger_event_id))
        raise AssertionError("send-disabled maintenance result must not dispatch replay")

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        self.due_retry_calls += 1
        raise AssertionError("send-disabled result consumer must not run due retry promotion")


class NotificationDeliveryResultLedger:
    def __init__(self, fixture: dict[str, Any]) -> None:
        self.event_outbox = [_parse_event_row(row) for row in fixture["event_outbox"]]
        self.notification_plans = {
            UUID(row["notification_plan_id"]): _plan_from_fixture(row)
            for row in fixture["notification_plans"]
        }
        self.notification_renders = deepcopy(fixture["notification_renders"])
        self.notification_delivery_records = [
            _parse_delivery_record(row) for row in fixture["notification_delivery_records"]
        ]
        self.pipeline_runs = deepcopy(fixture["pipeline_runs"])
        self.job_attempts = deepcopy(fixture["job_attempts"])
        self.replay_requests = deepcopy(fixture["replay_requests"])
        self.dead_letter_entries = deepcopy(fixture["dead_letter_entries"])

        self.loaded_outbox_event_ids: list[UUID] = []
        self.loaded_notification_plan_ids: list[UUID] = []
        self.loaded_latest_delivery_plan_ids: list[UUID] = []
        self.loaded_latest_delivery_statuses: list[str] = []

    def transaction(self):
        return Tx()

    @property
    def trigger_event_id(self) -> UUID:
        return UUID(str(self.event_outbox[0]["event_id"]))

    @property
    def notification_plan_id(self) -> UUID:
        return UUID(str(self.event_outbox[0]["payload_json"]["notification_plan_id"]))

    async def load_outbox_event(self, event_id: UUID):
        self.loaded_outbox_event_ids.append(event_id)
        for row in self.event_outbox:
            if row["event_id"] == event_id:
                return OutboxEvent(
                    event_id=row["event_id"],
                    event_type=row["event_type"],
                    aggregate_type=row["aggregate_type"],
                    aggregate_id=row["aggregate_id"],
                    payload_json=deepcopy(row["payload_json"]),
                )
        return None

    async def load_notification_plan(self, notification_plan_id: UUID):
        self.loaded_notification_plan_ids.append(notification_plan_id)
        return self.notification_plans.get(notification_plan_id)

    async def load_latest_delivery_record(self, notification_plan_id: UUID):
        self.loaded_latest_delivery_plan_ids.append(notification_plan_id)
        matches = [
            row
            for row in self.notification_delivery_records
            if row["notification_plan_id"] == notification_plan_id
        ]
        if not matches:
            return None
        latest = sorted(matches, key=lambda row: row["created_at"])[-1]
        delivery_status = latest.get("delivery_status") or latest["result_status"]
        self.loaded_latest_delivery_statuses.append(delivery_status)
        return LatestDeliveryRecord(
            notification_delivery_record_id=latest["notification_delivery_record_id"],
            notification_plan_id=latest["notification_plan_id"],
            delivery_status=delivery_status,
            attempt_count=latest["attempt_count"],
            transport_error_code=latest["transport_error_code"],
            transport_error_class=latest["transport_error_class"],
            telegram_response_json=deepcopy(latest["telegram_response_json"]),
            created_at=latest["created_at"],
        )

    async def count_delivery_attempts(self, notification_plan_id: UUID) -> int:
        return sum(
            1
            for row in self.notification_delivery_records
            if row["notification_plan_id"] == notification_plan_id
        )

    async def load_due_retry_candidates(self, limit: int, now: datetime):
        raise AssertionError("delivery result consumer must not scan due retry candidates")

    async def insert_plan_created_outbox(
        self,
        *,
        notification_plan_id: UUID,
        dedupe_key: str,
        payload_json: dict[str, Any],
    ) -> bool:
        self.event_outbox.append(
            {
                "event_id": uuid4(),
                "event_type": "notification.plan.created.v1",
                "aggregate_type": "notification_plan",
                "aggregate_id": notification_plan_id,
                "dedupe_key": dedupe_key,
                "payload_json": deepcopy(payload_json),
                "status": "pending",
                "created_at": datetime.now().astimezone(),
            }
        )
        return True

    async def insert_retry_ceiling_dead_letter(self, *, notification_plan_id: UUID, retry_count: int) -> bool:
        self.dead_letter_entries.append(
            {
                "stage_name": "maintenance_delivery_retry",
                "queue_name": "q.maintenance",
                "root_object_type": "notification_plan",
                "root_object_id": notification_plan_id,
                "last_error_code": "max_notification_retry_attempts_exceeded",
                "retry_count": retry_count,
            }
        )
        return True

    async def load_replay_request(self, replay_request_id: UUID):
        return next(
            (
                row
                for row in self.replay_requests
                if row.get("replay_request_id") == replay_request_id
            ),
            None,
        )

    async def update_replay_request_status(self, replay_request_id: UUID, status: str) -> None:
        for row in self.replay_requests:
            if row.get("replay_request_id") == replay_request_id:
                row["status"] = status
                return
        raise AssertionError("unexpected replay request status update")

    async def insert_job_attempt(self, **kwargs) -> None:
        self.job_attempts.append(deepcopy(kwargs))

    async def count_delivery_result_noop_job_attempts(self, notification_plan_id: UUID) -> int:
        return sum(
            1
            for row in self.job_attempts
            if row["stage_name"] == "maintenance_delivery_result"
            and row["queue_name"] == "q.maintenance"
            and row["root_object_type"] == "notification_plan"
            and row["root_object_id"] == notification_plan_id
            and row["attempt_status"] == "succeeded"
            and row["error_code"] == "delivery_result_suppressed_dry_run_noop"
        )

    async def insert_delivery_result_noop_job_attempt(self, notification_plan_id: UUID) -> bool:
        if await self.count_delivery_result_noop_job_attempts(notification_plan_id):
            return False
        await self.insert_job_attempt(
            stage_name="maintenance_delivery_result",
            queue_name="q.maintenance",
            root_object_type="notification_plan",
            root_object_id=notification_plan_id,
            attempt_status="succeeded",
            error_code="delivery_result_suppressed_dry_run_noop",
        )
        return True

    async def count_delivery_result_sent_success_job_attempts(self, notification_delivery_record_id: UUID) -> int:
        return sum(
            1
            for row in self.job_attempts
            if row["stage_name"] == "maintenance_delivery_result"
            and row["queue_name"] == "q.maintenance"
            and row["root_object_type"] == "notification_delivery_record"
            and row["root_object_id"] == notification_delivery_record_id
            and row["attempt_status"] == "succeeded"
            and row["error_code"] == "delivery_result_sent_terminal_success"
        )

    async def insert_delivery_result_sent_success_job_attempt(self, notification_delivery_record_id: UUID) -> bool:
        if await self.count_delivery_result_sent_success_job_attempts(notification_delivery_record_id):
            return False
        await self.insert_job_attempt(
            stage_name="maintenance_delivery_result",
            queue_name="q.maintenance",
            root_object_type="notification_delivery_record",
            root_object_id=notification_delivery_record_id,
            attempt_status="succeeded",
            error_code="delivery_result_sent_terminal_success",
        )
        return True


@pytest.mark.asyncio
async def test_send_disabled_delivery_result_to_maintenance_is_non_retryable_noop(monkeypatch) -> None:
    tripwires = _install_runtime_tripwires(monkeypatch)
    ledger = NotificationDeliveryResultLedger(_load_fixture())
    event = ledger.event_outbox[0]
    plan_before = deepcopy(ledger.notification_plans)
    renders_before = deepcopy(ledger.notification_renders)
    delivery_records_before = deepcopy(ledger.notification_delivery_records)
    decoy_plan_id = uuid4()
    decoy_delivery_record_id = uuid4()

    assert _event_type_count(ledger, "notification.delivery.result.v1") == 1
    assert _event_type_count(ledger, "notification.plan.created.v1") == 0
    assert ledger.replay_requests == []
    assert ledger.dead_letter_entries == []
    assert ledger.notification_delivery_records[0]["attempt_count"] == 0
    assert ledger.notification_delivery_records[0]["delivery_status"] == "suppressed"
    assert ledger.notification_delivery_records[0]["transport_error_code"] == "notification_send_flag_disabled"
    assert ledger.notification_delivery_records[0]["transport_error_class"] is None
    assert ledger.notification_delivery_records[0]["telegram_message_id"] is None

    consumer = FakeConsumer(
        _stream_message(
            ledger.trigger_event_id,
            decoy_plan_id=decoy_plan_id,
            decoy_delivery_record_id=decoy_delivery_record_id,
        )
    )
    recording_service = RecordingMaintenanceService(
        MaintenanceService(_config(), repository=ledger)  # type: ignore[arg-type]
    )
    worker = MaintenanceQueueWorker(_config(), consumer=consumer, service=recording_service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert consumer.acked == ["maintenance-1"]
    assert recording_service.trigger_event_ids == [str(ledger.trigger_event_id)]
    assert recording_service.replay_trigger_event_ids == []
    assert recording_service.due_retry_calls == 0
    assert ledger.loaded_outbox_event_ids == [ledger.trigger_event_id]
    assert ledger.loaded_notification_plan_ids == [ledger.notification_plan_id]
    assert ledger.loaded_latest_delivery_plan_ids == [ledger.notification_plan_id]
    assert ledger.loaded_latest_delivery_statuses == ["suppressed"]
    assert decoy_plan_id not in ledger.loaded_notification_plan_ids
    assert decoy_delivery_record_id not in {
        row["notification_delivery_record_id"] for row in ledger.notification_delivery_records
    }

    assert len(recording_service.results) == 1
    service_result = recording_service.results[0]
    assert service_result is not None
    assert service_result.processed is True
    assert service_result.classification != "retryable_candidate"
    assert service_result.classification != "terminal_failure"
    assert service_result.action != "record_retryable_interpretation"
    assert service_result.action != "record_terminal_failure"
    assert service_result.retry_intent_written is False
    assert service_result.dead_letter_written is False
    assert service_result.replay_request_written is False

    delivery_decision = classify_delivery_result_dry_run_noop(
        delivery_status="suppressed",
        delivery_reason="notification_send_flag_disabled",
    )
    assert delivery_decision.maintenance_classification == "suppressed_not_auto_retryable"
    assert delivery_decision.auto_retry_allowed is False
    assert delivery_decision.retry_intent_allowed is False
    assert delivery_decision.replay_dispatch_allowed is False
    assert delivery_decision.dead_letter_allowed is False

    assert _event_type_count(ledger, "notification.plan.created.v1") == 0
    assert ledger.replay_requests == []
    assert ledger.dead_letter_entries == []
    assert ledger.notification_plans == plan_before
    assert ledger.notification_renders == renders_before
    assert ledger.notification_delivery_records == delivery_records_before
    assert all(
        row["stage_name"] == "maintenance_delivery_result"
        and row["queue_name"] == "q.maintenance"
        and row["root_object_type"] in {"notification_plan", "notification_delivery_record"}
        for row in ledger.job_attempts
    )
    assert tripwires.calls == []


def _load_fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _config() -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        maintenance_consumer_name="test",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
        replay_consumer_name="test",
        batch_size=1,
        block_ms=1,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=False,
        notifier_telegram_dry_run=False,
        enable_delivery_retry_promotion=True,
        enable_replay_to_prod_db=False,
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


def _stream_message(
    trigger_event_id: UUID,
    *,
    decoy_plan_id: UUID,
    decoy_delivery_record_id: UUID,
) -> StreamMessage:
    decoy_id = str(uuid4())
    return StreamMessage(
        stream="q.maintenance",
        message_id="maintenance-1",
        fields={
            "job_id": decoy_id,
            "stage_name": "maintenance-decoy",
            "root_object_type": "analysis",
            "root_object_id": str(decoy_plan_id),
            "idempotency_key": "decoy-idempotency-key",
            "pipeline_run_id": str(uuid4()),
            "not_before": "2099-01-01T00:00:00+00:00",
            "trigger_event_id": str(trigger_event_id),
            "payload_json": json.dumps(
                {
                    "notification_plan_id": str(decoy_plan_id),
                    "notification_delivery_record_id": str(decoy_delivery_record_id),
                    "delivery_status": "failed_retryable",
                    "transport_error_code": "telegram_retryable",
                }
            ),
            "notification_plan_id": str(decoy_plan_id),
            "notification_delivery_record_id": str(decoy_delivery_record_id),
            "delivery_status": "failed_retryable",
            "transport_error_code": "telegram_retryable",
            "telegram_bot_token": "poisoned",
            "openai_api_key": "poisoned",
            "redis_url": "poisoned",
        },
    )


def _install_runtime_tripwires(monkeypatch: pytest.MonkeyPatch) -> RuntimeTripwires:
    from services.analysis_validator import worker as analysis_validator_worker
    from services.judge_openai import openai_client as judge_openai_client
    from services.judge_openai import worker as judge_openai_worker
    from services.notifier_telegram import telegram_client
    from services.notifier_telegram import worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.outbox_relay import service as outbox_relay_service
    from services.policy_engine import worker as policy_engine_worker

    tripwires = RuntimeTripwires()
    monkeypatch.setattr(notifier_worker, "NotifierTelegramWorker", tripwires.fail("NotifierTelegramWorker"))
    monkeypatch.setattr(telegram_client.TelegramBotClient, "send_message", tripwires.fail("TelegramBotClient.send_message"))
    monkeypatch.setattr(
        telegram_client.TelegramBotClient,
        "edit_message_text",
        tripwires.fail("TelegramBotClient.edit_message_text"),
    )
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", tripwires.fail("RedisStreamsPublisher"))
    monkeypatch.setattr(outbox_relay_service, "OutboxRelayService", tripwires.fail("OutboxRelayService"))
    monkeypatch.setattr(policy_engine_worker, "PolicyEngineWorker", tripwires.fail("PolicyEngineWorker"))
    monkeypatch.setattr(analysis_validator_worker, "AnalysisValidatorWorker", tripwires.fail("AnalysisValidatorWorker"))
    monkeypatch.setattr(judge_openai_worker, "JudgeOpenAIWorker", tripwires.fail("JudgeOpenAIWorker"))
    monkeypatch.setattr(judge_openai_client, "OpenAIJudgeClient", tripwires.fail("OpenAIJudgeClient"))
    return tripwires


def _event_type_count(ledger: NotificationDeliveryResultLedger, event_type: str) -> int:
    return sum(1 for row in ledger.event_outbox if row["event_type"] == event_type)


def _parse_event_row(row: dict[str, Any]) -> dict[str, Any]:
    parsed = deepcopy(row)
    parsed["event_id"] = UUID(parsed["event_id"])
    parsed["aggregate_id"] = UUID(parsed["aggregate_id"])
    return parsed


def _plan_from_fixture(row: dict[str, Any]) -> NotificationPlanRecord:
    return NotificationPlanRecord(
        notification_plan_id=UUID(row["notification_plan_id"]),
        analysis_id=UUID(row["analysis_id"]),
        candidate_group_id=UUID(row["candidate_group_id"]),
        delivery_decision=row["delivery_decision"],
        urgency_profile=row["urgency_profile"],
        target_chat_id=int(row["target_chat_id"]),
        target_thread_id=row["target_thread_id"],
        render_profile=row["render_profile"],
        dedupe_subject_key=row["dedupe_subject_key"],
        material_change_hash=row["material_change_hash"],
        send_after=_datetime_or_none(row["send_after"]),
        suppress_reason_code=row["suppress_reason_code"],
        status=row["status"],
    )


def _parse_delivery_record(row: dict[str, Any]) -> dict[str, Any]:
    parsed = deepcopy(row)
    parsed["notification_delivery_record_id"] = UUID(parsed["notification_delivery_record_id"])
    parsed["notification_plan_id"] = UUID(parsed["notification_plan_id"])
    parsed["created_at"] = datetime.fromisoformat(parsed["created_at"])
    return parsed


def _datetime_or_none(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
