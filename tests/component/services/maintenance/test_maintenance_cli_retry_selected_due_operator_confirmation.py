from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from services.maintenance import main as maintenance_main
from services.maintenance.models import SelectedPlanRecoveryRow
from tests.unit.services.maintenance.test_batch_recovery_validation import _config, _row


class FakeManualRetryRepository:
    def __init__(self, rows: list[SelectedPlanRecoveryRow]) -> None:
        self.rows = {row.notification_plan_id: row for row in rows}
        self.load_calls: list[list[UUID]] = []
        self.replay_requests: list[dict] = []
        self.event_outbox: list[dict] = []
        self.job_attempts: list[dict] = []
        self.notification_plan_mutations: list[dict] = []
        self.notification_delivery_record_mutations: list[dict] = []

    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]):
        self.load_calls.append(notification_plan_ids)
        return [self.rows[plan_id] for plan_id in notification_plan_ids if plan_id in self.rows]

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[UUID], requested_by: str) -> int:
        raise AssertionError("retry-selected-due must not create replay_requests")

    async def insert_manual_retry_intent_outbox(
        self,
        *,
        row: SelectedPlanRecoveryRow,
        recovery_batch_id: str,
        dedupe_key: str,
        payload_json: dict,
    ) -> bool:
        self.event_outbox.append(
            {
                "event_type": "notification.plan.created.v1",
                "status": "pending",
                "notification_plan_id": row.notification_plan_id,
                "recovery_batch_id": recovery_batch_id,
                "dedupe_key": dedupe_key,
                "payload_json": payload_json,
            }
        )
        return True


def _retry_args(plan_id: str) -> list[str]:
    return [
        "batch-recovery",
        "retry-selected-due",
        "--plan-id",
        plan_id,
        "--requested-by",
        "test/operator",
        "--confirm",
        "write",
    ]


@pytest.mark.asyncio
async def test_missing_confirm_write_fails_before_runner_is_called(monkeypatch) -> None:
    calls: list[str] = []

    async def fail_batch_recovery(config, args):
        calls.append(args.recovery_mode)
        raise AssertionError("unconfirmed retry-selected-due must not reach the runner")

    monkeypatch.setattr(maintenance_main, "_run_batch_recovery", fail_batch_recovery)

    with pytest.raises(SystemExit) as exc:
        await maintenance_main._run(
            [
                "batch-recovery",
                "retry-selected-due",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "test/operator",
            ]
        )

    assert exc.value.code == 2
    assert calls == []


@pytest.mark.asyncio
async def test_malformed_uuid_fails_before_config_or_engine_creation(monkeypatch, capsys) -> None:
    def fail_from_env(cls):
        raise AssertionError("malformed retry-selected-due input must not load runtime config")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_from_env))

    exit_code = await maintenance_main._run(_retry_args("not-a-uuid"))
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "invalid_notification_plan_id"
    assert payload["emitted_count"] == 0


@pytest.mark.asyncio
async def test_malformed_uuid_does_not_load_rows_or_write() -> None:
    repository = FakeManualRetryRepository([])
    emitted: list[str] = []
    args = maintenance_main.build_parser().parse_args(_retry_args("not-a-uuid"))

    exit_code = await maintenance_main.run_retry_selected_due_batch_recovery(
        _config(),
        args,
        repository,
        emit_json=emitted.append,
    )
    payload = json.loads(emitted[0])

    assert exit_code == 2
    assert payload["reason_code"] == "invalid_notification_plan_id"
    assert repository.load_calls == []
    assert repository.replay_requests == []
    assert repository.event_outbox == []
    assert repository.job_attempts == []
    assert repository.notification_plan_mutations == []
    assert repository.notification_delivery_record_mutations == []


@pytest.mark.asyncio
async def test_confirmed_eligible_due_retry_creates_manual_retry_intent_outbox_only() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = _row(delivery_status="failed_retryable", send_after=now - timedelta(seconds=1), attempt_count=1)
    repository = FakeManualRetryRepository([row])
    emitted: list[str] = []
    args = maintenance_main.build_parser().parse_args(_retry_args(str(row.notification_plan_id)))

    exit_code = await maintenance_main.run_retry_selected_due_batch_recovery(
        _config(),
        args,
        repository,
        emit_json=emitted.append,
    )
    payload = json.loads(emitted[0])

    assert exit_code == 0
    assert payload["recovery_mode"] == "retry-selected-due"
    assert payload["selected_count"] == 1
    assert payload["accepted_count"] == 1
    assert payload["emitted_count"] == 1
    assert repository.load_calls == [[row.notification_plan_id]]
    assert repository.replay_requests == []
    assert repository.job_attempts == []
    assert repository.notification_plan_mutations == []
    assert repository.notification_delivery_record_mutations == []
    assert len(repository.event_outbox) == 1
    event = repository.event_outbox[0]
    assert event["event_type"] == "notification.plan.created.v1"
    assert event["payload_json"]["notification_plan_id"] == str(row.notification_plan_id)
    assert event["payload_json"]["retry_reason"] == "manual_selected_due_retry"
    assert event["payload_json"]["previous_attempt_count"] == 1
    assert event["payload_json"]["send_after"] is None


@pytest.mark.asyncio
async def test_ineligible_retry_selected_due_rows_are_skipped_without_writes() -> None:
    now = datetime.now(timezone.utc)
    cases = [
        (
            "send_disabled_suppressed",
            _row(delivery_status="suppressed", send_after=now - timedelta(seconds=1), send_disabled=True),
            "status_is_not_failed_retryable",
        ),
        (
            "failed_terminal",
            _row(delivery_status="failed_terminal", send_after=now - timedelta(seconds=1)),
            "status_is_not_failed_retryable",
        ),
        (
            "not_yet_due",
            _row(delivery_status="failed_retryable", send_after=now + timedelta(seconds=30)),
            "send_after_not_due_yet",
        ),
        (
            "ceiling_exceeded",
            _row(delivery_status="failed_retryable", send_after=now - timedelta(seconds=1), attempt_count=3),
            "retry_ceiling_exceeded",
        ),
        (
            "open_replay_target",
            _row(
                delivery_status="failed_retryable",
                send_after=now - timedelta(seconds=1),
                has_open_replay_request=True,
            ),
            "open_replay_request_exists",
        ),
    ]

    for label, row, reason_code in cases:
        repository = FakeManualRetryRepository([row])
        emitted: list[str] = []
        args = maintenance_main.build_parser().parse_args(_retry_args(str(row.notification_plan_id)))

        exit_code = await maintenance_main.run_retry_selected_due_batch_recovery(
            _config(),
            args,
            repository,
            emit_json=emitted.append,
        )
        payload = json.loads(emitted[0])

        assert exit_code == 0, label
        assert payload["accepted_count"] == 0, label
        assert payload["emitted_count"] == 0, label
        assert payload["skipped_reason_codes"] == {reason_code: 1}, label
        assert repository.replay_requests == [], label
        assert repository.event_outbox == [], label
        assert repository.job_attempts == [], label
        assert repository.notification_plan_mutations == [], label
        assert repository.notification_delivery_record_mutations == [], label
