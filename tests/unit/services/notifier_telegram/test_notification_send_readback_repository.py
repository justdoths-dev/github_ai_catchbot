from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from src.services.notifier_telegram.bounded_notification_send_dry_run_runner import _durable_readback_from_raw
from src.services.notifier_telegram.repositories import NotifierTelegramRepository


class _ScalarResult:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one(self) -> int:
        return self._value


class _MappingResult:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def mappings(self) -> "_MappingResult":
        return self

    def first(self) -> dict[str, Any]:
        return self._row


class _ReadbackSession:
    def __init__(
        self,
        *,
        plans: list[dict[str, Any]],
        renders: list[dict[str, Any]],
        delivery_records: list[dict[str, Any]],
        events: list[dict[str, Any]],
    ) -> None:
        self.plans = plans
        self.renders = renders
        self.delivery_records = delivery_records
        self.events = events
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def in_transaction(self) -> bool:
        return False

    def begin(self) -> Any:
        raise AssertionError("readback tests do not open transactions")

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        sql = str(statement)
        bound = dict(params or {})
        self.calls.append((sql, bound))
        if "FROM notification_plans" in sql and "candidate_group_id" in sql:
            return _ScalarResult(self._plan_count(bound, exact=True))
        if "FROM notification_plans" in sql:
            return _ScalarResult(self._plan_count(bound, exact=False))
        if "FROM notification_renders" in sql:
            return _ScalarResult(self._render_count(bound))
        if "FROM notification_delivery_records" in sql:
            return _ScalarResult(self._delivery_record_count(bound))
        if "FROM event_outbox" in sql:
            matches = [event for event in self.events if self._event_matches(event, bound)]
            statuses = [str(event["status"]) for event in matches if event.get("status") is not None]
            return _MappingResult(
                {
                    "event_count": len(matches),
                    "event_status": max(statuses) if statuses else None,
                }
            )
        raise AssertionError(f"unexpected SQL: {sql}")

    def _plan_count(self, params: dict[str, Any], *, exact: bool) -> int:
        return sum(1 for plan in self.plans if self._plan_matches(plan, params, exact=exact))

    def _plan_matches(self, plan: dict[str, Any], params: dict[str, Any], *, exact: bool) -> bool:
        if str(plan["analysis_id"]) != params["analysis_id"]:
            return False
        if int(plan["target_chat_id"]) != int(params["target_chat_id"]):
            return False
        if plan["material_change_hash"] != params["material_change_hash"]:
            return False
        if not exact:
            return True
        return (
            str(plan["notification_plan_id"]) == params["notification_plan_id"]
            and str(plan["candidate_group_id"]) == params["candidate_group_id"]
            and plan["dedupe_subject_key"] == params["dedupe_subject_key"]
        )

    def _render_count(self, params: dict[str, Any]) -> int:
        return sum(
            1
            for render in self.renders
            if str(render["notification_plan_id"]) == params["notification_plan_id"]
            and render["render_hash"] == params["render_hash"]
        )

    def _delivery_record_count(self, params: dict[str, Any]) -> int:
        return sum(1 for record in self.delivery_records if self._delivery_record_matches(record, params))

    def _delivery_record_matches(self, record: dict[str, Any], params: dict[str, Any]) -> bool:
        return (
            str(record["notification_delivery_record_id"]) == params["notification_delivery_record_id"]
            and str(record["notification_plan_id"]) == params["notification_plan_id"]
            and record["delivery_status"] == params["delivery_status"]
            and record.get("telegram_chat_id") == params["telegram_chat_id"]
            and record.get("telegram_message_id") == params["telegram_message_id"]
            and int(record["attempt_count"]) == int(params["attempt_count"])
            and record.get("transport_error_code") == params["transport_error_code"]
        )

    def _event_matches(self, event: dict[str, Any], params: dict[str, Any]) -> bool:
        payload = event["payload_json"]
        return (
            str(event["event_id"]) == params["delivery_result_event_id"]
            and event["event_type"] == "notification.delivery.result.v1"
            and event["aggregate_type"] == "notification_plan"
            and str(event["aggregate_id"]) == params["notification_plan_id"]
            and event["dedupe_key"] == params["dedupe_key"]
            and _json_text(payload, "notification_plan_id") == params["notification_plan_id_text"]
            and _json_text(payload, "notification_delivery_record_id")
            == params["notification_delivery_record_id_text"]
            and _json_text(payload, "delivery_status") == params["delivery_status"]
            and _json_text(payload, "attempt_count") == params["attempt_count_text"]
            and _json_text(payload, "telegram_chat_id") == params["telegram_chat_id_text"]
            and _json_text(payload, "telegram_message_id") == params["telegram_message_id_text"]
            and _json_text(payload, "transport_error_code") == params["transport_error_code"]
            and _json_text(payload, "edited") == params["edited_text"]
        )


def _json_text(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _state(*, current_event_status: str = "published") -> dict[str, Any]:
    plan_id = uuid4()
    analysis_id = uuid4()
    candidate_group_id = uuid4()
    old_delivery_record_id = uuid4()
    current_delivery_record_id = uuid4()
    old_event_id = uuid4()
    current_event_id = uuid4()
    target_chat_id = 12345
    current_message_id = 777
    created_at = datetime.now(timezone.utc)
    return {
        "plan": {
            "notification_plan_id": plan_id,
            "analysis_id": analysis_id,
            "candidate_group_id": candidate_group_id,
            "target_chat_id": target_chat_id,
            "dedupe_subject_key": "subject",
            "material_change_hash": "material",
            "status": "sent",
        },
        "render": {
            "notification_plan_id": plan_id,
            "render_hash": "render-hash",
        },
        "old_delivery_record": {
            "notification_delivery_record_id": old_delivery_record_id,
            "notification_plan_id": plan_id,
            "delivery_status": "suppressed",
            "telegram_chat_id": None,
            "telegram_message_id": None,
            "attempt_count": 0,
            "transport_error_code": "notification_send_flag_disabled",
            "created_at": created_at,
        },
        "current_delivery_record": {
            "notification_delivery_record_id": current_delivery_record_id,
            "notification_plan_id": plan_id,
            "delivery_status": "sent",
            "telegram_chat_id": target_chat_id,
            "telegram_message_id": current_message_id,
            "attempt_count": 2,
            "transport_error_code": None,
            "created_at": created_at,
        },
        "old_event": {
            "event_id": old_event_id,
            "event_type": "notification.delivery.result.v1",
            "aggregate_type": "notification_plan",
            "aggregate_id": plan_id,
            "dedupe_key": f"notification-delivery-result:{plan_id}:{old_delivery_record_id}",
            "payload_json": {
                "notification_plan_id": str(plan_id),
                "notification_delivery_record_id": str(old_delivery_record_id),
                "delivery_status": "suppressed",
                "telegram_chat_id": None,
                "telegram_message_id": None,
                "attempt_count": 0,
                "transport_error_code": "notification_send_flag_disabled",
                "edited": False,
            },
            "status": "pending",
        },
        "current_event": {
            "event_id": current_event_id,
            "event_type": "notification.delivery.result.v1",
            "aggregate_type": "notification_plan",
            "aggregate_id": plan_id,
            "dedupe_key": f"notification-delivery-result:{plan_id}:{current_delivery_record_id}",
            "payload_json": {
                "notification_plan_id": str(plan_id),
                "notification_delivery_record_id": str(current_delivery_record_id),
                "delivery_status": "sent",
                "telegram_chat_id": target_chat_id,
                "telegram_message_id": current_message_id,
                "attempt_count": 2,
                "transport_error_code": None,
                "edited": False,
            },
            "status": current_event_status,
        },
    }


async def _load_readback(
    state: dict[str, Any],
    *,
    delivery_result_event_id: UUID | None = None,
    notification_delivery_record_id: UUID | None = None,
    telegram_message_id: int | None = None,
    attempt_count: int | None = None,
) -> tuple[dict[str, Any], _ReadbackSession]:
    plan = state["plan"]
    current_record = state["current_delivery_record"]
    current_event = state["current_event"]
    session = _ReadbackSession(
        plans=[plan],
        renders=[state["render"]],
        delivery_records=[state["old_delivery_record"], current_record],
        events=[state["old_event"], current_event],
    )
    repository = NotifierTelegramRepository(session)
    readback = await repository.load_bounded_notification_send_readback(
        notification_plan_id=plan["notification_plan_id"],
        analysis_id=plan["analysis_id"],
        candidate_group_id=plan["candidate_group_id"],
        target_chat_id=plan["target_chat_id"],
        dedupe_subject_key=plan["dedupe_subject_key"],
        material_change_hash=plan["material_change_hash"],
        render_hash=state["render"]["render_hash"],
        notification_delivery_record_id=notification_delivery_record_id
        or current_record["notification_delivery_record_id"],
        delivery_result_event_id=delivery_result_event_id or current_event["event_id"],
        delivery_status=current_record["delivery_status"],
        telegram_chat_id=current_record["telegram_chat_id"],
        telegram_message_id=telegram_message_id
        if telegram_message_id is not None
        else current_record["telegram_message_id"],
        attempt_count=attempt_count if attempt_count is not None else current_record["attempt_count"],
        transport_error_code=current_record["transport_error_code"],
        edited=False,
    )
    return readback, session


@pytest.mark.asyncio
async def test_exact_current_published_delivery_result_is_ack_safe_with_stale_suppressed_history() -> None:
    readback, session = await _load_readback(_state())

    durable = _durable_readback_from_raw(
        readback,
        q_maintenance_route_verified=True,
        q_maintenance_message_thin=True,
    )

    assert durable.ack_safe is True
    assert durable.notification_plan_count == 1
    assert durable.notification_render_count == 1
    assert durable.notification_delivery_record_count == 1
    assert durable.notification_delivery_result_event_count == 1
    assert durable.delivery_result_event_status == "published"
    event_sql = next(sql for sql, _params in session.calls if "FROM event_outbox" in sql)
    assert "payload_json ->> 'notification_plan_id' = :notification_plan_id_text" in event_sql
    assert "payload_json ->> 'notification_delivery_record_id' = :notification_delivery_record_id_text" in event_sql
    assert "payload_json ->> 'notification_plan_id' = :notification_plan_id\n" not in event_sql
    delivery_sql = next(sql for sql, _params in session.calls if "FROM notification_delivery_records" in sql)
    assert "telegram_chat_id IS NOT DISTINCT FROM CAST(:telegram_chat_id AS bigint)" in delivery_sql
    assert "attempt_count = CAST(:attempt_count AS integer)" in delivery_sql


@pytest.mark.asyncio
async def test_stale_pending_event_does_not_authorize_ack_when_exact_current_event_is_missing() -> None:
    readback, _session = await _load_readback(_state(), delivery_result_event_id=uuid4())

    durable = _durable_readback_from_raw(
        readback,
        q_maintenance_route_verified=True,
        q_maintenance_message_thin=True,
    )

    assert durable.ack_safe is False
    assert durable.notification_delivery_record_count == 1
    assert durable.notification_delivery_result_event_count == 0
    assert "notification_delivery_result_event_readback_not_exactly_once" in durable.checks_failed
    assert "notification_delivery_result_event_not_published" in durable.checks_failed


@pytest.mark.asyncio
async def test_exact_current_unpublished_event_remains_ack_unsafe_even_with_stale_pending_event() -> None:
    readback, _session = await _load_readback(_state(current_event_status="pending"))

    durable = _durable_readback_from_raw(
        readback,
        q_maintenance_route_verified=True,
        q_maintenance_message_thin=True,
    )

    assert durable.ack_safe is False
    assert durable.notification_delivery_result_event_count == 1
    assert durable.delivery_result_event_status == "pending"
    assert durable.checks_failed == ("notification_delivery_result_event_not_published",)


@pytest.mark.asyncio
async def test_mismatched_delivery_record_or_delivery_fields_remain_ack_unsafe() -> None:
    readback_wrong_record, _session = await _load_readback(
        _state(),
        notification_delivery_record_id=uuid4(),
    )
    readback_wrong_message, _session = await _load_readback(_state(), telegram_message_id=778)
    readback_wrong_attempt, _session = await _load_readback(_state(), attempt_count=1)

    wrong_record = _durable_readback_from_raw(
        readback_wrong_record,
        q_maintenance_route_verified=True,
        q_maintenance_message_thin=True,
    )
    wrong_message = _durable_readback_from_raw(
        readback_wrong_message,
        q_maintenance_route_verified=True,
        q_maintenance_message_thin=True,
    )
    wrong_attempt = _durable_readback_from_raw(
        readback_wrong_attempt,
        q_maintenance_route_verified=True,
        q_maintenance_message_thin=True,
    )

    assert wrong_record.ack_safe is False
    assert wrong_record.notification_delivery_record_count == 0
    assert wrong_record.notification_delivery_result_event_count == 0
    assert "notification_delivery_record_readback_not_exactly_once" in wrong_record.checks_failed
    assert wrong_message.ack_safe is False
    assert wrong_message.notification_delivery_record_count == 0
    assert wrong_message.notification_delivery_result_event_count == 0
    assert wrong_attempt.ack_safe is False
    assert wrong_attempt.notification_delivery_record_count == 0
    assert wrong_attempt.notification_delivery_result_event_count == 0


def test_current_event_route_and_thin_message_checks_remain_required_for_ack_safety() -> None:
    durable = _durable_readback_from_raw(
        {
            "notification_plan_count": 1,
            "notification_plan_material_count": 1,
            "notification_render_count": 1,
            "notification_delivery_record_count": 1,
            "notification_delivery_result_event_count": 1,
            "delivery_result_event_status": "published",
        },
        q_maintenance_route_verified=False,
        q_maintenance_message_thin=False,
    )

    assert durable.ack_safe is False
    assert durable.checks_failed == (
        "q_maintenance_route_readback_failed",
        "q_maintenance_thin_message_readback_failed",
    )
