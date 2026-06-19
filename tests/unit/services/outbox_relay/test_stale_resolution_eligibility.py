from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from src.services.outbox_relay.eligibility import (
    DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
    DELIVERY_RESULT_FAILED_TERMINAL_DLQ_RECEIPT_CODE,
    DELIVERY_RESULT_SUPERSEDED_NOOP_RECEIPT_CODE,
    DELIVERY_RESULT_SUPPRESSED_NOOP_RECEIPT_CODE,
    DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
    EVENT_OUTBOX_ROOT_OBJECT_TYPE,
    JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE,
    MAINTENANCE_DELIVERY_RESULT_STAGE,
    MAINTENANCE_QUEUE_NAME,
    MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE,
    POLICY_APPLY_STALE_PROOF_ERROR_CODE,
)
from src.services.outbox_relay.repositories import OutboxRelayRepository


def _uuid(suffix: str) -> UUID:
    return UUID(hex=f"00000000000040008000{suffix.rjust(12, '0')}")


JUDGE_EVENT_ID = _uuid("aaa1")
POLICY_EVENT_ID = _uuid("aaa2")
DELIVERY_EVENT_ID = _uuid("aaa3")
UNKNOWN_EVENT_ID = _uuid("aaa4")


class _Result:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _RelaySession:
    def __init__(self, events: list[dict[str, object]], proofs: list[dict[str, object]] | None = None) -> None:
        self.events = events
        self.proofs = proofs or []
        self.statements: list[str] = []

    async def execute(self, statement, params=None):
        text = getattr(statement, "text", str(statement))
        self.statements.append(text)
        limit = int((params or {}).get("limit", 100))
        rows = [event for event in self.events if _canonical_relay_eligible(event, self.proofs)]
        rows.sort(key=lambda row: (row["created_at"], row["event_id"]))
        return _Result(rows[:limit])


def _event(event_id: UUID, event_type: str, *, status: str = "pending") -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": "event_outbox",
        "aggregate_id": uuid4(),
        "dedupe_key": f"dedupe:{event_id.hex[-8:]}",
        "payload_json": {},
        "status": status,
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _proof(event_id: UUID, error_code: str, **overrides: object) -> dict[str, object]:
    proof = {
        "stage_name": MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE,
        "queue_name": MAINTENANCE_QUEUE_NAME,
        "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
        "root_object_id": event_id,
        "attempt_status": "succeeded",
        "error_code": error_code,
    }
    proof.update(overrides)
    return proof


def _delivery_proof(
    event_id: UUID,
    error_code: str = DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
    **overrides: object,
) -> dict[str, object]:
    proof = _proof(event_id, error_code, stage_name=MAINTENANCE_DELIVERY_RESULT_STAGE)
    proof.update(overrides)
    return proof


def _canonical_relay_eligible(event: dict[str, object], proofs: list[dict[str, object]]) -> bool:
    if event["status"] != "pending":
        return False
    expected = {
        "judge.output.ready.v1": (
            MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE,
            {JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE},
        ),
        "analysis.policy.apply.v1": (
            MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE,
            {POLICY_APPLY_STALE_PROOF_ERROR_CODE},
        ),
        "notification.delivery.result.v1": (
            MAINTENANCE_DELIVERY_RESULT_STAGE,
            {
                DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE,
                DELIVERY_RESULT_SUPPRESSED_NOOP_RECEIPT_CODE,
                DELIVERY_RESULT_SUPERSEDED_NOOP_RECEIPT_CODE,
                DELIVERY_RESULT_FAILED_RETRYABLE_RECEIPT_CODE,
                DELIVERY_RESULT_FAILED_TERMINAL_DLQ_RECEIPT_CODE,
            },
        ),
    }.get(event["event_type"])
    if expected is None:
        return True
    expected_stage, expected_errors = expected
    return not any(
        proof.get("stage_name") == expected_stage
        and proof.get("queue_name") == MAINTENANCE_QUEUE_NAME
        and proof.get("root_object_type") == EVENT_OUTBOX_ROOT_OBJECT_TYPE
        and proof.get("root_object_id") == event["event_id"]
        and proof.get("attempt_status") == "succeeded"
        and proof.get("error_code") in expected_errors
        for proof in proofs
    )


async def _fetch_ids(events: list[dict[str, object]], proofs: list[dict[str, object]] | None = None) -> tuple[list[UUID], str]:
    session = _RelaySession(events, proofs)
    rows = await OutboxRelayRepository(session).fetch_pending_batch(limit=100)
    return [row.event_id for row in rows], "\n".join(session.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_id", "event_type", "error_code"),
    [
        (JUDGE_EVENT_ID, "judge.output.ready.v1", JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE),
        (POLICY_EVENT_ID, "analysis.policy.apply.v1", POLICY_APPLY_STALE_PROOF_ERROR_CODE),
    ],
)
async def test_matching_maintenance_stale_resolution_proof_excludes_actual_relay_fetch(
    event_id: UUID,
    event_type: str,
    error_code: str,
) -> None:
    events = [_event(event_id, event_type)]

    before_ids, statement = await _fetch_ids(events)
    after_ids, _ = await _fetch_ids(events, [_proof(event_id, error_code)])

    assert before_ids == [event_id]
    assert after_ids == []
    assert "NOT EXISTS" in statement
    assert "maintenance_stale_outbox_hygiene" in statement
    assert "maintenance_delivery_result" in statement
    assert "stale_outbox_judge_output_ready_already_handed_off_logical_noop" in statement
    assert "stale_outbox_policy_apply_already_analyzed_logical_noop" in statement
    assert "delivery_result_terminal_success_handled" in statement


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proof",
    [
        _proof(JUDGE_EVENT_ID, "wrong_error_code"),
        _proof(JUDGE_EVENT_ID, JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE, stage_name="wrong_stage"),
        _proof(JUDGE_EVENT_ID, JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE, queue_name="wrong_queue"),
        _proof(JUDGE_EVENT_ID, JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE, root_object_type="wrong_root"),
        _proof(JUDGE_EVENT_ID, JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE, attempt_status="failed_terminal"),
        _proof(JUDGE_EVENT_ID, POLICY_APPLY_STALE_PROOF_ERROR_CODE),
    ],
)
async def test_non_matching_maintenance_proofs_do_not_exclude_judge_event(proof: dict[str, object]) -> None:
    ids, _statement = await _fetch_ids([_event(JUDGE_EVENT_ID, "judge.output.ready.v1")], [proof])

    assert ids == [JUDGE_EVENT_ID]


@pytest.mark.asyncio
async def test_policy_proof_cannot_suppress_judge_and_judge_proof_cannot_suppress_policy() -> None:
    events = [
        _event(JUDGE_EVENT_ID, "judge.output.ready.v1"),
        _event(POLICY_EVENT_ID, "analysis.policy.apply.v1"),
    ]
    proofs = [
        _proof(JUDGE_EVENT_ID, POLICY_APPLY_STALE_PROOF_ERROR_CODE),
        _proof(POLICY_EVENT_ID, JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE),
    ]

    ids, _statement = await _fetch_ids(events, proofs)

    assert ids == [JUDGE_EVENT_ID, POLICY_EVENT_ID]


@pytest.mark.asyncio
async def test_delivery_result_unknown_and_ordinary_pending_rows_remain_relay_eligible() -> None:
    events = [
        _event(DELIVERY_EVENT_ID, "notification.delivery.result.v1"),
        _event(UNKNOWN_EVENT_ID, "unknown.event.v1"),
    ]
    proofs = [
        _proof(DELIVERY_EVENT_ID, JUDGE_OUTPUT_READY_STALE_PROOF_ERROR_CODE),
        _proof(UNKNOWN_EVENT_ID, POLICY_APPLY_STALE_PROOF_ERROR_CODE),
    ]

    ids, _statement = await _fetch_ids(events, proofs)

    assert ids == [DELIVERY_EVENT_ID, UNKNOWN_EVENT_ID]


@pytest.mark.asyncio
async def test_exact_delivery_result_receipt_excludes_only_that_event_from_actual_relay_fetch() -> None:
    other_delivery_event_id = _uuid("aaa5")
    events = [
        _event(DELIVERY_EVENT_ID, "notification.delivery.result.v1"),
        _event(other_delivery_event_id, "notification.delivery.result.v1"),
    ]

    before_ids, _statement = await _fetch_ids(events)
    after_ids, _statement = await _fetch_ids(events, [_delivery_proof(DELIVERY_EVENT_ID)])

    assert before_ids == [DELIVERY_EVENT_ID, other_delivery_event_id]
    assert after_ids == [other_delivery_event_id]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proof",
    [
        _delivery_proof(DELIVERY_EVENT_ID, "wrong_error_code"),
        _delivery_proof(DELIVERY_EVENT_ID, stage_name="wrong_stage"),
        _delivery_proof(DELIVERY_EVENT_ID, queue_name="wrong_queue"),
        _delivery_proof(DELIVERY_EVENT_ID, root_object_type="wrong_root"),
        _delivery_proof(DELIVERY_EVENT_ID, root_object_id=_uuid("bad1")),
        _delivery_proof(DELIVERY_EVENT_ID, attempt_status="failed_terminal"),
        _proof(DELIVERY_EVENT_ID, DELIVERY_RESULT_TERMINAL_SUCCESS_RECEIPT_CODE),
    ],
)
async def test_non_matching_delivery_result_receipts_do_not_exclude_delivery_event(proof: dict[str, object]) -> None:
    ids, _statement = await _fetch_ids([_event(DELIVERY_EVENT_ID, "notification.delivery.result.v1")], [proof])

    assert ids == [DELIVERY_EVENT_ID]


@pytest.mark.asyncio
async def test_delivery_result_receipt_cannot_exclude_judge_or_policy_events() -> None:
    events = [
        _event(JUDGE_EVENT_ID, "judge.output.ready.v1"),
        _event(POLICY_EVENT_ID, "analysis.policy.apply.v1"),
    ]
    proofs = [
        _delivery_proof(JUDGE_EVENT_ID),
        _delivery_proof(POLICY_EVENT_ID),
    ]

    ids, _statement = await _fetch_ids(events, proofs)

    assert ids == [JUDGE_EVENT_ID, POLICY_EVENT_ID]
