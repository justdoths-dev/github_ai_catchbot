from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID

from .models import SelectedPlanRecoveryRow


BatchRecoveryAction = Literal["replay_request_created", "skipped"]
BatchRecoveryStatus = Literal["completed", "rejected", "noop"]

BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED = "batch_recovery_operator_confirmation_required"
BATCH_RECOVERY_NO_SELECTED_PLANS = "batch_recovery_no_selected_plans"
BATCH_RECOVERY_PLAN_MISSING = "batch_recovery_plan_missing"
BATCH_RECOVERY_DUPLICATE_PLAN_ID = "batch_recovery_duplicate_plan_id"
BATCH_RECOVERY_OPEN_REPLAY_EXISTS = "batch_recovery_open_replay_exists"
BATCH_RECOVERY_DELIVERY_DLQ_BLOCKS_REPLAY = "batch_recovery_delivery_dlq_blocks_replay"
BATCH_RECOVERY_ALREADY_DELIVERED = "batch_recovery_already_delivered"
BATCH_RECOVERY_NOT_REPLAY_CANDIDATE = "batch_recovery_not_replay_candidate"
BATCH_RECOVERY_REPLAY_REQUEST_CREATED = "batch_recovery_replay_request_created"

EXPLICIT_DELIVERY_REPLAY_MANUAL_ACTION = "request_explicit_delivery_replay"
EXPLICIT_DELIVERY_REPLAY_HINT = "delivery_replay_from_notification_plan"

DELIVERED_DELIVERY_STATUSES = frozenset({"sent", "edited"})
EXPLICIT_REPLAY_PLAN_STATUSES = frozenset({"failed_terminal"})


class SelectedDeliveryReplayRepository(Protocol):
    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]) -> list[SelectedPlanRecoveryRow]: ...

    async def insert_replay_requests_for_selected_plans(
        self,
        *,
        plan_ids: list[UUID],
        requested_by: str,
    ) -> int: ...


@dataclass(slots=True, frozen=True)
class BatchRecoveryPlanResult:
    notification_plan_id: UUID
    action: BatchRecoveryAction
    reason_code: str
    replay_request_created: bool


@dataclass(slots=True, frozen=True)
class BatchRecoveryResult:
    status: BatchRecoveryStatus
    reason_code: str | None
    requested_count: int
    created_count: int
    skipped_count: int
    results: list[BatchRecoveryPlanResult]


async def prepare_delivery_replay_requests_for_selected_plans(
    *,
    repository: SelectedDeliveryReplayRepository,
    selected_plan_ids: list[UUID],
    requested_by: str,
    operator_confirmed: bool,
) -> BatchRecoveryResult:
    if not selected_plan_ids:
        return BatchRecoveryResult(
            status="noop",
            reason_code=BATCH_RECOVERY_NO_SELECTED_PLANS,
            requested_count=0,
            created_count=0,
            skipped_count=0,
            results=[],
        )

    if not operator_confirmed:
        return BatchRecoveryResult(
            status="rejected",
            reason_code=BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED,
            requested_count=len(selected_plan_ids),
            created_count=0,
            skipped_count=len(selected_plan_ids),
            results=[
                BatchRecoveryPlanResult(
                    notification_plan_id=plan_id,
                    action="skipped",
                    reason_code=BATCH_RECOVERY_OPERATOR_CONFIRMATION_REQUIRED,
                    replay_request_created=False,
                )
                for plan_id in selected_plan_ids
            ],
        )

    first_seen_plan_ids: list[UUID] = []
    seen_plan_ids: set[UUID] = set()
    for plan_id in selected_plan_ids:
        if plan_id in seen_plan_ids:
            continue
        seen_plan_ids.add(plan_id)
        first_seen_plan_ids.append(plan_id)

    rows = await repository.load_selected_recovery_rows(first_seen_plan_ids)
    rows_by_id = {row.notification_plan_id: row for row in rows}

    processed_plan_ids: set[UUID] = set()
    results: list[BatchRecoveryPlanResult] = []
    created_count = 0
    for plan_id in selected_plan_ids:
        if plan_id in processed_plan_ids:
            results.append(
                BatchRecoveryPlanResult(
                    notification_plan_id=plan_id,
                    action="skipped",
                    reason_code=BATCH_RECOVERY_DUPLICATE_PLAN_ID,
                    replay_request_created=False,
                )
            )
            continue
        processed_plan_ids.add(plan_id)

        row = rows_by_id.get(plan_id)
        if row is None:
            results.append(_skipped_result(plan_id, BATCH_RECOVERY_PLAN_MISSING))
            continue

        reason_code = classify_selected_plan_for_delivery_replay(row)
        if reason_code is not None:
            results.append(_skipped_result(plan_id, reason_code))
            continue

        inserted = await repository.insert_replay_requests_for_selected_plans(
            plan_ids=[plan_id],
            requested_by=requested_by,
        )
        if inserted == 1:
            created_count += 1
            results.append(
                BatchRecoveryPlanResult(
                    notification_plan_id=plan_id,
                    action="replay_request_created",
                    reason_code=BATCH_RECOVERY_REPLAY_REQUEST_CREATED,
                    replay_request_created=True,
                )
            )
        else:
            results.append(_skipped_result(plan_id, BATCH_RECOVERY_OPEN_REPLAY_EXISTS))

    return BatchRecoveryResult(
        status="completed",
        reason_code=None,
        requested_count=len(selected_plan_ids),
        created_count=created_count,
        skipped_count=len(results) - created_count,
        results=results,
    )


def classify_selected_plan_for_delivery_replay(row: SelectedPlanRecoveryRow) -> str | None:
    if row.has_open_replay_request:
        return BATCH_RECOVERY_OPEN_REPLAY_EXISTS
    if _is_delivered(row):
        return BATCH_RECOVERY_ALREADY_DELIVERED
    if row.has_delivery_dlq and not _delivery_dlq_allows_explicit_replay(row):
        return BATCH_RECOVERY_DELIVERY_DLQ_BLOCKS_REPLAY
    if _delivery_dlq_allows_explicit_replay(row):
        return None
    if row.delivery_status == "suppressed" and row.send_disabled:
        return None
    if row.delivery_status == "failed_terminal":
        return None
    if row.delivery_status is None and row.plan_status in EXPLICIT_REPLAY_PLAN_STATUSES:
        return None
    return BATCH_RECOVERY_NOT_REPLAY_CANDIDATE


def _skipped_result(plan_id: UUID, reason_code: str) -> BatchRecoveryPlanResult:
    return BatchRecoveryPlanResult(
        notification_plan_id=plan_id,
        action="skipped",
        reason_code=reason_code,
        replay_request_created=False,
    )


def _is_delivered(row: SelectedPlanRecoveryRow) -> bool:
    return row.delivery_status in DELIVERED_DELIVERY_STATUSES or row.plan_status in DELIVERED_DELIVERY_STATUSES


def _delivery_dlq_allows_explicit_replay(row: SelectedPlanRecoveryRow) -> bool:
    return (
        row.has_delivery_dlq
        and (
            row.delivery_dlq_next_manual_action == EXPLICIT_DELIVERY_REPLAY_MANUAL_ACTION
            or row.delivery_dlq_replay_hint == EXPLICIT_DELIVERY_REPLAY_HINT
        )
    )
