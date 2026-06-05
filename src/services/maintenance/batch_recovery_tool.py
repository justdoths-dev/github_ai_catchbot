from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID, uuid4

from .batch_recovery import (
    BATCH_RECOVERY_OPEN_REPLAY_EXISTS,
    BATCH_RECOVERY_PLAN_MISSING,
    classify_selected_plan_for_delivery_replay,
)
from .config import MaintenanceConfig
from .models import RecoveryBatchResult, SelectedPlanRecoveryRow


class BatchRecoveryRepository(Protocol):
    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]) -> list[SelectedPlanRecoveryRow]: ...

    async def insert_replay_requests_for_selected_plans(
        self,
        *,
        plan_ids: list[UUID],
        requested_by: str,
    ) -> int: ...

    async def insert_manual_retry_intent_outbox(
        self,
        *,
        row: SelectedPlanRecoveryRow,
        recovery_batch_id: str,
        dedupe_key: str,
        payload_json: dict,
    ) -> bool: ...


class DeliveryBatchRecoveryTool:
    def __init__(
        self,
        config: MaintenanceConfig,
        *,
        repository: BatchRecoveryRepository,
        now_fn=None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    async def replay_selected(self, *, plan_ids: list[str | UUID], requested_by: str) -> RecoveryBatchResult:
        recovery_batch_id = str(uuid4())
        normalized_plan_ids, skipped = _normalize_plan_ids(plan_ids)
        rows = await self._repository.load_selected_recovery_rows(normalized_plan_ids)
        rows_by_id = {row.notification_plan_id: row for row in rows}
        accepted_plan_ids: list[UUID] = []

        for plan_id in normalized_plan_ids:
            row = rows_by_id.get(plan_id)
            reason = BATCH_RECOVERY_PLAN_MISSING if row is None else self._validate_replay_row(row)
            if reason is None and row is not None:
                accepted_plan_ids.append(row.notification_plan_id)
            else:
                _increment(skipped, reason)

        inserted_count = await self._repository.insert_replay_requests_for_selected_plans(
            plan_ids=accepted_plan_ids,
            requested_by=requested_by,
        )
        if inserted_count < len(accepted_plan_ids):
            skipped[BATCH_RECOVERY_OPEN_REPLAY_EXISTS] = len(accepted_plan_ids) - inserted_count

        return RecoveryBatchResult(
            recovery_batch_id=recovery_batch_id,
            recovery_mode="replay-selected",
            selected_count=len(plan_ids),
            accepted_count=len(accepted_plan_ids),
            skipped_count=len(plan_ids) - inserted_count,
            emitted_count=inserted_count,
            skipped_reason_codes=skipped,
        )

    async def retry_selected_due(self, *, plan_ids: list[str | UUID], requested_by: str) -> RecoveryBatchResult:
        del requested_by
        recovery_batch_id = str(uuid4())
        normalized_plan_ids, skipped = _normalize_plan_ids(plan_ids)
        rows = await self._repository.load_selected_recovery_rows(normalized_plan_ids)
        rows_by_id = {row.notification_plan_id: row for row in rows}
        accepted_rows: list[SelectedPlanRecoveryRow] = []

        now = self._now_fn()
        for plan_id in normalized_plan_ids:
            row = rows_by_id.get(plan_id)
            reason = "notification_plan_not_found" if row is None else self._validate_due_retry_row(row, now=now)
            if reason is None and row is not None:
                accepted_rows.append(row)
            else:
                _increment(skipped, reason)

        emitted_count = 0
        for row in accepted_rows:
            inserted = await self._repository.insert_manual_retry_intent_outbox(
                row=row,
                recovery_batch_id=recovery_batch_id,
                dedupe_key=manual_retry_intent_dedupe_key(row),
                payload_json=manual_retry_intent_payload(row=row, recovery_batch_id=recovery_batch_id),
            )
            if inserted:
                emitted_count += 1
            else:
                _increment(skipped, "manual_retry_intent_exists_at_insert")

        return RecoveryBatchResult(
            recovery_batch_id=recovery_batch_id,
            recovery_mode="retry-selected-due",
            selected_count=len(plan_ids),
            accepted_count=len(accepted_rows),
            skipped_count=len(plan_ids) - emitted_count,
            emitted_count=emitted_count,
            skipped_reason_codes=skipped,
        )

    def _validate_replay_row(self, row: SelectedPlanRecoveryRow) -> str | None:
        return classify_selected_plan_for_delivery_replay(row)

    def _validate_due_retry_row(self, row: SelectedPlanRecoveryRow, *, now: datetime) -> str | None:
        if row.delivery_status != "failed_retryable":
            return "status_is_not_failed_retryable"
        if row.send_disabled:
            return "send_disabled_rows_require_replay"
        if row.send_after is None:
            return "send_after_missing"
        if _as_utc(row.send_after) > _as_utc(now):
            return "send_after_not_due_yet"
        if (row.attempt_count or 0) >= self._config.delivery_retry_max_attempts:
            return "retry_ceiling_exceeded"
        if row.has_open_replay_request:
            return "open_replay_request_exists"
        return None


def manual_retry_intent_payload(*, row: SelectedPlanRecoveryRow, recovery_batch_id: str) -> dict:
    return {
        "notification_plan_id": str(row.notification_plan_id),
        "analysis_id": str(row.analysis_id),
        "candidate_group_id": str(row.candidate_group_id),
        "delivery_decision": row.delivery_decision,
        "urgency_profile": row.urgency_profile,
        "target_chat_id": row.target_chat_id,
        "target_thread_id": row.target_thread_id,
        "render_profile": row.render_profile,
        "dedupe_subject_key": row.dedupe_subject_key,
        "material_change_hash": row.material_change_hash,
        "send_after": None,
        "retry_reason": "manual_selected_due_retry",
        "previous_attempt_count": row.attempt_count or 0,
        "recovery_batch_id": recovery_batch_id,
    }


def manual_retry_intent_dedupe_key(row: SelectedPlanRecoveryRow) -> str:
    send_after_epoch = "none"
    if row.send_after is not None:
        send_after_epoch = str(int(_as_utc(row.send_after).timestamp()))
    return f"notify:manual-retry-intent:{row.notification_plan_id}:{row.attempt_count or 0}:{send_after_epoch}"


def _normalize_plan_ids(plan_ids: list[str | UUID]) -> tuple[list[UUID], dict[str, int]]:
    normalized: list[UUID] = []
    seen: set[UUID] = set()
    skipped: dict[str, int] = {}
    for raw_plan_id in plan_ids:
        try:
            plan_id = raw_plan_id if isinstance(raw_plan_id, UUID) else UUID(str(raw_plan_id))
        except (TypeError, ValueError, AttributeError):
            _increment(skipped, "invalid_notification_plan_id")
            continue
        if plan_id in seen:
            _increment(skipped, "duplicate_notification_plan_id")
            continue
        seen.add(plan_id)
        normalized.append(plan_id)
    return normalized, skipped


def _increment(values: dict[str, int], key: str | None) -> None:
    if key is None:
        return
    values[key] = values.get(key, 0) + 1


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
