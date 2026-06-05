from __future__ import annotations

from uuid import UUID

from services.maintenance.models import SelectedPlanRecoveryRow


OPEN_REPLAY_STATUSES = {"requested", "dispatched", "pending"}


class FakeSelectedPlanReplayRepository:
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
        inserted = 0
        for plan_id in plan_ids:
            if self.has_open_replay_request(plan_id):
                continue
            self.replay_requests.append(
                {
                    "replay_type": "delivery",
                    "root_object_type": "notification_plan",
                    "root_object_id": plan_id,
                    "requested_by": requested_by,
                    "status": "requested",
                }
            )
            inserted += 1
        return inserted

    def add_open_replay_request(self, plan_id: UUID, *, status: str = "requested") -> None:
        self.replay_requests.append(
            {
                "replay_type": "delivery",
                "root_object_type": "notification_plan",
                "root_object_id": plan_id,
                "requested_by": "existing/operator",
                "status": status,
            }
        )

    def has_open_replay_request(self, plan_id: UUID) -> bool:
        return any(
            row["replay_type"] == "delivery"
            and row["root_object_type"] == "notification_plan"
            and row["root_object_id"] == plan_id
            and row["status"] in OPEN_REPLAY_STATUSES
            for row in self.replay_requests
        )
