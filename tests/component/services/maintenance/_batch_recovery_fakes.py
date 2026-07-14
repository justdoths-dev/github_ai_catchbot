from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

from services.maintenance.models import SelectedPlanRecoveryRow
from services.maintenance.repositories import MaintenanceRepository


OPEN_REPLAY_STATUSES = {"requested", "dispatched", "pending"}


class FakeSelectedPlanReplayRepository:
    def __init__(
        self,
        rows: list[SelectedPlanRecoveryRow],
        *,
        atomic_session: "AtomicReplayInsertSession | None" = None,
    ) -> None:
        self.rows = {row.notification_plan_id: row for row in rows}
        self.load_calls: list[list[UUID]] = []
        self._atomic_repository = MaintenanceRepository(atomic_session) if atomic_session is not None else None
        self.replay_requests = atomic_session.committed_replay_requests if atomic_session is not None else []
        self.event_outbox = atomic_session.committed_replay_events if atomic_session is not None else []
        self.job_attempts: list[dict] = []
        self.notification_plan_mutations: list[dict] = []
        self.notification_delivery_record_mutations: list[dict] = []

    def transaction(self):
        if self._atomic_repository is None:
            raise AssertionError("transaction support requires an atomic session")
        return self._atomic_repository.transaction()

    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]):
        self.load_calls.append(notification_plan_ids)
        if self._atomic_repository is not None:
            return [
                replace(
                    self.rows[plan_id],
                    has_open_replay_request=self.has_open_replay_request(plan_id),
                )
                for plan_id in notification_plan_ids
                if plan_id in self.rows
            ]
        return [self.rows[plan_id] for plan_id in notification_plan_ids if plan_id in self.rows]

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[UUID], requested_by: str) -> int:
        if self._atomic_repository is not None:
            return await self._atomic_repository.insert_replay_requests_for_selected_plans(
                plan_ids=plan_ids,
                requested_by=requested_by,
            )
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


class _ReplayInsertCountResult:
    def __init__(self, *, replay_request_count: int, replay_event_count: int) -> None:
        self._row = {
            "replay_request_count": replay_request_count,
            "replay_event_count": replay_event_count,
        }

    def mappings(self):
        return self

    def one(self):
        return self._row


class _AtomicReplayInsertTransaction:
    def __init__(self, session: "AtomicReplayInsertSession") -> None:
        self._session = session

    async def __aenter__(self):
        self._session.active_transaction = True
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        del exc, tb
        self._session.active_transaction = False
        if exc_type is None:
            self._session.committed_replay_requests.extend(self._session.pending_replay_requests)
            self._session.committed_replay_events.extend(self._session.pending_replay_events)
        else:
            self._session.rolled_back = True
        self._session.pending_replay_requests.clear()
        self._session.pending_replay_events.clear()
        return False


class AtomicReplayInsertSession:
    def __init__(self, *, replay_event_count_override: int | None = None) -> None:
        self.replay_event_count_override = replay_event_count_override
        self.active_transaction = False
        self.rolled_back = False
        self.pending_replay_requests: list[dict] = []
        self.pending_replay_events: list[dict] = []
        self.committed_replay_requests: list[dict] = []
        self.committed_replay_events: list[dict] = []
        self.statement = ""
        self.params: dict | None = None
        self.execute_calls = 0

    def in_transaction(self) -> bool:
        return self.active_transaction

    def begin(self):
        return _AtomicReplayInsertTransaction(self)

    async def execute(self, statement, params=None):
        if not self.active_transaction:
            raise AssertionError("replay request/event insert must run inside the existing transaction")
        self.execute_calls += 1
        self.statement = str(statement)
        self.params = params
        open_plan_ids = {row["root_object_id"] for row in self.committed_replay_requests}
        new_plan_ids: list[UUID] = []
        for raw_plan_id in params["plan_ids"]:
            plan_id = UUID(str(raw_plan_id))
            if plan_id not in open_plan_ids and plan_id not in new_plan_ids:
                new_plan_ids.append(plan_id)

        inserted_requests: list[dict] = []
        for plan_id in new_plan_ids:
            replay_request_id = uuid4()
            inserted_requests.append(
                {
                    "replay_request_id": replay_request_id,
                    "replay_type": "delivery",
                    "root_object_type": "notification_plan",
                    "root_object_id": plan_id,
                    "requested_by": params["requested_by"],
                    "status": "requested",
                }
            )
        replay_event_count = (
            len(inserted_requests)
            if self.replay_event_count_override is None
            else self.replay_event_count_override
        )
        inserted_events = [
            {
                "event_id": uuid4(),
                "event_type": "replay.requested.v1",
                "aggregate_type": "replay_request",
                "aggregate_id": request["replay_request_id"],
                "dedupe_key": f"maintenance:replay-requested:v1:{request['replay_request_id']}",
                "payload_json": {
                    "replay_request_id": str(request["replay_request_id"]),
                    "replay_type": "delivery",
                    "root_object_type": "notification_plan",
                    "root_object_id": str(request["root_object_id"]),
                    "replay_reason": "batch_recovery_replay_selected",
                },
                "status": "pending",
            }
            for request in inserted_requests[:replay_event_count]
        ]
        self.pending_replay_requests.extend(inserted_requests)
        self.pending_replay_events.extend(inserted_events)
        return _ReplayInsertCountResult(
            replay_request_count=len(inserted_requests),
            replay_event_count=len(inserted_events),
        )
