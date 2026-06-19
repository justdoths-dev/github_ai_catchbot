from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local validation fallback only
    sa = None

from .eligibility import stale_resolution_exclusion_not_exists_sql
from .models import OutboxEventRow


class AsyncSessionLike(Protocol):
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class OutboxRelayRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    async def fetch_pending_batch(self, *, limit: int) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(
                f"""
                SELECT
                    eo.event_id,
                    eo.event_type,
                    eo.aggregate_type,
                    eo.aggregate_id,
                    eo.dedupe_key,
                    eo.payload_json,
                    eo.status,
                    eo.fail_count,
                    eo.created_at
                FROM event_outbox eo
                WHERE eo.status = 'pending'::outbox_status_enum
                  AND {stale_resolution_exclusion_not_exists_sql("eo")}
                ORDER BY eo.created_at ASC, eo.event_id ASC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        rows: list[OutboxEventRow] = []
        for row in result.mappings().all():
            payload = row["payload_json"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            rows.append(
                OutboxEventRow(
                    event_id=row["event_id"],
                    event_type=row["event_type"],
                    aggregate_type=row["aggregate_type"],
                    aggregate_id=row["aggregate_id"],
                    dedupe_key=row["dedupe_key"],
                    payload_json=payload or {},
                    status=str(row["status"]),
                    fail_count=int(row["fail_count"]),
                    created_at=row["created_at"],
                )
            )
        return rows

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        published_at = published_at or datetime.now(timezone.utc)
        await self._session.execute(
            _sql(
                """
                UPDATE event_outbox
                SET
                    status = 'published'::outbox_status_enum,
                    published_at = :published_at,
                    last_error = NULL
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {
                "event_id": str(event_id),
                "published_at": published_at,
            },
        )

    async def mark_failed(self, *, event_id: UUID, error_text: str) -> None:
        await self._session.execute(
            _sql(
                """
                UPDATE event_outbox
                SET
                    status = 'failed'::outbox_status_enum,
                    fail_count = fail_count + 1,
                    last_error = :error_text
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {
                "event_id": str(event_id),
                "error_text": error_text,
            },
        )

    async def reset_failed_to_pending(self, *, event_ids: Iterable[UUID]) -> None:
        values = [str(v) for v in event_ids]
        if not values:
            return
        await self._session.execute(
            _sql(
                """
                UPDATE event_outbox
                SET status = 'pending'::outbox_status_enum
                WHERE event_id = ANY(CAST(:event_ids AS uuid[]))
                  AND status = 'failed'::outbox_status_enum
                """
            ),
            {"event_ids": values},
        )

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        await self._session.execute(
            _sql(
                """
                INSERT INTO job_attempts (
                    stage_name,
                    queue_name,
                    root_object_type,
                    root_object_id,
                    attempt_no,
                    lease_owner,
                    started_at,
                    finished_at,
                    attempt_status,
                    error_code,
                    retry_after_at
                ) VALUES (
                    :stage_name,
                    :queue_name,
                    :root_object_type,
                    CAST(:root_object_id AS uuid),
                    1,
                    NULL,
                    now(),
                    now(),
                    CAST(:attempt_status AS job_attempt_status_enum),
                    :error_code,
                    NULL
                )
                """
            ),
            {
                "stage_name": stage_name,
                "queue_name": queue_name,
                "root_object_type": root_object_type,
                "root_object_id": str(root_object_id),
                "attempt_status": attempt_status,
                "error_code": error_code,
            },
        )


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)
