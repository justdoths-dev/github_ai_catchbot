from __future__ import annotations

import pytest

from services.maintenance.worker import DueRetryPromotionWorker

from ._fakes import config


class FakeDueRetryService:
    def __init__(self, processed: int = 1) -> None:
        self.processed = processed
        self.promote_calls = 0
        self.limits: list[int | None] = []

    async def handle_maintenance_trigger_event(self, trigger_event_id: str) -> None:
        raise AssertionError("due retry worker must not consume q.maintenance")

    async def handle_replay_trigger_event(self, trigger_event_id: str) -> None:
        raise AssertionError("due retry worker must not consume q.replay")

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        self.promote_calls += 1
        self.limits.append(limit)
        return self.processed


@pytest.mark.asyncio
async def test_due_retry_worker_run_once_calls_due_scan_without_queue_consumer() -> None:
    service = FakeDueRetryService(processed=2)
    worker = DueRetryPromotionWorker(config(), service=service)

    result = await worker.run_once()

    assert result.processed == 2
    assert result.acked == 0
    assert service.promote_calls == 1
    assert service.limits == [None]
    assert not hasattr(worker, "_consumer")


@pytest.mark.asyncio
async def test_due_retry_worker_stops_cleanly() -> None:
    service = FakeDueRetryService(processed=0)
    worker = DueRetryPromotionWorker(config(), service=service)

    await worker.stop()
    await worker.run_forever()

    assert service.promote_calls == 0
