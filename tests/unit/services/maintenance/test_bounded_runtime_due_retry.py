from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.maintenance.bounded_runtime import (
    BoundedMaintenanceDueRetryConfig,
    BoundedMaintenanceRuntimeConfig,
    run_bounded_maintenance_due_retry,
)
from services.maintenance.models import RetryPromotionCandidate
from tests.component.services.maintenance._fakes import config, latest_delivery_record, plan


class FakeDueRetryRuntime:
    def __init__(self, *, state, candidates, action_count: int = 0, raises: bool = False) -> None:
        self.state = state
        self.candidates = candidates
        self.action_count = action_count
        self.raises = raises
        self.preview_calls = []
        self.execute_calls = []

    async def preview_candidates(self, limit: int, now: datetime):
        self.state.database_read_attempted = True
        self.preview_calls.append((limit, now))
        return self.candidates[:limit]

    async def promote_due_retries_once(self, limit: int, now: datetime):
        self.state.database_read_attempted = True
        self.state.database_write_attempted = True
        self.state.service_called = True
        self.execute_calls.append((limit, now))
        if self.raises:
            raise RuntimeError("sentinel due retry failure with redacted locator")
        return self.action_count

    async def commit_database(self):
        self.state.database_committed = True

    async def rollback_database(self):
        self.state.database_rolled_back = True

    async def close(self):
        return None


def _runtime_loader():
    return BoundedMaintenanceRuntimeConfig(maintenance_config=config())


def _candidate(attempt_count: int = 1):
    notification_plan = plan(send_after=datetime.now(timezone.utc) - timedelta(seconds=1))
    latest = latest_delivery_record(
        notification_plan_id=notification_plan.notification_plan_id,
        attempt_count=attempt_count,
    )
    return RetryPromotionCandidate(
        plan=notification_plan,
        latest_delivery=latest,
        delivery_attempt_count=attempt_count,
    )


def _due_config(*, mode: str = "preview") -> BoundedMaintenanceDueRetryConfig:
    return BoundedMaintenanceDueRetryConfig(
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        allow_database_write=mode == "execute",
        mode=mode,
        limit=5,
        now_utc=datetime(2026, 6, 18, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_due_retry_preview_reports_suffixes_only_and_writes_nothing() -> None:
    candidates = [_candidate(), _candidate()]
    runtime_holder = {}

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        runtime = FakeDueRetryRuntime(state=state, candidates=candidates)
        runtime_holder["runtime"] = runtime
        return runtime

    result = await run_bounded_maintenance_due_retry(
        _due_config(),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["due_candidate_count"] == 2
    assert report["database_read_attempted"] is True
    assert report["database_write_attempted"] is False
    assert report["redis_read_attempted"] is False
    assert report["redis_consume_called"] is False
    assert report["redis_ack_attempted"] is False
    for candidate in candidates:
        assert str(candidate.plan.notification_plan_id) not in str(report)
        assert str(candidate.latest_delivery.notification_delivery_record_id) not in str(report)
    assert runtime_holder["runtime"].execute_calls == []


@pytest.mark.asyncio
async def test_due_retry_execute_calls_source_promotion_and_returns_action_count_without_redis() -> None:
    candidates = [_candidate()]
    runtime_holder = {}

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        runtime = FakeDueRetryRuntime(state=state, candidates=candidates, action_count=1)
        runtime_holder["runtime"] = runtime
        return runtime

    result = await run_bounded_maintenance_due_retry(
        _due_config(mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["due_action_count"] == 1
    assert report["database_committed"] is True
    assert report["database_write_attempted"] is True
    assert report["redis_client_created"] is False
    assert report["redis_consume_called"] is False
    assert report["redis_ack_attempted"] is False
    assert len(runtime_holder["runtime"].execute_calls) == 1


@pytest.mark.asyncio
async def test_due_retry_execute_failure_rolls_back_and_redacts_exception_detail() -> None:
    candidates = [_candidate()]

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        return FakeDueRetryRuntime(state=state, candidates=candidates, raises=True)

    result = await run_bounded_maintenance_due_retry(
        _due_config(mode="execute"),
        runtime_config_loader=_runtime_loader,
        runtime_builder=builder,
    )

    report = result.to_sanitized_dict()
    assert result.ok is False
    assert result.error_code == "due_retry_execution_failed"
    assert report["database_rolled_back"] is True
    assert "redacted locator" not in str(report)


@pytest.mark.asyncio
async def test_due_retry_preview_requires_database_read_gate() -> None:
    result = await run_bounded_maintenance_due_retry(
        BoundedMaintenanceDueRetryConfig(
            operator_approved=True,
            allow_runtime_config=True,
            allow_database_read=False,
            mode="preview",
            limit=1,
        ),
        runtime_config_loader=_runtime_loader,
        runtime_builder=None,
    )

    assert result.ok is False
    assert result.error_code == "database_read_not_allowed"
