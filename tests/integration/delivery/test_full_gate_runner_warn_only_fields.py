from __future__ import annotations

import pytest

from services.maintenance.delivery_gate_runner import DeliveryGateRunner

from tests.unit.services.maintenance.test_delivery_gate_runner import FakeGateRepository, _config, _snapshot


@pytest.mark.asyncio
async def test_full_gate_runner_keeps_operator_review_and_duplicate_ratio_warn_only() -> None:
    report = await DeliveryGateRunner(
        _config(),
        repository=FakeGateRepository(_snapshot(duplicate_noop_ratio_1h=0.5)),
    ).run(mode="full")

    assert report.gate_status == "warn"
    assert report.blocking_reason_codes == []
    assert report.warning_reason_codes == [
        "delivery_gate_duplicate_noop_ratio_review_required",
        "delivery_gate_operator_review_required",
    ]
    assert report.metrics[-1].metric_name == "duplicate_noop_ratio_1h"
    assert report.metrics[-1].severity == "warn"
    assert report.metrics[-1].passed is False
