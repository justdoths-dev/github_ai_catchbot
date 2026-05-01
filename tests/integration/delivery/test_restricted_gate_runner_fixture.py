from __future__ import annotations

import pytest

from services.maintenance.delivery_gate_runner import DeliveryGateRunner

from tests.unit.services.maintenance.test_delivery_gate_runner import FakeGateRepository, _config, _snapshot


@pytest.mark.asyncio
async def test_restricted_gate_runner_fixture_passes_and_fails_on_dlq() -> None:
    healthy = await DeliveryGateRunner(_config(), repository=FakeGateRepository(_snapshot())).run(mode="restricted")
    with_dlq = await DeliveryGateRunner(
        _config(),
        repository=FakeGateRepository(_snapshot(open_delivery_dlq_count=1)),
    ).run(mode="restricted")

    assert healthy.gate_status == "pass"
    assert with_dlq.gate_status == "fail"
    assert with_dlq.blocking_reason_codes == ["delivery_gate_open_dlq_present"]
