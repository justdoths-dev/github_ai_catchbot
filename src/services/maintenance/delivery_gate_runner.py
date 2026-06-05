from __future__ import annotations

from .delivery_operations_gate import (
    FULL_DLQ_OLDEST_AGE_THRESHOLD_SEC,
    DeliveryOperationsGate,
    DeliveryOperationsGateRepository,
)


DeliveryGateRepository = DeliveryOperationsGateRepository


class DeliveryGateRunner(DeliveryOperationsGate):
    pass
