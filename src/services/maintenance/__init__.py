from __future__ import annotations

from typing import Any

__all__ = ["MaintenanceConfig", "MaintenanceService"]


def __getattr__(name: str) -> Any:
    if name == "MaintenanceConfig":
        from .config import MaintenanceConfig

        return MaintenanceConfig
    if name == "MaintenanceService":
        from .service import MaintenanceService

        return MaintenanceService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
