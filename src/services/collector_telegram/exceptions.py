from __future__ import annotations


class CollectorError(Exception):
    """Base class for collector-specific failures."""


class ConfigurationError(CollectorError):
    """Raised when configuration is invalid or incomplete."""


# Backward-compatibility alias for existing repo tests/imports.
CollectorTelegramConfigError = ConfigurationError


class AuthorizationError(CollectorError):
    """Raised when TDLib authorization flow is invalid or broken."""


class AuthorizationManualInterventionRequired(AuthorizationError):
    """Raised when operator action is required to continue authorization."""


class TDLibTransportError(CollectorError):
    """Raised for low-level TDLib transport failures."""


class RepositoryInvariantError(CollectorError):
    """Raised when persistence invariants are broken.

    These are normally terminal and should fail fast.
    """


class UpdateApplyRetryableError(CollectorError):
    """Raised when an update application may succeed on retry."""


class UpdateApplyTerminalError(CollectorError):
    """Raised when an update application must not be retried as-is."""


class ReconcileRetryableError(CollectorError):
    """Raised when reconcile may succeed on retry."""


class ReconcileTerminalError(CollectorError):
    """Raised when reconcile encountered a terminal condition."""


class SingletonViolationError(CollectorError):
    """Raised when prod single-instance collector guard is violated."""