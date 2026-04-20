"""Bootstrap skeleton for the Telegram collector service."""

from .auth_fsm import AuthorizationFSM, AuthTransitionResult
from .config import CollectorTelegramConfig
from .exceptions import (
    AuthorizationError,
    AuthorizationManualInterventionRequired,
    CollectorError,
    CollectorTelegramConfigError,
    CollectorTelegramError,
    CollectorTelegramLifecycleError,
    CollectorTelegramRuntimeError,
    ConfigurationError,
    ReconcileRetryableError,
    ReconcileTerminalError,
    RepositoryInvariantError,
    TDLibTransportError,
    UpdateApplyRetryableError,
    UpdateApplyTerminalError,
)
from .idempotency import IdempotencyPolicy
from .message_projection import MessageProjectionBuilder
from .models import (
    CollectorEnvironment,
    CollectorLifecycleState,
    CollectorMode,
    OutboxDraft,
    OutboxEventDraft,
    ReconcileSummary,
    RuntimeSnapshot,
    SourceMessageProjection,
    SourceMessageVersionProjection,
    TrackedChat,
)
from .outbox import CollectorOutboxBuilder
from .reconcile import ReconcileService
from .registry_sync import ChannelRegistrySyncService, RegistrySyncSummary
from .repositories import CollectorRepository
from .runtime import CollectorTelegramRuntime
from .service import CollectorTelegramService
from .tdlib_client import TDLibClient, TDLibRequest, TDLibTransportProtocol
from .update_dispatcher import DispatchContext, UpdateDispatcher
from .update_handlers import CollectorUpdateHandlers, UpdateHandlingResult

__all__ = [
    'AuthTransitionResult',
    'AuthorizationError',
    'AuthorizationFSM',
    'AuthorizationManualInterventionRequired',
    'ChannelRegistrySyncService',
    'CollectorEnvironment',
    'CollectorError',
    'CollectorLifecycleState',
    'CollectorMode',
    'CollectorOutboxBuilder',
    'CollectorRepository',
    'CollectorTelegramConfig',
    'CollectorTelegramConfigError',
    'CollectorTelegramError',
    'CollectorTelegramLifecycleError',
    'CollectorTelegramRuntime',
    'CollectorTelegramRuntimeError',
    'CollectorTelegramService',
    'CollectorUpdateHandlers',
    'ConfigurationError',
    'DispatchContext',
    'IdempotencyPolicy',
    'MessageProjectionBuilder',
    'OutboxDraft',
    'OutboxEventDraft',
    'ReconcileRetryableError',
    'ReconcileService',
    'ReconcileSummary',
    'ReconcileTerminalError',
    'RegistrySyncSummary',
    'RepositoryInvariantError',
    'RuntimeSnapshot',
    'SourceMessageProjection',
    'SourceMessageVersionProjection',
    'TDLibClient',
    'TDLibRequest',
    'TDLibTransportError',
    'TDLibTransportProtocol',
    'TrackedChat',
    'UpdateApplyRetryableError',
    'UpdateApplyTerminalError',
    'UpdateDispatcher',
    'UpdateHandlingResult',
]
