from __future__ import annotations

import asyncio
import copy
import contextlib
import errno
import hashlib
import json
import logging
import os
import stat
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher
from src.services.outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError

from .config import CollectorTelegramConfig
from .exceptions import TDLibTransportError
from .idempotency import IdempotencyPolicy
from .message_projection import MessageProjectionBuilder
from .models import SourceMessageProjection
from .outbox import CollectorOutboxBuilder
from .repositories import CollectorRepository
from .tdlib_client import TDJsonTransport, TDLibClient

SCHEMA_VERSION = "live_collector_one_channel_source_last_rollout_v1"
THREE_CHANNEL_SCHEMA_VERSION = "live_collector_three_channel_source_last_rollout_v1"
FULL_REGISTRY_SCHEMA_VERSION = "live_collector_full_registry_source_last_rollout_v1"
SEARCH_SCHEMA_VERSION = "github_url_live_target_bounded_search_v1"
TARGET_LOCATOR_SCHEMA_VERSION = "github_url_live_target_locator_v1"
TARGET_LOCATOR_MAX_BYTES = 2048
TARGET_LOCATOR_PRIVATE_MODE = 0o600
RUNNER_NAME = "bounded_collector_history_ingest_runner"
MODE_PLAN = "plan"
MODE_EXECUTE = "execute"
MODE_SEARCH = "search"
EXECUTE_CONFIRM_TOKEN = "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE"
THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN = "LIVE_COLLECTOR_3_CHANNEL_SOURCE_LAST_EXECUTE"
FULL_REGISTRY_EXECUTE_CONFIRM_TOKEN = "LIVE_COLLECTOR_FULL_REGISTRY_SOURCE_LAST_EXECUTE"
SEARCH_CONFIRM_TOKEN = "GITHUB_URL_LIVE_TARGET_BOUNDED_SEARCH"
THREE_CHANNEL_TARGET_COUNT = 3
MAX_FULL_REGISTRY_TARGETS = 30
ROLLOUT_SCOPE_EXACT_TARGETS = "exact_targets"
ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY = "full_tracked_registry"
DEFAULT_HISTORY_LIMIT = 10
MAX_HISTORY_LIMIT = 30
DEFAULT_MAX_MESSAGES = DEFAULT_HISTORY_LIMIT
MAX_MESSAGES_HARD_LIMIT = MAX_HISTORY_LIMIT
EXACT_TARGET_HISTORY_WINDOW_LIMIT_HARD_CAP = 3
EXACT_TARGET_HISTORY_WINDOW_OFFSETS = (0, -1, -2)
EXACT_TARGET_ANCHORLESS_HISTORY_WINDOW_LIMIT_HARD_CAP = 3
HISTORY_READ_TIMEOUT_SEC = 30.0
HISTORY_RECEIVE_TIMEOUT_SEC = 1.0
AUTH_READY_TIMEOUT_SEC = 30.0
AUTH_RECEIVE_TIMEOUT_SEC = 1.0
SOURCE_KIND_PUBLIC_USERNAME = "public_username"
QUEUE_NAME = "q.source.normalize"
STAGE_NAME = "normalize"
ROOT_OBJECT_TYPE = "source_message"
DEFAULT_XADD_MAXLEN = 10000

_INTERACTIVE_AUTHORIZATION_STATES = frozenset(
    {
        "authorizationStateWaitPhoneNumber",
        "authorizationStateWaitCode",
        "authorizationStateWaitOtherDeviceConfirmation",
        "authorizationStateWaitPassword",
    }
)
_TERMINAL_NOT_READY_AUTHORIZATION_STATES = frozenset(
    {
        "authorizationStateLoggingOut",
        "authorizationStateClosing",
        "authorizationStateClosed",
    }
)
_SOURCE_EVENT_TYPES = frozenset(
    {
        "source_message.created.v1",
        "source_message.edited.v1",
        "source_message.deleted.v1",
        "source_message.reconciled.v1",
    }
)
HISTORY_READ_CAUSE_REQUEST_CONSTRUCTION_REPAIR = "request_construction_repair"
HISTORY_READ_CAUSE_BOUNDED_WINDOW_ADJUSTMENT = "bounded_window_adjustment"
HISTORY_READ_CAUSE_RUNTIME_TDLIB_ACCESS_ISSUE = "runtime_tdlib_access_issue"
HISTORY_READ_CAUSE_TARGET_UNAVAILABLE = "target_unavailable"
_HISTORY_READ_FAILURE_CAUSE_BUCKETS = frozenset(
    {
        HISTORY_READ_CAUSE_REQUEST_CONSTRUCTION_REPAIR,
        HISTORY_READ_CAUSE_BOUNDED_WINDOW_ADJUSTMENT,
        HISTORY_READ_CAUSE_RUNTIME_TDLIB_ACCESS_ISSUE,
        HISTORY_READ_CAUSE_TARGET_UNAVAILABLE,
    }
)
EXACT_MESSAGE_READ_CAUSE_NOT_FOUND = "not_found"
EXACT_MESSAGE_READ_CAUSE_TDLIB_ERROR = "tdlib_error"
EXACT_MESSAGE_READ_CAUSE_UNSUPPORTED_RESPONSE = "unsupported_response"
EXACT_MESSAGE_READ_CAUSE_TIMEOUT = "timeout"
_EXACT_MESSAGE_READ_FAILURE_CAUSE_BUCKETS = frozenset(
    {
        EXACT_MESSAGE_READ_CAUSE_NOT_FOUND,
        EXACT_MESSAGE_READ_CAUSE_TDLIB_ERROR,
        EXACT_MESSAGE_READ_CAUSE_UNSUPPORTED_RESPONSE,
        EXACT_MESSAGE_READ_CAUSE_TIMEOUT,
    }
)
_SEARCH_REASON_CODES = frozenset(
    {
        "github_url_live_target_found",
        "github_url_live_target_not_found_in_approved_window",
        "github_url_live_search_history_empty",
        "github_url_live_search_projection_failed",
        "github_url_live_search_selected_message_id_invalid",
        "github_url_live_search_failed",
        "unsupported_cli_argument",
        "operator_approval_missing",
        "search_rollout_scope_not_allowed",
        "search_requires_exactly_one_target",
        "search_max_messages_required",
        "search_max_messages_out_of_bounds",
        "search_target_message_id_not_allowed",
        "search_registry_suffix_not_allowed",
        "search_max_targets_not_allowed",
        "search_confirm_token_missing",
        "search_confirm_token_invalid",
        "search_write_authority_not_allowed",
        "search_publish_authority_not_allowed",
        "search_history_request_count_exceeded",
        "search_redis_runtime_not_allowed",
        "search_runtime_commit_not_allowed",
        "target_locator_input_not_allowed_in_search",
        "target_locator_write_authority_missing",
        "target_locator_output_path_required",
        "target_locator_output_path_relative",
        "target_locator_output_path_outside_allowed_roots",
        "target_locator_output_path_traversal_not_allowed",
        "target_locator_output_root_not_real",
        "target_locator_output_parent_missing",
        "target_locator_output_parent_symlink_not_allowed",
        "target_locator_output_parent_not_directory",
        "target_locator_output_parent_invalid",
        "target_locator_output_target_symlink_not_allowed",
        "target_locator_output_target_exists",
        "target_locator_output_create_failed",
        "target_locator_output_write_failed",
        "target_locator_output_private_mode_unconfirmed",
        "target_locator_output_readback_failed",
        "target_locator_output_close_failed",
        "target_locator_output_cleanup_failed",
        "target_locator_output_cleanup_unconfirmed",
        "target_locator_payload_too_large",
        "target_locator_payload_invalid",
        "source_kind_unsupported",
        "direct_chat_id_target_not_allowed",
        "direct_registry_id_target_not_allowed",
        "direct_chat_or_registry_id_target_not_allowed",
        "runtime_config_not_allowed",
        "database_read_not_allowed",
        "telegram_read_not_allowed",
        "runtime_config_failed",
        "registry_target_missing",
        "registry_target_multiple",
        "registry_id_invalid",
        "registry_target_not_active",
        "registry_target_not_joined",
        "registry_target_chat_id_missing",
        "registry_target_chat_id_invalid",
        "tdlib_log_suppression_unconfirmed",
        "tdlib_initialize_failed",
        "tdlib_parameters_required",
        "tdlib_not_authorized",
        "tdlib_auth_state_invalid",
        "tdlib_auth_ready_timeout",
        "telegram_history_read_failed",
        "telegram_history_response_invalid",
        "telegram_history_read_timeout",
        "history_result_exceeds_requested_limit",
        "non_target_channel_history_message",
        "runtime_rollback_failed",
    }
)

JsonDict = dict[str, Any]
RuntimeConfigLoader = Callable[[], CollectorTelegramConfig]

_TARGET_LOCATOR_FIELDS = frozenset(
    {
        "schema_version",
        "source_kind",
        "source_value",
        "target_message_id",
        "target_fingerprint",
        "selected_message_fingerprint",
    }
)


class BoundedHistoryIngestError(RuntimeError):
    def __init__(
        self,
        error_code: str,
        *,
        partial_publish: "PublishEventsResult | None" = None,
        history_read_failure_cause_bucket: str | None = None,
    ) -> None:
        super().__init__(error_code)
        self.error_code = error_code
        self.partial_publish = partial_publish
        self.history_read_failure_cause_bucket = (
            history_read_failure_cause_bucket
            if history_read_failure_cause_bucket in _HISTORY_READ_FAILURE_CAUSE_BUCKETS
            else None
        )


class _RunResultReady(Exception):
    pass


class BoundedHistoryRepository(Protocol):
    def transaction(self) -> Any: ...

    async def find_public_username_registry_targets(self, normalized_source_value: str) -> Sequence[Any]: ...

    async def list_reconcile_targets(self, limit: int) -> Sequence[Any]: ...

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None: ...

    async def get_latest_version(self, source_message_id: str) -> Mapping[str, Any] | None: ...

    async def upsert_source_message(
        self,
        projection: SourceMessageProjection,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]: ...

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]: ...

    async def insert_outbox_event(self, event: Any) -> bool | None: ...

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str) -> Any | None: ...

    async def mark_outbox_published(
        self,
        *,
        event_id: UUID,
        published_at: datetime | None = None,
    ) -> bool | None: ...

    async def update_channel_sync_cursor(
        self,
        *,
        registry_id: str,
        last_seen_message_id: int | None = None,
        last_seen_message_date: datetime | None = None,
        last_history_sync_at: datetime | None = None,
    ) -> None: ...

    async def count_source_message_versions(self, source_message_id: str) -> int: ...

    async def count_source_created_events(self, source_message_id: str) -> int: ...

    async def count_source_outbox_events(self, source_message_id: str) -> int: ...


class BoundedHistoryClient(Protocol):
    async def fetch_newest_history_messages(
        self,
        *,
        chat_id: int,
        limit: int,
        from_message_id: int = 0,
        offset: int = 0,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def read_exact_message(self, *, chat_id: int, message_id: int) -> Mapping[str, Any] | None: ...

    async def close(self) -> None: ...


class BoundedHistoryRedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestConfig:
    mode: str = MODE_PLAN
    rollout_scope: str = ROLLOUT_SCOPE_EXACT_TARGETS
    source_kind: str = SOURCE_KIND_PUBLIC_USERNAME
    source_value: str | None = None
    source_values: tuple[str, ...] = ()
    target_message_id: int | None = None
    target_locator_path: str | None = None
    target_locator_output_path: str | None = None
    allow_target_locator_write: bool = False
    registry_id_suffix: str | None = None
    max_targets: int | None = None
    history_limit: int = DEFAULT_HISTORY_LIMIT
    operator_approved: bool = False
    confirm_token: str | None = None
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_telegram_read: bool = False
    allow_database_write: bool = False
    allow_source_message_write: bool = False
    allow_source_version_write: bool = False
    allow_source_outbox_write: bool = False
    allow_source_outbox_publish: bool = False
    allow_redis_publish: bool = False
    # Legacy construction aliases kept for old tests/importers; the CLI no longer exposes them.
    allow_outbox_write: bool = False
    max_messages: int | None = None
    chat_id: int | None = None
    registry_id: str | None = None


@dataclass(slots=True)
class BoundedTelegramCollectorHistoryIngestState:
    runtime_config_attempted: bool = False
    runtime_builder_attempted: bool = False
    database_read_attempted: bool = False
    registry_lookup_attempted: bool = False
    registry_targets_seen_count: int = 0
    history_window_attempts: int = 0
    history_request_count: int = 0
    tdlib_auth_ready_checked: bool = False
    tdlib_auth_ready: bool = False
    tdlib_parameters_submitted: bool = False
    tdlib_log_suppression_attempted: bool = False
    tdlib_log_suppression_confirmed: bool = False
    telegram_read_attempted: bool = False
    telegram_read_called: bool = False
    telegram_read_succeeded: bool = False
    exact_message_read_attempted: bool = False
    exact_message_read_succeeded: bool = False
    exact_message_read_failure_cause_bucket: str | None = None
    source_message_write_attempted: bool = False
    source_version_write_attempted: bool = False
    source_outbox_write_attempted: bool = False
    channel_cursor_write_attempted: bool = False
    source_outbox_publish_attempted: bool = False
    redis_publish_attempted: bool = False
    event_outbox_status_write_attempted: bool = False
    runtime_close_attempted: bool = False
    runtime_close_succeeded: bool = False
    runtime_close_commit_requested: bool | None = None

    @property
    def database_write_attempted(self) -> bool:
        return (
            self.source_message_write_attempted
            or self.source_version_write_attempted
            or self.source_outbox_write_attempted
            or self.channel_cursor_write_attempted
            or self.event_outbox_status_write_attempted
        )


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestRuntimeHandle:
    repository: BoundedHistoryRepository
    history_client: BoundedHistoryClient
    close: Callable[[bool], Awaitable[None]]
    redis_publisher: BoundedHistoryRedisPublisher | None = None
    commit: Callable[[], Awaitable[None]] | None = None
    rollback: Callable[[], Awaitable[None]] | None = None


class BoundedTelegramCollectorHistoryIngestRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: CollectorTelegramConfig,
        state: BoundedTelegramCollectorHistoryIngestState,
        logger: logging.Logger,
    ) -> BoundedTelegramCollectorHistoryIngestRuntimeHandle: ...


@dataclass(frozen=True, slots=True)
class HistoryMessageApplyResult:
    source_message_created: bool = False
    source_version_appended: bool = False
    outbox_event_inserted: bool = False
    idempotent_noop: bool = False
    source_message_id_suffix: str | None = None
    outbox_event: OutboxEventRow | None = None


@dataclass(frozen=True, slots=True)
class HistoryMessagePreviewResult:
    would_insert_source_message: bool = False
    would_append_source_version: bool = False
    would_insert_outbox_event: bool = False
    would_skip_same_hash: bool = False


@dataclass(frozen=True, slots=True)
class PublishEventsResult:
    published_count: int = 0
    marked_published_count: int = 0
    event_fingerprints: tuple[str, ...] = ()
    redis_message_fingerprints: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SourceLastReadbackResult:
    source_current_found_count: int = 0
    source_version_rows_count: int = 0
    source_created_events_count: int = 0
    source_outbox_events_count: int = 0
    source_message_fingerprints: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorHistoryIngestResult:
    status: str
    ok: bool
    error_code: str | None
    config: BoundedTelegramCollectorHistoryIngestConfig
    state: BoundedTelegramCollectorHistoryIngestState = field(default_factory=BoundedTelegramCollectorHistoryIngestState)
    mode: str = MODE_PLAN
    rollout_scope: str = ROLLOUT_SCOPE_EXACT_TARGETS
    source_kind: str = SOURCE_KIND_PUBLIC_USERNAME
    source_value_surface: str | None = None
    target_registry_id_suffix: str | None = None
    target_joined: bool = False
    target_chat_id_present: bool = False
    messages_requested: int = DEFAULT_HISTORY_LIMIT
    messages_seen: int = 0
    would_insert_source_messages_count: int = 0
    would_append_source_versions_count: int = 0
    would_insert_outbox_events_count: int = 0
    would_skip_same_hash_count: int = 0
    source_messages_created_count: int = 0
    source_versions_appended_count: int = 0
    outbox_events_inserted_count: int = 0
    source_created_events_count: int = 0
    idempotent_noop_count: int = 0
    duplicate_noop_proof_count: int = 0
    redis_events_published_count: int = 0
    event_outbox_marked_published_count: int = 0
    source_message_id_suffixes: tuple[str, ...] = ()
    event_id_suffixes: tuple[str, ...] = ()
    redis_message_id_suffixes: tuple[str, ...] = ()
    exact_channel_target_fingerprint: str | None = None
    registry_target_fingerprint: str | None = None
    source_message_fingerprints: tuple[str, ...] = ()
    source_outbox_event_fingerprints: tuple[str, ...] = ()
    redis_message_fingerprints: tuple[str, ...] = ()
    readback: SourceLastReadbackResult = field(default_factory=SourceLastReadbackResult)
    target_count: int = 0
    max_targets: int | None = None
    target_fingerprints: tuple[str, ...] = ()
    per_channel_results: tuple[JsonDict, ...] = ()
    partial_committed_target_fingerprints: tuple[str, ...] = ()
    error_class: str | None = None
    history_read_failure_cause_bucket: str | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        gates = {
            "operator_approved": self.config.operator_approved,
            "confirm_token_present": bool((self.config.confirm_token or "").strip()),
            "confirm_token_valid": _confirm_token_valid(self.config),
            "runtime_config_allowed": self.config.allow_runtime_config,
            "target_message_id_present": self.config.target_message_id is not None,
            "target_locator_present": self.config.target_locator_path is not None,
            "target_locator_consumption_supported": True,
            "database_read_allowed": self.config.allow_database_read,
            "telegram_read_allowed": self.config.allow_telegram_read,
            "database_write_allowed": self.config.allow_database_write,
            "source_message_write_allowed": self.config.allow_source_message_write,
            "source_version_write_allowed": self.config.allow_source_version_write,
            "source_outbox_write_allowed": _source_outbox_write_allowed(self.config),
            "source_outbox_publish_allowed": self.config.allow_source_outbox_publish,
            "redis_publish_allowed": self.config.allow_redis_publish,
        }
        side_effects = {
            "db_write": self.state.database_write_attempted,
            "redis_mutation": self.redis_events_published_count > 0,
            "telegram_read_called": self.state.telegram_read_called,
            "telegram_send_called": False,
            "telegram_edit_called": False,
            "openai_called": False,
            "github_called": False,
            "x_called": False,
            "web_called": False,
            "notification_table_write": False,
            "worker_loop_started": False,
            "systemd_called": False,
            "docker_called": False,
            "alembic_called": False,
        }
        authority = {
            "live_telegram_read_attempted": self.state.telegram_read_attempted,
            "telegram_send_attempted": False,
            "openai_attempted": False,
            "github_attempted": False,
            "x_attempted": False,
            "web_attempted": False,
            "redis_consume_or_ack": False,
            "broad_registry_ingest": False,
            "bounded_full_registry_rollout": self.rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY,
            "docker_or_systemd_called": False,
            "alembic_or_ddl_ran": False,
        }
        bounded_counts = {
            "registry_targets": self.state.registry_targets_seen_count,
            "source_messages_created": self.source_messages_created_count,
            "source_versions_created": self.source_versions_appended_count,
            "source_created_events": self.source_created_events_count,
            "source_normalize_handoffs": self.redis_events_published_count,
            "duplicate_noops": self.duplicate_noop_proof_count,
        }
        readback = {
            "source_current_found_count": self.readback.source_current_found_count,
            "source_version_rows_count": self.readback.source_version_rows_count,
            "source_created_events_count": self.readback.source_created_events_count,
            "source_outbox_events_count": self.readback.source_outbox_events_count,
        }
        redactions_applied = {
            "full_chat_id_omitted": True,
            "full_registry_id_omitted": True,
            "source_ref_omitted": True,
            "full_source_message_id_omitted": True,
            "full_event_id_omitted": True,
            "full_redis_message_id_omitted": True,
            "raw_message_json_omitted": True,
            "message_text_omitted": True,
            "entities_json_omitted": True,
            "url_surface_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "telegram_credentials_omitted": True,
            "tdlib_session_paths_omitted": True,
            "target_locator_path_omitted": True,
            "target_locator_basename_omitted": True,
            "target_locator_content_omitted": True,
            "target_locator_raw_target_values_omitted": True,
            "exception_detail_omitted": True,
            "traceback_omitted": True,
            "stderr_omitted": True,
        }
        raw_values_printed = {
            "source_text": False,
            "source_ref": False,
            "url": False,
            "raw_id": False,
            "target_locator_path": False,
            "target_locator_basename": False,
            "target_locator_content": False,
            "tdlib_payload": False,
            "database_url": False,
            "redis_url": False,
            "secret": False,
            "runtime_value": False,
            "stderr": False,
            "traceback": False,
            "exception_body": False,
        }
        rollback_stop_readback = {
            "always_on_collector_started": False,
            "broad_worker_started": False,
            "exact_runner_completed": self.ok,
        }
        duplicate_noop_proof = {
            "proved_count": self.duplicate_noop_proof_count,
            "without_second_telegram_read": True,
            "per_channel": [
                {
                    "target_fingerprint": channel.get("target_fingerprint"),
                    "proved_count": channel.get("duplicate_noop_proof", {}).get("proved_count", 0),
                }
                for channel in self.per_channel_results
            ],
        }
        return {
            "schema_version": _schema_version_for_result(self.config),
            "runner_name": RUNNER_NAME,
            "mode": self.mode,
            "rollout_scope": self.rollout_scope,
            "status": _report_status(self.status, self.ok),
            "reason_code": self.error_code or "ok",
            "history_read_failure_cause_bucket": self.history_read_failure_cause_bucket,
            "exact_message_read_attempted": self.state.exact_message_read_attempted,
            "exact_message_read_succeeded": self.state.exact_message_read_succeeded,
            "exact_message_read_failure_cause_bucket": self.state.exact_message_read_failure_cause_bucket
            if self.state.exact_message_read_failure_cause_bucket
            in _EXACT_MESSAGE_READ_FAILURE_CAUSE_BUCKETS
            else None,
            "target_count": self.target_count,
            "max_targets": self.max_targets,
            "target_fingerprints": list(self.target_fingerprints),
            "per_channel_results": list(self.per_channel_results),
            "duplicate_noop_proof": duplicate_noop_proof,
            "partial_committed_target_fingerprints": list(self.partial_committed_target_fingerprints),
            "exact_channel_target_fingerprint": self.exact_channel_target_fingerprint,
            "target_message_fingerprint": _target_message_fingerprint(self.config.target_message_id),
            "target_locator_present": self.config.target_locator_path is not None,
            "target_locator_schema_version": TARGET_LOCATOR_SCHEMA_VERSION,
            "target_locator_consumption_supported": True,
            "target_locator_path_omitted": True,
            "target_locator_content_omitted": True,
            "target_locator_raw_target_values_omitted": True,
            "registry_target_fingerprint": self.registry_target_fingerprint,
            "source_message_fingerprints": list(self.source_message_fingerprints),
            "source_outbox_event_fingerprints": list(self.source_outbox_event_fingerprints),
            "bounded_counts": bounded_counts,
            "authority": authority,
            "redactions_applied": redactions_applied,
            "raw_values_printed": raw_values_printed,
            "rollback_stop_readback": rollback_stop_readback,
            "readback": readback,
            "source_kind": self.source_kind,
            "source_value_surface": None,
            "source_value_fingerprint": self.exact_channel_target_fingerprint,
            "target_registry_id_suffix": None,
            "target_registry_fingerprint": self.registry_target_fingerprint,
            "target_joined": self.target_joined,
            "target_chat_id_present": self.target_chat_id_present,
            "gates": gates,
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "target_message_id_present": self.config.target_message_id is not None,
            "database_read_allowed": self.config.allow_database_read,
            "telegram_read_allowed": self.config.allow_telegram_read,
            "database_write_allowed": self.config.allow_database_write,
            "source_message_write_allowed": self.config.allow_source_message_write,
            "source_version_write_allowed": self.config.allow_source_version_write,
            "source_outbox_write_allowed": _source_outbox_write_allowed(self.config),
            "source_outbox_publish_allowed": self.config.allow_source_outbox_publish,
            "redis_publish_allowed": self.config.allow_redis_publish,
            "runtime_config_attempted": self.state.runtime_config_attempted,
            "runtime_builder_attempted": self.state.runtime_builder_attempted,
            "database_read_attempted": self.state.database_read_attempted,
            "registry_lookup_attempted": self.state.registry_lookup_attempted,
            "registry_read_count": self.state.registry_targets_seen_count,
            "registry_targets_seen_count": self.state.registry_targets_seen_count,
            "history_window_attempts": self.state.history_window_attempts,
            "tdlib_auth_ready_checked": self.state.tdlib_auth_ready_checked,
            "tdlib_auth_ready": self.state.tdlib_auth_ready,
            "tdlib_parameters_submitted": self.state.tdlib_parameters_submitted,
            "tdlib_log_suppression_attempted": self.state.tdlib_log_suppression_attempted,
            "tdlib_log_suppression_confirmed": self.state.tdlib_log_suppression_confirmed,
            "telegram_read_attempted": self.state.telegram_read_attempted,
            "telegram_read_called": self.state.telegram_read_called,
            "database_write_attempted": self.state.database_write_attempted,
            "source_message_write_attempted": self.state.source_message_write_attempted,
            "source_version_write_attempted": self.state.source_version_write_attempted,
            "source_outbox_write_attempted": self.state.source_outbox_write_attempted,
            "outbox_write_attempted": self.state.source_outbox_write_attempted,
            "channel_cursor_write_attempted": self.state.channel_cursor_write_attempted,
            "source_outbox_publish_attempted": self.state.source_outbox_publish_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "event_outbox_status_write_attempted": self.state.event_outbox_status_write_attempted,
            "messages_requested": self.messages_requested,
            "messages_seen": self.messages_seen,
            "would_insert_source_messages_count": self.would_insert_source_messages_count,
            "would_append_source_versions_count": self.would_append_source_versions_count,
            "would_insert_outbox_events_count": self.would_insert_outbox_events_count,
            "would_skip_same_hash_count": self.would_skip_same_hash_count,
            "source_messages_created_count": self.source_messages_created_count,
            "source_versions_appended_count": self.source_versions_appended_count,
            "outbox_events_inserted_count": self.outbox_events_inserted_count,
            "source_created_events_count": self.source_created_events_count,
            "idempotent_noop_count": self.idempotent_noop_count,
            "duplicate_noop_proof_count": self.duplicate_noop_proof_count,
            "redis_events_published_count": self.redis_events_published_count,
            "event_outbox_marked_published_count": self.event_outbox_marked_published_count,
            "source_message_id_suffixes": [],
            "event_id_suffixes": [],
            "redis_message_id_suffixes": [],
            "redis_message_fingerprints": list(self.redis_message_fingerprints),
            "ok": self.ok,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "side_effects": side_effects,
        }


@dataclass(frozen=True, slots=True)
class BoundedTelegramCollectorGitHubSearchResult:
    status: str
    reason_code: str
    config: BoundedTelegramCollectorHistoryIngestConfig
    state: BoundedTelegramCollectorHistoryIngestState = field(default_factory=BoundedTelegramCollectorHistoryIngestState)
    target_fingerprint: str | None = None
    registry_target_fingerprint: str | None = None
    selected_message_fingerprint: str | None = None
    requested_max_messages: int | None = None
    messages_returned: int = 0
    messages_examined: int = 0
    messages_with_text_surface_count: int = 0
    messages_with_entity_surface_count: int = 0
    messages_with_url_surface_count: int = 0
    messages_with_entity_url_source_count: int = 0
    messages_with_preview_url_source_count: int = 0
    messages_with_regex_url_source_count: int = 0
    github_matching_message_count: int = 0
    selected_entity_match: bool = False
    selected_preview_match: bool = False
    selected_regex_match: bool = False
    safe_failure_bucket: str | None = None
    target_locator_write_attempted: bool = False
    target_locator_written: bool = False
    target_locator_private_mode_confirmed: bool = False

    @property
    def ok(self) -> bool:
        return self.status == "pass"

    def to_sanitized_dict(self) -> dict[str, Any]:
        write_authority_absent = not (
            self.config.allow_database_write
            or self.config.allow_source_message_write
            or self.config.allow_source_version_write
            or _source_outbox_write_allowed(self.config)
        )
        publish_authority_absent = not (
            self.config.allow_source_outbox_publish or self.config.allow_redis_publish
        )
        raw_values_printed = {
            "public_username": False,
            "channel_title": False,
            "chat_id": False,
            "registry_id": False,
            "message_id": False,
            "source_text": False,
            "caption_text": False,
            "url": False,
            "hostname": False,
            "repo_owner_name": False,
            "entities": False,
            "tdlib_payload": False,
            "message_link": False,
            "runtime_env_path": False,
            "runtime_value": False,
            "database_url": False,
            "redis_url": False,
            "credential": False,
            "confirm_token": False,
            "exception_body": False,
            "traceback": False,
            "stderr": False,
            "target_locator_path": False,
            "target_locator_basename": False,
            "target_locator_content": False,
            "target_locator_source_value": False,
            "target_locator_message_id": False,
        }
        target_locator_requested = bool(
            self.config.allow_target_locator_write
            or self.config.target_locator_output_path is not None
        )
        return {
            "schema_version": SEARCH_SCHEMA_VERSION,
            "status": self.status,
            "reason_code": _sanitize_search_reason_code(self.reason_code),
            "mode": MODE_SEARCH,
            "target_fingerprint": self.target_fingerprint,
            "registry_target_fingerprint": self.registry_target_fingerprint,
            "selected_message_fingerprint": self.selected_message_fingerprint,
            "target_locator_requested": target_locator_requested,
            "target_locator_written": self.target_locator_written,
            "target_locator_schema_version": TARGET_LOCATOR_SCHEMA_VERSION,
            "target_locator_private_mode_confirmed": self.target_locator_private_mode_confirmed,
            "target_locator_consumption_supported": True,
            "requested_max_messages": self.requested_max_messages,
            "messages_returned": self.messages_returned,
            "messages_examined": self.messages_examined,
            "messages_with_text_surface_count": self.messages_with_text_surface_count,
            "messages_with_entity_surface_count": self.messages_with_entity_surface_count,
            "messages_with_url_surface_count": self.messages_with_url_surface_count,
            "messages_with_entity_url_source_count": self.messages_with_entity_url_source_count,
            "messages_with_preview_url_source_count": self.messages_with_preview_url_source_count,
            "messages_with_regex_url_source_count": self.messages_with_regex_url_source_count,
            "github_matching_message_count": self.github_matching_message_count,
            "github_url_present": self.github_matching_message_count > 0,
            "selected_match_source_buckets": {
                "entity": self.selected_entity_match,
                "preview": self.selected_preview_match,
                "regex": self.selected_regex_match,
            },
            "history_request_count": self.state.history_request_count,
            "telegram_read_attempted": self.state.telegram_read_attempted,
            "telegram_read_called": self.state.telegram_read_called,
            "telegram_read_succeeded": self.state.telegram_read_succeeded,
            "tdlib": {
                "auth_ready_checked": self.state.tdlib_auth_ready_checked,
                "auth_ready": self.state.tdlib_auth_ready,
                "parameters_submitted": self.state.tdlib_parameters_submitted,
                "log_suppression_attempted": self.state.tdlib_log_suppression_attempted,
                "log_suppression_confirmed": self.state.tdlib_log_suppression_confirmed,
            },
            "safe_failure_bucket": self.safe_failure_bucket
            if self.safe_failure_bucket in _HISTORY_READ_FAILURE_CAUSE_BUCKETS
            else None,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "confirm_token_present": bool((self.config.confirm_token or "").strip()),
                "confirm_token_valid": _search_confirm_token_valid(self.config),
                "runtime_config_allowed": self.config.allow_runtime_config,
                "database_read_allowed": self.config.allow_database_read,
                "telegram_read_allowed": self.config.allow_telegram_read,
                "exact_single_public_username_required": True,
                "max_messages_explicit": self.config.max_messages is not None,
                "write_authority_absent": write_authority_absent,
                "publish_authority_absent": publish_authority_absent,
                "target_locator_write_allowed": self.config.allow_target_locator_write,
                "target_locator_output_path_present": self.config.target_locator_output_path
                is not None,
                "target_locator_consumption_supported": True,
            },
            "authority": {
                "database_read_allowed": self.config.allow_database_read,
                "telegram_read_allowed": self.config.allow_telegram_read,
                "database_write_allowed": False,
                "source_truth_write_allowed": False,
                "cursor_write_allowed": False,
                "redis_allowed": False,
                "provider_calls_allowed": False,
                "openai_allowed": False,
                "notifier_allowed": False,
            },
            "side_effects": {
                "database_write_attempted": self.state.database_write_attempted,
                "source_message_write_attempted": self.state.source_message_write_attempted,
                "source_version_write_attempted": self.state.source_version_write_attempted,
                "source_outbox_write_attempted": self.state.source_outbox_write_attempted,
                "channel_cursor_write_attempted": self.state.channel_cursor_write_attempted,
                "source_outbox_publish_attempted": self.state.source_outbox_publish_attempted,
                "redis_publish_attempted": self.state.redis_publish_attempted,
                "history_ingest_processor_instantiated": False,
                "read_exact_message_called": self.state.exact_message_read_attempted,
                "provider_or_openai_called": False,
                "telegram_send_or_edit_called": False,
                "notifier_called": False,
                "target_locator_write_attempted": self.target_locator_write_attempted,
            },
            "redactions_applied": {
                "public_username_omitted": True,
                "channel_title_omitted": True,
                "full_chat_id_omitted": True,
                "full_registry_id_omitted": True,
                "full_message_id_omitted": True,
                "raw_message_json_omitted": True,
                "message_text_omitted": True,
                "caption_text_omitted": True,
                "raw_url_omitted": True,
                "hostname_omitted": True,
                "repo_owner_name_omitted": True,
                "entities_json_omitted": True,
                "url_surface_json_omitted": True,
                "tdlib_payload_omitted": True,
                "message_link_omitted": True,
                "runtime_env_path_omitted": True,
                "runtime_values_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "telegram_credentials_omitted": True,
                "confirm_token_omitted": True,
                "target_locator_path_omitted": True,
                "target_locator_basename_omitted": True,
                "target_locator_content_omitted": True,
                "target_locator_raw_target_values_omitted": True,
                "exception_detail_omitted": True,
                "traceback_omitted": True,
                "stderr_omitted": True,
            },
            "raw_values_printed": raw_values_printed,
            "rollback_close_readback": {
                "close_attempted": self.state.runtime_close_attempted,
                "close_succeeded": self.state.runtime_close_succeeded,
                "rollback_requested": self.state.runtime_close_commit_requested is False,
                "commit_requested": self.state.runtime_close_commit_requested is True,
                "commit_called": False,
            },
        }


@dataclass(frozen=True, slots=True)
class _ResolvedTarget:
    chat_id: int
    registry_id: str
    source_value_surface: str
    joined: bool
    chat_id_present: bool
    last_seen_message_id: int | None = None
    last_seen_message_date: datetime | None = None


@dataclass(frozen=True, slots=True)
class _TargetSourceLastExecution:
    target: _ResolvedTarget
    messages_seen: int = 0
    history_window_attempts: int = 0
    exact_message_read_attempted: bool = False
    exact_message_read_succeeded: bool = False
    exact_message_read_failure_cause_bucket: str | None = None
    source_messages_created_count: int = 0
    source_versions_appended_count: int = 0
    outbox_events_inserted_count: int = 0
    source_created_events_count: int = 0
    idempotent_noop_count: int = 0
    duplicate_noop_proof_count: int = 0
    outbox_events: tuple[OutboxEventRow, ...] = ()
    source_message_suffixes: tuple[str, ...] = ()
    source_outbox_event_fingerprints: tuple[str, ...] = ()
    readback: SourceLastReadbackResult = field(default_factory=SourceLastReadbackResult)
    source_commit_durable: bool = False


class _TargetSourceLastExecutionFailed(Exception):
    def __init__(
        self,
        error_code: str,
        *,
        partial: _TargetSourceLastExecution,
        error_class: str | None = None,
        history_read_failure_cause_bucket: str | None = None,
    ) -> None:
        super().__init__(error_code)
        self.error_code = error_code
        self.partial = partial
        self.error_class = error_class
        self.history_read_failure_cause_bucket = (
            history_read_failure_cause_bucket
            if history_read_failure_cause_bucket in _HISTORY_READ_FAILURE_CAUSE_BUCKETS
            else None
        )


@dataclass(frozen=True, slots=True)
class _HistoryReadWindow:
    limit: int
    from_message_id: int = 0
    offset: int = 0


class _TDLibBoundedHistoryClient:
    def __init__(
        self,
        tdlib: TDLibClient,
        *,
        state: BoundedTelegramCollectorHistoryIngestState,
        timeout_sec: float = HISTORY_READ_TIMEOUT_SEC,
        auth_ready_timeout_sec: float = AUTH_READY_TIMEOUT_SEC,
        require_native_log_suppression: bool = True,
    ) -> None:
        self._tdlib = tdlib
        self._state = state
        self._timeout_sec = timeout_sec
        self._auth_ready_timeout_sec = auth_ready_timeout_sec
        self._require_native_log_suppression = require_native_log_suppression

    async def fetch_newest_history_messages(
        self,
        *,
        chat_id: int,
        limit: int,
        from_message_id: int = 0,
        offset: int = 0,
    ) -> Sequence[Mapping[str, Any]]:
        await self._ensure_ready_for_history_read()
        request = self._tdlib.build_get_chat_history_request(
            chat_id=chat_id,
            from_message_id=from_message_id,
            offset=offset,
            limit=limit,
            only_local=False,
        )
        payload = dict(request.payload)
        extra = f"{RUNNER_NAME}:{uuid4()}"
        payload["@extra"] = extra
        self._state.telegram_read_called = True
        self._state.history_request_count += 1
        await self._tdlib.send(payload)

        deadline = time.monotonic() + self._timeout_sec
        while time.monotonic() < deadline:
            receive_timeout = min(HISTORY_RECEIVE_TIMEOUT_SEC, max(0.0, deadline - time.monotonic()))
            response = await self._tdlib.receive(receive_timeout)
            if response is None:
                continue
            if response.get("@extra") != extra:
                continue
            if response.get("@type") == "error":
                raise BoundedHistoryIngestError(
                    "telegram_history_read_failed",
                    history_read_failure_cause_bucket=_classify_tdlib_history_read_error_cause(response),
                )
            messages = response.get("messages")
            if not isinstance(messages, list):
                raise BoundedHistoryIngestError("telegram_history_response_invalid")
            if len(messages) > limit:
                raise BoundedHistoryIngestError("history_result_exceeds_requested_limit")
            if any(not isinstance(message, Mapping) for message in messages):
                raise BoundedHistoryIngestError("telegram_history_response_invalid")
            self._state.telegram_read_succeeded = True
            return tuple(messages)
        raise BoundedHistoryIngestError("telegram_history_read_timeout")

    async def read_exact_message(self, *, chat_id: int, message_id: int) -> Mapping[str, Any] | None:
        await self._ensure_ready_for_history_read()
        payload = {
            "@type": "getMessage",
            "chat_id": chat_id,
            "message_id": message_id,
        }
        extra = f"{RUNNER_NAME}:exact_message:{uuid4()}"
        payload["@extra"] = extra
        self._state.telegram_read_called = True
        await self._tdlib.send(payload)

        deadline = time.monotonic() + self._timeout_sec
        while time.monotonic() < deadline:
            receive_timeout = min(HISTORY_RECEIVE_TIMEOUT_SEC, max(0.0, deadline - time.monotonic()))
            response = await self._tdlib.receive(receive_timeout)
            if response is None:
                continue
            if response.get("@extra") != extra:
                continue
            if response.get("@type") == "error":
                self._state.exact_message_read_failure_cause_bucket = (
                    _classify_tdlib_exact_message_read_error_cause(response)
                )
                return None
            if response.get("@type") != "message":
                self._state.exact_message_read_failure_cause_bucket = (
                    EXACT_MESSAGE_READ_CAUSE_UNSUPPORTED_RESPONSE
                )
                return None
            return dict(response)
        self._state.exact_message_read_failure_cause_bucket = EXACT_MESSAGE_READ_CAUSE_TIMEOUT
        return None

    async def close(self) -> None:
        await self._tdlib.close()

    async def _ensure_ready_for_history_read(self) -> None:
        try:
            await self._tdlib.initialize()
        except TDLibTransportError as exc:
            self._copy_log_suppression_state()
            if self._state.tdlib_log_suppression_attempted and not self._state.tdlib_log_suppression_confirmed:
                raise BoundedHistoryIngestError("tdlib_log_suppression_unconfirmed") from exc
            raise BoundedHistoryIngestError("tdlib_initialize_failed") from exc

        self._copy_log_suppression_state()
        if self._require_native_log_suppression and not self._state.tdlib_log_suppression_confirmed:
            raise BoundedHistoryIngestError("tdlib_log_suppression_unconfirmed")

        self._state.tdlib_auth_ready_checked = True
        await self._request_authorization_state()

        deadline = time.monotonic() + self._auth_ready_timeout_sec
        while time.monotonic() < deadline:
            receive_timeout = min(AUTH_RECEIVE_TIMEOUT_SEC, max(0.0, deadline - time.monotonic()))
            response = await self._tdlib.receive(receive_timeout)
            if response is None:
                continue
            authorization_state = _authorization_state_from_tdlib_payload(response)
            if authorization_state is None:
                if response.get("@type") == "error" and _is_bounded_auth_extra(response.get("@extra")):
                    raise BoundedHistoryIngestError("tdlib_parameters_required")
                continue

            state_type = authorization_state.get("@type")
            if state_type == "authorizationStateReady":
                self._state.tdlib_auth_ready = True
                return
            if state_type == "authorizationStateWaitTdlibParameters":
                await self._submit_tdlib_parameters()
                continue
            if state_type == "authorizationStateWaitEncryptionKey":
                await self._submit_database_encryption_key()
                continue
            if state_type in _INTERACTIVE_AUTHORIZATION_STATES:
                raise BoundedHistoryIngestError("tdlib_not_authorized")
            if state_type in _TERMINAL_NOT_READY_AUTHORIZATION_STATES:
                raise BoundedHistoryIngestError("tdlib_not_authorized")
            raise BoundedHistoryIngestError("tdlib_auth_state_invalid")

        raise BoundedHistoryIngestError("tdlib_auth_ready_timeout")

    async def _request_authorization_state(self) -> None:
        payload = dict(self._tdlib.build_get_authorization_state_request().payload)
        payload["@extra"] = f"{RUNNER_NAME}:auth_state:{uuid4()}"
        await self._tdlib.send(payload)

    async def _submit_tdlib_parameters(self) -> None:
        payload = dict(self._tdlib.build_set_tdlib_parameters_request().payload)
        payload["@extra"] = f"{RUNNER_NAME}:tdlib_parameters:{uuid4()}"
        self._state.tdlib_parameters_submitted = True
        await self._tdlib.send(payload)

    async def _submit_database_encryption_key(self) -> None:
        payload = dict(self._tdlib.build_check_database_encryption_key_request().payload)
        payload["@extra"] = f"{RUNNER_NAME}:database_encryption_key:{uuid4()}"
        await self._tdlib.send(payload)

    def _copy_log_suppression_state(self) -> None:
        self._state.tdlib_log_suppression_attempted = self._tdlib.native_log_suppression_attempted()
        self._state.tdlib_log_suppression_confirmed = self._tdlib.native_log_suppression_confirmed()


def _exact_target_history_read_windows(*, target_message_id: int, history_limit: int) -> tuple[_HistoryReadWindow, ...]:
    base_limit = min(history_limit, EXACT_TARGET_HISTORY_WINDOW_LIMIT_HARD_CAP)
    windows: list[_HistoryReadWindow] = []
    for offset in EXACT_TARGET_HISTORY_WINDOW_OFFSETS:
        minimum_limit = max(1, -offset + 1)
        limit = min(EXACT_TARGET_HISTORY_WINDOW_LIMIT_HARD_CAP, max(base_limit, minimum_limit))
        if offset < 0 and limit < -offset:
            continue
        windows.append(
            _HistoryReadWindow(
                from_message_id=target_message_id,
                offset=offset,
                limit=limit,
            )
        )
    return tuple(windows)


def _exact_target_anchorless_history_read_windows() -> tuple[_HistoryReadWindow, ...]:
    return (
        _HistoryReadWindow(
            from_message_id=0,
            offset=0,
            limit=EXACT_TARGET_ANCHORLESS_HISTORY_WINDOW_LIMIT_HARD_CAP,
        ),
    )


def _select_exact_target_messages(
    messages: Sequence[Mapping[str, Any]],
    *,
    target_message_id: int,
) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        dict(message)
        for message in messages
        if isinstance(message, Mapping) and _message_id(message) == target_message_id
    )


async def _read_direct_exact_target_message(
    *,
    history_client: BoundedHistoryClient,
    target: _ResolvedTarget,
    target_message_id: int,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> tuple[Mapping[str, Any], ...]:
    state.exact_message_read_attempted = True
    state.exact_message_read_succeeded = False
    state.exact_message_read_failure_cause_bucket = None

    message = await history_client.read_exact_message(
        chat_id=target.chat_id,
        message_id=target_message_id,
    )
    if message is None or not isinstance(message, Mapping):
        if state.exact_message_read_failure_cause_bucket not in _EXACT_MESSAGE_READ_FAILURE_CAUSE_BUCKETS:
            state.exact_message_read_failure_cause_bucket = EXACT_MESSAGE_READ_CAUSE_NOT_FOUND
        return ()

    if _message_id(message) != target_message_id:
        state.exact_message_read_failure_cause_bucket = EXACT_MESSAGE_READ_CAUSE_UNSUPPORTED_RESPONSE
        return ()

    message_chat_id = _message_chat_id(message)
    if message_chat_id is None or message_chat_id != target.chat_id:
        state.exact_message_read_failure_cause_bucket = EXACT_MESSAGE_READ_CAUSE_UNSUPPORTED_RESPONSE
        return ()

    state.exact_message_read_succeeded = True
    state.exact_message_read_failure_cause_bucket = None
    return (dict(message),)


async def _fetch_history_for_target(
    *,
    history_client: BoundedHistoryClient,
    target: _ResolvedTarget,
    history_limit: int,
    target_message_id: int | None,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> Sequence[Mapping[str, Any]]:
    if target_message_id is None:
        state.history_window_attempts += 1
        return await history_client.fetch_newest_history_messages(
            chat_id=target.chat_id,
            limit=history_limit,
        )

    bounded_window_failure: BoundedHistoryIngestError | None = None
    saw_successful_window = False
    for window in _exact_target_history_read_windows(
        target_message_id=target_message_id,
        history_limit=history_limit,
    ):
        state.history_window_attempts += 1
        try:
            messages = await history_client.fetch_newest_history_messages(
                chat_id=target.chat_id,
                limit=window.limit,
                from_message_id=window.from_message_id,
                offset=window.offset,
            )
        except BoundedHistoryIngestError as exc:
            if (
                exc.error_code == "telegram_history_read_failed"
                and exc.history_read_failure_cause_bucket == HISTORY_READ_CAUSE_BOUNDED_WINDOW_ADJUSTMENT
            ):
                bounded_window_failure = exc
                continue
            raise

        saw_successful_window = True
        if len(messages) > window.limit:
            raise BoundedHistoryIngestError("history_result_exceeds_requested_limit")
        selected_messages = _select_exact_target_messages(messages, target_message_id=target_message_id)
        if selected_messages:
            return tuple(selected_messages)

    if bounded_window_failure is not None and not saw_successful_window:
        anchorless_bounded_window_failure: BoundedHistoryIngestError | None = None
        for window in _exact_target_anchorless_history_read_windows():
            state.history_window_attempts += 1
            try:
                messages = await history_client.fetch_newest_history_messages(
                    chat_id=target.chat_id,
                    limit=window.limit,
                    from_message_id=window.from_message_id,
                    offset=window.offset,
                )
            except BoundedHistoryIngestError as exc:
                if (
                    exc.error_code == "telegram_history_read_failed"
                    and exc.history_read_failure_cause_bucket == HISTORY_READ_CAUSE_BOUNDED_WINDOW_ADJUSTMENT
                ):
                    anchorless_bounded_window_failure = exc
                    continue
                raise

            if len(messages) > window.limit:
                raise BoundedHistoryIngestError("history_result_exceeds_requested_limit")
            selected_messages = _select_exact_target_messages(messages, target_message_id=target_message_id)
            if selected_messages:
                return tuple(selected_messages)
        if anchorless_bounded_window_failure is not None:
            raise anchorless_bounded_window_failure

    direct_message = await _read_direct_exact_target_message(
        history_client=history_client,
        target=target,
        target_message_id=target_message_id,
        state=state,
    )
    if direct_message:
        return direct_message
    return ()


class HistoryMessageIngestProcessor:
    def __init__(
        self,
        *,
        repository: BoundedHistoryRepository,
        projection_builder: MessageProjectionBuilder,
        outbox_builder: CollectorOutboxBuilder,
        state: BoundedTelegramCollectorHistoryIngestState,
    ) -> None:
        self._repository = repository
        self._projection_builder = projection_builder
        self._outbox_builder = outbox_builder
        self._state = state

    async def preview_history_message(self, message: Mapping[str, Any]) -> HistoryMessagePreviewResult:
        projection = self._projection_builder.build_source_projection(dict(message))
        self._state.database_read_attempted = True
        existing = await self._repository.get_source_message(
            platform="telegram",
            chat_id=projection.chat_id,
            message_id=projection.message_id,
        )
        source_message_id = _coerce_source_message_id(existing)
        if source_message_id is None:
            return HistoryMessagePreviewResult(
                would_insert_source_message=True,
                would_append_source_version=True,
                would_insert_outbox_event=True,
            )
        latest = await self._repository.get_latest_version(source_message_id)
        if latest is not None and str(latest.get("content_hash")) == projection.content_hash:
            return HistoryMessagePreviewResult(would_skip_same_hash=True)
        return HistoryMessagePreviewResult(
            would_append_source_version=True,
            would_insert_outbox_event=True,
        )

    async def apply_history_message(self, message: Mapping[str, Any]) -> HistoryMessageApplyResult:
        projection = self._projection_builder.build_source_projection(dict(message))
        observed_at = datetime.now(timezone.utc)

        async with self._repository.transaction():
            existing = await self._repository.get_source_message(
                platform="telegram",
                chat_id=projection.chat_id,
                message_id=projection.message_id,
            )
            source_message_id = _coerce_source_message_id(existing)
            if source_message_id is not None:
                latest = await self._repository.get_latest_version(source_message_id)
                if latest is not None and str(latest.get("content_hash")) == projection.content_hash:
                    return HistoryMessageApplyResult(
                        idempotent_noop=True,
                        source_message_id_suffix=_safe_suffix(source_message_id),
                    )

            self._state.source_message_write_attempted = True
            current_row = await self._repository.upsert_source_message(projection, platform="telegram")
            resolved_source_message_id = _require_source_message_id(current_row)

            self._state.source_version_write_attempted = True
            changed, version_row = await self._repository.append_source_message_version_if_changed(
                source_message_id=resolved_source_message_id,
                projection=projection,
                version_reason="new" if existing is None else "reconcile",
                observed_at=observed_at,
                telegram_edit_date=projection.edited_at,
            )
            if not changed or version_row is None:
                return HistoryMessageApplyResult(
                    idempotent_noop=True,
                    source_message_id_suffix=_safe_suffix(resolved_source_message_id),
                )

            version_no = _require_version_no(version_row)
            if existing is None:
                outbox = self._outbox_builder.build_created(
                    source_message_id=resolved_source_message_id,
                    current_version_no=version_no,
                    logical_post_key=projection.logical_post_key,
                    occurred_at=projection.posted_at,
                )
            else:
                outbox = self._outbox_builder.build_reconciled(
                    source_message_id=resolved_source_message_id,
                    current_version_no=version_no,
                    logical_post_key=projection.logical_post_key,
                    occurred_at=observed_at,
                    reconcile_reason="bounded_history_ingest",
                )
            self._state.source_outbox_write_attempted = True
            inserted = await self._repository.insert_outbox_event(outbox)
            outbox_event = None
            if inserted is None or bool(inserted):
                outbox_event = _coerce_outbox_event_row(
                    await self._repository.get_outbox_event_by_dedupe_key(outbox.dedupe_key)
                )
                if outbox_event is None:
                    raise BoundedHistoryIngestError("outbox_event_readback_missing")
            return HistoryMessageApplyResult(
                source_message_created=existing is None,
                source_version_appended=True,
                outbox_event_inserted=True if inserted is None else bool(inserted),
                idempotent_noop=False,
                source_message_id_suffix=_safe_suffix(resolved_source_message_id),
                outbox_event=outbox_event,
            )


async def build_default_bounded_history_ingest_runtime(
    runtime_config: CollectorTelegramConfig,
    state: BoundedTelegramCollectorHistoryIngestState,
    logger: logging.Logger,
) -> BoundedTelegramCollectorHistoryIngestRuntimeHandle:
    runtime_config.ensure_runtime_dirs()
    engine = create_async_engine(runtime_config.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    session = session_factory()
    repository = CollectorRepository(session, logger=logger)
    tdlib = TDLibClient(runtime_config, transport=TDJsonTransport(), logger=logger)
    history_client = _TDLibBoundedHistoryClient(tdlib, state=state)
    redis_client: Any | None = None
    redis_publisher: RedisStreamsPublisher | None = None
    if runtime_config.redis_url:
        from redis.asyncio import Redis  # type: ignore[import-not-found]

        redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
        redis_publisher = RedisStreamsPublisher(redis_client, maxlen=DEFAULT_XADD_MAXLEN)

    transaction_finalized = False

    async def commit() -> None:
        nonlocal transaction_finalized
        await session.commit()
        transaction_finalized = True

    async def rollback() -> None:
        nonlocal transaction_finalized
        await session.rollback()
        transaction_finalized = True

    async def close(commit: bool) -> None:
        try:
            if commit:
                if not transaction_finalized:
                    await session.commit()
            else:
                await session.rollback()
        finally:
            with contextlib.suppress(Exception):
                await history_client.close()
            if redis_client is not None:
                close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
                if close_client is not None:
                    with contextlib.suppress(Exception):
                        result = close_client()
                        if hasattr(result, "__await__"):
                            await result
            with contextlib.suppress(Exception):
                await session.close()
            with contextlib.suppress(Exception):
                await engine.dispose()

    return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
        repository=repository,
        history_client=history_client,
        redis_publisher=redis_publisher,
        close=close,
        commit=commit,
        rollback=rollback,
    )


async def build_default_bounded_history_search_runtime(
    runtime_config: CollectorTelegramConfig,
    state: BoundedTelegramCollectorHistoryIngestState,
    logger: logging.Logger,
) -> BoundedTelegramCollectorHistoryIngestRuntimeHandle:
    runtime_config.ensure_runtime_dirs()
    engine = create_async_engine(runtime_config.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    session = session_factory()
    repository = CollectorRepository(session, logger=logger)
    tdlib = TDLibClient(runtime_config, transport=TDJsonTransport(), logger=logger)
    history_client = _TDLibBoundedHistoryClient(tdlib, state=state)

    async def rollback() -> None:
        await session.rollback()

    async def close(commit: bool) -> None:
        if commit:
            raise BoundedHistoryIngestError("search_runtime_commit_not_allowed")
        rollback_error: Exception | None = None
        cleanup_error: Exception | None = None
        try:
            await session.rollback()
        except Exception as exc:
            rollback_error = exc
        for cleanup in (history_client.close, session.close, engine.dispose):
            try:
                await cleanup()
            except Exception as exc:
                if cleanup_error is None:
                    cleanup_error = exc
        if rollback_error is not None:
            raise rollback_error
        if cleanup_error is not None:
            raise cleanup_error

    return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
        repository=repository,
        history_client=history_client,
        redis_publisher=None,
        close=close,
        commit=None,
        rollback=rollback,
    )


async def _run_bounded_telegram_collector_github_search(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    runtime_config_loader: RuntimeConfigLoader | None = None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedTelegramCollectorGitHubSearchResult:
    state = BoundedTelegramCollectorHistoryIngestState()
    effective_logger = logger or logging.getLogger(__name__)
    normalized_source_value, source_error = _normalize_search_source_value(config)
    requested_max_messages = _search_requested_max_messages(config.max_messages)
    target_fingerprint = (
        _input_target_fingerprint(normalized_source_value)
        if normalized_source_value is not None
        else None
    )
    registry_target_fingerprint: str | None = None
    selected_message_fingerprint: str | None = None
    selected_target_message_id: int | None = None
    target_locator_write_attempted = False
    target_locator_written = False
    target_locator_private_mode_confirmed = False
    messages_returned = 0
    messages_examined = 0
    messages_with_text_surface_count = 0
    messages_with_entity_surface_count = 0
    messages_with_url_surface_count = 0
    messages_with_entity_url_source_count = 0
    messages_with_preview_url_source_count = 0
    messages_with_regex_url_source_count = 0
    github_matching_message_count = 0
    selected_source_kinds: set[str] = set()

    def make_result(
        status: str,
        reason_code: str,
        *,
        safe_failure_bucket: str | None = None,
    ) -> BoundedTelegramCollectorGitHubSearchResult:
        return BoundedTelegramCollectorGitHubSearchResult(
            status=status,
            reason_code=reason_code,
            config=config,
            state=state,
            target_fingerprint=target_fingerprint,
            registry_target_fingerprint=registry_target_fingerprint,
            selected_message_fingerprint=selected_message_fingerprint,
            requested_max_messages=requested_max_messages,
            messages_returned=messages_returned,
            messages_examined=messages_examined,
            messages_with_text_surface_count=messages_with_text_surface_count,
            messages_with_entity_surface_count=messages_with_entity_surface_count,
            messages_with_url_surface_count=messages_with_url_surface_count,
            messages_with_entity_url_source_count=messages_with_entity_url_source_count,
            messages_with_preview_url_source_count=messages_with_preview_url_source_count,
            messages_with_regex_url_source_count=messages_with_regex_url_source_count,
            github_matching_message_count=github_matching_message_count,
            selected_entity_match="entity" in selected_source_kinds,
            selected_preview_match="preview" in selected_source_kinds,
            selected_regex_match="regex" in selected_source_kinds,
            safe_failure_bucket=safe_failure_bucket,
            target_locator_write_attempted=target_locator_write_attempted,
            target_locator_written=target_locator_written,
            target_locator_private_mode_confirmed=target_locator_private_mode_confirmed,
        )

    gate_error = _search_authority_gate_error(
        config,
        normalized_source_value=normalized_source_value,
        source_error=source_error,
    )
    if gate_error is not None:
        return make_result("blocked", gate_error)

    loader = runtime_config_loader or CollectorTelegramConfig.from_env
    state.runtime_config_attempted = True
    try:
        runtime_config = loader()
    except Exception:
        return make_result("blocked", "runtime_config_failed")

    builder = runtime_builder or build_default_bounded_history_search_runtime
    state.runtime_builder_attempted = True
    runtime: BoundedTelegramCollectorHistoryIngestRuntimeHandle | None = None
    result: BoundedTelegramCollectorGitHubSearchResult | None = None
    try:
        runtime = await builder(runtime_config, state, effective_logger)
        if runtime.redis_publisher is not None:
            raise BoundedHistoryIngestError("search_redis_runtime_not_allowed")

        assert normalized_source_value is not None
        assert requested_max_messages is not None
        targets = await _resolve_exact_public_username_targets(
            config=config,
            repository=runtime.repository,
            state=state,
            normalized_source_values=(normalized_source_value,),
        )
        target = targets[0]
        registry_target_fingerprint = _target_fingerprint(target)

        state.telegram_read_attempted = True
        request_count_before = state.history_request_count
        messages = await runtime.history_client.fetch_newest_history_messages(
            chat_id=target.chat_id,
            limit=requested_max_messages,
            from_message_id=0,
            offset=0,
        )
        if state.history_request_count == request_count_before:
            state.history_request_count += 1
            state.telegram_read_called = True
        if state.history_request_count != request_count_before + 1:
            raise BoundedHistoryIngestError("search_history_request_count_exceeded")
        state.telegram_read_succeeded = True

        returned_messages = tuple(messages)
        messages_returned = len(returned_messages)
        if messages_returned > requested_max_messages:
            raise BoundedHistoryIngestError("history_result_exceeds_requested_limit")
        if any(not isinstance(message, Mapping) for message in returned_messages):
            raise BoundedHistoryIngestError("telegram_history_response_invalid")
        selected_messages = tuple(dict(message) for message in returned_messages)
        _validate_selected_history_messages_for_target(selected_messages, target)
        if not selected_messages:
            result = make_result("blocked", "github_url_live_search_history_empty")
            raise _RunResultReady

        projection_builder = MessageProjectionBuilder(logger=effective_logger)
        for message in selected_messages:
            try:
                projection = projection_builder.build_source_projection(copy.deepcopy(message))
            except Exception as exc:
                raise BoundedHistoryIngestError("github_url_live_search_projection_failed") from exc

            messages_examined += 1
            messages_with_text_surface_count += int(bool(projection.text_surface))
            messages_with_entity_surface_count += int(bool(projection.entities_json))
            url_entries = tuple(
                entry
                for entry in (projection.url_surface_json or ())
                if isinstance(entry, Mapping)
            )
            messages_with_url_surface_count += int(bool(url_entries))
            source_kinds = {
                str(entry.get("source_kind"))
                for entry in url_entries
                if entry.get("source_kind") in {"entity", "preview", "regex"}
            }
            messages_with_entity_url_source_count += int("entity" in source_kinds)
            messages_with_preview_url_source_count += int("preview" in source_kinds)
            messages_with_regex_url_source_count += int("regex" in source_kinds)

            github_source_kinds = {
                str(entry.get("source_kind"))
                for entry in url_entries
                if entry.get("source_kind") in {"entity", "preview", "regex"}
                and _is_github_url(entry.get("observed_url"))
            }
            if not github_source_kinds:
                continue
            github_matching_message_count += 1
            if selected_message_fingerprint is None:
                raw_message_id = message.get("id")
                if (
                    not _valid_target_message_id(raw_message_id)
                    or not _valid_target_message_id(projection.message_id)
                    or raw_message_id != projection.message_id
                ):
                    raise BoundedHistoryIngestError(
                        "github_url_live_search_selected_message_id_invalid"
                    )
                selected_target_message_id = projection.message_id
                selected_message_fingerprint = _target_message_fingerprint(selected_target_message_id)
                selected_source_kinds = github_source_kinds

        if selected_message_fingerprint is None:
            result = make_result(
                "blocked",
                "github_url_live_target_not_found_in_approved_window",
            )
        else:
            result = make_result("pass", "github_url_live_target_found")
        raise _RunResultReady
    except _RunResultReady:
        pass
    except BoundedHistoryIngestError as exc:
        result = make_result(
            "blocked",
            exc.error_code,
            safe_failure_bucket=exc.history_read_failure_cause_bucket,
        )
    except Exception:
        result = make_result("blocked", "github_url_live_search_failed")
    finally:
        if runtime is not None:
            state.runtime_close_attempted = True
            state.runtime_close_commit_requested = False
            try:
                await runtime.close(False)
                state.runtime_close_succeeded = True
            except Exception:
                state.runtime_close_succeeded = False
                result = make_result("blocked", "runtime_rollback_failed")

    assert result is not None
    if result.ok and config.allow_target_locator_write:
        assert normalized_source_value is not None
        assert selected_target_message_id is not None
        assert selected_message_fingerprint is not None
        assert target_fingerprint is not None
        target_locator_write_attempted = True
        locator_error = _write_target_locator(
            config.target_locator_output_path,
            source_value=normalized_source_value,
            target_message_id=selected_target_message_id,
            target_fingerprint=target_fingerprint,
            selected_message_fingerprint=selected_message_fingerprint,
        )
        if locator_error is None:
            target_locator_written = True
            target_locator_private_mode_confirmed = True
            result = make_result("pass", "github_url_live_target_found")
        else:
            result = make_result("blocked", locator_error)
    return result


async def run_bounded_telegram_collector_history_ingest(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    runtime_config_loader: RuntimeConfigLoader | None = None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedTelegramCollectorHistoryIngestResult | BoundedTelegramCollectorGitHubSearchResult:
    if _normalize_mode(config.mode) == MODE_SEARCH:
        return await _run_bounded_telegram_collector_github_search(
            config,
            runtime_config_loader=runtime_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )

    state = BoundedTelegramCollectorHistoryIngestState()
    effective_logger = logger or logging.getLogger(__name__)
    mode = _normalize_mode(config.mode)
    target_locator_error = _target_locator_argument_error(config)
    target_locator_present = config.target_locator_path is not None
    rollout_scope, rollout_scope_error = _normalize_rollout_scope(config.rollout_scope)
    history_limit = _history_limit(config)
    normalized_source_values, target_error = _normalize_source_values(config)
    normalized_source_value = normalized_source_values[0] if len(normalized_source_values) == 1 else None
    input_target_fingerprints = tuple(_input_target_fingerprint(value) for value in normalized_source_values)

    def target_report_fingerprint(target: _ResolvedTarget) -> str:
        if rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY:
            return _target_fingerprint(target) or _input_target_fingerprint(target.source_value_surface)
        return _input_target_fingerprint(target.source_value_surface)

    def make_result(
        status: str,
        error_code: str | None,
        *,
        ok: bool = False,
        error_class: str | None = None,
        target: _ResolvedTarget | None = None,
        messages_seen: int = 0,
        would_insert: int = 0,
        would_append: int = 0,
        would_outbox: int = 0,
        would_skip: int = 0,
        created_count: int = 0,
        version_count: int = 0,
        outbox_count: int = 0,
        created_event_count: int = 0,
        noop_count: int = 0,
        duplicate_noop_count: int = 0,
        publish: PublishEventsResult | None = None,
        source_message_suffixes: Sequence[str] = (),
        source_outbox_event_fingerprints: Sequence[str] = (),
        readback: SourceLastReadbackResult | None = None,
        targets: Sequence[_ResolvedTarget] = (),
        channel_results: Sequence[JsonDict] = (),
        partial_committed_target_fingerprints: Sequence[str] = (),
        history_read_failure_cause_bucket: str | None = None,
    ) -> BoundedTelegramCollectorHistoryIngestResult:
        source_readback = readback or SourceLastReadbackResult()
        publish_event_fingerprints = () if publish is None else publish.event_fingerprints
        publish_redis_fingerprints = () if publish is None else publish.redis_message_fingerprints
        resolved_target_fingerprints = tuple(
            _input_target_fingerprint(item.source_value_surface) for item in targets
        )
        registry_target_fingerprints = tuple(
            fingerprint for fingerprint in (_target_fingerprint(item) for item in targets) if fingerprint
        )
        report_target_fingerprints = (
            registry_target_fingerprints
            if rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY
            else
            tuple(input_target_fingerprints)
            if input_target_fingerprints
            else resolved_target_fingerprints
        )
        if rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY:
            target_count = len(targets) if targets else state.registry_targets_seen_count
        else:
            target_count = len(normalized_source_values)
        return BoundedTelegramCollectorHistoryIngestResult(
            status=status,
            ok=ok,
            error_code=error_code,
            config=config,
            state=state,
            mode=mode,
            rollout_scope=rollout_scope,
            source_kind=SOURCE_KIND_PUBLIC_USERNAME,
            source_value_surface=normalized_source_value,
            target_registry_id_suffix=_safe_suffix(target.registry_id) if target is not None else None,
            target_joined=target.joined if target is not None else False,
            target_chat_id_present=target.chat_id_present if target is not None else False,
            messages_requested=history_limit,
            messages_seen=messages_seen,
            would_insert_source_messages_count=would_insert,
            would_append_source_versions_count=would_append,
            would_insert_outbox_events_count=would_outbox,
            would_skip_same_hash_count=would_skip,
            source_messages_created_count=created_count,
            source_versions_appended_count=version_count,
            outbox_events_inserted_count=outbox_count,
            source_created_events_count=created_event_count,
            idempotent_noop_count=noop_count,
            duplicate_noop_proof_count=duplicate_noop_count,
            redis_events_published_count=0 if publish is None else publish.published_count,
            event_outbox_marked_published_count=0 if publish is None else publish.marked_published_count,
            source_message_id_suffixes=tuple(source_message_suffixes),
            event_id_suffixes=(),
            redis_message_id_suffixes=(),
            exact_channel_target_fingerprint=_fingerprint(
                "channel_target",
                f"{SOURCE_KIND_PUBLIC_USERNAME}:{normalized_source_value}",
            )
            if normalized_source_value is not None
            else None,
            registry_target_fingerprint=_target_fingerprint(target),
            source_message_fingerprints=source_readback.source_message_fingerprints,
            source_outbox_event_fingerprints=tuple(source_outbox_event_fingerprints or publish_event_fingerprints),
            redis_message_fingerprints=publish_redis_fingerprints,
            readback=source_readback,
            target_count=target_count,
            max_targets=config.max_targets,
            target_fingerprints=report_target_fingerprints,
            per_channel_results=tuple(dict(item) for item in channel_results),
            partial_committed_target_fingerprints=tuple(partial_committed_target_fingerprints),
            error_class=error_class,
            history_read_failure_cause_bucket=history_read_failure_cause_bucket
            if history_read_failure_cause_bucket in _HISTORY_READ_FAILURE_CAUSE_BUCKETS
            else None,
        )

    if not config.operator_approved:
        return make_result("blocked", "operator_approval_missing")
    if mode not in {MODE_PLAN, MODE_EXECUTE}:
        return make_result("blocked", "mode_invalid")
    if rollout_scope_error is not None:
        return make_result("blocked", rollout_scope_error)
    if config.source_kind != SOURCE_KIND_PUBLIC_USERNAME:
        return make_result("blocked", "source_kind_unsupported")
    if config.chat_id is not None or config.registry_id is not None:
        return make_result("blocked", "direct_chat_or_registry_id_target_not_allowed")
    if target_locator_error is not None:
        return make_result("blocked", target_locator_error)
    if target_locator_present and rollout_scope != ROLLOUT_SCOPE_EXACT_TARGETS:
        return make_result("blocked", "target_locator_rollout_scope_not_allowed")
    if not target_locator_present:
        if target_error is not None:
            return make_result("blocked", target_error)
        target_message_error = _target_message_id_error(
            config,
            rollout_scope=rollout_scope,
            normalized_source_values=normalized_source_values,
        )
        if target_message_error is not None:
            return make_result("blocked", target_message_error)
    if rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY and config.registry_id_suffix:
        return make_result("blocked", "registry_id_suffix_not_allowed_for_full_registry_rollout")
    if _is_three_channel_rollout(config) and config.registry_id_suffix:
        return make_result("blocked", "registry_id_suffix_not_allowed_for_three_channel_rollout")
    if not _valid_history_limit(history_limit):
        return make_result("blocked", "history_limit_out_of_bounds")
    if rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY:
        full_registry_error = _full_registry_bound_error(config)
        if full_registry_error is not None:
            return make_result("blocked", full_registry_error)
    if mode == MODE_EXECUTE and not _confirm_token_valid(config):
        return make_result(
            "blocked",
            "confirm_token_missing" if not (config.confirm_token or "").strip() else "confirm_token_invalid",
        )
    if mode == MODE_PLAN:
        plan_error = _plan_authority_gate_error(config)
        if plan_error is not None:
            return make_result("blocked", plan_error)
    if not config.allow_runtime_config:
        return make_result("blocked", "runtime_config_not_allowed")
    if not config.allow_database_read:
        return make_result("blocked", "database_read_not_allowed")

    if target_locator_present and mode == MODE_EXECUTE:
        write_error = _execute_write_gate_error(config)
        if write_error is not None:
            return make_result("blocked", write_error)
        if config.allow_source_outbox_publish and not config.allow_redis_publish:
            return make_result("blocked", "redis_publish_not_allowed")

    if target_locator_present:
        config, target_locator_load_error = _consume_target_locator(config)
        if target_locator_load_error is not None:
            return make_result("blocked", target_locator_load_error)
        normalized_source_values, target_error = _normalize_source_values(config)
        normalized_source_value = (
            normalized_source_values[0] if len(normalized_source_values) == 1 else None
        )
        input_target_fingerprints = tuple(
            _input_target_fingerprint(value) for value in normalized_source_values
        )
        if target_error is not None:
            return make_result("blocked", target_error)
        target_message_error = _target_message_id_error(
            config,
            rollout_scope=rollout_scope,
            normalized_source_values=normalized_source_values,
        )
        if target_message_error is not None:
            return make_result("blocked", target_message_error)

    loader = runtime_config_loader or CollectorTelegramConfig.from_env
    state.runtime_config_attempted = True
    try:
        runtime_config = loader()
    except Exception as exc:
        return make_result("blocked", "runtime_config_failed", error_class=_safe_exception_class(exc))

    if mode == MODE_EXECUTE:
        write_error = _execute_write_gate_error(config)
        if write_error is not None:
            return make_result("blocked", write_error)
        if config.allow_source_outbox_publish and not config.allow_redis_publish:
            return make_result("blocked", "redis_publish_not_allowed")

    builder = runtime_builder or build_default_bounded_history_ingest_runtime
    state.runtime_builder_attempted = True
    runtime: BoundedTelegramCollectorHistoryIngestRuntimeHandle | None = None
    close_commit = False
    result: BoundedTelegramCollectorHistoryIngestResult | None = None
    try:
        runtime = await builder(runtime_config, state, effective_logger)
        if rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY:
            targets = await _resolve_full_tracked_registry_targets(
                config=config,
                repository=runtime.repository,
                state=state,
            )
        else:
            targets = await _resolve_exact_public_username_targets(
                config=config,
                repository=runtime.repository,
                state=state,
                normalized_source_values=normalized_source_values,
            )

        if mode == MODE_PLAN:
            channel_results = [
                _channel_report(
                    target=target,
                    history_limit=history_limit,
                    status="pass",
                    reason_code="ok",
                )
                for target in targets
            ]
            result = make_result(
                "plan_completed",
                None,
                ok=True,
                target=targets[0] if len(targets) == 1 else None,
                targets=targets,
                channel_results=channel_results,
            )
            raise _RunResultReady

        if not config.allow_telegram_read:
            result = make_result(
                "blocked",
                "telegram_read_not_allowed",
                target=targets[0] if len(targets) == 1 else None,
                targets=targets,
            )
            raise _RunResultReady

        executions: list[_TargetSourceLastExecution] = []
        channel_reports: list[JsonDict] = []
        source_committed_target_fingerprints: list[str] = []
        for target in targets:
            try:
                execution = await _execute_target_source_last(
                    config=config,
                    runtime=runtime,
                    target=target,
                    history_limit=history_limit,
                    state=state,
                    logger=effective_logger,
                )
            except _TargetSourceLastExecutionFailed as exc:
                close_commit = False
                partial_reports = [
                    *channel_reports,
                    _channel_report(
                        target=exc.partial.target,
                        history_limit=history_limit,
                        status="failed",
                        reason_code=exc.error_code,
                        execution=exc.partial,
                        history_read_failure_cause_bucket=exc.history_read_failure_cause_bucket,
                    ),
                ]
                if exc.partial.source_commit_durable:
                    source_committed_target_fingerprints.append(target_report_fingerprint(exc.partial.target))
                aggregate = _aggregate_target_executions([*executions, exc.partial])
                result = make_result(
                    "failed",
                    exc.error_code,
                    error_class=exc.error_class,
                    target=targets[0] if len(targets) == 1 else None,
                    targets=targets,
                    messages_seen=aggregate["messages_seen"],
                    created_count=aggregate["created_count"],
                    version_count=aggregate["version_count"],
                    outbox_count=aggregate["outbox_count"],
                    created_event_count=aggregate["created_event_count"],
                    noop_count=aggregate["noop_count"],
                    duplicate_noop_count=aggregate["duplicate_noop_count"],
                    source_message_suffixes=aggregate["source_message_suffixes"],
                    source_outbox_event_fingerprints=aggregate["source_outbox_event_fingerprints"],
                    readback=aggregate["readback"],
                    channel_results=partial_reports,
                    partial_committed_target_fingerprints=source_committed_target_fingerprints,
                    history_read_failure_cause_bucket=exc.history_read_failure_cause_bucket,
                )
                raise _RunResultReady

            executions.append(execution)
            source_committed_target_fingerprints.append(target_report_fingerprint(target))
            channel_reports.append(
                _channel_report(
                    target=target,
                    history_limit=history_limit,
                    status="pass",
                    reason_code="ok",
                    execution=execution,
                )
            )

        aggregate = _aggregate_target_executions(executions)
        publish_result = PublishEventsResult()
        if config.allow_source_outbox_publish:
            if runtime.redis_publisher is None:
                close_commit = False
                result = make_result(
                    "blocked",
                    "redis_runtime_unavailable",
                    target=targets[0] if len(targets) == 1 else None,
                    targets=targets,
                    messages_seen=aggregate["messages_seen"],
                    created_count=aggregate["created_count"],
                    version_count=aggregate["version_count"],
                    outbox_count=aggregate["outbox_count"],
                    created_event_count=aggregate["created_event_count"],
                    noop_count=aggregate["noop_count"],
                    duplicate_noop_count=aggregate["duplicate_noop_count"],
                    source_message_suffixes=aggregate["source_message_suffixes"],
                    source_outbox_event_fingerprints=aggregate["source_outbox_event_fingerprints"],
                    readback=aggregate["readback"],
                    channel_results=channel_reports,
                    partial_committed_target_fingerprints=source_committed_target_fingerprints,
                )
                raise _RunResultReady
            try:
                publish_result = await _publish_source_outbox_events(
                    repository=runtime.repository,
                    publisher=runtime.redis_publisher,
                    events=aggregate["outbox_events"],
                    state=state,
                    commit_mark_published=runtime.commit,
                )
            except BoundedHistoryIngestError as exc:
                close_commit = False
                result = make_result(
                    "failed",
                    exc.error_code,
                    error_class=_safe_exception_class(exc.__cause__) if exc.__cause__ is not None else None,
                    target=targets[0] if len(targets) == 1 else None,
                    targets=targets,
                    messages_seen=aggregate["messages_seen"],
                    created_count=aggregate["created_count"],
                    version_count=aggregate["version_count"],
                    outbox_count=aggregate["outbox_count"],
                    created_event_count=aggregate["created_event_count"],
                    noop_count=aggregate["noop_count"],
                    duplicate_noop_count=aggregate["duplicate_noop_count"],
                    publish=exc.partial_publish,
                    source_message_suffixes=aggregate["source_message_suffixes"],
                    source_outbox_event_fingerprints=aggregate["source_outbox_event_fingerprints"],
                    readback=aggregate["readback"],
                    channel_results=channel_reports,
                    partial_committed_target_fingerprints=source_committed_target_fingerprints,
                )
                raise _RunResultReady

        close_commit = True
        result = make_result(
            "completed",
            None,
            ok=True,
            target=targets[0] if len(targets) == 1 else None,
            targets=targets,
            messages_seen=aggregate["messages_seen"],
            created_count=aggregate["created_count"],
            version_count=aggregate["version_count"],
            outbox_count=aggregate["outbox_count"],
            created_event_count=aggregate["created_event_count"],
            noop_count=aggregate["noop_count"],
            duplicate_noop_count=aggregate["duplicate_noop_count"],
            publish=publish_result,
            source_message_suffixes=aggregate["source_message_suffixes"],
            source_outbox_event_fingerprints=aggregate["source_outbox_event_fingerprints"],
            readback=aggregate["readback"],
            channel_results=channel_reports,
            partial_committed_target_fingerprints=(),
        )
        raise _RunResultReady
    except _RunResultReady:
        pass
    except BoundedHistoryIngestError as exc:
        result = make_result(
            "blocked",
            exc.error_code,
            history_read_failure_cause_bucket=exc.history_read_failure_cause_bucket,
        )
    except Exception as exc:
        result = make_result("failed", "unexpected_failure", error_class=_safe_exception_class(exc))
    finally:
        if runtime is not None:
            try:
                await runtime.close(close_commit)
            except Exception as exc:
                result = make_result(
                    "failed",
                    _runtime_close_error_code(close_commit),
                    error_class=_safe_exception_class(exc),
                )

    assert result is not None
    return result


def run_bounded_telegram_collector_history_ingest_sync(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    runtime_config_loader: RuntimeConfigLoader | None = None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedTelegramCollectorHistoryIngestResult | BoundedTelegramCollectorGitHubSearchResult:
    return asyncio.run(
        run_bounded_telegram_collector_history_ingest(
            config,
            runtime_config_loader=runtime_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return BoundedTelegramCollectorHistoryIngestResult(
        status="blocked",
        ok=False,
        error_code=error_code,
        config=BoundedTelegramCollectorHistoryIngestConfig(),
    ).to_sanitized_dict()


def search_argument_error_report(error_code: str) -> dict[str, Any]:
    return BoundedTelegramCollectorGitHubSearchResult(
        status="blocked",
        reason_code=error_code,
        config=BoundedTelegramCollectorHistoryIngestConfig(mode=MODE_SEARCH),
    ).to_sanitized_dict()


async def _resolve_exact_public_username_target(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    repository: BoundedHistoryRepository,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> _ResolvedTarget:
    normalized_source_values, error_code = _normalize_source_values(config)
    if error_code is not None:
        raise BoundedHistoryIngestError(error_code)
    if len(normalized_source_values) != 1:
        raise BoundedHistoryIngestError("target_count_must_equal_one")
    targets = await _resolve_exact_public_username_targets(
        config=config,
        repository=repository,
        state=state,
        normalized_source_values=normalized_source_values,
    )
    return targets[0]


async def _resolve_exact_public_username_targets(
    *,
    config: BoundedTelegramCollectorHistoryIngestConfig,
    repository: BoundedHistoryRepository,
    state: BoundedTelegramCollectorHistoryIngestState,
    normalized_source_values: Sequence[str],
) -> tuple[_ResolvedTarget, ...]:
    targets: list[_ResolvedTarget] = []
    for normalized_source_value in normalized_source_values:
        targets.append(
            await _resolve_exact_public_username_target_value(
                config=config,
                repository=repository,
                state=state,
                normalized_source_value=normalized_source_value,
            )
        )
    return tuple(targets)


async def _resolve_full_tracked_registry_targets(
    *,
    config: BoundedTelegramCollectorHistoryIngestConfig,
    repository: BoundedHistoryRepository,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> tuple[_ResolvedTarget, ...]:
    max_targets = _required_full_registry_max_targets(config)
    state.registry_lookup_attempted = True
    state.database_read_attempted = True
    rows = list(await repository.list_reconcile_targets(max_targets + 1))
    state.registry_targets_seen_count += len(rows)
    if not rows:
        raise BoundedHistoryIngestError("registry_targets_empty")
    if len(rows) > max_targets:
        raise BoundedHistoryIngestError("target_count_exceeds_max_targets")

    targets: list[_ResolvedTarget] = []
    target_fingerprints: set[str] = set()
    source_fingerprints: set[str] = set()
    for row in rows:
        source_value = _normalize_source_value(_row_value(row, "source_value"))
        if source_value is None:
            raise BoundedHistoryIngestError("registry_target_source_value_invalid")
        if _looks_like_direct_chat_id(source_value) or _looks_like_registry_id(source_value):
            raise BoundedHistoryIngestError("registry_target_source_value_invalid")
        target = _resolved_target_from_registry_row(
            config=config,
            row=row,
            normalized_source_value=source_value,
        )
        target_fingerprint = _target_fingerprint(target)
        source_fingerprint = _input_target_fingerprint(target.source_value_surface)
        if target_fingerprint in target_fingerprints or source_fingerprint in source_fingerprints:
            raise BoundedHistoryIngestError("registry_target_duplicate")
        if target_fingerprint is not None:
            target_fingerprints.add(target_fingerprint)
        source_fingerprints.add(source_fingerprint)
        targets.append(target)
    return tuple(targets)


async def _resolve_exact_public_username_target_value(
    *,
    config: BoundedTelegramCollectorHistoryIngestConfig,
    repository: BoundedHistoryRepository,
    state: BoundedTelegramCollectorHistoryIngestState,
    normalized_source_value: str,
) -> _ResolvedTarget:
    if not normalized_source_value:
        raise BoundedHistoryIngestError("source_value_missing")
    state.registry_lookup_attempted = True
    state.database_read_attempted = True
    rows = list(await repository.find_public_username_registry_targets(normalized_source_value))
    state.registry_targets_seen_count += len(rows)
    if not rows:
        raise BoundedHistoryIngestError("registry_target_missing")
    if len(rows) > 1:
        raise BoundedHistoryIngestError("registry_target_multiple")

    return _resolved_target_from_registry_row(
        config=config,
        row=rows[0],
        normalized_source_value=normalized_source_value,
    )


def _resolved_target_from_registry_row(
    *,
    config: BoundedTelegramCollectorHistoryIngestConfig,
    row: Any,
    normalized_source_value: str,
) -> _ResolvedTarget:
    registry_id = _row_value(row, "registry_id")
    if not isinstance(registry_id, str) or not registry_id:
        registry_id = str(registry_id) if isinstance(registry_id, UUID) else ""
    if not registry_id:
        raise BoundedHistoryIngestError("registry_id_invalid")
    if (
        _normalize_rollout_scope(config.rollout_scope)[0] != ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY
        and config.registry_id_suffix
        and not registry_id.endswith(config.registry_id_suffix)
    ):
        raise BoundedHistoryIngestError("registry_id_suffix_mismatch")
    if _row_value(row, "source_kind") != SOURCE_KIND_PUBLIC_USERNAME:
        raise BoundedHistoryIngestError("source_kind_unsupported")
    if _row_value(row, "desired_state") != "active":
        raise BoundedHistoryIngestError("registry_target_not_active")
    access_state = _row_value(row, "access_state")
    if access_state != "joined":
        raise BoundedHistoryIngestError("registry_target_not_joined")
    chat_id = _row_value(row, "chat_id")
    if chat_id is None:
        raise BoundedHistoryIngestError("registry_target_chat_id_missing")
    try:
        chat_id_int = int(chat_id)
    except (TypeError, ValueError) as exc:
        raise BoundedHistoryIngestError("registry_target_chat_id_invalid") from exc
    return _ResolvedTarget(
        chat_id=chat_id_int,
        registry_id=registry_id,
        source_value_surface=normalized_source_value,
        joined=True,
        chat_id_present=True,
        last_seen_message_id=_optional_int(_row_value(row, "last_seen_message_id")),
        last_seen_message_date=_optional_datetime(_row_value(row, "last_seen_message_date")),
    )


async def _commit_source_ingest(runtime: BoundedTelegramCollectorHistoryIngestRuntimeHandle) -> None:
    if runtime.commit is None:
        raise BoundedHistoryIngestError("runtime_commit_unavailable")
    await runtime.commit()


async def _execute_target_source_last(
    *,
    config: BoundedTelegramCollectorHistoryIngestConfig,
    runtime: BoundedTelegramCollectorHistoryIngestRuntimeHandle,
    target: _ResolvedTarget,
    history_limit: int,
    state: BoundedTelegramCollectorHistoryIngestState,
    logger: logging.Logger,
) -> _TargetSourceLastExecution:
    selected_messages: list[Mapping[str, Any]] = []
    created_count = 0
    version_count = 0
    outbox_count = 0
    created_event_count = 0
    noop_count = 0
    duplicate_noop_count = 0
    outbox_events: list[OutboxEventRow] = []
    outbox_event_fingerprints: list[str] = []
    source_message_suffixes: list[str] = []
    readback = SourceLastReadbackResult()
    source_commit_durable = False
    history_window_attempts = 0
    exact_message_read_attempted = False
    exact_message_read_succeeded = False
    exact_message_read_failure_cause_bucket: str | None = None

    def partial() -> _TargetSourceLastExecution:
        return _TargetSourceLastExecution(
            target=target,
            messages_seen=len(selected_messages),
            history_window_attempts=history_window_attempts,
            exact_message_read_attempted=exact_message_read_attempted,
            exact_message_read_succeeded=exact_message_read_succeeded,
            exact_message_read_failure_cause_bucket=exact_message_read_failure_cause_bucket
            if exact_message_read_failure_cause_bucket in _EXACT_MESSAGE_READ_FAILURE_CAUSE_BUCKETS
            else None,
            source_messages_created_count=created_count,
            source_versions_appended_count=version_count,
            outbox_events_inserted_count=outbox_count,
            source_created_events_count=created_event_count,
            idempotent_noop_count=noop_count,
            duplicate_noop_proof_count=duplicate_noop_count,
            outbox_events=tuple(outbox_events),
            source_message_suffixes=tuple(source_message_suffixes),
            source_outbox_event_fingerprints=tuple(outbox_event_fingerprints),
            readback=readback,
            source_commit_durable=source_commit_durable,
        )

    try:
        state.telegram_read_attempted = True
        attempt_start = state.history_window_attempts
        exact_attempted_before = state.exact_message_read_attempted
        try:
            messages = await _fetch_history_for_target(
                history_client=runtime.history_client,
                target=target,
                history_limit=history_limit,
                target_message_id=config.target_message_id,
                state=state,
            )
        finally:
            history_window_attempts = state.history_window_attempts - attempt_start
            exact_message_read_attempted = state.exact_message_read_attempted and not exact_attempted_before
            exact_message_read_succeeded = (
                state.exact_message_read_succeeded if exact_message_read_attempted else False
            )
            exact_message_read_failure_cause_bucket = (
                state.exact_message_read_failure_cause_bucket if exact_message_read_attempted else None
            )
        state.telegram_read_called = True
        if len(messages) > history_limit:
            raise _TargetSourceLastExecutionFailed(
                "history_result_exceeds_requested_limit",
                partial=partial(),
            )
        selected_messages = [dict(message) for message in messages if isinstance(message, Mapping)][:history_limit]
        if config.target_message_id is not None:
            selected_messages = [
                message for message in selected_messages if _message_id(message) == config.target_message_id
            ]
            if not selected_messages:
                raise _TargetSourceLastExecutionFailed(
                    "target_message_missing",
                    partial=partial(),
                )
        _validate_selected_history_messages_for_target(selected_messages, target)

        processor = HistoryMessageIngestProcessor(
            repository=runtime.repository,
            projection_builder=MessageProjectionBuilder(logger=logger),
            outbox_builder=CollectorOutboxBuilder(IdempotencyPolicy()),
            state=state,
        )

        last_seen_message_id: int | None = None
        last_seen_message_date: datetime | None = None
        for message in reversed(selected_messages):
            applied = await processor.apply_history_message(message)
            created_count += int(applied.source_message_created)
            version_count += int(applied.source_version_appended)
            outbox_count += int(applied.outbox_event_inserted)
            noop_count += int(applied.idempotent_noop)
            if applied.source_message_id_suffix is not None:
                source_message_suffixes.append(applied.source_message_id_suffix)
            if applied.outbox_event is not None:
                outbox_events.append(applied.outbox_event)
                outbox_event_fingerprints.append(_fingerprint("source_outbox_event", applied.outbox_event.event_id))
                created_event_count += int(applied.outbox_event.event_type == "source_message.created.v1")
            message_id = _message_id(message)
            message_date = _message_date(message)
            if message_id is not None and (last_seen_message_id is None or message_id > last_seen_message_id):
                last_seen_message_id = message_id
                last_seen_message_date = message_date

        if selected_messages and (created_count > 0 or version_count > 0 or outbox_count > 0):
            async with runtime.repository.transaction():
                state.channel_cursor_write_attempted = True
                await runtime.repository.update_channel_sync_cursor(
                    registry_id=target.registry_id,
                    last_seen_message_id=last_seen_message_id,
                    last_seen_message_date=last_seen_message_date,
                    last_history_sync_at=datetime.now(timezone.utc),
                )

        if runtime.commit is not None or config.allow_source_outbox_publish:
            try:
                await _commit_source_ingest(runtime)
                source_commit_durable = True
            except BoundedHistoryIngestError as exc:
                raise _TargetSourceLastExecutionFailed(exc.error_code, partial=partial()) from exc
            except Exception as exc:
                raise _TargetSourceLastExecutionFailed(
                    "runtime_commit_failed",
                    partial=partial(),
                    error_class=_safe_exception_class(exc),
                ) from exc
        else:
            source_commit_durable = True

        try:
            duplicate_noop_count = await _prove_duplicate_noop(
                processor=processor,
                messages=selected_messages,
            )
            readback = await _readback_source_last_proof(
                repository=runtime.repository,
                projection_builder=MessageProjectionBuilder(logger=logger),
                messages=selected_messages,
                target=target,
                state=state,
            )
            if runtime.commit is not None:
                await _commit_source_ingest(runtime)
        except BoundedHistoryIngestError as exc:
            raise _TargetSourceLastExecutionFailed(exc.error_code, partial=partial()) from exc
        except Exception as exc:
            raise _TargetSourceLastExecutionFailed(
                "source_readback_commit_failed",
                partial=partial(),
                error_class=_safe_exception_class(exc),
            ) from exc

        return partial()
    except _TargetSourceLastExecutionFailed:
        raise
    except BoundedHistoryIngestError as exc:
        raise _TargetSourceLastExecutionFailed(
            exc.error_code,
            partial=partial(),
            history_read_failure_cause_bucket=exc.history_read_failure_cause_bucket,
        ) from exc
    except Exception as exc:
        raise _TargetSourceLastExecutionFailed(
            "unexpected_failure",
            partial=partial(),
            error_class=_safe_exception_class(exc),
        ) from exc


async def _prove_duplicate_noop(
    *,
    processor: HistoryMessageIngestProcessor,
    messages: Sequence[Mapping[str, Any]],
) -> int:
    duplicate_noops = 0
    for message in reversed([dict(item) for item in messages if isinstance(item, Mapping)]):
        applied = await processor.apply_history_message(message)
        if not applied.idempotent_noop:
            raise BoundedHistoryIngestError("duplicate_noop_proof_failed")
        duplicate_noops += 1
    return duplicate_noops


async def _readback_source_last_proof(
    *,
    repository: BoundedHistoryRepository,
    projection_builder: MessageProjectionBuilder,
    messages: Sequence[Mapping[str, Any]],
    target: _ResolvedTarget,
    state: BoundedTelegramCollectorHistoryIngestState,
) -> SourceLastReadbackResult:
    source_current_found = 0
    source_version_rows = 0
    source_created_events = 0
    source_outbox_events = 0
    fingerprints: list[str] = []
    seen_source_ids: set[str] = set()

    for message in reversed([dict(item) for item in messages if isinstance(item, Mapping)]):
        projection = projection_builder.build_source_projection(message)
        if projection.chat_id != target.chat_id:
            raise BoundedHistoryIngestError("non_target_channel_readback_mismatch")
        state.database_read_attempted = True
        row = await repository.get_source_message(
            platform="telegram",
            chat_id=projection.chat_id,
            message_id=projection.message_id,
        )
        if row is None:
            raise BoundedHistoryIngestError("source_current_readback_missing")
        source_message_id = _require_source_message_id(row)
        if source_message_id in seen_source_ids:
            continue
        seen_source_ids.add(source_message_id)
        latest = await repository.get_latest_version(source_message_id)
        if latest is None:
            raise BoundedHistoryIngestError("source_version_readback_missing")
        if str(latest.get("content_hash")) != projection.content_hash:
            raise BoundedHistoryIngestError("source_version_content_hash_mismatch")
        version_count = await repository.count_source_message_versions(source_message_id)
        created_count = await repository.count_source_created_events(source_message_id)
        outbox_count = await repository.count_source_outbox_events(source_message_id)
        if version_count < 1:
            raise BoundedHistoryIngestError("source_version_readback_missing")
        if outbox_count < 1:
            raise BoundedHistoryIngestError("source_outbox_readback_missing")
        source_current_found += 1
        source_version_rows += version_count
        source_created_events += created_count
        source_outbox_events += outbox_count
        fingerprints.append(_fingerprint("source_message", source_message_id))

    return SourceLastReadbackResult(
        source_current_found_count=source_current_found,
        source_version_rows_count=source_version_rows,
        source_created_events_count=source_created_events,
        source_outbox_events_count=source_outbox_events,
        source_message_fingerprints=tuple(fingerprints),
    )


def _aggregate_target_executions(executions: Sequence[_TargetSourceLastExecution]) -> dict[str, Any]:
    readback = SourceLastReadbackResult(
        source_current_found_count=sum(item.readback.source_current_found_count for item in executions),
        source_version_rows_count=sum(item.readback.source_version_rows_count for item in executions),
        source_created_events_count=sum(item.readback.source_created_events_count for item in executions),
        source_outbox_events_count=sum(item.readback.source_outbox_events_count for item in executions),
        source_message_fingerprints=tuple(
            fingerprint
            for item in executions
            for fingerprint in item.readback.source_message_fingerprints
        ),
    )
    return {
        "messages_seen": sum(item.messages_seen for item in executions),
        "created_count": sum(item.source_messages_created_count for item in executions),
        "version_count": sum(item.source_versions_appended_count for item in executions),
        "outbox_count": sum(item.outbox_events_inserted_count for item in executions),
        "created_event_count": sum(item.source_created_events_count for item in executions),
        "noop_count": sum(item.idempotent_noop_count for item in executions),
        "duplicate_noop_count": sum(item.duplicate_noop_proof_count for item in executions),
        "outbox_events": tuple(event for item in executions for event in item.outbox_events),
        "source_message_suffixes": tuple(
            suffix for item in executions for suffix in item.source_message_suffixes
        ),
        "source_outbox_event_fingerprints": tuple(
            fingerprint
            for item in executions
            for fingerprint in item.source_outbox_event_fingerprints
        ),
        "readback": readback,
    }


def _channel_report(
    *,
    target: _ResolvedTarget,
    history_limit: int,
    status: str,
    reason_code: str,
    execution: _TargetSourceLastExecution | None = None,
    history_read_failure_cause_bucket: str | None = None,
) -> JsonDict:
    readback = execution.readback if execution is not None else SourceLastReadbackResult()
    return {
        "target_fingerprint": _input_target_fingerprint(target.source_value_surface),
        "registry_target_fingerprint": _target_fingerprint(target),
        "status": status,
        "reason_code": reason_code,
        "history_read_failure_cause_bucket": history_read_failure_cause_bucket
        if history_read_failure_cause_bucket in _HISTORY_READ_FAILURE_CAUSE_BUCKETS
        else None,
        "messages_requested": history_limit,
        "messages_seen": 0 if execution is None else execution.messages_seen,
        "history_window_attempts": 0 if execution is None else execution.history_window_attempts,
        "exact_message_read_attempted": False if execution is None else execution.exact_message_read_attempted,
        "exact_message_read_succeeded": False if execution is None else execution.exact_message_read_succeeded,
        "exact_message_read_failure_cause_bucket": None
        if execution is None
        else execution.exact_message_read_failure_cause_bucket
        if execution.exact_message_read_failure_cause_bucket in _EXACT_MESSAGE_READ_FAILURE_CAUSE_BUCKETS
        else None,
        "bounded_counts": {
            "source_messages_created": 0 if execution is None else execution.source_messages_created_count,
            "source_versions_created": 0 if execution is None else execution.source_versions_appended_count,
            "source_created_events": 0 if execution is None else execution.source_created_events_count,
            "source_normalize_handoffs": 0,
            "duplicate_noops": 0 if execution is None else execution.duplicate_noop_proof_count,
        },
        "readback": {
            "source_current_found_count": readback.source_current_found_count,
            "source_version_rows_count": readback.source_version_rows_count,
            "source_created_events_count": readback.source_created_events_count,
            "source_outbox_events_count": readback.source_outbox_events_count,
        },
        "source_message_fingerprints": list(readback.source_message_fingerprints),
        "source_outbox_event_fingerprints": []
        if execution is None
        else list(execution.source_outbox_event_fingerprints),
        "duplicate_noop_proof": {
            "proved_count": 0 if execution is None else execution.duplicate_noop_proof_count,
            "without_second_telegram_read": True,
        },
        "source_commit_durable": False if execution is None else execution.source_commit_durable,
    }


async def _publish_source_outbox_events(
    *,
    repository: BoundedHistoryRepository,
    publisher: BoundedHistoryRedisPublisher,
    events: Sequence[OutboxEventRow],
    state: BoundedTelegramCollectorHistoryIngestState,
    commit_mark_published: Callable[[], Awaitable[None]] | None,
) -> PublishEventsResult:
    resolver = OutboxRouteResolver()
    published = 0
    marked = 0
    event_fingerprints: list[str] = []
    redis_fingerprints: list[str] = []
    for row in events:
        if row.status != "pending":
            continue
        if row.aggregate_type != ROOT_OBJECT_TYPE or row.event_type not in _SOURCE_EVENT_TYPES:
            raise BoundedHistoryIngestError("source_outbox_event_contract_mismatch")
        try:
            route = resolver.resolve(row)
        except UnsupportedOutboxEventTypeError as exc:
            raise BoundedHistoryIngestError("source_outbox_route_invalid") from exc
        if route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            raise BoundedHistoryIngestError("source_outbox_route_invalid")
        message = _build_stream_message(row, route)
        state.source_outbox_publish_attempted = True
        state.redis_publish_attempted = True
        try:
            redis_message_id = await publisher.publish(route, message)
        except Exception as exc:
            raise BoundedHistoryIngestError("redis_publish_failed") from exc
        partial_after_xadd = PublishEventsResult(
            published_count=published + 1,
            marked_published_count=marked,
            event_fingerprints=tuple(
                value for value in [*event_fingerprints, _fingerprint("source_outbox_event", row.event_id)] if value
            ),
            redis_message_fingerprints=tuple(
                value for value in [*redis_fingerprints, _fingerprint("redis_message", redis_message_id)] if value
            ),
        )
        try:
            async with repository.transaction():
                state.event_outbox_status_write_attempted = True
                try:
                    marked_result = await repository.mark_outbox_published(
                        event_id=row.event_id,
                        published_at=datetime.now(timezone.utc),
                    )
                except Exception as exc:
                    raise BoundedHistoryIngestError(
                        "event_outbox_mark_published_failed",
                        partial_publish=partial_after_xadd,
                    ) from exc
                if marked_result is False:
                    raise BoundedHistoryIngestError(
                        "event_outbox_mark_published_failed",
                        partial_publish=partial_after_xadd,
                    )
        except BoundedHistoryIngestError:
            raise
        except Exception as exc:
            raise BoundedHistoryIngestError(
                "event_outbox_mark_published_commit_failed",
                partial_publish=partial_after_xadd,
            ) from exc
        if commit_mark_published is None:
            raise BoundedHistoryIngestError(
                "event_outbox_mark_published_commit_unavailable",
                partial_publish=partial_after_xadd,
            )
        try:
            await commit_mark_published()
        except Exception as exc:
            raise BoundedHistoryIngestError(
                "event_outbox_mark_published_commit_failed",
                partial_publish=partial_after_xadd,
            ) from exc
        published += 1
        marked += 1
        event_fingerprints.append(_fingerprint("source_outbox_event", row.event_id))
        redis_fingerprints.append(_fingerprint("redis_message", redis_message_id))
    return PublishEventsResult(
        published_count=published,
        marked_published_count=marked,
        event_fingerprints=tuple(value for value in event_fingerprints if value),
        redis_message_fingerprints=tuple(value for value in redis_fingerprints if value),
    )


def _build_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=str(row.event_id),
    )


def _coerce_outbox_event_row(row: Any | None) -> OutboxEventRow | None:
    if row is None:
        return None
    if isinstance(row, OutboxEventRow):
        return row
    if not isinstance(row, Mapping):
        raise BoundedHistoryIngestError("outbox_event_readback_invalid")
    payload = row.get("payload_json") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        fail_count=int(row.get("fail_count") or 0),
        created_at=row.get("created_at") or datetime.now(timezone.utc),
    )


def _authorization_state_from_tdlib_payload(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    if payload.get("@type") == "updateAuthorizationState":
        authorization_state = payload.get("authorization_state")
        return authorization_state if isinstance(authorization_state, Mapping) else None
    payload_type = payload.get("@type")
    if isinstance(payload_type, str) and payload_type.startswith("authorizationState"):
        return payload
    return None


def _is_bounded_auth_extra(value: object) -> bool:
    return isinstance(value, str) and value.startswith(f"{RUNNER_NAME}:")


def _consume_target_locator(
    config: BoundedTelegramCollectorHistoryIngestConfig,
) -> tuple[BoundedTelegramCollectorHistoryIngestConfig, str | None]:
    argument_error = _target_locator_argument_error(config)
    if argument_error is not None:
        return config, argument_error
    if config.target_locator_path is None:
        return config, None
    try:
        locator = _read_target_locator(config.target_locator_path)
    except BoundedHistoryIngestError as exc:
        return config, exc.error_code
    return (
        replace(
            config,
            source_value=str(locator["source_value"]),
            source_values=(),
            target_message_id=int(locator["target_message_id"]),
        ),
        None,
    )


def _target_locator_argument_error(
    config: BoundedTelegramCollectorHistoryIngestConfig,
) -> str | None:
    if config.allow_target_locator_write or config.target_locator_output_path is not None:
        return "target_locator_write_search_mode_required"
    if config.target_locator_path is None:
        return None
    if config.source_value is not None or config.source_values or config.target_message_id is not None:
        return "target_locator_direct_target_ambiguity"
    return None


def _validate_target_locator_output_path(value: object | None) -> str | None:
    try:
        parent_fd, target_name = _open_target_locator_parent_fd(
            value,
            reason_prefix="target_locator_output",
        )
    except BoundedHistoryIngestError as exc:
        return exc.error_code
    try:
        try:
            target_stat = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        except OSError:
            return "target_locator_output_parent_invalid"
        if stat.S_ISLNK(target_stat.st_mode):
            return "target_locator_output_target_symlink_not_allowed"
        return "target_locator_output_target_exists"
    finally:
        with contextlib.suppress(OSError):
            os.close(parent_fd)


def _write_target_locator(
    output_path: object | None,
    *,
    source_value: str,
    target_message_id: int,
    target_fingerprint: str,
    selected_message_fingerprint: str,
) -> str | None:
    try:
        locator = {
            "schema_version": TARGET_LOCATOR_SCHEMA_VERSION,
            "source_kind": SOURCE_KIND_PUBLIC_USERNAME,
            "source_value": source_value,
            "target_message_id": target_message_id,
            "target_fingerprint": target_fingerprint,
            "selected_message_fingerprint": selected_message_fingerprint,
        }
        encoded = (
            json.dumps(locator, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        return "target_locator_payload_invalid"
    if len(encoded) > TARGET_LOCATOR_MAX_BYTES:
        return "target_locator_payload_too_large"

    try:
        parent_fd, target_name = _open_target_locator_parent_fd(
            output_path,
            reason_prefix="target_locator_output",
        )
    except BoundedHistoryIngestError as exc:
        return exc.error_code

    fd: int | None = None
    created_by_operation = False
    created_identity: tuple[int, int] | None = None
    error_code: str | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(
                target_name,
                flags,
                TARGET_LOCATOR_PRIVATE_MODE,
                dir_fd=parent_fd,
            )
            created_by_operation = True
        except FileExistsError:
            try:
                existing_stat = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError:
                error_code = "target_locator_output_target_exists"
            else:
                error_code = (
                    "target_locator_output_target_symlink_not_allowed"
                    if stat.S_ISLNK(existing_stat.st_mode)
                    else "target_locator_output_target_exists"
                )
        except OSError as exc:
            error_code = (
                "target_locator_output_target_symlink_not_allowed"
                if exc.errno == errno.ELOOP
                else "target_locator_output_create_failed"
            )

        if fd is not None and error_code is None:
            created_stat = os.fstat(fd)
            created_identity = (created_stat.st_dev, created_stat.st_ino)
            if not stat.S_ISREG(created_stat.st_mode):
                error_code = "target_locator_output_create_failed"
            else:
                try:
                    os.fchmod(fd, TARGET_LOCATOR_PRIVATE_MODE)
                    private_stat = os.fstat(fd)
                except OSError:
                    error_code = "target_locator_output_private_mode_unconfirmed"
                else:
                    if stat.S_IMODE(private_stat.st_mode) != TARGET_LOCATOR_PRIVATE_MODE:
                        error_code = "target_locator_output_private_mode_unconfirmed"

        if fd is not None and error_code is None:
            try:
                written = os.write(fd, encoded)
                if written != len(encoded):
                    error_code = "target_locator_output_write_failed"
                else:
                    os.fsync(fd)
                    if not _created_target_locator_matches(
                        fd,
                        parent_fd,
                        target_name,
                        created_identity=created_identity,
                        expected_size=len(encoded),
                    ):
                        error_code = "target_locator_output_readback_failed"
            except OSError:
                error_code = "target_locator_output_write_failed"
    except Exception:
        error_code = error_code or "target_locator_output_write_failed"
    finally:
        if error_code is not None and created_by_operation:
            if created_identity is None:
                error_code = "target_locator_output_cleanup_unconfirmed"
            elif not _remove_created_target_locator(
                parent_fd,
                target_name,
                created_identity=created_identity,
            ):
                error_code = "target_locator_output_cleanup_failed"
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                if error_code is None:
                    error_code = "target_locator_output_close_failed"
                    if not _created_fd_matches_identity(fd, created_identity):
                        error_code = "target_locator_output_cleanup_unconfirmed"
                    elif not _remove_created_target_locator(
                        parent_fd,
                        target_name,
                        created_identity=created_identity,
                    ):
                        error_code = "target_locator_output_cleanup_failed"
                with contextlib.suppress(OSError):
                    os.close(fd)
        with contextlib.suppress(OSError):
            os.close(parent_fd)
    return error_code


def _remove_created_target_locator(
    parent_fd: int,
    target_name: str,
    *,
    created_identity: tuple[int, int],
) -> bool:
    try:
        current_stat = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return False
    if (
        stat.S_ISREG(current_stat.st_mode)
        and (current_stat.st_dev, current_stat.st_ino) == created_identity
    ):
        try:
            os.unlink(target_name, dir_fd=parent_fd)
        except OSError:
            return False
        return True
    return False


def _created_fd_matches_identity(
    fd: int,
    created_identity: tuple[int, int] | None,
) -> bool:
    if created_identity is None:
        return False
    try:
        opened_stat = os.fstat(fd)
    except OSError:
        return False
    return (opened_stat.st_dev, opened_stat.st_ino) == created_identity


def _created_target_locator_matches(
    fd: int,
    parent_fd: int,
    target_name: str,
    *,
    created_identity: tuple[int, int] | None,
    expected_size: int,
) -> bool:
    if created_identity is None:
        return False
    try:
        opened_stat = os.fstat(fd)
        path_stat = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return False
    return (
        stat.S_ISREG(opened_stat.st_mode)
        and stat.S_ISREG(path_stat.st_mode)
        and (opened_stat.st_dev, opened_stat.st_ino) == created_identity
        and (path_stat.st_dev, path_stat.st_ino) == created_identity
        and stat.S_IMODE(opened_stat.st_mode) == TARGET_LOCATOR_PRIVATE_MODE
        and stat.S_IMODE(path_stat.st_mode) == TARGET_LOCATOR_PRIVATE_MODE
        and opened_stat.st_size == expected_size
        and path_stat.st_size == expected_size
    )


def _read_target_locator(value: object | None) -> dict[str, Any]:
    encoded = _read_target_locator_bytes(value)
    try:
        decoded = encoded.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise BoundedHistoryIngestError("target_locator_input_invalid_utf8") from exc
    try:
        parsed = json.loads(decoded, object_pairs_hook=_target_locator_object_from_pairs)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise BoundedHistoryIngestError("target_locator_input_malformed_json") from exc
    if not isinstance(parsed, dict):
        raise BoundedHistoryIngestError("target_locator_input_field_set_invalid")
    if frozenset(parsed) != _TARGET_LOCATOR_FIELDS:
        raise BoundedHistoryIngestError("target_locator_input_field_set_invalid")
    if parsed.get("schema_version") != TARGET_LOCATOR_SCHEMA_VERSION:
        raise BoundedHistoryIngestError("target_locator_input_schema_unsupported")
    if parsed.get("source_kind") != SOURCE_KIND_PUBLIC_USERNAME:
        raise BoundedHistoryIngestError("target_locator_input_source_kind_unsupported")

    source_value = parsed.get("source_value")
    target_message_id = parsed.get("target_message_id")
    target_fingerprint = parsed.get("target_fingerprint")
    selected_message_fingerprint = parsed.get("selected_message_fingerprint")
    if (
        not isinstance(source_value, str)
        or type(target_message_id) is not int
        or not isinstance(target_fingerprint, str)
        or not isinstance(selected_message_fingerprint, str)
    ):
        raise BoundedHistoryIngestError("target_locator_input_field_type_invalid")
    normalized_source_value = _normalize_source_value(source_value)
    if normalized_source_value is None:
        raise BoundedHistoryIngestError("target_locator_input_source_value_invalid")
    if source_value != normalized_source_value:
        raise BoundedHistoryIngestError("target_locator_input_source_value_not_normalized")
    if not _valid_target_message_id(target_message_id):
        raise BoundedHistoryIngestError("target_locator_input_target_message_id_invalid")
    if target_fingerprint != _input_target_fingerprint(normalized_source_value):
        raise BoundedHistoryIngestError("target_locator_input_target_fingerprint_mismatch")
    if selected_message_fingerprint != _target_message_fingerprint(target_message_id):
        raise BoundedHistoryIngestError(
            "target_locator_input_selected_message_fingerprint_mismatch"
        )
    return parsed


def _read_target_locator_bytes(value: object | None) -> bytes:
    parent_fd, target_name = _open_target_locator_parent_fd(
        value,
        reason_prefix="target_locator_input",
    )
    fd: int | None = None
    try:
        try:
            target_stat = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError as exc:
            raise BoundedHistoryIngestError("target_locator_input_not_found") from exc
        except OSError as exc:
            raise BoundedHistoryIngestError("target_locator_input_read_failed") from exc
        if stat.S_ISLNK(target_stat.st_mode):
            raise BoundedHistoryIngestError("target_locator_input_target_symlink_not_allowed")
        if not stat.S_ISREG(target_stat.st_mode):
            raise BoundedHistoryIngestError("target_locator_input_not_regular")

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NONBLOCK", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(target_name, flags, dir_fd=parent_fd)
        except OSError as exc:
            reason_code = (
                "target_locator_input_target_symlink_not_allowed"
                if exc.errno == errno.ELOOP
                else "target_locator_input_read_failed"
            )
            raise BoundedHistoryIngestError(reason_code) from exc

        try:
            opened_stat = os.fstat(fd)
        except OSError as exc:
            raise BoundedHistoryIngestError("target_locator_input_read_failed") from exc
        if not stat.S_ISREG(opened_stat.st_mode):
            raise BoundedHistoryIngestError("target_locator_input_not_regular")
        if stat.S_IMODE(opened_stat.st_mode) != TARGET_LOCATOR_PRIVATE_MODE:
            raise BoundedHistoryIngestError("target_locator_input_mode_not_private")
        if opened_stat.st_size > TARGET_LOCATOR_MAX_BYTES:
            raise BoundedHistoryIngestError("target_locator_input_too_large")
        try:
            encoded = os.read(fd, TARGET_LOCATOR_MAX_BYTES + 1)
        except OSError as exc:
            raise BoundedHistoryIngestError("target_locator_input_read_failed") from exc
        if len(encoded) > TARGET_LOCATOR_MAX_BYTES:
            raise BoundedHistoryIngestError("target_locator_input_too_large")
        if len(encoded) != opened_stat.st_size:
            raise BoundedHistoryIngestError("target_locator_input_changed_during_read")
        try:
            readback_stat = os.fstat(fd)
        except OSError as exc:
            raise BoundedHistoryIngestError(
                "target_locator_input_changed_during_read"
            ) from exc
        if (
            (readback_stat.st_dev, readback_stat.st_ino)
            != (opened_stat.st_dev, opened_stat.st_ino)
            or not stat.S_ISREG(readback_stat.st_mode)
            or stat.S_IMODE(readback_stat.st_mode) != TARGET_LOCATOR_PRIVATE_MODE
            or readback_stat.st_size != opened_stat.st_size
        ):
            raise BoundedHistoryIngestError("target_locator_input_changed_during_read")
        return encoded
    finally:
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)
        with contextlib.suppress(OSError):
            os.close(parent_fd)


def _target_locator_object_from_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate target locator field")
        result[key] = value
    return result


def _open_target_locator_parent_fd(
    value: object | None,
    *,
    reason_prefix: str,
) -> tuple[int, str]:
    root_components, relative_components = _target_locator_path_components(
        value,
        reason_prefix=reason_prefix,
    )
    directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        current_fd = os.open("/", directory_flags)
    except OSError as exc:
        raise BoundedHistoryIngestError(f"{reason_prefix}_root_not_real") from exc

    parent_components = (*root_components, *relative_components[:-1])
    try:
        for index, component in enumerate(parent_components):
            is_allowed_root_component = index < len(root_components)
            try:
                component_stat = os.stat(
                    component,
                    dir_fd=current_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError as exc:
                reason = (
                    f"{reason_prefix}_root_not_real"
                    if is_allowed_root_component
                    else f"{reason_prefix}_parent_missing"
                )
                raise BoundedHistoryIngestError(reason) from exc
            except OSError as exc:
                raise BoundedHistoryIngestError(f"{reason_prefix}_parent_invalid") from exc
            if stat.S_ISLNK(component_stat.st_mode):
                reason = (
                    f"{reason_prefix}_root_not_real"
                    if is_allowed_root_component
                    else f"{reason_prefix}_parent_symlink_not_allowed"
                )
                raise BoundedHistoryIngestError(reason)
            if not stat.S_ISDIR(component_stat.st_mode):
                reason = (
                    f"{reason_prefix}_root_not_real"
                    if is_allowed_root_component
                    else f"{reason_prefix}_parent_not_directory"
                )
                raise BoundedHistoryIngestError(reason)
            try:
                next_fd = os.open(component, directory_flags, dir_fd=current_fd)
            except OSError as exc:
                if exc.errno == errno.ELOOP:
                    reason = (
                        f"{reason_prefix}_root_not_real"
                        if is_allowed_root_component
                        else f"{reason_prefix}_parent_symlink_not_allowed"
                    )
                elif exc.errno == errno.ENOTDIR:
                    reason = f"{reason_prefix}_parent_not_directory"
                else:
                    reason = f"{reason_prefix}_parent_invalid"
                raise BoundedHistoryIngestError(reason) from exc
            os.close(current_fd)
            current_fd = next_fd
        return current_fd, relative_components[-1]
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(current_fd)
        raise


def _target_locator_path_components(
    value: object | None,
    *,
    reason_prefix: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if not isinstance(value, str) or not value:
        raise BoundedHistoryIngestError(f"{reason_prefix}_path_relative")
    if "\x00" in value or not value.startswith("/"):
        raise BoundedHistoryIngestError(f"{reason_prefix}_path_relative")
    raw_components = value.split("/")
    if value.startswith("/tmp/"):
        root_components = ("tmp",)
        relative_components = tuple(raw_components[2:])
    elif value.startswith("/var/tmp/"):
        root_components = ("var", "tmp")
        relative_components = tuple(raw_components[3:])
    else:
        raise BoundedHistoryIngestError(f"{reason_prefix}_path_outside_allowed_roots")
    if not relative_components or any(
        component in {"", ".", ".."} for component in relative_components
    ):
        raise BoundedHistoryIngestError(f"{reason_prefix}_path_traversal_not_allowed")
    return root_components, relative_components


def _normalize_mode(value: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized


def _normalize_search_source_value(
    config: BoundedTelegramCollectorHistoryIngestConfig,
) -> tuple[str | None, str | None]:
    raw_values: Sequence[object | None]
    if config.source_values:
        raw_values = tuple(config.source_values)
    elif config.source_value is not None:
        raw_values = (config.source_value,)
    else:
        raw_values = ()
    if len(raw_values) != 1:
        return None, "search_requires_exactly_one_target"
    normalized = _normalize_source_value(raw_values[0])
    if normalized is None:
        return None, "search_requires_exactly_one_target"
    if _looks_like_direct_chat_id(normalized):
        return normalized, "direct_chat_id_target_not_allowed"
    if _looks_like_registry_id(normalized):
        return normalized, "direct_registry_id_target_not_allowed"
    return normalized, None


def _search_requested_max_messages(value: object | None) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _search_authority_gate_error(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    normalized_source_value: str | None,
    source_error: str | None,
) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    rollout_scope, rollout_scope_error = _normalize_rollout_scope(config.rollout_scope)
    if rollout_scope_error is not None or rollout_scope != ROLLOUT_SCOPE_EXACT_TARGETS:
        return "search_rollout_scope_not_allowed"
    if config.source_kind != SOURCE_KIND_PUBLIC_USERNAME:
        return "source_kind_unsupported"
    if config.chat_id is not None or config.registry_id is not None:
        return "direct_chat_or_registry_id_target_not_allowed"
    if config.target_locator_path is not None:
        return "target_locator_input_not_allowed_in_search"
    if source_error is not None or normalized_source_value is None:
        return source_error or "search_requires_exactly_one_target"
    if config.max_messages is None:
        return "search_max_messages_required"
    requested_max_messages = _search_requested_max_messages(config.max_messages)
    if requested_max_messages is None or not 1 <= requested_max_messages <= MAX_MESSAGES_HARD_LIMIT:
        return "search_max_messages_out_of_bounds"
    if config.target_message_id is not None:
        return "search_target_message_id_not_allowed"
    if config.registry_id_suffix is not None:
        return "search_registry_suffix_not_allowed"
    if config.max_targets is not None:
        return "search_max_targets_not_allowed"
    if config.target_locator_output_path is not None and not config.allow_target_locator_write:
        return "target_locator_write_authority_missing"
    if config.allow_target_locator_write and not str(config.target_locator_output_path or "").strip():
        return "target_locator_output_path_required"
    if config.allow_target_locator_write:
        locator_path_error = _validate_target_locator_output_path(config.target_locator_output_path)
        if locator_path_error is not None:
            return locator_path_error
    if not (config.confirm_token or "").strip():
        return "search_confirm_token_missing"
    if not _search_confirm_token_valid(config):
        return "search_confirm_token_invalid"
    if (
        config.allow_database_write
        or config.allow_source_message_write
        or config.allow_source_version_write
        or _source_outbox_write_allowed(config)
    ):
        return "search_write_authority_not_allowed"
    if config.allow_source_outbox_publish or config.allow_redis_publish:
        return "search_publish_authority_not_allowed"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_telegram_read:
        return "telegram_read_not_allowed"
    return None


def _history_limit(config: BoundedTelegramCollectorHistoryIngestConfig) -> int:
    value = config.history_limit if config.max_messages is None else config.max_messages
    return int(value)


def _valid_history_limit(value: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_HISTORY_LIMIT


def _normalize_rollout_scope(value: str) -> tuple[str, str | None]:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if normalized in {"", ROLLOUT_SCOPE_EXACT_TARGETS, "exact", "exact_target", "exact_targets"}:
        return ROLLOUT_SCOPE_EXACT_TARGETS, None
    if normalized in {ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY, "full_registry"}:
        return ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY, None
    return normalized, "rollout_scope_unsupported"


def _normalize_source_values(config: BoundedTelegramCollectorHistoryIngestConfig) -> tuple[tuple[str, ...], str | None]:
    rollout_scope, rollout_scope_error = _normalize_rollout_scope(config.rollout_scope)
    if rollout_scope_error is not None:
        return (), None
    if rollout_scope == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY:
        if config.source_value is not None or config.source_values:
            return (), "full_registry_source_value_not_allowed"
        return (), None

    raw_values: Sequence[str | None]
    if config.source_values:
        raw_values = tuple(config.source_values)
    elif config.source_value is not None:
        raw_values = (config.source_value,)
    else:
        raw_values = ()

    normalized_values: list[str] = []
    for raw_value in raw_values:
        normalized = _normalize_source_value(raw_value)
        if normalized is None:
            return tuple(normalized_values), "source_value_missing"
        if _looks_like_direct_chat_id(normalized):
            return tuple(normalized_values), "direct_chat_id_target_not_allowed"
        if _looks_like_registry_id(normalized):
            return tuple(normalized_values), "direct_registry_id_target_not_allowed"
        normalized_values.append(normalized)

    if not normalized_values:
        return (), "source_value_missing"
    if len(normalized_values) not in {1, THREE_CHANNEL_TARGET_COUNT}:
        return tuple(normalized_values), "target_count_must_equal_three"
    if len(set(normalized_values)) != len(normalized_values):
        return tuple(normalized_values), "target_duplicate"
    return tuple(normalized_values), None


def _normalize_source_value(value: object | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    normalized = value.strip().lstrip("@").strip().lower()
    try:
        normalized.encode("utf-8", errors="strict")
    except UnicodeError:
        return None
    return normalized or None


def _target_message_id_error(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    rollout_scope: str,
    normalized_source_values: Sequence[str],
) -> str | None:
    if config.target_message_id is None:
        return None
    if not _valid_target_message_id(config.target_message_id):
        return "target_message_id_invalid"
    if rollout_scope != ROLLOUT_SCOPE_EXACT_TARGETS or len(normalized_source_values) != 1:
        return "target_message_requires_single_exact_target"
    return None


def _valid_target_message_id(value: object) -> bool:
    return type(value) is int and value > 0


def _source_outbox_write_allowed(config: BoundedTelegramCollectorHistoryIngestConfig) -> bool:
    return bool(config.allow_source_outbox_write or config.allow_outbox_write)


def _confirm_token_valid(config: BoundedTelegramCollectorHistoryIngestConfig) -> bool:
    if _normalize_mode(config.mode) == MODE_SEARCH:
        return _search_confirm_token_valid(config)
    if _is_full_registry_rollout(config):
        expected = FULL_REGISTRY_EXECUTE_CONFIRM_TOKEN
    elif _is_three_channel_rollout(config):
        expected = THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN
    else:
        expected = EXECUTE_CONFIRM_TOKEN
    return (config.confirm_token or "").strip() == expected


def _search_confirm_token_valid(config: BoundedTelegramCollectorHistoryIngestConfig) -> bool:
    return (config.confirm_token or "").strip() == SEARCH_CONFIRM_TOKEN


def _is_three_channel_rollout(config: BoundedTelegramCollectorHistoryIngestConfig) -> bool:
    if _is_full_registry_rollout(config):
        return False
    normalized_values, error_code = _normalize_source_values(config)
    return error_code is None and len(normalized_values) == THREE_CHANNEL_TARGET_COUNT


def _is_full_registry_rollout(config: BoundedTelegramCollectorHistoryIngestConfig) -> bool:
    return _normalize_rollout_scope(config.rollout_scope)[0] == ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY


def _schema_version_for_result(config: BoundedTelegramCollectorHistoryIngestConfig) -> str:
    if _is_full_registry_rollout(config):
        return FULL_REGISTRY_SCHEMA_VERSION
    return THREE_CHANNEL_SCHEMA_VERSION if _is_three_channel_rollout(config) else SCHEMA_VERSION


def _full_registry_bound_error(config: BoundedTelegramCollectorHistoryIngestConfig) -> str | None:
    max_targets = config.max_targets
    if max_targets is None:
        return "max_targets_required"
    if not isinstance(max_targets, int) or isinstance(max_targets, bool) or max_targets < 1:
        return "max_targets_out_of_bounds"
    if max_targets > MAX_FULL_REGISTRY_TARGETS:
        return "max_targets_exceeds_hard_cap"
    return None


def _required_full_registry_max_targets(config: BoundedTelegramCollectorHistoryIngestConfig) -> int:
    error = _full_registry_bound_error(config)
    if error is not None:
        raise BoundedHistoryIngestError(error)
    assert config.max_targets is not None
    return config.max_targets


def _looks_like_direct_chat_id(value: str) -> bool:
    candidate = value.removeprefix("+").removeprefix("-")
    return bool(candidate) and candidate.isdigit()


def _looks_like_registry_id(value: str) -> bool:
    with contextlib.suppress(ValueError):
        UUID(value)
        return True
    return False


def _plan_authority_gate_error(config: BoundedTelegramCollectorHistoryIngestConfig) -> str | None:
    if config.allow_telegram_read:
        return "plan_telegram_read_not_allowed"
    if (
        config.allow_database_write
        or config.allow_source_message_write
        or config.allow_source_version_write
        or _source_outbox_write_allowed(config)
    ):
        return "plan_database_write_not_allowed"
    if config.allow_source_outbox_publish or config.allow_redis_publish:
        return "plan_redis_write_not_allowed"
    return None


def _execute_write_gate_error(config: BoundedTelegramCollectorHistoryIngestConfig) -> str | None:
    if not config.allow_telegram_read:
        return "telegram_read_not_allowed"
    if not config.allow_database_write:
        return "database_write_not_allowed"
    if not config.allow_source_message_write:
        return "source_message_write_not_allowed"
    if not config.allow_source_version_write:
        return "source_version_write_not_allowed"
    if not _source_outbox_write_allowed(config):
        return "source_outbox_write_not_allowed"
    return None


def _coerce_source_message_id(row: Mapping[str, Any] | None) -> str | None:
    if row is None:
        return None
    source_message_id = _coerce_uuid_string(row.get("source_message_id"))
    if source_message_id is not None:
        return source_message_id
    raise BoundedHistoryIngestError("source_message_id_invalid")


def _require_source_message_id(row: Mapping[str, Any]) -> str:
    source_message_id = _coerce_uuid_string(row.get("source_message_id"))
    if source_message_id is not None:
        return source_message_id
    raise BoundedHistoryIngestError("source_message_id_invalid")


def _coerce_uuid_string(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, UUID):
        return str(value)
    return None


def _require_version_no(row: Mapping[str, Any]) -> int:
    value = row.get("version_no")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise BoundedHistoryIngestError("source_version_no_invalid")


def _message_id(message: Mapping[str, Any]) -> int | None:
    try:
        return int(message["id"])
    except (KeyError, TypeError, ValueError):
        return None


def _message_date(message: Mapping[str, Any]) -> datetime | None:
    raw = message.get("date")
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _message_chat_id(message: Mapping[str, Any]) -> int | None:
    try:
        return int(message["chat_id"])
    except (KeyError, TypeError, ValueError):
        return None


def _validate_selected_history_messages_for_target(
    messages: Sequence[Mapping[str, Any]],
    target: _ResolvedTarget,
) -> None:
    for message in messages:
        if _message_chat_id(message) != target.chat_id:
            raise BoundedHistoryIngestError("non_target_channel_history_message")


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _safe_suffix(value: object | None, *, width: int = 8) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text[-width:]


def _safe_hash12(value: object | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]


def _fingerprint(kind: str, value: object | None) -> str:
    digest = hashlib.sha256(f"{kind}:{value}".encode("utf-8")).hexdigest()[:16]
    return f"sha256:{digest}"


def _input_target_fingerprint(normalized_source_value: str | None) -> str:
    return _fingerprint(
        "channel_target",
        f"{SOURCE_KIND_PUBLIC_USERNAME}:{normalized_source_value}",
    )


def _target_message_fingerprint(target_message_id: int | None) -> str | None:
    if target_message_id is None:
        return None
    return _fingerprint("target_message", target_message_id)


def _target_fingerprint(target: _ResolvedTarget | None) -> str | None:
    if target is None:
        return None
    return _fingerprint("registry_target", f"{target.registry_id}:{target.chat_id}")


def _is_github_url(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
    except ValueError:
        return False
    if parsed.scheme.lower() not in {"http", "https"} or hostname is None:
        return False
    normalized_hostname = hostname.lower()
    return normalized_hostname == "github.com" or normalized_hostname.endswith(".github.com")


def _sanitize_search_reason_code(value: object) -> str:
    return str(value) if value in _SEARCH_REASON_CODES else "github_url_live_search_failed"


def _report_status(status: str, ok: bool) -> str:
    if ok:
        return "pass"
    if status == "failed":
        return "failed"
    return "blocked"


def _runtime_close_error_code(commit: bool) -> str:
    return "runtime_commit_failed" if commit else "runtime_rollback_failed"


def _classify_tdlib_history_read_error_cause(response: Mapping[str, Any]) -> str:
    code = _tdlib_error_code(response.get("code"))
    message = _tdlib_error_message(response.get("message"))
    normalized = message.replace("_", " ").replace("-", " ")

    runtime_or_access_markers = (
        "authorization",
        "authorized",
        "forbidden",
        "private",
        "access",
        "not enough rights",
        "flood",
        "too many requests",
        "rate limit",
        "timeout",
        "temporarily",
        "connection",
        "network",
        "database",
        "tdlib",
    )
    if code in {401, 403, 429, 500, 502, 503, 504} or any(
        marker in normalized for marker in runtime_or_access_markers
    ):
        return HISTORY_READ_CAUSE_RUNTIME_TDLIB_ACCESS_ISSUE

    target_unavailable_markers = (
        "message not found",
        "chat not found",
        "not found",
        "deleted",
        "unavailable",
        "deactivated",
        "migrated",
    )
    if code == 404 or any(marker in normalized for marker in target_unavailable_markers):
        return HISTORY_READ_CAUSE_TARGET_UNAVAILABLE

    bounded_window_markers = (
        "from message",
        "from message id",
        "from_message_id",
        "offset",
        "limit",
        "history",
        "range",
        "window",
        "too old",
        "too new",
    )
    if any(marker in normalized for marker in bounded_window_markers):
        return HISTORY_READ_CAUSE_BOUNDED_WINDOW_ADJUSTMENT

    request_repair_markers = (
        "bad request",
        "invalid",
        "wrong",
        "unsupported",
        "parameter",
        "parse",
        "chat id",
        "chat_id",
        "message id",
        "message_id",
        "identifier",
    )
    if code == 400 or any(marker in normalized for marker in request_repair_markers):
        return HISTORY_READ_CAUSE_REQUEST_CONSTRUCTION_REPAIR

    return HISTORY_READ_CAUSE_RUNTIME_TDLIB_ACCESS_ISSUE


def _classify_tdlib_exact_message_read_error_cause(response: Mapping[str, Any]) -> str:
    code = _tdlib_error_code(response.get("code"))
    message = _tdlib_error_message(response.get("message"))
    normalized = message.replace("_", " ").replace("-", " ")
    if code == 404 or "message not found" in normalized or "not found" in normalized:
        return EXACT_MESSAGE_READ_CAUSE_NOT_FOUND
    return EXACT_MESSAGE_READ_CAUSE_TDLIB_ERROR


def _tdlib_error_code(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _tdlib_error_message(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.lower()


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


__all__ = [
    "BoundedHistoryIngestError",
    "BoundedTelegramCollectorGitHubSearchResult",
    "BoundedTelegramCollectorHistoryIngestConfig",
    "BoundedTelegramCollectorHistoryIngestResult",
    "BoundedTelegramCollectorHistoryIngestRuntimeBuilder",
    "BoundedTelegramCollectorHistoryIngestRuntimeHandle",
    "BoundedTelegramCollectorHistoryIngestState",
    "DEFAULT_HISTORY_LIMIT",
    "DEFAULT_MAX_MESSAGES",
    "EXECUTE_CONFIRM_TOKEN",
    "FULL_REGISTRY_EXECUTE_CONFIRM_TOKEN",
    "FULL_REGISTRY_SCHEMA_VERSION",
    "MAX_HISTORY_LIMIT",
    "MAX_FULL_REGISTRY_TARGETS",
    "MAX_MESSAGES_HARD_LIMIT",
    "MODE_SEARCH",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "SEARCH_CONFIRM_TOKEN",
    "SEARCH_SCHEMA_VERSION",
    "TARGET_LOCATOR_MAX_BYTES",
    "TARGET_LOCATOR_PRIVATE_MODE",
    "TARGET_LOCATOR_SCHEMA_VERSION",
    "THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN",
    "THREE_CHANNEL_SCHEMA_VERSION",
    "THREE_CHANNEL_TARGET_COUNT",
    "ROLLOUT_SCOPE_EXACT_TARGETS",
    "ROLLOUT_SCOPE_FULL_TRACKED_REGISTRY",
    "argument_error_report",
    "build_default_bounded_history_ingest_runtime",
    "build_default_bounded_history_search_runtime",
    "render_sanitized_json",
    "run_bounded_telegram_collector_history_ingest",
    "run_bounded_telegram_collector_history_ingest_sync",
    "search_argument_error_report",
]
