"""Typed collector models used by the bootstrap skeleton."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class CollectorEnvironment(StrEnum):
    """Deployment environment for collector configuration rules."""

    DEV = "dev"
    TEST = "test"
    PROD = "prod"


class CollectorMode(StrEnum):
    """Runtime mode for the collector service."""

    LIVE = "live"
    REPLAY = "replay"


class CollectorLifecycleState(StrEnum):
    """Lifecycle states for the bootstrap runtime."""

    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(slots=True, frozen=True)
class TrackedChat:
    """Collector-local projection of the tracked channel registry row."""

    registry_id: str
    chat_id: int | None
    desired_state: str
    access_state: str
    source_kind: str
    source_value: str
    priority_weight: int = 100
    last_seen_message_id: int | None = None
    last_seen_message_date: datetime | None = None


@dataclass(slots=True, frozen=True)
class SourceMessageProjection:
    """Minimal typed view of the canonical source message row."""

    chat_id: int
    message_id: int
    logical_post_key: str
    is_channel_post: bool
    posted_at: datetime
    edited_at: datetime | None
    message_link: str | None
    author_signature: str | None
    forward_info_json: dict[str, Any] | None
    content_type: str | None
    text_body: str | None
    caption_text: str | None
    text_surface: str | None
    entities_json: list[dict[str, Any]] | None
    url_surface_json: list[dict[str, Any]] | None
    raw_message_json: dict[str, Any]
    content_hash: str


@dataclass(slots=True, frozen=True)
class SourceMessageVersionProjection:
    """Typed view of a single source message version row."""

    source_message_id: UUID | None
    version_no: int | None
    version_reason: str
    observed_at: datetime
    telegram_edit_date: datetime | None
    text_surface: str | None
    entities_json: list[dict[str, Any]] | None
    raw_message_json: dict[str, Any]
    content_hash: str


@dataclass(slots=True, frozen=True)
class OutboxEventDraft:
    """Future event outbox payload assembled by the collector boundary."""

    event_type: str
    aggregate_type: str
    aggregate_id: str | UUID
    dedupe_key: str
    payload_json: dict[str, Any]


# Backward-compatible alias used by the current generated code.
OutboxDraft = OutboxEventDraft


@dataclass(slots=True, frozen=True)
class ReconcileSummary:
    """Placeholder summary for future reconcile work."""

    chat_id: int
    result_type: str
    processed_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    gap_filled_count: int = 0
    error_code: str | None = None


@dataclass(slots=True, frozen=True)
class RuntimeSnapshot:
    """Observed runtime state exposed by the bootstrap runtime."""

    lifecycle_state: CollectorLifecycleState
    app_env: CollectorEnvironment
    collector_mode: CollectorMode
    started_at: datetime | None = None
    stop_requested_at: datetime | None = None
    last_tick_at: datetime | None = None
    heartbeat_count: int = 0
    tracked_chat_count: int = 0
    pending_reconcile_count: int = 0
    stop_reason: str | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)
