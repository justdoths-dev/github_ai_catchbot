from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal


class CollectorEnvironment(StrEnum):
    PROD = "prod"
    DEV = "dev"
    TEST = "test"


class CollectorMode(StrEnum):
    LIVE = "live"
    REPLAY = "replay"


class CollectorLifecycleState(StrEnum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    READY = "ready"
    DEGRADED = "degraded"
    FAILING = "failing"
    STOPPING = "stopping"
    STOPPED = "stopped"


AppEnv = Literal["prod", "dev", "test"]

DesiredState = Literal["active", "paused", "removed"]
AccessState = Literal[
    "unresolved",
    "resolved_not_joined",
    "join_attempted",
    "join_requested",
    "joined",
    "forbidden",
    "not_found",
    "left",
    "access_lost",
]

CollectorHealthState = Literal["starting", "ready", "degraded", "failing", "stopped"]


@dataclass(slots=True, frozen=True)
class TrackedChat:
    registry_id: str
    chat_id: int | None
    desired_state: DesiredState
    access_state: AccessState
    source_kind: str
    source_value: str
    priority_weight: int = 100
    last_seen_message_id: int | None = None
    last_seen_message_date: datetime | None = None


@dataclass(slots=True, frozen=True)
class SourceMessageProjection:
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
    source_message_id: str | None
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
    event_type: str
    aggregate_type: str
    aggregate_id: str
    dedupe_key: str
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class ReconcileSummary:
    chat_id: int
    result_type: str
    processed_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    gap_filled_count: int = 0
    error_code: str | None = None


@dataclass(slots=True)
class RuntimeSnapshot:
    health_state: str = "starting"
    started_at: datetime | None = None
    last_tick_at: datetime | None = None
    last_update_received_at: datetime | None = None
    tracked_channels_active: int = 0
    reconcile_runs_total: int = 0
    reconcile_gap_fills_total: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class CollectorServiceSnapshot:
    lifecycle_state: CollectorLifecycleState
    app_env: CollectorEnvironment
    collector_mode: CollectorMode
    stop_reason: str | None
    heartbeat_count: int
    started_at: datetime | None = None