"""Collector persistence adapter for 0001_ingest_core tables."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping

import sqlalchemy as sa
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from .exceptions import RepositoryInvariantError
from .idempotency import IdempotencyPolicy
from .models import OutboxEventDraft, SourceMessageProjection, TrackedChat

JsonDict = dict[str, Any]


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _jsonb_dumps(value: Any) -> str | None:
    if value is None:
        return None
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, ensure_ascii=False, default=_json_default)


class CollectorRepository:
    """Collector persistence adapter.

    This repository intentionally targets only `0001_ingest_core` tables:
    - telegram_channel_registry
    - telegram_raw_updates
    - source_messages
    - source_message_versions
    - event_outbox

    Atomicity rule:
    current row update + optional version append + outbox insert must run inside
    a single database transaction managed by the caller.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        logger: logging.Logger | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
    ) -> None:
        self._session = session
        self._logger = logger or logging.getLogger(__name__)
        self._idempotency_policy = idempotency_policy or IdempotencyPolicy()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        if self._session.in_transaction():
            yield self._session
            return
        async with self._session.begin():
            yield self._session

    async def insert_raw_update(
        self,
        *,
        update_type: str,
        payload_json: JsonDict,
        chat_id: int | None = None,
        message_id: int | None = None,
    ) -> int:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO telegram_raw_updates (
                    update_type,
                    chat_id,
                    message_id,
                    payload_json,
                    apply_status
                )
                VALUES (
                    :update_type,
                    :chat_id,
                    :message_id,
                    CAST(:payload_json AS jsonb),
                    'pending'
                )
                RETURNING update_seq
                """
            ),
            {
                "update_type": update_type,
                "chat_id": chat_id,
                "message_id": message_id,
                "payload_json": _jsonb_dumps(payload_json),
            },
        )
        return int(result.scalar_one())

    async def mark_raw_update_applied(self, update_seq: int) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE telegram_raw_updates
                SET
                    apply_status = 'applied',
                    applied_at = now(),
                    error_text = NULL
                WHERE update_seq = :update_seq
                """
            ),
            {"update_seq": update_seq},
        )

    async def mark_raw_update_failed(self, update_seq: int, error_text: str) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE telegram_raw_updates
                SET
                    apply_status = 'failed',
                    error_text = :error_text
                WHERE update_seq = :update_seq
                """
            ),
            {"update_seq": update_seq, "error_text": error_text},
        )

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM source_messages
                WHERE platform = :platform
                  AND chat_id = :chat_id
                  AND message_id = :message_id
                """
            ),
            {"platform": platform, "chat_id": chat_id, "message_id": message_id},
        )
        return result.mappings().first()

    async def get_latest_version(self, source_message_id: str) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT *
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                ORDER BY version_no DESC
                LIMIT 1
                """
            ),
            {"source_message_id": source_message_id},
        )
        return result.mappings().first()

    async def upsert_source_message(
        self,
        projection: SourceMessageProjection,
        *,
        platform: str = 'telegram',
    ) -> Mapping[str, Any]:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO source_messages (
                    platform,
                    chat_id,
                    message_id,
                    logical_post_key,
                    is_channel_post,
                    posted_at,
                    edited_at,
                    deleted_at,
                    delete_kind,
                    message_link,
                    author_signature,
                    forward_info_json,
                    content_type,
                    text_body,
                    caption_text,
                    text_surface,
                    entities_json,
                    url_surface_json,
                    raw_message_json,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    :platform,
                    :chat_id,
                    :message_id,
                    :logical_post_key,
                    :is_channel_post,
                    :posted_at,
                    :edited_at,
                    NULL,
                    'none',
                    :message_link,
                    :author_signature,
                    CAST(:forward_info_json AS jsonb),
                    :content_type,
                    :text_body,
                    :caption_text,
                    :text_surface,
                    CAST(:entities_json AS jsonb),
                    CAST(:url_surface_json AS jsonb),
                    CAST(:raw_message_json AS jsonb),
                    now(),
                    now()
                )
                ON CONFLICT (platform, chat_id, message_id)
                DO UPDATE SET
                    logical_post_key = EXCLUDED.logical_post_key,
                    is_channel_post = EXCLUDED.is_channel_post,
                    posted_at = LEAST(source_messages.posted_at, EXCLUDED.posted_at),
                    edited_at = CASE
                        WHEN EXCLUDED.edited_at IS NULL THEN source_messages.edited_at
                        WHEN source_messages.edited_at IS NULL THEN EXCLUDED.edited_at
                        ELSE GREATEST(source_messages.edited_at, EXCLUDED.edited_at)
                    END,
                    deleted_at = NULL,
                    delete_kind = 'none',
                    message_link = EXCLUDED.message_link,
                    author_signature = EXCLUDED.author_signature,
                    forward_info_json = EXCLUDED.forward_info_json,
                    content_type = EXCLUDED.content_type,
                    text_body = EXCLUDED.text_body,
                    caption_text = EXCLUDED.caption_text,
                    text_surface = EXCLUDED.text_surface,
                    entities_json = EXCLUDED.entities_json,
                    url_surface_json = EXCLUDED.url_surface_json,
                    raw_message_json = EXCLUDED.raw_message_json,
                    last_seen_at = now()
                RETURNING *
                """
            ),
            {
                "platform": platform,
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "logical_post_key": projection.logical_post_key,
                "is_channel_post": projection.is_channel_post,
                "posted_at": projection.posted_at,
                "edited_at": projection.edited_at,
                "message_link": projection.message_link,
                "author_signature": projection.author_signature,
                "forward_info_json": _jsonb_dumps(projection.forward_info_json),
                "content_type": projection.content_type,
                "text_body": projection.text_body,
                "caption_text": projection.caption_text,
                "text_surface": projection.text_surface,
                "entities_json": _jsonb_dumps(projection.entities_json),
                "url_surface_json": _jsonb_dumps(projection.url_surface_json),
                "raw_message_json": _jsonb_dumps(projection.raw_message_json),
            },
        )
        return result.mappings().one()

    async def append_source_message_version(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> Mapping[str, Any]:
        latest = await self.get_latest_version(source_message_id)
        next_version_no = 1 if latest is None else int(latest['version_no']) + 1

        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO source_message_versions (
                    source_message_id,
                    version_no,
                    version_reason,
                    observed_at,
                    telegram_edit_date,
                    text_surface,
                    entities_json,
                    raw_message_json,
                    content_hash
                )
                VALUES (
                    CAST(:source_message_id AS uuid),
                    :version_no,
                    :version_reason,
                    CAST(:observed_at AS timestamptz),
                    CAST(:telegram_edit_date AS timestamptz),
                    :text_surface,
                    CAST(:entities_json AS jsonb),
                    CAST(:raw_message_json AS jsonb),
                    :content_hash
                )
                RETURNING *
                """
            ),
            {
                'source_message_id': source_message_id,
                'version_no': next_version_no,
                'version_reason': version_reason,
                'observed_at': observed_at or datetime.now(timezone.utc),
                'telegram_edit_date': telegram_edit_date,
                'text_surface': projection.text_surface,
                'entities_json': _jsonb_dumps(projection.entities_json),
                'raw_message_json': _jsonb_dumps(projection.raw_message_json),
                'content_hash': projection.content_hash,
            },
        )
        version_row = result.mappings().one()

        updated_current = await self._session.execute(
            sa.text(
                """
                UPDATE source_messages
                SET
                    current_version_no = :current_version_no,
                    edited_at = CASE
                        WHEN CAST(:edited_at AS timestamptz) IS NULL THEN edited_at
                        WHEN edited_at IS NULL THEN CAST(:edited_at AS timestamptz)
                        ELSE GREATEST(edited_at, CAST(:edited_at AS timestamptz))
                    END,
                    deleted_at = NULL,
                    delete_kind = 'none',
                    message_link = :message_link,
                    author_signature = :author_signature,
                    forward_info_json = CAST(:forward_info_json AS jsonb),
                    content_type = :content_type,
                    text_body = :text_body,
                    caption_text = :caption_text,
                    text_surface = :text_surface,
                    entities_json = CAST(:entities_json AS jsonb),
                    url_surface_json = CAST(:url_surface_json AS jsonb),
                    raw_message_json = CAST(:raw_message_json AS jsonb),
                    last_seen_at = now()
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                RETURNING source_message_id
                """
            ),
            {
                'source_message_id': source_message_id,
                'current_version_no': next_version_no,
                'edited_at': projection.edited_at,
                'message_link': projection.message_link,
                'author_signature': projection.author_signature,
                'forward_info_json': _jsonb_dumps(projection.forward_info_json),
                'content_type': projection.content_type,
                'text_body': projection.text_body,
                'caption_text': projection.caption_text,
                'text_surface': projection.text_surface,
                'entities_json': _jsonb_dumps(projection.entities_json),
                'url_surface_json': _jsonb_dumps(projection.url_surface_json),
                'raw_message_json': _jsonb_dumps(projection.raw_message_json),
            },
        )
        if updated_current.mappings().first() is None:
            raise RepositoryInvariantError(
                f"source_messages row missing while appending version: {source_message_id}"
            )
        return version_row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]:
        latest = await self.get_latest_version(source_message_id)
        previous_hash = None if latest is None else str(latest['content_hash'])
        if not self._idempotency_policy.should_append_new_version(previous_hash, projection.content_hash):
            return False, None
        version_row = await self.append_source_message_version(
            source_message_id=source_message_id,
            projection=projection,
            version_reason=version_reason,
            observed_at=observed_at,
            telegram_edit_date=telegram_edit_date,
        )
        return True, version_row

    async def mark_message_deleted(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
        delete_kind: str,
        deleted_at: datetime | None = None,
    ) -> Mapping[str, Any] | None:
        result = await self._session.execute(
            sa.text(
                """
                UPDATE source_messages
                SET
                    deleted_at = :deleted_at,
                    delete_kind = :delete_kind,
                    last_seen_at = now()
                WHERE platform = :platform
                  AND chat_id = :chat_id
                  AND message_id = :message_id
                RETURNING *
                """
            ),
            {
                'platform': platform,
                'chat_id': chat_id,
                'message_id': message_id,
                'delete_kind': delete_kind,
                'deleted_at': deleted_at or datetime.now(timezone.utc),
            },
        )
        return result.mappings().first()

    async def insert_outbox_event(self, event: OutboxEventDraft) -> bool:
        result = await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status
                )
                VALUES (
                    :event_type,
                    :aggregate_type,
                    CAST(:aggregate_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                RETURNING event_id
                """
            ),
            {
                'event_type': event.event_type,
                'aggregate_type': event.aggregate_type,
                'aggregate_id': str(event.aggregate_id),
                'dedupe_key': event.dedupe_key,
                'payload_json': _jsonb_dumps(event.payload_json),
            },
        )
        return result.scalar_one_or_none() is not None

    async def get_active_joined_tracked_chat_by_registry_id(
        self,
        registry_id: str,
    ) -> TrackedChat | None:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    registry_id,
                    chat_id,
                    desired_state,
                    access_state,
                    source_kind,
                    source_value,
                    priority_weight,
                    last_seen_message_id,
                    last_seen_message_date
                FROM telegram_channel_registry
                WHERE registry_id = CAST(:registry_id AS uuid)
                  AND desired_state = 'active'
                  AND access_state = 'joined'
                  AND chat_id IS NOT NULL
                LIMIT 2
                """
            ),
            {"registry_id": registry_id},
        )
        rows = result.mappings().all()
        if len(rows) != 1:
            return None
        return self._tracked_chat_from_row(rows[0])

    async def list_active_tracked_chats(self) -> list[TrackedChat]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    registry_id,
                    chat_id,
                    desired_state,
                    access_state,
                    source_kind,
                    source_value,
                    priority_weight,
                    last_seen_message_id,
                    last_seen_message_date
                FROM telegram_channel_registry
                WHERE desired_state = 'active'
                  AND access_state = 'joined'
                  AND chat_id IS NOT NULL
                ORDER BY priority_weight DESC, registry_id ASC
                """
            )
        )
        return [self._tracked_chat_from_row(row) for row in result.mappings().all()]

    async def list_reconcile_targets(self, limit: int) -> list[TrackedChat]:
        result = await self._session.execute(
            sa.text(
                """
                SELECT
                    registry_id,
                    chat_id,
                    desired_state,
                    access_state,
                    source_kind,
                    source_value,
                    priority_weight,
                    last_seen_message_id,
                    last_seen_message_date
                FROM telegram_channel_registry
                WHERE desired_state = 'active'
                  AND access_state = 'joined'
                  AND chat_id IS NOT NULL
                ORDER BY
                    last_history_sync_at NULLS FIRST,
                    last_history_sync_at ASC,
                    priority_weight DESC,
                    registry_id ASC
                LIMIT :limit
                """
            ),
            {'limit': limit},
        )
        return [self._tracked_chat_from_row(row) for row in result.mappings().all()]

    async def update_channel_sync_cursor(
        self,
        *,
        registry_id: str,
        last_seen_message_id: int | None = None,
        last_seen_message_date: datetime | None = None,
        last_history_sync_at: datetime | None = None,
    ) -> None:
        await self._session.execute(
            sa.text(
                """
                UPDATE telegram_channel_registry
                SET
                    last_seen_message_id = COALESCE(:last_seen_message_id, last_seen_message_id),
                    last_seen_message_date = COALESCE(:last_seen_message_date, last_seen_message_date),
                    last_history_sync_at = COALESCE(:last_history_sync_at, last_history_sync_at),
                    updated_at = now()
                WHERE registry_id = CAST(:registry_id AS uuid)
                """
            ),
            {
                'registry_id': registry_id,
                'last_seen_message_id': last_seen_message_id,
                'last_seen_message_date': last_seen_message_date,
                'last_history_sync_at': last_history_sync_at,
            },
        )

    @staticmethod
    def _tracked_chat_from_row(row: RowMapping) -> TrackedChat:
        return TrackedChat(
            registry_id=str(row['registry_id']),
            chat_id=row['chat_id'],
            desired_state=row['desired_state'],
            access_state=row['access_state'],
            source_kind=row['source_kind'],
            source_value=row['source_value'],
            priority_weight=int(row['priority_weight']),
            last_seen_message_id=row['last_seen_message_id'],
            last_seen_message_date=row['last_seen_message_date'],
        )
