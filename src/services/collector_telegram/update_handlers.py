"""Update handler implementations for collector live update ingestion."""

from __future__ import annotations

import copy
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

from .exceptions import RepositoryInvariantError, UpdateApplyRetryableError, UpdateApplyTerminalError
from .message_projection import MessageProjectionBuilder
from .models import SourceMessageProjection
from .outbox import CollectorOutboxBuilder

JsonDict = dict[str, Any]


class CollectorRepositoryProtocol(Protocol):
    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int) -> Mapping[str, Any] | None: ...
    async def upsert_source_message(self, projection: SourceMessageProjection, *, platform: str = 'telegram') -> Mapping[str, Any]: ...
    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]: ...
    async def mark_message_deleted(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
        delete_kind: str,
        deleted_at: datetime | None = None,
    ) -> Mapping[str, Any] | None: ...
    async def insert_outbox_event(self, event) -> None: ...


@dataclass(slots=True, frozen=True)
class UpdateHandlingResult:
    handled: bool
    source_message_ids: list[str] = field(default_factory=list)
    version_appended: bool = False
    outbox_events_created: int = 0
    reconcile_requested: bool = False
    reconcile_reason: str | None = None
    note: str | None = None


class CollectorUpdateHandlers:
    """Apply collector-side update handling.

    Transaction rule:
    - dispatcher manages raw update journaling and outer success/failure marking,
    - handlers mutate only current/version/outbox state,
    - handlers do not open their own transactions.
    """

    def __init__(
        self,
        repository: CollectorRepositoryProtocol,
        projection_builder: MessageProjectionBuilder,
        outbox_builder: CollectorOutboxBuilder,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._repository = repository
        self._projection_builder = projection_builder
        self._outbox_builder = outbox_builder
        self._logger = logger or logging.getLogger(__name__)

    async def handle_update_new_message(self, update: JsonDict) -> UpdateHandlingResult:
        message = self._require_mapping(update.get('message'), 'updateNewMessage.message')
        projection = self._projection_builder.build_source_projection(message)

        current_row = await self._repository.upsert_source_message(projection)
        source_message_id = self._require_uuid(current_row.get('source_message_id'), 'source_message_id')

        changed, version_row = await self._repository.append_source_message_version_if_changed(
            source_message_id=source_message_id,
            projection=projection,
            version_reason='new',
            observed_at=datetime.now(timezone.utc),
            telegram_edit_date=projection.edited_at,
        )

        outbox_events_created = 0
        if changed:
            version_no = self._require_int(version_row, 'version_no')
            event = self._outbox_builder.build_created(
                source_message_id=source_message_id,
                current_version_no=version_no,
                logical_post_key=projection.logical_post_key,
                occurred_at=projection.posted_at,
            )
            await self._repository.insert_outbox_event(event)
            outbox_events_created = 1

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=[source_message_id],
            version_appended=changed,
            outbox_events_created=outbox_events_created,
        )

    async def handle_update_message_edited(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get('chat_id'), 'updateMessageEdited.chat_id')
        message_id = self._require_int_from_value(update.get('message_id'), 'updateMessageEdited.message_id')
        current_row = await self._load_current_source_message(chat_id=chat_id, message_id=message_id)

        existing_raw = self._load_raw_message_json(current_row)
        synthetic_message = copy.deepcopy(existing_raw)
        if isinstance(update.get('edit_date'), (int, float)):
            synthetic_message['edit_date'] = update['edit_date']

        projection = self._projection_builder.build_source_projection(synthetic_message)
        await self._repository.upsert_source_message(projection)

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=[self._require_uuid(current_row.get('source_message_id'), 'source_message_id')],
            version_appended=False,
            outbox_events_created=0,
            note='metadata-only edit observed; waiting for updateMessageContent or reconcile for version append',
        )

    async def handle_update_message_content(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get('chat_id'), 'updateMessageContent.chat_id')
        message_id = self._require_int_from_value(update.get('message_id'), 'updateMessageContent.message_id')
        current_row = await self._load_current_source_message(chat_id=chat_id, message_id=message_id)

        existing_raw = self._load_raw_message_json(current_row)
        synthetic_message = copy.deepcopy(existing_raw)

        new_content = self._get_mapping_or_none(update.get('new_content')) or self._get_mapping_or_none(update.get('content'))
        if new_content is None:
            raise UpdateApplyRetryableError('updateMessageContent arrived without content payload')
        synthetic_message['content'] = copy.deepcopy(new_content)

        if isinstance(update.get('edit_date'), (int, float)):
            synthetic_message['edit_date'] = update['edit_date']

        projection = self._projection_builder.build_source_projection(synthetic_message)
        current_row = await self._repository.upsert_source_message(projection)
        source_message_id = self._require_uuid(current_row.get('source_message_id'), 'source_message_id')

        changed, version_row = await self._repository.append_source_message_version_if_changed(
            source_message_id=source_message_id,
            projection=projection,
            version_reason='content_change',
            observed_at=datetime.now(timezone.utc),
            telegram_edit_date=projection.edited_at,
        )

        outbox_events_created = 0
        if changed:
            version_no = self._require_int(version_row, 'version_no')
            event = self._outbox_builder.build_edited(
                source_message_id=source_message_id,
                current_version_no=version_no,
                logical_post_key=projection.logical_post_key,
                occurred_at=projection.edited_at or datetime.now(timezone.utc),
            )
            await self._repository.insert_outbox_event(event)
            outbox_events_created = 1

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=[source_message_id],
            version_appended=changed,
            outbox_events_created=outbox_events_created,
        )

    async def handle_update_delete_messages(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get('chat_id'), 'updateDeleteMessages.chat_id')
        message_ids = update.get('message_ids') or []
        if not isinstance(message_ids, list):
            raise UpdateApplyTerminalError('updateDeleteMessages.message_ids must be a list')

        is_permanent = bool(update.get('is_permanent', False))
        from_cache = bool(update.get('from_cache', False))
        delete_kind = self._map_delete_kind(is_permanent=is_permanent, from_cache=from_cache)
        deleted_at = datetime.now(timezone.utc)

        source_message_ids: list[str] = []
        outbox_events_created = 0
        for raw_message_id in message_ids:
            if not isinstance(raw_message_id, int):
                continue
            current_row = await self._repository.mark_message_deleted(
                platform='telegram',
                chat_id=chat_id,
                message_id=raw_message_id,
                delete_kind=delete_kind,
                deleted_at=deleted_at,
            )
            if current_row is None:
                self._logger.warning(
                    'delete_update_for_unknown_message',
                    extra={
                        'service': 'collector-telegram',
                        'event': 'delete_update_for_unknown_message',
                        'chat_id': chat_id,
                        'message_id': raw_message_id,
                    },
                )
                continue

            source_message_id = self._require_uuid(current_row.get('source_message_id'), 'source_message_id')
            logical_post_key = self._coerce_non_empty_str(current_row.get('logical_post_key'), 'logical_post_key')
            current_version_no = self._require_int_from_value(current_row.get('current_version_no'), 'current_version_no')

            event = self._outbox_builder.build_deleted(
                source_message_id=source_message_id,
                current_version_no=current_version_no,
                logical_post_key=logical_post_key,
                occurred_at=deleted_at,
                delete_kind=delete_kind,
            )
            await self._repository.insert_outbox_event(event)

            source_message_ids.append(source_message_id)
            outbox_events_created += 1

        return UpdateHandlingResult(
            handled=True,
            source_message_ids=source_message_ids,
            version_appended=False,
            outbox_events_created=outbox_events_created,
        )

    async def handle_update_chat_last_message(self, update: JsonDict) -> UpdateHandlingResult:
        chat_id = self._require_int_from_value(update.get('chat_id'), 'updateChatLastMessage.chat_id')
        last_message = self._get_mapping_or_none(update.get('last_message'))
        if last_message is None:
            return UpdateHandlingResult(
                handled=True,
                reconcile_requested=True,
                reconcile_reason='last_message_missing',
                note=f'chat {chat_id} reported null last_message; reconcile should be prioritized',
            )
        current_last_message_id = last_message.get('id')
        return UpdateHandlingResult(
            handled=True,
            reconcile_requested=False,
            note=f'chat {chat_id} last message observed: {current_last_message_id}',
        )

    async def _load_current_source_message(self, *, chat_id: int, message_id: int) -> Mapping[str, Any]:
        current_row = await self._repository.get_source_message(platform='telegram', chat_id=chat_id, message_id=message_id)
        if current_row is None:
            raise UpdateApplyRetryableError(
                f'current source message missing for chat_id={chat_id}, message_id={message_id}; reconcile may recover'
            )
        return current_row

    def _load_raw_message_json(self, current_row: Mapping[str, Any]) -> JsonDict:
        raw_value = current_row.get('raw_message_json')
        if isinstance(raw_value, dict):
            return raw_value
        if isinstance(raw_value, str):
            try:
                decoded = json.loads(raw_value)
            except json.JSONDecodeError as exc:
                raise RepositoryInvariantError('raw_message_json is not valid JSON') from exc
            if isinstance(decoded, dict):
                return decoded
        raise RepositoryInvariantError('raw_message_json is missing or not a JSON object')

    def _map_delete_kind(self, *, is_permanent: bool, from_cache: bool) -> str:
        if is_permanent:
            return 'permanent'
        if from_cache:
            return 'cache_only'
        return 'cache_only'

    def _require_mapping(self, value: Any, label: str) -> JsonDict:
        if not isinstance(value, dict):
            raise UpdateApplyTerminalError(f'{label} must be an object')
        return value

    def _get_mapping_or_none(self, value: Any) -> JsonDict | None:
        return value if isinstance(value, dict) else None

    def _require_uuid(self, value: Any, label: str) -> str:
        if isinstance(value, str) and value:
            return value
        raise RepositoryInvariantError(f'{label} is missing or invalid')

    def _require_int(self, mapping: Mapping[str, Any] | None, key: str) -> int:
        if mapping is None:
            raise RepositoryInvariantError(f'mapping missing while reading {key}')
        return self._require_int_from_value(mapping.get(key), key)

    def _require_int_from_value(self, value: Any, label: str) -> int:
        if isinstance(value, bool):
            raise UpdateApplyTerminalError(f'{label} must be an integer')
        if not isinstance(value, int):
            raise UpdateApplyTerminalError(f'{label} must be an integer')
        return value

    def _coerce_non_empty_str(self, value: Any, label: str) -> str:
        if isinstance(value, str) and value.strip():
            return value.strip()
        raise RepositoryInvariantError(f'{label} is missing or empty')
