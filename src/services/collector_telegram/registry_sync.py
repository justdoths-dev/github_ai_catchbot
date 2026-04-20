"""Tracked channel onboarding and access refresh for collector-telegram."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .config import CollectorTelegramConfig
from .exceptions import UpdateApplyRetryableError, UpdateApplyTerminalError
from .models import TrackedChat

JsonDict = dict[str, Any]


class TDLibRegistryProtocol(Protocol):
    def build_search_public_chat_request(self, username: str) -> Any: ...

    def build_join_chat_request(self, chat_id: int) -> Any: ...

    def build_join_chat_by_invite_link_request(self, invite_link: str) -> Any: ...

    def build_get_chat_history_request(
        self,
        *,
        chat_id: int,
        from_message_id: int = 0,
        offset: int = 0,
        limit: int = 1,
        only_local: bool = False,
    ) -> Any: ...

    async def call(self, request: JsonDict, timeout: float = 30.0) -> JsonDict | None: ...


class RegistrySyncRepositoryProtocol(Protocol):
    async def list_active_tracked_chats(self) -> list[TrackedChat]: ...

    async def list_registry_rows_by_access_states(
        self,
        access_states: Sequence[str],
        *,
        desired_state: str = 'active',
    ) -> list[Mapping[str, Any]]: ...

    async def mark_channel_resolved(
        self,
        *,
        registry_id: str,
        chat_id: int,
        username_snapshot: str | None,
        title_snapshot: str | None,
        chat_type: str | None,
        access_state: str,
        last_resolved_at: datetime,
    ) -> None: ...

    async def mark_channel_access_state(
        self,
        *,
        registry_id: str,
        access_state: str,
        last_join_attempt_at: datetime | None = None,
        last_resolved_at: datetime | None = None,
        chat_id: int | None = None,
        username_snapshot: str | None = None,
        title_snapshot: str | None = None,
        chat_type: str | None = None,
        notes_append: str | None = None,
    ) -> None: ...


@dataclass(slots=True, frozen=True)
class RegistrySyncSummary:
    processed_count: int = 0
    joined_count: int = 0
    join_requested_count: int = 0
    access_lost_count: int = 0
    forbidden_count: int = 0
    not_found_count: int = 0
    transient_failed_count: int = 0
    no_change_count: int = 0


class ChannelRegistrySyncService:
    """Tracked chat onboarding and access refresh.

    This service keeps collector ownership narrow:
    - resolve source_value -> chat_id anchor,
    - attempt join where allowed,
    - refresh access states conservatively,
    - load active tracked chats for downstream collector loops.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        tdlib: TDLibRegistryProtocol,
        repository: RegistrySyncRepositoryProtocol,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._tdlib = tdlib
        self._repository = repository
        self._logger = logger or logging.getLogger(__name__)

    async def load_active_channels(self) -> list[TrackedChat]:
        return await self._repository.list_active_tracked_chats()

    async def sync_unresolved_channels(self) -> RegistrySyncSummary:
        rows = await self._repository.list_registry_rows_by_access_states(
            ['unresolved', 'resolved_not_joined'],
            desired_state='active',
        )
        return await self._sync_rows(rows, mode='onboarding')

    async def sync_join_requested_channels(self) -> RegistrySyncSummary:
        rows = await self._repository.list_registry_rows_by_access_states(
            ['join_requested'],
            desired_state='active',
        )
        return await self._sync_rows(rows, mode='join_requested')

    async def sync_access_lost_channels(self) -> RegistrySyncSummary:
        rows = await self._repository.list_registry_rows_by_access_states(
            ['access_lost', 'left'],
            desired_state='active',
        )
        return await self._sync_rows(rows, mode='access_recovery')

    async def _sync_rows(self, rows: Sequence[Mapping[str, Any]], *, mode: str) -> RegistrySyncSummary:
        processed = joined = join_requested = access_lost = forbidden = not_found = transient_failed = no_change = 0
        for row in rows:
            processed += 1
            outcome = await self._sync_single_row(row, mode=mode)
            match outcome:
                case 'joined':
                    joined += 1
                case 'join_requested':
                    join_requested += 1
                case 'access_lost':
                    access_lost += 1
                case 'forbidden':
                    forbidden += 1
                case 'not_found':
                    not_found += 1
                case 'transient_failed':
                    transient_failed += 1
                case _:
                    no_change += 1
        return RegistrySyncSummary(
            processed_count=processed,
            joined_count=joined,
            join_requested_count=join_requested,
            access_lost_count=access_lost,
            forbidden_count=forbidden,
            not_found_count=not_found,
            transient_failed_count=transient_failed,
            no_change_count=no_change,
        )

    async def _sync_single_row(self, row: Mapping[str, Any], *, mode: str) -> str:
        registry_id = str(row['registry_id'])
        source_kind = str(row['source_kind'])
        source_value = str(row['source_value'])
        now = datetime.now(timezone.utc)

        try:
            if source_kind == 'public_username':
                return await self._sync_public_username_row(
                    registry_id=registry_id,
                    source_value=source_value,
                    existing_chat_id=self._safe_int(row.get('chat_id')),
                    mode=mode,
                    now=now,
                )

            if source_kind == 'invite_link':
                return await self._sync_invite_link_row(
                    registry_id=registry_id,
                    invite_link=source_value,
                    mode=mode,
                    now=now,
                )

            if source_kind == 'chat_id':
                chat_id = self._safe_int(source_value) or self._safe_int(row.get('chat_id'))
                if chat_id is None:
                    await self._repository.mark_channel_access_state(
                        registry_id=registry_id,
                        access_state='not_found',
                        notes_append='chat_id source_kind row missing numeric chat_id',
                        last_resolved_at=now,
                    )
                    return 'not_found'
                return await self._probe_chat_access(
                    registry_id=registry_id,
                    chat_id=chat_id,
                    now=now,
                )

            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state='not_found',
                notes_append=f'unsupported source_kind={source_kind}',
                last_resolved_at=now,
            )
            return 'not_found'
        except UpdateApplyTerminalError as exc:
            self._logger.warning(
                'collector_registry_sync_terminal',
                extra={
                    'service': 'collector-telegram',
                    'event': 'collector_registry_sync_terminal',
                    'registry_id': registry_id,
                    'source_kind': source_kind,
                    'error': str(exc),
                },
            )
            return 'forbidden'
        except UpdateApplyRetryableError as exc:
            self._logger.warning(
                'collector_registry_sync_retryable',
                extra={
                    'service': 'collector-telegram',
                    'event': 'collector_registry_sync_retryable',
                    'registry_id': registry_id,
                    'source_kind': source_kind,
                    'error': str(exc),
                },
            )
            return 'transient_failed'

    async def _sync_public_username_row(
        self,
        *,
        registry_id: str,
        source_value: str,
        existing_chat_id: int | None,
        mode: str,
        now: datetime,
    ) -> str:
        request = self._tdlib.build_search_public_chat_request(source_value)
        response = await self._tdlib.call(self._unwrap_request(request), timeout=30.0)
        if response is None:
            raise UpdateApplyRetryableError('searchPublicChat returned no response')

        if response.get('@type') == 'error':
            access_state = self._classify_error_state(response)
            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state=access_state,
                last_resolved_at=now,
                notes_append=self._error_note(response),
            )
            return access_state

        chat_id = self._safe_int(response.get('id'))
        if chat_id is None:
            raise UpdateApplyRetryableError('searchPublicChat response missing chat id')

        username_snapshot = self._extract_username_snapshot(response)
        title_snapshot = self._extract_title_snapshot(response)
        chat_type = self._extract_chat_type(response)

        await self._repository.mark_channel_resolved(
            registry_id=registry_id,
            chat_id=chat_id,
            username_snapshot=username_snapshot,
            title_snapshot=title_snapshot,
            chat_type=chat_type,
            access_state='resolved_not_joined',
            last_resolved_at=now,
        )

        if mode == 'access_recovery' and existing_chat_id is not None:
            return await self._probe_chat_access(
                registry_id=registry_id,
                chat_id=existing_chat_id,
                now=now,
            )

        join_request = self._tdlib.build_join_chat_request(chat_id)
        join_response = await self._tdlib.call(self._unwrap_request(join_request), timeout=30.0)
        access_state = self._classify_join_result(join_response)
        await self._repository.mark_channel_access_state(
            registry_id=registry_id,
            access_state=access_state,
            chat_id=chat_id,
            username_snapshot=username_snapshot,
            title_snapshot=title_snapshot,
            chat_type=chat_type,
            last_join_attempt_at=now,
            last_resolved_at=now,
            notes_append=None if access_state == 'joined' else self._safe_response_note(join_response),
        )
        return access_state

    async def _sync_invite_link_row(
        self,
        *,
        registry_id: str,
        invite_link: str,
        mode: str,
        now: datetime,
    ) -> str:
        if mode == 'join_requested':
            return 'no_change'

        request = self._tdlib.build_join_chat_by_invite_link_request(invite_link)
        response = await self._tdlib.call(self._unwrap_request(request), timeout=30.0)
        if response is None:
            raise UpdateApplyRetryableError('joinChatByInviteLink returned no response')

        if response.get('@type') == 'error':
            access_state = self._classify_error_state(response)
            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state=access_state,
                last_join_attempt_at=now,
                last_resolved_at=now,
                notes_append=self._error_note(response),
            )
            return access_state

        chat_id = self._safe_int(response.get('id'))
        username_snapshot = self._extract_username_snapshot(response)
        title_snapshot = self._extract_title_snapshot(response)
        chat_type = self._extract_chat_type(response)

        if chat_id is None:
            raise UpdateApplyRetryableError('joinChatByInviteLink response missing chat id')

        await self._repository.mark_channel_access_state(
            registry_id=registry_id,
            access_state='joined',
            chat_id=chat_id,
            username_snapshot=username_snapshot,
            title_snapshot=title_snapshot,
            chat_type=chat_type,
            last_join_attempt_at=now,
            last_resolved_at=now,
        )
        return 'joined'

    async def _probe_chat_access(self, *, registry_id: str, chat_id: int, now: datetime) -> str:
        request = self._tdlib.build_get_chat_history_request(
            chat_id=chat_id,
            from_message_id=0,
            offset=0,
            limit=1,
            only_local=False,
        )
        response = await self._tdlib.call(self._unwrap_request(request), timeout=30.0)
        if response is None:
            raise UpdateApplyRetryableError('getChatHistory returned no response while probing chat access')

        if response.get('@type') == 'error':
            access_state = self._classify_error_state(response)
            await self._repository.mark_channel_access_state(
                registry_id=registry_id,
                access_state=access_state,
                last_resolved_at=now,
                notes_append=self._error_note(response),
            )
            return access_state

        await self._repository.mark_channel_access_state(
            registry_id=registry_id,
            access_state='joined',
            chat_id=chat_id,
            last_resolved_at=now,
        )
        return 'joined'

    @staticmethod
    def _unwrap_request(request: Any) -> JsonDict:
        payload = getattr(request, 'payload', request)
        if isinstance(payload, dict) and 'payload' in payload and isinstance(payload.get('payload'), dict):
            payload = payload['payload']
        if not isinstance(payload, dict):
            raise UpdateApplyTerminalError('TDLib request payload must be a dict')
        return payload

    @staticmethod
    def _classify_join_result(response: JsonDict | None) -> str:
        if response is None:
            return 'joined'
        if response.get('@type') != 'error':
            return 'joined'
        return ChannelRegistrySyncService._classify_error_state(response)

    @staticmethod
    def _classify_error_state(response: JsonDict) -> str:
        message = str(response.get('message', '')).upper()
        if 'INVITE_REQUEST_SENT' in message:
            return 'join_requested'
        if any(token in message for token in ('CHAT_NOT_FOUND', 'INVITE_LINK_INVALID', 'USERNAME_NOT_OCCUPIED')):
            return 'not_found'
        if any(token in message for token in ('FORBIDDEN', 'CHANNEL_PRIVATE', 'USER_BANNED_IN_CHANNEL')):
            return 'access_lost'
        return 'access_lost'

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_title_snapshot(chat_payload: JsonDict) -> str | None:
        value = chat_payload.get('title')
        return value if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _extract_chat_type(chat_payload: JsonDict) -> str | None:
        raw = chat_payload.get('type')
        if not isinstance(raw, dict):
            return None
        type_name = raw.get('@type')
        if not isinstance(type_name, str):
            return None
        return type_name.removeprefix('chatType') or type_name

    @staticmethod
    def _extract_username_snapshot(chat_payload: JsonDict) -> str | None:
        usernames = chat_payload.get('usernames')
        if isinstance(usernames, dict):
            active = usernames.get('active_usernames')
            if isinstance(active, list) and active:
                candidate = active[0]
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        username = chat_payload.get('username')
        if isinstance(username, str) and username.strip():
            return username.strip()
        return None

    @staticmethod
    def _error_note(response: JsonDict) -> str:
        code = response.get('code')
        message = response.get('message')
        return f'tdlib_error code={code} message={message}'

    @staticmethod
    def _safe_response_note(response: JsonDict | None) -> str | None:
        if not isinstance(response, dict):
            return None
        if response.get('@type') != 'error':
            return None
        return ChannelRegistrySyncService._error_note(response)
