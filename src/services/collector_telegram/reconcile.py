"""History-based reconcile and backfill logic for collector-telegram."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol

from .config import CollectorTelegramConfig
from .exceptions import ReconcileRetryableError, ReconcileTerminalError
from .models import ReconcileSummary, SourceMessageProjection
from .outbox import CollectorOutboxBuilder

JsonDict = dict[str, Any]


class TDLibHistoryProtocol(Protocol):
    def build_get_chat_history_request(
        self,
        *,
        chat_id: int,
        from_message_id: int = 0,
        offset: int = 0,
        limit: int = 50,
        only_local: bool = False,
    ) -> Any: ...

    async def call(self, request: JsonDict, timeout: float = 30.0) -> JsonDict | None: ...


class ProjectionBuilderProtocol(Protocol):
    def build_source_projection(self, message: JsonDict) -> SourceMessageProjection: ...


class ReconcileRepositoryProtocol(Protocol):
    def transaction(self): ...

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None: ...

    async def upsert_source_message(
        self,
        projection: SourceMessageProjection,
        *,
        platform: str = 'telegram',
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

    async def insert_outbox_event(self, event) -> None: ...

    async def list_reconcile_targets(self, limit: int) -> list[Any]: ...

    async def update_channel_sync_cursor(
        self,
        *,
        registry_id: str,
        last_seen_message_id: int | None = None,
        last_seen_message_date: datetime | None = None,
        last_history_sync_at: datetime | None = None,
    ) -> None: ...


class ReconcileService:
    """History-based recovery and gap-fill logic for collector-telegram.

    Design constraints preserved from the source docs:
    - warm backfill uses only_local=true,
    - authoritative reconcile uses only_local=false,
    - repeated history reads are expected and must be idempotent,
    - reconcile emits collector outbox events only when message state actually advances.
    """

    def __init__(
        self,
        config: CollectorTelegramConfig,
        *,
        tdlib: TDLibHistoryProtocol,
        repository: ReconcileRepositoryProtocol,
        projection_builder: ProjectionBuilderProtocol,
        outbox_builder: CollectorOutboxBuilder,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._tdlib = tdlib
        self._repository = repository
        self._projection_builder = projection_builder
        self._outbox_builder = outbox_builder
        self._logger = logger or logging.getLogger(__name__)

    async def run_startup_warm_backfill(self, chat_id: int) -> ReconcileSummary:
        return await self._run_history_scan(
            chat_id=chat_id,
            only_local=True,
            limit=self._config.warm_backfill_limit,
            reason='startup_warm_backfill',
        )

    async def run_authoritative_reconcile(self, chat_id: int, reason: str = 'scheduled') -> ReconcileSummary:
        return await self._run_history_scan(
            chat_id=chat_id,
            only_local=False,
            limit=self._config.reconcile_backfill_limit,
            reason=reason,
        )

    async def run_gap_fill(self, chat_id: int, reason: str) -> ReconcileSummary:
        return await self._run_history_scan(
            chat_id=chat_id,
            only_local=False,
            limit=self._config.reconcile_backfill_limit,
            reason=reason,
        )

    async def run_scheduled_targets(self, *, limit: int = 20) -> list[ReconcileSummary]:
        targets = await self._repository.list_reconcile_targets(limit)
        results: list[ReconcileSummary] = []
        for target in targets:
            chat_id = getattr(target, 'chat_id', None)
            if chat_id is None:
                continue
            results.append(
                await self.run_authoritative_reconcile(
                    chat_id=int(chat_id),
                    reason='scheduled_reconcile',
                )
            )
        return results

    async def _run_history_scan(
        self,
        *,
        chat_id: int,
        only_local: bool,
        limit: int,
        reason: str,
    ) -> ReconcileSummary:
        observed_at = datetime.now(timezone.utc)
        messages = await self._fetch_chat_history(
            chat_id=chat_id,
            only_local=only_local,
            limit=limit,
        )
        if not messages:
            return ReconcileSummary(
                chat_id=chat_id,
                result_type='no_changes',
                processed_count=0,
                inserted_count=0,
                updated_count=0,
                gap_filled_count=0,
            )

        processed_count = 0
        inserted_count = 0
        updated_count = 0
        gap_filled_count = 0

        for message in reversed(messages):
            processed_count += 1
            applied = await self._apply_history_message(
                message=message,
                reason=reason,
                observed_at=observed_at,
            )
            if applied['inserted']:
                inserted_count += 1
                gap_filled_count += 1
            if applied['updated']:
                updated_count += 1
                gap_filled_count += 1

        result_type = 'gap_filled' if gap_filled_count > 0 else 'cursor_advanced'
        return ReconcileSummary(
            chat_id=chat_id,
            result_type=result_type,
            processed_count=processed_count,
            inserted_count=inserted_count,
            updated_count=updated_count,
            gap_filled_count=gap_filled_count,
            error_code=None,
        )

    async def _fetch_chat_history(
        self,
        *,
        chat_id: int,
        only_local: bool,
        limit: int,
    ) -> Sequence[JsonDict]:
        request = self._tdlib.build_get_chat_history_request(
            chat_id=chat_id,
            from_message_id=0,
            offset=0,
            limit=limit,
            only_local=only_local,
        )
        payload = self._unwrap_request(request)
        response = await self._tdlib.call(payload, timeout=30.0)
        if response is None:
            raise ReconcileRetryableError('TDLib returned no response for getChatHistory')

        if response.get('@type') == 'error':
            code = response.get('code')
            message = response.get('message')
            error_text = f'getChatHistory failed: code={code}, message={message}'
            if self._is_access_error(response):
                raise ReconcileTerminalError(error_text)
            raise ReconcileRetryableError(error_text)

        messages = response.get('messages')
        if not isinstance(messages, list):
            raise ReconcileRetryableError('TDLib getChatHistory response missing messages list')
        return [m for m in messages if isinstance(m, dict)]

    async def _apply_history_message(
        self,
        *,
        message: JsonDict,
        reason: str,
        observed_at: datetime,
    ) -> dict[str, bool]:
        projection = self._projection_builder.build_source_projection(message)
        existing = await self._repository.get_source_message(
            platform='telegram',
            chat_id=projection.chat_id,
            message_id=projection.message_id,
        )

        async with self._repository.transaction():
            current_row = await self._repository.upsert_source_message(projection, platform='telegram')
            source_message_id = str(current_row['source_message_id'])

            changed, version_row = await self._repository.append_source_message_version_if_changed(
                source_message_id=source_message_id,
                projection=projection,
                version_reason='reconcile',
                observed_at=observed_at,
                telegram_edit_date=projection.edited_at,
            )

            if changed and version_row is not None:
                outbox = self._outbox_builder.build_reconciled(
                    source_message_id=source_message_id,
                    current_version_no=int(version_row['version_no']),
                    logical_post_key=projection.logical_post_key,
                    occurred_at=observed_at,
                    reconcile_reason=reason,
                )
                await self._repository.insert_outbox_event(outbox)

        inserted = existing is None and changed
        updated = existing is not None and changed
        return {'inserted': inserted, 'updated': updated}

    @staticmethod
    def _unwrap_request(request: Any) -> JsonDict:
        payload = getattr(request, 'payload', request)
        if not isinstance(payload, dict):
            raise ReconcileTerminalError('TDLib request payload must be a dict')
        return payload

    @staticmethod
    def _is_access_error(response: JsonDict) -> bool:
        message = str(response.get('message', '')).upper()
        return any(
            token in message
            for token in (
                'CHAT_NOT_FOUND',
                'FORBIDDEN',
                'CHANNEL_PRIVATE',
                'USER_BANNED_IN_CHANNEL',
            )
        )
