"""Raw update journaling and dispatch for collector update handlers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from .exceptions import UpdateApplyRetryableError, UpdateApplyTerminalError
from .update_handlers import CollectorUpdateHandlers, UpdateHandlingResult

JsonDict = dict[str, Any]
HandlerCallable = Callable[[JsonDict], Awaitable[UpdateHandlingResult]]


class CollectorRepositoryProtocol(Protocol):
    async def transaction(self): ...
    async def insert_raw_update(
        self,
        *,
        update_type: str,
        payload_json: JsonDict,
        chat_id: int | None = None,
        message_id: int | None = None,
    ) -> int: ...
    async def mark_raw_update_applied(self, update_seq: int) -> None: ...
    async def mark_raw_update_failed(self, update_seq: int, error_text: str) -> None: ...


@dataclass(slots=True, frozen=True)
class DispatchContext:
    raw_update_seq: int
    update_type: str
    chat_id: int | None
    message_id: int | None


class UpdateDispatcher:
    """Route raw TDLib updates into collector handlers.

    Journaling rule used here:
    1. raw update row is persisted first,
    2. business mutation runs after that,
    3. raw row is marked applied/failed in follow-up transaction.

    This intentionally preserves failed raw updates for replay/debug,
    even if business-state mutation rolls back.
    """

    def __init__(
        self,
        repository: CollectorRepositoryProtocol,
        handlers: CollectorUpdateHandlers,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._repository = repository
        self._handlers = handlers
        self._logger = logger or logging.getLogger(__name__)
        self._route_map: dict[str, HandlerCallable] = {
            'updateNewMessage': self._handlers.handle_update_new_message,
            'updateMessageEdited': self._handlers.handle_update_message_edited,
            'updateMessageContent': self._handlers.handle_update_message_content,
            'updateDeleteMessages': self._handlers.handle_update_delete_messages,
            'updateChatLastMessage': self._handlers.handle_update_chat_last_message,
        }

    async def dispatch(self, update: JsonDict) -> UpdateHandlingResult:
        update_type = self._require_update_type(update)
        chat_id, message_id = self._extract_chat_message_ids(update_type, update)

        async with self._repository.transaction():
            raw_update_seq = await self._repository.insert_raw_update(
                update_type=update_type,
                payload_json=update,
                chat_id=chat_id,
                message_id=message_id,
            )

        context = DispatchContext(
            raw_update_seq=raw_update_seq,
            update_type=update_type,
            chat_id=chat_id,
            message_id=message_id,
        )

        handler = self._route_map.get(update_type)
        if handler is None:
            async with self._repository.transaction():
                await self._repository.mark_raw_update_applied(raw_update_seq)
            self._logger.info(
                'collector_update_ignored',
                extra={
                    'service': 'collector-telegram',
                    'event': 'collector_update_ignored',
                    'update_type': update_type,
                    'chat_id': chat_id,
                    'message_id': message_id,
                },
            )
            return UpdateHandlingResult(handled=False, note=f'ignored unsupported update type: {update_type}')

        try:
            async with self._repository.transaction():
                result = await handler(update)
                await self._repository.mark_raw_update_applied(raw_update_seq)
                return result
        except (UpdateApplyRetryableError, UpdateApplyTerminalError) as exc:
            await self._mark_failed(raw_update_seq, exc)
            raise
        except Exception as exc:
            wrapped = UpdateApplyRetryableError(
                f'unexpected collector update application failure: {update_type}'
            )
            await self._mark_failed(raw_update_seq, wrapped)
            raise wrapped from exc

    async def _mark_failed(self, raw_update_seq: int, exc: Exception) -> None:
        async with self._repository.transaction():
            await self._repository.mark_raw_update_failed(raw_update_seq, str(exc))

    def _require_update_type(self, update: JsonDict) -> str:
        raw = update.get('@type')
        if not isinstance(raw, str) or not raw:
            raise UpdateApplyTerminalError('update payload is missing @type')
        return raw

    def _extract_chat_message_ids(self, update_type: str, update: JsonDict) -> tuple[int | None, int | None]:
        if update_type == 'updateNewMessage':
            message = update.get('message')
            if isinstance(message, dict):
                return self._coerce_int_or_none(message.get('chat_id')), self._coerce_int_or_none(message.get('id'))
            return None, None

        if update_type in {'updateMessageEdited', 'updateMessageContent', 'updateChatLastMessage'}:
            return self._coerce_int_or_none(update.get('chat_id')), self._coerce_int_or_none(update.get('message_id'))

        if update_type == 'updateDeleteMessages':
            message_ids = update.get('message_ids')
            first_message_id = None
            if isinstance(message_ids, list) and message_ids:
                first_message_id = self._coerce_int_or_none(message_ids[0])
            return self._coerce_int_or_none(update.get('chat_id')), first_message_id
        return None, None

    def _coerce_int_or_none(self, value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        return None
