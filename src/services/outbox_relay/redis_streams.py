from __future__ import annotations

from typing import Any, Protocol

from .models import QueueRoute, RedisQueuedMessage


class RedisXAddClient(Protocol):
    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        maxlen: int | None = None,
        approximate: bool | None = None,
    ) -> Any: ...


class RedisStreamsPublisher:
    def __init__(self, client: RedisXAddClient, *, maxlen: int | None = None) -> None:
        self._client = client
        self._maxlen = maxlen

    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        fields = message.as_stream_fields()
        if self._maxlen is None:
            message_id = await self._client.xadd(route.queue_name, fields)
        else:
            message_id = await self._client.xadd(
                route.queue_name,
                fields,
                maxlen=self._maxlen,
                approximate=True,
            )
        if isinstance(message_id, bytes):
            return message_id.decode("utf-8")
        return str(message_id)
