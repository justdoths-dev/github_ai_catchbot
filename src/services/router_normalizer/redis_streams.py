from __future__ import annotations

from typing import Any, Protocol

from .models import RedisNormalizeMessage


class RedisStreamConsumerClient(Protocol):
    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> Any: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...


class RedisStreamsConsumer:
    def __init__(
        self,
        client: RedisStreamConsumerClient,
        *,
        queue_name: str,
        consumer_group: str,
        consumer_name: str,
        block_ms: int,
        batch_size: int,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._batch_size = batch_size

    async def ensure_group(self) -> None:
        try:
            await self._client.xgroup_create(
                self._queue_name,
                self._consumer_group,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_batch(self) -> list[tuple[str, RedisNormalizeMessage]]:
        raw = await self._client.xreadgroup(
            self._consumer_group,
            self._consumer_name,
            {self._queue_name: ">"},
            count=self._batch_size,
            block=self._block_ms,
        )
        messages: list[tuple[str, RedisNormalizeMessage]] = []
        for _stream_name, entries in raw or []:
            for message_id, fields in entries:
                decoded_fields = _decode_fields(fields)
                messages.append((str(message_id), RedisNormalizeMessage.from_stream_fields(decoded_fields)))
        return messages

    async def ack(self, message_id: str) -> None:
        await self._client.xack(self._queue_name, self._consumer_group, message_id)


def _decode_fields(fields: dict[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        decoded_value = value.decode("utf-8") if isinstance(value, bytes) else value
        decoded[decoded_key] = decoded_value
    return decoded

