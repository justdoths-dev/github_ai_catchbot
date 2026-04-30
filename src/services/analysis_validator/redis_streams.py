from __future__ import annotations

from typing import Any, Protocol

from .models import StreamMessage


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


class RedisStreamConsumer:
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

    async def read_batch(self) -> list[StreamMessage]:
        raw = await self._client.xreadgroup(
            self._consumer_group,
            self._consumer_name,
            {self._queue_name: ">"},
            count=self._batch_size,
            block=self._block_ms,
        )
        messages: list[StreamMessage] = []
        for stream_name, entries in raw or []:
            stream = _decode_value(stream_name)
            for message_id, fields in entries:
                messages.append(
                    StreamMessage(
                        stream=stream,
                        message_id=_decode_value(message_id),
                        fields={_decode_value(key): _decode_value(value) for key, value in fields.items()},
                    )
                )
        return messages

    async def ack(self, message_id: str) -> None:
        await self._client.xack(self._queue_name, self._consumer_group, message_id)


def _decode_value(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)
