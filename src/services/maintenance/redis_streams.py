from __future__ import annotations

from typing import Any, Protocol

from .models import StreamMessage


class RedisStreamConsumerClient(Protocol):
    async def xgroup_create(self, name: str, groupname: str, id: str = "$", mkstream: bool = False) -> Any: ...
    async def xinfo_groups(self, name: str) -> Any: ...
    async def xrange(self, name: str, min: str = "-", max: str = "+", count: int | None = None) -> Any: ...
    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...
    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...


class RedisConsumerGroupMissingError(RuntimeError):
    pass


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

    async def ensure_group(self, *, allow_create: bool = True) -> bool:
        if not allow_create:
            return await self.group_exists()
        try:
            await self._client.xgroup_create(self._queue_name, self._consumer_group, id="0", mkstream=True)
            return True
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise
            return True

    async def group_exists(self) -> bool:
        return await self._load_group() is not None

    async def preview_batch(self, *, count: int | None = None) -> list[StreamMessage]:
        group = await self._load_group()
        if group is None:
            raise RedisConsumerGroupMissingError("consumer_group_missing")
        last_delivered_id = _decode_value(_dict_get(group, "last-delivered-id") or "0-0")
        raw = await self._client.xrange(
            self._queue_name,
            min=f"({last_delivered_id}",
            max="+",
            count=count or self._batch_size,
        )
        return [_stream_message_from_entry(self._queue_name, entry) for entry in raw or []]

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

    async def _load_group(self) -> dict[Any, Any] | None:
        groups = await self._client.xinfo_groups(self._queue_name)
        for group in groups or []:
            if isinstance(group, dict) and _decode_value(_dict_get(group, "name")) == self._consumer_group:
                return group
        return None


def _decode_value(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _dict_get(mapping: dict[Any, Any], key: str) -> Any:
    return mapping.get(key, mapping.get(key.encode("utf-8")))


def _stream_message_from_entry(queue_name: str, entry: Any) -> StreamMessage:
    message_id, fields = entry
    return StreamMessage(
        stream=queue_name,
        message_id=_decode_value(message_id),
        fields={_decode_value(key): _decode_value(value) for key, value in (fields or {}).items()},
    )
