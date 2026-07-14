from __future__ import annotations

import pytest

from services.maintenance.models import StreamMessage
from services.maintenance.redis_streams import RedisStreamConsumer


QUEUE_NAME = "q.replay"
CONSUMER_GROUP = "maintenance-replay"
CONSUMER_NAME = "maintenance-replay-1"


class FakeRedisStreamsClient:
    def __init__(self, *, pending=None, new=None, pending_error: Exception | None = None) -> None:
        self.pending = list(pending or [])
        self.new = list(new or [])
        self.pending_error = pending_error
        self.read_calls: list[dict[str, object]] = []
        self.ack_calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.forbidden_calls: list[str] = []

    async def xreadgroup(self, groupname, consumername, streams, *, count=None, block=None):
        self.read_calls.append(
            {
                "groupname": groupname,
                "consumername": consumername,
                "streams": streams,
                "count": count,
                "block": block,
            }
        )
        stream_id = streams[QUEUE_NAME]
        if stream_id == "0":
            if self.pending_error is not None:
                raise self.pending_error
            entries = self.pending
        else:
            entries = self.new
        if not entries:
            return []
        return [(QUEUE_NAME.encode(), entries[:count])]

    async def xack(self, name, groupname, *ids):
        self.ack_calls.append((name, groupname, ids))
        return len(ids)

    async def xgroup_create(self, *args, **kwargs):
        del args, kwargs
        self.forbidden_calls.append("xgroup_create")

    async def xpending(self, *args, **kwargs):
        del args, kwargs
        self.forbidden_calls.append("xpending")

    async def xautoclaim(self, *args, **kwargs):
        del args, kwargs
        self.forbidden_calls.append("xautoclaim")

    async def xclaim(self, *args, **kwargs):
        del args, kwargs
        self.forbidden_calls.append("xclaim")


def _consumer(client: FakeRedisStreamsClient, *, batch_size: int = 2) -> RedisStreamConsumer:
    return RedisStreamConsumer(
        client,
        queue_name=QUEUE_NAME,
        consumer_group=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        block_ms=1250,
        batch_size=batch_size,
    )


@pytest.mark.asyncio
async def test_read_batch_returns_own_pending_first_without_new_message_read() -> None:
    client = FakeRedisStreamsClient(
        pending=[
            (b"1-0", {b"trigger_event_id": b"event-one"}),
            (b"2-0", None),
            (b"3-0", {b"trigger_event_id": b"outside-bound"}),
        ],
        new=[(b"4-0", {b"trigger_event_id": b"new-event"})],
    )

    messages = await _consumer(client, batch_size=2).read_batch()

    assert messages == [
        StreamMessage(stream=QUEUE_NAME, message_id="1-0", fields={"trigger_event_id": "event-one"}),
        StreamMessage(stream=QUEUE_NAME, message_id="2-0", fields={}),
    ]
    assert client.read_calls == [
        {
            "groupname": CONSUMER_GROUP,
            "consumername": CONSUMER_NAME,
            "streams": {QUEUE_NAME: "0"},
            "count": 2,
            "block": None,
        }
    ]
    assert client.forbidden_calls == []


@pytest.mark.asyncio
async def test_read_batch_falls_through_to_bounded_blocking_new_read_only_when_pending_is_empty() -> None:
    client = FakeRedisStreamsClient(new=[(b"5-0", {b"trigger_event_id": b"new-event"})])

    messages = await _consumer(client, batch_size=1).read_batch()

    assert messages == [
        StreamMessage(stream=QUEUE_NAME, message_id="5-0", fields={"trigger_event_id": "new-event"})
    ]
    assert client.read_calls == [
        {
            "groupname": CONSUMER_GROUP,
            "consumername": CONSUMER_NAME,
            "streams": {QUEUE_NAME: "0"},
            "count": 1,
            "block": None,
        },
        {
            "groupname": CONSUMER_GROUP,
            "consumername": CONSUMER_NAME,
            "streams": {QUEUE_NAME: ">"},
            "count": 1,
            "block": 1250,
        },
    ]
    assert client.forbidden_calls == []


@pytest.mark.asyncio
async def test_pending_read_error_propagates_without_falling_through_to_new_entries() -> None:
    client = FakeRedisStreamsClient(
        pending_error=RuntimeError("redacted pending read failure"),
        new=[(b"6-0", {b"trigger_event_id": b"must-not-read"})],
    )

    with pytest.raises(RuntimeError, match="redacted pending read failure"):
        await _consumer(client).read_batch()

    assert len(client.read_calls) == 1
    assert client.read_calls[0]["streams"] == {QUEUE_NAME: "0"}
    assert client.forbidden_calls == []


@pytest.mark.asyncio
async def test_ack_targets_only_the_exact_message_id() -> None:
    client = FakeRedisStreamsClient()
    consumer = _consumer(client)

    await consumer.ack("7-0")

    assert client.ack_calls == [(QUEUE_NAME, CONSUMER_GROUP, ("7-0",))]
    assert client.forbidden_calls == []
