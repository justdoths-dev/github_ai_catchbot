from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.idempotency import IdempotencyPolicy
from services.collector_telegram.message_projection import MessageProjectionBuilder
from services.collector_telegram.outbox import CollectorOutboxBuilder
from services.collector_telegram.update_handlers import CollectorUpdateHandlers


class FakeRepository:
    def __init__(self) -> None:
        self.current = {}
        self.versions = {}
        self.outbox = []

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int):
        return self.current.get((platform, chat_id, message_id))

    async def upsert_source_message(self, projection, *, platform: str = 'telegram'):
        key = (platform, projection.chat_id, projection.message_id)
        row = self.current.get(key)
        if row is None:
            row = {
                'source_message_id': f'src-{projection.chat_id}-{projection.message_id}',
                'logical_post_key': projection.logical_post_key,
                'current_version_no': 0,
                'raw_message_json': projection.raw_message_json,
            }
            self.current[key] = row
        row.update(
            {
                'logical_post_key': projection.logical_post_key,
                'raw_message_json': projection.raw_message_json,
            }
        )
        return row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection,
        version_reason: str,
        observed_at=None,
        telegram_edit_date=None,
    ):
        previous_hash = self.versions.get(source_message_id, [])[-1]['content_hash'] if self.versions.get(source_message_id) else None
        if previous_hash == projection.content_hash:
            return False, None
        version_no = len(self.versions.get(source_message_id, [])) + 1
        row = {'version_no': version_no, 'content_hash': projection.content_hash, 'version_reason': version_reason}
        self.versions.setdefault(source_message_id, []).append(row)
        for current_row in self.current.values():
            if current_row['source_message_id'] == source_message_id:
                current_row['current_version_no'] = version_no
                current_row['raw_message_json'] = projection.raw_message_json
        return True, row

    async def mark_message_deleted(self, *, platform: str, chat_id: int, message_id: int, delete_kind: str, deleted_at=None):
        key = (platform, chat_id, message_id)
        row = self.current.get(key)
        if row is None:
            return None
        row['delete_kind'] = delete_kind
        return row

    async def insert_outbox_event(self, event):
        self.outbox.append(event)


class UpdateHandlersTests(unittest.IsolatedAsyncioTestCase):
    async def test_handle_update_new_message_writes_version_and_outbox(self) -> None:
        repository = FakeRepository()
        handlers = CollectorUpdateHandlers(
            repository,
            MessageProjectionBuilder(),
            CollectorOutboxBuilder(IdempotencyPolicy()),
        )

        result = await handlers.handle_update_new_message(
            {
                '@type': 'updateNewMessage',
                'message': {
                    'chat_id': 100,
                    'id': 200,
                    'date': 1713550000,
                    'content': {'@type': 'messageText', 'text': {'text': 'hello', 'entities': []}},
                },
            }
        )

        self.assertTrue(result.handled)
        self.assertTrue(result.version_appended)
        self.assertEqual(result.outbox_events_created, 1)
        self.assertEqual(len(repository.outbox), 1)
        self.assertEqual(repository.outbox[0].event_type, 'source_message.created.v1')


if __name__ == '__main__':
    unittest.main()
