from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.update_dispatcher import UpdateDispatcher
from services.collector_telegram.update_handlers import UpdateHandlingResult


class FakeTransaction:
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.raw_updates = []
        self.applied = []
        self.failed = []
        self.next_seq = 1

    def transaction(self):
        return FakeTransaction()

    async def insert_raw_update(self, *, update_type, payload_json, chat_id=None, message_id=None):
        seq = self.next_seq
        self.next_seq += 1
        self.raw_updates.append((seq, update_type, chat_id, message_id, payload_json))
        return seq

    async def mark_raw_update_applied(self, update_seq: int) -> None:
        self.applied.append(update_seq)

    async def mark_raw_update_failed(self, update_seq: int, error_text: str) -> None:
        self.failed.append((update_seq, error_text))


class FakeHandlers:
    async def handle_update_new_message(self, update):
        return UpdateHandlingResult(handled=True, source_message_ids=['msg-1'])

    async def handle_update_message_edited(self, update):
        return UpdateHandlingResult(handled=True)

    async def handle_update_message_content(self, update):
        return UpdateHandlingResult(handled=True)

    async def handle_update_delete_messages(self, update):
        return UpdateHandlingResult(handled=True)

    async def handle_update_chat_last_message(self, update):
        return UpdateHandlingResult(handled=True, reconcile_requested=True, reconcile_reason='last_message_missing')


class UpdateDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_supported_update_marks_raw_update_applied(self) -> None:
        repository = FakeRepository()
        dispatcher = UpdateDispatcher(repository, FakeHandlers())

        result = await dispatcher.dispatch(
            {
                '@type': 'updateNewMessage',
                'message': {'chat_id': 100, 'id': 200},
            }
        )

        self.assertTrue(result.handled)
        self.assertEqual(repository.applied, [1])
        self.assertEqual(repository.failed, [])

    async def test_dispatch_unknown_update_is_logged_as_ignored_noop(self) -> None:
        repository = FakeRepository()
        dispatcher = UpdateDispatcher(repository, FakeHandlers())

        result = await dispatcher.dispatch({'@type': 'updateSomeFutureType', 'chat_id': 1})

        self.assertFalse(result.handled)
        self.assertIn('ignored unsupported update type', result.note)
        self.assertEqual(repository.applied, [1])
        self.assertEqual(repository.failed, [])


if __name__ == '__main__':
    unittest.main()
