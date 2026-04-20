from __future__ import annotations

import sys
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.models import CollectorEnvironment, CollectorMode, SourceMessageProjection
from services.collector_telegram.idempotency import IdempotencyPolicy
from services.collector_telegram.outbox import CollectorOutboxBuilder
from services.collector_telegram.reconcile import ReconcileService


class StubTDLib:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def build_get_chat_history_request(self, **kwargs):
        return {'payload': {'@type': 'getChatHistory', **kwargs}}

    async def call(self, request, timeout: float = 30.0):
        self.calls.append((request, timeout))
        return self._response


class StubProjectionBuilder:
    def build_source_projection(self, message):
        return SourceMessageProjection(
            chat_id=int(message['chat_id']),
            message_id=int(message['id']),
            logical_post_key=f"tg:{message['chat_id']}:{message['id']}",
            is_channel_post=False,
            posted_at=datetime.fromtimestamp(message['date'], tz=timezone.utc),
            edited_at=None,
            message_link=None,
            author_signature=None,
            forward_info_json=None,
            content_type='text',
            text_body='hello',
            caption_text=None,
            text_surface='hello',
            entities_json=[],
            url_surface_json=[],
            raw_message_json=message,
            content_hash=f"hash:{message['id']}",
        )


class StubRepository:
    def __init__(self):
        self.messages = {}
        self.outbox = []

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def get_source_message(self, *, platform, chat_id, message_id):
        return self.messages.get((chat_id, message_id))

    async def upsert_source_message(self, projection, *, platform='telegram'):
        row = {
            'source_message_id': f'source-{projection.chat_id}-{projection.message_id}',
            'chat_id': projection.chat_id,
            'message_id': projection.message_id,
        }
        self.messages[(projection.chat_id, projection.message_id)] = row
        return row

    async def append_source_message_version_if_changed(self, *, source_message_id, projection, version_reason, observed_at=None, telegram_edit_date=None):
        return True, {'version_no': 1}

    async def insert_outbox_event(self, event):
        self.outbox.append(event)

    async def list_reconcile_targets(self, limit):
        return []

    async def update_channel_sync_cursor(self, **kwargs):
        return None


class ReconcileServiceTests(unittest.IsolatedAsyncioTestCase):
    def _config(self):
        return CollectorTelegramConfig(
            app_env=CollectorEnvironment.DEV,
            database_url='postgresql://collector:secret@localhost:5432/catchbot',
            redis_url=None,
            collector_mode=CollectorMode.REPLAY,
            telegram_api_id=12345,
            telegram_api_hash='hash-value',
            telegram_phone_number='+10000000000',
            telegram_2fa_password=None,
            tdlib_state_dir='/tmp/catchbot-tdlib-state',
            tdlib_files_dir='/tmp/catchbot-tdlib-files',
            tdlib_db_encryption_key='enc-key',
            reconcile_interval_sec=300,
            reconcile_backfill_limit=50,
            warm_backfill_limit=30,
            history_page_limit=50,
            log_level='INFO',
        )

    async def test_warm_backfill_returns_no_changes_for_empty_history(self):
        service = ReconcileService(
            self._config(),
            tdlib=StubTDLib({'messages': []}),
            repository=StubRepository(),
            projection_builder=StubProjectionBuilder(),
            outbox_builder=CollectorOutboxBuilder(IdempotencyPolicy()),
        )
        summary = await service.run_startup_warm_backfill(1001)
        self.assertEqual(summary.chat_id, 1001)
        self.assertEqual(summary.result_type, 'no_changes')
        self.assertEqual(summary.processed_count, 0)

    async def test_authoritative_reconcile_emits_reconciled_outbox_on_change(self):
        repository = StubRepository()
        service = ReconcileService(
            self._config(),
            tdlib=StubTDLib({'messages': [{'chat_id': 1001, 'id': 2002, 'date': 1710000000}]}),
            repository=repository,
            projection_builder=StubProjectionBuilder(),
            outbox_builder=CollectorOutboxBuilder(IdempotencyPolicy()),
        )
        summary = await service.run_authoritative_reconcile(1001, reason='scheduled')
        self.assertEqual(summary.result_type, 'gap_filled')
        self.assertEqual(summary.processed_count, 1)
        self.assertEqual(summary.inserted_count, 1)
        self.assertEqual(len(repository.outbox), 1)
        self.assertEqual(repository.outbox[0].event_type, 'source_message.reconciled.v1')


if __name__ == '__main__':
    unittest.main()
