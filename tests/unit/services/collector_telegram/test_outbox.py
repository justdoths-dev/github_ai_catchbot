from __future__ import annotations

import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.idempotency import IdempotencyPolicy
from services.collector_telegram.outbox import CollectorOutboxBuilder


class CollectorOutboxBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.builder = CollectorOutboxBuilder(policy=IdempotencyPolicy())
        self.source_message_id = '11111111-1111-1111-1111-111111111111'
        self.occurred_at = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)

    def test_build_created_minimum_payload(self) -> None:
        event = self.builder.build_created(
            source_message_id=self.source_message_id,
            current_version_no=1,
            logical_post_key='tg:-100:123',
            occurred_at=self.occurred_at,
        )
        self.assertEqual(event.event_type, 'source_message.created.v1')
        self.assertEqual(event.aggregate_type, 'source_message')
        self.assertEqual(event.aggregate_id, self.source_message_id)
        self.assertEqual(event.payload_json['source_message_id'], self.source_message_id)
        self.assertEqual(event.payload_json['current_version_no'], 1)
        self.assertEqual(event.payload_json['logical_post_key'], 'tg:-100:123')
        self.assertEqual(event.payload_json['occurred_at'], '2026-04-20T00:00:00+00:00')

    def test_build_deleted_includes_delete_kind(self) -> None:
        event = self.builder.build_deleted(
            source_message_id=self.source_message_id,
            current_version_no=2,
            logical_post_key='tg:-100:123',
            occurred_at=self.occurred_at,
            delete_kind='permanent',
        )
        self.assertEqual(event.event_type, 'source_message.deleted.v1')
        self.assertEqual(event.payload_json['delete_kind'], 'permanent')
        self.assertEqual(
            event.dedupe_key,
            'srcmsg:delete:11111111-1111-1111-1111-111111111111:2',
        )

    def test_build_reconciled_includes_reconcile_reason(self) -> None:
        event = self.builder.build_reconciled(
            source_message_id=self.source_message_id,
            current_version_no=3,
            logical_post_key='tg:-100:123',
            occurred_at=self.occurred_at,
            reconcile_reason='startup_warm_backfill',
        )
        self.assertEqual(event.event_type, 'source_message.reconciled.v1')
        self.assertEqual(event.payload_json['reconcile_reason'], 'startup_warm_backfill')
        self.assertEqual(
            event.dedupe_key,
            'srcmsg:reconcile:11111111-1111-1111-1111-111111111111:3:startup_warm_backfill',
        )


if __name__ == '__main__':
    unittest.main()
