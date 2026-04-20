from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.idempotency import IdempotencyPolicy


class IdempotencyPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = IdempotencyPolicy()

    def test_same_hash_skips_new_version(self) -> None:
        self.assertFalse(self.policy.should_append_new_version('abc', 'abc'))

    def test_different_hash_appends_new_version(self) -> None:
        self.assertTrue(self.policy.should_append_new_version('abc', 'def'))

    def test_semantic_event_dedupe_key_uses_locked_prefix(self) -> None:
        key = self.policy.semantic_event_dedupe_key(
            'source_message.created.v1',
            '11111111-1111-1111-1111-111111111111',
            2,
        )
        self.assertEqual(key, 'srcmsg:create:11111111-1111-1111-1111-111111111111:2')

    def test_reconcile_key_appends_extra_suffix(self) -> None:
        key = self.policy.semantic_event_dedupe_key(
            'source_message.reconciled.v1',
            '11111111-1111-1111-1111-111111111111',
            3,
            extra='startup_warm_backfill',
        )
        self.assertEqual(
            key,
            'srcmsg:reconcile:11111111-1111-1111-1111-111111111111:3:startup_warm_backfill',
        )


if __name__ == '__main__':
    unittest.main()
