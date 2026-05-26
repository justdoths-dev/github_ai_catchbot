from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.models import SourceMessageProjection
from services.collector_telegram.repositories import CollectorRepository


class _FakeMappingResult:
    def __init__(
        self,
        *,
        first: dict[str, Any] | None = None,
        one: dict[str, Any] | None = None,
    ) -> None:
        self._first = first
        self._one = one if one is not None else first

    def mappings(self) -> _FakeMappingResult:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._first

    def one(self) -> dict[str, Any]:
        if self._one is None:
            raise AssertionError('expected one row')
        return self._one


class _FakeSession:
    def __init__(self, latest_rows: list[dict[str, Any] | None]) -> None:
        self.latest_rows = list(latest_rows)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _FakeMappingResult:
        sql = str(statement)
        call_params = params or {}
        self.calls.append((sql, call_params))

        if 'FROM source_message_versions' in sql:
            if not self.latest_rows:
                raise AssertionError('missing queued latest version row')
            return _FakeMappingResult(first=self.latest_rows.pop(0))
        if 'INSERT INTO source_message_versions' in sql:
            return _FakeMappingResult(
                one={
                    'source_message_version_id': 'version-row',
                    'version_no': call_params['version_no'],
                    'content_hash': call_params['content_hash'],
                }
            )
        if 'UPDATE source_messages' in sql:
            return _FakeMappingResult(first={'source_message_id': call_params['source_message_id']})
        raise AssertionError(f'unexpected SQL statement: {sql}')


def _projection(
    *,
    edited_at: datetime | None = None,
    content_hash: str = 'new-hash',
) -> SourceMessageProjection:
    return SourceMessageProjection(
        chat_id=100,
        message_id=200,
        logical_post_key='tg:100:200',
        is_channel_post=True,
        posted_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        edited_at=edited_at,
        message_link='https://t.me/c/100/200',
        author_signature=None,
        forward_info_json=None,
        content_type='text',
        text_body='hello',
        caption_text=None,
        text_surface='hello',
        entities_json=[],
        url_surface_json=[],
        raw_message_json={'@type': 'message', 'id': 200},
        content_hash=content_hash,
    )


def _normalize_sql(sql: str) -> str:
    return ' '.join(sql.split())


class CollectorRepositorySqlContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_append_source_message_version_if_changed_casts_nullable_datetime_binds_when_none(
        self,
    ) -> None:
        latest = {'version_no': 2, 'content_hash': 'old-hash'}
        session = _FakeSession([latest, latest])
        repository = CollectorRepository(session)  # type: ignore[arg-type]

        changed, version_row = await repository.append_source_message_version_if_changed(
            source_message_id='11111111-1111-1111-1111-111111111111',
            projection=_projection(edited_at=None),
            version_reason='reconcile',
            observed_at=None,
            telegram_edit_date=None,
        )

        self.assertTrue(changed)
        self.assertEqual(version_row['version_no'], 3)

        insert_sql, insert_params = next(
            (sql, params) for sql, params in session.calls if 'INSERT INTO source_message_versions' in sql
        )
        update_sql, update_params = next(
            (sql, params) for sql, params in session.calls if 'UPDATE source_messages' in sql
        )
        normalized_insert = _normalize_sql(insert_sql)
        normalized_update = _normalize_sql(update_sql)

        self.assertIn('CAST(:source_message_id AS uuid)', normalized_insert)
        self.assertIn('CAST(:observed_at AS timestamptz)', normalized_insert)
        self.assertIn('CAST(:telegram_edit_date AS timestamptz)', normalized_insert)
        self.assertIn('CAST(:entities_json AS jsonb)', normalized_insert)
        self.assertIn('CAST(:raw_message_json AS jsonb)', normalized_insert)
        self.assertIn('CAST(:source_message_id AS uuid)', normalized_update)
        self.assertIn('CAST(:forward_info_json AS jsonb)', normalized_update)
        self.assertIn('CAST(:entities_json AS jsonb)', normalized_update)
        self.assertIn('CAST(:url_surface_json AS jsonb)', normalized_update)
        self.assertIn('CAST(:raw_message_json AS jsonb)', normalized_update)
        self.assertIn(
            'WHEN CAST(:edited_at AS timestamptz) IS NULL THEN edited_at',
            normalized_update,
        )
        self.assertIn(
            'WHEN edited_at IS NULL THEN CAST(:edited_at AS timestamptz)',
            normalized_update,
        )
        self.assertIn(
            'ELSE GREATEST(edited_at, CAST(:edited_at AS timestamptz))',
            normalized_update,
        )
        self.assertIsNotNone(insert_params['observed_at'])
        self.assertIsNone(insert_params['telegram_edit_date'])
        self.assertIsNone(update_params['edited_at'])
        self.assertEqual(update_params['current_version_no'], 3)

    async def test_append_source_message_version_if_changed_preserves_non_null_datetime_params(
        self,
    ) -> None:
        latest = {'version_no': 1, 'content_hash': 'old-hash'}
        session = _FakeSession([latest, latest])
        repository = CollectorRepository(session)  # type: ignore[arg-type]
        edited_at = datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc)
        observed_at = datetime(2024, 1, 2, 12, 31, tzinfo=timezone.utc)

        changed, version_row = await repository.append_source_message_version_if_changed(
            source_message_id='11111111-1111-1111-1111-111111111111',
            projection=_projection(edited_at=edited_at),
            version_reason='edit',
            observed_at=observed_at,
            telegram_edit_date=edited_at,
        )

        self.assertTrue(changed)
        self.assertEqual(version_row['version_no'], 2)

        _, insert_params = next(
            (sql, params) for sql, params in session.calls if 'INSERT INTO source_message_versions' in sql
        )
        _, update_params = next(
            (sql, params) for sql, params in session.calls if 'UPDATE source_messages' in sql
        )

        self.assertEqual(insert_params['observed_at'], observed_at)
        self.assertEqual(insert_params['telegram_edit_date'], edited_at)
        self.assertEqual(update_params['edited_at'], edited_at)
        self.assertEqual(update_params['current_version_no'], 2)

    async def test_append_source_message_version_if_changed_skips_same_content_hash(
        self,
    ) -> None:
        session = _FakeSession([{'version_no': 4, 'content_hash': 'same-hash'}])
        repository = CollectorRepository(session)  # type: ignore[arg-type]

        changed, version_row = await repository.append_source_message_version_if_changed(
            source_message_id='11111111-1111-1111-1111-111111111111',
            projection=_projection(content_hash='same-hash'),
            version_reason='reconcile',
        )

        self.assertFalse(changed)
        self.assertIsNone(version_row)
        self.assertFalse(any('INSERT INTO source_message_versions' in sql for sql, _ in session.calls))
        self.assertFalse(any('UPDATE source_messages' in sql for sql, _ in session.calls))


if __name__ == '__main__':
    unittest.main()
