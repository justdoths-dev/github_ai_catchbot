from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.config import CollectorTelegramConfig
from services.collector_telegram.models import CollectorEnvironment, CollectorMode, TrackedChat
from services.collector_telegram.registry_sync import ChannelRegistrySyncService


class StubTDLib:
    def build_search_public_chat_request(self, username: str):
        return {'payload': {'@type': 'searchPublicChat', 'username': username}}

    def build_join_chat_request(self, chat_id: int):
        return {'payload': {'@type': 'joinChat', 'chat_id': chat_id}}

    def build_join_chat_by_invite_link_request(self, invite_link: str):
        return {'payload': {'@type': 'joinChatByInviteLink', 'invite_link': invite_link}}

    def build_get_chat_history_request(self, **kwargs):
        return {'payload': {'@type': 'getChatHistory', **kwargs}}

    async def call(self, request, timeout: float = 30.0):
        if request.get('@type') == 'searchPublicChat':
            return {
                '@type': 'chat',
                'id': 12345,
                'title': 'Catchbot Channel',
                'username': 'catchbot_channel',
                'type': {'@type': 'chatTypeSupergroup'},
            }
        if request.get('@type') == 'joinChat':
            return {'@type': 'ok'}
        if request.get('@type') == 'getChatHistory':
            return {'messages': []}
        return {'@type': 'ok'}


class StubRepository:
    def __init__(self):
        self.rows = [
            {
                'registry_id': 'r1',
                'source_kind': 'public_username',
                'source_value': 'catchbot_channel',
                'chat_id': None,
            }
        ]
        self.resolved_calls = []
        self.access_calls = []

    async def list_active_tracked_chats(self):
        return [
            TrackedChat(
                registry_id='r1',
                chat_id=12345,
                desired_state='active',
                access_state='joined',
                source_kind='public_username',
                source_value='catchbot_channel',
            )
        ]

    async def list_registry_rows_by_access_states(self, access_states, *, desired_state='active'):
        return list(self.rows)

    async def mark_channel_resolved(self, **kwargs):
        self.resolved_calls.append(kwargs)

    async def mark_channel_access_state(self, **kwargs):
        self.access_calls.append(kwargs)


class RegistrySyncServiceTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_load_active_channels_delegates_to_repository(self):
        repository = StubRepository()
        service = ChannelRegistrySyncService(self._config(), tdlib=StubTDLib(), repository=repository)
        channels = await service.load_active_channels()
        self.assertEqual(len(channels), 1)
        self.assertEqual(channels[0].chat_id, 12345)

    async def test_sync_unresolved_channels_resolves_and_marks_joined(self):
        repository = StubRepository()
        service = ChannelRegistrySyncService(self._config(), tdlib=StubTDLib(), repository=repository)
        summary = await service.sync_unresolved_channels()
        self.assertEqual(summary.processed_count, 1)
        self.assertEqual(summary.joined_count, 1)
        self.assertEqual(len(repository.resolved_calls), 1)
        self.assertEqual(len(repository.access_calls), 1)
        self.assertEqual(repository.access_calls[0]['access_state'], 'joined')


if __name__ == '__main__':
    unittest.main()
