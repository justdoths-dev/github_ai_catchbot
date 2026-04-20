from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from services.collector_telegram.message_projection import MessageProjectionBuilder


class MessageProjectionBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.builder = MessageProjectionBuilder()

    def test_album_logical_post_key_uses_media_album_id(self) -> None:
        message = {
            'chat_id': 100,
            'id': 200,
            'date': 1713550000,
            'media_album_id': 9001,
            'content': {'@type': 'messageText', 'text': {'text': 'hello', 'entities': []}},
        }
        projection = self.builder.build_source_projection(message)
        self.assertEqual(projection.logical_post_key, 'tg:100:album:9001')

    def test_text_url_entity_is_preferred_into_url_surface(self) -> None:
        message = {
            'chat_id': 100,
            'id': 200,
            'date': 1713550000,
            'content': {
                '@type': 'messageText',
                'text': {
                    'text': 'Read this repo',
                    'entities': [
                        {
                            'offset': 0,
                            'length': 4,
                            'type': {
                                '@type': 'textEntityTypeTextUrl',
                                'url': 'https://github.com/example/repo',
                            },
                        }
                    ],
                },
            },
        }
        projection = self.builder.build_source_projection(message)
        self.assertIsNotNone(projection.url_surface_json)
        self.assertEqual(projection.url_surface_json[0]['observed_url'], 'https://github.com/example/repo')
        self.assertEqual(projection.url_surface_json[0]['source_kind'], 'entity')

    def test_content_hash_is_deterministic_for_same_message(self) -> None:
        message = {
            'chat_id': 100,
            'id': 200,
            'date': 1713550000,
            'content': {
                '@type': 'messageText',
                'text': {
                    'text': 'hello world',
                    'entities': [],
                },
            },
        }
        projection_a = self.builder.build_source_projection(message)
        projection_b = self.builder.build_source_projection(message)
        self.assertEqual(projection_a.content_hash, projection_b.content_hash)


if __name__ == '__main__':
    unittest.main()
