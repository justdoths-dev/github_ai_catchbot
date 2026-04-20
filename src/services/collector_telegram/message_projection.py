"""Message-to-projection builder for collector update handling."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

from .models import SourceMessageProjection, SourceMessageVersionProjection

JsonDict = dict[str, Any]
_URL_REGEX = re.compile(r"https?://\S+")
_CAPTION_CONTENT_TYPES = {
    'messageAnimation',
    'messageAudio',
    'messageDocument',
    'messagePaidMedia',
    'messagePhoto',
    'messageVideo',
    'messageVoiceNote',
}


class MessageProjectionBuilder:
    """Build collector-side current/version projections from TDLib messages.

    Design constraints preserved from the locked collector docs:
    - raw message JSON is preserved,
    - projection is derived surface only,
    - entity-first URL extraction is preferred,
    - logical_post_key keeps message-level storage while allowing later post-level merge.
    """

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def build_source_projection(self, message: JsonDict) -> SourceMessageProjection:
        chat_id = int(message['chat_id'])
        message_id = int(message['id'])
        content = self._get_mapping(message.get('content'))
        content_type = self._content_type_name(content.get('@type'))

        text_body, text_entities = self._extract_text_body(content)
        caption_text, caption_entities = self._extract_caption(content)
        combined_entities = self._combine_entities(text_entities, caption_entities)
        url_surface = self._extract_url_surface(
            message=message,
            text_body=text_body,
            caption_text=caption_text,
            entities=combined_entities,
        )

        projection = SourceMessageProjection(
            chat_id=chat_id,
            message_id=message_id,
            logical_post_key=self.compute_logical_post_key(message),
            is_channel_post=bool(message.get('is_channel_post', False)),
            posted_at=self._unix_to_datetime(message.get('date')) or datetime.now(timezone.utc),
            edited_at=self._unix_to_datetime(message.get('edit_date')),
            message_link=self._extract_message_link(message),
            author_signature=self._coerce_str_or_none(message.get('author_signature')),
            forward_info_json=self._get_mapping_or_none(message.get('forward_info')),
            content_type=content_type,
            text_body=text_body,
            caption_text=caption_text,
            text_surface=self._build_text_surface(text_body=text_body, caption_text=caption_text),
            entities_json=combined_entities or None,
            url_surface_json=url_surface or None,
            raw_message_json=copy.deepcopy(message),
            content_hash='',
        )
        content_hash = self.compute_content_hash(projection)
        return SourceMessageProjection(
            chat_id=projection.chat_id,
            message_id=projection.message_id,
            logical_post_key=projection.logical_post_key,
            is_channel_post=projection.is_channel_post,
            posted_at=projection.posted_at,
            edited_at=projection.edited_at,
            message_link=projection.message_link,
            author_signature=projection.author_signature,
            forward_info_json=projection.forward_info_json,
            content_type=projection.content_type,
            text_body=projection.text_body,
            caption_text=projection.caption_text,
            text_surface=projection.text_surface,
            entities_json=projection.entities_json,
            url_surface_json=projection.url_surface_json,
            raw_message_json=projection.raw_message_json,
            content_hash=content_hash,
        )

    def build_version_projection(
        self,
        message: JsonDict,
        reason: str,
        *,
        source_message_id: str | None = None,
        version_no: int | None = None,
    ) -> SourceMessageVersionProjection:
        source_projection = self.build_source_projection(message)
        return SourceMessageVersionProjection(
            source_message_id=None if source_message_id is None else source_message_id,  # existing model allows UUID|None at runtime
            version_no=version_no,
            version_reason=reason,
            observed_at=datetime.now(timezone.utc),
            telegram_edit_date=source_projection.edited_at,
            text_surface=source_projection.text_surface,
            entities_json=source_projection.entities_json,
            raw_message_json=source_projection.raw_message_json,
            content_hash=source_projection.content_hash,
        )

    def compute_logical_post_key(self, message: JsonDict) -> str:
        chat_id = int(message['chat_id'])
        message_id = int(message['id'])
        media_album_id = message.get('media_album_id')
        try:
            media_album_id_int = int(media_album_id or 0)
        except (TypeError, ValueError):
            media_album_id_int = 0
        if media_album_id_int != 0:
            return f'tg:{chat_id}:album:{media_album_id_int}'
        return f'tg:{chat_id}:{message_id}'

    def compute_content_hash(self, projection: SourceMessageProjection) -> str:
        canonical_payload = {
            'content_type': projection.content_type,
            'text_body': self._normalize_for_hash(projection.text_body),
            'caption_text': self._normalize_for_hash(projection.caption_text),
            'text_surface': self._normalize_for_hash(projection.text_surface),
            'entities_json': projection.entities_json or [],
            'url_surface_json': projection.url_surface_json or [],
            'author_signature': projection.author_signature,
            'forward_info_json': projection.forward_info_json,
            'logical_post_key': projection.logical_post_key,
            'is_channel_post': projection.is_channel_post,
        }
        payload = json.dumps(canonical_payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()

    def _extract_text_body(self, content: JsonDict) -> tuple[str | None, list[JsonDict]]:
        if content.get('@type') != 'messageText':
            return None, []
        formatted_text = self._get_mapping_or_none(content.get('text')) or {}
        return self._extract_formatted_text(formatted_text, surface='text_body')

    def _extract_caption(self, content: JsonDict) -> tuple[str | None, list[JsonDict]]:
        if content.get('@type') not in _CAPTION_CONTENT_TYPES:
            return None, []
        formatted_text = self._get_mapping_or_none(content.get('caption')) or {}
        return self._extract_formatted_text(formatted_text, surface='caption_text')

    def _extract_formatted_text(self, formatted_text: JsonDict, *, surface: str) -> tuple[str | None, list[JsonDict]]:
        text = self._coerce_str_or_none(formatted_text.get('text'))
        entities_raw = formatted_text.get('entities') or []
        entities: list[JsonDict] = []
        for entity in entities_raw:
            entity_map = self._get_mapping_or_none(entity)
            if entity_map is None:
                continue
            entity_copy = copy.deepcopy(entity_map)
            entity_copy['surface'] = surface
            entities.append(entity_copy)
        return text, entities

    def _combine_entities(self, text_entities: list[JsonDict], caption_entities: list[JsonDict]) -> list[JsonDict]:
        combined: list[JsonDict] = []
        combined.extend(text_entities)
        combined.extend(caption_entities)
        return combined

    def _build_text_surface(self, *, text_body: str | None, caption_text: str | None) -> str | None:
        parts = [part.strip() for part in [text_body, caption_text] if part and part.strip()]
        if not parts:
            return None
        return '\n\n'.join(parts)

    def _extract_url_surface(
        self,
        *,
        message: JsonDict,
        text_body: str | None,
        caption_text: str | None,
        entities: list[JsonDict],
    ) -> list[JsonDict]:
        urls: list[JsonDict] = []
        seen: set[tuple[str, str]] = set()

        def add(url: str | None, source_kind: str, *, context: str | None = None) -> None:
            normalized = self._normalize_observed_url(url)
            if not normalized:
                return
            key = (source_kind, normalized)
            if key in seen:
                return
            seen.add(key)
            entry: JsonDict = {'observed_url': normalized, 'source_kind': source_kind}
            if context:
                entry['context'] = context
            urls.append(entry)

        for entity in entities:
            entity_type = self._entity_type_name(entity)
            surface_name = self._coerce_str_or_none(entity.get('surface')) or 'unknown'
            surface_text = text_body if surface_name == 'text_body' else caption_text
            if entity_type == 'textEntityTypeTextUrl':
                add(self._extract_text_url_entity_url(entity), 'entity', context=surface_name)
                continue
            if entity_type == 'textEntityTypeUrl':
                add(self._extract_url_from_entity_slice(surface_text, entity), 'entity', context=surface_name)
                continue

        for preview_url in self._extract_preview_urls(message):
            add(preview_url, 'preview')

        for raw_text in [text_body, caption_text]:
            if not raw_text:
                continue
            for match in _URL_REGEX.findall(raw_text):
                add(match, 'regex')
        return urls

    def _extract_preview_urls(self, message: JsonDict) -> list[str]:
        content = self._get_mapping(message.get('content'))
        preview = self._get_mapping_or_none(content.get('link_preview'))
        if preview is None:
            return []
        candidates: list[str] = []
        for key in ('url', 'site_name', 'title'):
            value = preview.get(key)
            if isinstance(value, str) and value.startswith(('http://', 'https://')):
                candidates.append(value)
        type_specific = preview.get('type')
        if isinstance(type_specific, dict):
            for nested_value in type_specific.values():
                if isinstance(nested_value, str) and nested_value.startswith(('http://', 'https://')):
                    candidates.append(nested_value)
        return candidates

    def _extract_message_link(self, message: JsonDict) -> str | None:
        for key in ('message_link', '_message_link'):
            value = message.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _extract_url_from_entity_slice(self, text: str | None, entity: JsonDict) -> str | None:
        if not text:
            return None
        offset = entity.get('offset')
        length = entity.get('length')
        if not isinstance(offset, int) or not isinstance(length, int):
            return None
        if offset < 0 or length <= 0:
            return None
        try:
            return text[offset: offset + length]
        except Exception:
            self._logger.debug('failed_entity_slice', exc_info=True)
            return None

    def _extract_text_url_entity_url(self, entity: JsonDict) -> str | None:
        entity_type = self._get_mapping_or_none(entity.get('type')) or {}
        url = entity_type.get('url')
        return self._coerce_str_or_none(url)

    def _entity_type_name(self, entity: JsonDict) -> str | None:
        entity_type = self._get_mapping_or_none(entity.get('type')) or {}
        raw = entity_type.get('@type')
        return raw if isinstance(raw, str) else None

    def _content_type_name(self, raw_type: Any) -> str | None:
        if not isinstance(raw_type, str):
            return None
        if raw_type.startswith('message'):
            suffix = raw_type[len('message'):]
            return suffix[:1].lower() + suffix[1:] if suffix else 'message'
        return raw_type

    def _normalize_observed_url(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize('NFKC', value).strip()
        if not normalized:
            return None
        if not normalized.startswith(('http://', 'https://')):
            return None
        return normalized

    def _normalize_for_hash(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize('NFKC', value)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized or None

    def _unix_to_datetime(self, value: Any) -> datetime | None:
        if value in (None, 0, ''):
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if not isinstance(value, (int, float)):
            return None
        return datetime.fromtimestamp(float(value), tz=timezone.utc)

    def _get_mapping(self, value: Any) -> JsonDict:
        mapping = self._get_mapping_or_none(value)
        return mapping or {}

    def _get_mapping_or_none(self, value: Any) -> JsonDict | None:
        return value if isinstance(value, dict) else None

    def _coerce_str_or_none(self, value: Any) -> str | None:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return None
