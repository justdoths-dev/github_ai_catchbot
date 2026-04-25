from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable

from .models import ExtractedUrl, SourceMessageSnapshot, TextSurfaces


_URL_REGEX = re.compile(r"https?://[^\s<>'\")\]]+", re.IGNORECASE)
_SOURCE_PRIORITY = {"entity": 0, "preview": 1, "regex": 2}


def extract_urls(snapshot: SourceMessageSnapshot, surfaces: TextSurfaces) -> list[ExtractedUrl]:
    candidates: list[ExtractedUrl] = []
    candidates.extend(_extract_from_url_surface(snapshot.url_surface_json or []))
    candidates.extend(_extract_from_entities(snapshot.entities_json or [], surfaces.raw_text_surface))
    candidates.extend(_extract_preview_from_raw(snapshot.raw_message_json))
    candidates.extend(_extract_regex(surfaces.raw_text_surface))
    return _dedupe_by_url(candidates)


def _extract_from_url_surface(rows: Iterable[dict[str, Any]]) -> list[ExtractedUrl]:
    extracted: list[ExtractedUrl] = []
    for row in rows:
        observed_url = _normalize_observed_url(row.get("observed_url"))
        if observed_url is None:
            continue
        source_kind = _coerce_source_kind(row.get("source_kind"))
        context = _optional_str(row.get("context")) or _optional_str(row.get("context_path"))
        extracted.append(ExtractedUrl(observed_url=observed_url, source_kind=source_kind, context_path=context))
    return sorted(extracted, key=lambda item: _SOURCE_PRIORITY.get(item.source_kind, 99))


def _extract_from_entities(rows: Iterable[dict[str, Any]], text_surface: str) -> list[ExtractedUrl]:
    extracted: list[ExtractedUrl] = []
    for index, entity in enumerate(rows):
        if not isinstance(entity, dict):
            continue
        entity_type = _entity_type_name(entity)
        observed_url = None
        if entity_type == "textEntityTypeTextUrl":
            observed_url = _text_url_entity_url(entity)
        elif entity_type == "textEntityTypeUrl":
            observed_url = _entity_text_slice(text_surface, entity)
        normalized = _normalize_observed_url(observed_url)
        if normalized is None:
            continue
        extracted.append(
            ExtractedUrl(
                observed_url=normalized,
                source_kind="entity",
                context_path=f"entities_json[{index}]",
            )
        )
    return extracted


def _extract_preview_from_raw(raw_message_json: dict[str, Any]) -> list[ExtractedUrl]:
    content = raw_message_json.get("content")
    if not isinstance(content, dict):
        return []
    preview = content.get("link_preview")
    if not isinstance(preview, dict):
        return []
    urls: list[ExtractedUrl] = []
    for value in _walk_preview_values(preview):
        observed_url = _normalize_observed_url(value)
        if observed_url is not None:
            urls.append(ExtractedUrl(observed_url=observed_url, source_kind="preview", context_path="raw_message_json"))
    return urls


def _walk_preview_values(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for nested in value.values():
            yield from _walk_preview_values(nested)
        return
    if isinstance(value, list):
        for nested in value:
            yield from _walk_preview_values(nested)


def _entity_type_name(entity: dict[str, Any]) -> str | None:
    entity_type = entity.get("type")
    if isinstance(entity_type, dict):
        raw = entity_type.get("@type")
        if isinstance(raw, str):
            return raw
    return None


def _text_url_entity_url(entity: dict[str, Any]) -> str | None:
    entity_type = entity.get("type")
    if isinstance(entity_type, dict):
        value = entity_type.get("url")
        if isinstance(value, str):
            return value
    return None


def _entity_text_slice(text_surface: str, entity: dict[str, Any]) -> str | None:
    offset = entity.get("offset")
    length = entity.get("length")
    if not isinstance(offset, int) or not isinstance(length, int):
        return None
    if offset < 0 or length <= 0:
        return None
    return text_surface[offset : offset + length]


def _extract_regex(text: str) -> list[ExtractedUrl]:
    urls: list[ExtractedUrl] = []
    for match in _URL_REGEX.findall(text or ""):
        observed_url = _normalize_observed_url(match.rstrip(".,;:!?"))
        if observed_url is not None:
            urls.append(ExtractedUrl(observed_url=observed_url, source_kind="regex"))
    return urls


def _dedupe_by_url(candidates: list[ExtractedUrl]) -> list[ExtractedUrl]:
    selected: dict[str, ExtractedUrl] = {}
    for candidate in sorted(candidates, key=lambda item: _SOURCE_PRIORITY.get(item.source_kind, 99)):
        key = candidate.observed_url
        if key not in selected:
            selected[key] = candidate
    return list(selected.values())


def _normalize_observed_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = unicodedata.normalize("NFKC", value).strip()
    if not normalized.lower().startswith(("http://", "https://")):
        return None
    return normalized


def _coerce_source_kind(value: Any) -> str:
    if isinstance(value, str) and value.strip() in {"entity", "preview", "regex"}:
        return value.strip()
    return "entity"


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
