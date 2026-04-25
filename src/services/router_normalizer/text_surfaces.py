from __future__ import annotations

import re
import unicodedata

from .models import SourceMessageSnapshot, TextSurfaces


_WHITESPACE_RE = re.compile(r"\s+")
_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff]")


def build_text_surfaces(snapshot: SourceMessageSnapshot) -> TextSurfaces:
    raw = _first_text(
        snapshot.text_surface,
        _join_text(snapshot.text_body, snapshot.caption_text),
    )
    display = _normalize_display(raw)
    keyword_scan = _normalize_keyword_scan(display)
    hash_surface = _normalize_hash_surface(display)
    return TextSurfaces(
        raw_text_surface=raw,
        keyword_scan_surface=keyword_scan,
        hash_surface=hash_surface,
        display_surface=display,
    )


def _join_text(*parts: str | None) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    return "\n\n".join(cleaned)


def _first_text(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return unicodedata.normalize("NFKC", value).replace("\r\n", "\n").replace("\r", "\n").strip()
    return ""


def _normalize_display(value: str) -> str:
    value = _ZERO_WIDTH_RE.sub("", value)
    lines = [_WHITESPACE_RE.sub(" ", line).strip() for line in value.split("\n")]
    display = "\n".join(line for line in lines if line)
    return display.strip()


def _normalize_keyword_scan(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def _normalize_hash_surface(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return _WHITESPACE_RE.sub(" ", normalized).strip()
