from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin


_WS_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://[^\s<>()\[\]{}\"']+")
_VISIBLE_TEXT_TAGS = {"article", "main", "p", "h1", "h2", "h3", "li"}


@dataclass(slots=True, frozen=True)
class ParsedArticle:
    canonical_url_candidate: str | None
    site_name: str | None
    title: str | None
    description: str | None
    author: str | None
    published_at: datetime | None
    main_text_excerpt: str | None
    outbound_links: list[str]
    normalized_projection: dict[str, Any]


class ArticleParser:
    def __init__(self, *, excerpt_chars: int, max_outbound_links: int) -> None:
        self._excerpt_chars = excerpt_chars
        self._max_outbound_links = max_outbound_links

    def parse(self, *, final_url: str, content_type: str | None, body_text: str) -> ParsedArticle:
        if content_type in {"text/plain", "text/markdown"}:
            return self._parse_plain(body_text)

        parser = _ArticleHtmlParser(
            base_url=final_url,
            excerpt_chars=self._excerpt_chars,
            max_outbound_links=self._max_outbound_links,
        )
        parser.feed(body_text)
        return parser.build()

    def _parse_plain(self, body_text: str) -> ParsedArticle:
        clean = _clean_text(body_text)
        title = None
        for line in body_text.splitlines():
            line = line.strip()
            if line:
                title = line[:120]
                break
        outbound_links = _dedupe(_URL_RE.findall(body_text), limit=self._max_outbound_links)
        return ParsedArticle(
            canonical_url_candidate=None,
            site_name=None,
            title=title,
            description=None,
            author=None,
            published_at=None,
            main_text_excerpt=clean[: self._excerpt_chars] if clean else None,
            outbound_links=outbound_links,
            normalized_projection={"plain_text_mode": True},
        )


class _ArticleHtmlParser(HTMLParser):
    def __init__(self, *, base_url: str, excerpt_chars: int, max_outbound_links: int) -> None:
        super().__init__(convert_charrefs=True)
        self._base_url = base_url
        self._excerpt_chars = excerpt_chars
        self._max_outbound_links = max_outbound_links
        self._tag_stack: list[str] = []
        self._skip_depth = 0
        self._title_chunks: list[str] = []
        self._text_chunks: list[str] = []
        self._outbound_links: list[str] = []
        self._meta: dict[str, str] = {}
        self._canonical_url_candidate: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        self._tag_stack.append(tag)
        attrs_map = {key.lower(): (value or "") for key, value in attrs}
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if tag == "meta":
            key = (attrs_map.get("property") or attrs_map.get("name") or attrs_map.get("itemprop") or "").lower()
            value = attrs_map.get("content", "").strip()
            if key and value and key not in self._meta:
                self._meta[key] = value
        if tag == "link":
            rel = attrs_map.get("rel", "").lower()
            href = attrs_map.get("href", "").strip()
            if "canonical" in rel and href and self._canonical_url_candidate is None:
                self._canonical_url_candidate = urljoin(self._base_url, href)
        if tag == "a":
            href = attrs_map.get("href", "").strip()
            if href and len(self._outbound_links) < self._max_outbound_links:
                absolute = urljoin(self._base_url, href)
                if absolute.startswith(("http://", "https://")):
                    self._outbound_links.append(absolute)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = _clean_text(data)
        if not text:
            return
        current_tag = self._tag_stack[-1] if self._tag_stack else ""
        if current_tag == "title":
            self._title_chunks.append(text)
        elif current_tag in _VISIBLE_TEXT_TAGS or any(tag in _VISIBLE_TEXT_TAGS for tag in self._tag_stack):
            self._text_chunks.append(text)

    def build(self) -> ParsedArticle:
        title = _first_non_empty(
            " ".join(self._title_chunks),
            self._meta.get("og:title"),
            self._meta.get("twitter:title"),
        )
        description = _first_non_empty(
            self._meta.get("description"),
            self._meta.get("og:description"),
            self._meta.get("twitter:description"),
        )
        author = _first_non_empty(
            self._meta.get("author"),
            self._meta.get("article:author"),
            self._meta.get("parsely-author"),
        )
        published_at = _parse_datetime(
            _first_non_empty(
                self._meta.get("article:published_time"),
                self._meta.get("parsely-pub-date"),
                self._meta.get("pubdate"),
                self._meta.get("date"),
            )
        )
        excerpt = _clean_text(" ".join(self._text_chunks))
        return ParsedArticle(
            canonical_url_candidate=self._canonical_url_candidate,
            site_name=_first_non_empty(self._meta.get("og:site_name"), self._meta.get("application-name")),
            title=title,
            description=description,
            author=author,
            published_at=published_at,
            main_text_excerpt=excerpt[: self._excerpt_chars] if excerpt else None,
            outbound_links=_dedupe(self._outbound_links, limit=self._max_outbound_links),
            normalized_projection={"meta": self._meta, "title_chunks": self._title_chunks},
        )


def _clean_text(value: str) -> str:
    return _WS_RE.sub(" ", value).strip()


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        cleaned = _clean_text(value or "")
        if cleaned:
            return cleaned
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _dedupe(values: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.rstrip(".,;)")
        if cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
        if len(result) >= limit:
            break
    return result
