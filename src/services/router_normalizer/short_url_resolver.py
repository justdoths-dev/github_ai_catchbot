from __future__ import annotations

import asyncio
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .models import ExtractedUrl, ResolvedUrl


@dataclass(slots=True, frozen=True)
class ShortUrlResolver:
    allowlist: tuple[str, ...]
    hop_limit: int
    timeout_seconds: float

    async def resolve(self, url: ExtractedUrl) -> ResolvedUrl:
        normalized = _strip_fragment(url.observed_url)
        host = _host(normalized)
        if host not in self.allowlist:
            return ResolvedUrl(
                observed_url=url.observed_url,
                normalized_url=normalized,
                resolved_url=None,
                source_kind=url.source_kind,
                context_path=url.context_path,
            )
        try:
            resolved = await asyncio.to_thread(
                _resolve_sync,
                normalized,
                self.hop_limit,
                self.timeout_seconds,
                self.allowlist,
            )
        except Exception:
            return ResolvedUrl(
                observed_url=url.observed_url,
                normalized_url=normalized,
                resolved_url=None,
                source_kind=url.source_kind,
                context_path=url.context_path,
                resolution_status="short_url_unresolved",
            )
        return ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=normalized,
            resolved_url=resolved,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status="short_url_resolved",
        )


def _resolve_sync(url: str, hop_limit: int, timeout_seconds: float, allowlist: tuple[str, ...]) -> str:
    current = url
    for _ in range(hop_limit):
        request = Request(current, method="HEAD", headers={"User-Agent": "github-ai-catchbot-router-normalizer/1"})
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                next_url = response.geturl()
        except HTTPError as exc:
            next_url = exc.geturl()
        except URLError:
            raise
        if not next_url or next_url == current:
            return current
        current = next_url
        if _host(current) not in allowlist:
            return current
    return current


def _strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def _host(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host
