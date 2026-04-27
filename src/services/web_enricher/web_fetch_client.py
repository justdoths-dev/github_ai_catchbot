from __future__ import annotations

import asyncio
import hashlib
import socket
import urllib.error
import urllib.parse
import urllib.request

from .models import FetchedDocument


class WebFetchClientError(Exception):
    pass


class WebRateLimitedError(WebFetchClientError):
    pass


class WebAccessDeniedError(WebFetchClientError):
    pass


class WebNotFoundError(WebFetchClientError):
    pass


class WebPermanentFetchError(WebFetchClientError):
    pass


class WebTransientFetchError(WebFetchClientError):
    pass


class UnsupportedContentTypeError(WebFetchClientError):
    pass


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def normalize_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    return content_type.split(";", 1)[0].strip().lower() or None


def ensure_supported_content_type(content_type: str | None, allowlist: tuple[str, ...]) -> str | None:
    normalized = normalize_content_type(content_type)
    if normalized is not None and normalized not in allowlist:
        raise UnsupportedContentTypeError(normalized)
    return normalized


class WebFetchClient:
    def __init__(
        self,
        *,
        timeout_sec: float,
        max_redirects: int,
        max_bytes: int,
        user_agent: str,
        content_type_allowlist: tuple[str, ...],
    ) -> None:
        self._timeout_sec = timeout_sec
        self._max_redirects = max_redirects
        self._max_bytes = max_bytes
        self._user_agent = user_agent
        self._content_type_allowlist = content_type_allowlist
        self._opener = urllib.request.build_opener(_NoRedirectHandler)

    async def fetch(self, url: str) -> FetchedDocument:
        return await asyncio.to_thread(self._fetch_sync, url)

    async def close(self) -> None:
        return None

    def _fetch_sync(self, url: str) -> FetchedDocument:
        current_url = url
        anomalies: list[str] = []
        for hop in range(self._max_redirects + 1):
            request = urllib.request.Request(
                current_url,
                method="GET",
                headers={
                    "User-Agent": self._user_agent,
                    "Accept": "text/html,application/xhtml+xml,text/plain,text/markdown;q=0.9,*/*;q=0.1",
                },
            )
            try:
                with self._opener.open(request, timeout=self._timeout_sec) as response:
                    content_type = ensure_supported_content_type(
                        response.headers.get("content-type"),
                        self._content_type_allowlist,
                    )
                    raw = response.read(self._max_bytes + 1)
                    if len(raw) > self._max_bytes:
                        raw = raw[: self._max_bytes]
                        anomalies.append("body_truncated_at_max_bytes")
                    encoding = response.headers.get_content_charset() or "utf-8"
                    body_text = raw.decode(encoding, errors="replace")
                    return FetchedDocument(
                        requested_url=url,
                        final_url=response.geturl(),
                        status_code=int(response.status),
                        content_type=content_type,
                        body_bytes=raw,
                        body_text=body_text,
                        response_headers_subset=_headers_subset(response.headers),
                        content_hash=hashlib.sha256(raw).hexdigest(),
                        fetch_anomalies=anomalies,
                    )
            except urllib.error.HTTPError as exc:
                if exc.code in {301, 302, 303, 307, 308}:
                    if hop >= self._max_redirects:
                        raise WebTransientFetchError("redirect_hop_cap_exceeded") from exc
                    location = exc.headers.get("location")
                    if not location:
                        raise WebPermanentFetchError("redirect_without_location") from exc
                    current_url = urllib.parse.urljoin(current_url, location)
                    continue
                if exc.code == 429:
                    raise WebRateLimitedError("rate_limited") from exc
                if exc.code in {401, 403}:
                    raise WebAccessDeniedError("access_denied") from exc
                if exc.code == 404:
                    raise WebNotFoundError("not_found") from exc
                if exc.code >= 500:
                    raise WebTransientFetchError(f"http_{exc.code}") from exc
                raise WebPermanentFetchError(f"http_{exc.code}") from exc
            except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
                raise WebTransientFetchError(str(exc)) from exc
        raise WebTransientFetchError("redirect_hop_cap_exceeded")


def _headers_subset(headers) -> dict[str, str]:  # type: ignore[no-untyped-def]
    keys = ("content-type", "etag", "last-modified", "cache-control")
    return {key: headers.get(key, "") for key in keys if headers.get(key)}
