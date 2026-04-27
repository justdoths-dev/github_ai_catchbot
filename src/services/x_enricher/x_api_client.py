from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .models import XApiRequestProfile


class XApiClientError(Exception):
    pass


class XRateLimitedError(XApiClientError):
    pass


class XAccessDeniedError(XApiClientError):
    pass


class XNotFoundError(XApiClientError):
    pass


class XApiClient:
    def __init__(
        self,
        *,
        api_base_url: str,
        bearer_token: str,
        timeout_sec: float,
        request_max_ids: int = 100,
    ) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._bearer_token = bearer_token
        self._timeout_sec = timeout_sec
        self._request_max_ids = request_max_ids

    def default_request_profile(self) -> XApiRequestProfile:
        return XApiRequestProfile(
            tweet_fields=(
                "author_id",
                "attachments",
                "conversation_id",
                "created_at",
                "edit_history_tweet_ids",
                "entities",
                "lang",
                "note_tweet",
                "possibly_sensitive",
                "public_metrics",
                "referenced_tweets",
            ),
            expansions=(
                "author_id",
                "attachments.media_keys",
                "referenced_tweets.id",
                "referenced_tweets.id.author_id",
                "referenced_tweets.id.attachments.media_keys",
                "edit_history_tweet_ids",
            ),
            user_fields=("id", "username", "name", "verified", "created_at", "public_metrics"),
            media_fields=(
                "media_key",
                "type",
                "preview_image_url",
                "url",
                "alt_text",
                "duration_ms",
                "width",
                "height",
                "public_metrics",
            ),
        )

    async def get_posts_by_ids(self, *, post_ids: list[str], profile: XApiRequestProfile) -> dict[str, Any]:
        if not post_ids:
            raise ValueError("post_ids must not be empty")
        if len(post_ids) > self._request_max_ids:
            raise ValueError(f"post_ids exceeds max {self._request_max_ids}")
        query = urllib.parse.urlencode(
            {
                "ids": ",".join(post_ids),
                "tweet.fields": ",".join(profile.tweet_fields),
                "expansions": ",".join(profile.expansions),
                "user.fields": ",".join(profile.user_fields),
                "media.fields": ",".join(profile.media_fields),
            }
        )
        return await asyncio.to_thread(self._get_json_sync, f"/2/tweets?{query}")

    async def close(self) -> None:
        return None

    def _get_json_sync(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self._api_base_url}{path}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._bearer_token}",
                "User-Agent": "github-ai-catchbot-x-enricher",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
                body = response.read().decode("utf-8")
                payload = json.loads(body) if body else {}
                return payload if isinstance(payload, dict) else {"data": payload}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 404:
                raise XNotFoundError(body) from exc
            if exc.code == 429:
                raise XRateLimitedError(body) from exc
            if exc.code in {401, 403}:
                if "rate limit" in body.lower() or exc.headers.get("x-rate-limit-remaining") == "0":
                    raise XRateLimitedError(body) from exc
                raise XAccessDeniedError(body) from exc
            raise XApiClientError(body) from exc
        except urllib.error.URLError as exc:
            raise XApiClientError(str(exc)) from exc
