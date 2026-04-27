from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol


class TokenProvider(Protocol):
    async def get_token(self) -> str: ...


class GitHubClientError(Exception):
    pass


class GitHubRateLimitedError(GitHubClientError):
    pass


class GitHubAccessDeniedError(GitHubClientError):
    pass


class GitHubNotFoundError(GitHubClientError):
    pass


class GitHubClient:
    def __init__(
        self,
        *,
        api_base_url: str,
        timeout_sec: float,
        token_provider: TokenProvider | None = None,
    ) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._token_provider = token_provider

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> dict[str, Any]:
        return await self._get_json(f"/repos/{owner}/{repo}", auth_mode=auth_mode)

    async def get_tree(self, owner: str, repo: str, ref: str, *, recursive: bool, auth_mode: str) -> dict[str, Any]:
        suffix = "?recursive=1" if recursive else ""
        return await self._get_json(f"/repos/{owner}/{repo}/git/trees/{ref}{suffix}", auth_mode=auth_mode)

    async def get_contents(
        self,
        owner: str,
        repo: str,
        path: str,
        *,
        ref: str | None,
        auth_mode: str,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        quoted_path = urllib.parse.quote(path)
        ref_suffix = f"?ref={urllib.parse.quote(ref)}" if ref else ""
        return await self._get_json(f"/repos/{owner}/{repo}/contents/{quoted_path}{ref_suffix}", auth_mode=auth_mode)

    async def get_releases(self, owner: str, repo: str, *, auth_mode: str) -> list[dict[str, Any]]:
        payload = await self._get_json(f"/repos/{owner}/{repo}/releases", auth_mode=auth_mode)
        return payload if isinstance(payload, list) else []

    async def get_default_branch_head(self, owner: str, repo: str, default_branch: str, *, auth_mode: str) -> dict[str, Any]:
        return await self._get_json(f"/repos/{owner}/{repo}/commits/{default_branch}", auth_mode=auth_mode)

    async def get_gist(self, gist_id: str, *, auth_mode: str) -> dict[str, Any]:
        return await self._get_json(f"/gists/{gist_id}", auth_mode=auth_mode)

    async def _get_json(self, path: str, *, auth_mode: str) -> Any:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "github-ai-catchbot-gh-enricher",
        }
        if auth_mode == "app_installation" and self._token_provider is not None:
            headers["Authorization"] = f"Bearer {await self._token_provider.get_token()}"

        return await asyncio.to_thread(self._get_json_sync, path, headers)

    def _get_json_sync(self, path: str, headers: dict[str, str]) -> Any:
        request = urllib.request.Request(f"{self._api_base_url}{path}", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 404:
                raise GitHubNotFoundError(body) from exc
            if exc.code in {401, 403}:
                if "rate limit" in body.lower() or exc.headers.get("x-ratelimit-remaining") == "0":
                    raise GitHubRateLimitedError(body) from exc
                raise GitHubAccessDeniedError(body) from exc
            if exc.code >= 500:
                raise GitHubClientError(body) from exc
            raise GitHubClientError(body) from exc
        except urllib.error.URLError as exc:
            raise GitHubClientError(str(exc)) from exc
