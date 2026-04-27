from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any


class GitHubAppAuthError(RuntimeError):
    pass


@dataclass(slots=True)
class GitHubInstallationToken:
    token: str
    expires_at_epoch: int


class GitHubAppTokenProvider:
    def __init__(
        self,
        *,
        app_id: str,
        installation_id: str,
        private_key_pem: str,
        api_base_url: str,
        timeout_sec: float,
    ) -> None:
        self._app_id = app_id
        self._installation_id = installation_id
        self._private_key_pem = private_key_pem
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._cached: GitHubInstallationToken | None = None

    async def get_token(self) -> str:
        now = int(time.time())
        if self._cached is not None and now < self._cached.expires_at_epoch - 60:
            return self._cached.token

        app_jwt = self._build_app_jwt(now)
        payload = await asyncio.to_thread(self._request_installation_token, app_jwt)
        token = str(payload["token"])
        expires_at_epoch = self._iso_to_epoch(payload.get("expires_at"))
        self._cached = GitHubInstallationToken(token=token, expires_at_epoch=expires_at_epoch)
        return token

    def _build_app_jwt(self, now: int) -> str:
        try:
            import jwt  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:  # pragma: no cover - environment dependent
            raise GitHubAppAuthError("PyJWT is required for GitHub App authentication") from exc

        payload = {"iat": now - 60, "exp": now + 540, "iss": self._app_id}
        encoded = jwt.encode(payload, self._private_key_pem, algorithm="RS256")
        return encoded if isinstance(encoded, str) else encoded.decode("utf-8")

    def _request_installation_token(self, app_jwt: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self._api_base_url}/app/installations/{self._installation_id}/access_tokens",
            method="POST",
            headers={
                "Authorization": f"Bearer {app_jwt}",
                "Accept": "application/vnd.github+json",
                "User-Agent": "github-ai-catchbot-gh-enricher",
            },
        )
        with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
            raw = response.read().decode("utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise GitHubAppAuthError("GitHub installation token response was not an object")
        return parsed

    @staticmethod
    def _iso_to_epoch(value: Any) -> int:
        if not isinstance(value, str) or not value:
            return int(time.time()) + 300
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
