from __future__ import annotations

import asyncio
import json
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol


CANARY_NAME = "restricted_x_read_canary"
DEFAULT_X_API_BASE_URL = "https://api.x.com"
ALLOWED_X_API_BASE_URLS = frozenset({"https://api.x.com", "https://api.twitter.com"})
DEFAULT_MAX_REQUESTS = 3
DEFAULT_TIMEOUT_MS = 8000
MAX_RESPONSE_BYTES = 262_144
POST_ID_RE = re.compile(r"^[0-9]+$")
BLOCKED_ERROR_CODES = frozenset(
    {
        "operator_approval_missing",
        "network_not_allowed",
        "post_id_missing",
        "post_id_invalid",
        "credential_missing",
        "base_url_not_allowed",
        "request_cap_invalid",
    }
)
ERROR_CODES = BLOCKED_ERROR_CODES | frozenset(
    {
        "request_cap_exceeded",
        "x_rate_limited",
        "x_access_denied",
        "x_not_found",
        "x_transient_error",
        "x_response_invalid",
    }
)
SIDE_EFFECT_FLAGS = {
    "database_write": False,
    "redis_write": False,
    "outbox_emit": False,
    "artifact_snapshot_write": False,
    "telegram_call": False,
    "openai_call": False,
    "worker_started": False,
    "repo_state_mutation": False,
}


class RestrictedXReadHttpClientProtocol(Protocol):
    async def get_post_by_id(
        self,
        *,
        base_url: str,
        bearer_token: str,
        post_id: str,
        timeout_ms: int,
    ) -> "RestrictedXReadHttpResponse": ...


class RedactedXCanaryError(RuntimeError):
    def __init__(
        self,
        error_code: str,
        *,
        observed_post_count: int | None = None,
        observed_root_post_found: bool | None = None,
        includes_present: bool | None = None,
        errors_count: int | None = None,
    ) -> None:
        safe_code = error_code if error_code in ERROR_CODES else "x_transient_error"
        self.error_code = safe_code
        self.observed_post_count = observed_post_count
        self.observed_root_post_found = observed_root_post_found
        self.includes_present = includes_present
        self.errors_count = errors_count
        super().__init__(safe_code)


class RestrictedXReadNetworkError(RuntimeError):
    pass


@dataclass(slots=True)
class RestrictedXReadCanaryRequestBudget:
    max_requests: int = DEFAULT_MAX_REQUESTS
    request_count: int = 0
    network_attempted: bool = False

    def acquire(self) -> None:
        if self.request_count >= self.max_requests:
            raise RedactedXCanaryError("request_cap_exceeded")
        self.request_count += 1
        self.network_attempted = True


@dataclass(slots=True, frozen=True)
class RestrictedXReadCanaryConfig:
    post_id: str | None
    operator_approved: bool
    allow_network: bool
    bearer_token: str | None
    x_base_url: str = DEFAULT_X_API_BASE_URL
    max_requests: int = DEFAULT_MAX_REQUESTS
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    mode: str = "restricted_live_read"


@dataclass(slots=True, frozen=True)
class RestrictedXReadHttpResponse:
    status_code: int
    content_type: str | None = None
    payload: Mapping[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class RestrictedXReadCanaryResult:
    canary_name: str
    mode: str
    post_id: str
    network_attempted: bool
    request_count: int
    max_requests: int
    status: str
    ok: bool
    error_code: str | None
    status_code_class: str | None
    content_type: str | None
    observed_post_count: int | None
    observed_root_post_found: bool | None
    includes_present: bool | None
    errors_count: int | None
    redactions_applied: tuple[str, ...]
    side_effects: Mapping[str, bool] = field(default_factory=dict)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "canary_name": self.canary_name,
            "mode": self.mode,
            "post_id": self.post_id,
            "network_attempted": self.network_attempted,
            "request_count": self.request_count,
            "max_requests": self.max_requests,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "status_code_class": self.status_code_class,
            "content_type": self.content_type,
            "observed_post_count": self.observed_post_count,
            "observed_root_post_found": self.observed_root_post_found,
            "includes_present": self.includes_present,
            "errors_count": self.errors_count,
            "redactions_applied": list(self.redactions_applied),
            "side_effects": dict(self.side_effects),
        }


@dataclass(slots=True, frozen=True)
class _PayloadObservations:
    observed_post_count: int
    observed_root_post_found: bool
    includes_present: bool
    errors_count: int


class RestrictedXReadHttpClient:
    async def get_post_by_id(
        self,
        *,
        base_url: str,
        bearer_token: str,
        post_id: str,
        timeout_ms: int,
    ) -> RestrictedXReadHttpResponse:
        return await asyncio.to_thread(
            self._get_post_by_id_sync,
            base_url=base_url,
            bearer_token=bearer_token,
            post_id=post_id,
            timeout_ms=timeout_ms,
        )

    def _get_post_by_id_sync(
        self,
        *,
        base_url: str,
        bearer_token: str,
        post_id: str,
        timeout_ms: int,
    ) -> RestrictedXReadHttpResponse:
        query = urllib.parse.urlencode({"ids": post_id})
        request = urllib.request.Request(
            f"{base_url}/2/tweets?{query}",
            method="GET",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {bearer_token}",
                "User-Agent": "github-ai-catchbot-restricted-x-read-canary/0.1",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=_timeout_sec(timeout_ms)) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
                return RestrictedXReadHttpResponse(
                    status_code=int(response.status),
                    content_type=_normalize_content_type(response.headers.get("content-type")),
                    payload=_json_object_or_none(body),
                )
        except urllib.error.HTTPError as exc:
            body = exc.read(MAX_RESPONSE_BYTES + 1)
            return RestrictedXReadHttpResponse(
                status_code=int(exc.code),
                content_type=_normalize_content_type(exc.headers.get("content-type")),
                payload=_json_object_or_none(body),
            )
        except (
            urllib.error.URLError,
            TimeoutError,
            socket.timeout,
            ConnectionError,
            ConnectionResetError,
        ) as exc:
            raise RestrictedXReadNetworkError("x_transient_error") from exc


class RestrictedXReadCanary:
    def __init__(
        self,
        config: RestrictedXReadCanaryConfig,
        *,
        client: RestrictedXReadHttpClientProtocol,
        request_budget: RestrictedXReadCanaryRequestBudget | None = None,
    ) -> None:
        self._config = config
        self._request_budget = request_budget or RestrictedXReadCanaryRequestBudget(
            max_requests=_request_cap_or_zero(config.max_requests)
        )
        self._client = client

    async def run(self) -> RestrictedXReadCanaryResult:
        safe_post_id = normalize_post_id(self._config.post_id) or ""
        error_code: str | None = None
        status_code_class: str | None = None
        content_type: str | None = None
        observed_post_count: int | None = None
        observed_root_post_found: bool | None = None
        includes_present: bool | None = None
        errors_count: int | None = None

        try:
            target = self._validate_preconditions()
            safe_post_id = target["post_id"]
            self._request_budget.acquire()
            response = await self._client.get_post_by_id(
                base_url=target["x_base_url"],
                bearer_token=target["bearer_token"],
                post_id=target["post_id"],
                timeout_ms=_positive_int(self._config.timeout_ms, DEFAULT_TIMEOUT_MS),
            )
            status_code = _safe_status_code(response.status_code)
            status_code_class = _status_code_class(status_code)
            content_type = _normalize_content_type(response.content_type)
            _raise_for_status(status_code)
            if content_type != "application/json":
                raise RedactedXCanaryError("x_response_invalid")
            observations = _observe_payload(response.payload, target["post_id"])
            observed_post_count = observations.observed_post_count
            observed_root_post_found = observations.observed_root_post_found
            includes_present = observations.includes_present
            errors_count = observations.errors_count
        except RedactedXCanaryError as exc:
            error_code = exc.error_code
            observed_post_count = _coalesce(observed_post_count, exc.observed_post_count)
            observed_root_post_found = _coalesce(observed_root_post_found, exc.observed_root_post_found)
            includes_present = _coalesce(includes_present, exc.includes_present)
            errors_count = _coalesce(errors_count, exc.errors_count)
        except (RestrictedXReadNetworkError, TimeoutError, ConnectionError, ConnectionResetError):
            error_code = "x_transient_error"
        except Exception:  # noqa: BLE001 - operator output must stay bucketed.
            error_code = "x_transient_error"

        return self._result(
            post_id=safe_post_id,
            error_code=error_code,
            status_code_class=status_code_class,
            content_type=content_type,
            observed_post_count=observed_post_count,
            observed_root_post_found=observed_root_post_found,
            includes_present=includes_present,
            errors_count=errors_count,
        )

    def _validate_preconditions(self) -> dict[str, str]:
        if not self._config.operator_approved:
            raise RedactedXCanaryError("operator_approval_missing")
        if not self._config.allow_network:
            raise RedactedXCanaryError("network_not_allowed")
        post_id = normalize_post_id(self._config.post_id)
        if (self._config.post_id or "").strip() == "":
            raise RedactedXCanaryError("post_id_missing")
        if post_id is None:
            raise RedactedXCanaryError("post_id_invalid")
        bearer_token = (self._config.bearer_token or "").strip()
        if not bearer_token:
            raise RedactedXCanaryError("credential_missing")
        x_base_url = normalize_x_api_base_url(self._config.x_base_url)
        if x_base_url is None:
            raise RedactedXCanaryError("base_url_not_allowed")
        if not _valid_request_cap(self._config.max_requests):
            raise RedactedXCanaryError("request_cap_invalid")
        return {"post_id": post_id, "bearer_token": bearer_token, "x_base_url": x_base_url}

    def _result(
        self,
        *,
        post_id: str,
        error_code: str | None,
        status_code_class: str | None,
        content_type: str | None,
        observed_post_count: int | None,
        observed_root_post_found: bool | None,
        includes_present: bool | None,
        errors_count: int | None,
    ) -> RestrictedXReadCanaryResult:
        return RestrictedXReadCanaryResult(
            canary_name=CANARY_NAME,
            mode="restricted_live_read",
            post_id=post_id if normalize_post_id(post_id) is not None else "",
            network_attempted=self._request_budget.network_attempted,
            request_count=self._request_budget.request_count,
            max_requests=self._request_budget.max_requests,
            status=_result_status(error_code, self._request_budget.network_attempted),
            ok=error_code is None,
            error_code=error_code,
            status_code_class=status_code_class,
            content_type=content_type,
            observed_post_count=observed_post_count,
            observed_root_post_found=observed_root_post_found,
            includes_present=includes_present,
            errors_count=errors_count,
            redactions_applied=_redactions(error_code),
            side_effects=SIDE_EFFECT_FLAGS,
        )


async def run_restricted_x_read_canary(
    config: RestrictedXReadCanaryConfig,
    *,
    client: RestrictedXReadHttpClientProtocol | None = None,
    request_budget: RestrictedXReadCanaryRequestBudget | None = None,
) -> RestrictedXReadCanaryResult:
    return await RestrictedXReadCanary(
        config,
        client=client or RestrictedXReadHttpClient(),
        request_budget=request_budget,
    ).run()


def normalize_post_id(value: str | None) -> str | None:
    post_id = (value or "").strip()
    if not post_id or POST_ID_RE.fullmatch(post_id) is None:
        return None
    return post_id


def normalize_x_api_base_url(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        parsed = urllib.parse.urlsplit(raw)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme.lower() != "https":
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    if parsed.query or parsed.fragment:
        return None
    path = parsed.path.rstrip("/")
    if path:
        return None
    host = (parsed.hostname or "").rstrip(".").lower()
    if port is not None:
        return None
    normalized = f"https://{host}"
    return normalized if normalized in ALLOWED_X_API_BASE_URLS else None


def _observe_payload(payload: Mapping[str, Any] | None, requested_post_id: str) -> _PayloadObservations:
    if not isinstance(payload, Mapping):
        raise RedactedXCanaryError("x_response_invalid")
    data = payload.get("data", [])
    if data is None:
        data = []
    errors = payload.get("errors", [])
    if errors is None:
        errors = []
    includes = payload.get("includes")
    if not isinstance(data, list) or not isinstance(errors, list):
        raise RedactedXCanaryError("x_response_invalid")
    for item in data:
        if not isinstance(item, Mapping):
            raise RedactedXCanaryError("x_response_invalid")
    observed_post_count = len(data)
    includes_present = isinstance(includes, Mapping) and bool(includes)
    errors_count = len(errors)
    observed_root_post_found = any(item.get("id") == requested_post_id for item in data)
    if not observed_root_post_found:
        raise RedactedXCanaryError(
            "x_not_found",
            observed_post_count=observed_post_count,
            observed_root_post_found=False,
            includes_present=includes_present,
            errors_count=errors_count,
        )
    return _PayloadObservations(
        observed_post_count=observed_post_count,
        observed_root_post_found=True,
        includes_present=includes_present,
        errors_count=errors_count,
    )


def _raise_for_status(status_code: int) -> None:
    if status_code == 429:
        raise RedactedXCanaryError("x_rate_limited")
    if status_code in {401, 403}:
        raise RedactedXCanaryError("x_access_denied")
    if status_code == 404:
        raise RedactedXCanaryError("x_not_found")
    if status_code >= 500:
        raise RedactedXCanaryError("x_transient_error")
    if status_code < 200 or status_code >= 300:
        raise RedactedXCanaryError("x_response_invalid")


def _safe_status_code(value: Any) -> int:
    try:
        status_code = int(value)
    except (TypeError, ValueError) as exc:
        raise RedactedXCanaryError("x_response_invalid") from exc
    if status_code < 100 or status_code > 599:
        raise RedactedXCanaryError("x_response_invalid")
    return status_code


def _status_code_class(status_code: int) -> str:
    return f"{status_code // 100}xx"


def _normalize_content_type(value: str | None) -> str | None:
    raw = (value or "").strip().lower()
    if not raw:
        return None
    return raw.split(";", 1)[0].strip() or None


def _json_object_or_none(body: bytes) -> Mapping[str, Any] | None:
    try:
        payload = json.loads(body.decode("utf-8")) if body else {}
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None


def _valid_request_cap(value: Any) -> bool:
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _request_cap_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _positive_int(value: Any, default: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return number if number > 0 else default


def _timeout_sec(timeout_ms: int) -> float:
    return _positive_int(timeout_ms, DEFAULT_TIMEOUT_MS) / 1000.0


def _result_status(error_code: str | None, network_attempted: bool) -> str:
    if error_code is None:
        return "pass"
    if not network_attempted and error_code in (BLOCKED_ERROR_CODES | {"request_cap_exceeded"}):
        return "blocked"
    return "fail"


def _redactions(error_code: str | None) -> tuple[str, ...]:
    redactions = [
        "bearer_token_omitted",
        "authorization_header_omitted",
        "raw_response_body_omitted",
        "response_headers_omitted",
        "cookies_omitted",
        "post_text_omitted",
        "author_identity_omitted",
        "media_urls_omitted",
    ]
    if error_code is not None:
        redactions.append("exception_detail_omitted")
    return tuple(redactions)


def _coalesce(first: Any, second: Any) -> Any:
    return first if first is not None else second
