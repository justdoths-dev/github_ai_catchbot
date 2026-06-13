from __future__ import annotations

import asyncio
import ipaddress
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from .web_fetch_client import normalize_content_type


CANARY_NAME = "restricted_web_read_canary"
DEFAULT_MAX_REQUESTS = 3
DEFAULT_MAX_REDIRECTS = 3
DEFAULT_MAX_BYTES = 65_536
DEFAULT_TIMEOUT_SEC = 6.0
DEFAULT_USER_AGENT = "catchbot-restricted-web-read-canary/0.1"
DEFAULT_CONTENT_TYPE_ALLOWLIST = (
    "text/html",
    "application/xhtml+xml",
    "text/plain",
    "text/markdown",
)
REDIRECT_STATUS_CODES = frozenset({301, 302, 303, 307, 308})
BLOCKED_ERROR_CODES = frozenset(
    {
        "operator_approval_missing",
        "network_not_allowed",
        "invalid_url_target",
        "unsupported_scheme",
        "private_or_local_target",
    }
)
ERROR_CODES = BLOCKED_ERROR_CODES | frozenset(
    {
        "redirect_limit_exceeded",
        "request_cap_exceeded",
        "unsupported_content_type",
        "rate_limited",
        "access_denied",
        "not_found",
        "network_error",
        "unexpected_error",
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
LOCAL_HOSTNAMES = frozenset(
    {
        "localhost",
        "ip6-localhost",
        "ip6-loopback",
        "metadata",
        "metadata.google.internal",
    }
)
LOCAL_HOST_SUFFIXES = (
    ".localhost",
    ".local",
    ".localdomain",
    ".internal",
    ".lan",
)


class RestrictedWebReadHttpClientProtocol(Protocol):
    async def get(
        self,
        url: str,
        *,
        timeout_sec: float,
        max_bytes: int,
    ) -> "RestrictedWebReadHttpResponse": ...


class RedactedWebCanaryError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        safe_code = error_code if error_code in ERROR_CODES else "unexpected_error"
        self.error_code = safe_code
        super().__init__(safe_code)


class RestrictedWebReadNetworkError(RuntimeError):
    pass


@dataclass(slots=True)
class RestrictedWebReadCanaryRequestBudget:
    max_requests: int = DEFAULT_MAX_REQUESTS
    request_count: int = 0
    network_attempted: bool = False

    def acquire(self) -> None:
        if self.request_count >= self.max_requests:
            raise RedactedWebCanaryError("request_cap_exceeded")
        self.request_count += 1
        self.network_attempted = True


@dataclass(slots=True, frozen=True)
class RestrictedWebReadCanaryConfig:
    url: str | None
    operator_approved: bool
    allow_network: bool
    max_requests: int = DEFAULT_MAX_REQUESTS
    max_redirects: int = DEFAULT_MAX_REDIRECTS
    max_bytes: int = DEFAULT_MAX_BYTES
    timeout_sec: float = DEFAULT_TIMEOUT_SEC
    content_type_allowlist: tuple[str, ...] = DEFAULT_CONTENT_TYPE_ALLOWLIST
    mode: str = "restricted_live_read"


@dataclass(slots=True, frozen=True)
class RestrictedWebReadHttpResponse:
    status_code: int
    headers: Mapping[str, str] = field(default_factory=dict)
    body_bytes: bytes = b""


@dataclass(slots=True, frozen=True)
class _ValidatedUrlTarget:
    network_url: str
    sanitized_url: str
    host: str


@dataclass(slots=True, frozen=True)
class RestrictedWebReadCanaryResult:
    canary_name: str
    mode: str
    url: str
    final_url_host: str | None
    network_attempted: bool
    request_count: int
    redirect_count: int
    status: str
    ok: bool
    error_code: str | None
    redactions_applied: tuple[str, ...]
    content_type: str | None
    status_code_class: str | None
    body_bytes_observed: int | None
    max_requests: int
    max_redirects: int
    max_bytes: int
    side_effects: Mapping[str, bool] = field(default_factory=dict)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "canary_name": self.canary_name,
            "mode": self.mode,
            "url": self.url,
            "final_url_host": self.final_url_host,
            "network_attempted": self.network_attempted,
            "request_count": self.request_count,
            "redirect_count": self.redirect_count,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "redactions_applied": list(self.redactions_applied),
            "content_type": self.content_type,
            "status_code_class": self.status_code_class,
            "body_bytes_observed": self.body_bytes_observed,
            "max_requests": self.max_requests,
            "max_redirects": self.max_redirects,
            "max_bytes": self.max_bytes,
            "side_effects": dict(self.side_effects),
        }


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


class RestrictedWebReadHttpClient:
    def __init__(self, *, user_agent: str = DEFAULT_USER_AGENT) -> None:
        self._user_agent = user_agent
        self._opener = urllib.request.build_opener(_NoRedirectHandler)

    async def get(
        self,
        url: str,
        *,
        timeout_sec: float,
        max_bytes: int,
    ) -> RestrictedWebReadHttpResponse:
        return await asyncio.to_thread(
            self._get_sync,
            url,
            timeout_sec=timeout_sec,
            max_bytes=max_bytes,
        )

    def _get_sync(
        self,
        url: str,
        *,
        timeout_sec: float,
        max_bytes: int,
    ) -> RestrictedWebReadHttpResponse:
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "User-Agent": self._user_agent,
                "Accept": "text/html,application/xhtml+xml,text/plain,text/markdown;q=0.9,*/*;q=0.1",
            },
        )
        try:
            with self._opener.open(request, timeout=timeout_sec) as response:
                return RestrictedWebReadHttpResponse(
                    status_code=int(response.status),
                    headers=_lower_headers(response.headers),
                    body_bytes=response.read(max_bytes + 1),
                )
        except urllib.error.HTTPError as exc:
            return RestrictedWebReadHttpResponse(
                status_code=int(exc.code),
                headers=_lower_headers(exc.headers),
                body_bytes=b"",
            )
        except (
            urllib.error.URLError,
            TimeoutError,
            socket.timeout,
            ConnectionError,
            ConnectionResetError,
        ) as exc:
            raise RestrictedWebReadNetworkError("network_error") from exc


class RestrictedWebReadCanary:
    def __init__(
        self,
        config: RestrictedWebReadCanaryConfig,
        *,
        http_client: RestrictedWebReadHttpClientProtocol,
        request_budget: RestrictedWebReadCanaryRequestBudget | None = None,
    ) -> None:
        self._config = config
        self._http_client = http_client
        self._request_budget = request_budget or RestrictedWebReadCanaryRequestBudget(
            max_requests=max(0, int(config.max_requests))
        )

    async def run(self) -> RestrictedWebReadCanaryResult:
        safe_url = sanitize_url_for_output(self._config.url)
        final_url_host: str | None = None
        content_type: str | None = None
        status_code_class: str | None = None
        body_bytes_observed: int | None = None
        redirect_count = 0
        error_code: str | None = None

        try:
            target = self._validate_preconditions()
            safe_url = target.sanitized_url
            current_url = target.network_url
            final_url_host = target.host

            while True:
                self._request_budget.acquire()
                response = await self._http_client.get(
                    current_url,
                    timeout_sec=_positive_float(self._config.timeout_sec, DEFAULT_TIMEOUT_SEC),
                    max_bytes=_nonnegative_int(self._config.max_bytes),
                )
                status_code = _safe_status_code(response.status_code)
                status_code_class = _status_code_class(status_code)

                if status_code in REDIRECT_STATUS_CODES:
                    if redirect_count >= _nonnegative_int(self._config.max_redirects):
                        raise RedactedWebCanaryError("redirect_limit_exceeded")
                    location = _header_value(response.headers, "location")
                    if not location:
                        raise RedactedWebCanaryError("unexpected_error")
                    target = validate_url_target(urllib.parse.urljoin(current_url, location))
                    current_url = target.network_url
                    final_url_host = target.host
                    redirect_count += 1
                    continue

                _raise_for_status(status_code)
                content_type = normalize_content_type(_header_value(response.headers, "content-type"))
                if content_type is None or content_type not in _content_type_allowlist(
                    self._config.content_type_allowlist
                ):
                    raise RedactedWebCanaryError("unsupported_content_type")

                body = _bounded_body(response.body_bytes, _nonnegative_int(self._config.max_bytes))
                body_bytes_observed = len(body)
                break
        except RedactedWebCanaryError as exc:
            error_code = exc.error_code
        except (RestrictedWebReadNetworkError, TimeoutError, ConnectionError, ConnectionResetError):
            error_code = "network_error"
        except Exception:  # noqa: BLE001 - operator output must stay bucketed.
            error_code = "unexpected_error"

        return self._result(
            url=safe_url,
            final_url_host=final_url_host,
            error_code=error_code,
            redirect_count=redirect_count,
            content_type=content_type,
            status_code_class=status_code_class,
            body_bytes_observed=body_bytes_observed,
        )

    def _validate_preconditions(self) -> _ValidatedUrlTarget:
        if not self._config.operator_approved:
            raise RedactedWebCanaryError("operator_approval_missing")
        if not self._config.allow_network:
            raise RedactedWebCanaryError("network_not_allowed")
        return validate_url_target(self._config.url)

    def _result(
        self,
        *,
        url: str,
        final_url_host: str | None,
        error_code: str | None,
        redirect_count: int,
        content_type: str | None,
        status_code_class: str | None,
        body_bytes_observed: int | None,
    ) -> RestrictedWebReadCanaryResult:
        status = _result_status(error_code, self._request_budget.network_attempted)
        return RestrictedWebReadCanaryResult(
            canary_name=CANARY_NAME,
            mode="restricted_live_read",
            url=url,
            final_url_host=final_url_host,
            network_attempted=self._request_budget.network_attempted,
            request_count=self._request_budget.request_count,
            redirect_count=redirect_count,
            status=status,
            ok=error_code is None,
            error_code=error_code,
            redactions_applied=_redactions_for_url(self._config.url, error_code),
            content_type=content_type,
            status_code_class=status_code_class,
            body_bytes_observed=body_bytes_observed,
            max_requests=self._request_budget.max_requests,
            max_redirects=_nonnegative_int(self._config.max_redirects),
            max_bytes=_nonnegative_int(self._config.max_bytes),
            side_effects=SIDE_EFFECT_FLAGS,
        )


def validate_url_target(value: str | None) -> _ValidatedUrlTarget:
    raw = (value or "").strip()
    if not raw:
        raise RedactedWebCanaryError("invalid_url_target")
    try:
        parsed = urllib.parse.urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        raise RedactedWebCanaryError("invalid_url_target") from exc
    if not parsed.scheme:
        raise RedactedWebCanaryError("invalid_url_target")
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise RedactedWebCanaryError("unsupported_scheme")
    if parsed.username is not None or parsed.password is not None:
        raise RedactedWebCanaryError("invalid_url_target")
    host = (parsed.hostname or "").rstrip(".").lower()
    if not host:
        raise RedactedWebCanaryError("invalid_url_target")
    if _is_private_or_local_host(host):
        raise RedactedWebCanaryError("private_or_local_target")
    netloc = _netloc(host, port)
    network_url = urllib.parse.urlunsplit((scheme, netloc, parsed.path or "", parsed.query, ""))
    sanitized_url = urllib.parse.urlunsplit((scheme, netloc, parsed.path or "", "", ""))
    return _ValidatedUrlTarget(network_url=network_url, sanitized_url=sanitized_url, host=host)


def sanitize_url_for_output(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urllib.parse.urlsplit(raw)
        port = parsed.port
    except ValueError:
        return ""
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").rstrip(".").lower()
    if not scheme or not host:
        return ""
    return urllib.parse.urlunsplit((scheme, _netloc(host, port), parsed.path or "", "", ""))


def _is_private_or_local_host(host: str) -> bool:
    if host in LOCAL_HOSTNAMES or any(host.endswith(suffix) for suffix in LOCAL_HOST_SUFFIXES):
        return True
    if "." not in host and ":" not in host:
        return True
    if _looks_like_numeric_ipv4(host):
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return (
        not ip.is_global
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_private
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _looks_like_numeric_ipv4(host: str) -> bool:
    parts = host.split(".")
    if not parts:
        return False
    return all(part.isdigit() or part.lower().startswith("0x") for part in parts)


def _netloc(host: str, port: int | None) -> str:
    display_host = f"[{host}]" if ":" in host else host
    return f"{display_host}:{port}" if port is not None else display_host


def _content_type_allowlist(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(value.strip().lower() for value in values if value and value.strip())
    return normalized or DEFAULT_CONTENT_TYPE_ALLOWLIST


def _safe_status_code(value: Any) -> int:
    try:
        status_code = int(value)
    except (TypeError, ValueError) as exc:
        raise RedactedWebCanaryError("unexpected_error") from exc
    if status_code < 100 or status_code > 599:
        raise RedactedWebCanaryError("unexpected_error")
    return status_code


def _status_code_class(status_code: int) -> str:
    return f"{status_code // 100}xx"


def _raise_for_status(status_code: int) -> None:
    if status_code == 429:
        raise RedactedWebCanaryError("rate_limited")
    if status_code in {401, 403}:
        raise RedactedWebCanaryError("access_denied")
    if status_code == 404:
        raise RedactedWebCanaryError("not_found")
    if status_code >= 500:
        raise RedactedWebCanaryError("network_error")
    if status_code < 200 or status_code >= 300:
        raise RedactedWebCanaryError("unexpected_error")


def _bounded_body(value: bytes, max_bytes: int) -> bytes:
    if not isinstance(value, bytes):
        raise RedactedWebCanaryError("unexpected_error")
    return value[:max_bytes]


def _header_value(headers: Mapping[str, str], key: str) -> str | None:
    lowered_key = key.lower()
    for raw_key, value in headers.items():
        if str(raw_key).lower() == lowered_key:
            return str(value)
    return None


def _lower_headers(headers) -> dict[str, str]:  # type: ignore[no-untyped-def]
    return {str(key).lower(): str(value) for key, value in headers.items()}


def _nonnegative_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, number)


def _positive_float(value: Any, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number > 0 else default


def _result_status(error_code: str | None, network_attempted: bool) -> str:
    if error_code is None:
        return "pass"
    if error_code in BLOCKED_ERROR_CODES and not network_attempted:
        return "blocked"
    return "fail"


def _redactions_for_url(value: str | None, error_code: str | None) -> tuple[str, ...]:
    redactions = [
        "raw_response_body_omitted",
        "response_headers_omitted",
        "cookie_auth_headers_omitted",
    ]
    try:
        parsed = urllib.parse.urlsplit((value or "").strip())
    except ValueError:
        parsed = None
    if parsed is not None and (parsed.query or parsed.fragment or parsed.username or parsed.password):
        redactions.append("url_sensitive_parts_omitted")
    if error_code is not None:
        redactions.append("exception_detail_omitted")
    return tuple(redactions)
