from __future__ import annotations

import asyncio
import json

import pytest

from src.services.web_enricher.restricted_read_canary import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_REDIRECTS,
    DEFAULT_MAX_REQUESTS,
    RestrictedWebReadCanary,
    RestrictedWebReadCanaryConfig,
    RestrictedWebReadHttpResponse,
)


SECRET_BODY = b"sentinel_raw_body_value"
SECRET_HEADER = "sentinel_header_value"
SECRET_QUERY = "sentinel_query_value"
SECRET_EXCEPTION = "sentinel_exception_value"


class FakeHttpClient:
    def __init__(self, steps: list[RestrictedWebReadHttpResponse | BaseException]) -> None:
        self.steps = list(steps)
        self.calls: list[tuple[str, float, int]] = []

    async def get(self, url: str, *, timeout_sec: float, max_bytes: int) -> RestrictedWebReadHttpResponse:
        self.calls.append((url, timeout_sec, max_bytes))
        if not self.steps:
            raise AssertionError("unexpected fake HTTP call")
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step


def _response(
    status_code: int,
    *,
    content_type: str | None = "text/html; charset=utf-8",
    body: bytes = b"<html>ok</html>",
    location: str | None = None,
) -> RestrictedWebReadHttpResponse:
    headers: dict[str, str] = {
        "set-cookie": SECRET_HEADER,
        "authorization": SECRET_HEADER,
        "x-debug": SECRET_HEADER,
    }
    if content_type is not None:
        headers["content-type"] = content_type
    if location is not None:
        headers["location"] = location
    return RestrictedWebReadHttpResponse(status_code=status_code, headers=headers, body_bytes=body)


def _run(
    client: FakeHttpClient,
    *,
    url: str | None = "https://example.com/article",
    operator_approved: bool = True,
    allow_network: bool = True,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    max_redirects: int = DEFAULT_MAX_REDIRECTS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> dict:
    result = asyncio.run(
        RestrictedWebReadCanary(
            RestrictedWebReadCanaryConfig(
                url=url,
                operator_approved=operator_approved,
                allow_network=allow_network,
                max_requests=max_requests,
                max_redirects=max_redirects,
                max_bytes=max_bytes,
            ),
            http_client=client,
        ).run()
    )
    return result.to_sanitized_dict()


def _render(report: dict) -> str:
    return json.dumps(report, sort_keys=True)


def test_success_with_fake_client_returns_sanitized_json() -> None:
    client = FakeHttpClient([_response(200, body=SECRET_BODY)])

    report = _run(client, url=f"https://example.com/article?token={SECRET_QUERY}#fragment")
    text = _render(report)

    assert report["canary_name"] == "restricted_web_read_canary"
    assert report["mode"] == "restricted_live_read"
    assert report["url"] == "https://example.com/article"
    assert report["final_url_host"] == "example.com"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert report["redirect_count"] == 0
    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["error_code"] is None
    assert report["content_type"] == "text/html"
    assert report["status_code_class"] == "2xx"
    assert report["body_bytes_observed"] == len(SECRET_BODY)
    assert report["max_requests"] == DEFAULT_MAX_REQUESTS
    assert report["max_redirects"] == DEFAULT_MAX_REDIRECTS
    assert report["max_bytes"] == DEFAULT_MAX_BYTES
    assert all(value is False for value in report["side_effects"].values())
    assert SECRET_BODY.decode() not in text
    assert SECRET_HEADER not in text
    assert SECRET_QUERY not in text
    assert client.calls == [(f"https://example.com/article?token={SECRET_QUERY}", 6.0, DEFAULT_MAX_BYTES)]


def test_missing_approval_blocks_before_network() -> None:
    client = FakeHttpClient([_response(200)])

    report = _run(client, operator_approved=False)

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_missing_allow_network_blocks_before_network() -> None:
    client = FakeHttpClient([_response(200)])

    report = _run(client, allow_network=False)

    assert report["status"] == "blocked"
    assert report["error_code"] == "network_not_allowed"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_invalid_url_blocks_before_network() -> None:
    client = FakeHttpClient([_response(200)])

    report = _run(client, url="not a url")

    assert report["status"] == "blocked"
    assert report["error_code"] == "invalid_url_target"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_unsupported_scheme_blocks_before_network() -> None:
    client = FakeHttpClient([_response(200)])

    report = _run(client, url="ftp://example.com/file")

    assert report["status"] == "blocked"
    assert report["error_code"] == "unsupported_scheme"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/",
        "http://127.0.0.1/",
        "http://10.0.0.1/",
        "http://169.254.169.254/latest/meta-data/",
        "http://[::1]/",
        "http://metadata.google.internal/",
        "http://intranet/",
    ],
)
def test_private_local_internal_targets_block_before_network(url: str) -> None:
    client = FakeHttpClient([_response(200)])

    report = _run(client, url=url)

    assert report["status"] == "blocked"
    assert report["error_code"] == "private_or_local_target"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_redirect_cap_enforced() -> None:
    client = FakeHttpClient(
        [
            _response(302, location="https://example.com/next"),
            _response(302, location="https://example.com/final"),
        ]
    )

    report = _run(client, max_redirects=1)

    assert report["status"] == "fail"
    assert report["error_code"] == "redirect_limit_exceeded"
    assert report["network_attempted"] is True
    assert report["request_count"] == 2
    assert report["redirect_count"] == 1
    assert [call[0] for call in client.calls] == [
        "https://example.com/article",
        "https://example.com/next",
    ]


def test_request_cap_enforced() -> None:
    client = FakeHttpClient([_response(302, location="https://example.com/next")])

    report = _run(client, max_requests=1, max_redirects=3)

    assert report["status"] == "fail"
    assert report["error_code"] == "request_cap_exceeded"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert report["redirect_count"] == 1
    assert [call[0] for call in client.calls] == ["https://example.com/article"]


def test_timeout_network_errors_bucketed_and_redacted() -> None:
    client = FakeHttpClient([TimeoutError(SECRET_EXCEPTION)])

    report = _run(client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["error_code"] == "network_error"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert SECRET_EXCEPTION not in text


def test_unsupported_content_type_bucketed_and_redacted() -> None:
    client = FakeHttpClient([_response(200, content_type="application/json", body=SECRET_BODY)])

    report = _run(client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["error_code"] == "unsupported_content_type"
    assert report["content_type"] == "application/json"
    assert report["status_code_class"] == "2xx"
    assert report["body_bytes_observed"] is None
    assert SECRET_BODY.decode() not in text
    assert SECRET_HEADER not in text


@pytest.mark.parametrize(
    ("status_code", "error_code"),
    [
        (429, "rate_limited"),
        (403, "access_denied"),
        (404, "not_found"),
    ],
)
def test_http_statuses_bucketed(status_code: int, error_code: str) -> None:
    client = FakeHttpClient([_response(status_code, body=SECRET_BODY)])

    report = _run(client)

    assert report["status"] == "fail"
    assert report["error_code"] == error_code
    assert report["status_code_class"] == "4xx"
    assert report["body_bytes_observed"] is None


def test_raw_body_header_cookie_auth_exception_details_never_appear_in_json() -> None:
    client = FakeHttpClient([RuntimeError(SECRET_EXCEPTION)])

    report = _run(client, url=f"https://example.com/article?auth={SECRET_QUERY}")
    text = _render(report)

    assert report["error_code"] == "unexpected_error"
    assert SECRET_BODY.decode() not in text
    assert SECRET_HEADER not in text
    assert SECRET_QUERY not in text
    assert SECRET_EXCEPTION not in text
    assert "raw_response_body_omitted" in report["redactions_applied"]
    assert "response_headers_omitted" in report["redactions_applied"]
    assert "cookie_auth_headers_omitted" in report["redactions_applied"]
    assert "exception_detail_omitted" in report["redactions_applied"]


def test_all_side_effect_flags_are_false_on_blocked_and_failed_results() -> None:
    blocked = _run(FakeHttpClient([_response(200)]), operator_approved=False)
    failed = _run(FakeHttpClient([_response(429)]))

    assert all(value is False for value in blocked["side_effects"].values())
    assert all(value is False for value in failed["side_effects"].values())
