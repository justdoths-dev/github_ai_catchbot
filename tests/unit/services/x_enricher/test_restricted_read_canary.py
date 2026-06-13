from __future__ import annotations

import asyncio
import json

import pytest

from src.services.x_enricher.restricted_read_canary import (
    DEFAULT_MAX_REQUESTS,
    RestrictedXReadCanaryConfig,
    RestrictedXReadCanaryRequestBudget,
    RestrictedXReadHttpResponse,
    run_restricted_x_read_canary,
)


POST_ID = "1881234567890123456"
TOKEN_VALUE = "sentinel_x_bearer_token"
SECRET_TEXT = "sentinel_post_text_value"
SECRET_AUTHOR = "sentinel_author_identity"
SECRET_MEDIA_URL = "https://media.example/sentinel.jpg"
SECRET_EXCEPTION = "sentinel_exception_detail"


class FakeXReadClient:
    def __init__(
        self,
        response: RestrictedXReadHttpResponse | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.response = response or _response()
        self.error = error
        self.calls: list[tuple[str, str, str, int]] = []

    async def get_post_by_id(
        self,
        *,
        base_url: str,
        bearer_token: str,
        post_id: str,
        timeout_ms: int,
    ) -> RestrictedXReadHttpResponse:
        self.calls.append((base_url, bearer_token, post_id, timeout_ms))
        if self.error is not None:
            raise self.error
        return self.response


def _response(
    status_code: int = 200,
    *,
    content_type: str | None = "application/json; charset=utf-8",
    payload: dict | None = None,
) -> RestrictedXReadHttpResponse:
    return RestrictedXReadHttpResponse(
        status_code=status_code,
        content_type=content_type,
        payload=payload
        if payload is not None
        else {
            "data": [
                {
                    "id": POST_ID,
                    "text": SECRET_TEXT,
                    "author_id": "42",
                    "attachments": {"media_keys": ["m1"]},
                }
            ],
            "includes": {
                "users": [{"id": "42", "username": SECRET_AUTHOR, "name": SECRET_AUTHOR}],
                "media": [{"media_key": "m1", "url": SECRET_MEDIA_URL}],
            },
            "errors": [{"title": "partial non-root warning"}],
        },
    )


def _config(**overrides) -> RestrictedXReadCanaryConfig:
    values = {
        "post_id": POST_ID,
        "operator_approved": True,
        "allow_network": True,
        "bearer_token": TOKEN_VALUE,
        "x_base_url": "https://api.x.com",
        "max_requests": DEFAULT_MAX_REQUESTS,
    }
    values.update(overrides)
    return RestrictedXReadCanaryConfig(**values)


def _run(
    config: RestrictedXReadCanaryConfig,
    client: FakeXReadClient,
    *,
    budget: RestrictedXReadCanaryRequestBudget | None = None,
) -> dict:
    result = asyncio.run(
        run_restricted_x_read_canary(
            config,
            client=client,
            request_budget=budget,
        )
    )
    return result.to_sanitized_dict()


def _render(report: dict) -> str:
    return json.dumps(report, sort_keys=True)


@pytest.mark.parametrize(
    ("config", "expected_code"),
    [
        (_config(operator_approved=False), "operator_approval_missing"),
        (_config(allow_network=False), "network_not_allowed"),
        (_config(post_id=None), "post_id_missing"),
        (_config(post_id=""), "post_id_missing"),
        (_config(post_id="188abc"), "post_id_invalid"),
        (_config(bearer_token=""), "credential_missing"),
        (_config(x_base_url="https://example.com"), "base_url_not_allowed"),
        (_config(x_base_url="http://api.x.com"), "base_url_not_allowed"),
        (_config(max_requests=0), "request_cap_invalid"),
    ],
)
def test_precondition_failures_block_before_network(config: RestrictedXReadCanaryConfig, expected_code: str) -> None:
    client = FakeXReadClient()

    report = _run(config, client)

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == expected_code
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_success_with_fake_client_returns_sanitized_read_only_json() -> None:
    client = FakeXReadClient()

    report = _run(_config(), client)
    text = _render(report)

    assert report["canary_name"] == "restricted_x_read_canary"
    assert report["mode"] == "restricted_live_read"
    assert report["post_id"] == POST_ID
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert report["max_requests"] == DEFAULT_MAX_REQUESTS
    assert report["status"] == "pass"
    assert report["ok"] is True
    assert report["error_code"] is None
    assert report["status_code_class"] == "2xx"
    assert report["content_type"] == "application/json"
    assert report["observed_post_count"] == 1
    assert report["observed_root_post_found"] is True
    assert report["includes_present"] is True
    assert report["errors_count"] == 1
    assert report["side_effects"] == {
        "database_write": False,
        "redis_write": False,
        "outbox_emit": False,
        "artifact_snapshot_write": False,
        "telegram_call": False,
        "openai_call": False,
        "worker_started": False,
        "repo_state_mutation": False,
    }
    assert TOKEN_VALUE not in text
    assert "Authorization" not in text
    assert SECRET_TEXT not in text
    assert SECRET_AUTHOR not in text
    assert SECRET_MEDIA_URL not in text
    assert client.calls == [("https://api.x.com", TOKEN_VALUE, POST_ID, 8000)]


@pytest.mark.parametrize(
    ("status_code", "expected_code"),
    [
        (429, "x_rate_limited"),
        (401, "x_access_denied"),
        (403, "x_access_denied"),
        (500, "x_transient_error"),
    ],
)
def test_x_status_failures_are_bucketed(status_code: int, expected_code: str) -> None:
    client = FakeXReadClient(_response(status_code=status_code))

    report = _run(_config(), client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["ok"] is False
    assert report["error_code"] == expected_code
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert report["status_code_class"] in {"4xx", "5xx"}
    assert TOKEN_VALUE not in text
    assert SECRET_TEXT not in text


@pytest.mark.parametrize(
    "response",
    [
        _response(status_code=404),
        _response(payload={"data": [], "errors": [{"title": "Not Found"}]}),
    ],
)
def test_404_or_missing_root_post_maps_to_x_not_found(response: RestrictedXReadHttpResponse) -> None:
    client = FakeXReadClient(response)

    report = _run(_config(), client)

    assert report["status"] == "fail"
    assert report["error_code"] == "x_not_found"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1


@pytest.mark.parametrize(
    "response",
    [
        _response(content_type="text/html"),
        _response(payload={"data": {"id": POST_ID}}),
        RestrictedXReadHttpResponse(status_code=200, content_type="application/json", payload=None),
    ],
)
def test_malformed_response_maps_to_x_response_invalid(response: RestrictedXReadHttpResponse) -> None:
    client = FakeXReadClient(response)

    report = _run(_config(), client)

    assert report["status"] == "fail"
    assert report["error_code"] == "x_response_invalid"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1


def test_request_cap_exceeded_blocks_without_calling_client() -> None:
    client = FakeXReadClient()
    budget = RestrictedXReadCanaryRequestBudget(max_requests=0)

    report = _run(_config(), client, budget=budget)

    assert report["status"] == "blocked"
    assert report["error_code"] == "request_cap_exceeded"
    assert report["network_attempted"] is False
    assert report["request_count"] == 0
    assert client.calls == []


def test_client_exception_is_bucketed_without_exception_detail_leakage() -> None:
    client = FakeXReadClient(error=RuntimeError(SECRET_EXCEPTION))

    report = _run(_config(), client)
    text = _render(report)

    assert report["status"] == "fail"
    assert report["error_code"] == "x_transient_error"
    assert report["network_attempted"] is True
    assert report["request_count"] == 1
    assert SECRET_EXCEPTION not in text
    assert "exception_detail_omitted" in report["redactions_applied"]
