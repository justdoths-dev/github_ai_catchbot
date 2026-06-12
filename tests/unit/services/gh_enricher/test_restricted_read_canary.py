from __future__ import annotations

import asyncio
import json

from src.services.gh_enricher.github_client import (
    GitHubAccessDeniedError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitedError,
)
from src.services.gh_enricher.restricted_read_canary import (
    RequestCountingTokenProvider,
    RestrictedGitHubReadCanary,
    RestrictedGitHubReadCanaryConfig,
    RestrictedGitHubReadCanaryRequestBudget,
    RedactedGitHubCanaryError,
)


TOKEN_VALUE = "sentinel_auth_value"
PRIVATE_KEY_VALUE = "sentinel_private_key_material"


class FakeGitHubClient:
    def __init__(
        self,
        *,
        repo_payload: dict | None = None,
        head_payload: dict | None = None,
        repo_error: Exception | None = None,
        head_error: Exception | None = None,
    ) -> None:
        self.repo_payload = repo_payload or {
            "full_name": "octocat/Hello-World",
            "visibility": "public",
            "default_branch": "main",
        }
        self.head_payload = head_payload or {"sha": "abc123"}
        self.repo_error = repo_error
        self.head_error = head_error
        self.calls: list[tuple[str, str, str, str]] = []

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str):
        self.calls.append(("get_repo", owner, repo, auth_mode))
        if self.repo_error is not None:
            raise self.repo_error
        return self.repo_payload

    async def get_default_branch_head(self, owner: str, repo: str, default_branch: str, *, auth_mode: str):
        self.calls.append(("get_default_branch_head", owner, repo, default_branch, auth_mode))
        if self.head_error is not None:
            raise self.head_error
        return self.head_payload


class FakeTokenProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def get_token(self) -> str:
        self.calls += 1
        return TOKEN_VALUE


def _config(**overrides) -> RestrictedGitHubReadCanaryConfig:
    values = {
        "repo_full_name": "octocat/Hello-World",
        "operator_approved": True,
        "allow_network": True,
        "credentials_present": True,
        "credential_source_kind": "injected_fake",
        "max_requests": 3,
    }
    values.update(overrides)
    return RestrictedGitHubReadCanaryConfig(**values)


def _run(config: RestrictedGitHubReadCanaryConfig, client: FakeGitHubClient, *, budget=None):
    return asyncio.run(
        RestrictedGitHubReadCanary(
            config,
            github_client=client,
            request_budget=budget,
        ).run()
    )


def test_fake_client_success_returns_sanitized_read_only_json() -> None:
    client = FakeGitHubClient()

    result = _run(_config(), client)
    payload = result.to_sanitized_dict()

    assert result.ok is True
    assert payload["status"] == "pass"
    assert payload["repo_full_name"] == "octocat/Hello-World"
    assert payload["network_attempted"] is True
    assert payload["request_count"] == 2
    assert payload["error_code"] is None
    assert payload["credential_source_kind"] == "injected_fake"
    assert payload["observed_repo_visibility"] == "public"
    assert payload["observed_default_branch"] == "main"
    assert payload["observed_rate_limit_remaining"] is None
    assert payload["side_effects"] == {
        "database_write": False,
        "redis_write": False,
        "outbox_emit": False,
        "artifact_snapshot_write": False,
        "telegram_call": False,
        "openai_call": False,
        "worker_started": False,
        "repo_state_mutation": False,
    }
    assert client.calls == [
        ("get_repo", "octocat", "Hello-World", "app_installation"),
        ("get_default_branch_head", "octocat", "Hello-World", "main", "app_installation"),
    ]


def test_approval_network_repo_and_credential_gates_block_before_network() -> None:
    cases = [
        (_config(operator_approved=False), "operator_approval_missing"),
        (_config(allow_network=False), "network_not_allowed"),
        (_config(repo_full_name="octocat"), "invalid_repo_target"),
        (_config(repo_full_name="octocat/Hello World"), "invalid_repo_target"),
        (_config(credentials_present=False), "credential_missing"),
    ]

    for config, expected_code in cases:
        client = FakeGitHubClient()
        result = _run(config, client)

        assert result.ok is False
        assert result.status == "blocked"
        assert result.error_code == expected_code
        assert result.network_attempted is False
        assert result.request_count == 0
        assert client.calls == []


def test_request_cap_is_enforced_without_retrying_past_cap() -> None:
    client = FakeGitHubClient()

    result = _run(_config(max_requests=1), client)

    assert result.ok is False
    assert result.error_code == "request_cap_exceeded"
    assert result.request_count == 1
    assert [call[0] for call in client.calls] == ["get_repo"]


def test_auth_rate_limit_access_and_network_failures_are_bucketed_and_redacted() -> None:
    cases = [
        (RedactedGitHubCanaryError("auth_failed"), "auth_failed"),
        (GitHubRateLimitedError(f"rate limited {TOKEN_VALUE}"), "rate_limited"),
        (GitHubAccessDeniedError(f"denied {TOKEN_VALUE}"), "not_found_or_not_authorized"),
        (GitHubNotFoundError(f"missing {TOKEN_VALUE}"), "not_found_or_not_authorized"),
        (GitHubClientError(f"network body {TOKEN_VALUE}"), "network_error"),
        (RuntimeError(f"unexpected {TOKEN_VALUE}"), "unexpected_error"),
    ]

    for error, expected_code in cases:
        client = FakeGitHubClient(repo_error=error)
        result = _run(_config(), client)
        text = json.dumps(result.to_sanitized_dict(), sort_keys=True)

        assert result.ok is False
        assert result.error_code == expected_code
        assert TOKEN_VALUE not in text
        assert "Authorization" not in text


def test_default_branch_head_failure_is_classified_without_body_leakage() -> None:
    client = FakeGitHubClient(head_error=GitHubRateLimitedError(f"head failed {TOKEN_VALUE}"))

    result = _run(_config(), client)
    text = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "rate_limited"
    assert result.request_count == 2
    assert TOKEN_VALUE not in text


def test_counting_token_provider_counts_one_token_network_request_and_caches_value() -> None:
    token_provider = FakeTokenProvider()
    budget = RestrictedGitHubReadCanaryRequestBudget(max_requests=3)
    counting = RequestCountingTokenProvider(token_provider, budget)

    first = asyncio.run(counting.get_token())
    second = asyncio.run(counting.get_token())

    assert first == TOKEN_VALUE
    assert second == TOKEN_VALUE
    assert token_provider.calls == 1
    assert budget.request_count == 1
    assert budget.network_attempted is True


def test_credential_source_kind_is_label_only() -> None:
    client = FakeGitHubClient()
    result = _run(_config(credential_source_kind=PRIVATE_KEY_VALUE), client)
    text = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.credential_source_kind == "unknown"
    assert PRIVATE_KEY_VALUE not in text
