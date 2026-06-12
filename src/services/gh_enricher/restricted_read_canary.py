from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from .github_app_auth import GitHubAppAuthError
from .github_client import (
    GitHubAccessDeniedError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitedError,
)


CANARY_NAME = "restricted_github_read_canary"
DEFAULT_MAX_REQUESTS = 3
REPO_FULL_NAME_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}/[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$"
)
BLOCKED_ERROR_CODES = frozenset(
    {
        "operator_approval_missing",
        "network_not_allowed",
        "invalid_repo_target",
        "credential_missing",
    }
)
ERROR_CODES = BLOCKED_ERROR_CODES | frozenset(
    {
        "auth_failed",
        "rate_limited",
        "not_found_or_not_authorized",
        "request_cap_exceeded",
        "network_error",
        "unexpected_error",
    }
)
SAFE_CREDENTIAL_SOURCE_KINDS = frozenset(
    {
        "github_app_env",
        "injected_fake",
        "test_fake",
        "unknown",
    }
)


class GitHubReadCanaryClient(Protocol):
    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> Mapping[str, Any]: ...

    async def get_default_branch_head(
        self,
        owner: str,
        repo: str,
        default_branch: str,
        *,
        auth_mode: str,
    ) -> Mapping[str, Any]: ...


class GitHubTokenProvider(Protocol):
    async def get_token(self) -> str: ...


class RedactedGitHubCanaryError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        safe_code = error_code if error_code in ERROR_CODES else "unexpected_error"
        self.error_code = safe_code
        super().__init__(safe_code)


@dataclass(slots=True)
class RestrictedGitHubReadCanaryRequestBudget:
    max_requests: int = DEFAULT_MAX_REQUESTS
    request_count: int = 0
    network_attempted: bool = False

    def acquire(self) -> None:
        if self.request_count >= self.max_requests:
            raise RedactedGitHubCanaryError("request_cap_exceeded")
        self.request_count += 1
        self.network_attempted = True


class RequestCountingTokenProvider:
    def __init__(
        self,
        token_provider: GitHubTokenProvider,
        request_budget: RestrictedGitHubReadCanaryRequestBudget,
    ) -> None:
        self._token_provider = token_provider
        self._request_budget = request_budget
        self._cached_token: str | None = None

    async def get_token(self) -> str:
        if self._cached_token is not None:
            return self._cached_token
        self._request_budget.acquire()
        try:
            self._cached_token = await self._token_provider.get_token()
            return self._cached_token
        except GitHubAppAuthError as exc:
            raise RedactedGitHubCanaryError("auth_failed") from exc
        except Exception as exc:  # noqa: BLE001 - operator output must stay bucketed.
            raise RedactedGitHubCanaryError("auth_failed") from exc


@dataclass(slots=True, frozen=True)
class RestrictedGitHubReadCanaryConfig:
    repo_full_name: str | None
    operator_approved: bool
    allow_network: bool
    credentials_present: bool
    credential_source_kind: str = "github_app_env"
    max_requests: int = DEFAULT_MAX_REQUESTS
    mode: str = "restricted_live_read"
    auth_mode: str = "app_installation"


@dataclass(slots=True, frozen=True)
class RestrictedGitHubReadCanaryResult:
    canary_name: str
    mode: str
    repo_full_name: str
    network_attempted: bool
    request_count: int
    status: str
    ok: bool
    error_code: str | None
    redactions_applied: tuple[str, ...]
    credential_source_kind: str
    observed_repo_visibility: str | None = None
    observed_default_branch: str | None = None
    observed_rate_limit_remaining: int | None = None
    max_requests: int = DEFAULT_MAX_REQUESTS
    side_effects: Mapping[str, bool] = field(default_factory=dict)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "canary_name": self.canary_name,
            "mode": self.mode,
            "repo_full_name": self.repo_full_name,
            "network_attempted": self.network_attempted,
            "request_count": self.request_count,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "redactions_applied": list(self.redactions_applied),
            "credential_source_kind": self.credential_source_kind,
            "observed_repo_visibility": self.observed_repo_visibility,
            "observed_default_branch": self.observed_default_branch,
            "observed_rate_limit_remaining": self.observed_rate_limit_remaining,
            "max_requests": self.max_requests,
            "side_effects": dict(self.side_effects),
        }


class RestrictedGitHubReadCanary:
    def __init__(
        self,
        config: RestrictedGitHubReadCanaryConfig,
        *,
        github_client: GitHubReadCanaryClient,
        request_budget: RestrictedGitHubReadCanaryRequestBudget | None = None,
    ) -> None:
        self._config = config
        self._github_client = github_client
        self._request_budget = request_budget or RestrictedGitHubReadCanaryRequestBudget(
            max_requests=config.max_requests
        )

    async def run(self) -> RestrictedGitHubReadCanaryResult:
        safe_repo = ""
        observed_visibility: str | None = None
        observed_default_branch: str | None = None
        error_code: str | None = None

        try:
            safe_repo = self._validate_preconditions()
            owner, repo = safe_repo.split("/", 1)

            repo_payload = await self._get_repo(owner, repo)
            observed_visibility = _repo_visibility(repo_payload)
            observed_default_branch = _optional_str(repo_payload.get("default_branch"))
            if observed_default_branch is None:
                raise RedactedGitHubCanaryError("unexpected_error")

            await self._get_default_branch_head(owner, repo, observed_default_branch)
        except RedactedGitHubCanaryError as exc:
            error_code = exc.error_code
        except GitHubRateLimitedError:
            error_code = "rate_limited"
        except (GitHubAccessDeniedError, GitHubNotFoundError):
            error_code = "not_found_or_not_authorized"
        except GitHubClientError:
            error_code = "network_error"
        except Exception:  # noqa: BLE001 - never expose raw exception text to operators.
            error_code = "unexpected_error"

        return self._result(
            repo_full_name=safe_repo,
            error_code=error_code,
            observed_repo_visibility=observed_visibility,
            observed_default_branch=observed_default_branch,
        )

    def _validate_preconditions(self) -> str:
        if not self._config.operator_approved:
            raise RedactedGitHubCanaryError("operator_approval_missing")
        if not self._config.allow_network:
            raise RedactedGitHubCanaryError("network_not_allowed")
        repo = normalize_repo_full_name(self._config.repo_full_name)
        if repo is None:
            raise RedactedGitHubCanaryError("invalid_repo_target")
        if not self._config.credentials_present:
            raise RedactedGitHubCanaryError("credential_missing")
        return repo

    async def _get_repo(self, owner: str, repo: str) -> Mapping[str, Any]:
        self._request_budget.acquire()
        payload = await self._github_client.get_repo(owner, repo, auth_mode=self._config.auth_mode)
        if not isinstance(payload, Mapping):
            raise RedactedGitHubCanaryError("unexpected_error")
        return payload

    async def _get_default_branch_head(
        self,
        owner: str,
        repo: str,
        default_branch: str,
    ) -> Mapping[str, Any]:
        self._request_budget.acquire()
        payload = await self._github_client.get_default_branch_head(
            owner,
            repo,
            default_branch,
            auth_mode=self._config.auth_mode,
        )
        if not isinstance(payload, Mapping):
            raise RedactedGitHubCanaryError("unexpected_error")
        return payload

    def _result(
        self,
        *,
        repo_full_name: str,
        error_code: str | None,
        observed_repo_visibility: str | None,
        observed_default_branch: str | None,
    ) -> RestrictedGitHubReadCanaryResult:
        status = "pass" if error_code is None else ("blocked" if error_code in BLOCKED_ERROR_CODES else "fail")
        redactions = ["credential_values_omitted", "raw_response_body_omitted"]
        if error_code is not None:
            redactions.append("exception_detail_omitted")

        return RestrictedGitHubReadCanaryResult(
            canary_name=CANARY_NAME,
            mode=_safe_mode(self._config.mode),
            repo_full_name=repo_full_name,
            network_attempted=self._request_budget.network_attempted,
            request_count=self._request_budget.request_count,
            status=status,
            ok=error_code is None,
            error_code=error_code,
            redactions_applied=tuple(redactions),
            credential_source_kind=_safe_credential_source_kind(self._config.credential_source_kind),
            observed_repo_visibility=observed_repo_visibility,
            observed_default_branch=observed_default_branch,
            observed_rate_limit_remaining=None,
            max_requests=self._request_budget.max_requests,
            side_effects={
                "database_write": False,
                "redis_write": False,
                "outbox_emit": False,
                "artifact_snapshot_write": False,
                "telegram_call": False,
                "openai_call": False,
                "worker_started": False,
                "repo_state_mutation": False,
            },
        )


def normalize_repo_full_name(value: str | None) -> str | None:
    repo = (value or "").strip()
    if not repo or not REPO_FULL_NAME_RE.fullmatch(repo):
        return None
    owner, name = repo.split("/", 1)
    if ".." in owner or ".." in name:
        return None
    return f"{owner}/{name}"


def _repo_visibility(payload: Mapping[str, Any]) -> str | None:
    visibility = payload.get("visibility")
    if isinstance(visibility, str) and visibility in {"public", "private", "internal"}:
        return visibility
    private = payload.get("private")
    if isinstance(private, bool):
        return "private" if private else "public"
    return None


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _safe_credential_source_kind(value: str) -> str:
    return value if value in SAFE_CREDENTIAL_SOURCE_KINDS else "unknown"


def _safe_mode(value: str) -> str:
    return value if value == "restricted_live_read" else "restricted_live_read"
