from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.gh_enricher.github_app_auth import GitHubAppTokenProvider
from src.services.gh_enricher.github_client import GitHubClient
from src.services.gh_enricher.restricted_read_canary import (
    DEFAULT_MAX_REQUESTS,
    RequestCountingTokenProvider,
    RestrictedGitHubReadCanary,
    RestrictedGitHubReadCanaryConfig,
    RestrictedGitHubReadCanaryRequestBudget,
)


DEFAULT_GITHUB_API_BASE_URL = "https://api.github.com"


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a restricted operator-approved read-only GitHub canary."
    )
    parser.add_argument("--repo", help="GitHub repository target in owner/name form")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--github-api-base-url", default=None)
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=False) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    github_client: Any | None = None,
) -> RunnerResult:
    return asyncio.run(run_async(args, env=env, github_client=github_client))


async def run_async(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    github_client: Any | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    request_budget = RestrictedGitHubReadCanaryRequestBudget(max_requests=int(args.max_requests))
    credentials = _github_app_credentials(effective_env)
    api_base_url = str(
        args.github_api_base_url
        or effective_env.get("GITHUB_API_BASE_URL")
        or DEFAULT_GITHUB_API_BASE_URL
    ).strip()
    config = RestrictedGitHubReadCanaryConfig(
        repo_full_name=args.repo,
        operator_approved=bool(args.operator_approved),
        allow_network=bool(args.allow_network),
        credentials_present=all(credentials.values()),
        credential_source_kind="github_app_env",
        max_requests=request_budget.max_requests,
    )

    active_client = github_client
    if active_client is None:
        token_provider = None
        if all(credentials.values()):
            token_provider = RequestCountingTokenProvider(
                GitHubAppTokenProvider(
                    app_id=credentials["app_id"],
                    installation_id=credentials["installation_id"],
                    private_key_pem=credentials["private_key"],
                    api_base_url=api_base_url,
                    timeout_sec=_request_timeout(effective_env),
                ),
                request_budget,
            )
        active_client = GitHubClient(
            api_base_url=api_base_url,
            timeout_sec=_request_timeout(effective_env),
            token_provider=token_provider,
        )

    canary = RestrictedGitHubReadCanary(
        config,
        github_client=active_client,
        request_budget=request_budget,
    )
    result = await canary.run()
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def _github_app_credentials(env: Mapping[str, str]) -> dict[str, str]:
    return {
        "app_id": str(env.get("GITHUB_APP_ID") or "").strip(),
        "installation_id": str(env.get("GITHUB_INSTALLATION_ID") or "").strip(),
        "private_key": str(env.get("GITHUB_PRIVATE_KEY") or "").strip(),
    }


def _request_timeout(env: Mapping[str, str]) -> float:
    raw = str(env.get("GH_ENRICHER_REQUEST_TIMEOUT_SEC") or "10").strip()
    try:
        timeout = float(raw)
    except ValueError:
        return 10.0
    return timeout if timeout > 0 else 10.0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
