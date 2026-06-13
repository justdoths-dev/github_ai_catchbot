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

from src.services.x_enricher.restricted_read_canary import (
    DEFAULT_MAX_REQUESTS,
    DEFAULT_TIMEOUT_MS,
    DEFAULT_X_API_BASE_URL,
    RestrictedXReadCanaryConfig,
    RestrictedXReadHttpClient,
    run_restricted_x_read_canary,
)


DEFAULT_X_BEARER_TOKEN_ENV = "X_BEARER_TOKEN"


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a restricted operator-approved read-only X canary."
    )
    parser.add_argument("--post-id", help="Public X post ID to read")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--x-bearer-token-env", default=DEFAULT_X_BEARER_TOKEN_ENV)
    parser.add_argument("--x-base-url", default=DEFAULT_X_API_BASE_URL)
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS)
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=False) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    client: Any | None = None,
) -> RunnerResult:
    return asyncio.run(run_async(args, env=env, client=client))


async def run_async(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    client: Any | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    token_env_name = str(args.x_bearer_token_env or "").strip()
    bearer_token = str(effective_env.get(token_env_name, "")).strip() if token_env_name else ""
    config = RestrictedXReadCanaryConfig(
        post_id=args.post_id,
        operator_approved=bool(args.operator_approved),
        allow_network=bool(args.allow_network),
        bearer_token=bearer_token,
        x_base_url=str(args.x_base_url or "").strip(),
        max_requests=int(args.max_requests),
        timeout_ms=int(args.timeout_ms),
    )
    result = await run_restricted_x_read_canary(
        config,
        client=client or RestrictedXReadHttpClient(),
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
