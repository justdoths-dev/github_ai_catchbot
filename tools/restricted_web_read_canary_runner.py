from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.web_enricher.restricted_read_canary import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_REDIRECTS,
    DEFAULT_MAX_REQUESTS,
    DEFAULT_TIMEOUT_SEC,
    RestrictedWebReadCanary,
    RestrictedWebReadCanaryConfig,
    RestrictedWebReadCanaryRequestBudget,
    RestrictedWebReadHttpClient,
)


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a restricted operator-approved read-only Web canary."
    )
    parser.add_argument("--url", help="Public HTTP(S) URL to read")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--max-redirects", type=int, default=DEFAULT_MAX_REDIRECTS)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--timeout-sec", type=float, default=DEFAULT_TIMEOUT_SEC)
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=False) + "\n"


def run(
    args: argparse.Namespace,
    *,
    http_client: Any | None = None,
) -> RunnerResult:
    return asyncio.run(run_async(args, http_client=http_client))


async def run_async(
    args: argparse.Namespace,
    *,
    http_client: Any | None = None,
) -> RunnerResult:
    request_budget = RestrictedWebReadCanaryRequestBudget(max_requests=max(0, int(args.max_requests)))
    config = RestrictedWebReadCanaryConfig(
        url=args.url,
        operator_approved=bool(args.operator_approved),
        allow_network=bool(args.allow_network),
        max_requests=request_budget.max_requests,
        max_redirects=max(0, int(args.max_redirects)),
        max_bytes=max(0, int(args.max_bytes)),
        timeout_sec=float(args.timeout_sec),
    )
    canary = RestrictedWebReadCanary(
        config,
        http_client=http_client or RestrictedWebReadHttpClient(),
        request_budget=request_budget,
    )
    result = await canary.run()
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
