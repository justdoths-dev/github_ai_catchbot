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

from src.services.judge_openai.restricted_judge_canary import (
    DEFAULT_FIXTURE_PROFILE,
    DEFAULT_MAX_INPUT_CHARS,
    DEFAULT_MAX_OUTPUT_TOKENS,
    DEFAULT_MAX_REQUESTS,
    DEFAULT_MODEL,
    DEFAULT_REASONING_EFFORT,
    DEFAULT_TIMEOUT_MS,
    RestrictedOpenAIJudgeCanaryConfig,
    run_restricted_openai_judge_canary,
)


DEFAULT_OPENAI_API_KEY_ENV = "OPENAI_API_KEY"


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a restricted operator-approved OpenAI judge canary."
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--openai-api-key-env", default=DEFAULT_OPENAI_API_KEY_ENV)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    parser.add_argument("--fixture-profile", default=DEFAULT_FIXTURE_PROFILE)
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS)
    parser.add_argument("--max-output-tokens", type=int, default=DEFAULT_MAX_OUTPUT_TOKENS)
    parser.add_argument("--max-input-chars", type=int, default=DEFAULT_MAX_INPUT_CHARS)
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
    api_key_env_name = str(args.openai_api_key_env or "").strip()
    api_key = str(effective_env.get(api_key_env_name, "")).strip() if api_key_env_name else ""
    config = RestrictedOpenAIJudgeCanaryConfig(
        operator_approved=bool(args.operator_approved),
        allow_network=bool(args.allow_network),
        api_key=api_key,
        model=str(args.model or "").strip(),
        reasoning_effort=str(args.reasoning_effort or "").strip(),
        fixture_profile=str(args.fixture_profile or "").strip(),
        max_requests=int(args.max_requests),
        timeout_ms=int(args.timeout_ms),
        max_output_tokens=int(args.max_output_tokens),
        max_input_chars=int(args.max_input_chars),
    )
    result = await run_restricted_openai_judge_canary(config, client=client)
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
