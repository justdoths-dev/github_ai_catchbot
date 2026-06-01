from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.services.judge_openai.openai_client import OpenAIJudgeClient
from src.services.judge_openai.request_shape import (
    LOCKED_HOT_PATH_MODEL,
    summarize_responses_request_shape,
)
from src.services.judge_openai.service import JudgeOpenAIService


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_request_shape_diagnostic"
REPORT_TYPE = "judge_openai_request_shape_diagnostic_v1"
STATUS_PASSED = "judge_openai_request_shape_diagnostic_passed"
STATUS_FAILED = "blocked_judge_openai_request_shape_diagnostic_failed"
DEFAULT_REASONING_EFFORT = "low"
DEFAULT_PROMPT_CACHE_KEY = "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="No-network judge-openai Responses API request-shape diagnostic."
    )
    parser.add_argument("--model", default=LOCKED_HOT_PATH_MODEL)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    parser.add_argument("--max-output-tokens", type=int, default=500)
    parser.add_argument("--omit-prompt-cache-key", action="store_true")
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def generate_report(
    *,
    model: str = LOCKED_HOT_PATH_MODEL,
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    max_output_tokens: int | None = 500,
    prompt_cache_key: str | None = DEFAULT_PROMPT_CACHE_KEY,
) -> dict[str, Any]:
    request = OpenAIJudgeClient.build_request(
        model=model,
        reasoning_effort=reasoning_effort,
        developer_prompt="diagnostic developer prompt placeholder",
        user_context="diagnostic user context placeholder",
        json_schema=JudgeOpenAIService.judge_output_schema(),
        max_output_tokens=max_output_tokens,
        prompt_cache_key=prompt_cache_key,
    )
    summary = summarize_responses_request_shape(request)
    status = STATUS_PASSED if summary["request_shape_valid_bucket"] == "one" else STATUS_FAILED
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": status,
        **summary,
        "live_openai_call_attempted": False,
        "database_write_attempted": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "checks_failed": [] if status == STATUS_PASSED else ["request_shape.invalid"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = generate_report(
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        max_output_tokens=args.max_output_tokens,
        prompt_cache_key=None if args.omit_prompt_cache_key else DEFAULT_PROMPT_CACHE_KEY,
    )
    print(json.dumps(report, sort_keys=True))
    return 0 if report["contract_status"] == STATUS_PASSED else 1


if __name__ == "__main__":
    raise SystemExit(main())
