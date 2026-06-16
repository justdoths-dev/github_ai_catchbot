from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.judge_openai.bounded_fake_execution_runner import (
    BoundedJudgeOpenAIFakeExecutionConfig,
    BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder,
    BoundedJudgeOpenAIFakeExecutionRepositoryBuilder,
    BoundedJudgeOpenAIFakeExecutionResult,
    BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
    BoundedJudgeOpenAIRedisReaderBuilder,
    argument_error_report,
    render_sanitized_json,
    run_bounded_judge_openai_fake_execution_sync,
)


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description=(
            "Execute one exact q.analysis.judge job with deterministic fake OpenAI "
            "and publish its judge.output.ready.v1 handoff."
        ),
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-fake-openai", action="store_true")
    parser.add_argument("--redis-message-suffix")
    parser.add_argument("--trigger-event-suffix")
    parser.add_argument("--scan-limit", type=int, default=25)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    redis_reader_builder: BoundedJudgeOpenAIRedisReaderBuilder | None = None,
    repository_builder: BoundedJudgeOpenAIFakeExecutionRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder | None = None,
) -> RunnerResult:
    redis_message_suffix = _parse_optional_redis_message_suffix(args.redis_message_suffix)
    trigger_event_suffix = _parse_optional_uuid_suffix(args.trigger_event_suffix)
    for parsed in (redis_message_suffix, trigger_event_suffix):
        if isinstance(parsed, dict):
            return RunnerResult(exit_code=1, report=parsed)

    runner_kwargs: dict[str, Any] = {
        "redis_reader_builder": redis_reader_builder,
        "repository_builder": repository_builder,
        "redis_publisher_builder": redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader

    result = run_bounded_judge_openai_fake_execution_sync(
        BoundedJudgeOpenAIFakeExecutionConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_redis_read=bool(args.allow_redis_read),
            allow_redis_publish=bool(args.allow_redis_publish),
            allow_database_read=bool(args.allow_database_read),
            allow_database_write=bool(args.allow_database_write),
            allow_fake_openai=bool(args.allow_fake_openai),
            redis_message_suffix=redis_message_suffix,
            trigger_event_suffix=trigger_event_suffix,
            scan_limit=int(args.scan_limit),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    redis_reader_builder: BoundedJudgeOpenAIRedisReaderBuilder | None = None,
    repository_builder: BoundedJudgeOpenAIFakeExecutionRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        redis_reader_builder=redis_reader_builder,
        repository_builder=repository_builder,
        redis_publisher_builder=redis_publisher_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _parse_optional_uuid_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped):
        return stripped
    return argument_error_report("invalid_trigger_event_suffix")


def _parse_optional_redis_message_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip()
    if 3 <= len(stripped) <= 64 and all(char in "0123456789-" for char in stripped):
        return stripped
    return argument_error_report("invalid_redis_message_suffix")


__all__ = [
    "BoundedJudgeOpenAIFakeExecutionConfig",
    "BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder",
    "BoundedJudgeOpenAIFakeExecutionRepositoryBuilder",
    "BoundedJudgeOpenAIFakeExecutionResult",
    "BoundedJudgeOpenAIFakeExecutionRuntimeConfig",
    "BoundedJudgeOpenAIRedisReaderBuilder",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
