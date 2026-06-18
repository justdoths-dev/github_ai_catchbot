from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.outbox_relay.bounded_judge_output_ready_outbox_publish_runner import (
    DEFAULT_SCAN_LIMIT,
    BoundedJudgeOutputReadyOutboxPublishConfig,
    BoundedJudgeOutputReadyOutboxPublishResult,
    BoundedJudgeOutputReadyPublishRuntimeConfig,
    BoundedJudgeOutputReadyRedisPublisherBuilder,
    BoundedJudgeOutputReadyRepositoryBuilder,
    argument_error_report,
    render_sanitized_json,
    run_bounded_judge_output_ready_outbox_publish_sync,
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
        description="Publish exactly one judge.output.ready.v1 outbox row to q.analysis.validate.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--trigger-event-suffix")
    parser.add_argument("--judge-run-suffix")
    parser.add_argument("--judge-output-suffix")
    parser.add_argument("--scan-limit", type=int, default=DEFAULT_SCAN_LIMIT)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedJudgeOutputReadyRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOutputReadyRedisPublisherBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "repository_builder": repository_builder,
        "redis_publisher_builder": redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_judge_output_ready_outbox_publish_sync(
        BoundedJudgeOutputReadyOutboxPublishConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_database_write=bool(args.allow_database_write),
            allow_redis_publish=bool(args.allow_redis_publish),
            trigger_event_suffix=_clean_optional_suffix(args.trigger_event_suffix),
            judge_run_suffix=_clean_optional_suffix(args.judge_run_suffix),
            judge_output_suffix=_clean_optional_suffix(args.judge_output_suffix),
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
    repository_builder: BoundedJudgeOutputReadyRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOutputReadyRedisPublisherBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        repository_builder=repository_builder,
        redis_publisher_builder=redis_publisher_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _clean_optional_suffix(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().lower()


__all__ = [
    "BoundedJudgeOutputReadyOutboxPublishConfig",
    "BoundedJudgeOutputReadyOutboxPublishResult",
    "BoundedJudgeOutputReadyPublishRuntimeConfig",
    "BoundedJudgeOutputReadyRedisPublisherBuilder",
    "BoundedJudgeOutputReadyRepositoryBuilder",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
