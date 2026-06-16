from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from uuid import UUID


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.outbox_relay.bounded_judge_call_requested_outbox_publish_runner import (
    BoundedJudgeCallRequestedOutboxPublishConfig,
    BoundedJudgeCallRequestedOutboxPublishResult,
    BoundedJudgeCallRequestedPublishRuntimeConfig,
    BoundedJudgeCallRequestedRedisPublisherBuilder,
    BoundedJudgeCallRequestedRepositoryBuilder,
    argument_error_report,
    render_sanitized_json,
    run_bounded_judge_call_requested_outbox_publish_sync,
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
        description="Publish exactly one pending judge.call.requested.v1 outbox row to q.analysis.judge.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--trigger-event-id")
    parser.add_argument("--trigger-event-suffix")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedJudgeCallRequestedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeCallRequestedRedisPublisherBuilder | None = None,
) -> RunnerResult:
    trigger_event_id = _parse_optional_uuid(args.trigger_event_id, "invalid_trigger_event_id")
    trigger_event_suffix = _parse_optional_event_suffix(args.trigger_event_suffix)
    if isinstance(trigger_event_id, dict):
        return RunnerResult(exit_code=1, report=trigger_event_id)
    if isinstance(trigger_event_suffix, dict):
        return RunnerResult(exit_code=1, report=trigger_event_suffix)

    runner_kwargs: dict[str, Any] = {
        "repository_builder": repository_builder,
        "redis_publisher_builder": redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_judge_call_requested_outbox_publish_sync(
        BoundedJudgeCallRequestedOutboxPublishConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_redis_publish=bool(args.allow_redis_publish),
            allow_database_write=bool(args.allow_database_write),
            trigger_event_id=trigger_event_id,
            trigger_event_suffix=trigger_event_suffix,
            max_events=1,
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedJudgeCallRequestedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeCallRequestedRedisPublisherBuilder | None = None,
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


def _parse_optional_uuid(value: str | None, error_code: str) -> UUID | None | dict[str, Any]:
    if value is None:
        return None
    try:
        return UUID(value)
    except ValueError:
        return argument_error_report(error_code)


def _parse_optional_event_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped):
        return stripped
    return argument_error_report("invalid_trigger_event_suffix")


__all__ = [
    "BoundedJudgeCallRequestedOutboxPublishConfig",
    "BoundedJudgeCallRequestedOutboxPublishResult",
    "BoundedJudgeCallRequestedPublishRuntimeConfig",
    "BoundedJudgeCallRequestedRedisPublisherBuilder",
    "BoundedJudgeCallRequestedRepositoryBuilder",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
